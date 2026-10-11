package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type HistoryRecompressResult struct {
	Source, Output, Format, LogicalSHA256, FileSHA256 string
	SourceBytes, LogicalBytes, OutputBytes            uint64
	EncodeSeconds, VerifySeconds                      float64
}

// RecompressHistoryDiagnostic compares an R1/V2 source with production V2 and CDC V3
// codecs on exactly the same immutable logical stream. Outputs live in a new
// private directory. It never publishes, replaces or retires a source artifact.
// The complete output is decoded and its logical SHA-256 checked, so all old
// accessor offsets and all original history bytes remain valid unchanged.
func RecompressHistoryDiagnostic(ctx context.Context, source, outputDir string) ([]HistoryRecompressResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	var magic [8]byte
	if err == nil {
		err = historyReferenceReadAt(ctx, f, magic[:], 0)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil, err
	}
	var r historySegmentReader
	var logical uint64
	if string(magic[:]) == historyReferenceMagic {
		rr, err := openHistoryReferenceReader(ctx, source, historyReferenceMaxCache)
		if err != nil {
			return nil, err
		}
		r, logical = rr, rr.UncompressedSize()
	} else {
		cr, err := openCompressedBlockReaderWithCacheLimitContext(ctx, source, 1)
		if err != nil {
			return nil, err
		}
		r, logical = cr, cr.UncompressedSize()
	}
	defer r.Close()
	// The diagnostic also emits V2, whose in-memory directory is proportional
	// to logical size. Migration itself only emits bounded, paged CDC output.
	if logical > 64<<30 {
		return nil, errors.New("recompress diagnostic requires source <=64 GiB logical")
	}
	// Requiring a new directory also prevents overwriting previous evidence.
	if err := os.Mkdir(outputDir, 0700); err != nil {
		return nil, err
	}
	var results []HistoryRecompressResult
	var expected []byte
	buffer := make([]byte, 1<<20)
	for _, format := range []string{"2", "3"} {
		path := filepath.Join(outputDir, "v"+format+".seg")
		started := time.Now()
		stream, err := newHistoryCompressedStreamFormat(ctx, outputDir, historyCompressChunkSize, 1, format)
		if err != nil {
			return results, err
		}
		digest := sha256.New()
		var offset uint64
		for offset < logical {
			if err := ctx.Err(); err != nil {
				stream.Abort()
				return results, err
			}
			part := buffer[:min(uint64(len(buffer)), logical-offset)]
			if _, err := r.ReadAt(part, int64(offset)); err != nil {
				stream.Abort()
				return results, err
			}
			digest.Write(part)
			if _, err := stream.Write(part); err != nil {
				stream.Abort()
				return results, err
			}
			offset += uint64(len(part))
		}
		metadata, err := stream.FinishWithMetadataContext(ctx, path)
		stream.Abort()
		if err != nil {
			return results, err
		}
		encodeSeconds := time.Since(started).Seconds()
		sum := digest.Sum(nil)
		if expected == nil {
			expected = sum
		} else if !bytes.Equal(expected, sum) {
			return results, errors.New("source logical bytes changed")
		}
		encoded, err := openCompressedBlockReaderWithCacheLimit(path, 1)
		if err != nil {
			return results, err
		}
		started = time.Now()
		check := sha256.New()
		if encoded.uncSize != logical {
			encoded.Close()
			return results, errors.New("recompressed logical length differs")
		}
		for offset = 0; offset < encoded.uncSize; {
			if err = ctx.Err(); err != nil {
				break
			}
			part := buffer[:min(uint64(len(buffer)), encoded.uncSize-offset)]
			if _, err = encoded.ReadAt(part, int64(offset)); err != nil {
				break
			}
			check.Write(part)
			offset += uint64(len(part))
		}
		encoded.Close()
		if err != nil {
			return results, err
		}
		if !bytes.Equal(check.Sum(nil), sum) {
			return results, fmt.Errorf("format %s logical SHA-256 mismatch", format)
		}
		results = append(results, HistoryRecompressResult{Source: source, Output: path, Format: format, LogicalSHA256: hex.EncodeToString(sum), FileSHA256: hex.EncodeToString(metadata.checksum[:]), SourceBytes: uint64(info.Size()), LogicalBytes: logical, OutputBytes: metadata.size, EncodeSeconds: encodeSeconds, VerifySeconds: time.Since(started).Seconds()})
	}
	return results, nil
}
