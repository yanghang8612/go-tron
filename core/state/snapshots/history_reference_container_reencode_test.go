package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	referenceCodecSampleEnv    = "GTRON_REFERENCE_CODEC_SAMPLE"
	referenceCodecOutputDirEnv = "GTRON_REFERENCE_CODEC_OUTPUT_DIR"
	referenceCodecMaxPhysical  = uint64(512 << 20)
	referenceCodecMaxLogical   = uint64(2 << 30)
	referenceCodecTimeout      = 10 * time.Minute
)

type referenceCodecContainerReport struct {
	Path             string            `json:"path"`
	PhysicalBytes    uint64            `json:"physical_bytes"`
	StoredPayload    uint64            `json:"stored_payload_bytes"`
	ChunkDirectory   uint64            `json:"chunk_directory_bytes"`
	SpanDirectory    uint64            `json:"span_directory_bytes"`
	LogicalBytes     uint64            `json:"logical_bytes"`
	Chunks           uint64            `json:"chunks"`
	Spans            uint64            `json:"spans"`
	CodecChunks      map[uint32]int    `json:"codec_chunks"`
	CodecStoredBytes map[uint32]uint64 `json:"codec_stored_bytes"`
}

type referenceCodecReencodeReport struct {
	Scope                 string                        `json:"scope"`
	Source                referenceCodecContainerReport `json:"source"`
	Candidate             referenceCodecContainerReport `json:"candidate"`
	SourceValidateMS      int64                         `json:"source_validate_ms"`
	ReencodeAndFinalizeMS int64                         `json:"reencode_and_finalize_ms"`
	CandidateValidateMS   int64                         `json:"candidate_validate_ms"`
	VirtualCompareMS      int64                         `json:"virtual_compare_ms"`
	VirtualSHA256         string                        `json:"virtual_sha256"`
}

func describeReferenceContainer(ctx context.Context, path string, r *historyReferenceReader) (referenceCodecContainerReport, error) {
	h := r.header
	report := referenceCodecContainerReport{
		Path:             path,
		PhysicalBytes:    h.physical,
		StoredPayload:    h.chunkDir - historyReferenceHeaderSize,
		ChunkDirectory:   h.chunks * historyReferenceChunkEntrySize,
		SpanDirectory:    h.spans * historyReferenceSpanEntrySize,
		LogicalBytes:     h.logical,
		Chunks:           h.chunks,
		Spans:            h.spans,
		CodecChunks:      make(map[uint32]int),
		CodecStoredBytes: make(map[uint32]uint64),
	}
	for id := uint32(0); uint64(id) < h.chunks; id++ {
		chunk, err := r.chunk(ctx, id)
		if err != nil {
			return referenceCodecContainerReport{}, err
		}
		report.CodecChunks[chunk.codec]++
		report.CodecStoredBytes[chunk.codec] += uint64(chunk.stored)
	}
	return report, nil
}

func hashReferenceVirtual(ctx context.Context, r *historyReferenceReader) ([sha256.Size]byte, error) {
	if err := contextError(ctx); err != nil {
		return [sha256.Size]byte{}, err
	}
	hash := sha256.New()
	buf := make([]byte, 256<<10)
	for off := uint64(0); off < r.UncompressedSize(); {
		if err := contextError(ctx); err != nil {
			return [sha256.Size]byte{}, err
		}
		next := min(uint64(len(buf)), r.UncompressedSize()-off)
		n, err := r.ReadAt(buf[:next], int64(off))
		if err != nil && err != io.EOF {
			return [sha256.Size]byte{}, err
		}
		if n != int(next) {
			return [sha256.Size]byte{}, io.ErrUnexpectedEOF
		}
		_, _ = hash.Write(buf[:n])
		off += uint64(n)
	}
	var out [sha256.Size]byte
	copy(out[:], hash.Sum(nil))
	return out, nil
}

func reencodeReferenceContainer(ctx context.Context, source, candidate string) (report referenceCodecReencodeReport, resultErr error) {
	report.Scope = "R1 container-only re-encode; excludes trio companions, source build, compaction, manifest publication, and online throughput"
	sourceReader, err := openHistoryReferenceReader(ctx, source, 0)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, sourceReader.Close()) }()
	if sourceReader.header.physical > referenceCodecMaxPhysical || sourceReader.header.logical > referenceCodecMaxLogical {
		return report, fmt.Errorf("%w: diagnostic source physical=%d/%d logical=%d/%d", errHistoryReferenceBudget,
			sourceReader.header.physical, referenceCodecMaxPhysical, sourceReader.header.logical, referenceCodecMaxLogical)
	}

	started := time.Now()
	if err = sourceReader.ValidateAll(ctx); err != nil {
		return report, err
	}
	report.SourceValidateMS = time.Since(started).Milliseconds()
	if report.Source, err = describeReferenceContainer(ctx, source, sourceReader); err != nil {
		return report, err
	}

	writer, err := newHistoryReferenceWriter(ctx, filepath.Dir(candidate), 0)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, writer.Release()) }()
	started = time.Now()
	chunkIDs := make([]uint32, sourceReader.header.chunks)
	for oldID := uint32(0); uint64(oldID) < sourceReader.header.chunks; oldID++ {
		raw, loadErr := sourceReader.loadChunk(ctx, oldID)
		if loadErr != nil {
			return report, loadErr
		}
		chunkIDs[oldID], err = writer.StoreChunk(raw)
		if err != nil {
			return report, err
		}
	}
	for index := uint64(0); index < sourceReader.header.spans; index++ {
		span, spanErr := sourceReader.span(ctx, index)
		if spanErr != nil {
			return report, spanErr
		}
		if err = writer.WriteSpan(chunkIDs[span.chunk], span.offset, span.length); err != nil {
			return report, err
		}
	}
	if _, _, err = writer.Finalize(candidate); err != nil {
		return report, err
	}
	report.ReencodeAndFinalizeMS = time.Since(started).Milliseconds()

	candidateReader, err := openHistoryReferenceReader(ctx, candidate, 0)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, candidateReader.Close()) }()
	started = time.Now()
	if err = candidateReader.ValidateAll(ctx); err != nil {
		return report, err
	}
	report.CandidateValidateMS = time.Since(started).Milliseconds()
	if report.Candidate, err = describeReferenceContainer(ctx, candidate, candidateReader); err != nil {
		return report, err
	}
	if report.Source.LogicalBytes != report.Candidate.LogicalBytes {
		return report, fmt.Errorf("logical length differs: source=%d candidate=%d", report.Source.LogicalBytes, report.Candidate.LogicalBytes)
	}
	started = time.Now()
	sourceHash, err := hashReferenceVirtual(ctx, sourceReader)
	if err != nil {
		return report, err
	}
	candidateHash, err := hashReferenceVirtual(ctx, candidateReader)
	if err != nil {
		return report, err
	}
	report.VirtualCompareMS = time.Since(started).Milliseconds()
	if sourceHash != candidateHash {
		return report, fmt.Errorf("virtual SHA differs: source=%x candidate=%x", sourceHash, candidateHash)
	}
	report.VirtualSHA256 = fmt.Sprintf("sha256:%x", sourceHash)
	return report, nil
}

func TestHistoryReferenceContainerReencodeDiagnosticFixture(t *testing.T) {
	_, want, sourceReader := referenceContainerFixture(t, 0)
	sourceFile := sourceReader.file.(*os.File)
	candidate := filepath.Join(t.TempDir(), "candidate.r1")
	report, err := reencodeReferenceContainer(context.Background(), sourceFile.Name(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if report.Source.LogicalBytes != uint64(len(want)) || report.Candidate.LogicalBytes != uint64(len(want)) ||
		report.Source.Chunks == 0 || report.Source.Spans == 0 || report.Candidate.PhysicalBytes == 0 || report.VirtualSHA256 == "" {
		t.Fatalf("incomplete report: %+v", report)
	}
	if _, err = os.Stat(candidate); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = hashReferenceVirtual(canceled, sourceReader); !errors.Is(err, context.Canceled) {
		t.Fatalf("virtual hash ignored cancellation: %v", err)
	}

	// Admission happens from the authenticated-size header before ValidateAll:
	// even a subsequently unreadable payload must report the diagnostic cap.
	data, err := os.ReadFile(sourceFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	header, err := decodeHistoryReferenceHeader(data[:historyReferenceHeaderSize], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	header.logical = referenceCodecMaxLogical + 1
	encoded := header.encode()
	copy(data, encoded[:])
	data[historyReferenceHeaderSize] ^= 1
	overLimit := filepath.Join(t.TempDir(), "over-limit.r1")
	if err = os.WriteFile(overLimit, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = reencodeReferenceContainer(context.Background(), overLimit, filepath.Join(t.TempDir(), "unused.r1")); !errors.Is(err, errHistoryReferenceBudget) {
		t.Fatalf("over-limit source reached validation: %v", err)
	}
}

func TestHistoryReferenceContainerReencodeDiagnostic(t *testing.T) {
	source := os.Getenv(referenceCodecSampleEnv)
	if source == "" {
		t.Skipf("set %s to an immutable private copy of an R1 .seg", referenceCodecSampleEnv)
	}
	source, err := filepath.Abs(source)
	if err != nil {
		t.Fatal(err)
	}
	outputRoot := os.Getenv(referenceCodecOutputDirEnv)
	if outputRoot == "" {
		outputRoot = t.TempDir()
	} else {
		outputRoot, err = filepath.Abs(outputRoot)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Clean(outputRoot) == filepath.Dir(source) {
			t.Fatalf("%s must be independent of the source directory", referenceCodecOutputDirEnv)
		}
		if err = os.MkdirAll(outputRoot, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	runDir, err := os.MkdirTemp(outputRoot, "gtron-reference-codec-")
	if err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(runDir, "candidate.r1")
	ctx, cancel := context.WithTimeout(context.Background(), referenceCodecTimeout)
	defer cancel()
	report, err := reencodeReferenceContainer(ctx, source, candidate)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(encoded))
}
