package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func referenceTranscodeFixture(t *testing.T, format string) (string, []SegmentRef, map[string][]byte) {
	t.Helper()
	dir := t.TempDir()
	prev := make([]byte, 512<<10)
	rand.New(rand.NewSource(704)).Read(prev)
	var changes []*rawdb.StateDomainChange
	for i := 0; i < 8; i++ {
		changes = append(changes, &rawdb.StateDomainChange{BlockNum: uint64(i + 1), TxNum: uint64(i + 1), Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest,
			Domain: kvdomains.SystemDelegation, Key: []byte("delegation history key"), PrevExists: true, Prev: bytes.Clone(prev)})
	}
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 8, changes)
	for i := range refs {
		if refs[i].Kind != SegmentHistory || format == "raw" {
			continue
		}
		path := filepath.Join(dir, refs[i].Path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		tmp := path + ".encoding"
		if format == "1" {
			writer, e := newCompressedBlockWriter(dir, 1)
			if e != nil {
				t.Fatal(e)
			}
			for off := 0; off < len(data); off += historyCompressChunkSize {
				if _, err = writer.Append(data[off:min(len(data), off+historyCompressChunkSize)]); err != nil {
					t.Fatal(err)
				}
			}
			err = writer.Finish(tmp)
		} else {
			stream, e := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, format)
			if e != nil {
				t.Fatal(e)
			}
			defer stream.Abort()
			if _, err = stream.Write(data); err == nil {
				_, err = stream.FinishWithMetadataContext(context.Background(), tmp)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		refs[i].Size, refs[i].Checksum, err = stateDomainChangeBinaryFileMetadata(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	before := make(map[string][]byte)
	for _, ref := range refs {
		data, err := os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		before[ref.Path] = data
	}
	return dir, refs, before
}

func TestHistoryReferenceTranscodeAllFormatsFullTrio(t *testing.T) {
	for _, format := range []string{"raw", "1", "2", "3"} {
		t.Run(format, func(t *testing.T) {
			dir, source, before := referenceTranscodeFixture(t, format)
			estimate, err := HistoryReferenceTranscodeWorkBytesContext(context.Background(), dir, source)
			if err != nil {
				t.Fatal(err)
			}
			result, stats, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, source)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 3 || stats.PhysicalBytes == 0 || stats.LogicalBytes == 0 {
				t.Fatal("missing result", stats)
			}
			if estimate < 2*stats.UniqueChunkBytes+2*stats.PhysicalBytes {
				t.Fatal("space estimate below actual bounded data", estimate, stats)
			}
			var sourceHistory, resultHistory SegmentRef
			for i, old := range source {
				if old.Kind != result[i].Kind || old.Path == result[i].Path {
					t.Fatal("identity mapping")
				}
				data, err := os.ReadFile(filepath.Join(dir, old.Path))
				if err != nil || !bytes.Equal(data, before[old.Path]) {
					t.Fatal("source mutated", err)
				}
				if old.Kind != SegmentHistory {
					data, err = os.ReadFile(filepath.Join(dir, result[i].Path))
					if err != nil || !bytes.Equal(data, before[old.Path]) || result[i].Checksum != old.Checksum || result[i].Size != old.Size {
						t.Fatal("companion bytes changed", err)
					}
				} else {
					sourceHistory, resultHistory = old, result[i]
				}
			}
			src, srcSize, _, err := openHistorySegmentForRead(dir, sourceHistory)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			dst, dstSize, _, err := openHistorySegmentForRead(dir, resultHistory)
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			if srcSize != dstSize || stats.LogicalBytes != srcSize {
				t.Fatal("virtual size differs")
			}
			if err := historyReferenceCompareVirtual(context.Background(), src, dst, srcSize); err != nil {
				t.Fatal(err)
			}
			if format == "3" {
				cr, err := openCompressedBlockReaderWithCacheLimit(filepath.Join(dir, sourceHistory.Path), 1)
				if err != nil {
					t.Fatal(err)
				}
				defer cr.Close()
				if cr.cdc == nil {
					t.Fatal("fixture not CDC")
				}
				anchors := uint64(0)
				references := uint64(0)
				for i := uint64(0); i < cr.cdc.count; i++ {
					entry, _, err := cr.cdc.entry(i)
					if err != nil {
						t.Fatal(err)
					}
					if entry.anchor == cdcAnchor {
						anchors++
					} else {
						references++
					}
				}
				if references == 0 || stats.StoredChunks > anchors || stats.StoredChunks >= cr.cdc.count {
					t.Fatalf("anchor reuse lost: %+v anchors%d refs%d", stats, anchors, references)
				}
			}
			// The repeated run reuses the same fully matching content-addressed files.
			again, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, source)
			if err != nil {
				t.Fatal(err)
			}
			for i := range result {
				if result[i] != again[i] {
					t.Fatal("resume output changed")
				}
			}
			if _, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, result); !errors.Is(err, ErrHistoryReferenceAlreadyEncoded) {
				t.Fatal("already reference", err)
			}
			t.Logf("format=%s source physical=%d R1=%d virtual=%d", format, sourceHistory.Size, stats.PhysicalBytes, stats.LogicalBytes)
		})
	}
}

func TestHistoryReferenceTranscodeSourceAndOutputFailures(t *testing.T) {
	for _, fault := range []string{"missing checksum", "wrong kind", "corrupt source", "canceled", "destination conflict"} {
		t.Run(fault, func(t *testing.T) {
			dir, source, before := referenceTranscodeFixture(t, "3")
			ctx := context.Background()
			switch fault {
			case "missing checksum":
				source[0].Checksum = ""
			case "wrong kind":
				source[1].Kind = source[0].Kind
			case "corrupt source":
				p := filepath.Join(dir, source[0].Path)
				data := bytes.Clone(before[source[0].Path])
				data[len(data)/2] ^= 1
				if err := os.WriteFile(p, data, 0o600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "destination conflict":
				result, _, err := ReencodeHistoryReferenceTrioContext(ctx, dir, source)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, result[0].Path), []byte("different preexisting data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if result, _, err := ReencodeHistoryReferenceTrioContext(ctx, dir, source); err == nil || len(result) != 0 {
				t.Fatal("invalid transcode returned refs", err)
			}
			if fault != "corrupt source" {
				for path, old := range before {
					actual, err := os.ReadFile(filepath.Join(dir, path))
					if err != nil || !bytes.Equal(old, actual) {
						t.Fatal("failure mutated source", path, err)
					}
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if len(entry.Name()) >= len(".history-reference-") && entry.Name()[:len(".history-reference-")] == ".history-reference-" {
					t.Fatal("scratch leaked")
				}
			}
		})
	}
}

func TestHistoryReferenceTranscodeWorkEstimateBounds(t *testing.T) {
	dir, refs, original := referenceTranscodeFixture(t, "3")
	baseline, err := HistoryReferenceTranscodeWorkBytesContext(context.Background(), dir, refs)
	if err != nil {
		t.Fatal(err)
	}
	var accessor SegmentRef
	for _, ref := range refs {
		if ref.Kind == SegmentAccessor {
			accessor = ref
		}
	}
	// The estimate does not claim content verification. Mutating only the header
	// exercises the conservative V7 ETL term without allocating a giant fixture.
	data := bytes.Clone(original[accessor.Path])
	oldVersion := binary.BigEndian.Uint32(data[8:12])
	count := binary.BigEndian.Uint64(data[28:36])
	for _, version := range []uint32{stateDomainChangeBinaryVersionV6, stateDomainChangeBinaryVersionV7} {
		binary.BigEndian.PutUint32(data[8:12], version)
		if err := os.WriteFile(filepath.Join(dir, accessor.Path), data, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := HistoryReferenceTranscodeWorkBytesContext(context.Background(), dir, refs)
		if err != nil {
			t.Fatal(err)
		}
		want := baseline
		if oldVersion == stateDomainChangeBinaryVersionV7 {
			want -= count * 128
		}
		if version == stateDomainChangeBinaryVersionV7 {
			want += count * 128
		}
		if got != want {
			t.Fatal("incorrect verifier ETL term", got, want)
		}
	}
	for _, fault := range []string{"duplicate", "range", "path", "checksum", "accessor magic", "accessor version", "accessor count", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			candidate := append([]SegmentRef(nil), refs...)
			data := bytes.Clone(original[accessor.Path])
			ctx := context.Background()
			switch fault {
			case "duplicate":
				candidate[1] = candidate[2]
			case "range":
				candidate[1].ToTxNum++
			case "path":
				candidate[1].Path += "-unrelated"
			case "checksum":
				candidate[1].Checksum = ""
			case "accessor magic":
				data[0] ^= 1
			case "accessor version":
				binary.BigEndian.PutUint32(data[8:12], stateDomainChangeBinaryVersionV5)
			case "accessor count":
				binary.BigEndian.PutUint64(data[28:36], count+1)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := os.WriteFile(filepath.Join(dir, accessor.Path), data, 0o600); err != nil {
				t.Fatal(err)
			}
			if n, err := HistoryReferenceTranscodeWorkBytesContext(ctx, dir, candidate); err == nil || n != 0 {
				t.Fatal("invalid input returned space estimate", n, err)
			}
		})
	}
	// A forged old page table is rejected before the old reader allocates it.
	var huge [compressedBlockHeaderSize]byte
	copy(huge[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(huge[8:12], compressedBlockVersion)
	binary.BigEndian.PutUint64(huge[24:32], historyReferenceMaxMetadata/64+1)
	binary.BigEndian.PutUint64(huge[32:40], 1)
	path := filepath.Join(t.TempDir(), "oversized.seg")
	if err := os.WriteFile(path, huge[:], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := historyReferenceTranscodePreflight(context.Background(), path, uint64(len(huge))); !errors.Is(err, errHistoryReferenceBudget) {
		t.Fatal("oversized table did not fail before allocation", err)
	}
}
