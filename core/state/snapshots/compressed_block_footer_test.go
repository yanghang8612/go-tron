package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCompressedFooterStreamIdentity(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	const chunk = 16 << 10
	for _, size := range []int{0, 1, chunk - 1, chunk, chunk + 1, 8<<20 + 317} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			payload := make([]byte, size)
			_, _ = rand.New(rand.NewSource(int64(size))).Read(payload)
			w, err := newCompressedBlockStreamWriter(dir, chunk, 4)
			if err != nil {
				t.Fatal(err)
			}
			inode, err := w.body.tmp.Stat()
			if err != nil {
				t.Fatal(err)
			}
			for off := 0; off < size; {
				end := min(off+17001, size)
				if _, err := w.Write(payload[off:end]); err != nil {
					t.Fatal(err)
				}
				off = end
			}
			if size >= 36 {
				copy(payload[28:36], "patched!")
				if _, err := w.WriteAt(payload[28:36], 28); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(dir, "footer.cb")
			meta, err := w.FinishWithMetadata(path)
			if err != nil {
				t.Fatal(err)
			}
			final, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(inode, final) {
				t.Fatal("final file copied to a different inode")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if meta.checksum != sha256.Sum256(data) || meta.size != uint64(len(data)) {
				t.Fatal("streamed whole-file checksum/size mismatch")
			}
			decoded, err := decompressBlockBlob(data)
			if err != nil || !bytes.Equal(decoded, payload) {
				t.Fatalf("blob identity: %v", err)
			}
			r, err := openCompressedBlockReader(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			for off := 0; off < size; {
				n := min(33001, size-off)
				got := make([]byte, n)
				nr, err := r.ReadAt(got, int64(off))
				if err != nil || nr != n || !bytes.Equal(got, payload[off:off+n]) {
					t.Fatalf("ReadAt %d: n=%d err=%v", off, nr, err)
				}
				off += n
			}
			if size > chunk && r.table[0].compressedStart == 0 {
				t.Fatal("retained prefix was not rotated behind body")
			}
			files, _ := filepath.Glob(filepath.Join(dir, ".cbw-*"))
			if len(files) != 0 {
				t.Fatal("scratch remains", files)
			}
		})
	}
}

func TestCompressedFooterCompatibilityPaths(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"reset", TestCompressedBlockStreamWriterReset}, {"metadata", TestCompressedBlockStreamWriterMetadata},
		{"abort", TestCompressedHistoryTempAbortRemovesScratch}, {"range-and-key", TestCompressedHistorySegmentReaderEquivalence},
		{"compaction", TestCompactionMergesCompressedSources}, {"corruption", TestCompressedHistorySegmentSelfCheckCatchesCorruption},
		{"full-check", TestCompressedHistorySegmentFullReadAndCheck}, {"production-readers", TestCompressedHistorySegmentProductionReadPaths},
		{"space-inspection", TestInspectHistorySpaceProfilesCompressedV6Trio},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestCompressedFooterResetKeepsFormat(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	dir := t.TempDir()
	w, err := newCompressedBlockStreamWriter(dir, 64, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("discard"), 200000)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "1")
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("kept")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "reset.cb")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(data[8:12]) != 2 {
		t.Fatal("Reset changed the format of an existing stream")
	}
	got, err := decompressBlockBlob(data)
	if err != nil || string(got) != "kept" {
		t.Fatalf("reset bytes %q %v", got, err)
	}
}

func TestCompressedFooterRejectsCorruption(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	dir := t.TempDir()
	w, err := newCompressedBlockStreamWriter(dir, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("rows-"), 100)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ok.cb")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	table := int(binary.BigEndian.Uint64(original[len(original)-16 : len(original)-8]))
	trailer := len(original) - 48
	cases := map[string]func([]byte) []byte{
		"truncate": func(b []byte) []byte { return b[:len(b)-1] }, "append": func(b []byte) []byte { return append(b, 0) },
		"reserved": func(b []byte) []byte { b[20] = 1; return b }, "magic": func(b []byte) []byte { b[trailer] = 'x'; return b },
		"version": func(b []byte) []byte { b[11] = 3; return b }, "count": func(b []byte) []byte { binary.BigEndian.PutUint64(b[trailer+16:], ^uint64(0)); return b },
		"table-overflow": func(b []byte) []byte { binary.BigEndian.PutUint64(b[trailer+32:], ^uint64(0)); return b },
		"table-length":   func(b []byte) []byte { b[len(b)-1]++; return b },
		"records":        func(b []byte) []byte { b[trailer+15]++; return b }, "logical-origin": func(b []byte) []byte { b[table+7] = 1; return b },
		"logical-order":  func(b []byte) []byte { clear(b[table+28 : table+36]); return b },
		"prefix-overlap": func(b []byte) []byte { clear(b[table+8 : table+16]); return b },
		"body-gap":       func(b []byte) []byte { b[table+28+15] = 1; return b },
		"range-overflow": func(b []byte) []byte { binary.BigEndian.PutUint64(b[table+16:], ^uint64(0)); return b },
		"zero-records":   func(b []byte) []byte { clear(b[table+24 : table+28]); return b },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			data := mutate(bytes.Clone(original))
			bad := filepath.Join(dir, name+".cb")
			if err := os.WriteFile(bad, data, 0600); err != nil {
				t.Fatal(err)
			}
			if r, err := openCompressedBlockReader(bad); err == nil {
				_ = r.Close()
				t.Fatal("reader accepted corrupt layout")
			}
			if _, err := decompressBlockBlob(data); err == nil {
				t.Fatal("blob decoder accepted corrupt layout")
			}
		})
	}
}

func TestCompressedFooterFailureDoesNotPublish(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	for _, failure := range []string{"cancel-before", "cancel-after-flush", "closed-file", "rename"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newCompressedBlockStreamWriter(dir, 16384, 4)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(bytes.Repeat([]byte("input"), 400000)); err != nil {
				t.Fatal(err)
			}
			scratch := w.body.tmpName
			path := filepath.Join(dir, "target")
			if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finishCtx := ctx
			switch failure {
			case "cancel-before":
				cancel()
			case "cancel-after-flush":
				finishCtx = &historyCancelCheckContext{Context: ctx, cancel: cancel, check: func() bool {
					f, err := os.Open(scratch)
					if err != nil {
						return false
					}
					defer func() { _ = f.Close() }()
					st, err := f.Stat()
					if err != nil || st.Size() < 48 {
						return false
					}
					var tail [48]byte
					_, err = f.ReadAt(tail[:], st.Size()-48)
					return err == nil && string(tail[:8]) == compressedBlockFooterMagic
				}}
			case "closed-file":
				_ = w.body.tmp.Close()
			case "rename":
				path = filepath.Join(dir, "missing", "target")
			}
			meta, err := w.FinishWithMetadataContext(finishCtx, path)
			if err == nil {
				t.Fatal("injected failure was lost")
			}
			if meta != (snapshotFileMetadata{}) {
				t.Fatal("failed finish returned metadata")
			}
			if failure == "cancel-before" || failure == "cancel-after-flush" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation: %v", err)
				}
			}
			old, err := os.ReadFile(filepath.Join(dir, "target"))
			if err != nil || string(old) != "old" {
				t.Fatal("failed finish changed previous target")
			}
			w.Abort()
			files, _ := filepath.Glob(filepath.Join(dir, ".cbw-*"))
			if len(files) != 0 {
				t.Fatal("failed finish leaked scratch", files)
			}
		})
	}
}

func TestCompressedFooterMixedCompaction(t *testing.T) {
	dir := t.TempDir()
	var refs []SegmentRef
	for i, format := range []string{"1", "2", "1"} {
		t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
		tx := uint64(i + 1)
		var changes []*rawdb.StateDomainChange
		for seq := uint64(1); seq <= 256; seq++ {
			changes = append(changes, binaryStateDomainChange(tx, tx, seq, fmt.Sprintf("key-%04d-%s", seq, bytes.Repeat([]byte("k"), 768))))
		}
		seg, idx, acc, err := writeHistorySegmentFiles(dir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: tx, ToTxNum: tx, Path: stateDomainChangeHistorySegmentPath(tx, tx)}, changes)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, seg.Path))
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.BigEndian.Uint32(data[8:12]); fmt.Sprint(got) != format {
			t.Fatalf("source format %d != %s", got, format)
		}
		refs = append(refs, seg, acc, idx)
	}
	setCompactionRefAggregationSteps(refs[:3], 2)
	if err := PublishManifest(dir, NewManifest(1, 3, refs)); err != nil {
		t.Fatal(err)
	}
	// Closing the new-write gate must still permit old/new inputs to merge.
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "1")
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true})
	if err != nil || !result.Merged {
		t.Fatalf("mixed merge: %v %+v", err, result)
	}
	seg := compactionRefByKind(t, result, SegmentHistory)
	got, err := readStateDomainChangeBinarySegment(dir, seg)
	if err != nil || len(got) != 768 {
		t.Fatalf("mixed records %d %v", len(got), err)
	}
}

func BenchmarkCompressedFooterStream(b *testing.B) {
	for _, size := range []int{512 << 10, 8 << 20, 64 << 20} {
		for _, format := range []string{"1", "2"} {
			b.Run(fmt.Sprintf("size=%dMiB/format=%s", size>>20, format), func(b *testing.B) {
				b.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
				dir := b.TempDir()
				payload := make([]byte, size)
				_, _ = rand.New(rand.NewSource(914)).Read(payload)
				for off := 0; off+256 < size; off += 512 {
					copy(payload[off:off+256], bytes.Repeat([]byte{byte(off >> 10)}, 256))
				}
				path := filepath.Join(dir, "out.cb")
				var finish, totalBytes, avoided int64
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					w, err := newCompressedBlockStreamWriter(dir, historyCompressChunkSize, 4)
					if err != nil {
						b.Fatal(err)
					}
					body := w.body
					if _, err := io.Copy(w, bytes.NewReader(payload)); err != nil {
						b.Fatal(err)
					}
					started := time.Now()
					meta, err := w.FinishWithMetadata(path)
					finish += time.Since(started).Nanoseconds()
					if err != nil {
						b.Fatal(err)
					}
					totalBytes += int64(meta.size)
					if format == "2" {
						avoided += int64(body.compTotal)
					}
				}
				b.ReportMetric(float64(finish)/float64(b.N), "finish-ns/op")
				b.ReportMetric(float64(totalBytes)/float64(b.N), "output-B/op")
				b.ReportMetric(float64(avoided)/float64(b.N), "copy-avoided-B/op")
			})
		}
	}
}
