package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func indexPageFixture(t testing.TB, count uint64) (*os.File, string, stateDomainChangeBinaryHeader) {
	t.Helper()
	f, name, err := createStateDomainChangeBinaryTempFileInDir(t.TempDir(), "index")
	if err != nil {
		t.Fatal(err)
	}
	if err = writeStateDomainChangeBinaryHeaderTo(f, stateDomainChangeBinaryIndexMagic, 1, count, count); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < count; i++ {
		if err = writeStateDomainChangeBinaryIndexEntryTo(f, stateDomainChangeBinaryTxOffset{txNum: i + 1, offset: 4096 + i*113, recordIndex: i * 7, count: 7}); err != nil {
			t.Fatal(err)
		}
	}
	f, name, err = rewriteStateDomainChangeBinaryIndexV7UnpagedContext(context.Background(), f, name)
	if err != nil {
		t.Fatal(err)
	}
	h, err := readStateDomainChangeBinaryHeaderAt(f, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		t.Fatal(err)
	}
	return f, name, h
}

func pageIndexCopy(t testing.TB, source string, h stateDomainChangeBinaryHeader, pageBytes uint64, codec ...bool) (*os.File, string) {
	t.Helper()
	f, name, err := createStateDomainChangeBinaryTempFileInDir(t.TempDir(), "page-source")
	if err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(f, in)
	in.Close()
	if err != nil {
		t.Fatal(err)
	}
	useSnappy := true
	if len(codec) != 0 {
		useSnappy = codec[0]
	}
	f, name, err = rewriteHistoryIndexPagesWithCodec(context.Background(), f, name, h, pageBytes, useSnappy)
	if err != nil {
		t.Fatal(err)
	}
	return f, name
}

func TestHistoryIndexPagesVirtualBytesQueriesAndCorruption(t *testing.T) {
	f, name, h := indexPageFixture(t, 9000)
	defer f.Close()
	want, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, pageBytes := range []uint64{4096, 16384, 65536} {
		for _, useSnappy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/snappy=%v", pageBytes, useSnappy), func(t *testing.T) {
				out, path := pageIndexCopy(t, name, h, pageBytes, useSnappy)
				defer out.Close()
				st, _ := out.Stat()
				outer, err := readStateDomainChangeBinaryHeaderAt(out, stateDomainChangeBinaryIndexMagic)
				if err != nil || outer.version != 8 {
					t.Fatal(outer, err)
				}
				pages, err := openHistoryIndexPages(out, uint64(st.Size()), outer)
				if err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(want)+1)
				n, err := pages.ReadAt(got, 0)
				if err != io.EOF || n != len(want) || !bytes.Equal(got[:n], want) {
					t.Fatal("virtual stream differs", n, err)
				}
				if uint64(len(pages.cache))*pageBytes > 64<<10 {
					t.Fatal("cache exceeded budget")
				}
				reader, err := openStateDomainChangeBinaryIndexV7Reader(out, uint64(st.Size()), outer)
				if err != nil {
					t.Fatal(err)
				}
				for i := uint64(0); i < h.count; i++ {
					e, err := reader.entryAt(i)
					if err != nil || e.txNum != i+1 || e.offset != 4096+i*113 || e.recordIndex != i*7 {
						t.Fatal(i, e, err)
					}
				}
				reader.Close()
				// Corrupt an actual physical page, then open a fresh cache.
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)-1] ^= 1
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				bad, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer bad.Close()
				pr, err := openHistoryIndexPages(bad, uint64(len(data)), outer)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = pr.ReadAt(make([]byte, 1), int64(len(want)-1)); err == nil {
					t.Fatal("corrupt compressed page accepted")
				}
				pr.Close()
			})
		}
	}
}

func BenchmarkHistoryIndexPagedQueries(b *testing.B) {
	f, name, h := indexPageFixture(b, 100000)
	defer f.Close()
	for _, size := range []uint64{0, 4096, 16384} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var file *os.File
			var err error
			path := name
			if size != 0 {
				file, path = pageIndexCopy(b, name, h, size)
			} else {
				file, err = os.Open(name)
				if err != nil {
					b.Fatal(err)
				}
			}
			defer file.Close()
			_ = path
			st, _ := file.Stat()
			outer, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinaryIndexMagic)
			if err != nil {
				b.Fatal(err)
			}
			r, err := openStateDomainChangeBinaryIndexV7Reader(file, uint64(st.Size()), outer)
			if err != nil {
				b.Fatal(err)
			}
			defer r.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = r.entryAt(uint64(i*7919) % h.count); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(st.Size()), "file-bytes")
		})
	}
}

// This opt-in test writes only a new output directory and never publishes the
// pinned manifest. It compares complete original/new files and all entries.
func TestHistoryIndexPagesPinnedCorpus(t *testing.T) {
	dir, outDir := os.Getenv("GTRON_HISTORY_INDEX_CORPUS"), os.Getenv("GTRON_INDEX_MEASURE_OUT")
	if dir == "" || outDir == "" {
		t.Skip("explicit immutable corpus and new output directory required")
	}
	if err := os.Mkdir(outDir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	type result struct {
		Path                     string
		Codec                    string
		PageBytes                uint64
		Before, After, Rows      uint64
		BaselineQueryNS, QueryNS int64
	}
	var results []result
	for _, ref := range m.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentInverted {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		source, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		st, _ := source.Stat()
		if uint64(st.Size()) != ref.Size {
			t.Fatal("source size")
		}
		if sum, err := checksumFile(path); err != nil || sum != ref.Checksum {
			t.Fatal("source checksum", err)
		}
		h, err := readStateDomainChangeBinaryHeaderAt(source, stateDomainChangeBinaryIndexMagic)
		if err != nil {
			t.Fatal(err)
		}
		base, err := openStateDomainChangeBinaryIndexV7Reader(source, uint64(st.Size()), h)
		if err != nil {
			t.Fatal(err)
		}
		for _, pageBytes := range []uint64{4096, 16384} {
			input := filepath.Join(outDir, fmt.Sprintf("%d-%s", pageBytes, filepath.Base(path)))
			copy, err := os.OpenFile(input, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = io.Copy(copy, io.NewSectionReader(source, 0, st.Size())); err != nil {
				t.Fatal(err)
			}
			newFile, newName, err := rewriteHistoryIndexPagesWithSize(context.Background(), copy, input, h, pageBytes)
			if err != nil {
				t.Fatal(err)
			}
			newStat, _ := newFile.Stat()
			nh, err := readStateDomainChangeBinaryHeaderAt(newFile, stateDomainChangeBinaryIndexMagic)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := openStateDomainChangeBinaryIndexV7Reader(newFile, uint64(newStat.Size()), nh)
			if err != nil {
				t.Fatal(err)
			}
			for i := uint64(0); i < h.count; i++ {
				a, e := base.entryAt(i)
				b, f := candidate.entryAt(i)
				if e != nil || f != nil || a != b {
					t.Fatal("entry differs", ref.Path, i, e, f)
				}
			}
			queries := min(uint64(4096), h.count)
			var baselineNS, candidateNS int64
			for repeat := 0; repeat < 3; repeat++ {
				for q := uint64(0); q < queries; q++ {
					i := (q*7919 + uint64(repeat)*17) % h.count
					start := time.Now()
					a, e := base.entryAt(i)
					baselineNS += time.Since(start).Nanoseconds()
					start = time.Now()
					b, f := candidate.entryAt(i)
					candidateNS += time.Since(start).Nanoseconds()
					if e != nil || f != nil || a != b {
						t.Fatal("query differs", e, f)
					}
				}
			}
			results = append(results, result{ref.Path, "snappy", pageBytes, ref.Size, uint64(newStat.Size()), h.count, baselineNS / int64(queries*3), candidateNS / int64(queries*3)})
			candidate.Close()
			if err = os.Rename(newName, input); err != nil {
				t.Fatal(err)
			}
		}
		base.Close()
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(outDir, "results.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}
