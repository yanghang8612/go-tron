package freezer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func v2PipelineRecords(count, size int) [][]byte {
	rng := rand.New(rand.NewSource(8128))
	corpus := make([]byte, 256<<10)
	_, _ = rng.Read(corpus)
	rows := make([][]byte, count)
	for i := range rows {
		row := make([]byte, size)
		for offset := 0; offset < len(row); offset += 256 {
			base := rng.Intn(len(corpus) - 256)
			copy(row[offset:], corpus[base:base+256])
		}
		binary.BigEndian.PutUint64(row[:8], uint64(i))
		rows[i] = row
	}
	return rows
}

func TestV2ParallelFramesMatchSerialLayoutAndAllRecords(t *testing.T) {
	rows := v2PipelineRecords(512, 2048)
	for _, codec := range []uint32{v2CodecDefault, v2CodecBodiesRawDict, v2CodecBodiesTrainedDict} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			dir := t.TempDir()
			serial, parallel := filepath.Join(dir, "serial.seg"), filepath.Join(dir, "parallel.seg")
			// A source may reuse a borrowed buffer. The pipeline must finish its
			// sequential copy before asking that source for the next record.
			borrowed := make([]byte, len(rows[0]))
			read := func(n uint64) ([]byte, error) { copy(borrowed, rows[n]); return borrowed, nil }
			for _, pair := range []struct {
				path    string
				workers int
			}{{serial, 1}, {parallel, 4}} {
				if err := writeV2SegmentProfileWithWorkers(pair.path, 0, uint64(len(rows)), 8, codec, zstd.SpeedBetterCompression, read, read, pair.workers); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(serial)
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(parallel)
			if err != nil {
				t.Fatal(err)
			}
			// Independently trained dictionaries are validated by decoding. The
			// raw/default codecs additionally preserve exact byte equality; every
			// codec must preserve all records and the same frame boundaries.
			if codec != v2CodecBodiesTrainedDict && !bytes.Equal(got, want) {
				t.Fatal("parallel raw/default wire bytes differ")
			}
			for _, path := range []string{serial, parallel} {
				reader, err := openV2Segment(path, "bodies")
				if err != nil {
					t.Fatal(err)
				}
				if reader.codec != codec || reader.frameBlocks != 8 || len(reader.frames) != len(rows)/8 {
					t.Fatalf("changed frame layout: %+v", reader)
				}
				store := newTestV2Store(t, "bodies", reader)
				defer store.Close()
				for i, want := range rows {
					got, err := store.read("bodies", uint64(i))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("%s record %d: %v", path, i, err)
					}
				}
			}
		})
	}
}

func TestV2ParallelSourceFailureLeavesNoPublishedSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "failed.seg")
	wantErr := errors.New("source canceled after queued frames")
	read := func(n uint64) ([]byte, error) {
		if n == 64 {
			return nil, wantErr
		}
		return bytes.Repeat([]byte{byte(n)}, 8192), nil
	}
	err := writeV2SegmentProfileWithWorkers(path, 0, 512, 8, v2CodecDefault, zstd.SpeedDefault, read, nil, 4)
	if !errors.Is(err, wantErr) {
		t.Fatalf("source failure = %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed output was published: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed pipeline left temporary files: %v %v", entries, err)
	}
}

func TestV2ParallelEncoderInitializationFailureDrainsWorkers(t *testing.T) {
	primary, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	wantErr := errors.New("encoder initialization failed")
	next := uint64(0)
	err = writeV2FramePipeline(4, 100, primary, func() (*zstd.Encoder, error) { return nil, wantErr }, func(buffer []byte) (v2RawFrame, error) {
		next++
		return v2RawFrame{first: next, records: 1, data: append(buffer, byte(next))}, nil
	}, func(v2EncodedFrame) error { return nil })
	if !errors.Is(err, wantErr) {
		t.Fatalf("encoder failure was lost: %v", err)
	}
}

func TestV2ParallelOversizedFrameKeepsOrderAndRoundTrips(t *testing.T) {
	primary, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	readCount, written := uint64(0), uint64(0)
	err = writeV2FramePipeline(4, 5, primary, func() (*zstd.Encoder, error) { return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1)) }, func(buffer []byte) (v2RawFrame, error) {
		n := readCount
		readCount++
		data := append(buffer, byte(n))
		if n == 2 {
			data = bytes.Repeat([]byte{byte(n)}, int(v2CompressionInputBytes)+1)
		}
		return v2RawFrame{first: n, records: 1, data: data}, nil
	}, func(frame v2EncodedFrame) error {
		if frame.first != written {
			return fmt.Errorf("write order %d, want %d", frame.first, written)
		}
		written++
		raw, err := decoder.DecodeAll(frame.compressed, nil)
		if err != nil {
			return err
		}
		if uint64(len(raw)) != frame.rawBytes || bytes.Count(raw, []byte{byte(frame.first)}) != len(raw) {
			return errors.New("frame payload mismatch")
		}
		return nil
	})
	if err != nil || written != 5 {
		t.Fatalf("oversized ordered pipeline: wrote=%d err=%v", written, err)
	}
}

// Same immutable corpus, compression level, dictionary sampling, frame layout,
// fsync and rename for both paths. The serial branch is the original loop.
func BenchmarkV2FrameEncodingPipeline(b *testing.B) {
	rows := v2PipelineRecords(8192, 8192)
	read := func(n uint64) ([]byte, error) { return rows[n], nil }
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "bodies.seg")
			b.SetBytes(int64(len(rows) * len(rows[0])))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeV2SegmentProfileWithWorkers(path, 0, uint64(len(rows)), 64, v2CodecBodiesTrainedDict, zstd.SpeedBetterCompression, read, read, workers); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := verifyV2Segment(context.Background(), path, "bodies", 0, uint64(len(rows)), read); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(info.Size()), "output_B")
		})
	}
}

func BenchmarkV2DirectMigrationPipeline(b *testing.B) {
	rows := v2PipelineRecords(8192, 8192)
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(rows) * (len(rows[0]) + 1024 + 32)))
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, err := NewFreezer(b.TempDir(), "", false, 2049, map[string]TableConfig{
					"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true, NoSnappy: true},
				})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				result, err := store.MigrateV2(V2MigrationOptions{
					Tables: []string{"bodies", "tx_infos", "state_roots"}, SegmentBlocks: uint64(len(rows)), FrameBlocks: 64,
					MaxSegments: 1, Online: true, SourceHead: uint64(len(rows)), CompressionWorkers: workers,
					Source: func(kind string, n uint64) ([]byte, error) {
						switch kind {
						case "tx_infos":
							return rows[n][:1024], nil
						case "state_roots":
							return rows[n][:32], nil
						}
						return rows[n], nil
					},
				})
				b.StopTimer()
				if err != nil {
					store.Close()
					b.Fatal(err)
				}
				if result.End != uint64(len(rows)) {
					store.Close()
					b.Fatal("incomplete durable migration")
				}
				b.ReportMetric(float64(result.PhysicalBytesAfter), "output_B")
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
