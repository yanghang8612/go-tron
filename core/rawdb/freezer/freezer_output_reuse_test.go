package freezer

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestV2FrameOutputPoolCapacityAndOwnership(t *testing.T) {
	var pool v2FrameOutputPool
	if pool.take() != nil {
		t.Fatal("empty pool")
	}
	full := make([]byte, 1, int(v2CompressionOutputReuseBytes))
	pool.put(full)
	pool.put(make([]byte, 1))
	if pool.bytes != v2CompressionOutputReuseBytes || len(pool.buffers) != 1 {
		t.Fatal("idle capacity exceeded")
	}
	dst := pool.take()
	if len(dst) != 0 || cap(dst) != cap(full) || pool.bytes != 0 || len(pool.buffers) != 0 {
		t.Fatal("bad destination transfer")
	}
	dst = append(dst, 0x73)
	if full[0] != 0x73 {
		t.Fatal("destination was copied")
	}
	pool.put(nil)
	pool.put(make([]byte, 1, int(v2CompressionOutputReuseBytes)+1))
	if pool.bytes != 0 || len(pool.buffers) != 0 {
		t.Fatal("nil or oversized destination retained")
	}
	for range 3 {
		pool.put(make([]byte, 0, 12<<20))
	}
	if pool.bytes != 24<<20 || len(pool.buffers) != 2 {
		t.Fatal("aggregate retained capacity exceeded")
	}
}

func TestV2FrameDestinationReusePreservesFrames(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) {
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
			next, written := uint64(0), uint64(0)
			err = writeV2FramePipeline(workers, 96, primary, func() (*zstd.Encoder, error) { return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1)) }, func(buf []byte) (v2RawFrame, error) {
				n := next
				next++
				data := append(buf, bytes.Repeat([]byte{byte(n)}, int(4096+n*73))...)
				return v2RawFrame{first: n, records: 1, data: data}, nil
			}, func(frame v2EncodedFrame) error {
				if frame.first != written || frame.records != 1 || frame.checksum != crc32.Checksum(frame.compressed, v2CRC) {
					return errors.New("frame identity/checksum")
				}
				written++
				decoded, err := decoder.DecodeAll(frame.compressed, nil)
				if err != nil {
					return err
				}
				if uint64(len(decoded)) != frame.rawBytes || !bytes.Equal(decoded, bytes.Repeat([]byte{byte(frame.first)}, int(4096+frame.first*73))) {
					return errors.New("borrowed output overwritten before write returned")
				}
				return nil
			})
			if err != nil || written != 96 {
				t.Fatalf("frames %d: %v", written, err)
			}
		})
	}
}

func TestV2FrameLargeEncodedDestinationAndWriteFailure(t *testing.T) {
	// An incompressible result exceeds the reusable-output cap while its input
	// remains below the separate raw-input cap. It must still write/decode intact.
	large := make([]byte, int(v2CompressionOutputReuseBytes)+1)
	_, _ = rand.New(rand.NewSource(7254)).Read(large)
	for _, failure := range []string{"none", "read", "write", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			primary, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close()
			next, written := uint64(0), uint64(0)
			wantErr := errors.New(failure)
			if failure == "checkpoint" {
				wantErr = context.Canceled
			}
			sawLarge := false
			read := func(buf []byte) (v2RawFrame, error) {
				n := next
				next++
				if failure == "read" && n == 20 {
					return v2RawFrame{}, wantErr
				}
				if n == 1 {
					return v2RawFrame{first: n, records: 1, data: large}, nil
				}
				return v2RawFrame{first: n, records: 1, data: append(buf, byte(n))}, nil
			}
			write := func(frame v2EncodedFrame) error {
				if frame.first != written {
					return errors.New("write order changed")
				}
				written++
				if failure == "write" && frame.first == 20 {
					return wantErr
				}
				raw, err := decoder.DecodeAll(frame.compressed, nil)
				if err != nil {
					return err
				}
				if frame.first == 1 {
					sawLarge = uint64(len(frame.compressed)) > v2CompressionOutputReuseBytes
					if !bytes.Equal(raw, large) {
						return errors.New("large output changed")
					}
				} else if !bytes.Equal(raw, []byte{byte(frame.first)}) {
					return errors.New("small output changed")
				}
				return nil
			}
			checkpoint := func() error {
				if failure == "checkpoint" {
					return wantErr
				}
				return nil
			}
			err = writeV2FramePipelineWithCheckpoint(8, 40, primary, func() (*zstd.Encoder, error) {
				return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
			}, read, write, 24, checkpoint)
			if failure == "none" {
				if err != nil || written != 40 {
					t.Fatalf("complete: %d/%v", written, err)
				}
			} else if !errors.Is(err, wantErr) {
				t.Fatalf("lost %s error: %v", failure, err)
			}
			if !sawLarge {
				t.Fatal("fixture did not produce output larger than reusable cap")
			}
			// The pipeline's deferred join has completed: no worker owns a callback
			// buffer after return. Poisoning the owned source cannot race a worker.
			if failure != "none" {
				large[0] ^= 1
				large[0] ^= 1
			}
		})
	}
}

func TestV2FrameDestinationSizeMixHighWater(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var pool v2FrameOutputPool
	rng := rand.New(rand.NewSource(7321))
	first := make([]byte, 2<<20)
	_, _ = rng.Read(first)
	frame := encodeV2FrameInto(encoder, v2RawFrame{data: first}, pool.take())
	initialCap := cap(frame.compressed)
	pool.put(frame.compressed)
	job := v2FrameEncodeJob{dst: pool.take()}
	small := encodeV2FrameInto(encoder, v2RawFrame{data: []byte{0x73}}, job.dst)
	if cap(small.compressed) != initialCap || pool.bytes != 0 {
		t.Fatal("small frame did not retain leased large destination")
	}
	raw, err := decoder.DecodeAll(small.compressed, nil)
	if err != nil || !bytes.Equal(raw, []byte{0x73}) {
		t.Fatal("small frame changed")
	}
	job.dst = nil
	pool.put(small.compressed)
	job.dst = pool.take()
	bigger := make([]byte, 6<<20)
	_, _ = rng.Read(bigger)
	grown := encodeV2FrameInto(encoder, v2RawFrame{data: bigger}, job.dst)
	if cap(grown.compressed) <= cap(job.dst) || pool.bytes != 0 {
		t.Fatal("fixture did not grow an active destination")
	}
	// Both references remain live until ordered write succeeds. The pool's zero
	// idle count is deliberately not a total active-output memory accounting.
	t.Logf("initial_cap=%d small_cap=%d old_active_cap=%d new_active_cap=%d coexist_cap=%d", initialCap, cap(small.compressed), cap(job.dst), cap(grown.compressed), cap(job.dst)+cap(grown.compressed))
	raw, err = decoder.DecodeAll(grown.compressed, nil)
	if err != nil || !bytes.Equal(raw, bigger) {
		t.Fatal("grown output changed")
	}
	job.dst = nil
	pool.put(grown.compressed)
	if pool.bytes != uint64(cap(grown.compressed)) || pool.bytes > v2CompressionOutputReuseBytes {
		t.Fatal("idle accounting after write")
	}
}
