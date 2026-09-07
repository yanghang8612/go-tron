//go:build darwin || linux

package snapshots

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"golang.org/x/sys/unix"
)

// These tests never configure production compression. Every level receives
// identical complete V6 history bytes and identical independent 128KiB frames.
const historyLevelFrameBytes = 128 << 10

var historyLevelCases = []zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression}
var historyLevelShapes = []string{"account-v5", "storage-random32", "storage-counter32", "delegation-list"}

func historyLevelFixture(t testing.TB, shape string, small bool) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(20260907))
	address := func() []byte {
		b := make([]byte, 21)
		_, _ = rng.Read(b)
		b[0] = 0x41
		return b
	}
	owners := make([]common.Address, 256)
	for i := range owners {
		owners[i] = common.BytesToAddress(address())
	}
	count := 32768
	if small {
		count = 4096
	}
	var peers [][]byte
	if shape == "delegation-list" {
		count, peers = 12, make([][]byte, 65536)
		if small {
			count, peers = 4, make([][]byte, 8192)
		}
		for i := range peers {
			peers[i] = address()
		}
	}
	changes := make([]*rawdb.StateDomainChange, 0, count)
	for i := 0; i < count; i++ {
		tx, block := uint64(i+1), uint64(i/16+1)
		c := &rawdb.StateDomainChange{BlockNum: block, BlockHash: common.Hash{byte(block), byte(block >> 8)},
			TxNum: tx, Seq: uint64(i%16 + 1), Owner: owners[i%len(owners)], FlatDomain: rawdb.StateFlatDomainKVLatest,
			Generation: 1, Domain: kvdomains.ContractStorage, Key: []byte(fmt.Sprintf("slot/%06d", i%4096)), PrevExists: true}
		var err error
		switch shape {
		case "account-v5":
			c.FlatDomain, c.Generation, c.Domain, c.Key = rawdb.StateFlatDomainAccountLatest, 0, 0, nil
			pb := &corepb.Account{Address: c.Owner.Bytes(), Balance: int64(1_000_000 + i*17), NetUsage: int64(i % 1000),
				CreateTime: 1_600_000_000_000 + int64(i%256), LatestOprationTime: 1_700_000_000_000 + int64(i*3000),
				LatestConsumeTime: int64(i / 16), AccountName: []byte(fmt.Sprintf("account-%03d", i%256))}
			pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, byte(i%127 + 1)})
			core, marshalErr := types.NewAccountFromPB(pb).MarshalStorageCoreV4()
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			// Explicit V5 no-code envelope; state cannot be imported by its
			// snapshots dependency. The core is the real production encoder.
			c.Prev, err = rlp.EncodeToBytes([]any{uint64(5), core, []byte(nil), uint64(0), []byte(nil)})
		case "storage-random32", "storage-counter32":
			c.Prev = make([]byte, 32)
			if shape == "storage-random32" {
				_, _ = rng.Read(c.Prev)
			} else {
				for j, n := 31, uint64(i*19+1); n != 0; j, n = j-1, n>>8 {
					c.Prev[j] = byte(n)
				}
			}
		case "delegation-list":
			c.Owner, c.Domain, c.Key = common.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyStateKey(owners[0].Bytes())
			// Ordered random 21-byte addresses, a new insertion each version,
			// and changing metadata. This is synthetic, not a mainnet sample.
			list := append([][]byte{}, peers...)
			position := len(list) / 2
			list = append(list, nil)
			copy(list[position+1:], list[position:])
			list[position] = address()
			pb := &corepb.DelegatedResourceAccountIndex{Account: owners[0].Bytes(), ToAccounts: list, Timestamp: int64(i)}
			pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, byte(i + 1)})
			c.Prev, err = statecodec.MarshalDelegationIndex(pb)
		default:
			t.Fatalf("unknown fixture %q", shape)
		}
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, c)
	}
	raw, _, _, err := encodeStateDomainChangeBinarySegmentV6(1, uint64(count), normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func historyLevelFrames(raw []byte) [][]byte {
	frames := make([][]byte, 0, (len(raw)+historyLevelFrameBytes-1)/historyLevelFrameBytes)
	for off := 0; off < len(raw); off += historyLevelFrameBytes {
		frames = append(frames, raw[off:min(len(raw), off+historyLevelFrameBytes)])
	}
	return frames
}

func historyLevelParallel(n, workers int, fn func(int)) {
	if workers == 1 {
		for i := 0; i < n; i++ {
			fn(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < min(n, workers); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int(next.Add(1) - 1); i < n; i = int(next.Add(1) - 1) {
				fn(i)
			}
		}()
	}
	wg.Wait()
}

func historyLevelCodec(t testing.TB, level zstd.EncoderLevel, workers int) (*zstd.Encoder, *zstd.Decoder) {
	return historyLevelCodecProfile(t, level, workers, false)
}

func historyLevelCodecProfile(t testing.TB, level zstd.EncoderLevel, workers int, production bool) (*zstd.Encoder, *zstd.Decoder) {
	t.Helper()
	eopts := []zstd.EOption{zstd.WithEncoderLevel(level)}
	dopts := []zstd.DOption{zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true)}
	if !production {
		eopts = append(eopts, zstd.WithEncoderConcurrency(workers), zstd.WithWindowSize(historyLevelFrameBytes), zstd.WithSingleSegment(true), zstd.WithEncoderCRC(true))
		dopts = []zstd.DOption{zstd.WithDecoderConcurrency(workers), zstd.WithDecoderMaxMemory(historyLevelFrameBytes),
			zstd.WithDecoderMaxWindow(historyLevelFrameBytes), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true)}
	}
	// Production mode exactly matches cbCodec's options at SpeedDefault;
	// the stronger variants change only level. External task count is separate.
	e, err := zstd.NewWriter(nil, eopts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	d, err := zstd.NewReader(nil, dopts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return e, d
}

func TestHistoryCompressionLevelsProductionOptionsMatchFactory(t *testing.T) {
	factory, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	production, _ := historyLevelCodecProfile(t, zstd.SpeedDefault, 4, true)
	_, bounded := historyLevelCodec(t, zstd.SpeedDefault, 1)
	rng := rand.New(rand.NewSource(41))
	for _, n := range []int{1, 17, 1024, 1025, historyLevelFrameBytes - 1, historyLevelFrameBytes} {
		raw := make([]byte, n)
		_, _ = rng.Read(raw)
		encoded := production.EncodeAll(raw, nil)
		if !bytes.Equal(encoded, factory.EncodeAll(raw, nil)) {
			t.Fatalf("production baseline differs from cbCodec at %d bytes", n)
		}
		var h zstd.Header
		if err := h.Decode(encoded); err != nil || h.HasFCS && h.FrameContentSize != uint64(n) || !h.HasCheckSum || !h.SingleSegment && h.WindowSize > historyLevelFrameBytes {
			t.Fatalf("factory frame header=%+v err=%v", h, err)
		}
		decoded, err := bounded.DecodeAll(encoded, make([]byte, 0, n))
		if err != nil || !bytes.Equal(decoded, raw) {
			t.Fatalf("bounded factory frame %d: %v", n, err)
		}
	}
}

func TestHistoryCompressionLevelsByteExactAndWindowBound(t *testing.T) {
	if zstd.EncoderLevelFromZstd(6) != zstd.EncoderLevelFromZstd(9) {
		t.Fatal("update the documented level alias for the selected dependency")
	}
	for _, shape := range historyLevelShapes {
		t.Run(shape, func(t *testing.T) {
			raw := historyLevelFixture(t, shape, true)
			before := sha256.Sum256(raw)
			frames := historyLevelFrames(raw)
			for _, level := range historyLevelCases {
				var expected [][]byte
				for _, workers := range []int{1, 4, 8} {
					t.Run(fmt.Sprintf("%s/workers%d", level, workers), func(t *testing.T) {
						e, d := historyLevelCodec(t, level, workers)
						encoded, decoded := make([][]byte, len(frames)), make([][]byte, len(frames))
						errs := make([]error, len(frames))
						historyLevelParallel(len(frames), workers, func(i int) {
							encoded[i] = e.EncodeAll(frames[i], nil)
							decoded[i], errs[i] = d.DecodeAll(encoded[i], make([]byte, 0, len(frames[i])))
						})
						stored := 0
						for i := range frames {
							var h zstd.Header
							if err := h.Decode(encoded[i]); err != nil || !h.HasFCS || !h.SingleSegment || !h.HasCheckSum || h.FrameContentSize != uint64(len(frames[i])) || h.FrameContentSize > historyLevelFrameBytes {
								t.Fatalf("frame %d header=%+v err=%v", i, h, err)
							}
							if errs[i] != nil || !bytes.Equal(frames[i], decoded[i]) {
								t.Fatalf("frame %d exact decode: %v", i, errs[i])
							}
							if expected != nil && !bytes.Equal(encoded[i], expected[i]) {
								t.Fatalf("parallel completion changed compressed frame %d", i)
							}
							stored += len(encoded[i])
						}
						if expected == nil {
							expected = encoded
							t.Logf("SMOKE_SYNTHETIC raw=%d frames=%d zstd_frame_bytes=%d", len(raw), len(frames), stored)
						}
					})
				}
			}
			if sha256.Sum256(raw) != before {
				t.Fatal("encoding mutated the logical input")
			}
		})
	}
}

func historyLevelCPU(t testing.TB) time.Duration {
	t.Helper()
	var r unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &r); err != nil {
		t.Fatal(err)
	}
	return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
}

// Run one selected case per process with /usr/bin/time for process max RSS.
// The corpus and all encoded frames remain resident: B/op is allocation work,
// not a claimed production-pipeline memory limit. Codec initialization and
// fixture creation are outside the warmed steady-state timing below.
func BenchmarkHistoryCompressionLevels(b *testing.B) {
	benchmarkHistoryCompressionLevels(b, false)
}

func BenchmarkHistoryCompressionLevelsProductionOptions(b *testing.B) {
	benchmarkHistoryCompressionLevels(b, true)
}

func benchmarkHistoryCompressionLevels(b *testing.B, production bool) {
	for _, shape := range historyLevelShapes {
		b.Run(shape, func(b *testing.B) {
			raw := historyLevelFixture(b, shape, false)
			frames := historyLevelFrames(raw)
			for _, level := range historyLevelCases {
				for _, workers := range []int{1, 4, 8} {
					for _, operation := range []string{"encode", "decode"} {
						b.Run(fmt.Sprintf("%s/workers%d/%s", level, workers, operation), func(b *testing.B) {
							e, d := historyLevelCodecProfile(b, level, workers, production)
							encoded, decoded := make([][]byte, len(frames)), make([][]byte, len(frames))
							errs := make([]error, len(frames))
							historyLevelParallel(len(frames), workers, func(i int) {
								encoded[i] = e.EncodeAll(frames[i], nil)
								decoded[i], errs[i] = d.DecodeAll(encoded[i], make([]byte, 0, len(frames[i])))
							})
							stored := 0
							for i := range frames {
								if errs[i] != nil || !bytes.Equal(decoded[i], frames[i]) {
									b.Fatalf("warmup exact decode: %v", errs[i])
								}
								stored += len(encoded[i])
							}
							b.ReportAllocs()
							b.SetBytes(int64(len(raw)))
							b.ResetTimer()
							cpuStart := historyLevelCPU(b)
							for n := 0; n < b.N; n++ {
								historyLevelParallel(len(frames), workers, func(i int) {
									if operation == "encode" {
										encoded[i] = e.EncodeAll(frames[i], encoded[i][:0])
									} else {
										decoded[i], errs[i] = d.DecodeAll(encoded[i], decoded[i][:0:len(frames[i])])
									}
								})
							}
							cpu := historyLevelCPU(b) - cpuStart
							b.StopTimer()
							for i := range frames {
								got, err := d.DecodeAll(encoded[i], decoded[i][:0:len(frames[i])])
								if err != nil || errs[i] != nil || !bytes.Equal(got, frames[i]) {
									b.Fatalf("final exact decode: %v/%v", err, errs[i])
								}
							}
							b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
							b.ReportMetric(float64(stored), "zstd-frame-B")
							b.ReportMetric(float64(len(frames)), "frames/op")
							b.ReportMetric(historyLevelFrameBytes, "max-raw-frame-B")
						})
					}
				}
			}
		})
	}
}
