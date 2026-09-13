package rawdb

// This file is an offline experiment API. It neither opens a database nor
// installs a writer/reader format. In particular zstd blobs returned only as
// size measurements below are NOT a production hot-history format.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

var (
	ErrHistoryCodecBenchmarkLimit  = errors.New("rawdb: history codec benchmark budget exhausted")
	ErrHistoryCodecBenchmarkClosed = errors.New("rawdb: history codec benchmark closed")
)

// HistoryCodecBenchmarkOptions bounds input work, not process RSS or CPU time.
// Zero selects the defaults: 128 MiB/pack, 1 GiB encoded and decoded in total,
// 256 attempted packs, and 100,000 records/pack. Limits cannot exceed these
// defaults except MaxRowsPerPack, whose hard maximum is 1,000,000.
type HistoryCodecBenchmarkOptions struct {
	MaxDecodedBytes      uint64 `json:"max_decoded_bytes"`
	MaxTotalDecodedBytes uint64 `json:"max_total_decoded_bytes"`
	MaxTotalEncodedBytes uint64 `json:"max_total_encoded_bytes"`
	MaxSamples           uint64 `json:"max_samples"`
	MaxRowsPerPack       uint64 `json:"max_rows_per_pack"`
}

// HistoryCodecBenchmarkTiming separates elapsed time from whole-process CPU.
// Process CPU includes other goroutines and runtime work. It is available on
// Linux/Darwin and is useful only in an otherwise idle standalone benchmark.
// Neither timing is a claim about online import throughput or avoided I/O.
type HistoryCodecBenchmarkTiming struct {
	WallNS              int64 `json:"wall_ns"`
	ProcessCPUNS        int64 `json:"process_cpu_ns"`
	ProcessCPUAvailable bool  `json:"process_cpu_available"`
}

type HistoryCodecBenchmarkCandidate struct {
	Name               string                       `json:"name"`
	StorageFormat      string                       `json:"storage_format"`
	StoredBytes        uint64                       `json:"stored_bytes"`
	ProductionReadable bool                         `json:"production_readable"`
	Encode             *HistoryCodecBenchmarkTiming `json:"encode,omitempty"`
	Decode             HistoryCodecBenchmarkTiming  `json:"decode"`
	ByteExact          bool                         `json:"byte_exact"`
}

type HistoryCodecBenchmarkSample struct {
	BlockNum                    uint64                           `json:"block_num"`
	Completed                   bool                             `json:"completed"`
	EncodedSHA256               string                           `json:"encoded_sha256"`
	RawSHA256                   string                           `json:"raw_sha256"`
	DecodedBytes                uint64                           `json:"decoded_bytes"`
	Rows                        uint64                           `json:"rows"`
	PrevBytes                   uint64                           `json:"prev_bytes"`
	MaxPrevBytes                uint64                           `json:"max_prev_bytes"`
	LargePrevRows               uint64                           `json:"large_prev_rows"`
	HasRepeatedLargeKey         bool                             `json:"has_repeated_large_key"`
	ProductionCDCGate           bool                             `json:"production_cdc_gate"`
	ForcedCDCUsefulAgainstRaw   bool                             `json:"forced_cdc_useful_against_raw"`
	ForcedCDCWouldReplaceSnappy bool                             `json:"forced_cdc_would_replace_snappy"`
	CDCWorkers                  int                              `json:"cdc_workers"`
	ZstdWindowBytes             uint64                           `json:"zstd_window_bytes"`
	Validation                  HistoryCodecBenchmarkTiming      `json:"validation"`
	Candidates                  []HistoryCodecBenchmarkCandidate `json:"candidates"`
}

// Charged work includes failed/canceled attempts after admission. Completed is
// incremented only after every codec has passed byte-exact round-trip checks.
type HistoryCodecBenchmarkStats struct {
	Attempts            uint64 `json:"attempts"`
	Completed           uint64 `json:"completed"`
	ChargedEncodedBytes uint64 `json:"charged_encoded_bytes"`
	ChargedDecodedBytes uint64 `json:"charged_decoded_bytes"`
}

// HistoryCodecBenchmark consumes exported sequence-zero block pack values one
// at a time. Input is borrowed only until BenchmarkPack returns. No values or
// cross-pack dictionary survive a call; repeated blocks are independently
// charged. Callers must keep repair-row/effective-view statistics separate.
type HistoryCodecBenchmark struct {
	mu      sync.Mutex
	options HistoryCodecBenchmarkOptions
	stats   HistoryCodecBenchmarkStats
	closed  bool
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

const historyBenchmarkZstdWindow = 8 << 20

func NewHistoryCodecBenchmark(options HistoryCodecBenchmarkOptions) (*HistoryCodecBenchmark, error) {
	defaults := HistoryCodecBenchmarkOptions{128 << 20, 1 << 30, 1 << 30, 256, 100000}
	fields := []*uint64{&options.MaxDecodedBytes, &options.MaxTotalDecodedBytes, &options.MaxTotalEncodedBytes, &options.MaxSamples, &options.MaxRowsPerPack}
	values := []uint64{defaults.MaxDecodedBytes, defaults.MaxTotalDecodedBytes, defaults.MaxTotalEncodedBytes, defaults.MaxSamples, defaults.MaxRowsPerPack}
	for i, field := range fields {
		if *field == 0 {
			*field = values[i]
		}
	}
	if options.MaxDecodedBytes > 128<<20 || options.MaxTotalDecodedBytes > 1<<30 || options.MaxTotalEncodedBytes > 1<<30 || options.MaxSamples > 256 || options.MaxRowsPerPack > 1000000 {
		return nil, fmt.Errorf("rawdb: invalid history codec benchmark limits")
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(historyBenchmarkZstdWindow))
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(128<<20), zstd.WithDecoderMaxWindow(historyBenchmarkZstdWindow), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		_ = encoder.Close()
		return nil, err
	}
	return &HistoryCodecBenchmark{options: options, encoder: encoder, decoder: decoder}, nil
}

func (b *HistoryCodecBenchmark) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.decoder.Close()
	return b.encoder.Close()
}

func (b *HistoryCodecBenchmark) Stats() HistoryCodecBenchmarkStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

// BenchmarkPack verifies canonical production RLP and reports four independent
// storage choices. snappy_baseline includes the production raw fallback;
// cdc_forced bypasses the size/key gate but retains the existing CDC encoder's
// 12.5% raw-saving requirement, falling back to raw when it returns no candidate.
// zstd_default is a standalone frame with no proposed hot envelope overhead.
// Existing input has no encode timing because it was produced before this run.
//
// Context is checked before/after each codec and during row validation. A codec
// invocation is synchronous and cannot be hard-interrupted; Close and any
// concurrent BenchmarkPack call wait for that invocation. Codec construction
// is outside individual timings; lazy allocation remains inside codec timings.
func (b *HistoryCodecBenchmark) BenchmarkPack(ctx context.Context, blockNum uint64, encoded []byte) (sample HistoryCodecBenchmarkSample, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sample.BlockNum = blockNum
	if ctx == nil {
		return sample, errors.New("rawdb: nil history codec benchmark context")
	}
	if b.closed {
		return sample, ErrHistoryCodecBenchmarkClosed
	}
	if err := ctx.Err(); err != nil {
		return sample, err
	}
	if b.stats.Attempts >= b.options.MaxSamples || uint64(len(encoded)) > b.options.MaxTotalEncodedBytes-b.stats.ChargedEncodedBytes {
		return sample, ErrHistoryCodecBenchmarkLimit
	}
	b.stats.Attempts++
	b.stats.ChargedEncodedBytes += uint64(len(encoded))
	// Exported production values never exceed 128 MiB. Reject even raw input
	// before parsing; compressed claimed sizes are checked before allocation.
	if len(encoded) == 0 || len(encoded) > 128<<20 {
		return sample, fmt.Errorf("%w: encoded pack size", ErrHistoryCodecBenchmarkLimit)
	}
	_, decodedLen, compressed, err := stateDomainChangeBlockCompressionPayload(encoded)
	if err != nil {
		return sample, err
	}
	if !compressed {
		decodedLen = len(encoded)
	}
	if uint64(decodedLen) > b.options.MaxDecodedBytes || uint64(decodedLen) > b.options.MaxTotalDecodedBytes-b.stats.ChargedDecodedBytes {
		return sample, fmt.Errorf("%w: decoded pack size", ErrHistoryCodecBenchmarkLimit)
	}
	b.stats.ChargedDecodedBytes += uint64(decodedLen)
	sample.DecodedBytes = uint64(decodedLen)
	sample.EncodedSHA256 = historyBenchmarkDigest(encoded)
	start := startHistoryBenchmarkTiming()
	raw, err := decodeStateDomainChangeBlockStorage(encoded)
	existingDecode := start.finish()
	if err != nil {
		return sample, err
	}
	if len(raw) != decodedLen {
		return sample, errors.New("rawdb: history benchmark decoded size changed")
	}
	if err := ctx.Err(); err != nil {
		return sample, err
	}
	start = startHistoryBenchmarkTiming()
	if err := inspectHistoryBenchmarkPack(ctx, raw, blockNum, b.options.MaxRowsPerPack, &sample); err != nil {
		return sample, err
	}
	sample.Validation = start.finish()
	sample.RawSHA256 = historyBenchmarkDigest(raw)
	// The data gate is reported assuming the production writer flag is enabled;
	// this experiment never reads or changes that global flag.
	sample.ProductionCDCGate = len(raw) >= 2<<20 && sample.HasRepeatedLargeKey
	sample.CDCWorkers = historyBenchmarkCDCWorkers()
	sample.ZstdWindowBytes = historyBenchmarkZstdWindow
	sample.Candidates = append(sample.Candidates, HistoryCodecBenchmarkCandidate{
		Name: "existing", StorageFormat: historyBenchmarkFormat(encoded), StoredBytes: uint64(len(encoded)), ProductionReadable: true, Decode: existingDecode, ByteExact: true,
	})

	baselineBytes := 0
	for _, name := range []string{"snappy_baseline", "cdc_forced", "zstd_default"} {
		if err := ctx.Err(); err != nil {
			return sample, err
		}
		start = startHistoryBenchmarkTiming()
		var stored []byte
		switch name {
		case "snappy_baseline":
			stored, _ = encodeStateDomainChangeBlockStorage(raw)
			baselineBytes = len(stored)
		case "cdc_forced":
			stored, sample.ForcedCDCUsefulAgainstRaw = encodeStateChangeChunksWithWorkers(raw, sample.CDCWorkers)
			if !sample.ForcedCDCUsefulAgainstRaw {
				stored = raw
			}
			sample.ForcedCDCWouldReplaceSnappy = sample.ForcedCDCUsefulAgainstRaw && len(stored)*8 < baselineBytes*7
		case "zstd_default":
			stored = b.encoder.EncodeAll(raw, nil)
		}
		encodeTiming := start.finish()
		if err := ctx.Err(); err != nil {
			return sample, err
		}
		result := HistoryCodecBenchmarkCandidate{Name: name, StoredBytes: uint64(len(stored)), Encode: &encodeTiming, ProductionReadable: name != "zstd_default"}
		start = startHistoryBenchmarkTiming()
		var restored []byte
		if name == "zstd_default" {
			result.StorageFormat = "zstd_frame_only"
			restored, err = b.decoder.DecodeAll(stored, make([]byte, 0, len(raw)))
		} else {
			result.StorageFormat = historyBenchmarkFormat(stored)
			restored, err = decodeStateDomainChangeBlockStorage(stored)
		}
		result.Decode = start.finish()
		if err != nil {
			return sample, fmt.Errorf("rawdb: benchmark %s decode: %w", name, err)
		}
		if !bytes.Equal(raw, restored) {
			return sample, fmt.Errorf("rawdb: benchmark %s round trip changed bytes", name)
		}
		result.ByteExact = true
		sample.Candidates = append(sample.Candidates, result)
		if err := ctx.Err(); err != nil {
			return sample, err
		}
	}
	sample.Completed = true
	b.stats.Completed++
	return sample, nil
}

func inspectHistoryBenchmarkPack(ctx context.Context, raw []byte, blockNum, maxRows uint64, sample *HistoryCodecBenchmarkSample) error {
	// Validate shape/count with the bounded borrowed production parser before
	// the owning decoder allocates row structs. Large-key identities are bounded
	// by decoded bytes / 128 KiB; no Prev image is retained in the identity map.
	type identity struct {
		flat       StateFlatDomain
		owner      common.Address
		generation uint64
		domain     kvdomains.KVDomain
		key        string
	}
	seen := make(map[identity]struct{})
	_, err := iteratePersistedStateDomainChangeBlockBorrowed(raw, blockNum, func(change *StateDomainChange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if sample.Rows >= maxRows {
			return false, fmt.Errorf("%w: rows per pack", ErrHistoryCodecBenchmarkLimit)
		}
		sample.Rows++
		sample.PrevBytes += uint64(len(change.Prev))
		sample.MaxPrevBytes = max(sample.MaxPrevBytes, uint64(len(change.Prev)))
		if change.PrevExists && len(change.Prev) >= historychunk.MaxSize {
			sample.LargePrevRows++
			// Match stateChangeBlockHasLargeVersions, including fields that
			// some flat domains do not use to construct their physical key.
			key := identity{change.FlatDomain, change.Owner, change.Generation, change.Domain, string(change.Key)}
			if _, found := seen[key]; found {
				sample.HasRepeatedLargeKey = true
			}
			seen[key] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	changes, err := decodePersistedStateDomainChangeBlock(raw, blockNum)
	if err != nil {
		return err
	}
	rows := make([]persistedStateDomainChange, len(changes))
	for i, change := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[i] = persistedStateDomainChange{change.TxNum, change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key, change.PrevExists, change.Prev}
	}
	// Re-encode the owning production decode into a comparing writer, checking
	// every canonical RLP byte without retaining another raw result. RLP itself
	// also uses encoding buffers; input limits are not a process-memory ceiling.
	writer := &historyBenchmarkEqualWriter{want: raw}
	if err := rlp.Encode(writer, &persistedStateDomainChangeBlock{persistedStateDomainChangeBlockVersion, changes[0].Seq, rows}); err != nil {
		return fmt.Errorf("rawdb: history benchmark source round trip: %w", err)
	}
	if len(writer.want) != 0 {
		return errors.New("rawdb: history benchmark source round trip truncated")
	}
	return ctx.Err()
}

type historyBenchmarkEqualWriter struct{ want []byte }

func (w *historyBenchmarkEqualWriter) Write(p []byte) (int, error) {
	if len(p) > len(w.want) || !bytes.Equal(p, w.want[:len(p)]) {
		return 0, errors.New("history pack RLP bytes changed")
	}
	w.want = w.want[len(p):]
	return len(p), nil
}

func historyBenchmarkDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func historyBenchmarkFormat(data []byte) string {
	if len(data) > len(stateDomainChangeBlockEnvelopeMagic) && bytes.Equal(data[:len(stateDomainChangeBlockEnvelopeMagic)], stateDomainChangeBlockEnvelopeMagic[:]) {
		if data[len(stateDomainChangeBlockEnvelopeMagic)] == stateDomainChangeBlockChunksVersion {
			return "cdc_v2"
		}
		return "snappy_v1"
	}
	return "raw_rlp"
}

type historyBenchmarkTimer struct {
	wall time.Time
	cpu  int64
	ok   bool
}

func startHistoryBenchmarkTiming() historyBenchmarkTimer {
	cpu, ok := historyBenchmarkProcessCPU()
	return historyBenchmarkTimer{time.Now(), cpu, ok}
}

func (t historyBenchmarkTimer) finish() HistoryCodecBenchmarkTiming {
	wall := time.Since(t.wall).Nanoseconds()
	cpu, ok := historyBenchmarkProcessCPU()
	result := HistoryCodecBenchmarkTiming{WallNS: wall, ProcessCPUAvailable: t.ok && ok && cpu >= t.cpu}
	if result.ProcessCPUAvailable {
		result.ProcessCPUNS = cpu - t.cpu
	}
	return result
}
