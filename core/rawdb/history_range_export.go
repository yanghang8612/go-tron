package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const (
	StateHistoryRangeExportMaxBlocks       = uint64(256)
	StateHistoryRangeExportMaxBytes        = uint64(1 << 30)
	StateHistoryRangeExportMaxRows         = uint64(262144)
	StateHistoryRangeExportMaxDecodedBytes = uint64(4 << 30)
)

// ErrStateHistoryRangeExportBudget identifies a cooperative physical-copy or
// declared logical-size limit. No write exceeding a limit is attempted.
var ErrStateHistoryRangeExportBudget = errors.New("rawdb: history range export budget exhausted")

type StateHistoryRangeExportOptions struct {
	FromBlock       uint64 `json:"from_block"`
	ToBlock         uint64 `json:"to_block"`
	MaxBytes        uint64 `json:"max_bytes"`
	MaxRows         uint64 `json:"max_rows"`
	MaxDecodedBytes uint64 `json:"max_decoded_bytes"`
}

func (o StateHistoryRangeExportOptions) Validate() error {
	if o.FromBlock > o.ToBlock || o.ToBlock-o.FromBlock >= StateHistoryRangeExportMaxBlocks {
		return fmt.Errorf("rawdb: history export requires 1..%d inclusive blocks", StateHistoryRangeExportMaxBlocks)
	}
	if o.MaxBytes == 0 || o.MaxBytes > StateHistoryRangeExportMaxBytes || o.MaxRows == 0 || o.MaxRows > StateHistoryRangeExportMaxRows || o.MaxDecodedBytes == 0 || o.MaxDecodedBytes > StateHistoryRangeExportMaxDecodedBytes {
		return errors.New("rawdb: history export budgets must be positive and at most 1GiB physical, 262144 rows, 4GiB declared decoded")
	}
	return nil
}

type StateHistoryRangeExportEntry struct {
	KeyHex      string `json:"key_hex"`
	ValueBytes  uint64 `json:"value_bytes"`
	ValueSHA256 string `json:"value_sha256"`
	Family      string `json:"family"`
}

// Codec statistics cover successfully copied physical history rows/chunks.
// Chunk decoded bytes are separate from DeclaredDecodedBytes in the report,
// which counts each logical history pack/repair once, including repeated refs.
type StateHistoryRangeExportCodec struct {
	Rows                 uint64 `json:"rows"`
	ValueBytes           uint64 `json:"value_bytes"`
	DeclaredDecodedBytes uint64 `json:"declared_decoded_bytes"`
}

type StateHistoryRangeExportBlock struct {
	Block                uint64 `json:"block"`
	CanonicalHash        string `json:"canonical_hash"`
	BeginTxNum           uint64 `json:"begin_tx_num"`
	EndTxNum             uint64 `json:"end_tx_num"`
	PackPresent          bool   `json:"pack_present"`
	PackCodec            string `json:"pack_codec,omitempty"`
	DeclaredDecodedBytes uint64 `json:"declared_decoded_bytes"`
	RepairRows           uint64 `json:"repair_rows"`
}

type StateHistoryRangeExportReport struct {
	Complete              bool                                    `json:"complete"`
	ContentVerified       bool                                    `json:"content_verified"`
	StopReason            string                                  `json:"stop_reason"`
	Error                 string                                  `json:"error,omitempty"`
	FromBlock             uint64                                  `json:"from_block"`
	ToBlock               uint64                                  `json:"to_block"`
	FromTxNum             uint64                                  `json:"from_tx_num"`
	ToTxNum               uint64                                  `json:"to_tx_num"`
	Blocks                uint64                                  `json:"blocks"`
	PhysicalRows          uint64                                  `json:"physical_rows"`
	PhysicalBytes         uint64                                  `json:"physical_bytes"`
	DeclaredDecodedBytes  uint64                                  `json:"declared_decoded_bytes"`
	SharedReferenceCount  uint64                                  `json:"shared_reference_count"`
	UniqueChunkCount      uint64                                  `json:"unique_chunk_count"`
	MissingBucketMetadata []uint64                                `json:"missing_bucket_metadata,omitempty"`
	Codecs                map[string]StateHistoryRangeExportCodec `json:"codecs"`
	BlockDetails          []StateHistoryRangeExportBlock          `json:"block_details"`
	Entries               []StateHistoryRangeExportEntry          `json:"entries"`
	ManifestSHA256        string                                  `json:"manifest_sha256"`
}

// ExportStateHistoryRange copies a bounded inclusive range into a NEW private
// store supplied by the caller. It never writes the source and does not acquire
// or release the caller's pinned view. Canonical hot block bytes, tx ranges,
// every physical sequence row (including repairs), referenced shared chunks,
// and existing bucket metadata retain their original keys and values.
//
// Complete means physical capture completed, not that histories were decoded:
// ContentVerified is always false. Size/envelope preflight never materializes a
// pack or decompresses a chunk. The caller must fully decode/authenticate and
// compare the private copy before using a cold replay result. This partial DB
// is neither a chain backup nor evidence for pruning any source bucket.
//
// Limits bound copied key+value bytes, physical rows, and declared logical
// history bytes. Cancellation is checked between operations; a source Get,
// iterator step, block protobuf decode, or destination Put is not interruptible.
// On any error the destination can contain a prefix, including part of the last
// block. Entries describe only acknowledged successful writes; a failed Put
// may itself have written data. Callers must discard incomplete destinations.
func ExportStateHistoryRange(ctx context.Context, src StateHistoryReadView, dst ethdb.KeyValueWriter, opts StateHistoryRangeExportOptions) (report StateHistoryRangeExportReport, err error) {
	report = StateHistoryRangeExportReport{FromBlock: opts.FromBlock, ToBlock: opts.ToBlock, StopReason: "incomplete", Codecs: make(map[string]StateHistoryRangeExportCodec)}
	defer func() {
		sort.Slice(report.Entries, func(i, j int) bool { return report.Entries[i].KeyHex < report.Entries[j].KeyHex })
		report.ManifestSHA256 = stateHistoryRangeManifestDigest(report.Entries)
		if err != nil {
			report.Error = err.Error()
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				report.StopReason = "context"
			case errors.Is(err, ErrStateHistoryRangeExportBudget):
				report.StopReason = "budget"
			default:
				report.StopReason = "error"
			}
		}
	}()
	if err = opts.Validate(); err != nil {
		return report, err
	}
	if src == nil || !src.IsPinnedKeyValueView() {
		return report, ErrStateHistoryReadViewUnpinned
	}
	if dst == nil {
		return report, errors.New("rawdb: nil history export destination")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e := historyRangeExporter{ctx: ctx, src: src, dst: dst, opts: opts, report: &report, chunks: make(map[string]uint64), buckets: make(map[uint64]bool)}
	var previousHash common.Hash
	var previousEnd uint64
	for block := opts.FromBlock; ; block++ {
		var detail StateHistoryRangeExportBlock
		detail, err = e.copyBlock(block, previousHash, previousEnd, block != opts.FromBlock)
		if err != nil {
			return report, fmt.Errorf("rawdb: export history block %d: %w", block, err)
		}
		if report.Blocks == 0 {
			report.FromTxNum = detail.BeginTxNum
		}
		report.ToTxNum = detail.EndTxNum
		report.Blocks++
		report.BlockDetails = append(report.BlockDetails, detail)
		previousHash = common.HexToHash(detail.CanonicalHash)
		previousEnd = detail.EndTxNum
		if block == opts.ToBlock {
			break // no increment of a possible MaxUint64 final height
		}
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	report.Complete, report.StopReason = true, "complete"
	return report, nil
}

type historyRangeExporter struct {
	ctx     context.Context
	src     StateHistoryReadView
	dst     ethdb.KeyValueWriter
	opts    StateHistoryRangeExportOptions
	report  *StateHistoryRangeExportReport
	chunks  map[string]uint64
	buckets map[uint64]bool
}

func (e *historyRangeExporter) read(key []byte, family string, required bool) ([]byte, bool, error) {
	if err := e.ctx.Err(); err != nil {
		return nil, false, err
	}
	value, exists, err := readPresentValue(e.src, key, "history export "+family)
	if err == nil && required && !exists {
		err = fmt.Errorf("missing %s", family)
	}
	return value, exists, err
}

func (e *historyRangeExporter) put(key, value []byte, family, codec string, declared uint64) error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	physical := uint64(len(key)) + uint64(len(value))
	if e.report.PhysicalRows >= e.opts.MaxRows || physical > e.opts.MaxBytes-e.report.PhysicalBytes || declared > e.opts.MaxDecodedBytes-e.report.DeclaredDecodedBytes {
		return ErrStateHistoryRangeExportBudget
	}
	digest := sha256.Sum256(value)
	entry := StateHistoryRangeExportEntry{KeyHex: hex.EncodeToString(key), ValueBytes: uint64(len(value)), ValueSHA256: hex.EncodeToString(digest[:]), Family: family}
	// Iterators may lend buffers and a writer may retain its input. Hand over
	// private copies, so neither source buffers nor future iterator steps alias.
	if err := e.dst.Put(bytes.Clone(key), bytes.Clone(value)); err != nil {
		return fmt.Errorf("write history export %s: %w", family, err)
	}
	e.report.PhysicalRows++
	e.report.PhysicalBytes += physical
	e.report.DeclaredDecodedBytes += declared
	e.report.Entries = append(e.report.Entries, entry)
	if codec != "" {
		stat := e.report.Codecs[codec]
		stat.Rows++
		stat.ValueBytes += uint64(len(value))
		stat.DeclaredDecodedBytes += declared
		e.report.Codecs[codec] = stat
	}
	return nil
}

func (e *historyRangeExporter) copyBlock(height uint64, previousHash common.Hash, previousEnd uint64, checkParent bool) (detail StateHistoryRangeExportBlock, err error) {
	detail.Block = height
	blockRaw, _, err := e.read(blockKey(height), "canonical-block", true)
	if err != nil {
		return detail, err
	}
	block, err := types.UnmarshalBlock(blockRaw)
	if err != nil {
		return detail, fmt.Errorf("decode canonical block: %w", err)
	}
	if block.Proto().GetBlockHeader().GetRawData() == nil || block.Proto().GetBlockHeader().GetRawData().GetNumber() < 0 {
		return detail, errors.New("invalid canonical block header")
	}
	if block.Number() != height {
		return detail, fmt.Errorf("canonical block number %d does not match key %d", block.Number(), height)
	}
	if checkParent && block.ParentHash() != previousHash {
		return detail, errors.New("canonical parent hash discontinuity")
	}
	hash := block.Hash()
	txRaw, _, err := e.read(stateTxRangeKey(height), "tx-range", true)
	if err != nil {
		return detail, err
	}
	rangeHash, begin, end, err := decodeBorrowedStateTxRange(txRaw, height)
	if err != nil {
		return detail, err
	}
	if rangeHash != hash {
		return detail, errors.New("tx range hash does not match canonical block")
	}
	if checkParent && (previousEnd == ^uint64(0) || begin != previousEnd+1) {
		return detail, errors.New("tx range discontinuity")
	}
	detail.CanonicalHash, detail.BeginTxNum, detail.EndTxNum = hash.Hex(), begin, end
	if err := e.put(blockKey(height), blockRaw, "canonical-block", "", 0); err != nil {
		return detail, err
	}
	if err := e.put(stateTxRangeKey(height), txRaw, "tx-range", "", 0); err != nil {
		return detail, err
	}
	prefix := stateChangeSetBlockPrefix(height)
	it := e.src.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		if err := e.ctx.Err(); err != nil {
			return detail, err
		}
		key, value := it.Key(), it.Value()
		if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
			return detail, errors.New("invalid physical history sequence key")
		}
		seq := binary.BigEndian.Uint64(key[len(prefix):])
		codec, declared := "legacy-row", uint64(len(value))
		if seq == 0 {
			if isStateHistorySharedPack(value) {
				refs, length, count, _, err := sharedStateHistoryPackHeader(value, height)
				if err != nil {
					return detail, err
				}
				codec, declared = "shared3", uint64(length)
				// Refuse a logical-size overrun before any dependency reads.
				if declared > e.opts.MaxDecodedBytes-e.report.DeclaredDecodedBytes {
					return detail, ErrStateHistoryRangeExportBudget
				}
				if err := e.copySharedDependencies(height, refs, count); err != nil {
					return detail, err
				}
			} else {
				codec, declared, err = InspectStateHistoryPackEncoding(value)
				if err != nil {
					return detail, err
				}
			}
		} else if declared == 0 || declared > stateDomainChangeBlockMaxDecodedBytes {
			return detail, errors.New("invalid legacy history row size")
		}
		if err := e.put(key, value, "changeset", codec, declared); err != nil {
			return detail, err
		}
		detail.DeclaredDecodedBytes += declared
		if seq == 0 {
			detail.PackPresent, detail.PackCodec = true, codec
		} else {
			detail.RepairRows++
		}
	}
	if err := it.Error(); err != nil {
		return detail, fmt.Errorf("iterate physical history: %w", err)
	}
	return detail, nil
}

func (e *historyRangeExporter) copySharedDependencies(height uint64, refs []byte, count int) error {
	bucket := stateHistoryChunkBucket(height)
	if !e.buckets[bucket] {
		key := stateHistoryChunkBucketKey(bucket)
		meta, exists, err := e.read(key, "shared-bucket", false)
		if err != nil {
			return err
		}
		if exists {
			if len(meta) != 2 || meta[0] != 1 || meta[1] > 1 {
				return errors.New("invalid shared bucket metadata")
			}
			if err := e.put(key, meta, "shared-bucket", "", 0); err != nil {
				return err
			}
		} else {
			e.report.MissingBucketMetadata = append(e.report.MissingBucketMetadata, bucket)
		}
		e.buckets[bucket] = true
	}
	for i := 0; i < count; i++ {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		size, n := binary.Uvarint(refs) // complete table already preflighted
		var digest [32]byte
		copy(digest[:], refs[n:n+32])
		refs = refs[n+32:]
		key := stateHistoryChunkKey(bucket, digest)
		if previousSize, ok := e.chunks[string(key)]; ok {
			if size != previousSize {
				return errors.New("conflicting declared sizes for shared chunk")
			}
		} else {
			value, _, err := e.read(key, "shared-chunk", true)
			if err != nil {
				return err
			}
			codec, err := preflightHistoryExportChunk(value, size)
			if err != nil {
				return err
			}
			if err := e.put(key, value, "shared-chunk", codec, 0); err != nil {
				return err
			}
			stat := e.report.Codecs[codec]
			stat.DeclaredDecodedBytes += size
			e.report.Codecs[codec] = stat
			e.chunks[string(key)] = size
			e.report.UniqueChunkCount++
		}
		e.report.SharedReferenceCount++
	}
	return nil
}

// Mirrors the production decoder's allocation preflight without Snappy.Decode
// or decoded SHA authentication. A structurally valid corrupt body may pass;
// the required private-copy full decode is the content authentication step.
func preflightHistoryExportChunk(value []byte, want uint64) (string, error) {
	if want == 0 || want > historychunk.MaxSize || len(value) < 3 || len(value) > historychunk.MaxSize+binary.MaxVarintLen64+2 || value[0] != 1 || value[1] > 1 {
		return "", errors.New("invalid shared chunk envelope")
	}
	n, used := binary.Uvarint(value[2:])
	if used <= 0 || n != want {
		return "", errors.New("shared chunk length mismatch")
	}
	payload := value[2+used:]
	if value[1] == 0 {
		if uint64(len(payload)) != want {
			return "", errors.New("truncated shared raw chunk")
		}
		return "chunk-raw", nil
	}
	decoded, err := snappy.DecodedLen(payload)
	if err != nil || uint64(decoded) != want {
		return "", errors.New("invalid shared Snappy chunk size")
	}
	return "chunk-snappy", nil
}

// The manifest digest is SHA256 over sorted entries, each encoded as a u64 BE
// key length, raw key, u64 BE value length, and raw 32-byte value SHA256. Family
// labels are descriptive and excluded; no new persistent database schema exists.
func stateHistoryRangeManifestDigest(entries []StateHistoryRangeExportEntry) string {
	h := sha256.New()
	var n [8]byte
	for _, entry := range entries {
		key, _ := hex.DecodeString(entry.KeyHex)
		digest, _ := hex.DecodeString(entry.ValueSHA256)
		binary.BigEndian.PutUint64(n[:], uint64(len(key)))
		h.Write(n[:])
		h.Write(key)
		binary.BigEndian.PutUint64(n[:], entry.ValueBytes)
		h.Write(n[:])
		h.Write(digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}
