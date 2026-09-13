package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Version 3 shares immutable chunks within fixed 1024-block buckets. The
// complete original RLP, including every transaction/sequence/Prev, is retained.
// Unlike v2 it requires a coherent database reader and cannot be decoded from
// the pack bytes alone. No chunk points to another chunk or another bucket.
const stateDomainChangeBlockSharedVersion = byte(3)

var stateHistoryCrossBlockDedup atomic.Bool

// StateHistoryChunkAtomicWriter is an explicit publication capability, not a
// consequence of merely implementing Get and Put. A true result promises that
// all new chunks and the final pack belong to the same atomic block layer or
// batch; dependencies on prior layers cannot commit after an ancestor fails.
// The caller holds the writer/maintenance guard for the whole operation.
type StateHistoryChunkAtomicWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	StateHistoryChunkWritesAtomic() bool
}

// SetStateHistoryCrossBlockDedup controls new writes only. Readers must retain
// v3 support even when the writer is disabled, including a rollback release.
func SetStateHistoryCrossBlockDedup(enabled bool) { stateHistoryCrossBlockDedup.Store(enabled) }

var (
	sharedHistoryAttempts      = metrics.NewRegisteredCounter("state/history/shared/attempts", nil)
	sharedHistorySelected      = metrics.NewRegisteredCounter("state/history/shared/selected", nil)
	sharedHistoryFallback      = metrics.NewRegisteredCounter("state/history/shared/fallback", nil)
	sharedHistoryRetired       = metrics.NewRegisteredCounter("state/history/shared/retired_fallback", nil)
	sharedHistoryBaselineBytes = metrics.NewRegisteredCounter("state/history/shared/baseline_bytes", nil)
	sharedHistoryPackBytes     = metrics.NewRegisteredCounter("state/history/shared/pack_bytes", nil)
	sharedHistoryChunkBytes    = metrics.NewRegisteredCounter("state/history/shared/new_chunk_bytes", nil)
	sharedHistoryChunkKeyBytes = metrics.NewRegisteredCounter("state/history/shared/new_chunk_key_bytes", nil)
	sharedHistoryMetadataBytes = metrics.NewRegisteredCounter("state/history/shared/new_metadata_kv_bytes", nil)
	sharedHistoryChunkCount    = metrics.NewRegisteredCounter("state/history/shared/new_chunks", nil)
	sharedHistoryReusedBytes   = metrics.NewRegisteredCounter("state/history/shared/reused_raw_bytes", nil)
	sharedHistoryWorkNanos     = metrics.NewRegisteredCounter("state/history/shared/work_nanos", nil)
)

type stateHistorySharedWriteStats struct {
	baseline, pack, chunks, chunkKeys, newChunks, reused, metadata int
	work                                                           time.Duration
}

// Called only after the final pack Put returns successfully. These are staged
// publication counters, not an fsync acknowledgment or physical disk savings.
func (s *stateHistorySharedWriteStats) observe() {
	if s == nil {
		return
	}
	sharedHistorySelected.Inc(1)
	sharedHistoryBaselineBytes.Inc(int64(s.baseline))
	sharedHistoryPackBytes.Inc(int64(s.pack))
	sharedHistoryChunkBytes.Inc(int64(s.chunks))
	sharedHistoryChunkKeyBytes.Inc(int64(s.chunkKeys))
	sharedHistoryMetadataBytes.Inc(int64(s.metadata))
	sharedHistoryChunkCount.Inc(int64(s.newChunks))
	sharedHistoryReusedBytes.Inc(int64(s.reused))
	sharedHistoryWorkNanos.Inc(int64(s.work))
}

func isStateHistorySharedPack(data []byte) bool {
	return len(data) > len(stateDomainChangeBlockEnvelopeMagic) &&
		bytes.Equal(data[:len(stateDomainChangeBlockEnvelopeMagic)], stateDomainChangeBlockEnvelopeMagic[:]) &&
		data[len(stateDomainChangeBlockEnvelopeMagic)] == stateDomainChangeBlockSharedVersion
}

func stateHistorySharedEligible(changes []*StateDomainChange) bool {
	for _, c := range changes {
		if c != nil && c.PrevExists && len(c.Prev) >= historychunk.MaxSize {
			return true
		}
	}
	return false
}

type stateHistoryPlannedChunk struct {
	key, encoded []byte
	start, end   int
}

// planAndWriteSharedStateHistory never publishes a pack itself. It first
// validates every existing chunk and plans the complete bounded candidate,
// then writes missing chunks/metadata to the atomic scope. The caller writes
// the returned pack LAST and discards the scope on any error.
func planAndWriteSharedStateHistory(db ethdb.KeyValueWriter, blockNum uint64, raw, baseline []byte, changes []*StateDomainChange, enabled bool) ([]byte, *stateHistorySharedWriteStats, error) {
	return planAndWriteSharedStateHistoryWithChunkWork(db, blockNum, raw, baseline, changes, enabled, nil)
}

func planAndWriteSharedStateHistoryWithChunkWork(db ethdb.KeyValueWriter, blockNum uint64, raw, baseline []byte, changes []*StateDomainChange, enabled bool, work *stateChangeChunkWork) ([]byte, *stateHistorySharedWriteStats, error) {
	if !enabled || !stateHistorySharedEligible(changes) || len(raw) > stateDomainChangeBlockMaxDecodedBytes {
		return baseline, nil, nil
	}
	store, ok := db.(StateHistoryChunkAtomicWriter)
	if !ok || !store.StateHistoryChunkWritesAtomic() {
		return baseline, nil, nil
	}
	sharedHistoryAttempts.Inc(1)
	started := time.Now()
	bucket := stateHistoryChunkBucket(blockNum)
	metaKey := stateHistoryChunkBucketKey(bucket)
	meta, exists, err := readPresentValue(store, metaKey, "shared history bucket")
	if err != nil {
		return nil, nil, err
	}
	if exists {
		if len(meta) != 2 || meta[0] != 1 || meta[1] > 1 {
			return nil, nil, fmt.Errorf("rawdb: invalid shared history bucket metadata %d", bucket)
		}
		if meta[1] == 1 {
			sharedHistoryRetired.Inc(1)
			return baseline, nil, nil
		}
	}
	var cuts []int
	var hashes [][32]byte
	if work.matches(raw) {
		cuts, hashes = work.cuts, work.hashes
	} else {
		var splitter historychunk.Splitter
		cuts = splitter.Cuts(raw, min(4, runtime.GOMAXPROCS(0)))
		if len(cuts) == 0 || cuts[len(cuts)-1] != len(raw) {
			cuts = append(cuts, len(raw))
		}
	}
	pack := append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...)
	pack = append(pack, stateDomainChangeBlockSharedVersion)
	pack = binary.AppendUvarint(pack, blockNum)
	pack = binary.AppendUvarint(pack, uint64(len(raw)))
	digest := sha256.Sum256(raw)
	pack = append(pack, digest[:]...)
	pack = binary.AppendUvarint(pack, uint64(len(cuts)))
	seen := make(map[[32]byte]stateHistoryPlannedChunk, len(cuts))
	missing := make([]stateHistoryPlannedChunk, 0, len(cuts))
	stats := &stateHistorySharedWriteStats{baseline: len(baseline)}
	start := 0
	for index, end := range cuts {
		chunk := raw[start:end]
		var hash [32]byte
		if hashes != nil {
			hash = hashes[index]
		} else {
			hash = sha256.Sum256(chunk)
		}
		pack = binary.AppendUvarint(pack, uint64(len(chunk)))
		pack = append(pack, hash[:]...)
		if prior, ok := seen[hash]; ok {
			if !bytes.Equal(raw[prior.start:prior.end], chunk) {
				return nil, nil, fmt.Errorf("rawdb: shared history digest collision")
			}
			stats.reused += len(chunk)
			start = end
			continue
		}
		key := stateHistoryChunkKey(bucket, hash)
		stored, found, err := readPresentValue(store, key, "shared history chunk")
		if err != nil {
			return nil, nil, err
		}
		entry := stateHistoryPlannedChunk{key: key, start: start, end: end}
		if found {
			decoded, err := decodeStateHistorySharedChunk(stored, len(chunk), hash)
			if err != nil {
				return nil, nil, err
			}
			if !bytes.Equal(decoded, chunk) {
				return nil, nil, fmt.Errorf("rawdb: shared history digest collision")
			}
			stats.reused += len(chunk)
		} else {
			entry.encoded = encodeStateHistorySharedChunk(chunk)
			missing = append(missing, entry)
			stats.chunks += len(entry.encoded)
			stats.chunkKeys += len(key)
		}
		seen[hash] = entry
		start = end
	}
	stats.pack, stats.newChunks = len(pack), len(missing)
	// The first version seeds reusable chunks. Allow at most 12.5% additional
	// KV bytes over the already-selected old codec, including chunk keys and
	// bucket metadata. Future packs reuse the seed; uncompressible unique data
	// cannot create an unbounded per-block expansion.
	cost := stats.pack + stats.chunks + stats.chunkKeys
	if !exists {
		stats.metadata = len(metaKey) + 2
		cost += stats.metadata
	}
	if cost*8 > len(baseline)*9 {
		sharedHistoryFallback.Inc(1)
		return baseline, nil, nil
	}
	for _, chunk := range missing {
		if err := store.Put(chunk.key, chunk.encoded); err != nil {
			return nil, nil, err
		}
	}
	if !exists {
		if err := store.Put(metaKey, []byte{1, 0}); err != nil {
			return nil, nil, err
		}
	}
	stats.work = time.Since(started)
	return pack, stats, nil
}

func encodeStateHistorySharedChunk(raw []byte) []byte {
	compressed := snappy.Encode(nil, raw)
	codec, payload := byte(0), raw
	if len(compressed) < len(raw) {
		codec, payload = 1, compressed
	}
	out := []byte{1, codec}
	out = binary.AppendUvarint(out, uint64(len(raw)))
	return append(out, payload...)
}

func decodeStateHistorySharedChunk(data []byte, want int, hash [32]byte) ([]byte, error) {
	if want <= 0 || want > historychunk.MaxSize || len(data) < 3 || len(data) > historychunk.MaxSize+binary.MaxVarintLen64+2 || data[0] != 1 || data[1] > 1 {
		return nil, fmt.Errorf("rawdb: invalid shared history chunk envelope")
	}
	n, used := binary.Uvarint(data[2:])
	if used <= 0 || n != uint64(want) {
		return nil, fmt.Errorf("rawdb: shared history chunk length mismatch")
	}
	payload := data[2+used:]
	var raw []byte
	if data[1] == 0 {
		if len(payload) != want {
			return nil, fmt.Errorf("rawdb: truncated shared history raw chunk")
		}
		raw = payload
	} else {
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil || decodedLen != want {
			return nil, fmt.Errorf("rawdb: invalid shared history Snappy size")
		}
		var errDecode error
		raw, errDecode = snappy.Decode(nil, payload)
		if errDecode != nil {
			return nil, fmt.Errorf("rawdb: corrupt shared history chunk: %w", errDecode)
		}
	}
	if sha256.Sum256(raw) != hash {
		return nil, fmt.Errorf("rawdb: shared history chunk hash mismatch")
	}
	return raw, nil
}

// sharedStateHistoryPackHeader validates allocation and I/O bounds before any
// database reads. The physical block is bound into the envelope, and derives
// the only bucket a reference may use.
func sharedStateHistoryPackHeader(data []byte, blockNum uint64) (refs []byte, decodedLen, count int, digest [32]byte, err error) {
	if !isStateHistorySharedPack(data) {
		err = fmt.Errorf("rawdb: not a shared history pack")
		return
	}
	p := data[len(stateDomainChangeBlockEnvelopeMagic)+1:]
	physical, n := binary.Uvarint(p)
	if n <= 0 || physical != blockNum {
		err = fmt.Errorf("rawdb: shared history pack block mismatch")
		return
	}
	p = p[n:]
	size, n := binary.Uvarint(p)
	if n <= 0 || size == 0 || size > stateDomainChangeBlockMaxDecodedBytes {
		err = fmt.Errorf("rawdb: invalid shared history decoded size")
		return
	}
	p = p[n:]
	if len(p) < len(digest) {
		err = fmt.Errorf("rawdb: truncated shared history digest")
		return
	}
	copy(digest[:], p[:32])
	p = p[32:]
	nChunks, n := binary.Uvarint(p)
	if n <= 0 || nChunks == 0 || nChunks > uint64(stateDomainChangeBlockMaxDecodedBytes/historychunk.MinSize+1) {
		err = fmt.Errorf("rawdb: invalid shared history chunk count")
		return
	}
	p = p[n:]
	if uint64(len(p)) < nChunks*33 || uint64(len(p)) > nChunks*(32+binary.MaxVarintLen64) {
		err = fmt.Errorf("rawdb: invalid shared history reference bytes")
		return
	}
	// Preflight the complete reference table: a corrupt tail cannot cause
	// thousands of reads or publish callbacks from a partially decoded block.
	scan, total := p, uint64(0)
	for i := uint64(0); i < nChunks; i++ {
		length, used := binary.Uvarint(scan)
		if used <= 0 || length == 0 || length > historychunk.MaxSize || i+1 < nChunks && length < historychunk.MinSize || len(scan)-used < 32 || length > size-total {
			err = fmt.Errorf("rawdb: invalid shared history reference length")
			return
		}
		total += length
		scan = scan[used+32:]
	}
	if len(scan) != 0 || total != size {
		err = fmt.Errorf("rawdb: shared history reference total mismatch")
		return
	}
	return p, int(size), int(nChunks), digest, nil
}

func decodeStateHistorySharedPack(db ethdb.KeyValueReader, data []byte, blockNum uint64) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("rawdb: shared history requires a coherent database reader")
	}
	refs, length, count, digest, err := sharedStateHistoryPackHeader(data, blockNum)
	if err != nil {
		return nil, err
	}
	bucket := stateHistoryChunkBucket(blockNum)
	decoded := make([]byte, 0, length)
	for i := 0; i < count; i++ {
		size, n := binary.Uvarint(refs)
		var hash [32]byte
		copy(hash[:], refs[n:n+32])
		refs = refs[n+32:]
		data, exists, err := readPresentValue(db, stateHistoryChunkKey(bucket, hash), "shared history chunk")
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("rawdb: missing shared history chunk in bucket %d", bucket)
		}
		raw, err := decodeStateHistorySharedChunk(data, int(size), hash)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, raw...)
	}
	if sha256.Sum256(decoded) != digest {
		return nil, fmt.Errorf("rawdb: shared history pack hash mismatch")
	}
	return decoded, nil
}
