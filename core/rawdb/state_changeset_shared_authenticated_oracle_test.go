package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Frozen full planner and decoder oracle from 5915d3cbf8d051488efe63e7c81785619951700f.
// Only function names and the planner's decoder target are renamed.
func legacySharedAuthenticatedChunkPlanner(db ethdb.KeyValueWriter, blockNum uint64, raw, baseline []byte, changes []*StateDomainChange, enabled bool, work *stateChangeChunkWork) ([]byte, *stateHistorySharedWriteStats, error) {
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
			decoded, err := legacySharedAuthenticatedChunkDecoder(stored, len(chunk), hash)
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

func legacySharedAuthenticatedChunkDecoder(data []byte, want int, hash [32]byte) ([]byte, error) {
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
