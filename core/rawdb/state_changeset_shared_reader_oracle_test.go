package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Frozen complete shared reader from cda09766dfc6cce85253484430f4297d8751ea92.
// Only function names and calls between the five frozen functions are renamed.
// The unchanged database access helper, schema and format constants are shared.
func legacySharedHistoryReaderIsStateHistorySharedPack(data []byte) bool {
	return len(data) > len(stateDomainChangeBlockEnvelopeMagic) &&
		bytes.Equal(data[:len(stateDomainChangeBlockEnvelopeMagic)], stateDomainChangeBlockEnvelopeMagic[:]) &&
		data[len(stateDomainChangeBlockEnvelopeMagic)] == stateDomainChangeBlockSharedVersion
}

func legacySharedHistoryReaderDecodeStateHistorySharedChunk(data []byte, want int, hash [32]byte) ([]byte, error) {
	raw, err := legacySharedHistoryReaderDecodeStateHistorySharedChunkPayload(data, want)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(raw) != hash {
		return nil, fmt.Errorf("rawdb: shared history chunk hash mismatch")
	}
	return raw, nil
}

func legacySharedHistoryReaderDecodeStateHistorySharedChunkPayload(data []byte, want int) ([]byte, error) {
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
	return raw, nil
}

func legacySharedHistoryReaderSharedStateHistoryPackHeader(data []byte, blockNum uint64) (refs []byte, decodedLen, count int, digest [32]byte, err error) {
	if !legacySharedHistoryReaderIsStateHistorySharedPack(data) {
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

func legacySharedHistoryReaderDecodeStateHistorySharedPack(db ethdb.KeyValueReader, data []byte, blockNum uint64) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("rawdb: shared history requires a coherent database reader")
	}
	refs, length, count, digest, err := legacySharedHistoryReaderSharedStateHistoryPackHeader(data, blockNum)
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
		raw, err := legacySharedHistoryReaderDecodeStateHistorySharedChunk(data, int(size), hash)
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
