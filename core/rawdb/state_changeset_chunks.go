package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"runtime"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const stateDomainChangeBlockChunksVersion = byte(2)

var stateChangeBlockChunkEncoding atomic.Bool

// SetStateHistoryBlockDedup enables the additional foreground encoding work for
// repeated large values. Readers always support it. Runtime defaults to disabled
// until a representative replay establishes the import throughput tradeoff.
func SetStateHistoryBlockDedup(enabled bool) { stateChangeBlockChunkEncoding.Store(enabled) }

// Only repeated large values in one atomic block pack pay CDC/hash work. The
// usual small-row path and a block with just one large delegation list retain
// Snappy. No reference escapes a pack, so prune/unwind need no new dependencies.
func stateChangeBlockHasLargeVersions(changes []*StateDomainChange) bool {
	type identity struct {
		flat       StateFlatDomain
		owner      common.Address
		generation uint64
		domain     kvdomains.KVDomain
		key        string
	}
	var seen map[identity]bool
	for _, c := range changes {
		if !c.PrevExists || len(c.Prev) < historychunk.MaxSize {
			continue
		}
		if seen == nil {
			seen = make(map[identity]bool)
		}
		id := identity{c.FlatDomain, c.Owner, c.Generation, c.Domain, string(c.Key)}
		if seen[id] {
			return true
		}
		seen[id] = true
	}
	return false
}

func encodeStateDomainChangeBlockStorageForChanges(raw []byte, changes []*StateDomainChange) ([]byte, bool) {
	if stateChangeBlockChunkEncoding.Load() && len(raw) >= 2<<20 && len(raw) <= stateDomainChangeBlockMaxDecodedBytes && stateChangeBlockHasLargeVersions(changes) {
		baseline, compressed := encodeStateDomainChangeBlockStorage(raw)
		if encoded, useful := encodeStateChangeChunks(raw); useful && len(encoded)*8 < len(baseline)*7 {
			return encoded, true
		}
		return baseline, compressed
	}
	return encodeStateDomainChangeBlockStorage(raw)
}

type stateChangeChunkAnchor struct{ offset, size, index int }

// The payload starts with decoded-size and CRC, followed by raw (0), Snappy
// (1), or full-anchor reference (2) chunks. Anchor equality is checked in bytes,
// not inferred from a digest. Input already lives in the block's RLP buffer;
// dictionary entries borrow that buffer and add no copies of large Prev values.
func encodeStateChangeChunks(raw []byte) ([]byte, bool) {
	return encodeStateChangeChunksWithWorkers(raw, min(4, runtime.GOMAXPROCS(0)))
}

func encodeStateChangeChunksWithWorkers(raw []byte, workers int) ([]byte, bool) {
	out := make([]byte, 0, min(len(raw), 1<<20))
	out = append(out, stateDomainChangeBlockEnvelopeMagic[:]...)
	out = append(out, stateDomainChangeBlockChunksVersion)
	out = binary.AppendUvarint(out, uint64(len(raw)))
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(raw))
	anchors := make(map[[32]byte]stateChangeChunkAnchor)
	var splitter historychunk.Splitter
	cuts := splitter.Cuts(raw, workers)
	if len(raw) > 0 && (len(cuts) == 0 || cuts[len(cuts)-1] != len(raw)) {
		cuts = append(cuts, len(raw))
	}
	var scratch []byte
	offset := 0
	for index, end := range cuts {
		n := end - offset
		chunk := raw[offset : offset+n]
		digest := sha256.Sum256(chunk)
		prior, found := anchors[digest]
		if found && prior.size == n && bytes.Equal(raw[prior.offset:prior.offset+n], chunk) {
			out = append(out, 2)
			out = binary.AppendUvarint(out, uint64(n))
			out = binary.AppendUvarint(out, uint64(prior.index))
		} else {
			scratch = snappy.Encode(scratch, chunk)
			if len(scratch)+binary.MaxVarintLen64 < n {
				out = append(out, 1)
				out = binary.AppendUvarint(out, uint64(n))
				out = binary.AppendUvarint(out, uint64(len(scratch)))
				out = append(out, scratch...)
			} else {
				out = append(out, 0)
				out = binary.AppendUvarint(out, uint64(n))
				out = append(out, chunk...)
			}
			anchors[digest] = stateChangeChunkAnchor{offset, n, index}
		}
		offset += n
	}
	if len(out)*8 >= len(raw)*7 {
		return nil, false
	}
	return out, true
}

func decodeStateChangeChunks(dst, payload []byte) ([]byte, error) {
	bad := func() ([]byte, error) {
		return nil, fmt.Errorf("rawdb: invalid compressed state domain change chunk pack")
	}
	size, n := binary.Uvarint(payload)
	if n <= 0 || size == 0 || size > stateDomainChangeBlockMaxDecodedBytes {
		return bad()
	}
	payload = payload[n:]
	if len(payload) < 4 {
		return bad()
	}
	wantCRC := binary.LittleEndian.Uint32(payload)
	payload = payload[4:]
	if cap(dst) < int(size) {
		dst = make([]byte, int(size))
	} else {
		dst = dst[:int(size)]
	}
	anchors := make([]stateChangeChunkAnchor, 0, int(size)/historychunk.MinSize+1)
	readUint := func() (uint64, bool) {
		v, n := binary.Uvarint(payload)
		if n <= 0 {
			return 0, false
		}
		payload = payload[n:]
		return v, true
	}
	for offset := 0; offset < len(dst); {
		if len(payload) == 0 {
			return bad()
		}
		kind := payload[0]
		payload = payload[1:]
		length, ok := readUint()
		if !ok || length == 0 || length > historychunk.MaxSize || length > uint64(len(dst)-offset) || (length < historychunk.MinSize && length != uint64(len(dst)-offset)) {
			return bad()
		}
		end := offset + int(length)
		anchor := stateChangeChunkAnchor{offset: offset, size: int(length)}
		switch kind {
		case 0:
			if length > uint64(len(payload)) {
				return bad()
			}
			copy(dst[offset:end], payload[:int(length)])
			payload = payload[int(length):]
		case 1:
			stored, ok := readUint()
			if !ok || stored == 0 || stored > uint64(len(payload)) {
				return bad()
			}
			compressed := payload[:int(stored)]
			decodedLen, err := snappy.DecodedLen(compressed)
			if err != nil || decodedLen != int(length) {
				return bad()
			}
			if _, err := snappy.Decode(dst[offset:end], compressed); err != nil {
				return bad()
			}
			payload = payload[int(stored):]
		case 2:
			ref, ok := readUint()
			if !ok || ref >= uint64(len(anchors)) {
				return bad()
			}
			prior := anchors[ref]
			if prior.size != int(length) {
				return bad()
			} // Ref-to-ref entries have size zero.
			copy(dst[offset:end], dst[prior.offset:prior.offset+prior.size])
			anchor.size = 0
		default:
			return bad()
		}
		anchors = append(anchors, anchor)
		offset = end
	}
	if len(payload) != 0 || crc32.ChecksumIEEE(dst) != wantCRC {
		return bad()
	}
	return dst, nil
}

func decodeCompressedStateChangeBlock(dst, payload []byte, version byte) ([]byte, error) {
	if version == stateDomainChangeBlockChunksVersion {
		return decodeStateChangeChunks(dst, payload)
	}
	return snappy.Decode(dst, payload)
}
