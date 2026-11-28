package snapshots

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/klauspost/compress/zstd"
)

// recordColumnPageEncoder belongs to one serial writer or pipeline worker. Only key bytes
// differ between HRC2 modes; the remaining columns and literals are built once.
// Its returned bytes are borrowed until the next encode call.
type recordColumnPageEncoder struct {
	keys                   [3][]byte
	txs, lengths, flags    []byte
	literals, packed       []byte
	best, candidate        []byte
	header                 [historyRecordColumnHeader]byte
	tailStart, packedBytes int
}

func recordColumnReserve(p []byte, capacity int) []byte {
	if cap(p) < capacity {
		return make([]byte, 0, capacity)
	}
	return p[:0]
}

func (e *recordColumnPageEncoder) prepare(raw []byte, starts []uint32) error {
	if len(raw) == 0 || len(raw) > historyRecordColumnPageMax || len(starts) == 0 || len(starts) > len(raw)/21 {
		return errHistoryRecordColumns
	}
	n := len(starts)
	e.keys[0] = recordColumnReserve(e.keys[0], 5*n)
	e.keys[1] = recordColumnReserve(e.keys[1], 5*n)
	e.keys[2] = recordColumnReserve(e.keys[2], 4*n)[:4*n]
	e.txs = recordColumnReserve(e.txs, 10*n)
	e.lengths = recordColumnReserve(e.lengths, 5*n)
	e.flags = recordColumnReserve(e.flags, (n+7)/8)[:(n+7)/8]
	clear(e.flags)
	e.literals = recordColumnReserve(e.literals, len(raw))
	var end, prevTx uint64
	var prevKey uint32
	for i, offset := range starts {
		at := uint64(offset)
		if at < end || at+21 > uint64(len(raw)) || i > 0 && at != end {
			return errHistoryRecordColumns
		}
		h := raw[at : at+21]
		length := binary.BigEndian.Uint32(h[17:21])
		key := binary.BigEndian.Uint32(h[4:8])
		tx := binary.BigEndian.Uint64(h[8:16])
		if length > math.MaxUint32-17 || binary.BigEndian.Uint32(h[:4]) != 17+length || h[16] > 1 || tx < prevTx {
			return errHistoryRecordColumns
		}
		if i == 0 {
			e.literals = append(e.literals, raw[:at]...)
		}
		e.keys[0] = binary.AppendUvarint(e.keys[0], uint64(key))
		delta := int64(key) - int64(prevKey)
		e.keys[1] = binary.AppendUvarint(e.keys[1], uint64(delta<<1)^uint64(delta>>63))
		for b := 0; b < 4; b++ {
			e.keys[2][b*n+i] = h[4+b]
		}
		e.txs = binary.AppendUvarint(e.txs, tx-prevTx)
		e.lengths = binary.AppendUvarint(e.lengths, uint64(length))
		e.flags[i/8] |= h[16] << uint(i%8)
		next := at + 21 + uint64(length)
		e.literals = append(e.literals, raw[at+21:min(next, uint64(len(raw)))]...)
		if i == n-1 && next < uint64(len(raw)) {
			if uint64(len(raw))-next >= 21 {
				return errHistoryRecordColumns
			}
			e.literals = append(e.literals, raw[next:]...)
		}
		end, prevTx, prevKey = next, tx, key
	}
	maxKeys := max(len(e.keys[0]), len(e.keys[1]), len(e.keys[2]))
	e.tailStart = historyRecordColumnHeader + maxKeys
	e.packedBytes = e.tailStart + len(e.txs) + len(e.lengths) + len(e.flags) + len(e.literals)
	if e.packedBytes > len(raw)+historyRecordColumnHeader {
		return errHistoryRecordColumns
	}
	e.packed = recordColumnReserve(e.packed, e.packedBytes)[:e.packedBytes]
	at := e.tailStart
	for _, part := range [][]byte{e.txs, e.lengths, e.flags, e.literals} {
		at += copy(e.packed[at:], part)
	}
	clear(e.header[:])
	copy(e.header[:], "HRC2")
	for i, value := range []uint32{uint32(len(raw)), uint32(n), starts[0], 0, uint32(len(e.txs)), uint32(len(e.lengths)), 0} {
		binary.BigEndian.PutUint32(e.header[4+i*4:8+i*4], value)
	}
	binary.BigEndian.PutUint32(e.header[32:36], crc32.Checksum(raw, historyRecordColumnCRC))
	return nil
}

func (e *recordColumnPageEncoder) transformed(mode int) []byte {
	// End-align the varying key prefix. The common tail stays at one address;
	// changing modes neither rebuilds nor moves any previous-value bytes.
	start := e.tailStart - historyRecordColumnHeader - len(e.keys[mode])
	p := e.packed[start:e.packedBytes]
	copy(p[:historyRecordColumnHeader], e.header[:])
	binary.BigEndian.PutUint32(p[16:20], uint32(len(e.keys[mode])))
	binary.BigEndian.PutUint32(p[28:32], uint32(mode))
	copy(p[historyRecordColumnHeader:], e.keys[mode])
	return p
}

func recordColumnEncodeTo(enc *zstd.Encoder, data, dst []byte, mode byte) []byte {
	need := enc.MaxEncodedSize(len(data)) + 1
	if cap(dst) < need {
		dst = make([]byte, 1, need)
	} else {
		dst = dst[:1]
	}
	dst[0] = mode
	return enc.EncodeAll(data, dst)
}

// selectRecordColumnKeyMode picks the lowest-entropy key representation in O(N)
// without invoking Zstd multiple times per page.
func selectRecordColumnKeyMode(n, key0Len, key1Len int, key2 []byte) int {
	bestMode, bestScore := 0, key0Len
	if key1Len < bestScore {
		bestMode, bestScore = 1, key1Len
	}
	if n > 0 && len(key2) == 4*n {
		planeScore := 0
		for b := 0; b < 4; b++ {
			plane := key2[b*n : (b+1)*n]
			changes := 0
			for i := 1; i < len(plane); i++ {
				if plane[i] != plane[i-1] {
					changes++
				}
			}
			if changes > 0 {
				planeScore += changes + 1
			}
		}
		if planeScore < bestScore {
			bestMode = 2
		}
	}
	return bestMode
}

func (e *recordColumnPageEncoder) encode(raw []byte, starts []uint32, enc *zstd.Encoder) ([]byte, error) {
	if len(raw) == 0 || len(raw) > historyRecordColumnPageMax {
		return nil, errHistoryRecordColumns
	}
	if len(starts) == 0 {
		e.best = recordColumnEncodeTo(enc, raw, e.best, 0)
		return e.best, nil
	}
	if err := e.prepare(raw, starts); err != nil {
		return nil, err
	}
	mode := selectRecordColumnKeyMode(len(starts), len(e.keys[0]), len(e.keys[1]), e.keys[2])
	e.best = recordColumnEncodeTo(enc, e.transformed(mode), e.best, 1)
	if len(e.best) >= len(raw) {
		e.candidate = recordColumnEncodeTo(enc, raw, e.candidate, 0)
		if len(e.candidate) < len(e.best) {
			e.best, e.candidate = e.candidate, e.best
		}
	}
	return e.best, nil
}

// encodeOwned moves the winner to the caller. A queued result may outlive this
// worker's next encode, so only the other candidate remains in worker scratch.
// dst must be exclusively owned by the caller; it may become that candidate.
func (e *recordColumnPageEncoder) encodeOwned(raw []byte, starts []uint32, enc *zstd.Encoder, dst []byte) ([]byte, error) {
	e.best = dst
	stored, err := e.encode(raw, starts, enc)
	e.best = nil
	return stored, err
}
