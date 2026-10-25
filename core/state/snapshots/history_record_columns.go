package snapshots

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// Experimental page transform, deliberately not emitted by production writers.
// Every canonical byte is reconstructed within one existing 128 KiB read unit.
const historyRecordColumnHeader = 36
const historyRecordColumnPageMax = 128 << 10

var historyRecordColumnCRC = crc32.MakeTable(crc32.Castagnoli)

var errHistoryRecordColumns = errors.New("snapshots: invalid history record columns")

// starts lists every complete V6 record header in this page. A payload or a
// header crossing a page boundary stays opaque; no value parsing is required.
func encodeHistoryRecordColumns(raw []byte, starts []uint32, mode uint32) ([]byte, error) {
	if len(raw) == 0 || len(raw) > historyRecordColumnPageMax || len(starts) == 0 || len(starts) > len(raw)/21 || mode > 2 {
		return nil, errHistoryRecordColumns
	}
	var keys, txs, lengths, literals []byte
	if mode == 2 {
		keys = make([]byte, 4*len(starts))
	}
	flags := make([]byte, (len(starts)+7)/8)
	var end uint64
	var prevTx uint64
	var prevKey uint32
	for i, o := range starts {
		at := uint64(o)
		if at < end || at+21 > uint64(len(raw)) || i > 0 && at != end {
			return nil, errHistoryRecordColumns
		}
		h := raw[at : at+21]
		n := binary.BigEndian.Uint32(h[17:21])
		key := binary.BigEndian.Uint32(h[4:8])
		tx := binary.BigEndian.Uint64(h[8:16])
		if n > math.MaxUint32-17 || binary.BigEndian.Uint32(h[:4]) != 17+n || h[16] > 1 || tx < prevTx {
			return nil, errHistoryRecordColumns
		}
		if i == 0 {
			literals = append(literals, raw[:at]...)
		}
		switch mode {
		case 0:
			keys = binary.AppendUvarint(keys, uint64(key))
		case 1:
			d := int64(key) - int64(prevKey)
			keys = binary.AppendUvarint(keys, uint64(d<<1)^uint64(d>>63))
		case 2:
			for b := 0; b < 4; b++ {
				keys[b*len(starts)+i] = h[4+b]
			}
		}
		txs = binary.AppendUvarint(txs, tx-prevTx)
		lengths = binary.AppendUvarint(lengths, uint64(n))
		flags[i/8] |= h[16] << uint(i%8)
		next := at + 21 + uint64(n)
		payloadEnd := min(next, uint64(len(raw)))
		literals = append(literals, raw[at+21:payloadEnd]...)
		if i == len(starts)-1 && next < uint64(len(raw)) {
			if uint64(len(raw))-next >= 21 {
				return nil, errHistoryRecordColumns
			}
			literals = append(literals, raw[next:]...)
		}
		end = next
		prevTx = tx
		prevKey = key
	}
	out := make([]byte, historyRecordColumnHeader)
	copy(out, "HRC2")
	for i, n := range []uint32{uint32(len(raw)), uint32(len(starts)), starts[0], uint32(len(keys)), uint32(len(txs)), uint32(len(lengths)), mode} {
		binary.BigEndian.PutUint32(out[4+i*4:8+i*4], n)
	}
	binary.BigEndian.PutUint32(out[32:36], crc32.Checksum(raw, historyRecordColumnCRC))
	out = append(out, keys...)
	out = append(out, txs...)
	out = append(out, lengths...)
	out = append(out, flags...)
	out = append(out, literals...)
	return out, nil
}

func historyRecordColumnNumber(p *[]byte) (uint64, error) {
	v, n := binary.Uvarint(*p)
	if n <= 0 {
		return 0, errHistoryRecordColumns
	}
	if n > 1 && (*p)[n-1] == 0 {
		return 0, errHistoryRecordColumns
	}
	*p = (*p)[n:]
	return v, nil
}

func decodeHistoryRecordColumns(p []byte, expected uint64, dst []byte) ([]byte, error) {
	if len(p) < historyRecordColumnHeader || len(p) > 2*historyRecordColumnPageMax+historyRecordColumnHeader || string(p[:4]) != "HRC2" || expected == 0 || expected > historyRecordColumnPageMax {
		return nil, errHistoryRecordColumns
	}
	get := func(i int) uint64 { return uint64(binary.BigEndian.Uint32(p[4+i*4 : 8+i*4])) }
	n, count, first, kl, tl, ll, mode := get(0), get(1), get(2), get(3), get(4), get(5), get(6)
	fl := (count + 7) / 8
	if n != expected || count == 0 || count > n/21 || first > n-21 || mode > 2 || kl > count*5 || tl > count*10 || ll > count*5 || mode == 2 && kl != count*4 || historyRecordColumnHeader+kl+tl+ll+fl > uint64(len(p)) {
		return nil, errHistoryRecordColumns
	}
	rest := p[historyRecordColumnHeader:]
	keys := rest[:kl]
	rest = rest[kl:]
	txs := rest[:tl]
	rest = rest[tl:]
	lengths := rest[:ll]
	rest = rest[ll:]
	flags := rest[:fl]
	literal := rest[fl:]
	if uint64(len(literal)) != n-count*21 || first > uint64(len(literal)) || count%8 != 0 && flags[len(flags)-1]>>uint(count%8) != 0 {
		return nil, errHistoryRecordColumns
	}
	if uint64(cap(dst)) < n {
		dst = make([]byte, int(n))
	} else {
		dst = dst[:int(n)]
	}
	copy(dst, literal[:first])
	position := first
	literal = literal[first:]
	var tx uint64
	var key uint32
	for i := uint64(0); i < count; i++ {
		var value uint64
		var e error
		switch mode {
		case 0:
			value, e = historyRecordColumnNumber(&keys)
			if e != nil || value > math.MaxUint32 {
				return nil, errHistoryRecordColumns
			}
			key = uint32(value)
		case 1:
			value, e = historyRecordColumnNumber(&keys)
			if e != nil || value > 2*uint64(math.MaxUint32) {
				return nil, errHistoryRecordColumns
			}
			d := int64(value>>1) ^ -int64(value&1)
			next := int64(key) + d
			if next < 0 || next > math.MaxUint32 {
				return nil, errHistoryRecordColumns
			}
			key = uint32(next)
		case 2:
			key = uint32(keys[i])<<24 | uint32(keys[count+i])<<16 | uint32(keys[2*count+i])<<8 | uint32(keys[3*count+i])
		}
		delta, e := historyRecordColumnNumber(&txs)
		if e != nil || delta > math.MaxUint64-tx {
			return nil, errHistoryRecordColumns
		}
		tx += delta
		length, e := historyRecordColumnNumber(&lengths)
		if e != nil || length > math.MaxUint32-17 || position+21 > n {
			return nil, errHistoryRecordColumns
		}
		header := dst[position : position+21]
		binary.BigEndian.PutUint32(header[:4], uint32(17+length))
		binary.BigEndian.PutUint32(header[4:8], key)
		binary.BigEndian.PutUint64(header[8:16], tx)
		header[16] = (flags[i/8] >> uint(i%8)) & 1
		binary.BigEndian.PutUint32(header[17:21], uint32(length))
		position += 21
		take := length
		if i+1 == count {
			take = n - position
			if take > length+20 {
				return nil, errHistoryRecordColumns
			}
		}
		if take > uint64(len(literal)) || take > n-position {
			return nil, errHistoryRecordColumns
		}
		copy(dst[position:position+take], literal[:take])
		position += take
		literal = literal[take:]
	}
	if mode != 2 && len(keys) != 0 || len(txs) != 0 || len(lengths) != 0 || len(literal) != 0 || position != n {
		return nil, errHistoryRecordColumns
	}
	if crc32.Checksum(dst, historyRecordColumnCRC) != binary.BigEndian.Uint32(p[32:36]) {
		return nil, errHistoryRecordColumns
	}
	return dst, nil
}
