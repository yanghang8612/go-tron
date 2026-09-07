package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

// The experiment retains the complete original prefix and each 21-byte frame
// header. A tag after each header chooses literal, delta, or earlier-value ID.
// A deployment needs a new format version and rebuilt offset/anchor locators.
func encode(prefix []byte, rows []record, codec string, checkpoint int, result *codecResult) []byte {
	out := append([]byte("gtvbench"), 0)
	if codec == "same-key-copy-literal" {
		out[8] = 1
	}
	out = binary.AppendUvarint(out, uint64(len(prefix)))
	out = append(out, prefix...)
	previous := map[uint32][]byte{}
	counts := map[uint32]int{}
	unique := map[[32]byte][]int{}
	values := [][]byte{}
	for _, r := range rows {
		out = append(out, r.header...)
		mode := byte(0)
		var payload []byte
		if codec == "segment-exact-dedup" {
			h := hashValue(r.value)
			for _, id := range unique[h] {
				if bytes.Equal(values[id], r.value) {
					mode = 2
					payload = binary.AppendUvarint(nil, uint64(id))
					break
				}
			}
			if mode == 0 {
				unique[h] = append(unique[h], len(values))
				values = append(values, r.value)
			}
		} else if base, ok := previous[r.key]; ok && counts[r.key]%checkpoint != 0 {
			if codec == "same-key-copy-literal" {
				payload = copyDelta(base, r.value)
			} else {
				p, s := commonEnds(base, r.value)
				payload = binary.AppendUvarint(nil, uint64(p))
				payload = binary.AppendUvarint(payload, uint64(s))
				payload = append(payload, r.value[p:len(r.value)-s]...)
			}
			// Include the encoded patch-length overhead in the literal fallback.
			if len(payload)+uvarintLen(uint64(len(payload))) < len(r.value) {
				mode = 1
			}
		}
		out = append(out, mode)
		switch mode {
		case 0:
			out = append(out, r.value...)
			result.RawRecords++
		case 1:
			out = binary.AppendUvarint(out, uint64(len(payload)))
			out = append(out, payload...)
			result.DeltaRecords++
		case 2:
			out = append(out, payload...)
			result.ReferenceRecords++
		}
		previous[r.key] = r.value
		counts[r.key]++
	}
	return out
}

func decode(encoded []byte, maxRaw int) ([]byte, error) {
	if len(encoded) < 9 || string(encoded[:8]) != "gtvbench" || encoded[8] > 1 {
		return nil, errors.New("invalid experiment header")
	}
	copyMode := encoded[8] == 1
	data := encoded[9:]
	prefixN, err := takeUint(&data)
	if err != nil {
		return nil, err
	}
	prefix, err := take(&data, prefixN)
	if err != nil {
		return nil, err
	}
	if len(prefix) > maxRaw {
		return nil, errors.New("decoded limit")
	}
	out := append(make([]byte, 0, maxRaw), prefix...)
	previous := map[uint32][]byte{}
	values := [][]byte{}
	for len(data) > 0 {
		header, err := take(&data, 21)
		if err != nil {
			return nil, err
		}
		size := uint64(binary.BigEndian.Uint32(header[17:21]))
		key := binary.BigEndian.Uint32(header[4:8])
		tag, err := take(&data, 1)
		if err != nil {
			return nil, err
		}
		if len(out) > maxRaw-21 || size > uint64(maxRaw-len(out)-21) {
			return nil, errors.New("decoded limit")
		}
		var v []byte
		switch tag[0] {
		case 0:
			v, err = take(&data, size)
			if err == nil {
				values = append(values, v)
			}
		case 1:
			n, e := takeUint(&data)
			if e != nil {
				return nil, e
			}
			patch, e := take(&data, n)
			if e != nil {
				return nil, e
			}
			base, ok := previous[key]
			if !ok {
				return nil, errors.New("missing delta base")
			}
			if copyMode {
				v, err = applyCopyDelta(base, patch, int(size))
			} else {
				p, e := takeUint(&patch)
				if e != nil {
					return nil, e
				}
				s, e := takeUint(&patch)
				if e != nil {
					return nil, e
				}
				if p > uint64(len(base)) || s > uint64(len(base))-p || p+s+uint64(len(patch)) != size {
					return nil, errors.New("invalid prefix suffix patch")
				}
				v = append(v, base[:p]...)
				v = append(v, patch...)
				v = append(v, base[uint64(len(base))-s:]...)
			}
		case 2:
			id, e := takeUint(&data)
			if e != nil {
				return nil, e
			}
			if id >= uint64(len(values)) {
				return nil, errors.New("invalid value reference")
			}
			v = values[id]
		default:
			return nil, errors.New("invalid record tag")
		}
		if err != nil {
			return nil, err
		}
		if uint64(len(v)) != size {
			return nil, errors.New("value length mismatch")
		}
		out = append(out, header...)
		out = append(out, v...)
		previous[key] = out[len(out)-len(v):]
	}
	return out, nil
}

// Eight-byte base anchors, minimum 16-byte copies. This catches multiple stable
// interior fields even when an earlier varint changes width. Large bases fall
// back to literal to bound transient hash-map overhead during sampling.
func copyDelta(base, value []byte) []byte {
	if len(base) > 8<<20 {
		return append([]byte{0}, value...)
	}
	anchors := map[uint64]int{}
	for i := 0; i+8 <= len(base); i += 8 {
		h := binary.LittleEndian.Uint64(base[i : i+8])
		if _, ok := anchors[h]; !ok {
			anchors[h] = i
		}
	}
	var out []byte
	literal := 0
	for i := 0; i+16 <= len(value); {
		off, ok := anchors[binary.LittleEndian.Uint64(value[i:i+8])]
		n := 0
		if ok {
			n = 8
			for off+n < len(base) && i+n < len(value) && base[off+n] == value[i+n] {
				n++
			}
		}
		if n < 16 {
			i++
			continue
		}
		if i > literal {
			out = append(out, 0)
			out = binary.AppendUvarint(out, uint64(i-literal))
			out = append(out, value[literal:i]...)
		}
		out = append(out, 1)
		out = binary.AppendUvarint(out, uint64(off))
		out = binary.AppendUvarint(out, uint64(n))
		i += n
		literal = i
	}
	if literal < len(value) {
		out = append(out, 0)
		out = binary.AppendUvarint(out, uint64(len(value)-literal))
		out = append(out, value[literal:]...)
	}
	return out
}

func applyCopyDelta(base, patch []byte, size int) ([]byte, error) {
	out := make([]byte, 0, size)
	for len(patch) > 0 {
		tag := patch[0]
		patch = patch[1:]
		n, err := takeUint(&patch)
		if err != nil {
			return nil, err
		}
		if tag == 0 {
			v, e := take(&patch, n)
			if e != nil {
				return nil, e
			}
			if len(v) > size-len(out) {
				return nil, errors.New("literal overflow")
			}
			out = append(out, v...)
		} else if tag == 1 {
			length, e := takeUint(&patch)
			if e != nil {
				return nil, e
			}
			if n > uint64(len(base)) || length > uint64(len(base))-n || length > uint64(size-len(out)) {
				return nil, errors.New("copy overflow")
			}
			out = append(out, base[n:n+length]...)
		} else {
			return nil, errors.New("invalid patch tag")
		}
	}
	if len(out) != size {
		return nil, errors.New("patch length mismatch")
	}
	return out, nil
}

func take(data *[]byte, n uint64) ([]byte, error) {
	if n > uint64(len(*data)) {
		return nil, io.ErrUnexpectedEOF
	}
	v := (*data)[:n]
	*data = (*data)[n:]
	return v, nil
}
func takeUint(data *[]byte) (uint64, error) {
	n, k := binary.Uvarint(*data)
	if k <= 0 {
		return 0, errors.New("invalid varint")
	}
	*data = (*data)[k:]
	return n, nil
}
func uvarintLen(n uint64) int {
	k := 1
	for n >= 128 {
		n >>= 7
		k++
	}
	return k
}
func hashValue(v []byte) [32]byte { return sha256.Sum256(v) }
