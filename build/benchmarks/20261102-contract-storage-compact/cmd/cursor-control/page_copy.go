package main

// This is an offline format experiment, not an accepted rawdb schema. An SST
// entry's key is the last complete physical key in a page. Its value contains
// at most 128 tails and the exact pre-existing wrapped values. Logical keys,
// presence and canonical bytes can be recovered without consulting another
// page or a global dictionary.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

type row struct{ key, value []byte }

const (
	pageHeader  = 10
	maxPageRows = 128
	storageTail = 16
)

func encodePage(rows []row) ([]byte, error) {
	if len(rows) == 0 || len(rows) > maxPageRows {
		return nil, fmt.Errorf("invalid page row count %d", len(rows))
	}
	keyLen := len(rows[0].key)
	if keyLen < storageTail {
		return nil, fmt.Errorf("short key")
	}
	prefix := rows[0].key[:keyLen-storageTail]
	dict := make([][]byte, 0, len(rows))
	ids := make([]byte, len(rows))
	values := map[string]byte{}
	plainBytes, uniqueBytes := 0, 0
	for i, r := range rows {
		if len(r.key) != keyLen || !bytes.Equal(prefix, r.key[:keyLen-storageTail]) || i > 0 && bytes.Compare(rows[i-1].key, r.key) >= 0 {
			return nil, fmt.Errorf("page keys not strictly sorted with shared prefix")
		}
		if len(r.value) > 65535 {
			return nil, fmt.Errorf("value too long")
		}
		plainBytes += len(r.value)
		id, ok := values[string(r.value)]
		if !ok {
			id = byte(len(dict))
			values[string(r.value)] = id
			dict = append(dict, r.value)
			uniqueBytes += len(r.value)
		}
		ids[i] = id
	}
	useDict := len(rows)+2*(len(dict)+1)+uniqueBytes < 2*(len(rows)+1)+plainBytes
	n := len(rows)
	m := n
	payload := plainBytes
	if useDict {
		m = len(dict)
		payload = uniqueBytes
	}
	if payload > 65535 {
		return nil, fmt.Errorf("page payload exceeds bounded u16 offsets")
	}
	shared := 0
	firstTail, lastTail := rows[0].key[keyLen-storageTail:], rows[n-1].key[keyLen-storageTail:]
	for shared < storageTail && firstTail[shared] == lastTail[shared] {
		shared++
	}
	width := storageTail - shared
	length := pageHeader + n*width + 2*(m+1) + payload
	if useDict {
		length += n
	}
	out := make([]byte, length)
	copy(out, "SPG2")
	binary.LittleEndian.PutUint16(out[4:6], uint16(n))
	binary.LittleEndian.PutUint16(out[6:8], uint16(m))
	if useDict {
		out[8] = 1
	}
	out[9] = byte(shared)
	pos := pageHeader
	for _, r := range rows {
		copy(out[pos:pos+width], r.key[keyLen-width:])
		pos += width
	}
	if useDict {
		copy(out[pos:pos+n], ids)
		pos += n
	}
	offsets := out[pos : pos+2*(m+1)]
	pos += len(offsets)
	dataPos := 0
	for i := 0; i < m; i++ {
		binary.LittleEndian.PutUint16(offsets[2*i:], uint16(dataPos))
		v := rows[i].value
		if useDict {
			v = dict[i]
		}
		copy(out[pos+dataPos:], v)
		dataPos += len(v)
	}
	binary.LittleEndian.PutUint16(offsets[2*m:], uint16(dataPos))
	return out, nil
}

type pageView struct {
	n, m, width                int
	keys, ids, offsets, values []byte
}

func pageLayout(b []byte) (pageView, error) {
	var p pageView
	if len(b) < pageHeader || string(b[:4]) != "SPG2" {
		return p, fmt.Errorf("invalid page header")
	}
	p.n = int(binary.LittleEndian.Uint16(b[4:6]))
	p.m = int(binary.LittleEndian.Uint16(b[6:8]))
	flag := b[8]
	shared := int(b[9])
	p.width = storageTail - shared
	if p.n < 1 || p.n > maxPageRows || p.m < 1 || p.m > p.n || flag > 1 || flag == 0 && p.m != p.n || shared > storageTail || p.width == 0 && p.n > 1 {
		return p, fmt.Errorf("invalid page counts/flag")
	}
	need := pageHeader + p.n*p.width + 2*(p.m+1)
	if flag == 1 {
		need += p.n
	}
	if need > len(b) {
		return p, fmt.Errorf("truncated page")
	}
	pos := pageHeader
	p.keys = b[pos : pos+p.n*p.width]
	pos += len(p.keys)
	if flag == 1 {
		p.ids = b[pos : pos+p.n]
		pos += p.n
	}
	p.offsets = b[pos : pos+2*(p.m+1)]
	pos += len(p.offsets)
	p.values = b[pos:]
	if binary.LittleEndian.Uint16(p.offsets) != 0 || int(binary.LittleEndian.Uint16(p.offsets[2*p.m:])) != len(p.values) {
		return p, fmt.Errorf("page offsets do not close")
	}
	return p, nil
}

// Full validation is used on every newly encoded page and by the offline row
// oracle. Point lookup, like the underlying SST iterator, relies on the
// writer's sorted-key invariant and only decodes/checks the selected row.
func parsePage(b []byte) (pageView, error) {
	p, e := pageLayout(b)
	if e != nil {
		return p, e
	}
	for i := 0; i < p.m; i++ {
		if binary.LittleEndian.Uint16(p.offsets[2*i:]) > binary.LittleEndian.Uint16(p.offsets[2*i+2:]) {
			return p, fmt.Errorf("unordered offsets")
		}
	}
	for i := 1; i < p.n; i++ {
		if bytes.Compare(p.keys[(i-1)*p.width:i*p.width], p.keys[i*p.width:(i+1)*p.width]) >= 0 {
			return p, fmt.Errorf("unordered page tails")
		}
	}
	for _, id := range p.ids {
		if int(id) >= p.m {
			return p, fmt.Errorf("dictionary id outside page")
		}
	}
	return p, nil
}

func (p pageView) value(i int) []byte {
	if len(p.ids) > 0 {
		i = int(p.ids[i])
	}
	a := int(binary.LittleEndian.Uint16(p.offsets[2*i:]))
	b := int(binary.LittleEndian.Uint16(p.offsets[2*i+2:]))
	return p.values[a:b]
}

func lookupPage(pageKey, b, key []byte) ([]byte, bool, error) {
	if len(b) > 0 && b[0] <= 3 {
		if e := rawdb.ValidateStateKVLatestValue(b); e != nil {
			return nil, false, e
		}
		if !bytes.Equal(pageKey, key) {
			return nil, false, nil
		}
		return b, true, nil
	}
	if len(pageKey) != len(key) || len(key) < storageTail || !bytes.Equal(pageKey[:len(key)-storageTail], key[:len(key)-storageTail]) {
		return nil, false, nil
	}
	p, e := pageLayout(b)
	if e != nil {
		return nil, false, e
	}
	if !bytes.Equal(pageKey[:len(pageKey)-p.width], key[:len(key)-p.width]) {
		return nil, false, nil
	}
	if !bytes.Equal(pageKey[len(pageKey)-p.width:], p.keys[(p.n-1)*p.width:]) {
		return nil, false, fmt.Errorf("page key does not match last row")
	}
	tail := key[len(key)-p.width:]
	i := sort.Search(p.n, func(i int) bool { return bytes.Compare(p.keys[i*p.width:(i+1)*p.width], tail) >= 0 })
	if i == p.n || !bytes.Equal(p.keys[i*p.width:(i+1)*p.width], tail) {
		return nil, false, nil
	}
	id := i
	if len(p.ids) > 0 {
		id = int(p.ids[i])
		if id >= p.m {
			return nil, false, fmt.Errorf("dictionary id outside page")
		}
	}
	a := int(binary.LittleEndian.Uint16(p.offsets[2*id:]))
	end := int(binary.LittleEndian.Uint16(p.offsets[2*id+2:]))
	if a > end || end > len(p.values) {
		return nil, false, fmt.Errorf("selected value outside page")
	}
	return p.values[a:end], true, nil
}
