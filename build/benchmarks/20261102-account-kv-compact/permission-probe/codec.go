package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/tronprotocol/go-tron/common"
)

var oldMagic = []byte{'G', 'T', 'R', 'A', 1, 1}
var newMagic = []byte{'G', 'T', 'P', 'M', 1}

const (
	modeSelf      = 1
	modeOwner     = 2
	modeWitness   = 3
	modeActiveID  = 4
	modeActiveRaw = 5
)

var immutableOps = [][]byte{nil, func() []byte { v := make([]byte, 32); copy(v, []byte{0x7f, 0xff, 0x1f, 0xc0, 0x03, 0x3e}); return v }(), make([]byte, 32)}

type dec struct {
	data []byte
	off  int
}

func (d *dec) word() (uint64, error) {
	if d.off >= len(d.data) {
		return 0, fmt.Errorf("truncated word")
	}
	v, n := binary.Uvarint(d.data[d.off:])
	if n <= 0 {
		return 0, fmt.Errorf("bad word")
	}
	var b [10]byte
	if binary.PutUvarint(b[:], v) != n {
		return 0, fmt.Errorf("noncanonical word")
	}
	d.off += n
	return v, nil
}
func (d *dec) blob() ([]byte, error) {
	n, e := d.word()
	if e != nil {
		return nil, e
	}
	if n > uint64(len(d.data)-d.off) {
		return nil, fmt.Errorf("truncated blob")
	}
	v := d.data[d.off : d.off+int(n)]
	d.off += int(n)
	return v, nil
}
func (d *dec) done() error {
	if d.off != len(d.data) {
		return fmt.Errorf("trailing data")
	}
	return nil
}
func blob(dst, v []byte) []byte { return append(binary.AppendUvarint(dst, uint64(len(v))), v...) }
func zig(v int64) uint64        { return uint64(v<<1) ^ uint64(v>>63) }
func signed(v uint64) int64     { return int64(v>>1) ^ -int64(v&1) }

type span struct{ from, to int }
type parsed struct {
	typ, id, parent    int32
	name, ops, unknown []byte
	threshold          int64
	keys               uint64
	keyUnknown         bool
	self               []span
}

func parse(owner common.Address, value []byte) (p parsed, err error) {
	if !bytes.HasPrefix(value, oldMagic) {
		return p, fmt.Errorf("not GTRA permission v1")
	}
	d := dec{data: value, off: len(oldMagic)}
	i32 := func() (int32, error) {
		v, e := d.word()
		if e != nil {
			return 0, e
		}
		n := signed(v)
		if n < math.MinInt32 || n > math.MaxInt32 {
			return 0, fmt.Errorf("int32 overflow")
		}
		return int32(n), nil
	}
	if p.typ, err = i32(); err != nil {
		return p, err
	}
	if p.id, err = i32(); err != nil {
		return p, err
	}
	if p.name, err = d.blob(); err != nil {
		return p, err
	}
	if !utf8.Valid(p.name) {
		return p, fmt.Errorf("bad UTF8")
	}
	threshold, e := d.word()
	if e != nil {
		return p, e
	}
	p.threshold = signed(threshold)
	if p.parent, err = i32(); err != nil {
		return p, err
	}
	if p.ops, err = d.blob(); err != nil {
		return p, err
	}
	if p.keys, err = d.word(); err != nil {
		return p, err
	}
	if p.keys > uint64(len(value)-d.off) {
		return p, fmt.Errorf("bad key count")
	}
	for i := uint64(0); i < p.keys; i++ {
		if d.off >= len(value) || value[d.off] > 1 {
			return p, fmt.Errorf("bad key presence")
		}
		present := value[d.off]
		d.off++
		if present == 0 {
			continue
		}
		address, e := d.blob()
		if e != nil {
			return p, e
		}
		if bytes.Equal(address, owner[:]) {
			p.self = append(p.self, span{d.off - len(address), d.off})
		}
		if _, e = d.word(); e != nil {
			return p, e
		}
		unknown, e := d.blob()
		if e != nil {
			return p, e
		}
		p.keyUnknown = p.keyUnknown || len(unknown) > 0
	}
	if p.unknown, err = d.blob(); err != nil {
		return p, err
	}
	return p, d.done()
}
func template(owner common.Address, kind byte, ops []byte) []byte {
	typ, id, name := int64(0), int64(0), "owner"
	if kind == modeWitness {
		typ, id, name = 1, 1, "witness"
	}
	if kind == modeActiveID || kind == modeActiveRaw {
		typ, id, name = 2, 2, "active"
	}
	v := append([]byte(nil), oldMagic...)
	v = binary.AppendUvarint(v, zig(typ))
	v = binary.AppendUvarint(v, zig(id))
	v = blob(v, []byte(name))
	v = binary.AppendUvarint(v, zig(1))
	v = binary.AppendUvarint(v, 0)
	v = blob(v, ops)
	v = append(v, 1, 1)
	v = blob(v, owner[:])
	v = binary.AppendUvarint(v, zig(1))
	return append(v, 0, 0)
}
func compact(owner common.Address, key, value []byte) ([]byte, string, parsed, error) {
	p, e := parse(owner, value)
	if e != nil {
		return nil, "", p, e
	}
	if len(p.unknown) > 0 || p.keyUnknown {
		return value, "raw_unknown_fallback", p, nil
	}
	for _, entry := range []struct {
		key  []byte
		mode byte
	}{{[]byte{0}, modeOwner}, {[]byte{1}, modeWitness}} {
		if bytes.Equal(key, entry.key) && bytes.Equal(value, template(owner, entry.mode, nil)) {
			return append(append([]byte(nil), newMagic...), entry.mode), map[byte]string{modeOwner: "default_owner", modeWitness: "default_witness"}[entry.mode], p, nil
		}
	}
	if bytes.Equal(key, []byte{2, 0, 0, 0, 2}) && len(p.ops) == 32 && bytes.Equal(value, template(owner, modeActiveRaw, p.ops)) {
		for id := 1; id < len(immutableOps); id++ {
			if bytes.Equal(p.ops, immutableOps[id]) {
				v := append(append([]byte(nil), newMagic...), modeActiveID, byte(id))
				return v, fmt.Sprintf("default_active_immutable_ops_%d", id), p, nil
			}
		}
		v := append(append([]byte(nil), newMagic...), modeActiveRaw)
		return append(v, p.ops...), "default_active_exact_raw_ops", p, nil
	}
	if len(p.self) > 0 {
		v := append(append([]byte(nil), newMagic...), modeSelf)
		v = binary.AppendUvarint(v, uint64(len(p.self)))
		previous := 0
		for _, s := range p.self {
			v = blob(v, value[previous:s.from])
			previous = s.to
		}
		v = blob(v, value[previous:])
		if len(v) < len(value) {
			return v, "self_only_custom", p, nil
		}
	}
	return value, "raw_fallback", p, nil
}
func expand(owner common.Address, value []byte) ([]byte, error) {
	if !bytes.HasPrefix(value, newMagic) {
		return value, nil
	}
	if len(value) < 6 {
		return nil, fmt.Errorf("truncated permission template")
	}
	mode := value[5]
	d := dec{data: value, off: 6}
	switch mode {
	case modeOwner, modeWitness:
		if e := d.done(); e != nil {
			return nil, e
		}
		return template(owner, mode, nil), nil
	case modeActiveID:
		if len(value) != 7 || value[6] == 0 || int(value[6]) >= len(immutableOps) {
			return nil, fmt.Errorf("unknown immutable operation bitmap")
		}
		return template(owner, mode, immutableOps[value[6]]), nil
	case modeActiveRaw:
		if len(value) != 38 {
			return nil, fmt.Errorf("active exact bitmap length")
		}
		return template(owner, mode, value[6:]), nil
	case modeSelf:
		count, e := d.word()
		if e != nil || count == 0 || count > uint64(len(value)) {
			return nil, fmt.Errorf("bad SELF count")
		}
		var restored []byte
		for i := uint64(0); i < count; i++ {
			gap, e := d.blob()
			if e != nil {
				return nil, e
			}
			if len(restored)+len(gap)+21 > 16<<20 {
				return nil, fmt.Errorf("expanded value bound")
			}
			restored = append(restored, gap...)
			restored = append(restored, owner[:]...)
		}
		tail, e := d.blob()
		if e != nil {
			return nil, e
		}
		restored = append(restored, tail...)
		if e = d.done(); e != nil {
			return nil, e
		}
		p, e := parse(owner, restored)
		if e != nil || uint64(len(p.self)) != count {
			return nil, fmt.Errorf("invalid reconstructed SELF permission: %v", e)
		}
		return restored, nil
	default:
		return nil, fmt.Errorf("unknown permission mode")
	}
}
