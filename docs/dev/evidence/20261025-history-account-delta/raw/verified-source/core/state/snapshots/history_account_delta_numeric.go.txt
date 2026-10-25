package snapshots

import (
	"bytes"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/rlp"
)

type accountDeltaNumberField struct {
	bit     uint
	raw     []byte
	numeric bool
	number  uint64
}
type accountDeltaNumberCore struct {
	prefix, tail []byte
	fields       []accountDeltaNumberField
	numericMask  uint32
}

// Numeric patches are a reversible preconditioner for the existing V5/GTRC
// bytes. Unsupported cores and changes to opaque fields use ordinary patches.
// This parser never supplies defaults, discards fields or hydrates an account.
func parseAccountDeltaNumbers(value []byte) (c accountDeltaNumberCore, ok bool) {
	if len(value) > accountDeltaValueMax {
		return
	}
	payload, rest, e := rlp.SplitList(value)
	if e != nil || len(rest) != 0 {
		return
	}
	version, rest, e := rlp.SplitString(payload)
	if e != nil || len(version) != 1 || version[0] != 5 {
		return
	}
	core, tail, e := rlp.SplitString(rest)
	if e != nil || len(core) < 9 || string(core[:4]) != "GTRC" || (core[4] != 1 && core[4] != 2) {
		return
	}
	mask := binary.BigEndian.Uint32(core[5:9])
	if mask>>29 != 0 {
		return
	}
	c.prefix = core[:9]
	c.tail = tail
	r := accountDeltaParser{core[9:]}
	const byteFields = uint32(1<<0 | 1<<2 | 1<<13 | 1<<16 | 1<<17 | 1<<21 | 1<<24 | 1<<27)
	const flagFields = uint32(1<<8 | 1<<14 | 1<<15 | 1<<23 | 1<<28)
	for bit := uint(0); bit < 29; bit++ {
		if mask&(1<<bit) == 0 || flagFields&(1<<bit) != 0 {
			continue
		}
		before := r.p
		n, e := r.number()
		if e != nil {
			return accountDeltaNumberCore{}, false
		}
		numeric := byteFields&(1<<bit) == 0
		if !numeric {
			if n > uint64(len(r.p)) {
				return accountDeltaNumberCore{}, false
			}
			if _, e = r.take(int(n)); e != nil {
				return accountDeltaNumberCore{}, false
			}
		} else {
			c.numericMask |= 1 << bit
		}
		c.fields = append(c.fields, accountDeltaNumberField{bit, before[:len(before)-len(r.p)], numeric, n})
	}
	if len(r.p) != 0 || !bytes.Equal(buildAccountDeltaNumbers(c, nil), value) {
		return accountDeltaNumberCore{}, false
	}
	return c, true
}
func wrapAccountDeltaRLP(dst, p []byte, list bool) []byte {
	if !list && len(p) == 1 && p[0] < 0x80 {
		return append(dst, p...)
	}
	tag := byte(0x80)
	if list {
		tag = 0xc0
	}
	if len(p) < 56 {
		dst = append(dst, tag+byte(len(p)))
	} else {
		n := len(p)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		i := 0
		for b[i] == 0 {
			i++
		}
		dst = append(dst, tag+55+byte(8-i))
		dst = append(dst, b[i:]...)
	}
	return append(dst, p...)
}
func buildAccountDeltaNumbers(c accountDeltaNumberCore, numbers map[uint]uint64) []byte {
	core := append([]byte(nil), c.prefix...)
	for _, f := range c.fields {
		if n, ok := numbers[f.bit]; f.numeric && ok {
			core = binary.AppendUvarint(core, n)
		} else {
			core = append(core, f.raw...)
		}
	}
	payload := []byte{5}
	payload = wrapAccountDeltaRLP(payload, core, false)
	payload = append(payload, c.tail...)
	return wrapAccountDeltaRLP(nil, payload, true)
}
func accountDeltaUnzigBits(x uint64) uint64 { return x>>1 ^ (0 - x&1) }
func accountDeltaZigBits(x uint64) uint64   { return x<<1 ^ uint64(int64(x)>>63) }
func accountDeltaNumericPatch(base, value []byte) []byte {
	a, ok := parseAccountDeltaNumbers(base)
	if !ok {
		return nil
	}
	b, ok := parseAccountDeltaNumbers(value)
	if !ok || !bytes.Equal(a.prefix, b.prefix) || !bytes.Equal(a.tail, b.tail) || len(a.fields) != len(b.fields) {
		return nil
	}
	var mask uint32
	var changes []byte
	for i, f := range a.fields {
		g := b.fields[i]
		if f.numeric {
			if f.number != g.number {
				mask |= 1 << f.bit
				difference := accountDeltaUnzigBits(g.number) - accountDeltaUnzigBits(f.number)
				changes = binary.AppendUvarint(changes, accountDeltaZigBits(difference))
			}
		} else if !bytes.Equal(f.raw, g.raw) {
			return nil
		}
	}
	if mask == 0 {
		return nil
	}
	out := binary.AppendUvarint([]byte{2}, uint64(mask))
	return append(out, changes...)
}
func decodeAccountDeltaNumericPatch(r *accountDeltaParser, base []byte) ([]byte, error) {
	c, ok := parseAccountDeltaNumbers(base)
	if !ok {
		return nil, errAccountDelta
	}
	mask, e := r.number()
	if e != nil || mask == 0 || mask > uint64(^uint32(0)) || uint32(mask)&^c.numericMask != 0 {
		return nil, errAccountDelta
	}
	numbers := make(map[uint]uint64)
	for _, f := range c.fields {
		if uint32(mask)&(1<<f.bit) == 0 {
			continue
		}
		delta, e := r.number()
		if e != nil || delta == 0 {
			return nil, errAccountDelta
		}
		bits := accountDeltaUnzigBits(f.number) + accountDeltaUnzigBits(delta)
		numbers[f.bit] = accountDeltaZigBits(bits)
	}
	out := buildAccountDeltaNumbers(c, numbers)
	if len(out) > accountDeltaValueMax {
		return nil, errAccountDelta
	}
	return out, nil
}
