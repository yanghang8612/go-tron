package main

// Experimental physical representation only. Expanding it reproduces the exact
// old GTRA v1 bytes; it is not wired into rooted state or its MuHash source.
import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var resourceV1 = []byte{'G', 'T', 'R', 'A', 1, 5}
var resourceV2 = []byte{'G', 'T', 'R', 'A', 2, 5}

type resourceFields struct {
	words                       [10]uint64 // Original canonical ZigZag words, not arithmetic values.
	frozen                      bool
	frozenBalance, frozenExpiry uint64
	frozenUnknown, unknown      []byte
	optimized                   bool
}

type decoder struct {
	data []byte
	off  int
}

func (d *decoder) word() (uint64, error) {
	if d.off >= len(d.data) {
		return 0, fmt.Errorf("truncated word")
	}
	v, n := binary.Uvarint(d.data[d.off:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid word")
	}
	var b [10]byte
	if binary.PutUvarint(b[:], v) != n {
		return 0, fmt.Errorf("noncanonical word")
	}
	d.off += n
	return v, nil
}
func (d *decoder) blob() ([]byte, error) {
	n, err := d.word()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(d.data)-d.off) {
		return nil, fmt.Errorf("truncated blob")
	}
	v := d.data[d.off : d.off+int(n)]
	d.off += int(n)
	return v, nil
}
func (d *decoder) boolean() (bool, error) {
	if d.off == len(d.data) || d.data[d.off] > 1 {
		return false, fmt.Errorf("invalid boolean")
	}
	v := d.data[d.off] == 1
	d.off++
	return v, nil
}
func (d *decoder) done() error {
	if d.off != len(d.data) {
		return fmt.Errorf("trailing bytes")
	}
	return nil
}
func parseResourceV1(value []byte) (r resourceFields, err error) {
	if !bytes.HasPrefix(value, resourceV1) {
		return r, fmt.Errorf("not resource v1")
	}
	d := decoder{data: value, off: len(resourceV1)}
	if r.words[0], err = d.word(); err != nil {
		return r, err
	}
	if r.frozen, err = d.boolean(); err != nil {
		return r, err
	}
	if r.frozen {
		if r.frozenBalance, err = d.word(); err != nil {
			return r, err
		}
		if r.frozenExpiry, err = d.word(); err != nil {
			return r, err
		}
		if r.frozenUnknown, err = d.blob(); err != nil {
			return r, err
		}
	}
	for i := 1; i < len(r.words); i++ {
		if r.words[i], err = d.word(); err != nil {
			return r, err
		}
	}
	if r.optimized, err = d.boolean(); err != nil {
		return r, err
	}
	if r.unknown, err = d.blob(); err != nil {
		return r, err
	}
	return r, d.done()
}
func appendBlob(dst, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
func encodeResourceV1(r resourceFields) []byte {
	dst := append([]byte(nil), resourceV1...)
	dst = binary.AppendUvarint(dst, r.words[0])
	if r.frozen {
		dst = append(dst, 1)
		dst = binary.AppendUvarint(dst, r.frozenBalance)
		dst = binary.AppendUvarint(dst, r.frozenExpiry)
		dst = appendBlob(dst, r.frozenUnknown)
	} else {
		dst = append(dst, 0)
	}
	for _, v := range r.words[1:] {
		dst = binary.AppendUvarint(dst, v)
	}
	if r.optimized {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return appendBlob(dst, r.unknown)
}
func compactResource(value []byte) ([]byte, error) {
	r, err := parseResourceV1(value)
	if err != nil {
		return nil, err
	}
	var mask uint16
	for i, v := range r.words {
		if v != 0 {
			mask |= 1 << i
		}
	}
	if r.frozen {
		mask |= 1 << 10
	}
	if r.frozenBalance != 0 {
		mask |= 1 << 11
	}
	if r.frozenExpiry != 0 {
		mask |= 1 << 12
	}
	if r.optimized {
		mask |= 1 << 13
	}
	if len(r.unknown) > 0 {
		mask |= 1 << 14
	}
	if len(r.frozenUnknown) > 0 {
		mask |= 1 << 15
	}
	dst := append([]byte(nil), resourceV2...)
	dst = binary.LittleEndian.AppendUint16(dst, mask)
	for _, v := range r.words {
		if v != 0 {
			dst = binary.AppendUvarint(dst, v)
		}
	}
	if r.frozenBalance != 0 {
		dst = binary.AppendUvarint(dst, r.frozenBalance)
	}
	if r.frozenExpiry != 0 {
		dst = binary.AppendUvarint(dst, r.frozenExpiry)
	}
	if len(r.unknown) > 0 {
		dst = appendBlob(dst, r.unknown)
	}
	if len(r.frozenUnknown) > 0 {
		dst = appendBlob(dst, r.frozenUnknown)
	}
	return dst, nil
}
func expandResource(value []byte) ([]byte, error) {
	if len(value) < 8 || !bytes.HasPrefix(value, resourceV2) {
		return nil, fmt.Errorf("not resource v2")
	}
	mask := binary.LittleEndian.Uint16(value[6:8])
	d := decoder{data: value, off: 8}
	r := resourceFields{frozen: mask&(1<<10) != 0, optimized: mask&(1<<13) != 0}
	if !r.frozen && mask&((1<<11)|(1<<12)|(1<<15)) != 0 {
		return nil, fmt.Errorf("frozen fields without message")
	}
	nonzero := func() (uint64, error) {
		v, e := d.word()
		if e == nil && v == 0 {
			e = fmt.Errorf("zero in nonzero field")
		}
		return v, e
	}
	var err error
	for i := range r.words {
		if mask&(1<<i) != 0 {
			if r.words[i], err = nonzero(); err != nil {
				return nil, err
			}
		}
	}
	if mask&(1<<11) != 0 {
		if r.frozenBalance, err = nonzero(); err != nil {
			return nil, err
		}
	}
	if mask&(1<<12) != 0 {
		if r.frozenExpiry, err = nonzero(); err != nil {
			return nil, err
		}
	}
	nonempty := func() ([]byte, error) {
		v, e := d.blob()
		if e == nil && len(v) == 0 {
			e = fmt.Errorf("empty flagged blob")
		}
		return v, e
	}
	if mask&(1<<14) != 0 {
		if r.unknown, err = nonempty(); err != nil {
			return nil, err
		}
	}
	if mask&(1<<15) != 0 {
		if r.frozenUnknown, err = nonempty(); err != nil {
			return nil, err
		}
	}
	if err = d.done(); err != nil {
		return nil, err
	}
	return encodeResourceV1(r), nil
}
