package main

// This is an OFFLINE research codec, not a supported database format. It
// transcodes the existing complete Account native value without semantic
// pruning, external references, or delta chains. The original bytes are the
// correctness oracle, including nested unknown fields and map/list order.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var nativeMagic = []byte{0, 'G', 'T', 'S', 'V', 1}
var rewardMagic = []byte{0, 'G', 'T', 'R', 'V', 1}

const maxDepth = 64

type decoder struct {
	b []byte
	i int
}

func (d *decoder) take(n uint64) ([]byte, error) {
	if n > uint64(len(d.b)-d.i) {
		return nil, fmt.Errorf("truncated value at %d: need %d", d.i, n)
	}
	b := d.b[d.i : d.i+int(n)]
	d.i += int(n)
	return b, nil
}
func (d *decoder) varint() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint at %d", d.i)
	}
	var tmp [10]byte
	if n != binary.PutUvarint(tmp[:], v) {
		return 0, fmt.Errorf("noncanonical varint at %d", d.i)
	}
	d.i += n
	return v, nil
}
func (d *decoder) blob() ([]byte, error) {
	n, e := d.varint()
	if e != nil {
		return nil, e
	}
	return d.take(n)
}
func (d *decoder) done() error {
	if d.i != len(d.b) {
		return fmt.Errorf("trailing %d bytes", len(d.b)-d.i)
	}
	return nil
}
func appendBlob(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}
func encodeCompactSnapshot(raw []byte) ([]byte, error) {
	if !bytes.HasPrefix(raw, nativeMagic) {
		return nil, fmt.Errorf("non-native Account snapshot")
	}
	d := decoder{b: raw[len(nativeMagic):]}
	out, e := encodeMessage(append([]byte(nil), rewardMagic...), &d, new(corepb.Account).ProtoReflect().Descriptor(), 0)
	if e == nil {
		e = d.done()
	}
	return out, e
}
func decodeCompactSnapshot(raw []byte) ([]byte, error) {
	if !bytes.HasPrefix(raw, rewardMagic) {
		return nil, fmt.Errorf("non-compact Account snapshot")
	}
	d := decoder{b: raw[len(rewardMagic):]}
	out, e := decodeMessage(append([]byte(nil), nativeMagic...), &d, new(corepb.Account).ProtoReflect().Descriptor(), 0)
	if e == nil {
		e = d.done()
	}
	return out, e
}
func shape(fd protoreflect.FieldDescriptor) byte {
	if fd.IsMap() {
		return 0x80
	}
	if fd.IsList() {
		return 0x40
	}
	return 0
}
func encodeMessage(dst []byte, d *decoder, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("message nesting exceeds limit")
	}
	n, e := d.varint()
	if e != nil || n > uint64(md.Fields().Len()) {
		return nil, fmt.Errorf("invalid field count %d: %v", n, e)
	}
	dst = binary.AppendUvarint(dst, n)
	var prev uint64
	for i := uint64(0); i < n; i++ {
		num, e := d.varint()
		if e != nil || num <= prev || num > math.MaxInt32 {
			return nil, fmt.Errorf("invalid native field %d: %v", num, e)
		}
		fd := md.Fields().ByNumber(protoreflect.FieldNumber(num))
		if fd == nil {
			return nil, fmt.Errorf("unknown native field %d", num)
		}
		t, e := d.take(1)
		if e != nil || t[0] != shape(fd)|byte(fd.Kind()) {
			return nil, fmt.Errorf("native type mismatch field %d", num)
		}
		payload, e := d.blob()
		if e != nil {
			return nil, e
		}
		dst = binary.AppendUvarint(dst, num-prev)
		prev = num
		dst, e = encodeField(dst, payload, fd, depth+1)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", fd.FullName(), e)
		}
	}
	u, e := d.blob()
	if e != nil {
		return nil, e
	}
	return appendBlob(dst, u), nil
}
func encodeField(dst, raw []byte, fd protoreflect.FieldDescriptor, depth int) ([]byte, error) {
	if !fd.IsList() && !fd.IsMap() {
		return encodeScalar(dst, raw, fd.Kind(), fd.Message(), depth)
	}
	d := decoder{b: raw}
	n, e := d.varint()
	if e != nil || n == 0 || n > uint64(len(raw)) {
		return nil, fmt.Errorf("invalid container count")
	}
	dst = binary.AppendUvarint(dst, n)
	var prev []byte
	for i := uint64(0); i < n; i++ {
		if fd.IsMap() {
			key, e := d.blob()
			if e != nil || (prev != nil && bytes.Compare(prev, key) >= 0) {
				return nil, fmt.Errorf("invalid native map key")
			}
			prev = key
			dst, e = encodeScalar(dst, key, fd.MapKey().Kind(), fd.MapKey().Message(), depth)
			if e != nil {
				return nil, e
			}
		}
		v, e := d.blob()
		if e != nil {
			return nil, e
		}
		valueField := fd
		if fd.IsMap() {
			valueField = fd.MapValue()
		}
		dst, e = encodeScalar(dst, v, valueField.Kind(), valueField.Message(), depth)
		if e != nil {
			return nil, e
		}
	}
	return dst, d.done()
}
func integerKind(k protoreflect.Kind) (signed, narrow, ok bool) {
	switch k {
	case protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return true, true, true
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return true, false, true
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return false, true, true
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return false, false, true
	}
	return false, false, false
}
func encodeScalar(dst, raw []byte, k protoreflect.Kind, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
	if signed, narrow, ok := integerKind(k); ok {
		if len(raw) != 8 {
			return nil, fmt.Errorf("native integer length %d", len(raw))
		}
		u := binary.BigEndian.Uint64(raw)
		if signed {
			v := int64(u)
			if narrow && (v < math.MinInt32 || v > math.MaxInt32) {
				return nil, fmt.Errorf("native signed integer exceeds 32 bits")
			}
			u = uint64(v<<1) ^ uint64(v>>63)
		} else if narrow && u > math.MaxUint32 {
			return nil, fmt.Errorf("native unsigned integer exceeds 32 bits")
		}
		return binary.AppendUvarint(dst, u), nil
	}
	switch k {
	case protoreflect.MessageKind:
		d := decoder{b: raw}
		out, e := encodeMessage(dst, &d, md, depth)
		if e == nil {
			e = d.done()
		}
		return out, e
	case protoreflect.StringKind:
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf("invalid UTF8")
		}
		return appendBlob(dst, raw), nil
	case protoreflect.BytesKind:
		return appendBlob(dst, raw), nil
	case protoreflect.BoolKind:
		if len(raw) != 1 || raw[0] > 1 {
			return nil, fmt.Errorf("invalid native bool")
		}
	case protoreflect.FloatKind:
		if len(raw) != 4 {
			return nil, fmt.Errorf("invalid native float")
		}
	case protoreflect.DoubleKind:
		if len(raw) != 8 {
			return nil, fmt.Errorf("invalid native double")
		}
	default:
		return nil, fmt.Errorf("unsupported kind %v", k)
	}
	return append(dst, raw...), nil
}
func decodeMessage(dst []byte, d *decoder, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("message nesting exceeds limit")
	}
	n, e := d.varint()
	if e != nil || n > uint64(md.Fields().Len()) {
		return nil, fmt.Errorf("invalid compact field count %d", n)
	}
	dst = binary.AppendUvarint(dst, n)
	var prev uint64
	for i := uint64(0); i < n; i++ {
		delta, e := d.varint()
		if e != nil || delta == 0 || delta > math.MaxInt32-prev {
			return nil, fmt.Errorf("invalid compact field delta")
		}
		num := prev + delta
		fd := md.Fields().ByNumber(protoreflect.FieldNumber(num))
		if fd == nil {
			return nil, fmt.Errorf("unknown compact field %d", num)
		}
		prev = num
		payload, e := decodeField(nil, d, fd, depth+1)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", fd.FullName(), e)
		}
		dst = binary.AppendUvarint(dst, num)
		dst = append(dst, shape(fd)|byte(fd.Kind()))
		dst = appendBlob(dst, payload)
	}
	u, e := d.blob()
	if e != nil {
		return nil, e
	}
	return appendBlob(dst, u), nil
}
func decodeField(dst []byte, d *decoder, fd protoreflect.FieldDescriptor, depth int) ([]byte, error) {
	if !fd.IsList() && !fd.IsMap() {
		return decodeScalar(dst, d, fd.Kind(), fd.Message(), depth)
	}
	n, e := d.varint()
	if e != nil || n == 0 || n > uint64(len(d.b)-d.i) {
		return nil, fmt.Errorf("invalid compact container count")
	}
	dst = binary.AppendUvarint(dst, n)
	var prev []byte
	for i := uint64(0); i < n; i++ {
		if fd.IsMap() {
			key, e := decodeScalar(nil, d, fd.MapKey().Kind(), fd.MapKey().Message(), depth)
			if e != nil || (prev != nil && bytes.Compare(prev, key) >= 0) {
				return nil, fmt.Errorf("invalid compact map key")
			}
			prev = key
			dst = appendBlob(dst, key)
		}
		valueField := fd
		if fd.IsMap() {
			valueField = fd.MapValue()
		}
		value, e := decodeScalar(nil, d, valueField.Kind(), valueField.Message(), depth)
		if e != nil {
			return nil, e
		}
		dst = appendBlob(dst, value)
	}
	return dst, nil
}
func decodeScalar(dst []byte, d *decoder, k protoreflect.Kind, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
	if signed, narrow, ok := integerKind(k); ok {
		u, e := d.varint()
		if e != nil {
			return nil, e
		}
		if signed {
			v := int64(u>>1) ^ -int64(u&1)
			if narrow && (v < math.MinInt32 || v > math.MaxInt32) {
				return nil, fmt.Errorf("compact signed integer exceeds 32 bits")
			}
			u = uint64(v)
		} else if narrow && u > math.MaxUint32 {
			return nil, fmt.Errorf("compact unsigned integer exceeds 32 bits")
		}
		return binary.BigEndian.AppendUint64(dst, u), nil
	}
	var raw []byte
	var e error
	switch k {
	case protoreflect.MessageKind:
		return decodeMessage(dst, d, md, depth)
	case protoreflect.StringKind, protoreflect.BytesKind:
		raw, e = d.blob()
		if e == nil && k == protoreflect.StringKind && !utf8.Valid(raw) {
			e = fmt.Errorf("invalid compact UTF8")
		}
	case protoreflect.BoolKind:
		raw, e = d.take(1)
		if e == nil && raw[0] > 1 {
			e = fmt.Errorf("invalid compact bool")
		}
	case protoreflect.FloatKind:
		raw, e = d.take(4)
	case protoreflect.DoubleKind:
		raw, e = d.take(8)
	default:
		e = fmt.Errorf("unsupported compact kind %v", k)
	}
	return append(dst, raw...), e
}
