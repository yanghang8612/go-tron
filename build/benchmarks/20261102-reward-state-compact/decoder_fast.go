package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var fastAccountDescriptor = new(corepb.Account).ProtoReflect().Descriptor()

// decodeCompactSnapshotFast preserves the same experimental compact format and
// exact native-byte oracle. It appends children to the destination arena then
// backfills their old length prefixes, avoiding one allocation per native field.
func decodeCompactSnapshotFast(raw []byte) ([]byte, error) {
	if !bytes.HasPrefix(raw, rewardMagic) {
		return nil, fmt.Errorf("non-compact Account snapshot")
	}
	if len(raw) > (math.MaxInt-64)/3 {
		return nil, fmt.Errorf("oversized compact Account snapshot")
	}
	d := decoder{b: raw[len(rewardMagic):]}
	dst := append(make([]byte, 0, 3*len(raw)+64), nativeMagic...)
	out, e := decodeMessageFast(dst, &d, fastAccountDescriptor, 0)
	if e == nil {
		e = d.done()
	}
	return out, e
}

func reserveNativeBlob(dst []byte) ([]byte, int) {
	var zero [binary.MaxVarintLen64]byte
	pos := len(dst)
	return append(dst, zero[:]...), pos
}
func finishNativeBlob(dst []byte, pos int) ([]byte, int) {
	start := pos + binary.MaxVarintLen64
	n := binary.PutUvarint(dst[pos:start], uint64(len(dst)-start))
	copy(dst[pos+n:], dst[start:])
	return dst[:len(dst)-binary.MaxVarintLen64+n], pos + n
}

func decodeMessageFast(dst []byte, d *decoder, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
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
		dst = binary.AppendUvarint(dst, num)
		dst = append(dst, shape(fd)|byte(fd.Kind()))
		var pos int
		dst, pos = reserveNativeBlob(dst)
		dst, e = decodeFieldFast(dst, d, fd, depth+1)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", fd.FullName(), e)
		}
		dst, _ = finishNativeBlob(dst, pos)
	}
	u, e := d.blob()
	if e != nil {
		return nil, e
	}
	return appendBlob(dst, u), nil
}

func decodeFieldFast(dst []byte, d *decoder, fd protoreflect.FieldDescriptor, depth int) ([]byte, error) {
	if !fd.IsList() && !fd.IsMap() {
		return decodeScalarFast(dst, d, fd.Kind(), fd.Message(), depth)
	}
	n, e := d.varint()
	if e != nil || n == 0 || n > uint64(len(d.b)-d.i) {
		return nil, fmt.Errorf("invalid compact container count")
	}
	dst = binary.AppendUvarint(dst, n)
	var prev []byte
	for i := uint64(0); i < n; i++ {
		if fd.IsMap() {
			var pos, start int
			dst, pos = reserveNativeBlob(dst)
			dst, e = decodeScalarFast(dst, d, fd.MapKey().Kind(), fd.MapKey().Message(), depth)
			if e != nil {
				return nil, e
			}
			dst, start = finishNativeBlob(dst, pos)
			key := dst[start:]
			if prev != nil && bytes.Compare(prev, key) >= 0 {
				return nil, fmt.Errorf("invalid compact map key")
			}
			prev = key
		}
		valueField := fd
		if fd.IsMap() {
			valueField = fd.MapValue()
		}
		var pos int
		dst, pos = reserveNativeBlob(dst)
		dst, e = decodeScalarFast(dst, d, valueField.Kind(), valueField.Message(), depth)
		if e != nil {
			return nil, e
		}
		dst, _ = finishNativeBlob(dst, pos)
	}
	return dst, nil
}

func decodeScalarFast(dst []byte, d *decoder, kind protoreflect.Kind, md protoreflect.MessageDescriptor, depth int) ([]byte, error) {
	if kind == protoreflect.MessageKind {
		return decodeMessageFast(dst, d, md, depth)
	}
	return decodeScalar(dst, d, kind, md, depth)
}
