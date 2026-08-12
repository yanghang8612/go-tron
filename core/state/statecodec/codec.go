// Package statecodec implements the native, versioned encoding used for
// protobuf-shaped values in rooted state.  The in-memory API remains the
// generated protobuf API because it is part of go-tron's public and actuator
// surface; the durable representation is deliberately independent of the
// protobuf wire format.
package statecodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The leading zero is not a legal protobuf tag, so corrupt or accidentally
// protobuf-encoded rooted rows fail closed. The last byte is the durable
// format version.
var magic = [...]byte{0, 'G', 'T', 'S', 'V', 1}

const (
	shapeScalar byte = 0
	shapeList   byte = 0x40
	shapeMap    byte = 0x80
)

// IsNative reports whether data uses the versioned native state encoding.
func IsNative(data []byte) bool {
	return len(data) >= len(magic) && bytes.Equal(data[:len(magic)], magic[:])
}

// Marshal deterministically encodes msg without using protobuf wire encoding.
func Marshal(msg proto.Message) ([]byte, error) {
	if msg == nil || !msg.ProtoReflect().IsValid() {
		return nil, errors.New("statecodec: nil message")
	}
	out := append(make([]byte, 0, 128), magic[:]...)
	body, err := appendMessage(nil, msg.ProtoReflect())
	if err != nil {
		return nil, err
	}
	return append(out, body...), nil
}

// Unmarshal decodes a native rooted-state row. Protobuf wire data is rejected:
// this build only supports databases produced by a genesis sync with the
// native codec enabled.
func Unmarshal(data []byte, msg proto.Message) error {
	if msg == nil || !msg.ProtoReflect().IsValid() {
		return errors.New("statecodec: nil message")
	}
	if !IsNative(data) {
		return errors.New("statecodec: non-native rooted-state value")
	}
	proto.Reset(msg)
	if err := consumeMessage(data[len(magic):], msg.ProtoReflect()); err != nil {
		return err
	}
	return nil
}

type encodedField struct {
	num     protoreflect.FieldNumber
	shape   byte
	kind    protoreflect.Kind
	payload []byte
}

func appendMessage(dst []byte, msg protoreflect.Message) ([]byte, error) {
	fields := make([]encodedField, 0, msg.Descriptor().Fields().Len())
	var rangeErr error
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if fd.IsExtension() {
			rangeErr = fmt.Errorf("statecodec: extension field %d is unsupported", fd.Number())
			return false
		}
		shape := shapeScalar
		var payload []byte
		var err error
		switch {
		case fd.IsMap():
			shape = shapeMap
			payload, err = appendMap(nil, fd, value.Map())
		case fd.IsList():
			shape = shapeList
			payload, err = appendList(nil, fd, value.List())
		default:
			payload, err = appendScalar(nil, fd.Kind(), value)
		}
		if err != nil {
			rangeErr = fmt.Errorf("statecodec: encode %s: %w", fd.FullName(), err)
			return false
		}
		fields = append(fields, encodedField{fd.Number(), shape, fd.Kind(), payload})
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].num < fields[j].num })
	dst = binary.AppendUvarint(dst, uint64(len(fields)))
	for _, field := range fields {
		dst = binary.AppendUvarint(dst, uint64(field.num))
		dst = append(dst, field.shape|kindCode(field.kind))
		dst = binary.AppendUvarint(dst, uint64(len(field.payload)))
		dst = append(dst, field.payload...)
	}
	// Preserve generated-message unknown fields exactly. They remain opaque to
	// the current schema, but are framed by this codec rather than interpreted as
	// part of the native field stream.
	unknown := msg.GetUnknown()
	dst = binary.AppendUvarint(dst, uint64(len(unknown)))
	dst = append(dst, unknown...)
	return dst, nil
}

func appendList(dst []byte, fd protoreflect.FieldDescriptor, list protoreflect.List) ([]byte, error) {
	dst = binary.AppendUvarint(dst, uint64(list.Len()))
	for i := 0; i < list.Len(); i++ {
		value, err := appendScalar(nil, fd.Kind(), list.Get(i))
		if err != nil {
			return nil, err
		}
		dst = binary.AppendUvarint(dst, uint64(len(value)))
		dst = append(dst, value...)
	}
	return dst, nil
}

type mapEntry struct{ key, value []byte }

func appendMap(dst []byte, fd protoreflect.FieldDescriptor, m protoreflect.Map) ([]byte, error) {
	entries := make([]mapEntry, 0, m.Len())
	var rangeErr error
	m.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		encodedKey, err := appendScalar(nil, fd.MapKey().Kind(), key.Value())
		if err != nil {
			rangeErr = err
			return false
		}
		encodedValue, err := appendScalar(nil, fd.MapValue().Kind(), value)
		if err != nil {
			rangeErr = err
			return false
		}
		entries = append(entries, mapEntry{encodedKey, encodedValue})
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for _, entry := range entries {
		dst = binary.AppendUvarint(dst, uint64(len(entry.key)))
		dst = append(dst, entry.key...)
		dst = binary.AppendUvarint(dst, uint64(len(entry.value)))
		dst = append(dst, entry.value...)
	}
	return dst, nil
}

func appendScalar(dst []byte, kind protoreflect.Kind, value protoreflect.Value) ([]byte, error) {
	var fixed [8]byte
	switch kind {
	case protoreflect.BoolKind:
		if value.Bool() {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case protoreflect.EnumKind:
		binary.BigEndian.PutUint64(fixed[:], uint64(int64(value.Enum())))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		binary.BigEndian.PutUint64(fixed[:], uint64(value.Int()))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		binary.BigEndian.PutUint64(fixed[:], value.Uint())
	case protoreflect.FloatKind:
		binary.BigEndian.PutUint32(fixed[:4], math.Float32bits(float32(value.Float())))
		return append(dst, fixed[:4]...), nil
	case protoreflect.DoubleKind:
		binary.BigEndian.PutUint64(fixed[:], math.Float64bits(value.Float()))
	case protoreflect.StringKind:
		if !utf8.ValidString(value.String()) {
			return nil, errors.New("invalid UTF-8 string")
		}
		return append(dst, value.String()...), nil
	case protoreflect.BytesKind:
		return append(dst, value.Bytes()...), nil
	case protoreflect.MessageKind:
		return appendMessage(dst, value.Message())
	default:
		return nil, fmt.Errorf("unsupported kind %s", kind)
	}
	return append(dst, fixed[:]...), nil
}

func consumeMessage(data []byte, msg protoreflect.Message) error {
	count, rest, err := consumeUvarint(data)
	if err != nil {
		return fmt.Errorf("statecodec: field count: %w", err)
	}
	data = rest
	if count > uint64(msg.Descriptor().Fields().Len()) {
		return fmt.Errorf("statecodec: field count %d exceeds schema size", count)
	}
	var previous protoreflect.FieldNumber
	for i := uint64(0); i < count; i++ {
		number, tail, err := consumeUvarint(data)
		if err != nil {
			return fmt.Errorf("statecodec: field number: %w", err)
		}
		data = tail
		if number < uint64(protowire.MinValidNumber) || number > uint64(protowire.MaxValidNumber) {
			return fmt.Errorf("statecodec: field number %d is outside the protobuf range", number)
		}
		if len(data) == 0 {
			return errors.New("statecodec: missing field type")
		}
		typeCode := data[0]
		data = data[1:]
		length, tail, err := consumeUvarint(data)
		if err != nil {
			return fmt.Errorf("statecodec: field length: %w", err)
		}
		data = tail
		if length > uint64(len(data)) {
			return errors.New("statecodec: truncated field")
		}
		payload := data[:int(length)]
		data = data[int(length):]
		fd := msg.Descriptor().Fields().ByNumber(protoreflect.FieldNumber(number))
		if fd == nil {
			return fmt.Errorf("statecodec: field %d absent from schema", number)
		}
		if fd.Number() <= previous {
			return errors.New("statecodec: fields are not strictly ordered")
		}
		previous = fd.Number()
		if typeCode&0x3f != kindCode(fd.Kind()) {
			return fmt.Errorf("statecodec: field %s kind mismatch", fd.FullName())
		}
		switch typeCode & 0xc0 {
		case shapeMap:
			if !fd.IsMap() {
				return fmt.Errorf("statecodec: field %s is not a map", fd.FullName())
			}
			err = consumeMap(payload, fd, msg.Mutable(fd).Map())
		case shapeList:
			if !fd.IsList() || fd.IsMap() {
				return fmt.Errorf("statecodec: field %s is not a list", fd.FullName())
			}
			err = consumeList(payload, fd, msg.Mutable(fd).List())
		case shapeScalar:
			if fd.IsList() || fd.IsMap() {
				return fmt.Errorf("statecodec: field %s has scalar shape", fd.FullName())
			}
			err = setScalar(payload, fd.Kind(), msg, fd)
		default:
			err = fmt.Errorf("statecodec: invalid shape %#x", typeCode&0xc0)
		}
		if err != nil {
			return fmt.Errorf("statecodec: decode %s: %w", fd.FullName(), err)
		}
	}
	unknownLen, rest, err := consumeUvarint(data)
	if err != nil {
		return fmt.Errorf("statecodec: unknown length: %w", err)
	}
	if unknownLen != uint64(len(rest)) {
		return errors.New("statecodec: malformed unknown field trailer")
	}
	msg.SetUnknown(append([]byte(nil), rest...))
	return nil
}

func consumeList(data []byte, fd protoreflect.FieldDescriptor, list protoreflect.List) error {
	count, data, err := consumeUvarint(data)
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("non-canonical empty list field")
	}
	for i := uint64(0); i < count; i++ {
		payload, rest, err := consumeBytes(data)
		if err != nil {
			return err
		}
		data = rest
		if fd.Kind() == protoreflect.MessageKind {
			value := list.NewElement()
			if err := consumeMessage(payload, value.Message()); err != nil {
				return err
			}
			list.Append(value)
			continue
		}
		value, err := consumeScalar(payload, fd.Kind())
		if err != nil {
			return err
		}
		list.Append(value)
	}
	if len(data) != 0 {
		return errors.New("trailing list data")
	}
	return nil
}

func consumeMap(data []byte, fd protoreflect.FieldDescriptor, m protoreflect.Map) error {
	count, data, err := consumeUvarint(data)
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("non-canonical empty map field")
	}
	var previousKey []byte
	for i := uint64(0); i < count; i++ {
		keyData, rest, err := consumeBytes(data)
		if err != nil {
			return err
		}
		valueData, rest2, err := consumeBytes(rest)
		if err != nil {
			return err
		}
		data = rest2
		if previousKey != nil && bytes.Compare(previousKey, keyData) >= 0 {
			return errors.New("map keys are not strictly ordered")
		}
		previousKey = keyData
		key, err := consumeScalar(keyData, fd.MapKey().Kind())
		if err != nil {
			return err
		}
		mapKey := key.MapKey()
		if fd.MapValue().Kind() == protoreflect.MessageKind {
			value := m.NewValue()
			if err := consumeMessage(valueData, value.Message()); err != nil {
				return err
			}
			m.Set(mapKey, value)
			continue
		}
		value, err := consumeScalar(valueData, fd.MapValue().Kind())
		if err != nil {
			return err
		}
		m.Set(mapKey, value)
	}
	if len(data) != 0 {
		return errors.New("trailing map data")
	}
	return nil
}

func setScalar(data []byte, kind protoreflect.Kind, msg protoreflect.Message, fd protoreflect.FieldDescriptor) error {
	if kind == protoreflect.MessageKind {
		return consumeMessage(data, msg.Mutable(fd).Message())
	}
	value, err := consumeScalar(data, kind)
	if err != nil {
		return err
	}
	// proto3 scalars without explicit presence disappear from Message.Range at
	// their default value. Accepting such a field here would therefore give one
	// logical message two native encodings and make decode->encode non-idempotent.
	// Proto2 optional, proto3 optional and oneof fields retain meaningful default
	// presence and are deliberately exempt.
	if !fd.HasPresence() && scalarIsDefault(kind, value) {
		return errors.New("non-canonical explicit default value")
	}
	msg.Set(fd, value)
	return nil
}

func scalarIsDefault(kind protoreflect.Kind, value protoreflect.Value) bool {
	switch kind {
	case protoreflect.BoolKind:
		return !value.Bool()
	case protoreflect.EnumKind:
		return value.Enum() == 0
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int() == 0
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint() == 0
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float() == 0
	case protoreflect.StringKind:
		return value.String() == ""
	case protoreflect.BytesKind:
		return len(value.Bytes()) == 0
	default:
		return false
	}
}

func consumeScalar(data []byte, kind protoreflect.Kind) (protoreflect.Value, error) {
	switch kind {
	case protoreflect.BoolKind:
		if len(data) != 1 || data[0] > 1 {
			return protoreflect.Value{}, errors.New("invalid bool")
		}
		return protoreflect.ValueOfBool(data[0] == 1), nil
	case protoreflect.EnumKind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid enum")
		}
		value := int64(binary.BigEndian.Uint64(data))
		if value < math.MinInt32 || value > math.MaxInt32 {
			return protoreflect.Value{}, errors.New("enum exceeds int32 range")
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(value)), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid signed integer")
		}
		value := int64(binary.BigEndian.Uint64(data))
		if value < math.MinInt32 || value > math.MaxInt32 {
			return protoreflect.Value{}, errors.New("signed integer exceeds int32 range")
		}
		return protoreflect.ValueOfInt32(int32(value)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid signed integer")
		}
		return protoreflect.ValueOfInt64(int64(binary.BigEndian.Uint64(data))), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid unsigned integer")
		}
		value := binary.BigEndian.Uint64(data)
		if value > math.MaxUint32 {
			return protoreflect.Value{}, errors.New("unsigned integer exceeds uint32 range")
		}
		return protoreflect.ValueOfUint32(uint32(value)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid unsigned integer")
		}
		return protoreflect.ValueOfUint64(binary.BigEndian.Uint64(data)), nil
	case protoreflect.FloatKind:
		if len(data) != 4 {
			return protoreflect.Value{}, errors.New("invalid float")
		}
		return protoreflect.ValueOfFloat32(math.Float32frombits(binary.BigEndian.Uint32(data))), nil
	case protoreflect.DoubleKind:
		if len(data) != 8 {
			return protoreflect.Value{}, errors.New("invalid double")
		}
		return protoreflect.ValueOfFloat64(math.Float64frombits(binary.BigEndian.Uint64(data))), nil
	case protoreflect.StringKind:
		if !utf8.Valid(data) {
			return protoreflect.Value{}, errors.New("invalid UTF-8 string")
		}
		return protoreflect.ValueOfString(string(data)), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(append([]byte(nil), data...)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported scalar kind %s", kind)
	}
}

func kindCode(kind protoreflect.Kind) byte { return byte(kind) }

func consumeUvarint(data []byte) (uint64, []byte, error) {
	value, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, errors.New("invalid uvarint")
	}
	var canonical [binary.MaxVarintLen64]byte
	if n != binary.PutUvarint(canonical[:], value) {
		return 0, nil, errors.New("non-canonical uvarint")
	}
	return value, data[n:], nil
}

func consumeBytes(data []byte) ([]byte, []byte, error) {
	length, rest, err := consumeUvarint(data)
	if err != nil {
		return nil, nil, err
	}
	if length > uint64(len(rest)) {
		return nil, nil, errors.New("truncated bytes")
	}
	return rest[:int(length)], rest[int(length):], nil
}
