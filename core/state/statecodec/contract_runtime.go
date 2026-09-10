package statecodec

import (
	"encoding/binary"
	"math"
	"unicode/utf8"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ContractRuntimeFields is the subset of SmartContract needed during execution.
// OriginAddress and TrxHash borrow data passed to ReadContractRuntime; callers
// must not modify them or retain them beyond the input's lifetime.
type ContractRuntimeFields struct {
	OriginAddress              []byte
	ConsumeUserResourcePercent int64
	OriginEnergyLimit          int64
	TrxHash                    []byte
	Version                    int32
}

type contractRuntimeField struct {
	typeCode byte
	message  []contractRuntimeField
}

// These schemas describe every field, including the ABI graph that runtime
// callers do not need. The schema test checks them against generated descriptors.
var contractRuntimeParamSchema = []contractRuntimeField{
	{typeCode: byte(protoreflect.BoolKind)},
	{typeCode: byte(protoreflect.StringKind)},
	{typeCode: byte(protoreflect.StringKind)},
}

var contractRuntimeEntrySchema = []contractRuntimeField{
	{typeCode: byte(protoreflect.BoolKind)},
	{typeCode: byte(protoreflect.BoolKind)},
	{typeCode: byte(protoreflect.StringKind)},
	{typeCode: shapeList | byte(protoreflect.MessageKind), message: contractRuntimeParamSchema},
	{typeCode: shapeList | byte(protoreflect.MessageKind), message: contractRuntimeParamSchema},
	{typeCode: byte(protoreflect.EnumKind)},
	{typeCode: byte(protoreflect.BoolKind)},
	{typeCode: byte(protoreflect.EnumKind)},
}

var contractRuntimeABISchema = []contractRuntimeField{
	{typeCode: shapeList | byte(protoreflect.MessageKind), message: contractRuntimeEntrySchema},
}

var contractRuntimeSchema = []contractRuntimeField{
	{typeCode: byte(protoreflect.BytesKind)},
	{typeCode: byte(protoreflect.BytesKind)},
	{typeCode: byte(protoreflect.MessageKind), message: contractRuntimeABISchema},
	{typeCode: byte(protoreflect.BytesKind)},
	{typeCode: byte(protoreflect.Int64Kind)},
	{typeCode: byte(protoreflect.Int64Kind)},
	{typeCode: byte(protoreflect.StringKind)},
	{typeCode: byte(protoreflect.Int64Kind)},
	{typeCode: byte(protoreflect.BytesKind)},
	{typeCode: byte(protoreflect.BytesKind)},
	{typeCode: byte(protoreflect.Int32Kind)},
}

// ReadContractRuntime validates a native SmartContract as strictly as Unmarshal,
// including every nested ABI field, without allocating a protobuf object graph.
// Unknown trailers remain opaque, exactly as in the native codec. Invalid rows
// use Unmarshal to preserve its diagnostics; this fallback also preserves the
// generic decoder's acceptance if its schema gains a field before this reader.
func ReadContractRuntime(data []byte) (ContractRuntimeFields, error) {
	var fields [11][]byte
	if IsNative(data) && validateContractRuntimeMessage(data[len(magic):], contractRuntimeSchema, &fields) {
		out := ContractRuntimeFields{OriginAddress: fields[0], TrxHash: fields[9]}
		if fields[5] != nil {
			out.ConsumeUserResourcePercent = int64(binary.BigEndian.Uint64(fields[5]))
		}
		if fields[7] != nil {
			out.OriginEnergyLimit = int64(binary.BigEndian.Uint64(fields[7]))
		}
		if fields[10] != nil {
			out.Version = int32(binary.BigEndian.Uint64(fields[10]))
		}
		return out, nil
	}
	var out contractpb.SmartContract
	if err := Unmarshal(data, &out); err != nil {
		return ContractRuntimeFields{}, err
	}
	return ContractRuntimeFields{
		OriginAddress: out.OriginAddress, ConsumeUserResourcePercent: out.ConsumeUserResourcePercent,
		OriginEnergyLimit: out.OriginEnergyLimit, TrxHash: out.TrxHash, Version: out.Version,
	}, nil
}

func validateContractRuntimeMessage(data []byte, schema []contractRuntimeField, fields *[11][]byte) bool {
	count, data, err := consumeUvarint(data)
	if err != nil || count > uint64(len(schema)) {
		return false
	}
	var previous uint64
	for i := uint64(0); i < count; i++ {
		number, rest, err := consumeUvarint(data)
		if err != nil || number <= previous || number > uint64(len(schema)) || len(rest) == 0 {
			return false
		}
		previous = number
		field := schema[number-1]
		if rest[0] != field.typeCode {
			return false
		}
		payload, rest, err := consumeBytes(rest[1:])
		if err != nil {
			return false
		}
		if !validateContractRuntimeField(payload, field) {
			return false
		}
		if fields != nil {
			fields[number-1] = payload
		}
		data = rest
	}
	_, rest, err := consumeBytes(data)
	return err == nil && len(rest) == 0
}

func validateContractRuntimeField(data []byte, field contractRuntimeField) bool {
	switch field.typeCode {
	case byte(protoreflect.BytesKind):
		return len(data) != 0
	case byte(protoreflect.StringKind):
		return len(data) != 0 && utf8.Valid(data)
	case byte(protoreflect.BoolKind):
		return len(data) == 1 && data[0] == 1
	case byte(protoreflect.Int64Kind):
		return len(data) == 8 && binary.BigEndian.Uint64(data) != 0
	case byte(protoreflect.Int32Kind), byte(protoreflect.EnumKind):
		if len(data) != 8 {
			return false
		}
		value := int64(binary.BigEndian.Uint64(data))
		return value != 0 && value >= math.MinInt32 && value <= math.MaxInt32
	case byte(protoreflect.MessageKind):
		return validateContractRuntimeMessage(data, field.message, nil)
	case shapeList | byte(protoreflect.MessageKind):
		count, data, err := consumeUvarint(data)
		// Every element requires at least its length prefix. Bound corrupt counts
		// before iterating, and reject explicit empty lists like consumeList.
		if err != nil || count == 0 || count > uint64(len(data)) {
			return false
		}
		for i := uint64(0); i < count; i++ {
			payload, rest, err := consumeBytes(data)
			if err != nil || !validateContractRuntimeMessage(payload, field.message, nil) {
				return false
			}
			data = rest
		}
		return len(data) == 0
	default:
		return false
	}
}
