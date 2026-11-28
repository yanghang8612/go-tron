package statecodec

import (
	"encoding/binary"
	"math/bits"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MarshalContractState encodes ContractState in the exact same native format as
// Marshal, avoiding protoreflect and field-slice allocations on VM energy updates.
func MarshalContractState(msg *contractpb.ContractState) ([]byte, error) {
	if msg == nil {
		return Marshal(msg)
	}
	unknown := msg.ProtoReflect().GetUnknown()
	fieldCount := byte(0)
	if msg.EnergyUsage != 0 {
		fieldCount++
	}
	if msg.EnergyFactor != 0 {
		fieldCount++
	}
	if msg.UpdateCycle != 0 {
		fieldCount++
	}
	unknownSize := (bits.Len64(uint64(len(unknown))|1)+6)/7 + len(unknown)
	size := len(magic) + 1 + int(fieldCount)*11 + unknownSize
	out := make([]byte, 0, size)
	out = append(out, magic[:]...)
	out = append(out, fieldCount)
	if msg.EnergyUsage != 0 {
		out = append(out, 1, byte(protoreflect.Int64Kind), 8)
		out = binary.BigEndian.AppendUint64(out, uint64(msg.EnergyUsage))
	}
	if msg.EnergyFactor != 0 {
		out = append(out, 2, byte(protoreflect.Int64Kind), 8)
		out = binary.BigEndian.AppendUint64(out, uint64(msg.EnergyFactor))
	}
	if msg.UpdateCycle != 0 {
		out = append(out, 3, byte(protoreflect.Int64Kind), 8)
		out = binary.BigEndian.AppendUint64(out, uint64(msg.UpdateCycle))
	}
	out = binary.AppendUvarint(out, uint64(len(unknown)))
	out = append(out, unknown...)
	return out, nil
}

// UnmarshalContractState decodes a native ContractState row without reflection.
// Invalid rows fall back to the generic decoder so error diagnostics remain identical.
func UnmarshalContractState(data []byte) (*contractpb.ContractState, error) {
	if out, ok := unmarshalContractStateFast(data); ok {
		return out, nil
	}
	var out contractpb.ContractState
	if err := Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func unmarshalContractStateFast(data []byte) (*contractpb.ContractState, bool) {
	if !IsNative(data) {
		return nil, false
	}
	count, rest, err := consumeUvarint(data[len(magic):])
	if err != nil || count > 3 {
		return nil, false
	}
	var (
		previous     uint64
		energyUsage  int64
		energyFactor int64
		updateCycle  int64
	)
	for i := uint64(0); i < count; i++ {
		number, afterNum, err := consumeUvarint(rest)
		if err != nil || number <= previous || number > 3 || len(afterNum) == 0 {
			return nil, false
		}
		previous = number
		if afterNum[0] != byte(protoreflect.Int64Kind) {
			return nil, false
		}
		payload, afterVal, err := consumeBytes(afterNum[1:])
		if err != nil || len(payload) != 8 {
			return nil, false
		}
		v := int64(binary.BigEndian.Uint64(payload))
		if v == 0 {
			return nil, false
		}
		switch number {
		case 1:
			energyUsage = v
		case 2:
			energyFactor = v
		case 3:
			updateCycle = v
		}
		rest = afterVal
	}
	unknown, tail, err := consumeBytes(rest)
	if err != nil || len(tail) != 0 {
		return nil, false
	}
	out := &contractpb.ContractState{
		EnergyUsage:  energyUsage,
		EnergyFactor: energyFactor,
		UpdateCycle:  updateCycle,
	}
	if len(unknown) != 0 {
		out.ProtoReflect().SetUnknown(append([]byte(nil), unknown...))
	}
	return out, true
}
