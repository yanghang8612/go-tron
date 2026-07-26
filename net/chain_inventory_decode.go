package net

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type decodedChainInventoryID struct {
	hash   tcommon.Hash
	number int64
}

var chainInventoryDecodeLayoutOK = verifyChainInventoryDecodeLayout()

func verifyChainInventoryDecodeLayout() bool {
	fields := (&corepb.ChainInventory{}).ProtoReflect().Descriptor().Fields()
	idFields := (&corepb.ChainInventory_BlockId{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 2 &&
		chainInventoryFieldShape(fields, 1, protoreflect.MessageKind, true) &&
		chainInventoryFieldShape(fields, 2, protoreflect.Int64Kind, false) &&
		idFields.Len() == 2 &&
		chainInventoryFieldShape(idFields, 1, protoreflect.BytesKind, false) &&
		chainInventoryFieldShape(idFields, 2, protoreflect.Int64Kind, false)
}

func chainInventoryFieldShape(fields protoreflect.FieldDescriptors, number protoreflect.FieldNumber, kind protoreflect.Kind, list bool) bool {
	field := fields.ByNumber(number)
	return field != nil && field.Kind() == kind && field.IsList() == list
}

// consumeChainInventoryField parses non-group wire fields without recursively
// traversing attacker-controlled groups. Valid group-bearing messages use the
// generated decoder below, retaining protobuf's recursion limit on that cold
// compatibility path.
func consumeChainInventoryField(data []byte) (number protowire.Number, wireType protowire.Type, tagSize, fieldSize int, ok, useGenerated bool) {
	number, wireType, tagSize = protowire.ConsumeTag(data)
	if tagSize < 0 || !number.IsValid() {
		return 0, 0, 0, 0, false, false
	}
	if wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
		return 0, 0, 0, 0, false, true
	}
	valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
	if valueSize < 0 {
		return 0, 0, 0, 0, false, false
	}
	return number, wireType, tagSize, tagSize + valueSize, true, false
}

func decodeChainInventoryID(data []byte) (decodedChainInventoryID, bool, bool) {
	var id decodedChainInventoryID
	for len(data) != 0 {
		number, wireType, tagSize, fieldSize, ok, useGenerated := consumeChainInventoryField(data)
		if !ok {
			return decodedChainInventoryID{}, false, useGenerated
		}
		fieldData := data[:fieldSize]
		switch {
		case number == 1 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			id.hash = tcommon.BytesToHash(value)
		case number == 2 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			id.number = int64(value)
		}
		data = data[fieldSize:]
	}
	return id, true, false
}

// decodeChainInventory collapses the generated decoder's pointer graph (one
// child message and one owned hash slice per block id) into one contiguous
// value slice. It completes the entire decode before HandleChainInventory
// mutates sync state. Unknown fields and duplicate-field last-value semantics
// match protobuf; malformed input is rejected.
func decodeChainInventory(payload []byte) ([]decodedChainInventoryID, int64, bool) {
	if !chainInventoryDecodeLayoutOK {
		return decodeChainInventoryGenerated(payload)
	}
	count := 0
	for data := payload; len(data) != 0; {
		number, wireType, _, fieldSize, ok, useGenerated := consumeChainInventoryField(data)
		if !ok {
			if useGenerated {
				return decodeChainInventoryGenerated(payload)
			}
			return nil, 0, false
		}
		if number == 1 && wireType == protowire.BytesType {
			count++
		}
		data = data[fieldSize:]
	}

	ids := make([]decodedChainInventoryID, 0, count)
	var remainNum int64
	for data := payload; len(data) != 0; {
		number, wireType, tagSize, fieldSize, ok, useGenerated := consumeChainInventoryField(data)
		if !ok {
			if useGenerated {
				return decodeChainInventoryGenerated(payload)
			}
			return nil, 0, false
		}
		fieldData := data[:fieldSize]
		switch {
		case number == 1 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			id, valid, useGenerated := decodeChainInventoryID(value)
			if !valid {
				if useGenerated {
					return decodeChainInventoryGenerated(payload)
				}
				return nil, 0, false
			}
			ids = append(ids, id)
		case number == 2 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			remainNum = int64(value)
		}
		data = data[fieldSize:]
	}
	return ids, remainNum, true
}

func decodeChainInventoryGenerated(payload []byte) ([]decodedChainInventoryID, int64, bool) {
	var inventory corepb.ChainInventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return nil, 0, false
	}
	ids := make([]decodedChainInventoryID, len(inventory.Ids))
	for index, id := range inventory.Ids {
		ids[index] = decodedChainInventoryID{
			hash:   tcommon.BytesToHash(id.GetHash()),
			number: id.GetNumber(),
		}
	}
	return ids, inventory.RemainNum, true
}
