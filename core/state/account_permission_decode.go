package state

import (
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// decodedAccountPermission owns the common first Key and its pointer slot in
// the same allocation as the Permission. Additional keys use one contiguous
// value slice and one pointer slice instead of one allocation per submessage.
type decodedAccountPermission struct {
	permission corepb.Permission
	firstKey   corepb.Key
	firstKeys  [1]*corepb.Key
}

type accountPermissionBorrowedFields struct {
	name           []byte
	operations     []byte
	nameSeen       bool
	operationsSeen bool
	keyCount       int
	byteCount      int
}

var accountPermissionArenaLayoutOK = verifyAccountPermissionArenaLayout()

// verifyAccountPermissionArenaLayout ties the direct decoder to the generated
// Permission and Key schemas. Protobuf regeneration that changes either shape
// automatically returns to proto.Unmarshal.
func verifyAccountPermissionArenaLayout() bool {
	permissionFields := (&corepb.Permission{}).ProtoReflect().Descriptor().Fields()
	keyFields := (&corepb.Key{}).ProtoReflect().Descriptor().Fields()
	keys := permissionFields.ByNumber(7)
	return permissionFields.Len() == 7 &&
		accountPermissionFieldShape(permissionFields, 1, protoreflect.EnumKind, false) &&
		accountPermissionFieldShape(permissionFields, 2, protoreflect.Int32Kind, false) &&
		accountPermissionFieldShape(permissionFields, 3, protoreflect.StringKind, false) &&
		accountPermissionFieldShape(permissionFields, 4, protoreflect.Int64Kind, false) &&
		accountPermissionFieldShape(permissionFields, 5, protoreflect.Int32Kind, false) &&
		accountPermissionFieldShape(permissionFields, 6, protoreflect.BytesKind, false) &&
		keys != nil && keys.Kind() == protoreflect.MessageKind && keys.IsList() &&
		keys.Message().FullName() == (&corepb.Key{}).ProtoReflect().Descriptor().FullName() &&
		keyFields.Len() == 2 &&
		accountPermissionFieldShape(keyFields, 1, protoreflect.BytesKind, false) &&
		accountPermissionFieldShape(keyFields, 2, protoreflect.Int64Kind, false)
}

func accountPermissionFieldShape(fields protoreflect.FieldDescriptors, number protoreflect.FieldNumber, kind protoreflect.Kind, list bool) bool {
	field := fields.ByNumber(number)
	return field != nil && field.Kind() == kind && field.IsList() == list
}

// scanAccountPermissionKey validates a nested Key without allocating and
// returns its final (last-occurrence) address value. Group-bearing or malformed
// messages request a generated-decoder fallback so recursion and errors remain
// exactly protobuf-owned cold-path behavior.
func scanAccountPermissionKey(raw []byte) (address []byte, ok bool) {
	for data := raw; len(data) != 0; {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 || !number.IsValid() || wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
			return nil, false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return nil, false
		}
		if number == 1 && wireType == protowire.BytesType {
			address, _ = protowire.ConsumeBytes(data[tagSize:])
		}
		data = data[tagSize+valueSize:]
	}
	return address, true
}

func scanAccountPermission(raw []byte) (accountPermissionBorrowedFields, bool) {
	var fields accountPermissionBorrowedFields
	for data := raw; len(data) != 0; {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 || !number.IsValid() || wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
			return accountPermissionBorrowedFields{}, false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return accountPermissionBorrowedFields{}, false
		}
		fieldValue := data[tagSize : tagSize+valueSize]
		switch {
		case number == 3 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldValue)
			// The generated decoder rejects an invalid occurrence immediately,
			// even if a later duplicate would otherwise replace it.
			if !utf8.Valid(value) {
				return accountPermissionBorrowedFields{}, false
			}
			fields.name = value
			fields.nameSeen = true
		case number == 6 && wireType == protowire.BytesType:
			fields.operations, _ = protowire.ConsumeBytes(fieldValue)
			fields.operationsSeen = true
		case number == 7 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldValue)
			address, valid := scanAccountPermissionKey(value)
			if !valid || fields.keyCount == int(^uint(0)>>1) {
				return accountPermissionBorrowedFields{}, false
			}
			fields.keyCount++
			if len(address) > len(raw)-fields.byteCount {
				return accountPermissionBorrowedFields{}, false
			}
			fields.byteCount += len(address)
		}
		data = data[tagSize+valueSize:]
	}
	if len(fields.name) > len(raw)-fields.byteCount {
		return accountPermissionBorrowedFields{}, false
	}
	fields.byteCount += len(fields.name)
	if len(fields.operations) > len(raw)-fields.byteCount {
		return accountPermissionBorrowedFields{}, false
	}
	fields.byteCount += len(fields.operations)
	return fields, true
}

func decodeAccountPermissionKeyBorrowed(raw []byte, key *corepb.Key) bool {
	var unknown []byte
	for data := raw; len(data) != 0; {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 || !number.IsValid() || wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
			return false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return false
		}
		fieldData := data[:tagSize+valueSize]
		known := true
		switch {
		case number == 1 && wireType == protowire.BytesType:
			key.Address, _ = protowire.ConsumeBytes(fieldData[tagSize:])
		case number == 2 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			key.Weight = int64(value)
		default:
			known = false
		}
		if !known {
			unknown = protowire.AppendTag(unknown, number, wireType)
			unknown = append(unknown, fieldData[tagSize:]...)
		}
		data = data[tagSize+valueSize:]
	}
	if len(unknown) != 0 {
		key.ProtoReflect().SetUnknown(unknown)
	}
	return true
}

func copyAccountPermissionBorrowedBytes(permission *corepb.Permission, borrowed accountPermissionBorrowedFields) *corepb.Permission {
	var arena []byte
	if borrowed.byteCount != 0 {
		arena = make([]byte, borrowed.byteCount)
	}
	offset := 0
	if borrowed.nameSeen {
		end := offset + len(borrowed.name)
		copy(arena[offset:end], borrowed.name)
		permission.PermissionName = ownedBytesString(arena[offset:end:end])
		offset = end
	}
	if borrowed.operationsSeen {
		end := offset + len(borrowed.operations)
		copy(arena[offset:end], borrowed.operations)
		permission.Operations = arena[offset:end:end]
		if len(permission.Operations) == 0 {
			permission.Operations = []byte{}
		}
		offset = end
	}
	for _, key := range permission.Keys {
		if key.Address == nil {
			continue
		}
		end := offset + len(key.Address)
		copy(arena[offset:end], key.Address)
		key.Address = arena[offset:end:end]
		if len(key.Address) == 0 {
			key.Address = []byte{}
		}
		offset = end
	}
	return permission
}

// decodeAccountPermissionArena parses zero/one-Key rows in one pass. On seeing
// a second Key it scans only the unparsed suffix, then allocates exact-sized
// contiguous value and pointer slices and resumes decoding. Selected bytes are
// coalesced after parsing; malformed and group-bearing messages are delegated
// to the generated decoder.
func decodeAccountPermissionArena(raw []byte) (*corepb.Permission, error) {
	decoded := new(decodedAccountPermission)
	permission := &decoded.permission
	var borrowed accountPermissionBorrowedFields
	var unknown []byte
	for data := raw; len(data) != 0; {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 || !number.IsValid() || wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
			return decodeAccountPermissionGenerated(raw)
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return decodeAccountPermissionGenerated(raw)
		}
		fieldData := data[:tagSize+valueSize]
		known := true
		switch {
		case number == 1 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			permission.Type = corepb.Permission_PermissionType(value)
		case number == 2 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			permission.Id = int32(value)
		case number == 3 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			if !utf8.Valid(value) {
				return decodeAccountPermissionGenerated(raw)
			}
			borrowed.name = value
			borrowed.nameSeen = true
		case number == 4 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			permission.Threshold = int64(value)
		case number == 5 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			permission.ParentId = int32(value)
		case number == 6 && wireType == protowire.BytesType:
			borrowed.operations, _ = protowire.ConsumeBytes(fieldData[tagSize:])
			borrowed.operationsSeen = true
		case number == 7 && wireType == protowire.BytesType:
			if borrowed.keyCount == 0 {
				decoded.firstKeys[0] = &decoded.firstKey
				permission.Keys = decoded.firstKeys[:]
			} else if len(permission.Keys) == 1 {
				remainder, valid := scanAccountPermission(data)
				if !valid || remainder.keyCount == 0 || remainder.keyCount > int(^uint(0)>>1)-borrowed.keyCount {
					return decodeAccountPermissionGenerated(raw)
				}
				totalKeys := borrowed.keyCount + remainder.keyCount
				additionalKeys := make([]corepb.Key, totalKeys-1)
				permission.Keys = make([]*corepb.Key, totalKeys)
				permission.Keys[0] = &decoded.firstKey
				for index := range additionalKeys {
					permission.Keys[index+1] = &additionalKeys[index]
				}
			}
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			if borrowed.keyCount >= len(permission.Keys) || !decodeAccountPermissionKeyBorrowed(value, permission.Keys[borrowed.keyCount]) {
				return decodeAccountPermissionGenerated(raw)
			}
			borrowed.keyCount++
		default:
			known = false
		}
		if !known {
			unknown = protowire.AppendTag(unknown, number, wireType)
			unknown = append(unknown, fieldData[tagSize:]...)
		}
		data = data[tagSize+valueSize:]
	}
	if len(unknown) != 0 {
		permission.ProtoReflect().SetUnknown(unknown)
	}
	borrowed.byteCount = len(borrowed.name)
	if len(borrowed.operations) > len(raw)-borrowed.byteCount {
		return decodeAccountPermissionGenerated(raw)
	}
	borrowed.byteCount += len(borrowed.operations)
	for _, key := range permission.Keys {
		if len(key.Address) > len(raw)-borrowed.byteCount {
			return decodeAccountPermissionGenerated(raw)
		}
		borrowed.byteCount += len(key.Address)
	}
	return copyAccountPermissionBorrowedBytes(permission, borrowed), nil
}

func decodeAccountPermissionGenerated(raw []byte) (*corepb.Permission, error) {
	// Retain the single pointer-slot reserve for schema and cold-path fallbacks.
	type generatedAccountPermission struct {
		permission corepb.Permission
		firstKeys  [1]*corepb.Key
	}
	decoded := new(generatedAccountPermission)
	decoded.permission.Keys = decoded.firstKeys[:0]
	if err := (proto.UnmarshalOptions{Merge: true}).Unmarshal(raw, &decoded.permission); err != nil {
		return nil, err
	}
	return &decoded.permission, nil
}

func decodeAccountPermission(raw []byte) (*corepb.Permission, error) {
	if accountPermissionArenaLayoutOK {
		return decodeAccountPermissionArena(raw)
	}
	return decodeAccountPermissionGenerated(raw)
}
