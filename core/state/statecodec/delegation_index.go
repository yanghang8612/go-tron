package statecodec

import (
	"encoding/binary"
	"math/bits"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MarshalDelegationIndex encodes the delegation index in exactly the same
// native format as Marshal. Its concrete schema avoids reflection and temporary
// per-address encodings, and the complete row needs one allocation.
func MarshalDelegationIndex(msg *corepb.DelegatedResourceAccountIndex) ([]byte, error) {
	if msg == nil {
		return Marshal(msg)
	}
	unknown := msg.ProtoReflect().GetUnknown()
	fieldCount := byte(0)
	size := len(magic) + 1 + delegationUvarintSize(len(unknown)) + len(unknown)
	if len(msg.Account) != 0 {
		fieldCount++
		size += 2 + delegationUvarintSize(len(msg.Account)) + len(msg.Account)
	}
	fromSize, toSize := 0, 0
	if len(msg.FromAccounts) != 0 {
		fieldCount++
		fromSize = delegationListSize(msg.FromAccounts)
		size += 2 + delegationUvarintSize(fromSize) + fromSize
	}
	if len(msg.ToAccounts) != 0 {
		fieldCount++
		toSize = delegationListSize(msg.ToAccounts)
		size += 2 + delegationUvarintSize(toSize) + toSize
	}
	if msg.Timestamp != 0 {
		fieldCount++
		size += 3 + 8
	}
	out := make([]byte, 0, size)
	out = append(out, magic[:]...)
	out = append(out, fieldCount)
	if len(msg.Account) != 0 {
		out = append(out, 1, byte(protoreflect.BytesKind))
		out = binary.AppendUvarint(out, uint64(len(msg.Account)))
		out = append(out, msg.Account...)
	}
	if len(msg.FromAccounts) != 0 {
		out = appendDelegationList(out, 2, fromSize, msg.FromAccounts)
	}
	if len(msg.ToAccounts) != 0 {
		out = appendDelegationList(out, 3, toSize, msg.ToAccounts)
	}
	if msg.Timestamp != 0 {
		out = append(out, 4, byte(protoreflect.Int64Kind), 8)
		out = binary.BigEndian.AppendUint64(out, uint64(msg.Timestamp))
	}
	out = binary.AppendUvarint(out, uint64(len(unknown)))
	out = append(out, unknown...)
	return out, nil
}

func delegationUvarintSize(n int) int {
	return (bits.Len64(uint64(n)|1) + 6) / 7
}

func delegationListSize(list [][]byte) int {
	size := delegationUvarintSize(len(list))
	for _, address := range list {
		size += delegationUvarintSize(len(address)) + len(address)
	}
	return size
}

func appendDelegationList(dst []byte, number byte, size int, list [][]byte) []byte {
	dst = append(dst, number, shapeList|byte(protoreflect.BytesKind))
	dst = binary.AppendUvarint(dst, uint64(size))
	dst = binary.AppendUvarint(dst, uint64(len(list)))
	for _, address := range list {
		dst = binary.AppendUvarint(dst, uint64(len(address)))
		dst = append(dst, address...)
	}
	return dst
}

// UnmarshalDelegationIndex decodes a native delegation index with the same
// validation as Unmarshal. All bytes are owned by the returned message. The
// addresses share one arena and their slice headers share one backing array;
// each exposed slice has its capacity capped so append cannot alter a neighbor.
// Invalid rows use the generic decoder to retain its exact error diagnostics.
func UnmarshalDelegationIndex(data []byte) (*corepb.DelegatedResourceAccountIndex, error) {
	if fields, unknown, ok := delegationIndexFields(data); ok {
		fromCount, fromBytes, fromOK := delegationListInfo(fields[1])
		toCount, toBytes, toOK := delegationListInfo(fields[2])
		if fromOK && toOK {
			out := new(corepb.DelegatedResourceAccountIndex)
			arena := make([]byte, len(fields[0])+fromBytes+toBytes+len(unknown))
			headers := make([][]byte, fromCount+toCount)
			if len(fields[0]) != 0 {
				out.Account, arena = delegationCopyBytes(arena, fields[0])
			}
			if fromCount != 0 {
				out.FromAccounts = headers[:fromCount:fromCount]
				arena = decodeDelegationList(fields[1], out.FromAccounts, arena)
			}
			if toCount != 0 {
				out.ToAccounts = headers[fromCount:len(headers):len(headers)]
				arena = decodeDelegationList(fields[2], out.ToAccounts, arena)
			}
			if len(fields[3]) != 0 {
				out.Timestamp = int64(binary.BigEndian.Uint64(fields[3]))
			}
			if len(unknown) != 0 {
				ownedUnknown, _ := delegationCopyBytes(arena, unknown)
				out.ProtoReflect().SetUnknown(ownedUnknown)
			}
			return out, nil
		}
	}
	var out corepb.DelegatedResourceAccountIndex
	if err := Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// delegationIndexFields validates the envelope and scalar fields, returning
// views into the input only. Lists are validated before allocating the result.
func delegationIndexFields(data []byte) (fields [4][]byte, unknown []byte, ok bool) {
	if !IsNative(data) {
		return fields, nil, false
	}
	count, data, err := consumeUvarint(data[len(magic):])
	if err != nil || count > uint64(len(fields)) {
		return fields, nil, false
	}
	var previous uint64
	for i := uint64(0); i < count; i++ {
		number, rest, err := consumeUvarint(data)
		if err != nil || number <= previous || number > uint64(len(fields)) || len(rest) == 0 {
			return fields, nil, false
		}
		previous = number
		typeCode := rest[0]
		payload, rest, err := consumeBytes(rest[1:])
		if err != nil {
			return fields, nil, false
		}
		switch number {
		case 1:
			if typeCode != byte(protoreflect.BytesKind) || len(payload) == 0 {
				return fields, nil, false
			}
		case 2, 3:
			if typeCode != shapeList|byte(protoreflect.BytesKind) {
				return fields, nil, false
			}
		case 4:
			if typeCode != byte(protoreflect.Int64Kind) || len(payload) != 8 || binary.BigEndian.Uint64(payload) == 0 {
				return fields, nil, false
			}
		}
		fields[number-1] = payload
		data = rest
	}
	unknown, rest, err := consumeBytes(data)
	if err != nil || len(rest) != 0 {
		return fields, nil, false
	}
	return fields, unknown, true
}

func delegationListInfo(data []byte) (count, byteSize int, ok bool) {
	if data == nil { // The field is absent, rather than an encoded empty list.
		return 0, 0, true
	}
	n, data, err := consumeUvarint(data)
	// Each element needs at least one length byte. This also bounds allocation
	// and iteration for malicious counts before conversion from uint64 to int.
	if err != nil || n == 0 || n > uint64(len(data)) {
		return 0, 0, false
	}
	count = int(n)
	for i := 0; i < count; i++ {
		value, rest, err := consumeBytes(data)
		if err != nil {
			return 0, 0, false
		}
		byteSize += len(value)
		data = rest
	}
	return count, byteSize, len(data) == 0
}

func decodeDelegationList(data []byte, list [][]byte, arena []byte) []byte {
	_, data, _ = consumeUvarint(data) // Validated by delegationListInfo.
	for i := range list {
		value, rest, _ := consumeBytes(data)
		if len(value) != 0 {
			list[i], arena = delegationCopyBytes(arena, value)
		}
		data = rest
	}
	return arena
}

func delegationCopyBytes(arena, value []byte) ([]byte, []byte) {
	n := copy(arena, value)
	return arena[:n:n], arena[n:]
}
