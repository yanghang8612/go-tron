package types

import (
	"errors"
	"sync"
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var accountUnmarshalBasePool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

type accountDecodedMaps struct {
	asset                      map[string]int64
	latestAssetOperationTime   map[string]int64
	freeAssetNetUsage          map[string]int64
	assetV2                    map[string]int64
	latestAssetOperationTimeV2 map[string]int64
	freeAssetNetUsageV2        map[string]int64
}

type accountDecodedBytes struct {
	accountName     []byte
	code            []byte
	assetIssuedName []byte
	accountID       []byte
	codeHash        []byte
	assetIssuedID   []byte
}

func (fields *accountDecodedBytes) set(number protowire.Number, value []byte) {
	switch number {
	case 1:
		fields.accountName = value
	case 13:
		fields.code = value
	case 17:
		fields.assetIssuedName = value
	case 23:
		fields.accountID = value
	case 30:
		fields.codeHash = value
	case 57:
		fields.assetIssuedID = value
	}
}

func (fields *accountDecodedBytes) assign(pb *corepb.Account, inline []byte) {
	total := len(fields.accountName) + len(fields.code) + len(fields.assetIssuedName) +
		len(fields.accountID) + len(fields.codeHash) + len(fields.assetIssuedID)
	if total == 0 {
		return
	}
	var storage []byte
	if total <= len(inline) {
		storage = inline[:total]
	} else {
		storage = make([]byte, total)
	}
	offset := 0
	pb.AccountName = copyAccountDirectBytes(storage, &offset, fields.accountName)
	pb.Code = copyAccountDirectBytes(storage, &offset, fields.code)
	pb.AssetIssuedName = copyAccountDirectBytes(storage, &offset, fields.assetIssuedName)
	pb.AccountId = copyAccountDirectBytes(storage, &offset, fields.accountID)
	pb.CodeHash = copyAccountDirectBytes(storage, &offset, fields.codeHash)
	pb.AssetIssued_ID = copyAccountDirectBytes(storage, &offset, fields.assetIssuedID)
}

func copyAccountDirectBytes(storage []byte, offset *int, value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	start := *offset
	end := start + len(value)
	copy(storage[start:end], value)
	*offset = end
	return storage[start:end:end]
}

func (maps *accountDecodedMaps) set(number protowire.Number, key string, value int64) {
	var target *map[string]int64
	switch number {
	case 6:
		target = &maps.asset
	case 18:
		target = &maps.latestAssetOperationTime
	case 20:
		target = &maps.freeAssetNetUsage
	case 56:
		target = &maps.assetV2
	case 58:
		target = &maps.latestAssetOperationTimeV2
	case 59:
		target = &maps.freeAssetNetUsageV2
	default:
		return
	}
	if *target == nil {
		*target = make(map[string]int64)
	}
	(*target)[key] = value
}

func (maps *accountDecodedMaps) assign(pb *corepb.Account) {
	pb.Asset = maps.asset
	pb.LatestAssetOperationTime = maps.latestAssetOperationTime
	pb.FreeAssetNetUsage = maps.freeAssetNetUsage
	pb.AssetV2 = maps.assetV2
	pb.LatestAssetOperationTimeV2 = maps.latestAssetOperationTimeV2
	pb.FreeAssetNetUsageV2 = maps.freeAssetNetUsageV2
}

// unmarshalAccountDirectFieldsInto decodes Account's six string→int64 maps
// without protobuf reflection, moves the common address into caller-owned
// inline storage, and combines Account's other singular byte fields into one
// owned span. All remaining fields, including unknown top-level fields, stay
// on protobuf-go's generated decoder after the specialized records are filtered
// from a pooled wire buffer. The schema guard is shared with the direct map
// marshaler so a future Account field/layout change disables both fast paths.
func unmarshalAccountDirectFieldsInto(data []byte, pb *corepb.Account, inlineAddress, inlineDirectBytes []byte) (error, bool) {
	if !accountDirectMapLayoutOK || !accountWireMayContainDirectField(data) {
		return nil, false
	}

	var (
		maps         accountDecodedMaps
		byteFields   accountDecodedBytes
		basePtr      *[]byte
		base         []byte
		foundDirect  bool
		foundMap     bool
		foundAddress bool
		foundBytes   bool
		address      []byte
		wireOffset   int
	)
	defer func() {
		if basePtr == nil {
			return
		}
		if cap(base) <= 1<<20 {
			*basePtr = base[:0]
			accountUnmarshalBasePool.Put(basePtr)
		}
	}()

	for rest := data; len(rest) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(rest)
		if tagSize < 0 {
			return protowire.ParseError(tagSize), true
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, rest[tagSize:])
		if valueSize < 0 {
			return protowire.ParseError(valueSize), true
		}
		fieldSize := tagSize + valueSize
		if !isAccountDirectBytesField(number) && !isAccountMapField(number) {
			if foundDirect {
				base = append(base, rest[:fieldSize]...)
			}
			rest = rest[fieldSize:]
			wireOffset += fieldSize
			continue
		}

		// Let protobuf-go report the canonical error for a known specialized
		// field with an unexpected wire type. Valid encodings always use bytes.
		if wireType != protowire.BytesType {
			return nil, false
		}
		if !foundDirect {
			basePtr = accountUnmarshalBasePool.Get().(*[]byte)
			base = (*basePtr)[:0]
			base = append(base, data[:wireOffset]...)
			foundDirect = true
		}
		payload, payloadSize := protowire.ConsumeBytes(rest[tagSize:])
		if payloadSize < 0 {
			return protowire.ParseError(payloadSize), true
		}
		if number == 3 {
			// Singular protobuf bytes fields use the last wire occurrence. Skip all
			// occurrences and retain the final one, matching generated Unmarshal.
			address = payload
			foundAddress = true
			rest = rest[fieldSize:]
			wireOffset += fieldSize
			continue
		}
		if isAccountDirectBytesField(number) {
			byteFields.set(number, payload)
			foundBytes = true
			rest = rest[fieldSize:]
			wireOffset += fieldSize
			continue
		}
		key, value, err := consumeAccountMapEntry(payload)
		if err != nil {
			return err, true
		}
		foundMap = true
		maps.set(number, key, value)
		rest = rest[fieldSize:]
		wireOffset += fieldSize
	}
	if !foundDirect {
		return nil, false
	}

	if err := proto.Unmarshal(base, pb); err != nil {
		return err, true
	}
	if foundMap {
		maps.assign(pb)
	}
	if foundBytes {
		byteFields.assign(pb, inlineDirectBytes)
	}
	if foundAddress {
		switch {
		case len(address) == 0:
			pb.Address = nil
		case len(address) <= len(inlineAddress):
			copy(inlineAddress, address)
			pb.Address = inlineAddress[:len(address):len(address)]
		default:
			pb.Address = append([]byte(nil), address...)
		}
	}
	return nil, true
}

// accountWireMayContainDirectField is a cheap negative filter for accounts
// without a canonical address or map tag. It may return a false positive when
// a nested payload happens to contain one of these bytes; the full top-level
// scan below then rejects it. A deliberately non-canonical tag may miss the
// fast path, but still falls back to protobuf-go unchanged.
func accountWireMayContainDirectField(data []byte) bool {
	for i, first := range data {
		switch first {
		case 0x0a, 0x1a, 0x6a: // fields 1, 3 and 13, bytes
			return true
		case 0x8a, 0xba, 0xf2: // fields 17, 23 and 30, bytes
			if i+1 < len(data) && data[i+1] == 0x01 {
				return true
			}
		case 0x32: // field 6, bytes
			return true
		case 0x92, 0xa2: // fields 18 and 20, bytes
			if i+1 < len(data) && data[i+1] == 0x01 {
				return true
			}
		case 0xc2, 0xca, 0xd2, 0xda: // fields 56-59, bytes
			if i+1 < len(data) && data[i+1] == 0x03 {
				return true
			}
		}
	}
	return false
}

func consumeAccountMapEntry(entry []byte) (key string, value int64, err error) {
	for len(entry) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(entry)
		if tagSize < 0 {
			return "", 0, protowire.ParseError(tagSize)
		}
		if !number.IsValid() {
			return "", 0, errors.New("invalid account map field number")
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, entry[tagSize:])
		if valueSize < 0 {
			return "", 0, protowire.ParseError(valueSize)
		}
		switch number {
		case 1:
			if wireType != protowire.BytesType {
				break
			}
			bytesValue, size := protowire.ConsumeBytes(entry[tagSize:])
			if size < 0 {
				return "", 0, protowire.ParseError(size)
			}
			if !utf8.Valid(bytesValue) {
				return "", 0, errors.New("string field contains invalid UTF-8")
			}
			key = string(bytesValue)
		case 2:
			if wireType != protowire.VarintType {
				break
			}
			varintValue, size := protowire.ConsumeVarint(entry[tagSize:])
			if size < 0 {
				return "", 0, protowire.ParseError(size)
			}
			value = int64(varintValue)
		}
		entry = entry[tagSize+valueSize:]
	}
	return key, value, nil
}
