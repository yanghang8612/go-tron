package rawdb

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// drAccountIndexDecodePlan retains borrowed input spans only until decoding
// finishes. On the common known-field path, the returned protobuf owns one
// exact byte arena for the account and every repeated address.
type drAccountIndexDecodePlan struct {
	account      []byte
	accountSeen  bool
	fromCount    int
	toCount      int
	ownedBytes   int
	unknownBytes int
	timestamp    int64
}

func scanDrAccountIndex(data []byte) (drAccountIndexDecodePlan, error) {
	var plan drAccountIndexDecodePlan
	for len(data) != 0 {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 {
			return drAccountIndexDecodePlan{}, protowire.ParseError(tagSize)
		}
		if !number.IsValid() {
			return drAccountIndexDecodePlan{}, fmt.Errorf("dr account index: invalid field number %d", number)
		}
		if wireType == protowire.EndGroupType {
			return drAccountIndexDecodePlan{}, fmt.Errorf("dr account index: unexpected end group")
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return drAccountIndexDecodePlan{}, protowire.ParseError(valueSize)
		}
		fieldSize := tagSize + valueSize
		fieldValue := data[tagSize:fieldSize]

		switch {
		case number == 1 && wireType == protowire.BytesType:
			value, size := protowire.ConsumeBytes(fieldValue)
			if size < 0 {
				return drAccountIndexDecodePlan{}, protowire.ParseError(size)
			}
			plan.ownedBytes -= len(plan.account)
			plan.account = value
			plan.accountSeen = true
			plan.ownedBytes += len(value)
		case number == 2 && wireType == protowire.BytesType:
			value, size := protowire.ConsumeBytes(fieldValue)
			if size < 0 {
				return drAccountIndexDecodePlan{}, protowire.ParseError(size)
			}
			plan.fromCount++
			plan.ownedBytes += len(value)
		case number == 3 && wireType == protowire.BytesType:
			value, size := protowire.ConsumeBytes(fieldValue)
			if size < 0 {
				return drAccountIndexDecodePlan{}, protowire.ParseError(size)
			}
			plan.toCount++
			plan.ownedBytes += len(value)
		case number == 4 && wireType == protowire.VarintType:
			value, size := protowire.ConsumeVarint(fieldValue)
			if size < 0 {
				return drAccountIndexDecodePlan{}, protowire.ParseError(size)
			}
			plan.timestamp = int64(value)
		default:
			plan.unknownBytes += fieldSize
		}
		data = data[fieldSize:]
	}
	return plan, nil
}

// DecodeDrAccountIndexLegacy decodes the aggregate pre-proposal-69 delegation
// index without allocating one backing array per repeated address. The wire
// behavior matches protobuf unmarshalling: singular values use the final
// occurrence and repeated values preserve their field-local order. Messages
// containing unknown fields take the generated decoder fallback so its wire
// canonicalization behavior remains exact.
func DecodeDrAccountIndexLegacy(data []byte) (*corepb.DelegatedResourceAccountIndex, error) {
	plan, err := scanDrAccountIndex(data)
	if err != nil {
		return nil, err
	}
	// The generated decoder canonicalizes unknown field tags and values before
	// retaining them. Unknowns do not occur in the legacy schema, so preserve
	// exact protobuf behavior with a cold fallback instead of burdening the
	// normal address-arena path with a second canonical wire encoder.
	if plan.unknownBytes != 0 {
		var rec corepb.DelegatedResourceAccountIndex
		if err := proto.Unmarshal(data, &rec); err != nil {
			return nil, err
		}
		return &rec, nil
	}

	rec := &corepb.DelegatedResourceAccountIndex{Timestamp: plan.timestamp}
	if plan.fromCount+plan.toCount != 0 {
		accounts := make([][]byte, plan.fromCount+plan.toCount)
		if plan.fromCount != 0 {
			rec.FromAccounts = accounts[:plan.fromCount:plan.fromCount]
		}
		if plan.toCount != 0 {
			rec.ToAccounts = accounts[plan.fromCount:len(accounts):len(accounts)]
		}
	}

	var arena []byte
	if plan.ownedBytes != 0 {
		arena = make([]byte, plan.ownedBytes)
	}
	ownedPos := 0
	cloneOwned := func(value []byte) []byte {
		if len(value) == 0 {
			return []byte{}
		}
		start := ownedPos
		ownedPos += copy(arena[ownedPos:], value)
		return arena[start:ownedPos:ownedPos]
	}
	if plan.accountSeen {
		rec.Account = cloneOwned(plan.account)
	}

	fromPos, toPos := 0, 0
	for len(data) != 0 {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		fieldSize := tagSize + valueSize
		fieldValue := data[tagSize:fieldSize]

		switch {
		case number == 1 && wireType == protowire.BytesType:
			// The final account occurrence was copied from the scan plan.
		case number == 2 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldValue)
			rec.FromAccounts[fromPos] = cloneOwned(value)
			fromPos++
		case number == 3 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldValue)
			rec.ToAccounts[toPos] = cloneOwned(value)
			toPos++
		case number == 4 && wireType == protowire.VarintType:
			// The final timestamp occurrence was retained by the scan.
		default:
			panic("dr account index: unknown field escaped generated fallback")
		}
		data = data[fieldSize:]
	}
	return rec, nil
}
