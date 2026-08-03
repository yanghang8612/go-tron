package rawdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// CompactAncientV2Record adapts transaction-info ID compaction to the Ancient
// V2 migration hook. Non-receipt tables pass through unchanged. Malformed or
// historically exceptional rows are deliberately preserved so migration can
// never turn an unusual ordinal relationship into corrupt data.
func CompactAncientV2Record(kind string, _ uint64, data, body []byte) ([]byte, error) {
	if kind != ancientTxInfos {
		return data, nil
	}
	hashes, err := transactionHashesFromBlockWire(body)
	if err != nil {
		return data, nil
	}
	compact, err := compactTransactionInfoIDsForHashes(data, hashes)
	if err != nil {
		return data, nil
	}
	return compact, nil
}

func compactTransactionInfoIDsForHashes(retData []byte, hashes [][sha256.Size]byte) ([]byte, error) {
	infoCount, err := countRepeatedBytesField(retData, 3)
	if err != nil || len(hashes) != infoCount {
		return retData, err
	}
	matched := true
	if err := visitTransactionInfoIDs(retData, func(ordinal int, id []byte) error {
		if ordinal >= len(hashes) || (len(id) != 0 && !bytes.Equal(id, hashes[ordinal][:])) {
			matched = false
		}
		return nil
	}); err != nil {
		return retData, err
	}
	if !matched || infoCount == 0 {
		return retData, nil
	}
	return stripTransactionInfoIDs(retData)
}

// transactionHashesFromBlockWire derives transaction IDs without allocating
// full Block and Transaction protobuf graphs. A TRON transaction ID is the
// SHA-256 hash of Transaction.raw_data.
func transactionHashesFromBlockWire(data []byte) ([][sha256.Size]byte, error) {
	count, err := countRepeatedBytesField(data, 1)
	if err != nil {
		return nil, fmt.Errorf("count block transactions: %w", err)
	}
	hashes := make([][sha256.Size]byte, 0, count)
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		if number == 1 && wireType == protowire.BytesType {
			transaction, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return nil, protowire.ParseError(fieldLen)
			}
			rawData, err := transactionRawDataWire(transaction)
			if err != nil {
				return nil, err
			}
			hashes = append(hashes, sha256.Sum256(rawData))
			data = data[fieldLen:]
			continue
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil, protowire.ParseError(fieldLen)
		}
		data = data[fieldLen:]
	}
	return hashes, nil
}

func transactionRawDataWire(data []byte) ([]byte, error) {
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		if number == 1 && wireType == protowire.BytesType {
			raw, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return nil, protowire.ParseError(fieldLen)
			}
			return raw, nil
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil, protowire.ParseError(fieldLen)
		}
		data = data[fieldLen:]
	}
	return nil, fmt.Errorf("transaction has no raw_data")
}

func visitTransactionInfoIDs(data []byte, visit func(ordinal int, id []byte) error) error {
	ordinal := 0
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		if number == 3 && wireType == protowire.BytesType {
			payload, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return protowire.ParseError(fieldLen)
			}
			id, err := transactionInfoWireID(payload)
			if err != nil {
				return err
			}
			if err := visit(ordinal, id); err != nil {
				return err
			}
			ordinal++
			data = data[fieldLen:]
			continue
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return protowire.ParseError(fieldLen)
		}
		data = data[fieldLen:]
	}
	return nil
}

func transactionInfoWireID(data []byte) ([]byte, error) {
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		if number == 1 && wireType == protowire.BytesType {
			id, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return nil, protowire.ParseError(fieldLen)
			}
			return id, nil
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil, protowire.ParseError(fieldLen)
		}
		data = data[fieldLen:]
	}
	return nil, nil
}

func stripTransactionInfoIDs(retData []byte) ([]byte, error) {
	out := make([]byte, 0, len(retData))
	remaining := retData
	for len(remaining) > 0 {
		field := remaining
		number, wireType, tagLen := protowire.ConsumeTag(remaining)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		remaining = remaining[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, remaining)
		if fieldLen < 0 {
			return nil, protowire.ParseError(fieldLen)
		}
		totalLen := tagLen + fieldLen
		if number != 3 || wireType != protowire.BytesType {
			out = append(out, field[:totalLen]...)
			remaining = remaining[fieldLen:]
			continue
		}
		payload, payloadLen := protowire.ConsumeBytes(remaining)
		if payloadLen < 0 {
			return nil, protowire.ParseError(payloadLen)
		}
		compact, err := stripTransactionInfoID(payload)
		if err != nil {
			return nil, err
		}
		out = protowire.AppendTag(out, 3, protowire.BytesType)
		out = protowire.AppendBytes(out, compact)
		remaining = remaining[fieldLen:]
	}
	return out, nil
}

func stripTransactionInfoID(data []byte) ([]byte, error) {
	out := make([]byte, 0, len(data))
	for len(data) > 0 {
		field := data
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil, protowire.ParseError(fieldLen)
		}
		if number != 1 || wireType != protowire.BytesType {
			out = append(out, field[:tagLen+fieldLen]...)
		}
		data = data[fieldLen:]
	}
	return out, nil
}

func countRepeatedBytesField(data []byte, wanted protowire.Number) (int, error) {
	count := 0
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return 0, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return 0, protowire.ParseError(fieldLen)
		}
		if number == wanted && wireType == protowire.BytesType {
			count++
		}
		data = data[fieldLen:]
	}
	return count, nil
}
