package rawdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
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
	compact, _, _, err := CompactTransactionInfoIDsForBlock(data, body)
	if err != nil {
		return data, nil
	}
	return compact, nil
}

// CompactTransactionInfoIDsForBlock validates receipt IDs against transaction
// hashes derived directly from the matching block wire bytes before removing
// them. Unexpected historical rows pass through unchanged.
func CompactTransactionInfoIDsForBlock(retData, blockData []byte) (data []byte, infos, removed int, err error) {
	hashes, err := transactionHashesFromBlockWire(blockData)
	if err != nil {
		return nil, 0, 0, err
	}
	return compactTransactionInfoIDsForHashes(retData, hashes)
}

// AncientTransactionIndexEntries derives immutable index rows directly from
// the canonical block wire encoding. It deliberately shares the lightweight
// transaction-ID parser used by receipt compaction, avoiding allocation of a
// complete Block/Transaction protobuf graph during Ancient V2 migration.
func AncientTransactionIndexEntries(blockNum uint64, blockData []byte) ([]rawdbfreezer.TransactionIndexEntry, error) {
	hashes, err := transactionHashesFromBlockWire(blockData)
	if err != nil {
		return nil, err
	}
	entries := make([]rawdbfreezer.TransactionIndexEntry, len(hashes))
	for ordinal, hash := range hashes {
		location, err := EncodeTransactionLocation(blockNum, ordinal)
		if err != nil {
			return nil, err
		}
		entries[ordinal] = rawdbfreezer.TransactionIndexEntry{Hash: hash, Location: location}
	}
	return entries, nil
}

func compactTransactionInfoIDsForHashes(retData []byte, hashes [][sha256.Size]byte) (data []byte, infos, removed int, err error) {
	infoCount, err := countRepeatedBytesField(retData, 3)
	if err != nil || len(hashes) != infoCount {
		return retData, infoCount, 0, err
	}
	matched := true
	if err := visitTransactionInfoIDs(retData, func(ordinal int, id []byte) error {
		if ordinal >= len(hashes) || (len(id) != 0 && !bytes.Equal(id, hashes[ordinal][:])) {
			matched = false
		}
		return nil
	}); err != nil {
		return retData, infoCount, 0, err
	}
	if !matched || infoCount == 0 {
		return retData, infoCount, 0, nil
	}
	compact, removed, err := stripTransactionInfoIDs(retData)
	return compact, infoCount, removed, err
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

func stripTransactionInfoIDs(retData []byte) ([]byte, int, error) {
	out := make([]byte, 0, len(retData))
	remaining := retData
	removed := 0
	for len(remaining) > 0 {
		field := remaining
		number, wireType, tagLen := protowire.ConsumeTag(remaining)
		if tagLen < 0 {
			return nil, 0, protowire.ParseError(tagLen)
		}
		remaining = remaining[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, remaining)
		if fieldLen < 0 {
			return nil, 0, protowire.ParseError(fieldLen)
		}
		totalLen := tagLen + fieldLen
		if number != 3 || wireType != protowire.BytesType {
			out = append(out, field[:totalLen]...)
			remaining = remaining[fieldLen:]
			continue
		}
		payload, payloadLen := protowire.ConsumeBytes(remaining)
		if payloadLen < 0 {
			return nil, 0, protowire.ParseError(payloadLen)
		}
		compact, dropped, err := stripTransactionInfoID(payload)
		if err != nil {
			return nil, 0, err
		}
		removed += dropped
		out = protowire.AppendTag(out, 3, protowire.BytesType)
		out = protowire.AppendBytes(out, compact)
		remaining = remaining[fieldLen:]
	}
	if removed == 0 {
		return retData, 0, nil
	}
	return out, removed, nil
}

func stripTransactionInfoID(data []byte) ([]byte, int, error) {
	original := data
	out := make([]byte, 0, len(data))
	removed := 0
	for len(data) > 0 {
		field := data
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, 0, protowire.ParseError(tagLen)
		}
		data = data[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil, 0, protowire.ParseError(fieldLen)
		}
		if number != 1 || wireType != protowire.BytesType {
			out = append(out, field[:tagLen+fieldLen]...)
		} else {
			removed += tagLen + fieldLen
		}
		data = data[fieldLen:]
	}
	if removed == 0 {
		return original, 0, nil
	}
	return out, removed, nil
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
