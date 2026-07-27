package rawdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// CountBlockTransactions returns the number of transactions in a marshalled
// corepb.Block without unmarshalling the block.
func CountBlockTransactions(blockData []byte) (int, error) {
	count, err := countRepeatedBytesField(blockData, 1)
	if err != nil {
		return 0, fmt.Errorf("count block transactions: %w", err)
	}
	return count, nil
}

// CompactTransactionInfoIDsForBlock validates the ordinal relationship
// against transaction hashes derived directly from the marshalled block before
// removing IDs. A valid-but-unexpected historical row is returned unchanged.
func CompactTransactionInfoIDsForBlock(retData, blockData []byte) (data []byte, infos, removed int, err error) {
	hashes, err := TransactionHashesFromBlock(blockData)
	if err != nil {
		return nil, 0, 0, err
	}
	return CompactTransactionInfoIDsForHashes(retData, hashes)
}

// CompactTransactionInfoIDsForHashes is the allocation-bounded variant used
// by the freezer after it extracts hashes while the raw block view is valid.
func CompactTransactionInfoIDsForHashes(retData []byte, hashes [][sha256.Size]byte) (data []byte, infos, removed int, err error) {
	infoCount, err := countRepeatedBytesField(retData, 3)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("count transaction infos: %w", err)
	}
	if len(hashes) != infoCount {
		return retData, infoCount, 0, nil
	}
	matched := true
	if err := VisitTransactionInfoIDs(retData, func(ordinal int, id []byte) error {
		if ordinal >= len(hashes) || !bytes.Equal(id, hashes[ordinal][:]) {
			matched = false
		}
		return nil
	}); err != nil {
		return nil, 0, 0, err
	}
	if !matched {
		return retData, infoCount, 0, nil
	}
	return CompactTransactionInfoIDs(retData, infoCount)
}

// CompactTransactionInfoIDs removes the repeated transaction hash
// from every TransactionInfo in a TransactionRet row. The hash is already the
// key of the hot tx-* reverse index and can be reconstructed from the block's
// transaction at the same ordinal.
//
// Compaction is deliberately conditional on an exact block/info count match.
// A malformed or historically exceptional row is returned unchanged rather
// than risking an ordinal mismatch. All fields other than TransactionInfo.id,
// including unknown protobuf fields and their original wire encodings, are
// preserved byte-for-byte.
func CompactTransactionInfoIDs(retData []byte, expectedInfos int) (data []byte, infos, removed int, err error) {
	infoCount, err := countRepeatedBytesField(retData, 3)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("count transaction infos: %w", err)
	}
	if expectedInfos != infoCount {
		return retData, infoCount, 0, nil
	}
	if infoCount == 0 {
		return retData, 0, 0, nil
	}

	out := make([]byte, 0, len(retData))
	remaining := retData
	for len(remaining) > 0 {
		field := remaining
		number, wireType, tagLen := protowire.ConsumeTag(remaining)
		if tagLen < 0 {
			return nil, 0, 0, protowire.ParseError(tagLen)
		}
		remaining = remaining[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(number, wireType, remaining)
		if fieldLen < 0 {
			return nil, 0, 0, protowire.ParseError(fieldLen)
		}
		totalLen := tagLen + fieldLen
		if number != 3 || wireType != protowire.BytesType {
			out = append(out, field[:totalLen]...)
			remaining = remaining[fieldLen:]
			continue
		}

		payload, payloadLen := protowire.ConsumeBytes(remaining)
		if payloadLen < 0 {
			return nil, 0, 0, protowire.ParseError(payloadLen)
		}
		compact, dropped, compactErr := stripTransactionInfoID(payload)
		if compactErr != nil {
			return nil, 0, 0, compactErr
		}
		out = protowire.AppendTag(out, 3, protowire.BytesType)
		out = protowire.AppendBytes(out, compact)
		removed += dropped
		remaining = remaining[fieldLen:]
	}
	if removed == 0 {
		return retData, infoCount, 0, nil
	}
	return out, infoCount, removed, nil
}

// CompactAncientV2Record adapts transaction-info ID compaction to the V2
// migration transform hook. Non-tx_infos tables pass through unchanged.
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

// VisitValidatedTransactionInfoIDs visits only after verifying that every
// present TransactionInfo.id equals the hash of the transaction at the same
// block ordinal. A count or hash mismatch returns an error and invokes no
// callback.
func VisitValidatedTransactionInfoIDs(retData, blockData []byte, visit func(ordinal int, id []byte) error) error {
	hashes, err := TransactionHashesFromBlock(blockData)
	if err != nil {
		return err
	}
	infoCount, err := countRepeatedBytesField(retData, 3)
	if err != nil {
		return err
	}
	if len(hashes) != infoCount {
		return fmt.Errorf("transaction/info count mismatch: %d/%d", len(hashes), infoCount)
	}
	type location struct {
		ordinal int
		id      []byte
	}
	locations := make([]location, 0, infoCount)
	if err := VisitTransactionInfoIDs(retData, func(ordinal int, id []byte) error {
		if ordinal >= len(hashes) || !bytes.Equal(id, hashes[ordinal][:]) {
			return fmt.Errorf("transaction info ID mismatch at ordinal %d", ordinal)
		}
		locations = append(locations, location{ordinal: ordinal, id: id})
		return nil
	}); err != nil {
		return err
	}
	for _, location := range locations {
		if err := visit(location.ordinal, location.id); err != nil {
			return err
		}
	}
	return nil
}

// TransactionHashesFromBlock derives transaction IDs directly from protobuf
// wire bytes without constructing a Block or Transaction object.
func TransactionHashesFromBlock(data []byte) ([][sha256.Size]byte, error) {
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
			rawData, err := transactionRawData(transaction)
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

func transactionRawData(data []byte) ([]byte, error) {
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
	// A nil raw_data hashes to the zero value in types.Transaction.Hash rather
	// than SHA-256(empty), so refuse to compact this exceptional transaction.
	return nil, fmt.Errorf("transaction has no raw_data")
}

// VisitTransactionInfoIDs visits TransactionInfo.id fields in repeated-info
// order without unmarshalling the enclosing TransactionRet. The id slice is
// valid only until visit returns and must not be retained.
func VisitTransactionInfoIDs(data []byte, visit func(ordinal int, id []byte) error) error {
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
			if len(id) > 0 {
				if err := visit(ordinal, id); err != nil {
					return err
				}
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

func stripTransactionInfoID(data []byte) ([]byte, int, error) {
	var out []byte
	remaining := data
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
		if number == 1 && wireType == protowire.BytesType {
			if out == nil {
				out = make([]byte, 0, len(data)-totalLen)
				out = append(out, data[:len(data)-len(field)]...)
			}
			removed += totalLen
		} else if out != nil {
			out = append(out, field[:totalLen]...)
		}
		remaining = remaining[fieldLen:]
	}
	if out == nil {
		return data, 0, nil
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
