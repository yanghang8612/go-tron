package rawdb

import (
	"fmt"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// PrepareAncientV2TransactionInfos performs the strict direct-freezer coverage
// checks before applying the ordinary Ancient V2 receipt codec. The decoded
// receipt belongs exclusively to this call and is consumed by externalization;
// data and body remain immutable. Typed transaction hashes validate coverage,
// while the codec independently uses body wire hashes to decide ID stripping.
func PrepareAncientV2TransactionInfos(number uint64, txs []*types.Transaction, data, body []byte, externalize bool) ([]byte, error) {
	if len(data) == 0 {
		if number == 0 {
			if err := ValidateTransactionInfosForBlock(number, txs, nil, "chain freezer append"); err != nil {
				return nil, err
			}
		} else if len(txs) != 0 {
			return nil, fmt.Errorf("freezer: missing transaction info coverage for block %d with %d transactions", number, len(txs))
		}
		// Preserve the codec's existing empty-row behavior, including the
		// externalizer's error for an empty non-genesis row.
		if externalize {
			return CompactAncientV2RecordWithExternalLogs(ancientTxInfos, number, data, body)
		}
		return CompactAncientV2Record(ancientTxInfos, number, data, body)
	}
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(data, &ret); err != nil {
		return nil, fmt.Errorf("freezer: decode tx infos for block %d: %w", number, err)
	}
	if ret.BlockNumber != 0 && ret.BlockNumber != int64(number) {
		return nil, fmt.Errorf("freezer: tx infos row %d contains block number %d", number, ret.BlockNumber)
	}
	if err := ValidateTransactionInfosForBlock(number, txs, ret.Transactioninfo, "chain freezer append"); err != nil {
		return nil, err
	}
	if !externalize {
		return CompactAncientV2Record(ancientTxInfos, number, data, body)
	}

	// These wire-derived hashes must not be replaced with tx.Hash(). In
	// particular duplicate/noncanonical raw_data encodings can differ.
	hashes, err := transactionHashesFromBlockWire(body)
	if err != nil {
		return data, nil
	}
	compact, _, removed, err := compactTransactionInfoIDsForHashes(data, hashes)
	if err != nil {
		compact, removed = data, 0
	}
	if removed > 0 {
		// The wire stripper removed every bytes-typed ID field, including
		// duplicates. Synchronize only those known fields in our owned graph;
		// unknown fields and all other decoded values are unchanged.
		for _, info := range ret.Transactioninfo {
			info.Id = nil
		}
	}
	if !transactionInfoLogsMatchHashes(number, &ret, hashes) {
		return compact, nil
	}
	external, _, err := externalizeDecodedTransactionInfoLogs(compact, &ret)
	return external, err
}
