package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	transactionRetEnvelopeMagic      = "gtrcpt01"
	transactionRetEnvelopeVersion    = uint32(1)
	transactionRetEnvelopeHeaderSize = 32
	transactionRetExternalLogs       = uint32(1)
)

var transactionRetEnvelopeCRC = crc32.MakeTable(crc32.Castagnoli)

// ExternalizeTransactionInfoLogs removes the TransactionInfo.Log payloads
// from one already-validated TransactionRet row and wraps the remaining proto
// in a checksummed storage envelope. The envelope is an internal Ancient V2
// codec marker; public protobufs and wire responses remain unchanged.
//
// The caller must publish and authenticate an event-log snapshot covering the
// complete block before making this row visible. Readers fail closed when the
// matching cold coverage is unavailable, and reconstruct the exact Log slices
// from that immutable sidecar when it is present.
func ExternalizeTransactionInfoLogs(data []byte) ([]byte, uint64, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("rawdb: cannot externalize empty transaction ret")
	}
	if bytes.HasPrefix(data, []byte(transactionRetEnvelopeMagic)) {
		return nil, 0, errors.New("rawdb: transaction ret is already enveloped")
	}
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(data, &ret); err != nil {
		return nil, 0, fmt.Errorf("rawdb: decode transaction ret for log externalization: %w", err)
	}
	var logCount uint64
	for index, info := range ret.Transactioninfo {
		if info == nil {
			return nil, 0, fmt.Errorf("rawdb: nil transaction info %d during log externalization", index)
		}
		if uint64(len(info.Log)) > math.MaxUint64-logCount {
			return nil, 0, errors.New("rawdb: transaction log count overflow")
		}
		logCount += uint64(len(info.Log))
		info.Log = nil
	}
	// The overwhelming majority of ordinary transfers have no logs. Leaving
	// those rows as their already-compact protobuf avoids paying an envelope
	// header on every block while remaining unambiguous: only rows that actually
	// externalize payload need the reconstruction marker.
	if logCount == 0 {
		return data, 0, nil
	}
	payload, err := proto.Marshal(&ret)
	if err != nil {
		return nil, 0, fmt.Errorf("rawdb: encode transaction ret after log externalization: %w", err)
	}
	out := make([]byte, transactionRetEnvelopeHeaderSize+len(payload))
	copy(out[:8], transactionRetEnvelopeMagic)
	binary.BigEndian.PutUint32(out[8:12], transactionRetEnvelopeVersion)
	binary.BigEndian.PutUint32(out[12:16], transactionRetExternalLogs)
	binary.BigEndian.PutUint64(out[16:24], logCount)
	binary.BigEndian.PutUint32(out[24:28], crc32.Checksum(payload, transactionRetEnvelopeCRC))
	binary.BigEndian.PutUint32(out[28:32], crc32.Checksum(out[:28], transactionRetEnvelopeCRC))
	copy(out[transactionRetEnvelopeHeaderSize:], payload)
	// Do not make the logical row larger for a tiny receipt log. Keeping the
	// ordinary protobuf in that case is both unambiguous and cheaper; the log
	// remains self-contained and readers need no cold-sidecar reconstruction.
	if len(out) >= len(data) {
		return data, 0, nil
	}
	return out, uint64(len(data) - len(out)), nil
}

type decodedTransactionRetStorage struct {
	payload          []byte
	externalLogs     bool
	expectedLogCount uint64
}

func decodeTransactionRetStorage(data []byte) (decodedTransactionRetStorage, error) {
	if !bytes.HasPrefix(data, []byte(transactionRetEnvelopeMagic)) {
		return decodedTransactionRetStorage{payload: data}, nil
	}
	if len(data) < transactionRetEnvelopeHeaderSize {
		return decodedTransactionRetStorage{}, errors.New("rawdb: truncated transaction ret envelope")
	}
	if binary.BigEndian.Uint32(data[8:12]) != transactionRetEnvelopeVersion {
		return decodedTransactionRetStorage{}, errors.New("rawdb: unsupported transaction ret envelope version")
	}
	flags := binary.BigEndian.Uint32(data[12:16])
	if flags != transactionRetExternalLogs {
		return decodedTransactionRetStorage{}, fmt.Errorf("rawdb: unsupported transaction ret envelope flags %#x", flags)
	}
	if binary.BigEndian.Uint32(data[28:32]) != crc32.Checksum(data[:28], transactionRetEnvelopeCRC) {
		return decodedTransactionRetStorage{}, errors.New("rawdb: transaction ret envelope header checksum mismatch")
	}
	payload := data[transactionRetEnvelopeHeaderSize:]
	if binary.BigEndian.Uint32(data[24:28]) != crc32.Checksum(payload, transactionRetEnvelopeCRC) {
		return decodedTransactionRetStorage{}, errors.New("rawdb: transaction ret envelope payload checksum mismatch")
	}
	return decodedTransactionRetStorage{
		payload:          payload,
		externalLogs:     true,
		expectedLogCount: binary.BigEndian.Uint64(data[16:24]),
	}, nil
}

func hydrateExternalTransactionInfoLogs(db *ChainDB, blockNum uint64, infos []*corepb.TransactionInfo, expected uint64) error {
	if db == nil || db.eventLog == nil {
		return fmt.Errorf("rawdb: transaction receipts for block %d require cold event-log coverage", blockNum)
	}
	block, ok, err := ReadBlockStrict(db, blockNum)
	if err != nil {
		return fmt.Errorf("rawdb: read block %d for external transaction logs: %w", blockNum, err)
	}
	if !ok || block == nil {
		return fmt.Errorf("rawdb: block %d missing for external transaction logs", blockNum)
	}
	txs := block.Transactions()
	if err := ValidateTransactionInfosForBlock(blockNum, txs, infos, "external transaction logs"); err != nil {
		return err
	}
	for _, info := range infos {
		info.Log = nil
	}
	blockHash := block.Hash()
	var seen uint64
	appendLog := func(row EventLog) (bool, error) {
		if err := validateCoveredEventLogRow(blockNum, blockNum, row); err != nil {
			return false, err
		}
		if row.LogIndex != seen {
			return false, fmt.Errorf("rawdb: external log index %d for block %d, want %d", row.LogIndex, blockNum, seen)
		}
		if row.BlockHash != blockHash {
			return false, fmt.Errorf("rawdb: external log block hash %x for block %d does not match %x", row.BlockHash, blockNum, blockHash)
		}
		if row.TxIndex >= uint64(len(txs)) || txs[row.TxIndex] == nil {
			return false, fmt.Errorf("rawdb: external log transaction index %d outside block %d", row.TxIndex, blockNum)
		}
		wantTxHash := txs[row.TxIndex].Hash()
		if row.TxHash != wantTxHash {
			return false, fmt.Errorf("rawdb: external log transaction hash %x does not match block %d index %d hash %x", row.TxHash, blockNum, row.TxIndex, wantTxHash)
		}
		if row.Log == nil {
			return false, fmt.Errorf("rawdb: external log block=%d tx=%d index=%d is nil", blockNum, row.TxIndex, row.LogIndex)
		}
		infos[row.TxIndex].Log = append(infos[row.TxIndex].Log, proto.Clone(row.Log).(*corepb.TransactionInfo_Log))
		seen++
		return true, nil
	}

	if coveredReader, ok := db.eventLog.(CoveredEventLogReader); ok {
		covered, err := coveredReader.IterateCoveredEventLogs(blockNum, blockNum, EventLogFilter{}, appendLog)
		if err != nil {
			return fmt.Errorf("rawdb: read covered external transaction logs for block %d: %w", blockNum, err)
		}
		if !covered {
			return fmt.Errorf("rawdb: transaction receipts for block %d are outside cold event-log coverage", blockNum)
		}
	} else {
		covered, err := db.eventLog.EventLogRangeCovered(blockNum, blockNum)
		if err != nil {
			return fmt.Errorf("rawdb: verify external transaction log coverage for block %d: %w", blockNum, err)
		}
		if !covered {
			return fmt.Errorf("rawdb: transaction receipts for block %d are outside cold event-log coverage", blockNum)
		}
		if err := db.eventLog.IterateEventLogs(blockNum, blockNum, EventLogFilter{}, appendLog); err != nil {
			return fmt.Errorf("rawdb: read external transaction logs for block %d: %w", blockNum, err)
		}
	}
	if seen != expected {
		return fmt.Errorf("rawdb: external transaction log count %d for block %d, want %d", seen, blockNum, expected)
	}
	return nil
}

// CompactAncientV2RecordWithExternalLogs applies the ordinary receipt identity
// compaction first, then externalizes logs into the already-published event-log
// archive. Keeping the order stable makes the envelope's payload the smallest
// canonical protobuf representation.
func CompactAncientV2RecordWithExternalLogs(kind string, number uint64, data, body []byte) ([]byte, error) {
	compact, err := CompactAncientV2Record(kind, number, data, body)
	if err != nil || kind != ancientTxInfos {
		return compact, err
	}
	// Genesis is allowed to have no TransactionRet coverage. Preserve the
	// ancient empty row; there are no logs to externalize or hydrate.
	if number == 0 && len(compact) == 0 {
		return compact, nil
	}
	// CompactAncientV2Record deliberately preserves historically exceptional
	// rows instead of rejecting a migration. Apply the same rule here: only
	// split a receipt from its logs after re-verifying its ordinal identity
	// against the canonical body. Any unusual row stays self-contained.
	if !transactionInfoLogsMatchBody(number, compact, body) {
		return compact, nil
	}
	external, _, err := ExternalizeTransactionInfoLogs(compact)
	return external, err
}

func transactionInfoLogsMatchBody(number uint64, data, body []byte) bool {
	hashes, err := transactionHashesFromBlockWire(body)
	if err != nil {
		return false
	}
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(data, &ret); err != nil || !transactionInfoBlockNumberMatches(ret.BlockNumber, number) {
		return false
	}
	if number == 0 && len(ret.Transactioninfo) == 0 {
		return true
	}
	if len(ret.Transactioninfo) != len(hashes) {
		return false
	}
	for index, info := range ret.Transactioninfo {
		if info == nil || !transactionInfoBlockNumberMatches(info.BlockNumber, number) {
			return false
		}
		if len(info.Id) != 0 && !bytes.Equal(info.Id, hashes[index][:]) {
			return false
		}
	}
	return true
}
