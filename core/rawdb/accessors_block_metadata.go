package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	metadataBatchHeaderSize = 12
	// Pebble's deferred Set builder temporarily reserves maximum-width key and
	// value varints before shrinking the record. Exact final encoded size alone
	// therefore grows/copies the whole batch on its last row.
	metadataBatchRecordSlack = 2 * binary.MaxVarintLen64
)

type blockMetadataRow struct {
	key   []byte
	value []byte
}

// valueFuncBatch is implemented by Pebble batches that can expose their final
// value storage during construction. Large composite protobuf rows use it to
// encode once directly into the batch rather than allocating and then copying a
// same-sized temporary slice.
type valueFuncBatch interface {
	PutValueFunc(key []byte, valueLen int, fill func([]byte) error) error
}

// WriteBlockMetadataBatch atomically persists the hot block metadata rows with
// an encoded-size batch hint plus Pebble's one-record scratch allowance.
// Preparing the already-required protobuf payloads before constructing the
// batch avoids Pebble's geometric grow/copy cycle without an extra proto.Size
// traversal or a second marshal.
func WriteBlockMetadataBatch(db ethdb.Batcher, block *types.Block, stateRoot common.Hash, infos []*corepb.TransactionInfo) error {
	if db == nil || block == nil {
		return fmt.Errorf("write block metadata: nil database or block")
	}
	blockData, err := block.Marshal()
	if err != nil {
		return fmt.Errorf("marshal block: %w", err)
	}
	return WriteBlockMetadataBatchEncoded(db, block, blockData, stateRoot, infos)
}

// WriteBlockMetadataBatchEncoded is WriteBlockMetadataBatch with an immutable
// block protobuf payload already produced for the rewindable staged block row.
// Reusing it avoids marshaling the same block again in the durable publish
// tail. block remains the source of metadata indexes and must match blockData.
func WriteBlockMetadataBatchEncoded(db ethdb.Batcher, block *types.Block, blockData []byte, stateRoot common.Hash, infos []*corepb.TransactionInfo) error {
	if db == nil || block == nil {
		return fmt.Errorf("write block metadata: nil database or block")
	}
	blockHash := block.Hash()
	blockNum := block.Number()
	txs := block.Transactions()
	var numberValue [8]byte
	binary.BigEndian.PutUint64(numberValue[:], blockNum)
	var ringSlot [8]byte
	binary.BigEndian.PutUint64(ringSlot[:], blockNum%blockNumberHashSlots)
	ref := taposRefBytes(blockNum)

	keyBytes := len(blockStateRootPrefix) + len(blockHash) +
		len(blockPrefix) + len(numberValue) +
		len(blockHashPrefix) + len(blockHash) +
		len(blockNumberHashPrefix) + len(ringSlot) +
		len(taposPrefix) + len(ref) +
		len(txInfoBlockPrefix) + len(numberValue) +
		len(txs)*(len(txPrefix)+common.HashLength)
	for _, info := range infos {
		keyBytes += len(txInfoPrefix) + len(info.Id)
	}
	keyArena := make([]byte, keyBytes)
	keyOffset := 0
	metadataKey := func(prefix, suffix []byte) []byte {
		start := keyOffset
		keyOffset += len(prefix) + len(suffix)
		key := keyArena[start:keyOffset:keyOffset]
		n := copy(key, prefix)
		copy(key[n:], suffix)
		return key
	}

	rows := make([]blockMetadataRow, 0, 5+len(infos)+len(txs))
	rows = append(rows,
		blockMetadataRow{key: metadataKey(blockStateRootPrefix, blockHash[:]), value: stateRoot[:]},
		blockMetadataRow{key: metadataKey(blockPrefix, numberValue[:]), value: blockData},
		blockMetadataRow{key: metadataKey(blockHashPrefix, blockHash[:]), value: numberValue[:]},
		blockMetadataRow{key: metadataKey(blockNumberHashPrefix, ringSlot[:]), value: blockHash[:]},
		blockMetadataRow{key: metadataKey(taposPrefix, ref[:]), value: blockHash[8:16]},
	)
	infoRowStart := len(rows)
	for _, info := range infos {
		data, err := proto.Marshal(info)
		if err != nil {
			return fmt.Errorf("marshal tx info: %w", err)
		}
		rows = append(rows, blockMetadataRow{key: metadataKey(txInfoPrefix, info.Id), value: data})
	}
	var blockTimestamp int64
	if len(infos) > 0 {
		blockTimestamp = infos[0].BlockTimeStamp
	}
	infoRows := rows[infoRowStart:]
	retKey := metadataKey(txInfoBlockPrefix, numberValue[:])
	retSize := transactionRetRowsSize(int64(blockNum), blockTimestamp, infoRows)
	for _, tx := range txs {
		hash := tx.Hash()
		rows = append(rows, blockMetadataRow{key: metadataKey(txPrefix, hash[:]), value: numberValue[:]})
	}

	encodedSize := metadataBatchHeaderSize
	for _, row := range rows {
		encodedSize += metadataBatchSetRecordSize(row.key, row.value)
	}
	encodedSize += metadataBatchSetRecordSizeLen(len(retKey), retSize)
	batch := db.NewBatchWithSize(encodedSize + metadataBatchRecordSlack)
	defer closeMetadataBatch(batch)
	for _, row := range rows {
		if err := batch.Put(row.key, row.value); err != nil {
			return fmt.Errorf("write block metadata row: %w", err)
		}
	}
	if direct, ok := batch.(valueFuncBatch); ok {
		if err := direct.PutValueFunc(retKey, retSize, func(dst []byte) error {
			encoded := appendTransactionRetRows(dst[:0], int64(blockNum), blockTimestamp, infoRows)
			if len(encoded) != len(dst) {
				return fmt.Errorf("transaction ret encoded size %d, want %d", len(encoded), len(dst))
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write transaction ret row: %w", err)
		}
	} else {
		retData := marshalTransactionRetRows(int64(blockNum), blockTimestamp, infoRows)
		if err := batch.Put(retKey, retData); err != nil {
			return fmt.Errorf("write transaction ret row: %w", err)
		}
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write block metadata batch: %w", err)
	}
	return nil
}

// marshalTransactionRetRows builds the TransactionRet wire payload around
// TransactionInfo messages that WriteBlockMetadataBatch has already marshaled
// for the per-transaction index. Calling proto.Marshal on TransactionRet would
// traverse and marshal every nested info a second time.
//
// TransactionRet's schema is three ascending fields:
//
//	1: int64 blockNumber
//	2: int64 blockTimeStamp
//	3: repeated TransactionInfo transactioninfo
//
// Mirroring proto3's zero-value omission and generated field order produces
// the same wire bytes when given the same nested info payloads, while retaining
// unknown fields and map ordering exactly as encoded in each info row.
func marshalTransactionRetRows(blockNumber, blockTimestamp int64, infoRows []blockMetadataRow) []byte {
	size := transactionRetRowsSize(blockNumber, blockTimestamp, infoRows)
	return appendTransactionRetRows(make([]byte, 0, size), blockNumber, blockTimestamp, infoRows)
}

func transactionRetRowsSize(blockNumber, blockTimestamp int64, infoRows []blockMetadataRow) int {
	size := 0
	if blockNumber != 0 {
		size += protowire.SizeTag(1) + protowire.SizeVarint(uint64(blockNumber))
	}
	if blockTimestamp != 0 {
		size += protowire.SizeTag(2) + protowire.SizeVarint(uint64(blockTimestamp))
	}
	for _, row := range infoRows {
		size += protowire.SizeTag(3) + protowire.SizeBytes(len(row.value))
	}
	return size
}

func appendTransactionRetRows(data []byte, blockNumber, blockTimestamp int64, infoRows []blockMetadataRow) []byte {
	if blockNumber != 0 {
		data = protowire.AppendTag(data, 1, protowire.VarintType)
		data = protowire.AppendVarint(data, uint64(blockNumber))
	}
	if blockTimestamp != 0 {
		data = protowire.AppendTag(data, 2, protowire.VarintType)
		data = protowire.AppendVarint(data, uint64(blockTimestamp))
	}
	for _, row := range infoRows {
		data = protowire.AppendTag(data, 3, protowire.BytesType)
		data = protowire.AppendBytes(data, row.value)
	}
	return data
}

func metadataBatchSetRecordSize(key, value []byte) int {
	return metadataBatchSetRecordSizeLen(len(key), len(value))
}

func metadataBatchSetRecordSizeLen(keyLen, valueLen int) int {
	return 1 + metadataUvarintSize(keyLen) + keyLen + metadataUvarintSize(valueLen) + valueLen
}

func metadataUvarintSize(v int) int {
	size := 1
	for v >= 1<<7 {
		v >>= 7
		size++
	}
	return size
}

func closeMetadataBatch(batch ethdb.Batch) {
	if closer, ok := batch.(interface{ Close() }); ok {
		closer.Close()
	}
}
