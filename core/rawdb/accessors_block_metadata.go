package rawdb

import (
	"encoding/binary"
	"fmt"
	"sync"

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

type blockMetadataScratch struct {
	bytes        []byte
	rows         []blockMetadataRow
	infoPayloads [][]byte
}

const (
	maxPooledMetadataInfoArena    = 4 << 20
	maxPooledMetadataScratchBytes = 4 << 20
	maxPooledMetadataScratchRows  = 1 << 16
)

var metadataInfoArenaPool = sync.Pool{
	New: func() any {
		arena := make([]byte, 0, 64<<10)
		return &arena
	},
}

var blockMetadataScratchPool = sync.Pool{
	New: func() any {
		return &blockMetadataScratch{
			bytes:        make([]byte, 0, 64<<10),
			rows:         make([]blockMetadataRow, 0, 256),
			infoPayloads: make([][]byte, 0, 256),
		}
	},
}

func borrowBlockMetadataScratch(byteCount, rowCount, infoCount int) *blockMetadataScratch {
	scratch := blockMetadataScratchPool.Get().(*blockMetadataScratch)
	if cap(scratch.bytes) < byteCount {
		scratch.bytes = make([]byte, byteCount)
	} else {
		scratch.bytes = scratch.bytes[:byteCount]
	}
	if cap(scratch.rows) < rowCount {
		scratch.rows = make([]blockMetadataRow, 0, rowCount)
	} else {
		scratch.rows = scratch.rows[:0]
	}
	if cap(scratch.infoPayloads) < infoCount {
		scratch.infoPayloads = make([][]byte, 0, infoCount)
	} else {
		scratch.infoPayloads = scratch.infoPayloads[:0]
	}
	return scratch
}

func returnBlockMetadataScratch(scratch *blockMetadataScratch) {
	clear(scratch.rows)
	clear(scratch.infoPayloads)
	if cap(scratch.bytes) > maxPooledMetadataScratchBytes ||
		cap(scratch.rows) > maxPooledMetadataScratchRows ||
		cap(scratch.infoPayloads) > maxPooledMetadataScratchRows {
		return
	}
	scratch.bytes = scratch.bytes[:0]
	scratch.rows = scratch.rows[:0]
	scratch.infoPayloads = scratch.infoPayloads[:0]
	blockMetadataScratchPool.Put(scratch)
}

func borrowMetadataInfoArena() *[]byte {
	arenaPtr := metadataInfoArenaPool.Get().(*[]byte)
	*arenaPtr = (*arenaPtr)[:0]
	return arenaPtr
}

func returnMetadataInfoArena(arenaPtr *[]byte) {
	if cap(*arenaPtr) > maxPooledMetadataInfoArena {
		return
	}
	*arenaPtr = (*arenaPtr)[:0]
	metadataInfoArenaPool.Put(arenaPtr)
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
	return writeBlockMetadataBatchEncoded(db, block, blockData, stateRoot, infos, true, true)
}

// WriteStoredReplayBlockMetadataBatch rebuilds the mutable metadata removed by
// ResetMutableState without rewriting the canonical block body. Stored replay
// can only start after ReadBlock has proven the body exists in the preserved
// hot/freezer chain store; writing that often-large immutable row again adds WAL,
// memtable, and compaction traffic but cannot change replay state. The block-hash
// index is still refreshed because it is tiny and repairs legacy/incomplete
// indexes at negligible cost. When transactionInfosAlreadyAncient is true the
// canonical TransactionRet is also omitted, while every reset tx-hash location
// index is still rebuilt.
func WriteStoredReplayBlockMetadataBatch(db ethdb.Batcher, block *types.Block, stateRoot common.Hash, infos []*corepb.TransactionInfo, transactionInfosAlreadyAncient bool) error {
	return writeBlockMetadataBatchEncoded(db, block, nil, stateRoot, infos, false, !transactionInfosAlreadyAncient)
}

func writeBlockMetadataBatchEncoded(db ethdb.Batcher, block *types.Block, blockData []byte, stateRoot common.Hash, infos []*corepb.TransactionInfo, includeBlockBody, includeTransactionRet bool) error {
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
		len(blockHashPrefix) + len(blockHash) +
		len(blockNumberHashPrefix) + len(ringSlot) +
		len(taposPrefix) + len(ref) +
		len(txs)*(len(txPrefix)+common.HashLength)
	if includeBlockBody {
		keyBytes += len(blockPrefix) + len(numberValue)
	}
	if includeTransactionRet {
		keyBytes += len(txInfoBlockPrefix) + len(numberValue)
	}
	// Keys, transaction-location values, and slice descriptors are needed only
	// until the batch has copied them. Reuse one scratch object across blocks so
	// the durable metadata tail does not create four short-lived heap objects per
	// block. Capacity limits in returnBlockMetadataScratch keep outliers from
	// pinning unusually large buffers in the pool.
	rowCapacity := 4 + len(txs)
	if includeBlockBody {
		rowCapacity++
	}
	infoCapacity := 0
	if includeTransactionRet {
		infoCapacity = len(infos)
	}
	scratch := borrowBlockMetadataScratch(keyBytes+len(txs)*8, rowCapacity, infoCapacity)
	defer returnBlockMetadataScratch(scratch)
	keyArena := scratch.bytes[:keyBytes:keyBytes]
	keyOffset := 0
	metadataKey := func(prefix, suffix []byte) []byte {
		start := keyOffset
		keyOffset += len(prefix) + len(suffix)
		key := keyArena[start:keyOffset:keyOffset]
		n := copy(key, prefix)
		copy(key[n:], suffix)
		return key
	}

	rows := scratch.rows
	rows = append(rows, blockMetadataRow{key: metadataKey(blockStateRootPrefix, blockHash[:]), value: stateRoot[:]})
	if includeBlockBody {
		rows = append(rows, blockMetadataRow{key: metadataKey(blockPrefix, numberValue[:]), value: blockData})
	}
	rows = append(rows,
		blockMetadataRow{key: metadataKey(blockHashPrefix, blockHash[:]), value: numberValue[:]},
		blockMetadataRow{key: metadataKey(blockNumberHashPrefix, ringSlot[:]), value: blockHash[:]},
		blockMetadataRow{key: metadataKey(taposPrefix, ref[:]), value: blockHash[8:16]},
	)
	// Marshal every TransactionInfo once into a pooled temporary arena. The
	// payload slices feed the enclosing TransactionRet row; individual ti-* rows
	// are intentionally no longer written because tx-* plus tib-*/ancient
	// tx_infos provides the same lookup without duplicating the payload forever.
	var (
		infoArena    []byte
		infoArenaPtr *[]byte
		infoPayloads = scratch.infoPayloads
	)
	if includeTransactionRet && len(infos) > 0 {
		infoArenaPtr = borrowMetadataInfoArena()
		infoArena = *infoArenaPtr
		defer returnMetadataInfoArena(infoArenaPtr)
	}
	for _, info := range infos {
		start := len(infoArena)
		var err error
		infoArena, err = proto.MarshalOptions{}.MarshalAppend(infoArena, info)
		if err != nil {
			return fmt.Errorf("marshal tx info: %w", err)
		}
		infoPayloads = append(infoPayloads, infoArena[start:len(infoArena):len(infoArena)])
	}
	if infoArenaPtr != nil {
		// MarshalAppend may grow the borrowed slice on an unusually large block.
		// Publish that final backing array to the pool so subsequent blocks reuse
		// the new capacity instead of repeating the growth.
		*infoArenaPtr = infoArena
	}
	var blockTimestamp int64
	if len(infos) > 0 {
		blockTimestamp = infos[0].BlockTimeStamp
	}
	txLocationValues := scratch.bytes[keyBytes : keyBytes+len(txs)*8 : keyBytes+len(txs)*8]
	for i, tx := range txs {
		hash := tx.Hash()
		if blockNum > transactionLocationMaxBlock || i > int(^uint16(0)) {
			return fmt.Errorf("write block metadata: transaction location block=%d ordinal=%d out of range", blockNum, i)
		}
		packed := transactionLocationMarker | blockNum<<transactionLocationOrdinalBits | uint64(i)
		value := txLocationValues[i*8 : (i+1)*8 : (i+1)*8]
		binary.BigEndian.PutUint64(value, packed)
		rows = append(rows, blockMetadataRow{key: metadataKey(txPrefix, hash[:]), value: value})
	}

	encodedSize := metadataBatchHeaderSize
	for _, row := range rows {
		encodedSize += metadataBatchSetRecordSize(row.key, row.value)
	}
	var retKey []byte
	retSize := 0
	if includeTransactionRet {
		retKey = metadataKey(txInfoBlockPrefix, numberValue[:])
		retSize = transactionRetRowsSize(int64(blockNum), blockTimestamp, infoPayloads)
		encodedSize += metadataBatchSetRecordSizeLen(len(retKey), retSize)
	}
	batch := db.NewBatchWithSize(encodedSize + metadataBatchRecordSlack)
	defer closeMetadataBatch(batch)
	for _, row := range rows {
		if err := batch.Put(row.key, row.value); err != nil {
			return fmt.Errorf("write block metadata row: %w", err)
		}
	}
	if !includeTransactionRet {
		if err := batch.Write(); err != nil {
			return fmt.Errorf("write block metadata batch: %w", err)
		}
		return nil
	}
	if direct, ok := batch.(valueFuncBatch); ok {
		if err := direct.PutValueFunc(retKey, retSize, func(dst []byte) error {
			encoded := appendTransactionRetRows(dst[:0], int64(blockNum), blockTimestamp, infoPayloads)
			if len(encoded) != len(dst) {
				return fmt.Errorf("transaction ret encoded size %d, want %d", len(encoded), len(dst))
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write transaction ret row: %w", err)
		}
	} else {
		retData := marshalTransactionRetRows(int64(blockNum), blockTimestamp, infoPayloads)
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
// for the canonical block-level row. Calling proto.Marshal on TransactionRet
// would traverse and marshal every nested info a second time.
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
func marshalTransactionRetRows(blockNumber, blockTimestamp int64, infoPayloads [][]byte) []byte {
	size := transactionRetRowsSize(blockNumber, blockTimestamp, infoPayloads)
	return appendTransactionRetRows(make([]byte, 0, size), blockNumber, blockTimestamp, infoPayloads)
}

func transactionRetRowsSize(blockNumber, blockTimestamp int64, infoPayloads [][]byte) int {
	size := 0
	if blockNumber != 0 {
		size += protowire.SizeTag(1) + protowire.SizeVarint(uint64(blockNumber))
	}
	if blockTimestamp != 0 {
		size += protowire.SizeTag(2) + protowire.SizeVarint(uint64(blockTimestamp))
	}
	for _, payload := range infoPayloads {
		size += protowire.SizeTag(3) + protowire.SizeBytes(len(payload))
	}
	return size
}

func appendTransactionRetRows(data []byte, blockNumber, blockTimestamp int64, infoPayloads [][]byte) []byte {
	if blockNumber != 0 {
		data = protowire.AppendTag(data, 1, protowire.VarintType)
		data = protowire.AppendVarint(data, uint64(blockNumber))
	}
	if blockTimestamp != 0 {
		data = protowire.AppendTag(data, 2, protowire.VarintType)
		data = protowire.AppendVarint(data, uint64(blockTimestamp))
	}
	for _, payload := range infoPayloads {
		data = protowire.AppendTag(data, 3, protowire.BytesType)
		data = protowire.AppendBytes(data, payload)
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
