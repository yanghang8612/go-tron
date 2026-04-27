package rawdb

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var latestPbftBlockNumKey = []byte("LATEST_PBFT_BLOCK_NUM")

// WriteLatestPbftBlockNum records the highest PBFT-confirmed block number.
// Only-increases: if num <= current stored value the write is skipped, matching
// java-tron commonDataBase semantics.
func WriteLatestPbftBlockNum(db ethdb.KeyValueStore, num int64) {
	if cur := ReadLatestPbftBlockNum(db); num <= cur {
		return
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(num))
	db.Put(latestPbftBlockNumKey, val) //nolint:errcheck
}

// ReadLatestPbftBlockNum returns the stored latest PBFT-confirmed block number,
// or -1 if none has been written yet.
func ReadLatestPbftBlockNum(db ethdb.KeyValueReader) int64 {
	val, err := db.Get(latestPbftBlockNumKey)
	if err != nil || len(val) != 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(val))
}

// WriteBlockSignData stores a per-block PBFT commit result — the quorum
// signatures collected for blockNum. Mirrors java-tron
// PbftSignDataStore.putBlockSignData.
func WriteBlockSignData(db ethdb.KeyValueWriter, blockNum int64, r *corepb.PBFTCommitResult) error {
	if r == nil {
		return fmt.Errorf("pbft sign data: nil PBFTCommitResult")
	}
	data, err := proto.Marshal(r)
	if err != nil {
		return fmt.Errorf("pbft sign data: marshal: %w", err)
	}
	return db.Put(pbftBlockSignKey(blockNum), data)
}

// ReadBlockSignData returns the PBFTCommitResult stored for blockNum, or
// nil if absent.
func ReadBlockSignData(db ethdb.KeyValueReader, blockNum int64) *corepb.PBFTCommitResult {
	data, err := db.Get(pbftBlockSignKey(blockNum))
	if err != nil || len(data) == 0 {
		return nil
	}
	var r corepb.PBFTCommitResult
	if err := proto.Unmarshal(data, &r); err != nil {
		return nil
	}
	return &r
}

// HasBlockSignData reports whether a commit result is recorded for
// blockNum — useful to skip signing a block that's already finalised.
func HasBlockSignData(db ethdb.KeyValueReader, blockNum int64) bool {
	ok, _ := db.Has(pbftBlockSignKey(blockNum))
	return ok
}

// DeleteBlockSignData removes the per-block entry.
func DeleteBlockSignData(db ethdb.KeyValueWriter, blockNum int64) error {
	return db.Delete(pbftBlockSignKey(blockNum))
}

// WriteSrSignData stores the per-epoch SR-list commit result. Mirrors
// PbftSignDataStore.putSrSignData.
func WriteSrSignData(db ethdb.KeyValueWriter, epoch int64, r *corepb.PBFTCommitResult) error {
	if r == nil {
		return fmt.Errorf("pbft sign data: nil PBFTCommitResult")
	}
	data, err := proto.Marshal(r)
	if err != nil {
		return fmt.Errorf("pbft sign data: marshal: %w", err)
	}
	return db.Put(pbftSrSignKey(epoch), data)
}

// ReadSrSignData returns the per-epoch SR-list PBFTCommitResult or nil.
func ReadSrSignData(db ethdb.KeyValueReader, epoch int64) *corepb.PBFTCommitResult {
	data, err := db.Get(pbftSrSignKey(epoch))
	if err != nil || len(data) == 0 {
		return nil
	}
	var r corepb.PBFTCommitResult
	if err := proto.Unmarshal(data, &r); err != nil {
		return nil
	}
	return &r
}

// DeleteSrSignData removes the per-epoch SR-list entry.
func DeleteSrSignData(db ethdb.KeyValueWriter, epoch int64) error {
	return db.Delete(pbftSrSignKey(epoch))
}
