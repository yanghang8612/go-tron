package rawdb

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

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
