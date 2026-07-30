package rawdb

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var latestPbftBlockNumKey = []byte("LATEST_PBFT_BLOCK_NUM")

// DeleteLatestPbftBlockNum removes the LATEST_PBFT_BLOCK_NUM singleton key.
// Called by incremental unwind (and by ResetMutableState's singleton list) to
// reset PBFT tracking when the chain is rewound past the last solid block.
func DeleteLatestPbftBlockNum(db ethdb.KeyValueWriter) error {
	return db.Delete(latestPbftBlockNumKey)
}

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

// ReadLatestPbftBlockNumStrict returns the stored latest PBFT-confirmed block
// number and surfaces storage/corruption errors. Missing rows return
// (-1, false, nil).
func ReadLatestPbftBlockNumStrict(db ethdb.KeyValueReader) (int64, bool, error) {
	val, ok, err := readPresentValue(db, latestPbftBlockNumKey, "latest pbft block number")
	if err != nil || !ok {
		return -1, ok, err
	}
	if len(val) != 8 {
		return -1, false, fmt.Errorf("rawdb: decode latest pbft block number: length %d, want 8", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), true, nil
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

// ReadBlockSignDataStrict returns the PBFTCommitResult stored for blockNum and
// surfaces storage/corruption errors. Missing rows return (nil, false, nil).
// A present zero-byte protobuf decodes as an empty PBFTCommitResult with
// ok=true.
func ReadBlockSignDataStrict(db ethdb.KeyValueReader, blockNum int64) (*corepb.PBFTCommitResult, bool, error) {
	data, ok, err := readPresentValue(db, pbftBlockSignKey(blockNum), fmt.Sprintf("pbft block sign data %d", blockNum))
	if err != nil || !ok {
		return nil, ok, err
	}
	var r corepb.PBFTCommitResult
	if err := proto.Unmarshal(data, &r); err != nil {
		return nil, true, fmt.Errorf("rawdb: decode pbft block sign data %d: %w", blockNum, err)
	}
	return &r, true, nil
}

// HasBlockSignData reports whether a commit result is recorded for
// blockNum — useful to skip signing a block that's already finalised.
func HasBlockSignData(db ethdb.KeyValueReader, blockNum int64) bool {
	ok, _ := db.Has(pbftBlockSignKey(blockNum))
	return ok
}

// HasBlockSignDataStrict reports whether a commit result is recorded for
// blockNum and surfaces storage errors.
func HasBlockSignDataStrict(db ethdb.KeyValueReader, blockNum int64) (bool, error) {
	return readKeyPresence(db, pbftBlockSignKey(blockNum), fmt.Sprintf("pbft block sign data %d", blockNum))
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

// ReadSrSignDataStrict returns the per-epoch SR-list PBFTCommitResult and
// surfaces storage/corruption errors. Missing rows return (nil, false, nil).
// A present zero-byte protobuf decodes as an empty PBFTCommitResult with
// ok=true.
func ReadSrSignDataStrict(db ethdb.KeyValueReader, epoch int64) (*corepb.PBFTCommitResult, bool, error) {
	data, ok, err := readPresentValue(db, pbftSrSignKey(epoch), fmt.Sprintf("pbft sr sign data %d", epoch))
	if err != nil || !ok {
		return nil, ok, err
	}
	var r corepb.PBFTCommitResult
	if err := proto.Unmarshal(data, &r); err != nil {
		return nil, true, fmt.Errorf("rawdb: decode pbft sr sign data %d: %w", epoch, err)
	}
	return &r, true, nil
}

// DeleteSrSignData removes the per-epoch SR-list entry.
func DeleteSrSignData(db ethdb.KeyValueWriter, epoch int64) error {
	return db.Delete(pbftSrSignKey(epoch))
}
