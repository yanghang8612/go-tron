package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteAssetIssue stores an AssetIssueContract keyed by tokenID.
func WriteAssetIssue(db ethdb.KeyValueWriter, tokenID int64, c *contractpb.AssetIssueContract) error {
	data, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	return db.Put(assetKey(tokenID), data)
}

// ReadAssetIssue returns the AssetIssueContract for tokenID, or nil if not found.
func ReadAssetIssue(db ethdb.KeyValueReader, tokenID int64) *contractpb.AssetIssueContract {
	data, err := db.Get(assetKey(tokenID))
	if err != nil || len(data) == 0 {
		return nil
	}
	c := &contractpb.AssetIssueContract{}
	if err := proto.Unmarshal(data, c); err != nil {
		return nil
	}
	return c
}

// WriteAssetNameIndex stores a name → tokenID mapping.
func WriteAssetNameIndex(db ethdb.KeyValueWriter, name []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return db.Put(assetNameKey(name), buf)
}

// ReadAssetNameIndex returns the tokenID for the given name, and whether it exists.
func ReadAssetNameIndex(db ethdb.KeyValueReader, name []byte) (int64, bool) {
	data, err := db.Get(assetNameKey(name))
	if err != nil || len(data) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(data[:8])), true
}

// WriteAssetOwnerIndex stores an ownerAddr → tokenID mapping (21-byte TRON address).
func WriteAssetOwnerIndex(db ethdb.KeyValueWriter, ownerAddr []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return db.Put(assetOwnerKey(ownerAddr), buf)
}

// ReadAssetOwnerIndex returns the tokenID issued by ownerAddr, and whether it exists.
func ReadAssetOwnerIndex(db ethdb.KeyValueReader, ownerAddr []byte) (int64, bool) {
	data, err := db.Get(assetOwnerKey(ownerAddr))
	if err != nil || len(data) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(data[:8])), true
}

// WriteAssetIssueTime stores the block timestamp (ms) when the token was issued.
func WriteAssetIssueTime(db ethdb.KeyValueWriter, tokenID int64, issueTimeMs int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(issueTimeMs))
	return db.Put(assetIssueTimeKey(tokenID), buf)
}

// ReadAssetIssueTime returns the issue timestamp for tokenID, or 0 if not found.
func ReadAssetIssueTime(db ethdb.KeyValueReader, tokenID int64) int64 {
	data, err := db.Get(assetIssueTimeKey(tokenID))
	if err != nil || len(data) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data[:8]))
}

// ListAllAssets iterates the ast- prefix and returns all assets sorted by tokenID ascending.
func ListAllAssets(db ethdb.Iteratee) []*contractpb.AssetIssueContract {
	it := db.NewIterator(assetPrefix, nil)
	defer it.Release()
	var result []*contractpb.AssetIssueContract
	for it.Next() {
		c := &contractpb.AssetIssueContract{}
		if err := proto.Unmarshal(it.Value(), c); err == nil {
			result = append(result, c)
		}
	}
	return result
}

// ListAssetsPaginated returns up to limit assets starting at position offset (0-indexed).
func ListAssetsPaginated(db ethdb.Iteratee, offset, limit int) []*contractpb.AssetIssueContract {
	it := db.NewIterator(assetPrefix, nil)
	defer it.Release()
	var result []*contractpb.AssetIssueContract
	skipped := 0
	for it.Next() {
		if skipped < offset {
			skipped++
			continue
		}
		c := &contractpb.AssetIssueContract{}
		if err := proto.Unmarshal(it.Value(), c); err == nil {
			result = append(result, c)
		}
		if len(result) >= limit {
			break
		}
	}
	return result
}
