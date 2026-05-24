package state

import (
	"bytes"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestTrieNodeCacheReadThrough(t *testing.T) {
	disk := rawdb.WrapKeyValueStore(rawdb.NewMemoryDatabase())
	db := NewDatabaseWithConfig(disk, DatabaseConfig{CleanTrieCacheSizeBytes: 1024 * 1024})
	hash := ethcommon.HexToHash("0x1234")
	blob := []byte{0xc0}

	if err := disk.Put(hash.Bytes(), blob); err != nil {
		t.Fatal(err)
	}
	got, err := db.trieDisk.Get(hash.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("first get=%x, want %x", got, blob)
	}
	if err := disk.Delete(hash.Bytes()); err != nil {
		t.Fatal(err)
	}
	got, err = db.trieDisk.Get(hash.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("cached get=%x, want %x", got, blob)
	}
}

func TestTrieNodeBatchWriterPopulatesCacheAfterFlush(t *testing.T) {
	disk := rawdb.WrapKeyValueStore(rawdb.NewMemoryDatabase())
	db := NewDatabaseWithConfig(disk, DatabaseConfig{CleanTrieCacheSizeBytes: 1024 * 1024})
	writer := newTrieNodeBatchWriter(db)
	defer writer.release()

	hash := ethcommon.HexToHash("0x5678")
	blob := []byte{0xc0, 0x01}
	if err := writer.write(hash, blob); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if err := disk.Delete(hash.Bytes()); err != nil {
		t.Fatal(err)
	}
	got, err := db.trieDisk.Get(hash.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("cached writer node=%x, want %x", got, blob)
	}
}
