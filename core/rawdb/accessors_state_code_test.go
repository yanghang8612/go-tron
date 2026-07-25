package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
)

type stateCodeCountingReader struct {
	ethdb.KeyValueReader
	gets int
}

func (r *stateCodeCountingReader) Get(key []byte) ([]byte, error) {
	r.gets++
	return r.KeyValueReader.Get(key)
}

func TestStateCodeReadWrite(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	hash := common.Keccak256(code)

	if got := ReadStateCode(db, hash); got != nil {
		t.Fatalf("pre-write code = %x", got)
	}
	if err := WriteStateCode(db, hash, code); err != nil {
		t.Fatalf("write state code: %v", err)
	}
	got := ReadStateCode(db, hash)
	if !bytes.Equal(got, code) {
		t.Fatalf("code = %x, want %x", got, code)
	}
	got[0] = 0xff
	if reread := ReadStateCode(db, hash); bytes.Equal(reread, got) {
		t.Fatal("ReadStateCode returned aliased bytes")
	}
}

func TestStateCodeRejectsMismatchedHash(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateCode(db, common.Hash{0x01}, []byte{0x60, 0x00}); err == nil {
		t.Fatal("expected hash mismatch error")
	}
}

func TestStateCodeDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	code := []byte{0x60, 0x01}
	hash := common.Keccak256(code)
	if err := WriteStateCode(db, hash, code); err != nil {
		t.Fatalf("write state code: %v", err)
	}
	if err := DeleteStateCode(db, hash); err != nil {
		t.Fatalf("delete state code: %v", err)
	}
	if got := ReadStateCode(db, hash); got != nil {
		t.Fatalf("deleted code = %x", got)
	}
}

func TestStateCodeReadUsesBoundedBaseCacheAndReturnsOwnedBytes(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	code := bytes.Repeat([]byte{0x5a}, 4096)
	hash := common.Keccak256(code)
	if err := WriteStateCode(base, hash, code); err != nil {
		t.Fatal(err)
	}
	counting := &stateCodeCountingReader{KeyValueReader: base}
	buffer := blockbuffer.New(counting)
	buffer.SetBaseReadCacheSize(1 << 20)

	for i := 0; i < 3; i++ {
		got := ReadStateCode(buffer, hash)
		if !bytes.Equal(got, code) {
			t.Fatalf("read %d = %x, want bytecode", i, got[:min(len(got), 8)])
		}
		got[0] ^= 0xff
	}
	if counting.gets != 2 {
		t.Fatalf("durable code reads = %d, want 2 (probation + admission, then cache hit)", counting.gets)
	}
}
