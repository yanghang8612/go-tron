package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

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
