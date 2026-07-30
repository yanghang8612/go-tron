package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadBlock(t *testing.T) {
	chaindb := NewMemoryChainDB()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 126000,
			},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(chaindb, block)

	got := ReadBlock(chaindb, block.Number())
	if got == nil {
		t.Fatal("block not found")
	}
	if got.Number() != 42 {
		t.Fatalf("expected 42, got %d", got.Number())
	}
}

func TestWriteReadBlockByHash(t *testing.T) {
	chaindb := NewMemoryChainDB()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(chaindb, block)

	num := ReadBlockNumber(chaindb, block.Hash())
	if num == nil {
		t.Fatal("hash->number mapping not found")
	}
	if *num != 10 {
		t.Fatalf("expected 10, got %d", *num)
	}
}

func TestHeadBlock(t *testing.T) {
	db := NewMemoryDatabase()
	WriteHeadBlockHash(db, common.HexToHash("aabb"))
	h := ReadHeadBlockHash(db)
	if h != common.HexToHash("aabb") {
		t.Fatal("head block hash mismatch")
	}
}

func TestHashBoundaryRowsRejectMalformedValues(t *testing.T) {
	db := NewMemoryDatabase()
	if err := db.Put(headBlockKey, []byte{0xaa}); err != nil {
		t.Fatalf("put malformed head block hash: %v", err)
	}
	if got := ReadHeadBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("ReadHeadBlockHash malformed row = %x, want zero", got)
	}
	if err := db.Put(headSolidBlockKey, bytes.Repeat([]byte{0xbb}, common.HashLength-1)); err != nil {
		t.Fatalf("put malformed solid head block hash: %v", err)
	}
	if got := ReadHeadSolidBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("ReadHeadSolidBlockHash malformed row = %x, want zero", got)
	}
	if err := db.Put(genesisStateRootKey, bytes.Repeat([]byte{0xcc}, common.HashLength+1)); err != nil {
		t.Fatalf("put malformed genesis state root: %v", err)
	}
	if got := ReadGenesisStateRoot(db); got != (common.Hash{}) {
		t.Fatalf("ReadGenesisStateRoot malformed row = %x, want zero", got)
	}
}

func TestWriteReadAccount(t *testing.T) {
	db := NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(1000000)

	WriteAccount(db, addr, acc)
	got := ReadAccount(db, addr)
	if got == nil {
		t.Fatal("account not found")
	}
	if got.Balance() != 1000000 {
		t.Fatalf("expected 1000000, got %d", got.Balance())
	}
}

func TestAccountStrictReaders(t *testing.T) {
	db := NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})

	if got, ok, err := ReadAccountStrict(db, addr); got != nil || ok || err != nil {
		t.Fatalf("ReadAccountStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if ok, err := HasAccountStrict(db, addr); err != nil || ok {
		t.Fatalf("HasAccountStrict absent = %v/%v, want false/nil", ok, err)
	}

	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(123)
	WriteAccount(db, addr, acc)
	if ok, err := HasAccountStrict(db, addr); err != nil || !ok {
		t.Fatalf("HasAccountStrict present = %v/%v, want true/nil", ok, err)
	}
	got, ok, err := ReadAccountStrict(db, addr)
	if err != nil || !ok || got == nil || got.Balance() != 123 {
		t.Fatalf("ReadAccountStrict present = %v/%v/%v, want balance 123", got, ok, err)
	}

	for _, tc := range []struct {
		name   string
		reader ethdb.KeyValueReader
		want   string
	}{
		{name: "has", reader: failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, want: "presence"},
		{name: "get", reader: failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, want: "get boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok, err := ReadAccountStrict(tc.reader, addr); err == nil || ok || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ReadAccountStrict %s error ok=%v err=%v, want %q", tc.name, ok, err, tc.want)
			}
		})
	}
	if ok, err := HasAccountStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, addr); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("HasAccountStrict has error = %v/%v, want presence error", ok, err)
	}

	if err := db.Put(accountKey(addr.Bytes()), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadAccount(db, addr); got != nil {
		t.Fatalf("compat ReadAccount corrupt row = %v, want nil", got)
	}
	if got, ok, err := ReadAccountStrict(db, addr); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode account") {
		t.Fatalf("ReadAccountStrict corrupt row = %v/%v/%v, want decode error", got, ok, err)
	}
	if err := db.Put(accountKey(addr.Bytes()), nil); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadAccountStrict(db, addr); err != nil || !ok || got == nil {
		t.Fatalf("ReadAccountStrict empty proto = %v/%v/%v, want empty account/true/nil", got, ok, err)
	}
}

func TestWitnessStrictReader(t *testing.T) {
	db := NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 2, 3, 4, 5})

	if got, ok, err := ReadWitnessStrict(db, addr); got != nil || ok || err != nil {
		t.Fatalf("ReadWitnessStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	w := types.NewWitness(addr, "https://sr")
	w.SetVoteCount(99)
	WriteWitness(db, addr, w)
	got, ok, err := ReadWitnessStrict(db, addr)
	if err != nil || !ok || got == nil || got.URL() != "https://sr" || got.VoteCount() != 99 {
		t.Fatalf("ReadWitnessStrict present = %v/%v/%v, want witness/true/nil", got, ok, err)
	}
	if _, ok, err := ReadWitnessStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, addr); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadWitnessStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadWitnessStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, addr); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadWitnessStrict get error ok=%v err=%v, want get error", ok, err)
	}
	if err := db.Put(witnessKey(addr.Bytes()), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadWitness(db, addr); got != nil {
		t.Fatalf("compat ReadWitness corrupt row = %v, want nil", got)
	}
	if got, ok, err := ReadWitnessStrict(db, addr); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode witness") {
		t.Fatalf("ReadWitnessStrict corrupt row = %v/%v/%v, want decode error", got, ok, err)
	}
	if err := db.Put(witnessKey(addr.Bytes()), nil); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadWitnessStrict(db, addr); err != nil || !ok || got == nil {
		t.Fatalf("ReadWitnessStrict empty proto = %v/%v/%v, want empty witness/true/nil", got, ok, err)
	}
}
