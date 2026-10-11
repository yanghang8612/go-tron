package rawdb

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSharedHistoryBorrowedRangeReusesBufferAndAuthenticatesEveryBlock(t *testing.T) {
	db := sharedHistoryTestDB(t)
	var expected [][]byte
	var lastPack []byte
	for i, size := range []int{256 << 10, 32 << 10, 512 << 10, 64 << 10} {
		block := uint64(2050 + i)
		prev := bytes.Repeat([]byte{byte(i + 1)}, size)
		expected = append(expected, prev)
		row := &StateDomainChange{BlockNum: block, Seq: 1, TxNum: block, FlatDomain: StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte("scan"), PrevExists: true, Prev: prev}
		raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
		pack, chunks := ownedReadBenchmarkPack(raw, block, true)
		for key, value := range chunks {
			directTestPut(t, db, []byte(key), value)
		}
		directTestPut(t, db, stateChangeSetKey(block, 0), pack)
		if err := WriteStateTxRange(db, block, common.Hash{byte(i + 1)}, block, block); err != nil {
			t.Fatal(err)
		}
		lastPack = pack
	}
	var copied [][]byte
	err := IterateStateDomainChangesByBlockTxRangeBorrowed(db, 2050, 2053, 2050, 2053, func(row *StateDomainChange) (bool, error) {
		copied = append(copied, bytes.Clone(row.Prev))
		return true, nil
	})
	if err != nil || len(copied) != len(expected) {
		t.Fatalf("rows=%d err=%v", len(copied), err)
	}
	for i := range expected {
		if !bytes.Equal(copied[i], expected[i]) {
			t.Fatalf("block %d changed", i)
		}
	}
	// Valid chunks must not bypass the whole-pack digest when scratch contains
	// bytes from preceding blocks. The corrupt block must publish no callback.
	_, _, _, digest, err := sharedStateHistoryPackHeader(lastPack, 2053)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(lastPack)
	pos := bytes.Index(bad, digest[:])
	if pos < 0 || digest == (sha256.Sum256(nil)) {
		t.Fatal("missing fixture digest")
	}
	bad[pos] ^= 1
	directTestPut(t, db, stateChangeSetKey(2053, 0), bad)
	seen := 0
	err = IterateStateDomainChangesByBlockTxRangeBorrowed(db, 2050, 2053, 2050, 2053, func(*StateDomainChange) (bool, error) { seen++; return true, nil })
	if err == nil || seen != 3 {
		t.Fatalf("corrupt pack callbacks=%d err=%v", seen, err)
	}
}
