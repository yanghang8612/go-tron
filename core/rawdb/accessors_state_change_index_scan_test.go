package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestIterateStateChangeIndexRowsFiltersFamilyAndGroupsKeys(t *testing.T) {
	db := NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x11}
	changes := []*StateDomainChange{
		{BlockNum: 1, Seq: 1, TxNum: 1, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemReward, Key: []byte("a")},
		{BlockNum: 2, Seq: 1, TxNum: 2, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemReward, Key: []byte("a")},
		{BlockNum: 3, Seq: 1, TxNum: 3, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemReward, Key: []byte("b")},
		{BlockNum: 4, Seq: 1, TxNum: 4, FlatDomain: StateFlatDomainAccountLatest, Owner: owner},
	}
	for _, change := range changes {
		if err := WriteStateDomainChangeInverseIndex(db, change); err != nil {
			t.Fatal(err)
		}
	}

	var blocks []uint64
	var latestKeys [][]byte
	result, err := IterateStateChangeIndexRows(db, StateChangeIndexScanOptions{Family: StateChangeIndexKVLatest}, func(row StateChangeIndexRow) (bool, error) {
		blocks = append(blocks, row.BlockNum)
		latestKeys = append(latestKeys, append([]byte(nil), row.LatestKey...))
		if len(row.PhysicalKey) <= len(row.LatestKey) || row.ValueBytes != 0 {
			t.Fatalf("bad row: %+v", row)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("iterate KV latest index: %v", err)
	}
	if !result.Complete || result.Rows != 3 {
		t.Fatalf("result = %+v, want three complete rows", result)
	}
	if len(blocks) != 3 || blocks[0] != 1 || blocks[1] != 2 || blocks[2] != 3 {
		t.Fatalf("blocks = %v", blocks)
	}
	if string(latestKeys[0]) != string(latestKeys[1]) || string(latestKeys[1]) == string(latestKeys[2]) {
		t.Fatalf("latest key grouping is wrong")
	}

	limited, err := IterateStateChangeIndexRows(db, StateChangeIndexScanOptions{Family: StateChangeIndexAll, MaxRows: 2}, func(StateChangeIndexRow) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if limited.Complete || limited.Rows != 2 {
		t.Fatalf("limited result = %+v", limited)
	}
}
