package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestStateReadAheadPlanWarmsOwnerRowsInCurrentGeneration(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	owner, to, contract, witness := readAheadAddress(1), readAheadAddress(2), readAheadAddress(3), readAheadAddress(4)
	for _, address := range []struct{ last byte }{{1}, {2}, {3}, {4}} {
		encoded, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: 11}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, readAheadAddress(address.last), encoded); err != nil {
			t.Fatal(err)
		}
	}
	block := readAheadTestBlock(t, owner, to, contract, witness)
	block.Proto().Transactions[1].RawData.Contract[0].PermissionId = 5
	block = types.NewBlockFromPB(block.Proto())
	rows := []struct {
		domain kvdomains.KVDomain
		key    []byte
	}{
		{kvdomains.AccountPermissionAux, accountOwnerPermissionKey},
		{kvdomains.AccountPermissionAux, accountActivePermissionKey(5)},
		{kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0)},
		{kvdomains.AccountResourceAux, accountResourceKey},
	}
	for _, row := range rows {
		if err := rawdb.WriteStateKVLatest(disk, owner, 10, row.domain, row.key, []byte("old")); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateKVLatest(disk, owner, 11, row.domain, row.key, []byte("current")); err != nil {
			t.Fatal(err)
		}
	}
	base := &readAheadCountingReader{KeyValueReader: disk}
	buffer := blockbuffer.New(base)
	buffer.SetBaseReadCacheSize(1 << 20)
	p := NewStateReadAhead(buffer, StateReadAheadConfig{Workers: 1})
	defer p.Close()
	p.EnqueueBlock(block, 1)
	p.Wait()
	before := base.gets.Load()
	for _, row := range rows {
		value, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, owner, 11, row.domain, row.key)
		if err != nil || !ok || string(value) != "current" {
			t.Fatalf("owner row: %q/%v/%v", value, ok, err)
		}
	}
	if base.gets.Load() != before {
		t.Fatal("canonical permission/resource read reached durable base")
	}
	// A subsequent envelope-generation change must select different physical
	// rows, even when all of the old generation was warmed successfully.
	for _, row := range rows {
		value, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, owner, 12, row.domain, row.key)
		if err != nil || ok || value != nil {
			t.Fatalf("generation 12 reused old row: %q/%v/%v", value, ok, err)
		}
	}
}
