package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestDynamicPropertiesSnapshotNestedRollback(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("preexisting_dirty", 1)
	dp.SetString("preexisting_string", "before")
	dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{0x01}))

	outer := dp.Snapshot()
	dp.Set("preexisting_dirty", 2)
	dp.Set("new_integer", 3)
	dp.SetString("preexisting_string", "outer")
	dp.SetString("new_string", "outer")
	dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{0x02}))

	inner := dp.Snapshot()
	dp.Set("preexisting_dirty", 4)
	dp.SetString("preexisting_string", "inner")
	dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{0x03}))
	dp.RevertToSnapshot(inner)

	if got, _ := dp.Get("preexisting_dirty"); got != 2 {
		t.Fatalf("integer after inner revert = %d, want 2", got)
	}
	if got, _ := dp.GetString("preexisting_string"); got != "outer" {
		t.Fatalf("string after inner revert = %q, want outer", got)
	}
	if got := dp.LatestBlockHeaderHash(); got != common.BytesToHash([]byte{0x02}) {
		t.Fatalf("hash after inner revert = %x, want 02", got)
	}

	dp.RevertToSnapshot(outer)
	if got, _ := dp.Get("preexisting_dirty"); got != 1 {
		t.Fatalf("integer after outer revert = %d, want 1", got)
	}
	if _, ok := dp.Get("new_integer"); ok {
		t.Fatal("new integer survived outer revert")
	}
	if got, _ := dp.GetString("preexisting_string"); got != "before" {
		t.Fatalf("string after outer revert = %q, want before", got)
	}
	if _, ok := dp.GetString("new_string"); ok {
		t.Fatal("new string survived outer revert")
	}
	if got := dp.LatestBlockHeaderHash(); got != common.BytesToHash([]byte{0x01}) {
		t.Fatalf("hash after outer revert = %x, want 01", got)
	}
	if _, dirty := dp.dirty["preexisting_dirty"]; !dirty {
		t.Fatal("preexisting integer dirty flag was not restored")
	}
	if _, dirty := dp.stringDirty["preexisting_string"]; !dirty {
		t.Fatal("preexisting string dirty flag was not restored")
	}
	if !dp.hashDirty {
		t.Fatal("preexisting hash dirty flag was not restored")
	}
}

func TestDynamicPropertiesCommitNestedSnapshot(t *testing.T) {
	dp := NewDynamicProperties()
	outer := dp.Snapshot()
	dp.Set("outer", 1)

	inner := dp.Snapshot()
	dp.Set("inner", 2)
	dp.CommitSnapshot(inner)
	dp.RevertToSnapshot(outer)

	if _, ok := dp.Get("outer"); ok {
		t.Fatal("outer value survived enclosing rollback")
	}
	if _, ok := dp.Get("inner"); ok {
		t.Fatal("committed inner value survived enclosing rollback")
	}
}

func BenchmarkDynamicPropertiesSnapshot(b *testing.B) {
	dp := NewDynamicProperties()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := dp.Snapshot()
		dp.SetBlockEnergyUsage(int64(i + 1))
		dp.Set("total_transaction_cost", int64(i+1))
		dp.Set("total_net_weight", int64(i+1))
		dp.RevertToSnapshot(snap)
	}
}
