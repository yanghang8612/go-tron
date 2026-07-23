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

func TestDynamicPropertiesCoalescesCommittedNestedPreimages(t *testing.T) {
	dp := NewDynamicProperties()
	originalHash := common.BytesToHash([]byte{0x01})
	dp.Set("counter", 1)
	dp.SetString("label", "before")
	dp.SetLatestBlockHeaderHash(originalHash)

	outer := dp.Snapshot()
	for i := int64(2); i <= 64; i++ {
		inner := dp.Snapshot()
		// Multiple writes inside one transaction need only its first pre-image.
		dp.Set("counter", i)
		dp.Set("counter", i+1000)
		dp.SetString("label", "during")
		dp.SetString("label", "latest")
		dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{byte(i)}))
		dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{byte(i + 1)}))
		dp.CommitSnapshot(inner)
	}
	if got := len(dp.journal); got != 3 {
		t.Fatalf("journal entries after committed children = %d, want one int/string/hash pre-image", got)
	}

	// A later failed child must still restore the state produced by the last
	// successful child, without disturbing the compacted outer pre-images.
	wantCounter, _ := dp.Get("counter")
	wantLabel, _ := dp.GetString("label")
	wantHash := dp.LatestBlockHeaderHash()
	failed := dp.Snapshot()
	dp.Set("counter", -1)
	dp.SetString("label", "failed")
	dp.SetLatestBlockHeaderHash(common.BytesToHash([]byte{0xff}))
	dp.RevertToSnapshot(failed)
	if got, _ := dp.Get("counter"); got != wantCounter {
		t.Fatalf("counter after failed child = %d, want %d", got, wantCounter)
	}
	if got, _ := dp.GetString("label"); got != wantLabel {
		t.Fatalf("label after failed child = %q, want %q", got, wantLabel)
	}
	if got := dp.LatestBlockHeaderHash(); got != wantHash {
		t.Fatalf("hash after failed child = %x, want %x", got, wantHash)
	}

	dp.RevertToSnapshot(outer)
	if got, _ := dp.Get("counter"); got != 1 {
		t.Fatalf("counter after outer revert = %d, want 1", got)
	}
	if got, _ := dp.GetString("label"); got != "before" {
		t.Fatalf("label after outer revert = %q, want before", got)
	}
	if got := dp.LatestBlockHeaderHash(); got != originalHash {
		t.Fatalf("hash after outer revert = %x, want %x", got, originalHash)
	}
}

func TestDynamicPropertiesCoalescesThreeSnapshotLevels(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("counter", 1)

	outer := dp.Snapshot()
	dp.Set("counter", 2)

	middle := dp.Snapshot()
	dp.Set("counter", 3)
	dp.Set("middle_only", 3)

	inner := dp.Snapshot()
	dp.Set("counter", 4)
	dp.Set("middle_only", 4)
	dp.Set("inner_only", 4)
	dp.CommitSnapshot(inner)
	if got := len(dp.journal); got != 4 {
		// middle still needs its own counter pre-image until it commits.
		t.Fatalf("journal entries after inner commit = %d, want 4", got)
	}

	dp.CommitSnapshot(middle)
	if got := len(dp.journal); got != 3 {
		t.Fatalf("journal entries after middle commit = %d, want 3", got)
	}
	dp.RevertToSnapshot(outer)
	if got, _ := dp.Get("counter"); got != 1 {
		t.Fatalf("counter after outer revert = %d, want 1", got)
	}
	if _, ok := dp.Get("middle_only"); ok {
		t.Fatal("middle-only property survived outer revert")
	}
	if _, ok := dp.Get("inner_only"); ok {
		t.Fatal("inner-only property survived outer revert")
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

func BenchmarkDynamicPropertiesCommittedNestedSnapshots(b *testing.B) {
	const transactions = 1024
	b.ReportAllocs()
	for range b.N {
		dp := NewDynamicProperties()
		outer := dp.Snapshot()
		for tx := int64(1); tx <= transactions; tx++ {
			inner := dp.Snapshot()
			dp.SetPublicNetUsage(tx)
			dp.CommitSnapshot(inner)
		}
		if len(dp.journal) != 1 {
			b.Fatalf("journal entries = %d, want 1", len(dp.journal))
		}
		dp.RevertToSnapshot(outer)
	}
}
