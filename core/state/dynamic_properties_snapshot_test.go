package state

import (
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// TestDynamicPropertiesCopyFieldPolicy is deliberately exhaustive. The
// isolated canonical VM oracle depends on DynamicProperties.Copy remaining an
// exact value snapshot while dropping rollback history and recorder ownership.
// A newly added field must therefore receive an explicit copy/reset policy.
func TestDynamicPropertiesCopyFieldPolicy(t *testing.T) {
	policies := map[string]string{
		"props":                 "deep-copy",
		"dirty":                 "deep-copy",
		"stringProps":           "deep-copy",
		"stringDirty":           "deep-copy",
		"latestBlockHeaderHash": "copy",
		"hashDirty":             "copy",
		"transactionAccess":     "reset-recorder",
		"journal":               "reset-history",
		"snapshots":             "reset-history",
	}
	typ := reflect.TypeOf(DynamicProperties{})
	if len(policies) != typ.NumField() {
		t.Fatalf("DynamicProperties copy policy covers %d fields, struct has %d", len(policies), typ.NumField())
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Name
		if policies[field] == "" {
			t.Fatalf("DynamicProperties field %q has no copy policy", field)
		}
	}
}

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

func TestDynamicPropertiesCopyIsIndependentAndLazilyMutable(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("copy_int", 1)
	dp.SetString("copy_string", "one")
	dp.SetLatestBlockHeaderHash(common.Hash{0x11})
	sourceSnapshot := dp.Snapshot()
	dp.Set("copy_journal_int", 7)
	dp.SetString("copy_journal_string", "seven")
	dp.SetLatestBlockHeaderHash(common.Hash{0x22})
	var recorder TransactionAccessRecorder
	dp.SetTransactionAccessRecorder(&recorder)
	cp := dp.Copy()
	if cp.transactionAccess != nil || len(cp.journal) != 0 || len(cp.snapshots) != 0 {
		t.Fatalf("copy retained recorder or rollback history: recorder=%p journal=%d snapshots=%d",
			cp.transactionAccess, len(cp.journal), len(cp.snapshots))
	}
	if cp.latestBlockHeaderHash != (common.Hash{0x22}) || !cp.hashDirty {
		t.Fatalf("copy hash state = %x dirty=%v, want 22/true", cp.latestBlockHeaderHash, cp.hashDirty)
	}
	if !reflect.DeepEqual(cp.props, dp.props) || !reflect.DeepEqual(cp.dirty, dp.dirty) ||
		!reflect.DeepEqual(cp.stringProps, dp.stringProps) || !reflect.DeepEqual(cp.stringDirty, dp.stringDirty) {
		t.Fatal("copy omitted a dynamic-property value or dirty map")
	}
	delete(cp.dirty, "copy_int")
	delete(cp.stringDirty, "copy_string")
	if _, ok := dp.dirty["copy_int"]; !ok {
		t.Fatal("copy integer dirty map aliases source")
	}
	if _, ok := dp.stringDirty["copy_string"]; !ok {
		t.Fatal("copy string dirty map aliases source")
	}
	cp.Set("copy_int", 2)
	cp.SetString("copy_string", "two")
	cp.SetLatestBlockHeaderHash(common.Hash{0x33})
	if got, _ := dp.Get("copy_int"); got != 1 {
		t.Fatalf("source int after copy mutation = %d, want 1", got)
	}
	if got, _ := dp.GetString("copy_string"); got != "one" {
		t.Fatalf("source string after copy mutation = %q, want one", got)
	}
	if got := dp.LatestBlockHeaderHash(); got != (common.Hash{0x22}) {
		t.Fatalf("source hash after copy mutation = %x, want 22", got)
	}
	dp.RevertToSnapshot(sourceSnapshot)
	if got, ok := cp.Get("copy_journal_int"); !ok || got != 7 {
		t.Fatalf("copy changed after source rollback = %d ok=%v, want 7/true", got, ok)
	}
	if got := cp.LatestBlockHeaderHash(); got != (common.Hash{0x33}) {
		t.Fatalf("copy hash after source rollback = %x, want 33", got)
	}

	// Copy deliberately keeps empty maps nil. Public mutators must lazily make
	// them so even a sparse/test DynamicProperties remains safely mutable.
	emptyCopy := new(DynamicProperties).Copy()
	emptyCopy.Set("new_int", 3)
	emptyCopy.SetString("new_string", "three")
	if got, ok := emptyCopy.Get("new_int"); !ok || got != 3 {
		t.Fatalf("lazy int = %d ok=%v, want 3/true", got, ok)
	}
	if got, ok := emptyCopy.GetString("new_string"); !ok || got != "three" {
		t.Fatalf("lazy string = %q ok=%v, want three/true", got, ok)
	}
}

func TestDynamicPropertiesCopyIntoReusesAndClearsDestination(t *testing.T) {
	source := NewDynamicProperties()
	source.Set("copy_into", 7)
	source.SetString("copy_into_string", "seven")
	dst := NewDynamicProperties()
	dst.Set("stale", 1)
	dst.SetString("stale_string", "old")
	dst.Snapshot()

	got := source.CopyInto(dst)
	if got != dst {
		t.Fatal("CopyInto replaced destination")
	}
	if got.props == nil || got.stringProps == nil || len(got.journal) != 0 || len(got.snapshots) != 0 {
		t.Fatalf("reused snapshot not reset: %+v", got)
	}
	if _, ok := got.Get("stale"); ok {
		t.Fatal("CopyInto retained stale integer property")
	}
	if _, ok := got.GetString("stale_string"); ok {
		t.Fatal("CopyInto retained stale string property")
	}
	if value, ok := got.Get("copy_into"); !ok || value != 7 {
		t.Fatalf("copied integer = %d ok=%v, want 7/true", value, ok)
	}
	if value, ok := got.GetString("copy_into_string"); !ok || value != "seven" {
		t.Fatalf("copied string = %q ok=%v, want seven/true", value, ok)
	}
	source.Set("copy_into", 8)
	if value, _ := got.Get("copy_into"); value != 7 {
		t.Fatalf("destination aliases source: got %d, want 7", value)
	}
}

var benchmarkDynamicPropertiesCopy *DynamicProperties

func BenchmarkDynamicPropertiesCopy(b *testing.B) {
	dp := NewDynamicProperties()
	dp.SetLatestBlockHeaderNumber(1)
	dp.SetLatestBlockHeaderTimestamp(2)
	dp.SetLatestSolidifiedBlockNum(3)
	dp.SetLatestBlockHeaderHash(common.Hash{4})
	b.ReportAllocs()
	for range b.N {
		benchmarkDynamicPropertiesCopy = dp.Copy()
	}
}

func BenchmarkDynamicPropertiesCopyInto(b *testing.B) {
	dp := NewDynamicProperties()
	dp.SetLatestBlockHeaderNumber(1)
	dp.SetLatestBlockHeaderTimestamp(2)
	dp.SetLatestSolidifiedBlockNum(3)
	dp.SetLatestBlockHeaderHash(common.Hash{4})
	dst := new(DynamicProperties)
	b.ReportAllocs()
	for range b.N {
		benchmarkDynamicPropertiesCopy = dp.CopyInto(dst)
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
