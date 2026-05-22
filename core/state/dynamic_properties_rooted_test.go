package state

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func be8(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// flushAndReload mirrors the production persistence split: rooted keys go into
// the system account's KV and are committed into a state root; derived keys go
// into flat dp-. It then reloads from both, so a round-trip test exercises the
// same path applyBlock uses.
func flushAndReload(t *testing.T, dp *DynamicProperties) *DynamicProperties {
	t.Helper()
	sdb := newTestStateDB(t)
	if err := dp.FlushRooted(sdb); err != nil {
		t.Fatalf("flush rooted: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	dp.Flush(sdb.db.DiskDB()) // derived keys → dp-
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	return LoadDynamicProperties(sdb.db.DiskDB(), reopened)
}

// FlushRooted stages non-derived keys into the system account's KV (rooted into
// the committed state root); derived keys are left for the flat dp- Flush.
func TestDynPropRootedFlushExcludesDerived(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	dp.Set("next_maintenance_time", 12345) // rooted
	dp.SetLatestBlockHeaderNumber(7)       // derived

	if err := dp.FlushRooted(sdb); err != nil {
		t.Fatalf("flush rooted: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reopened.SystemKVGet(kvdomains.SystemDynamicProperty, []byte("next_maintenance_time"))
	if err != nil || !ok {
		t.Fatalf("rooted next_maintenance_time missing: ok=%v err=%v", ok, err)
	}
	if len(got) != 8 || int64(binary.BigEndian.Uint64(got)) != 12345 {
		t.Fatalf("rooted value = %v, want BE(12345)", got)
	}
	if _, ok, _ := reopened.SystemKVGet(kvdomains.SystemDynamicProperty, []byte("latest_block_header_number")); ok {
		t.Fatal("derived key must not be rooted into system-KV")
	}
}

// FlushRooted clears the rooted dirty entries so the later flat Flush only
// writes derived keys to dp-.
func TestDynPropFlushRootedClearsRootedDirty(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	dp.Set("next_maintenance_time", 999) // rooted
	dp.SetLatestBlockHeaderNumber(5)     // derived
	if err := dp.FlushRooted(sdb); err != nil {
		t.Fatalf("flush rooted: %v", err)
	}
	// Only the derived key may remain dirty; the flat Flush writes it to dp-.
	dp.Flush(sdb.db.DiskDB())
	if v := rawdb.ReadDynamicProperty(sdb.db.DiskDB(), "next_maintenance_time"); len(v) != 0 {
		t.Fatalf("rooted key must not be in dp-, got %v", v)
	}
	if v := rawdb.ReadDynamicProperty(sdb.db.DiskDB(), "latest_block_header_number"); len(v) != 8 {
		t.Fatalf("derived key must be in dp-, got %v", v)
	}
}

// LoadDynamicProperties merges rooted (system-KV) + derived (dp-).
func TestDynPropLoadMergesRootedAndDerived(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	dp.Set("next_maintenance_time", 777)  // rooted
	dp.SetString("memo_fee_history", "9") // rooted string
	if err := dp.FlushRooted(sdb); err != nil {
		t.Fatalf("flush rooted: %v", err)
	}
	root, _ := sdb.Commit()
	reopened, _ := New(root, sdb.db)

	// derived key lives in flat dp-
	rawdb.WriteDynamicProperty(sdb.db.DiskDB(), "latest_block_header_number", be8(42))

	loaded := LoadDynamicProperties(sdb.db.DiskDB(), reopened)
	if loaded.NextMaintenanceTime() != 777 {
		t.Fatalf("rooted int not loaded: %d", loaded.NextMaintenanceTime())
	}
	if got, _ := loaded.GetString("memo_fee_history"); got != "9" {
		t.Fatalf("rooted string not loaded: %q", got)
	}
	if loaded.LatestBlockHeaderNumber() != 42 {
		t.Fatalf("derived not loaded: %d", loaded.LatestBlockHeaderNumber())
	}
}

// TestDynPropRootedAnchorAndRewind is the Phase 3b acceptance gate: a rooted
// dynprop change moves the internal state root (anchor), and reopening an old
// root recovers the old rooted values (rewind). Covers an int64 AND a string
// key, and uses a fresh StateDB per commit to mirror applyBlock's per-block
// parent-root open.
func TestDynPropRootedAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)

	// R1: int64 + string rooted values.
	dp1 := NewDynamicProperties()
	dp1.Set("next_maintenance_time", 111)
	dp1.SetString("memo_fee_history", "0:111")
	if err := dp1.FlushRooted(sdb); err != nil {
		t.Fatalf("flush R1: %v", err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 built on R1, via a fresh StateDB (as applyBlock opens per block).
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	dp2 := NewDynamicProperties()
	dp2.Set("next_maintenance_time", 222)
	dp2.SetString("memo_fee_history", "0:222")
	if err := dp2.FlushRooted(sdb2); err != nil {
		t.Fatalf("flush R2: %v", err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	// Anchor: a rooted change must move the state root.
	if r1 == r2 {
		t.Fatal("anchor: rooted dynprop change did not move the state root")
	}

	// Rewind: reopening R1 recovers R1's values; R2 keeps R2's.
	atR1 := LoadDynamicProperties(sdb.db.DiskDB(), mustOpen(t, sdb, r1))
	if got := atR1.NextMaintenanceTime(); got != 111 {
		t.Fatalf("rewind R1 int64: got %d, want 111", got)
	}
	if got, _ := atR1.GetString("memo_fee_history"); got != "0:111" {
		t.Fatalf("rewind R1 string: got %q, want 0:111", got)
	}
	atR2 := LoadDynamicProperties(sdb.db.DiskDB(), mustOpen(t, sdb, r2))
	if got := atR2.NextMaintenanceTime(); got != 222 {
		t.Fatalf("R2 int64: got %d, want 222", got)
	}
	if got, _ := atR2.GetString("memo_fee_history"); got != "0:222" {
		t.Fatalf("R2 string: got %q, want 0:222", got)
	}
}

func mustOpen(t *testing.T, sdb *StateDB, root tcommon.Hash) *StateDB {
	t.Helper()
	s, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("open at %x: %v", root, err)
	}
	return s
}

// nil sysKV → rooted keys keep defaults, derived still load from dp-.
func TestDynPropLoadNilSysKVUsesDefaults(t *testing.T) {
	sdb := newTestStateDB(t)
	rawdb.WriteDynamicProperty(sdb.db.DiskDB(), "latest_block_header_number", be8(11))
	loaded := LoadDynamicProperties(sdb.db.DiskDB(), nil)
	def := NewDynamicProperties()
	if loaded.MaintenanceTimeInterval() != def.MaintenanceTimeInterval() {
		t.Fatalf("rooted should default with nil sysKV: %d", loaded.MaintenanceTimeInterval())
	}
	if loaded.LatestBlockHeaderNumber() != 11 {
		t.Fatalf("derived should load with nil sysKV: %d", loaded.LatestBlockHeaderNumber())
	}
}
