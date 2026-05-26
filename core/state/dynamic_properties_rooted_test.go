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

// FlushRooted stages every dynamic property into the system account's KV
// (rooted into the committed state root); derived keys are also left dirty for
// the flat dp- mirror.
func TestDynPropRootedFlushIncludesDerived(t *testing.T) {
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
	got, ok, err = reopened.SystemKVGet(kvdomains.SystemDynamicProperty, []byte("latest_block_header_number"))
	if err != nil || !ok {
		t.Fatalf("rooted latest_block_header_number missing: ok=%v err=%v", ok, err)
	}
	if len(got) != 8 || int64(binary.BigEndian.Uint64(got)) != 7 {
		t.Fatalf("rooted derived value = %v, want BE(7)", got)
	}
}

// FlushRooted clears non-derived dirty entries while derived entries remain for
// the later flat mirror Flush.
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

func TestDynPropFlatFlushWritesOnlyDerivedRuntimeKeys(t *testing.T) {
	sdb := newTestStateDB(t)
	db := sdb.db.DiskDB()
	dp := NewDynamicProperties()

	dp.Set("energy_fee", 420)                 // rooted governance
	dp.SetLatestProposalNum(9)                // rooted governance
	dp.SetString("energy_price_history", "x") // rooted string
	dp.SetLatestBlockHeaderNumber(5)          // derived/runtime mirror
	dp.SetLatestBlockHeaderHash(tcommon.HexToHash("1234"))

	dp.Flush(db)

	for _, key := range []string{"energy_fee", "latest_proposal_num", "energy_price_history"} {
		if v := rawdb.ReadDynamicProperty(db, key); len(v) != 0 {
			t.Fatalf("rooted key %q must not be in dp-, got %x", key, v)
		}
	}
	if v := rawdb.ReadDynamicProperty(db, "latest_block_header_number"); len(v) != 8 {
		t.Fatalf("derived number must be in dp-, got %x", v)
	}
	if v := rawdb.ReadDynamicProperty(db, "latest_block_header_hash"); len(v) != tcommon.HashLength {
		t.Fatalf("runtime hash must be in dp-, got %x", v)
	}
}

func TestDynPropFlushDerivedUsesTypedStoreBoundary(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("energy_fee", 420)
	dp.SetString("energy_price_history", "0:420")
	dp.SetLatestBlockHeaderNumber(5)
	dp.SetLatestBlockHeaderTimestamp(6)
	dp.SetLatestSolidifiedBlockNum(4)
	dp.SetLatestBlockHeaderHash(tcommon.HexToHash("1234"))

	store := newRecordingDerivedDPStore(true)
	dp.flushDerived(store)

	for _, key := range []string{"energy_fee", "energy_price_history"} {
		if _, ok := store.writes[key]; ok {
			t.Fatalf("rooted key %q must not be written through derived store", key)
		}
	}
	for _, key := range []string{
		"latest_block_header_number",
		"latest_block_header_timestamp",
		"latest_solidified_block_num",
		"latest_block_header_hash",
	} {
		if _, ok := store.writes[key]; !ok {
			t.Fatalf("derived key %q was not written through derived store", key)
		}
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

func TestDynPropLoadDerivedStoreFiltersRootedKeys(t *testing.T) {
	store := newRecordingDerivedDPStore(true)
	store.values["latest_block_header_number"] = be8(42)
	store.values["energy_fee"] = be8(999)
	loaded := loadDynamicPropertiesFromDerivedStore(store, nil)
	if loaded.LatestBlockHeaderNumber() != 42 {
		t.Fatalf("derived latest block number = %d, want 42", loaded.LatestBlockHeaderNumber())
	}
	if got := loaded.EnergyFee(); got != NewDynamicProperties().EnergyFee() {
		t.Fatalf("rooted energy_fee loaded from derived store: got %d", got)
	}

	fallback := newRecordingDerivedDPStore(false)
	fallback.values["latest_solidified_block_num"] = be8(7)
	loaded = loadDynamicPropertiesFromDerivedStore(fallback, nil)
	if got := loaded.LatestSolidifiedBlockNum(); got != 7 {
		t.Fatalf("fallback derived latest solidified = %d, want 7", got)
	}
	if !fallback.reads["latest_solidified_block_num"] {
		t.Fatal("fallback reader did not point-read derived key")
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

	// Flat latest is authoritative: reopening R1 reads current latest-domain
	// values. Historical reads are served by domain history/snapshots.
	atR1 := LoadDynamicProperties(sdb.db.DiskDB(), mustOpen(t, sdb, r1))
	if got := atR1.NextMaintenanceTime(); got != 222 {
		t.Fatalf("R1-open latest int64: got %d, want 222", got)
	}
	if got, _ := atR1.GetString("memo_fee_history"); got != "0:222" {
		t.Fatalf("R1-open latest string: got %q, want 0:222", got)
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

type recordingDerivedDPStore struct {
	values      map[string][]byte
	writes      map[string][]byte
	reads       map[string]bool
	iterateFast bool
}

func newRecordingDerivedDPStore(iterateFast bool) *recordingDerivedDPStore {
	return &recordingDerivedDPStore{
		values:      make(map[string][]byte),
		writes:      make(map[string][]byte),
		reads:       make(map[string]bool),
		iterateFast: iterateFast,
	}
}

func (s *recordingDerivedDPStore) ReadDerivedDynamicProperty(name string) []byte {
	s.reads[name] = true
	return append([]byte(nil), s.values[name]...)
}

func (s *recordingDerivedDPStore) IterateDerivedDynamicProperties(fn func(name string, value []byte)) bool {
	if !s.iterateFast {
		return false
	}
	for name, value := range s.values {
		fn(name, append([]byte(nil), value...))
	}
	return true
}

func (s *recordingDerivedDPStore) WriteDerivedDynamicProperty(name string, value []byte) {
	s.writes[name] = append([]byte(nil), value...)
}
