package state

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Test 1: Defaults — NewDynamicProperties returns correct default values.
func TestNewDynamicProperties_Defaults(t *testing.T) {
	dp := NewDynamicProperties()

	if got := dp.MaintenanceTimeInterval(); got != 21600000 {
		t.Errorf("MaintenanceTimeInterval: got %d, want 21600000", got)
	}
	if got := dp.WitnessPayPerBlock(); got != 32000000 {
		t.Errorf("WitnessPayPerBlock: got %d, want 32000000", got)
	}
	if got := dp.WitnessStandbyAllowance(); got != 115200000000 {
		t.Errorf("WitnessStandbyAllowance: got %d, want 115200000000", got)
	}
	if got := dp.TransactionFee(); got != 10 {
		t.Errorf("TransactionFee: got %d, want 10", got)
	}
	if got := dp.EnergyFee(); got != 100 {
		t.Errorf("EnergyFee: got %d, want 100", got)
	}
	if got := dp.CreateAccountFee(); got != 100000 {
		t.Errorf("CreateAccountFee: got %d, want 100000", got)
	}
	if got := dp.CreateNewAccountFeeInSystemContract(); got != 0 {
		t.Errorf("CreateNewAccountFeeInSystemContract: got %d, want 0", got)
	}
	if got := dp.TotalEnergyCurrentLimit(); got != 50000000000 {
		t.Errorf("TotalEnergyCurrentLimit: got %d, want 50000000000", got)
	}
	if got := dp.TotalNetLimit(); got != 43200000000 {
		t.Errorf("TotalNetLimit: got %d, want 43200000000", got)
	}
	if got := dp.UnfreezeDelayDays(); got != 0 {
		t.Errorf("UnfreezeDelayDays: got %d, want 0", got)
	}
	if got := dp.MaxCpuTimeOfOneTx(); got != 50 {
		t.Errorf("MaxCpuTimeOfOneTx: got %d, want 50", got)
	}
	if got := dp.AllowNewResourceModel(); got != false {
		t.Errorf("AllowNewResourceModel: got %v, want false", got)
	}
	if got := dp.LatestBlockHeaderNumber(); got != 0 {
		t.Errorf("LatestBlockHeaderNumber: got %d, want 0", got)
	}
	if got := dp.LatestBlockHeaderTimestamp(); got != 0 {
		t.Errorf("LatestBlockHeaderTimestamp: got %d, want 0", got)
	}
	if got := dp.LatestSolidifiedBlockNum(); got != 0 {
		t.Errorf("LatestSolidifiedBlockNum: got %d, want 0", got)
	}
	if got := dp.NextMaintenanceTime(); got != 0 {
		t.Errorf("NextMaintenanceTime: got %d, want 0", got)
	}
	if got := dp.LatestBlockHeaderHash(); !got.IsEmpty() {
		t.Errorf("LatestBlockHeaderHash: expected empty hash, got %v", got)
	}
}

// Test 2: Set/Get — set values and verify via typed getters.
func TestDynamicProperties_SetGet(t *testing.T) {
	dp := NewDynamicProperties()

	dp.SetLatestBlockHeaderNumber(42)
	dp.SetLatestBlockHeaderTimestamp(1700000000)
	dp.SetLatestSolidifiedBlockNum(40)
	dp.SetNextMaintenanceTime(1700021600000)

	h := common.HexToHash("aabbccdd")
	dp.SetLatestBlockHeaderHash(h)

	if got := dp.LatestBlockHeaderNumber(); got != 42 {
		t.Errorf("LatestBlockHeaderNumber: got %d, want 42", got)
	}
	if got := dp.LatestBlockHeaderTimestamp(); got != 1700000000 {
		t.Errorf("LatestBlockHeaderTimestamp: got %d, want 1700000000", got)
	}
	if got := dp.LatestSolidifiedBlockNum(); got != 40 {
		t.Errorf("LatestSolidifiedBlockNum: got %d, want 40", got)
	}
	if got := dp.NextMaintenanceTime(); got != 1700021600000 {
		t.Errorf("NextMaintenanceTime: got %d, want 1700021600000", got)
	}
	if got := dp.LatestBlockHeaderHash(); got != h {
		t.Errorf("LatestBlockHeaderHash: got %v, want %v", got, h)
	}
}

// Test 3: Flush and Load — flush to DB then load back and verify.
func TestDynamicProperties_FlushAndLoad(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dp := NewDynamicProperties()

	dp.SetLatestBlockHeaderNumber(100)
	dp.SetLatestBlockHeaderTimestamp(1699999999)
	dp.SetLatestSolidifiedBlockNum(99)
	dp.SetNextMaintenanceTime(1700100000)
	dp.Set("energy_fee", 200)

	h := common.HexToHash("deadbeef")
	dp.SetLatestBlockHeaderHash(h)

	dp.Flush(db)

	loaded := LoadDynamicProperties(db)

	if got := loaded.LatestBlockHeaderNumber(); got != 100 {
		t.Errorf("after load LatestBlockHeaderNumber: got %d, want 100", got)
	}
	if got := loaded.LatestBlockHeaderTimestamp(); got != 1699999999 {
		t.Errorf("after load LatestBlockHeaderTimestamp: got %d, want 1699999999", got)
	}
	if got := loaded.LatestSolidifiedBlockNum(); got != 99 {
		t.Errorf("after load LatestSolidifiedBlockNum: got %d, want 99", got)
	}
	if got := loaded.NextMaintenanceTime(); got != 1700100000 {
		t.Errorf("after load NextMaintenanceTime: got %d, want 1700100000", got)
	}
	if got := loaded.EnergyFee(); got != 200 {
		t.Errorf("after load EnergyFee: got %d, want 200", got)
	}
	if got := loaded.LatestBlockHeaderHash(); got != h {
		t.Errorf("after load LatestBlockHeaderHash: got %v, want %v", got, h)
	}
	// Defaults that weren't changed should still be intact.
	if got := loaded.MaintenanceTimeInterval(); got != 21600000 {
		t.Errorf("after load MaintenanceTimeInterval: got %d, want 21600000", got)
	}
}

// Test 4: Generic Get/Set — custom prop and nonexistent prop.
func TestDynamicProperties_GenericGetSet(t *testing.T) {
	dp := NewDynamicProperties()

	dp.Set("custom_param", 12345)
	v, ok := dp.Get("custom_param")
	if !ok {
		t.Error("Get custom_param: expected ok=true, got false")
	}
	if v != 12345 {
		t.Errorf("Get custom_param: got %d, want 12345", v)
	}

	_, ok = dp.Get("nonexistent_param")
	if ok {
		t.Error("Get nonexistent_param: expected ok=false, got true")
	}
}

// Test 5: Only dirty props flushed — create, flush empty, load, verify defaults still work.
func TestDynamicProperties_OnlyDirtyFlushed(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	// Flush a fresh DynamicProperties with no modifications.
	dp := NewDynamicProperties()
	dp.Flush(db)

	// Load from DB — defaults should survive via in-memory fallback.
	loaded := LoadDynamicProperties(db)

	// These should equal the defaults (nothing was flushed, so DB has no overrides,
	// and LoadDynamicProperties starts from defaults).
	if got := loaded.MaintenanceTimeInterval(); got != 21600000 {
		t.Errorf("MaintenanceTimeInterval: got %d, want 21600000", got)
	}
	if got := loaded.UnfreezeDelayDays(); got != 0 {
		t.Errorf("UnfreezeDelayDays: got %d, want 0", got)
	}
	if got := loaded.WitnessPayPerBlock(); got != 32000000 {
		t.Errorf("WitnessPayPerBlock: got %d, want 32000000", got)
	}

	// Now set one prop, flush, reload — only that one should differ from the fresh default.
	dp2 := NewDynamicProperties()
	dp2.Set("energy_fee", 999)
	dp2.Flush(db)

	loaded2 := LoadDynamicProperties(db)
	if got := loaded2.EnergyFee(); got != 999 {
		t.Errorf("EnergyFee after targeted flush: got %d, want 999", got)
	}
	// Other props remain at default.
	if got := loaded2.TransactionFee(); got != 10 {
		t.Errorf("TransactionFee should be default: got %d, want 10", got)
	}
}

// Test 7: FreeNetLimit typed getter.
func TestDynamicProperties_FreeNetLimit(t *testing.T) {
	dp := NewDynamicProperties()

	got := dp.FreeNetLimit()
	if got != 5000 {
		t.Fatalf("default FreeNetLimit: want 5000, got %d", got)
	}

	dp.Set("free_net_limit", 3000)
	if dp.FreeNetLimit() != 3000 {
		t.Fatalf("after set: want 3000, got %d", dp.FreeNetLimit())
	}
}

// Test 6: AllowNewResourceModel bool conversion.
func TestDynamicProperties_AllowNewResourceModel(t *testing.T) {
	dp := NewDynamicProperties()

	if dp.AllowNewResourceModel() {
		t.Error("AllowNewResourceModel: expected false by default")
	}

	dp.Set("allow_new_resource_model", 1)
	if !dp.AllowNewResourceModel() {
		t.Error("AllowNewResourceModel: expected true after setting to 1")
	}

	dp.Set("allow_new_resource_model", 0)
	if dp.AllowNewResourceModel() {
		t.Error("AllowNewResourceModel: expected false after setting back to 0")
	}
}

func TestDynamicProperties_All(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("energy_fee", 420)

	all := dp.All()
	if all["energy_fee"] != 420 {
		t.Fatalf("energy_fee: got %d, want 420", all["energy_fee"])
	}
	// Verify it's a copy
	all["energy_fee"] = 999
	if dp.EnergyFee() != 420 {
		t.Fatal("All() should return a copy, not a reference")
	}
	if _, ok := all["maintenance_time_interval"]; !ok {
		t.Fatal("missing maintenance_time_interval in All()")
	}
}

// String DP keys

func TestDynamicProperties_StringDefaults(t *testing.T) {
	dp := NewDynamicProperties()

	if got := dp.EnergyPriceHistory(); got != "0:100" {
		t.Errorf("EnergyPriceHistory default: got %q, want %q", got, "0:100")
	}
	if got := dp.BandwidthPriceHistory(); got != "0:10" {
		t.Errorf("BandwidthPriceHistory default: got %q, want %q", got, "0:10")
	}
	if got := dp.MemoFeeHistory(); got != "0:0" {
		t.Errorf("MemoFeeHistory default: got %q, want %q", got, "0:0")
	}
}

func TestDynamicProperties_StringSetGet(t *testing.T) {
	dp := NewDynamicProperties()

	dp.SetEnergyPriceHistory("0:100,1000000:200")
	if got := dp.EnergyPriceHistory(); got != "0:100,1000000:200" {
		t.Errorf("SetEnergyPriceHistory: got %q, want %q", got, "0:100,1000000:200")
	}
}

func TestDynamicProperties_StringFlushAndLoad(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dp := NewDynamicProperties()

	dp.SetEnergyPriceHistory("0:100,9999:200")
	dp.SetBandwidthPriceHistory("0:10,8888:20")
	dp.SetMemoFeeHistory("0:0,7777:50")
	dp.Flush(db)

	loaded := LoadDynamicProperties(db)

	if got := loaded.EnergyPriceHistory(); got != "0:100,9999:200" {
		t.Errorf("loaded EnergyPriceHistory: got %q, want %q", got, "0:100,9999:200")
	}
	if got := loaded.BandwidthPriceHistory(); got != "0:10,8888:20" {
		t.Errorf("loaded BandwidthPriceHistory: got %q, want %q", got, "0:10,8888:20")
	}
	if got := loaded.MemoFeeHistory(); got != "0:0,7777:50" {
		t.Errorf("loaded MemoFeeHistory: got %q, want %q", got, "0:0,7777:50")
	}
}

func TestDynamicProperties_BurnTrxAmount(t *testing.T) {
	dp := NewDynamicProperties()

	if got := dp.BurnTrxAmount(); got != 0 {
		t.Errorf("initial BurnTrxAmount: want 0, got %d", got)
	}

	dp.AddBurnTrx(1_000_000)
	if got := dp.BurnTrxAmount(); got != 1_000_000 {
		t.Errorf("after +1M: want 1000000, got %d", got)
	}

	dp.AddBurnTrx(500_000)
	if got := dp.BurnTrxAmount(); got != 1_500_000 {
		t.Errorf("after +500k: want 1500000, got %d", got)
	}

	// Zero delta is a no-op and must not mark the key dirty.
	dirtyBefore := len(dp.dirty)
	dp.AddBurnTrx(0)
	if len(dp.dirty) != dirtyBefore {
		t.Error("AddBurnTrx(0) must not change dirty set")
	}

	// Persistence round-trip: flush accumulated value, reload, verify.
	db := rawdb.NewMemoryDatabase()
	dp.Flush(db)
	loaded := LoadDynamicProperties(db)
	if got := loaded.BurnTrxAmount(); got != 1_500_000 {
		t.Errorf("after flush+load: want 1500000, got %d", got)
	}
}

func TestDynamicProperties_StringNotInAll(t *testing.T) {
	dp := NewDynamicProperties()
	all := dp.All()
	// The string-typed history keys must not appear in the int64 All() map.
	// (The "_done" int64 companions are fine to be there.)
	stringOnlyKeys := []string{"energy_price_history", "bandwidth_price_history", "memo_fee_history"}
	for _, k := range stringOnlyKeys {
		if _, ok := all[k]; ok {
			t.Errorf("All() must not contain string-typed key %q", k)
		}
	}
	// The int64 done-flags should be present.
	if _, ok := all["energy_price_history_done"]; !ok {
		t.Error("All() missing energy_price_history_done")
	}
	if _, ok := all["bandwidth_price_history_done"]; !ok {
		t.Error("All() missing bandwidth_price_history_done")
	}
	// Suppress unused import warning; strings.Contains is used elsewhere in the
	// test file so this reference keeps things tidy.
	_ = strings.Contains
}

func TestBlockFilledSlots_DefaultEmpty(t *testing.T) {
	dp := NewDynamicProperties()
	got := dp.BlockFilledSlots()
	if len(got) != BlockFilledSlotsNumber {
		t.Fatalf("default length: want %d, got %d", BlockFilledSlotsNumber, len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Errorf("default slot[%d] = %d, want 0", i, b)
		}
	}
	if got := dp.CalculateFilledSlotsCount(); got != 0 {
		t.Errorf("default fill rate: want 0, got %d", got)
	}
}

func TestBlockFilledSlots_RingRotation(t *testing.T) {
	dp := NewDynamicProperties()
	// Apply 130 filled blocks. Index should wrap; first 2 entries get
	// overwritten by the wrap, but every entry was set to 1 at some point so
	// the ring is fully filled.
	for i := 0; i < 130; i++ {
		dp.ApplyBlockToFilledSlots(true)
	}
	if got := dp.BlockFilledSlotsIndex(); got != 2 {
		t.Errorf("index after 130 applies: want 2 (130 mod 128), got %d", got)
	}
	for i, b := range dp.BlockFilledSlots() {
		if b != 1 {
			t.Errorf("slot[%d] = %d, want 1 (every position written)", i, b)
		}
	}
	if got := dp.CalculateFilledSlotsCount(); got != 100 {
		t.Errorf("fill rate after 130 applies: want 100, got %d", got)
	}
}

func TestBlockFilledSlots_FillRate(t *testing.T) {
	dp := NewDynamicProperties()
	// 64 filled, then 64 missed → 50% fill rate.
	for i := 0; i < 64; i++ {
		dp.ApplyBlockToFilledSlots(true)
	}
	for i := 0; i < 64; i++ {
		dp.ApplyBlockToFilledSlots(false)
	}
	if got := dp.CalculateFilledSlotsCount(); got != 50 {
		t.Errorf("fill rate: want 50, got %d", got)
	}
	if got := dp.BlockFilledSlotsIndex(); got != 0 {
		t.Errorf("index after 128 applies: want 0 (full wrap), got %d", got)
	}
}

func TestBlockFilledSlots_Persistence(t *testing.T) {
	dp := NewDynamicProperties()
	// Make a recognizable pattern: every other slot filled.
	for i := 0; i < 64; i++ {
		dp.ApplyBlockToFilledSlots(true)
		dp.ApplyBlockToFilledSlots(false)
	}
	want := dp.BlockFilledSlots()

	db := rawdb.NewMemoryDatabase()
	dp.Flush(db)
	loaded := LoadDynamicProperties(db)
	got := loaded.BlockFilledSlots()
	if len(got) != BlockFilledSlotsNumber {
		t.Fatalf("loaded length: want %d, got %d", BlockFilledSlotsNumber, len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot[%d]: want %d, got %d", i, want[i], got[i])
		}
	}
	if loaded.BlockFilledSlotsIndex() != dp.BlockFilledSlotsIndex() {
		t.Errorf("index round-trip: want %d, got %d",
			dp.BlockFilledSlotsIndex(), loaded.BlockFilledSlotsIndex())
	}
}

func TestBlockFilledSlots_SetLengthMismatchPanics(t *testing.T) {
	dp := NewDynamicProperties()
	defer func() {
		if r := recover(); r == nil {
			t.Error("SetBlockFilledSlots with wrong length must panic")
		}
	}()
	dp.SetBlockFilledSlots(make([]byte, 64)) // wrong length
}

// Suppress potentially-unused import warning when only some tests reference common.
var _ = common.Address{}

func TestAvailableContractType_Default(t *testing.T) {
	dp := NewDynamicProperties()
	got := dp.AvailableContractType()
	if len(got) != ContractTypeBitmapBytes {
		t.Fatalf("length: want %d, got %d", ContractTypeBitmapBytes, len(got))
	}
	wantPrefix := []byte{0x7f, 0xff, 0x1f, 0xc0, 0x03, 0x7e}
	for i, b := range wantPrefix {
		if got[i] != b {
			t.Errorf("byte[%d]: want 0x%02x, got 0x%02x", i, b, got[i])
		}
	}
	for i := len(wantPrefix); i < ContractTypeBitmapBytes; i++ {
		if got[i] != 0 {
			t.Errorf("byte[%d]: want 0 (zero-padded), got 0x%02x", i, got[i])
		}
	}
}

func TestActiveDefaultOperations_Default(t *testing.T) {
	dp := NewDynamicProperties()
	got := dp.ActiveDefaultOperations()
	if len(got) != ContractTypeBitmapBytes {
		t.Fatalf("length: want %d, got %d", ContractTypeBitmapBytes, len(got))
	}
	wantPrefix := []byte{0x7f, 0xff, 0x1f, 0xc0, 0x03, 0x3e}
	for i, b := range wantPrefix {
		if got[i] != b {
			t.Errorf("byte[%d]: want 0x%02x, got 0x%02x", i, b, got[i])
		}
	}
}

func TestIsContractTypeAvailable_DefaultSet(t *testing.T) {
	dp := NewDynamicProperties()
	// Defaults enable bits 0..6, 8..15, 20..28, 30, 33..38, 41..46.
	// Spot-check both available and unavailable bits.
	cases := []struct {
		id        int
		available bool
	}{
		{0, true},   // AccountCreate (bit 0 of 0x7f)
		{6, true},   // AssetIssue (bit 6 of 0x7f)
		{7, false},  // bit 7 of 0x7f is 0
		{45, true},  // UpdateEnergyLimit (in range 41..46)
		{48, false}, // ClearABI: not in default; activated by proposal 26
		{59, false}, // CancelAllUnfreezeV2: not in default
	}
	for _, c := range cases {
		got := dp.IsContractTypeAvailable(c.id)
		if got != c.available {
			t.Errorf("IsContractTypeAvailable(%d): want %v, got %v", c.id, c.available, got)
		}
	}
}

func TestAddSystemContractAndSetPermission_BothBitmaps(t *testing.T) {
	dp := NewDynamicProperties()
	if dp.IsContractTypeAvailable(48) {
		t.Fatal("invariant: bit 48 should not be available before activation")
	}
	dp.AddSystemContractAndSetPermission(48)

	if !dp.IsContractTypeAvailable(48) {
		t.Error("AvailableContractType bit 48 not set after activation")
	}
	if dp.ActiveDefaultOperations()[48/8]&(1<<(48%8)) == 0 {
		t.Error("ActiveDefaultOperations bit 48 not set after activation")
	}
}

func TestAddSystemContractAndSetPermission_Idempotent(t *testing.T) {
	dp := NewDynamicProperties()
	dp.AddSystemContractAndSetPermission(48)
	avail1 := append([]byte(nil), dp.AvailableContractType()...)
	active1 := append([]byte(nil), dp.ActiveDefaultOperations()...)

	dp.AddSystemContractAndSetPermission(48)
	avail2 := dp.AvailableContractType()
	active2 := dp.ActiveDefaultOperations()

	if string(avail1) != string(avail2) {
		t.Error("AvailableContractType changed on second add (must be idempotent)")
	}
	if string(active1) != string(active2) {
		t.Error("ActiveDefaultOperations changed on second add (must be idempotent)")
	}
}

func TestSetAvailableContractType_LengthMismatchPanics(t *testing.T) {
	dp := NewDynamicProperties()
	defer func() {
		if r := recover(); r == nil {
			t.Error("must panic on wrong length")
		}
	}()
	dp.SetAvailableContractType(make([]byte, 16))
}
