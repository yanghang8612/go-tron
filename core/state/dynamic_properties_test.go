package state

import (
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
	if got := dp.WitnessPayPerBlock(); got != 16000000 {
		t.Errorf("WitnessPayPerBlock: got %d, want 16000000", got)
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
	if got := dp.UnfreezeDelayDays(); got != 14 {
		t.Errorf("UnfreezeDelayDays: got %d, want 14", got)
	}
	if got := dp.MaxCpuTimeOfOneTx(); got != 80 {
		t.Errorf("MaxCpuTimeOfOneTx: got %d, want 80", got)
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
	if got := loaded.UnfreezeDelayDays(); got != 14 {
		t.Errorf("UnfreezeDelayDays: got %d, want 14", got)
	}
	if got := loaded.WitnessPayPerBlock(); got != 16000000 {
		t.Errorf("WitnessPayPerBlock: got %d, want 16000000", got)
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
	if got != 1500 {
		t.Fatalf("default FreeNetLimit: want 1500, got %d", got)
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
