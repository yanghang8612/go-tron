package fixture

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFromPath_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	payload := `{
		"schema": 1,
		"scenario": "test-round-trip",
		"javaTron": {"version": "4.7.5", "jarSha256": "abc1234", "configSha256": "deadbeef"},
		"extractedAt": "2026-04-15T12:00:00Z",
		"blockNum": 42,
		"blockHash": "0xdeadbeef",
		"dynamicProperties": {
			"MAINTENANCE_TIME_INTERVAL": 21600000,
			"MAX_INT64": 9223372036854775807
		}
	}`
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := loadFromPath(path)
	if err != nil {
		t.Fatalf("loadFromPath: %v", err)
	}

	if got.Schema != SchemaVersion {
		t.Errorf("schema: got %d, want %d", got.Schema, SchemaVersion)
	}
	if got.Scenario != "test-round-trip" {
		t.Errorf("scenario: got %q", got.Scenario)
	}
	if got.JavaTron.Version != "4.7.5" || got.JavaTron.JarSha256 != "abc1234" {
		t.Errorf("javaTron: got %+v", got.JavaTron)
	}
	if got.BlockNum != 42 {
		t.Errorf("blockNum: got %d", got.BlockNum)
	}
	if got.DynamicProperties["MAINTENANCE_TIME_INTERVAL"] != 21600000 {
		t.Errorf("MAINTENANCE_TIME_INTERVAL: got %d", got.DynamicProperties["MAINTENANCE_TIME_INTERVAL"])
	}
	if got.DynamicProperties["MAX_INT64"] != math.MaxInt64 {
		t.Errorf("MAX_INT64: got %d, want %d", got.DynamicProperties["MAX_INT64"], int64(math.MaxInt64))
	}
}

func TestLoadFromPath_SchemaMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	if err := os.WriteFile(path, []byte(`{"schema": 99, "scenario": "x"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadFromPath(path)
	if err == nil || !strings.Contains(err.Error(), "schema mismatch") {
		t.Fatalf("want schema mismatch error, got %v", err)
	}
}

func TestLoadFromPath_MissingFile(t *testing.T) {
	_, err := loadFromPath(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("want error for missing file, got nil")
	}
}

// Guards against a future refactor that silently lets a JSON number be
// coerced to float64 (>2^53 precision loss) without the validator noticing.
func TestLoadFromPath_PrecisionLossDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	// 9223372036854775808 = MaxInt64+1 — strictly un-representable as int64.
	payload := `{
		"schema": 1,
		"scenario": "overflow",
		"javaTron": {"version": "", "jarSha256": "", "configSha256": ""},
		"extractedAt": "",
		"blockNum": 0,
		"dynamicProperties": {"OVERFLOW": 9223372036854775808}
	}`
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadFromPath(path)
	if err == nil {
		t.Fatalf("want int64 overflow error, got nil")
	}
	// Either our walker catches it ("not int64") or json.Decoder catches it
	// at unmarshal ("cannot unmarshal number"). Both are acceptable —
	// what matters is the value does not silently decode to zero or float.
	msg := err.Error()
	if !strings.Contains(msg, "not int64") && !strings.Contains(msg, "cannot unmarshal number") {
		t.Fatalf("want overflow-related error, got %q", msg)
	}
}

func TestLoad_SyntheticV1Frozen(t *testing.T) {
	fix := Load(t, "02-v1-frozen-synthetic")

	if fix.Source != "synthetic" {
		t.Errorf("source: got %q, want %q", fix.Source, "synthetic")
	}
	if got := len(fix.Accounts); got != 3 {
		t.Fatalf("accounts: got %d, want 3", got)
	}

	pureV1 := fix.Accounts["TXfakeAcctPureV1"]
	if pureV1 == nil {
		t.Fatal("TXfakeAcctPureV1 missing")
	}
	if len(pureV1.FrozenBandwidth) != 1 || pureV1.FrozenBandwidth[0].Balance != 1_000_000_000 {
		t.Errorf("pureV1 frozenBandwidth: %+v", pureV1.FrozenBandwidth)
	}
	if pureV1.FrozenEnergy == nil || pureV1.FrozenEnergy.Balance != 1_000_000_000 {
		t.Errorf("pureV1 frozenEnergy: %+v", pureV1.FrozenEnergy)
	}
	if len(pureV1.FrozenV2) != 0 {
		t.Errorf("pureV1 frozenV2 should be empty, got %+v", pureV1.FrozenV2)
	}

	pureV2 := fix.Accounts["TXfakeAcctPureV2"]
	if pureV2 == nil {
		t.Fatal("TXfakeAcctPureV2 missing")
	}
	if len(pureV2.FrozenBandwidth) != 0 || pureV2.FrozenEnergy != nil {
		t.Errorf("pureV2 should have no V1 frozen: %+v", pureV2)
	}
	if len(pureV2.FrozenV2) != 2 {
		t.Errorf("pureV2 frozenV2: got %d entries, want 2", len(pureV2.FrozenV2))
	}

	mixed := fix.Accounts["TXfakeAcctMixed"]
	if mixed == nil {
		t.Fatal("TXfakeAcctMixed missing")
	}
	if mixed.DelegatedFrozenBandwidth != 500_000_000 || mixed.DelegatedFrozenEnergy != 500_000_000 {
		t.Errorf("mixed delegated: bw=%d energy=%d",
			mixed.DelegatedFrozenBandwidth, mixed.DelegatedFrozenEnergy)
	}
	if len(mixed.FrozenBandwidth) != 1 || len(mixed.FrozenV2) != 2 {
		t.Errorf("mixed shape: v1=%d v2=%d", len(mixed.FrozenBandwidth), len(mixed.FrozenV2))
	}

	if fix.DynamicProperties["total_net_weight"] != 3000 {
		t.Errorf("total_net_weight: got %d", fix.DynamicProperties["total_net_weight"])
	}
}

func TestDefaultPath_RejectsBadName(t *testing.T) {
	cases := []string{"", "../escape", "a/b", "with/slash"}
	for _, c := range cases {
		if _, err := defaultPath(c); err == nil {
			t.Errorf("defaultPath(%q): want error, got nil", c)
		}
	}
}
