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

func TestDefaultPath_RejectsBadName(t *testing.T) {
	cases := []string{"", "../escape", "a/b", "with/slash"}
	for _, c := range cases {
		if _, err := defaultPath(c); err == nil {
			t.Errorf("defaultPath(%q): want error, got nil", c)
		}
	}
}
