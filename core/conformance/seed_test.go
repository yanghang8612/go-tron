package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustAddr(s string) string {
	return "41" + strings.Repeat(s, 40)
}

func TestLoadSeed_MinimalRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	addr := mustAddr("a")
	writeJSON(t, path, Seed{
		Schema:       SchemaVersion,
		StartHeight:  1000,
		DynamicProps: map[string]int64{"energy_fee": 420},
		Accounts: []SeedAccount{
			{Address: addr, Balance: 9999, AccountType: 0},
		},
		ClosureAddresses: []string{addr},
	})
	loaded, err := LoadSeed(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.DynProps.EnergyFee(); got != 420 {
		t.Fatalf("energy_fee: got %d, want 420", got)
	}
	if len(loaded.Closure) != 1 {
		t.Fatalf("closure: %d", len(loaded.Closure))
	}
	if bal := loaded.StateDB.GetBalance(loaded.Closure[0]); bal != 9999 {
		t.Fatalf("balance: got %d, want 9999", bal)
	}
}

func TestLoadSeed_AcceptsJavaGetterKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	writeJSON(t, path, Seed{
		Schema:       SchemaVersion,
		DynamicProps: map[string]int64{"getEnergyFee": 777},
	})
	loaded, err := LoadSeed(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.DynProps.EnergyFee(); got != 777 {
		t.Fatalf("energy_fee via java getter: got %d, want 777", got)
	}
}

func TestLoadSeed_BadSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	writeJSON(t, path, Seed{Schema: SchemaVersion + 1})
	if _, err := LoadSeed(path); err == nil {
		t.Fatal("expected schema mismatch error")
	}
}

func TestLoadSeed_BadAddress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	writeJSON(t, path, Seed{
		Schema:           SchemaVersion,
		ClosureAddresses: []string{"not-a-41-prefixed-hex"},
	})
	if _, err := LoadSeed(path); err == nil {
		t.Fatal("expected address parse error")
	}
}

func TestLoadSeed_RawFieldRejectedForNow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	writeJSON(t, path, Seed{
		Schema: SchemaVersion,
		Accounts: []SeedAccount{
			{Address: mustAddr("a"), Raw: json.RawMessage(`{"balance":1}`)},
		},
	})
	if _, err := LoadSeed(path); err == nil {
		t.Fatal("expected raw unsupported error")
	}
}

func TestParseAddress(t *testing.T) {
	_, err := ParseAddress(mustAddr("a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseAddress("1234"); err == nil {
		t.Fatal("short address must fail")
	}
	if _, err := ParseAddress("42" + strings.Repeat("a", 40)); err == nil {
		t.Fatal("non-41-prefix must fail")
	}
	if _, err := ParseAddress("41" + strings.Repeat("g", 40)); err == nil {
		t.Fatal("non-hex must fail")
	}
}

func TestLoadFixtureMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	writeJSON(t, path, FixtureMeta{
		Schema:          SchemaVersion,
		Scenario:        "smoke",
		CapturedAt:      "2026-04-17T00:00:00Z",
		StartBlock:      100,
		EndBlock:        105,
		GenesisTime:     1529891469000,
		ActiveWitnesses: []string{mustAddr("b")},
	})
	m, err := LoadFixtureMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.StartBlock != 100 || m.EndBlock != 105 {
		t.Fatalf("meta: %+v", m)
	}

	badPath := filepath.Join(dir, "bad.json")
	writeJSON(t, badPath, FixtureMeta{Schema: SchemaVersion + 1})
	if _, err := LoadFixtureMeta(badPath); err == nil {
		t.Fatal("bad schema must fail")
	}
}
