package conformance

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSeed_RoundTrip(t *testing.T) {
	orig := Seed{
		Schema:          SchemaVersion,
		JavaTronVersion: "4.8.2",
		StartHeight:     100,
		DynamicProps:    map[string]int64{"getEnergyFee": 100, "getTransactionFee": 10},
		Accounts: []SeedAccount{
			{Address: "41aaa", Balance: 1000, AccountType: 0},
			{Address: "41bbb", Balance: 2000, AccountType: 1, FrozenV1Net: 500},
		},
		Contracts:        []SeedContract{{Address: "41ccc", CodeHex: "60"}},
		ClosureAddresses: []string{"41aaa", "41bbb", "41ccc"},
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var back Seed
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(orig, back) {
		t.Fatalf("roundtrip: got %+v\nwant %+v", back, orig)
	}
}

func TestOracleEntry_RoundTrip(t *testing.T) {
	orig := OracleEntry{
		BlockNum: 42,
		DigestB:  "deadbeef",
		DiagC:    json.RawMessage(`{"foo":"bar"}`),
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var back OracleEntry
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.BlockNum != 42 || back.DigestB != "deadbeef" {
		t.Fatalf("fields: %+v", back)
	}
	// Raw bytes may differ in whitespace; unmarshal and compare.
	var diag map[string]string
	if err := json.Unmarshal(back.DiagC, &diag); err != nil {
		t.Fatal(err)
	}
	if diag["foo"] != "bar" {
		t.Fatalf("diag: %+v", diag)
	}
}

func TestAllowlistEntry_RoundTrip(t *testing.T) {
	orig := AllowlistEntry{
		BlockNum:      45_000_001,
		Field:         "account:41aaa:balance",
		Reason:        "known: VI timing at maintenance boundary",
		TrackingIssue: "internal:M1.5-vi-timing",
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var back AllowlistEntry
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(orig, back) {
		t.Fatalf("roundtrip: %+v vs %+v", back, orig)
	}
}

func TestFixtureMeta_RoundTrip(t *testing.T) {
	orig := FixtureMeta{
		Schema:          SchemaVersion,
		Scenario:        "smoke",
		JavaTronVersion: "4.8.2",
		JarSha256:       "deadbeef",
		CapturedAt:      "2026-04-17T00:00:00Z",
		StartBlock:      100,
		EndBlock:        105,
		GenesisTime:     1529891469000,
		ActiveWitnesses: []string{"41" + "a"},
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var back FixtureMeta
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(orig, back) {
		t.Fatalf("roundtrip: %+v vs %+v", back, orig)
	}
}
