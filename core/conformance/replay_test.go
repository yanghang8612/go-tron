package conformance

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// buildRangeFixture delegates to BuildSyntheticRange so tests and the smoke-
// corpus generator share one code path.
func buildRangeFixture(t *testing.T, dir string, witnessBal int64) tcommon.Address {
	t.Helper()
	witnessHex := "41" + strings.Repeat("a", 40)
	err := BuildSyntheticRange(SyntheticRangeParams{
		Dir:        dir,
		Scenario:   "test",
		StartBlock: 100,
		BlockCount: 2,
		WitnessHex: witnessHex,
		WitnessBal: witnessBal,
		CapturedAt: "2026-04-17T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	w, _ := ParseAddress(witnessHex)
	return w
}

func TestReplayRange_SyntheticPass(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)
	cfg := ReplayConfig{
		RangeName:       "test-pass",
		SeedPath:        filepath.Join(dir, "seed.json"),
		BlocksPath:      filepath.Join(dir, "blocks.bin"),
		OraclePath:      filepath.Join(dir, "oracle.ndjson"),
		AllowlistPath:   filepath.Join(dir, "divergence-allowlist.json"),
		ActiveWitnesses: []tcommon.Address{witness},
	}
	rep, err := ReplayRange(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Passed {
		t.Fatalf("expected pass, got report:\n%s", rep.String())
	}
	if len(rep.BlockResults) != 2 {
		t.Fatalf("want 2 block results, got %d", len(rep.BlockResults))
	}
	if rep.FirstFailure != nil {
		t.Fatalf("unexpected failure: %+v", rep.FirstFailure)
	}
}

func TestReplayRange_SyntheticDivergence_DifferentSeed(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)

	// Rewrite seed with a different initial witness balance so the
	// post-state balance after block 100 differs → digest diverges.
	path := filepath.Join(dir, "seed.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var seed Seed
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatal(err)
	}
	seed.Accounts[0].Balance = 50_000 // was 1_000
	writeJSON(t, path, seed)

	cfg := ReplayConfig{
		RangeName:       "test-diverge",
		SeedPath:        path,
		BlocksPath:      filepath.Join(dir, "blocks.bin"),
		OraclePath:      filepath.Join(dir, "oracle.ndjson"),
		AllowlistPath:   filepath.Join(dir, "does-not-exist.json"),
		ActiveWitnesses: []tcommon.Address{witness},
	}
	rep, err := ReplayRange(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Passed {
		t.Fatal("expected failure")
	}
	if rep.FirstFailure == nil {
		t.Fatal("expected FirstFailure")
	}
	// Divergence must fire at the first block (100) since the balance gap
	// is present from the moment seed loads.
	if rep.FirstFailure.BlockNum != 100 {
		t.Fatalf("want failure at block 100, got %d", rep.FirstFailure.BlockNum)
	}
	// FieldDiffs must name the witness's balance field.
	found := false
	for _, d := range rep.FirstFailure.FieldDiffs {
		if strings.Contains(d.Field, "balance") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no balance-related field diff: %+v", rep.FirstFailure.FieldDiffs)
	}
}

func TestReplayRange_AllowlistCovers(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)

	// Same construction as the divergence test — different initial balance.
	path := filepath.Join(dir, "seed.json")
	raw, _ := os.ReadFile(path)
	var seed Seed
	_ = json.Unmarshal(raw, &seed)
	seed.Accounts[0].Balance = 50_000
	writeJSON(t, path, seed)

	// Determine the actual field name by computing the fields diffJSON
	// would emit for the witness account — safer than hardcoding.
	witnessHex := hex.EncodeToString(witness[:])

	// Broad allowlist: whitelist every account field, for both blocks.
	allowlistEntries := []AllowlistEntry{}
	for _, blk := range []uint64{100, 101} {
		for _, field := range []string{"balance", "createTime"} {
			allowlistEntries = append(allowlistEntries, AllowlistEntry{
				BlockNum: blk,
				Field:    "account:" + witnessHex + ":" + field,
				Reason:   "test",
			})
		}
	}
	writeJSON(t, filepath.Join(dir, "allowlist.json"), allowlistEntries)

	cfg := ReplayConfig{
		RangeName:       "test-allowlist",
		SeedPath:        path,
		BlocksPath:      filepath.Join(dir, "blocks.bin"),
		OraclePath:      filepath.Join(dir, "oracle.ndjson"),
		AllowlistPath:   filepath.Join(dir, "allowlist.json"),
		ActiveWitnesses: []tcommon.Address{witness},
	}
	rep, err := ReplayRange(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Passed {
		t.Fatalf("expected allowlist to cover divergence, got:\n%s", rep.String())
	}
	if rep.AllowlistHits == 0 {
		t.Fatal("expected allowlist hits > 0")
	}
}

func TestReplayRange_StaleAllowlist(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)

	// Add an allowlist entry for a block/field that never diverges.
	writeJSON(t, filepath.Join(dir, "allowlist.json"), []AllowlistEntry{
		{BlockNum: 100, Field: "dp:does_not_exist", Reason: "test"},
	})
	cfg := ReplayConfig{
		RangeName:       "test-stale",
		SeedPath:        filepath.Join(dir, "seed.json"),
		BlocksPath:      filepath.Join(dir, "blocks.bin"),
		OraclePath:      filepath.Join(dir, "oracle.ndjson"),
		AllowlistPath:   filepath.Join(dir, "allowlist.json"),
		ActiveWitnesses: []tcommon.Address{witness},
	}
	rep, err := ReplayRange(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Passed {
		t.Fatalf("clean replay should still pass (allowlist unused), got:\n%s", rep.String())
	}
	if len(rep.StaleAllowlistEntries) != 1 {
		t.Fatalf("want 1 stale entry, got %d", len(rep.StaleAllowlistEntries))
	}
}

func TestReplayRange_OracleHeightMismatch(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)

	// Rewrite oracle with wrong block numbers.
	path := filepath.Join(dir, "oracle.ndjson")
	rdr, _ := openOracleReader(path)
	var entries []OracleEntry
	for {
		e, err := rdr.Next()
		if err != nil {
			break
		}
		e.BlockNum += 10 // intentional mismatch
		entries = append(entries, e)
	}
	rdr.Close()
	if err := WriteOracle(path, entries); err != nil {
		t.Fatal(err)
	}

	cfg := ReplayConfig{
		RangeName:       "test-height",
		SeedPath:        filepath.Join(dir, "seed.json"),
		BlocksPath:      filepath.Join(dir, "blocks.bin"),
		OraclePath:      path,
		AllowlistPath:   filepath.Join(dir, "does-not-exist.json"),
		ActiveWitnesses: []tcommon.Address{witness},
	}
	if _, err := ReplayRange(context.Background(), cfg); err == nil {
		t.Fatal("expected height-mismatch error")
	}
}
