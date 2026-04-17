package conformance

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAllowlist_MissingFileIsEmpty(t *testing.T) {
	dir := t.TempDir()
	al, err := LoadAllowlist(filepath.Join(dir, "does-not-exist.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !al.Empty() {
		t.Fatal("missing file must load as empty")
	}
}

func TestAllowlist_EmptyFileIsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "al.json")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	al, err := LoadAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	if !al.Empty() {
		t.Fatal("empty file must load as empty")
	}
}

func TestAllowlist_LookupHit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "al.json")
	writeJSON(t, path, []AllowlistEntry{
		{BlockNum: 100, Field: "dp:latest_block_header_number", Reason: "known"},
		{BlockNum: 100, Field: "account:41aaa:balance", Reason: "known"},
		{BlockNum: 200, Field: "dp:total_energy_current_limit", Reason: "known"},
	})
	al, err := LoadAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	if al.Empty() {
		t.Fatal("should not be empty")
	}
	if !al.IsWhitelisted(100, "dp:latest_block_header_number") {
		t.Fatal("expected hit for 100/latest_block_header_number")
	}
	if al.IsWhitelisted(100, "dp:some_other_key") {
		t.Fatal("unexpected hit on non-listed field")
	}
	if al.IsWhitelisted(999, "dp:total_energy_current_limit") {
		t.Fatal("unexpected hit on non-listed block")
	}
}

func TestAllowlist_StaleDetection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "al.json")
	writeJSON(t, path, []AllowlistEntry{
		{BlockNum: 100, Field: "hit.me", Reason: "x"},
		{BlockNum: 100, Field: "stale.me", Reason: "y"},
		{BlockNum: 200, Field: "also.stale", Reason: "z"},
	})
	al, err := LoadAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	al.IsWhitelisted(100, "hit.me") // mark as hit

	stale := al.Stale()
	if len(stale) != 2 {
		t.Fatalf("want 2 stale, got %d (%+v)", len(stale), stale)
	}
	seen := map[string]bool{}
	for _, e := range stale {
		seen[e.Field] = true
	}
	if !seen["stale.me"] || !seen["also.stale"] {
		t.Fatalf("missing expected stale: %+v", stale)
	}
}

func TestAllowlist_Malformed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "al.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAllowlist(path); err == nil {
		t.Fatal("malformed json must error")
	}
}
