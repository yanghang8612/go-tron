// Package fixture loads golden state snapshots extracted from a local
// java-tron node. Fixtures live under test/fixtures/<name>/fixture.json
// and are produced by scripts/fixtures/run.sh; see
// docs/superpowers/specs/2026-04-15-fixture-extraction-design.md.
package fixture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// SchemaVersion is the fixture schema this package understands.
// Bump only when the on-disk layout changes in a non-backward-compatible way.
const SchemaVersion = 1

// Fixture is a snapshot of java-tron state after running a named scenario.
// Fields that the scenario did not populate are left nil.
type Fixture struct {
	Schema            int                 `json:"schema"`
	Scenario          string              `json:"scenario"`
	JavaTron          JavaTronVersion     `json:"javaTron"`
	ExtractedAt       string              `json:"extractedAt"`
	BlockNum          uint64              `json:"blockNum"`
	BlockHash         string              `json:"blockHash,omitempty"`
	DynamicProperties map[string]int64    `json:"dynamicProperties,omitempty"`
	Accounts          map[string]*Account `json:"accounts,omitempty"`
	Receipts          map[string]*Receipt `json:"receipts,omitempty"`
}

// JavaTronVersion pins the reference implementation that produced the fixture.
// JarSha256 is the sha256 of the FullNode.jar that was run; ConfigSha256
// hashes the scenario's config.conf. Together they make the observation
// reproducible without relying on java-tron emitting a version string.
type JavaTronVersion struct {
	Version      string `json:"version"`
	JarSha256    string `json:"jarSha256"`
	ConfigSha256 string `json:"configSha256"`
}

// Account is a post-state snapshot of one account. Fields are added on
// demand by the scenario that needs them; absent fields decode to zero.
type Account struct {
	Balance int64  `json:"balance"`
	Type    string `json:"type,omitempty"`
}

// Receipt is a post-state snapshot of one transaction receipt.
// Intentionally minimal; extend as consuming scenarios require.
type Receipt struct {
	Fee        int64  `json:"fee,omitempty"`
	EnergyUsed int64  `json:"energyUsed,omitempty"`
	NetUsage   int64  `json:"netUsage,omitempty"`
	Result     string `json:"result,omitempty"`
}

// Load reads test/fixtures/<name>/fixture.json, validates the schema, and
// returns the decoded Fixture. On any error it calls t.Fatalf — callers
// should treat the return value as always non-nil.
func Load(t testing.TB, name string) *Fixture {
	t.Helper()
	path, err := defaultPath(name)
	if err != nil {
		t.Fatalf("fixture %q: %v", name, err)
	}
	f, err := loadFromPath(path)
	if err != nil {
		t.Fatalf("fixture %q: %v", name, err)
	}
	return f
}

// loadFromPath is the pure decode path. Separated from Load so tests can
// exercise it without spawning a real *testing.T.
func loadFromPath(path string) (*Fixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var probe struct {
		Schema int `json:"schema"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	if probe.Schema != SchemaVersion {
		return nil, fmt.Errorf("schema mismatch: got %d, want %d", probe.Schema, SchemaVersion)
	}

	var f Fixture
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if err := validateInt64Precision(raw); err != nil {
		return nil, err
	}

	return &f, nil
}

// validateInt64Precision rescans the JSON with UseNumber to confirm that
// every number inside dynamicProperties / accounts / receipts fits in int64.
// Guards against silent precision loss if fixture.json ever contains a value
// Go's default json decoder would coerce to float64.
func validateInt64Precision(raw []byte) error {
	var generic map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return fmt.Errorf("precision rescan: %w", err)
	}
	for _, section := range []string{"dynamicProperties", "accounts", "receipts"} {
		if err := walkInt64(generic[section], section); err != nil {
			return err
		}
	}
	return nil
}

func walkInt64(v any, path string) error {
	switch vv := v.(type) {
	case nil:
		return nil
	case json.Number:
		if _, err := strconv.ParseInt(vv.String(), 10, 64); err != nil {
			return fmt.Errorf("%s: value %q not int64: %w", path, vv.String(), err)
		}
	case string, bool:
		return nil
	case map[string]any:
		for k, child := range vv {
			if err := walkInt64(child, path+"."+k); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range vv {
			if err := walkInt64(child, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s: unexpected type %T", path, v)
	}
	return nil
}

// defaultPath resolves <repo_root>/test/fixtures/<name>/fixture.json.
// Uses runtime.Caller to anchor on this source file rather than the
// caller's working directory, so tests pass regardless of where `go test`
// is invoked from.
func defaultPath(name string) (string, error) {
	if name == "" || filepath.Base(name) != name {
		return "", errors.New("invalid fixture name")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot resolve package source path")
	}
	// thisFile = .../go-tron/internal/testutil/fixture/fixture.go
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(root, "test", "fixtures", name, "fixture.json"), nil
}
