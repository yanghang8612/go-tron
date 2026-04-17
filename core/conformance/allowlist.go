package conformance

import (
	"encoding/json"
	"fmt"
	"os"
)

// Allowlist is the per-range catalog of currently-expected divergences.
// During M0" bring-up it's populated with known parity gaps (reward v2 VI
// timing, freeze-v2 window size, etc.). Exit criterion: every range's
// allowlist empty.
type Allowlist struct {
	entries map[uint64]map[string]AllowlistEntry // blockNum → field → entry
	hits    map[uint64]map[string]bool           // blockNum → field → seen
}

// LoadAllowlist reads divergence-allowlist.json. Missing file is treated as
// an empty allowlist (not an error) so new ranges start clean without
// requiring a placeholder commit.
func LoadAllowlist(path string) (*Allowlist, error) {
	al := &Allowlist{
		entries: make(map[uint64]map[string]AllowlistEntry),
		hits:    make(map[uint64]map[string]bool),
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return al, nil
		}
		return nil, fmt.Errorf("read allowlist: %w", err)
	}
	if len(raw) == 0 {
		return al, nil
	}
	var list []AllowlistEntry
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("parse allowlist: %w", err)
	}
	for _, e := range list {
		if _, ok := al.entries[e.BlockNum]; !ok {
			al.entries[e.BlockNum] = map[string]AllowlistEntry{}
			al.hits[e.BlockNum] = map[string]bool{}
		}
		al.entries[e.BlockNum][e.Field] = e
	}
	return al, nil
}

// IsWhitelisted returns true if (blockNum, field) is covered by the
// allowlist. Marks the entry as hit for stale-detection.
func (a *Allowlist) IsWhitelisted(blockNum uint64, field string) bool {
	fields, ok := a.entries[blockNum]
	if !ok {
		return false
	}
	if _, ok := fields[field]; !ok {
		return false
	}
	a.hits[blockNum][field] = true
	return true
}

// Empty reports whether the allowlist has no entries. Used by the
// --exit-gate check to enforce M0" closure criterion.
func (a *Allowlist) Empty() bool { return len(a.entries) == 0 }

// Stale returns entries that exist in the allowlist but were never hit
// during the run — strong hint the underlying parity gap has been fixed
// and the entry can be removed.
func (a *Allowlist) Stale() []AllowlistEntry {
	var out []AllowlistEntry
	for blk, fields := range a.entries {
		for f, e := range fields {
			if !a.hits[blk][f] {
				out = append(out, e)
			}
		}
	}
	return out
}
