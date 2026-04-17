// Package conformance implements the M0" conformance-replay harness.
// Design: docs/superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md
package conformance

import "encoding/json"

// SchemaVersion is the on-disk fixture format version. Bump whenever any of
// the JSON structs below change shape in an incompatible way.
const SchemaVersion = 1

// Seed is the on-disk layout of seed.json — the starting state for a range
// plus the range-wide touched-address closure.
type Seed struct {
	Schema           int               `json:"schema"`
	JavaTronVersion  string            `json:"javaTronVersion"`
	StartHeight      uint64            `json:"startHeight"`
	DynamicProps     map[string]int64  `json:"dynamicProperties"`
	DynamicPropsHex  map[string]string `json:"dynamicPropertiesBytes,omitempty"`
	Accounts         []SeedAccount     `json:"accounts"`
	Contracts        []SeedContract    `json:"contracts"`
	ClosureAddresses []string          `json:"closureAddresses"` // 41-prefixed hex
}

// SeedAccount carries the subset of Account fields we've needed so far. When
// a range surfaces a field not listed here, extend the struct rather than
// stuffing everything through Raw — Raw is the escape hatch, not the plan.
type SeedAccount struct {
	Address     string          `json:"address"`
	Balance     int64           `json:"balance"`
	AccountType int32           `json:"accountType"`
	FrozenV1Net int64           `json:"frozenV1Net,omitempty"`
	Raw         json.RawMessage `json:"raw,omitempty"`
}

// SeedContract carries bytecode and ContractState for a deployed contract.
type SeedContract struct {
	Address string          `json:"address"`
	CodeHex string          `json:"code"`
	Raw     json.RawMessage `json:"raw,omitempty"`
}

// OracleEntry is one line in oracle.ndjson — java-tron's state digest at the
// end of a specific block.
type OracleEntry struct {
	BlockNum uint64          `json:"blockNum"`
	DigestB  string          `json:"digestB"` // hex(32)
	DiagC    json.RawMessage `json:"diagC,omitempty"`
}

// AllowlistEntry is one element in divergence-allowlist.json. During M0"
// bring-up the allowlist catalogs known parity gaps; exit requires it empty.
type AllowlistEntry struct {
	BlockNum       uint64 `json:"blockNum"`
	Field          string `json:"field"`
	Reason         string `json:"reason"`
	TrackingIssue  string `json:"trackingIssue"`
	ExpiresIsoDate string `json:"expires,omitempty"`
}

// FixtureMeta is fixture.json — schema + provenance + the contextual values
// the replay engine needs from java-tron but can't derive from blocks alone.
type FixtureMeta struct {
	Schema          int      `json:"schema"`
	Scenario        string   `json:"scenario"`
	JavaTronVersion string   `json:"javaTron.version"`
	JarSha256       string   `json:"javaTron.jarSha256"`
	CapturedAt      string   `json:"capturedAt"`
	StartBlock      uint64   `json:"startBlock"`
	EndBlock        uint64   `json:"endBlock"`
	GenesisTime     int64    `json:"genesisTime"`     // ms; passed to ProcessBlock
	ActiveWitnesses []string `json:"activeWitnesses"` // 41-hex at StartBlock-1
}
