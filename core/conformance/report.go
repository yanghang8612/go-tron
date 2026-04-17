package conformance

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FieldDiff describes a single disagreement between gtron and java-tron
// for a (block, field) pair. Field uses dotted paths — e.g.
// "account:41abc…:balance" or "dp:energy_fee".
type FieldDiff struct {
	Field string `json:"field"`
	Got   string `json:"got"`
	Want  string `json:"want"`
}

// Divergence bundles the full failure payload for a single block. GotJSON
// and WantJSON are the C-digest payloads used by `gtron-replay --verbose`
// to print a human-readable diff.
type Divergence struct {
	BlockNum   uint64          `json:"blockNum"`
	FieldDiffs []FieldDiff     `json:"fieldDiffs"`
	GotJSON    json.RawMessage `json:"got,omitempty"`
	WantJSON   json.RawMessage `json:"want,omitempty"`
}

// BlockResult is one row in Report.BlockResults — pass or fail per block.
type BlockResult struct {
	BlockNum   uint64      `json:"blockNum"`
	Passed     bool        `json:"passed"`
	Divergence *Divergence `json:"divergence,omitempty"`
}

// Report is the terminal result of a ReplayRange invocation.
type Report struct {
	RangeName             string           `json:"rangeName"`
	Passed                bool             `json:"passed"`
	BlockResults          []BlockResult    `json:"blockResults"`
	FirstFailure          *Divergence      `json:"firstFailure,omitempty"`
	AllowlistHits         int              `json:"allowlistHits"`
	StaleAllowlistEntries []AllowlistEntry `json:"staleAllowlistEntries,omitempty"`
}

// String renders the report in a human-readable tabular form. No colors,
// no unicode frills — plain ASCII so it survives CI log scrapers.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== Conformance replay report: %s ===\n", r.RangeName)
	status := "PASS"
	if !r.Passed {
		status = "FAIL"
	}
	fmt.Fprintf(&b, "status          : %s\n", status)
	fmt.Fprintf(&b, "blocks replayed : %d\n", len(r.BlockResults))
	fmt.Fprintf(&b, "allowlist hits  : %d\n", r.AllowlistHits)
	fmt.Fprintf(&b, "stale allowlist : %d\n", len(r.StaleAllowlistEntries))
	if r.FirstFailure != nil {
		fmt.Fprintf(&b, "\n--- first failure at block %d ---\n", r.FirstFailure.BlockNum)
		for _, d := range r.FirstFailure.FieldDiffs {
			fmt.Fprintf(&b, "  %s\n    got : %s\n    want: %s\n", d.Field, d.Got, d.Want)
		}
	}
	if len(r.StaleAllowlistEntries) > 0 {
		b.WriteString("\n--- stale allowlist entries (never hit; candidates for removal) ---\n")
		for _, e := range r.StaleAllowlistEntries {
			fmt.Fprintf(&b, "  block=%d field=%s reason=%q\n", e.BlockNum, e.Field, e.Reason)
		}
	}
	return b.String()
}
