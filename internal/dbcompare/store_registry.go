package dbcompare

import "strings"

// javaStoreSpec is an explicit completeness contract. Every LevelDB directory
// discovered in a java-tron database must appear here. State=true means the
// store contributes to the requested state audit; Compare=false deliberately
// fails the coverage gate when that store is present.
type javaStoreSpec struct {
	Name     string
	Scope    string
	Required bool
	State    bool
	Compare  bool
}

var javaStoreSpecs = []javaStoreSpec{
	{Name: "abi", Scope: "state", State: true, Compare: true},
	{Name: "account", Scope: "state", Required: true, State: true, Compare: true},
	{Name: "account-asset", Scope: "state", State: true, Compare: true},
	{Name: "account-index", Scope: "state", State: true, Compare: true},
	{Name: "accountid-index", Scope: "state", State: true, Compare: true},
	{Name: "asset-issue", Scope: "state", State: true, Compare: true},
	{Name: "asset-issue-v2", Scope: "state", State: true, Compare: true},
	{Name: "code", Scope: "state", State: true, Compare: true},
	{Name: "contract", Scope: "state", Required: true, State: true, Compare: true},
	{Name: "contract-state", Scope: "state", State: true, Compare: true},
	{Name: "DelegatedResource", Scope: "state", State: true, Compare: true},
	{Name: "DelegatedResourceAccountIndex", Scope: "state", State: true, Compare: true},
	{Name: "delegation", Scope: "state", State: true, Compare: true},
	{Name: "exchange", Scope: "state", State: true, Compare: true},
	{Name: "exchange-v2", Scope: "state", State: true, Compare: true},
	{Name: "IncrementalMerkleTree", Scope: "state", State: true, Compare: true},
	{Name: "market_account", Scope: "state", State: true, Compare: true},
	{Name: "market_order", Scope: "state", State: true, Compare: true},
	{Name: "market_pair_price_to_order", Scope: "state", State: true, Compare: true},
	{Name: "market_pair_to_price", Scope: "state", State: true, Compare: true},
	{Name: "nullifier", Scope: "state", State: true, Compare: true},
	{Name: "properties", Scope: "state", Required: true, State: true, Compare: true},
	{Name: "proposal", Scope: "state", State: true, Compare: true},
	{Name: "recent-block", Scope: "state-index", State: true, Compare: true},
	{Name: "reward-vi", Scope: "state-cache", State: true, Compare: true},
	{Name: "storage-row", Scope: "state", State: true, Compare: true},
	{Name: "tree-block-index", Scope: "state-index", State: true, Compare: true},
	{Name: "votes", Scope: "state", State: true, Compare: true},
	{Name: "witness", Scope: "state", Required: true, State: true, Compare: true},
	{Name: "witness_schedule", Scope: "state", State: true, Compare: true},
	{Name: "zkProof", Scope: "state-cache", State: true, Compare: true},

	// These stores exist in newer java-tron builds but go-tron has no matching
	// state model yet. Presence is therefore a hard incomplete-coverage result.
	{Name: "accountTrie", Scope: "unsupported-state", State: true},
	{Name: "account-asset-issue", Scope: "unsupported-state", State: true},
	{Name: "IncrementalMerkleVoucher", Scope: "unsupported-state", State: true},
	{Name: "staker", Scope: "unsupported-state", State: true},
	{Name: "staker-index", Scope: "unsupported-state", State: true},
	{Name: "tracker", Scope: "unsupported-state", State: true},

	// Chain/history/runtime stores do not represent the mutable head state.
	// They are still enumerated so an unknown directory can never be silently
	// mistaken for a successful full-state audit.
	{Name: "block", Scope: "chain", Required: true},
	{Name: "block-index", Scope: "chain", Required: true},
	{Name: "trans", Scope: "chain"},
	{Name: "transactionHistoryStore", Scope: "history"},
	{Name: "transactionRetStore", Scope: "history"},
	{Name: "account-trace", Scope: "history"},
	{Name: "balance-trace", Scope: "history"},
	{Name: "section-bloom", Scope: "index"},
	{Name: "pbft-sign-data", Scope: "finality-metadata"},
	{Name: "common-database", Scope: "finality-metadata"},
	{Name: "common", Scope: "node-metadata"},
	{Name: "peers", Scope: "node-metadata"},
	{Name: "block_KDB", Scope: "runtime-fork-cache"},
	{Name: "recent-transaction", Scope: "runtime-cache"},
	{Name: "trans-cache", Scope: "runtime-cache"},
	{Name: "checkpoint", Scope: "recovery-wal"},
	{Name: "check-point-v2", Scope: "recovery-wal"},
	{Name: "tmp", Scope: "recovery-wal"},
}

func javaStoreSpecByName(name string) (javaStoreSpec, bool) {
	for _, spec := range javaStoreSpecs {
		if equalStoreName(spec.Name, name) {
			return spec, true
		}
	}
	return javaStoreSpec{}, false
}

func equalStoreName(a, b string) bool {
	if a == b {
		return true
	}
	// Java has historically changed only the case of a few store paths.
	return len(a) == len(b) && strings.EqualFold(a, b)
}
