package rawdb

import "testing"

func TestClassifyPhysicalKeyString(t *testing.T) {
	tests := []struct {
		key  string
		want PhysicalKeyFamily
	}{
		{"state-commitment-branch-v1-\x01\x02", PhysicalKeyFamilyCommitment},
		{"state-commitment-domain-v1-root", PhysicalKeyFamilyCommitment},
		{"state-account-latest-v1-owner", PhysicalKeyFamilyAccountLatest},
		{"state-kv-latest-v2-owner", PhysicalKeyFamilyKVLatest},
		{"state-kv-generation-v2-owner", PhysicalKeyFamilyKVGeneration},
		{"state-code-v1-hash", PhysicalKeyFamilyStateCode},
		{"state-tx-range-v1-row", PhysicalKeyFamilyStateHistory},
		{"state-changeset-v2-row", PhysicalKeyFamilyStateHistory},
		{"state-change-index-v2-row", PhysicalKeyFamilyStateHistory},
		{"sync-staged-block-v1-row", PhysicalKeyFamilyStagedBody},
		{"b-row", PhysicalKeyFamilyBlockBody},
		{"tx-hash", PhysicalKeyFamilyTransactionIndex},
		{"ti-hash", PhysicalKeyFamilyTransactionHistory},
		{"tib-height", PhysicalKeyFamilyTransactionHistory},
		{"LastBlock", PhysicalKeyFamilyChainMetadata},
		{"stage-progress-v1-Finish", PhysicalKeyFamilyChainMetadata},
		{"dp-energy_fee", PhysicalKeyFamilyOther},
	}
	for _, test := range tests {
		if got := ClassifyPhysicalKeyString(test.key); got != test.want {
			t.Fatalf("classify %q = %s, want %s", test.key, PhysicalKeyFamilyName(got), PhysicalKeyFamilyName(test.want))
		}
	}
}

func TestPhysicalKeyFamilyNamesAreUnique(t *testing.T) {
	seen := make(map[string]PhysicalKeyFamily, PhysicalKeyFamilyCount)
	for family := PhysicalKeyFamily(0); family < PhysicalKeyFamilyCount; family++ {
		name := PhysicalKeyFamilyName(family)
		if previous, ok := seen[name]; ok {
			t.Fatalf("families %d and %d share metric name %q", previous, family, name)
		}
		seen[name] = family
	}
}
