package params

import "testing"

func TestBlockInterval(t *testing.T) {
	if BlockProducedInterval != 3000 {
		t.Fatalf("expected 3000, got %d", BlockProducedInterval)
	}
}

func TestMaxActiveWitnesses(t *testing.T) {
	if MaxActiveWitnessNum != 27 {
		t.Fatalf("expected 27, got %d", MaxActiveWitnessNum)
	}
}

func TestMainnetConfig(t *testing.T) {
	cfg := MainnetChainConfig
	if cfg.ChainID != 1 {
		t.Fatalf("expected mainnet chain ID 1, got %d", cfg.ChainID)
	}
	if cfg.P2PVersion != 11111 {
		t.Fatalf("expected P2P version 11111, got %d", cfg.P2PVersion)
	}
}

func TestNileConfig(t *testing.T) {
	cfg := NileChainConfig
	if cfg.P2PVersion != 201910292 {
		t.Fatalf("expected nile P2P version 201910292, got %d", cfg.P2PVersion)
	}
}

// TestNileGenesis_ProposalExpireTimeOverride locks the Nile-specific
// proposal_expire_time seed against accidental removal. java-tron's
// config-nile.conf:517 sets `proposalExpireTime = 600000` (10 min);
// gtron's bare DP default is 259_200_000 (3 days, mainnet-biased), so
// dropping this seed silently reintroduces the proposal #1 CANCELED
// soak failure diagnosed 2026-05-11 (see memory:
// project_proposal_expire_time_bug.md).
func TestNileGenesis_ProposalExpireTimeOverride(t *testing.T) {
	g := DefaultNileGenesis()
	got, ok := g.DynamicProperties["proposal_expire_time"]
	if !ok {
		t.Fatal("DefaultNileGenesis.DynamicProperties missing proposal_expire_time override")
	}
	if got != 600000 {
		t.Fatalf("DefaultNileGenesis proposal_expire_time: got %d, want 600000 (config-nile.conf:517)", got)
	}
}
