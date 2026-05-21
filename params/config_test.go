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

// TestNileGenesis_MaintenanceTimeIntervalBootstrap locks the Nile
// bootstrap interval at 600_000 ms (10 min) — config-nile.conf:516.
// The mainnet default is 21_600_000 (6h). Proposals #19589 (2024-01)
// raised the live value to 21.6M and #19597 (2024-03) then set it to
// 1.8M (30 min, current Nile-live), so the chain only matches Nile
// when bootstrapped at 600k and allowed to replay those proposals.
func TestNileGenesis_MaintenanceTimeIntervalBootstrap(t *testing.T) {
	g := DefaultNileGenesis()
	got, ok := g.DynamicProperties["maintenance_time_interval"]
	if !ok {
		t.Fatal("DefaultNileGenesis.DynamicProperties missing maintenance_time_interval")
	}
	if got != 600000 {
		t.Fatalf("DefaultNileGenesis maintenance_time_interval: got %d, want 600000 (config-nile.conf:516)", got)
	}
}

// TestNileGenesis_ShieldedTransactionFeeBootstrap locks the historical
// shielded fee used by live Nile. Nile was initialized before java-tron
// c1485d4e8 lowered the missing-store default from 10_000_000 to 100_000,
// so the live chain kept 10 ZEN and reports that value in historical
// ShieldedTransfer transaction infos.
func TestNileGenesis_ShieldedTransactionFeeBootstrap(t *testing.T) {
	g := DefaultNileGenesis()
	got, ok := g.DynamicProperties["shielded_transaction_fee"]
	if !ok {
		t.Fatal("DefaultNileGenesis.DynamicProperties missing shielded_transaction_fee")
	}
	if got != 10000000 {
		t.Fatalf("DefaultNileGenesis shielded_transaction_fee: got %d, want 10000000", got)
	}
}
