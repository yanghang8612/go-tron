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
