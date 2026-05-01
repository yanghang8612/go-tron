package main

import (
	"encoding/hex"
	"path/filepath"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
)

// TestLoadGenesisFile_JavaTronPrivate exercises the JSON loader against the
// committed cross-impl fixture and asserts the resulting genesis hash matches
// what the live java-tron private chain reports (cross-checked via
// p2p.TestProbeJavaTronGenesis on 2026-05-02).
//
// This is the same hash assertion as
// core.TestGenesisToBlock_MatchesJavaTronPrivateChain, but driven through the
// JSON loader path — protecting against silent drift between the file schema
// and the in-code Genesis builder.
func TestLoadGenesisFile_JavaTronPrivate(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	g, err := loadGenesisFile(filepath.Join(repoRoot, "test/fixtures/cross-impl/java-tron-private.json"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if g.Config.P2PVersion != 0 {
		t.Errorf("p2p_version: want 0, got %d", g.Config.P2PVersion)
	}
	if len(g.Accounts) != 2 {
		t.Fatalf("accounts: want 2, got %d", len(g.Accounts))
	}
	if g.Accounts[1].Balance != -9_223_372_036_854_775_808 {
		t.Errorf("Blackhole balance: want int64.Min, got %d", g.Accounts[1].Balance)
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	block, err := core.GenesisToBlock(g, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatalf("GenesisToBlock: %v", err)
	}
	const wantHex = "000000000000000075da3fe749503edb5d6121d96d450b980294a03648934988"
	id := block.ID()
	if got := hex.EncodeToString(id.Hash[:]); got != wantHex {
		t.Fatalf("genesis BlockID:\n  want %s\n  got  %s", wantHex, got)
	}
}
