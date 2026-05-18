package core

import (
	"bytes"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// TestInsertBlock_PhaseTimings verifies that applyBlock emits a Trace-level
// "Block applied" record with the seven phase-timing keys defined in the
// block-sync-logging spec. We use a logfmt handler at Trace level so the
// record is captured verbatim into a buffer that we then grep.
func TestInsertBlock_PhaseTimings(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)

	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelTrace)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 9_000_000_000,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Block applied") {
		t.Fatalf("expected 'Block applied' line in log output, got:\n%s", out)
	}
	for _, key := range []string{
		"validate=",
		"execute=",
		"maintenance=",
		"commit=",
		"dpUpdate=",
		"persist=",
		"hooks=",
		"total=",
		"number=1",
	} {
		if !strings.Contains(out, key) {
			t.Errorf("missing key %q in Trace record:\n%s", key, out)
		}
	}
}
