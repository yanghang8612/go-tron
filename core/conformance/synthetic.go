package conformance

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// SyntheticRangeParams configures BuildSyntheticRange.
type SyntheticRangeParams struct {
	Dir         string // output dir; created if needed
	Scenario    string // goes into fixture.json
	StartBlock  uint64
	BlockCount  int
	WitnessHex  string // 42-char 41-prefixed hex; required
	WitnessBal  int64  // starting balance on the witness account
	CapturedAt  string // fixture.json timestamp; ISO-8601
	BlockPeriod int64  // ms between blocks; default 3000
}

// BuildSyntheticRange writes a self-consistent range to params.Dir: seed.json,
// blocks.bin, oracle.ndjson, divergence-allowlist.json (empty), fixture.json.
// The "oracle" is whatever the replay engine itself computes after running
// each block — i.e. this is the harness validated against itself, useful as
// a smoke corpus and for regression-testing the engine plumbing.
func BuildSyntheticRange(params SyntheticRangeParams) error {
	if params.BlockPeriod == 0 {
		params.BlockPeriod = 3000
	}
	if len(params.WitnessHex) != 42 {
		return fmt.Errorf("witnessHex must be 42 chars, got %d", len(params.WitnessHex))
	}
	if err := os.MkdirAll(params.Dir, 0o755); err != nil {
		return err
	}

	seed := Seed{
		Schema:       SchemaVersion,
		StartHeight:  params.StartBlock,
		DynamicProps: map[string]int64{"witness_pay_per_block": 32_000_000},
		Accounts: []SeedAccount{
			{Address: params.WitnessHex, Balance: params.WitnessBal, AccountType: 0},
		},
		ClosureAddresses: []string{params.WitnessHex},
	}
	if err := writeJSONFile(filepath.Join(params.Dir, "seed.json"), seed); err != nil {
		return err
	}

	witness, err := ParseAddress(params.WitnessHex)
	if err != nil {
		return err
	}

	// Build an empty-block chain from StartBlock..StartBlock+BlockCount-1.
	blocks := make([]*types.Block, params.BlockCount)
	var parent *types.Block
	for i := 0; i < params.BlockCount; i++ {
		n := params.StartBlock + uint64(i)
		raw := &corepb.BlockHeaderRaw{
			Number:         int64(n),
			Timestamp:      int64(n) * params.BlockPeriod,
			WitnessAddress: witness[:],
		}
		if parent != nil {
			h := parent.Hash()
			raw.ParentHash = h[:]
		}
		blk := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{RawData: raw},
		})
		blocks[i] = blk
		parent = blk
	}
	if err := WriteBlocks(filepath.Join(params.Dir, "blocks.bin"), blocks); err != nil {
		return err
	}

	// Run the blocks through the engine to capture the oracle.
	loaded, err := LoadSeed(filepath.Join(params.Dir, "seed.json"))
	if err != nil {
		return err
	}
	entries := make([]OracleEntry, 0, len(blocks))
	for _, blk := range blocks {
		if _, err := core.ProcessBlock(loaded.StateDB, loaded.DynProps, blk, loaded.DiskDB,
			[]tcommon.Address{witness}, 0); err != nil {
			return fmt.Errorf("ProcessBlock %d: %w", blk.Number(), err)
		}
		d := DigestB(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)
		entries = append(entries, OracleEntry{
			BlockNum: blk.Number(),
			DigestB:  hex.EncodeToString(d[:]),
			DiagC:    DigestC(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps),
		})
	}
	if err := WriteOracle(filepath.Join(params.Dir, "oracle.ndjson"), entries); err != nil {
		return err
	}

	// Empty allowlist.
	if err := os.WriteFile(filepath.Join(params.Dir, "divergence-allowlist.json"), []byte("[]\n"), 0o644); err != nil {
		return err
	}

	meta := FixtureMeta{
		Schema:          SchemaVersion,
		Scenario:        params.Scenario,
		JavaTronVersion: "synthetic",
		JarSha256:       strings.Repeat("0", 64),
		CapturedAt:      params.CapturedAt,
		StartBlock:      params.StartBlock,
		EndBlock:        params.StartBlock + uint64(params.BlockCount) - 1,
		GenesisTime:     0,
		ActiveWitnesses: []string{params.WitnessHex},
	}
	return writeJSONFile(filepath.Join(params.Dir, "fixture.json"), meta)
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
