package main

import (
	"fmt"
	"time"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/urfave/cli/v2"
)

var (
	backfillFromFlag = &cli.Uint64Flag{
		Name:  "from",
		Usage: "First block (inclusive) to re-derive history for. Block 0 (genesis) has no history; values <1 are clamped to 1.",
		Value: 1,
	}
	backfillToFlag = &cli.Uint64Flag{
		Name:  "to",
		Usage: "Last block (inclusive) to re-derive history for. Must be <= chain head. 0 means 'up to current head'.",
		Value: 0,
	}
	backfillResumeFlag = &cli.BoolFlag{
		Name:  "resume",
		Usage: "Continue from the persisted backfill cursor instead of restarting at --from.",
	}
	backfillProgressEveryFlag = &cli.Uint64Flag{
		Name:  "progress-every",
		Usage: "Log a progress line every N blocks.",
		Value: 10_000,
	}
)

// historyCommand groups the operator-facing State History Index maintenance
// subcommands. Today it holds only `backfill` (Slice 6); future slices can
// hang `verify` / `prune` / `info` here without growing the top-level CLI.
var historyCommand = &cli.Command{
	Name:  "history",
	Usage: "State History Index maintenance tools",
	Subcommands: []*cli.Command{
		{
			Name:  "backfill",
			Usage: "Re-derive the State History Index for a block range by replaying canonical blocks",
			Description: "Reconstructs the sh-* history rows for [--from, --to] by replaying each\n" +
				"block's state transition over the canonical chain data, using the same\n" +
				"capture path as live block application. Use this on a node that synced\n" +
				"before the index existed, or after pruning, to populate archive-query\n" +
				"history without a full re-sync.\n\n" +
				"The tool is idempotent (re-running over an indexed range overwrites\n" +
				"identically) and resumable (--resume continues from a persisted cursor).\n" +
				"It refuses ranges whose parent state is unavailable (pruned below the\n" +
				"range) with a clear error. It does NOT advance the chain head or mutate\n" +
				"live state — it only appends history index rows.",
			Flags: []cli.Flag{
				dataDirFlag,
				testnetFlag,
				genesisFileFlag,
				backfillFromFlag,
				backfillToFlag,
				backfillResumeFlag,
				backfillProgressEveryFlag,
			},
			Action: historyBackfillCmd,
		},
	},
}

func historyBackfillCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dbPath := chainDataDir(cfg.DataDir)

	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}

	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// SetupGenesisBlock is idempotent against an existing datadir; it returns
	// the persisted chain config (the source of truth for fork block numbers,
	// energy-limit fork, etc. that the replay must honour).
	chainConfig, _, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}

	bc, err := core.NewBlockChain(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), chainConfig)
	if err != nil {
		return fmt.Errorf("create blockchain: %w", err)
	}
	defer func() {
		if cerr := bc.Close(); cerr != nil {
			fmt.Printf("blockchain close: %v\n", cerr)
		}
	}()

	from := ctx.Uint64("from")
	to := ctx.Uint64("to")
	resume := ctx.Bool("resume")
	progressEvery := ctx.Uint64("progress-every")
	if progressEvery == 0 {
		progressEvery = 10_000
	}

	head := bc.CurrentBlock().Number()
	if to == 0 {
		// 0 is the "up to current head" sentinel.
		to = head
	}
	if head == 0 {
		return fmt.Errorf("backfill: chain has no blocks beyond genesis (head=0); nothing to do")
	}

	fmt.Printf("History backfill: datadir=%s range=[%d,%d] head=%d resume=%v\n",
		cfg.DataDir, from, to, head, resume)

	start := time.Now()
	var lastLogged uint64
	progressFn := func(p core.BackfillProgress) {
		// Always log the very last block; otherwise throttle to every N.
		if p.Block == p.To || p.Done == 1 || p.Block-lastLogged >= progressEvery {
			lastLogged = p.Block
			fmt.Printf("  block %d/%d (%d done, %.0f blk/s)\n", p.Block, p.To, p.Done, p.Rate)
		}
	}

	if err := bc.BackfillHistory(from, to, resume, progressFn); err != nil {
		return err
	}

	fmt.Printf("History backfill complete in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}
