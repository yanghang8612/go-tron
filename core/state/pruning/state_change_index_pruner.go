package pruning

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// DefaultStateChangeIndexPruneIntervalBlocks keeps the full ordered posting
// sweep infrequent at the live tip while bounding stale derived-index growth.
const DefaultStateChangeIndexPruneIntervalBlocks uint64 = 262_144

// StateChangeIndexPruner reclaims immutable packed posting frames after their
// authoritative hot changesets have moved into cold history.
type StateChangeIndexPruner struct {
	DB               ethdb.KeyValueStore
	SnapshotDir      string
	MinAdvanceBlocks uint64
	HeavyWorkGate    *maintenance.HeavyWorkGate
}

func (p StateChangeIndexPruner) OnePass(ctx context.Context) (*rawdb.StateChangePostingPruneResult, error) {
	if p.DB == nil {
		return nil, errors.New("pruning: nil state-change index database")
	}
	if p.SnapshotDir == "" {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manifest, err := snapshots.LoadProductionManifest(p.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum == 0 {
		return nil, nil
	}
	hotPruneBlock := manifest.Progress.HotPruneBlockNum
	previous := manifest.Progress.StateChangeIndexPruneBlockNum
	// Do not race the ordered index builder. Besides making the prune watermark
	// fast path valid, this ensures no rebuild can still be publishing directory
	// and posting rows for the prefix being swept.
	indexed, ok, err := rawdb.ReadStageProgressRow(p.DB, rawdb.StageStateHistoryIndex)
	if err != nil {
		return nil, err
	}
	if !ok || !indexed.HasBlockHash || indexed.BlockNum < hotPruneBlock {
		return nil, nil
	}
	minAdvance := p.MinAdvanceBlocks
	if minAdvance == 0 {
		minAdvance = DefaultStateChangeIndexPruneIntervalBlocks
	}
	if previous >= hotPruneBlock || (previous > 0 && hotPruneBlock-previous < minAdvance) {
		return nil, nil
	}
	release := func() {}
	if p.HeavyWorkGate != nil {
		var admitted bool
		release, admitted = p.HeavyWorkGate.TryAcquire()
		if !admitted {
			return nil, nil
		}
	}
	defer release()
	started := time.Now()
	result, err := rawdb.PruneStaleStateChangePostingIndexThroughContext(ctx, p.DB, hotPruneBlock)
	if err != nil {
		return nil, err
	}
	if err := snapshots.UpdateStateChangeIndexPruneProgress(p.SnapshotDir, hotPruneBlock); err != nil {
		return nil, err
	}
	log.Info("Stale state-change posting sweep completed",
		"prunedThrough", hotPruneBlock,
		"postingRows", result.PostingRowsScanned,
		"postingDeleted", result.PostingRowsDeleted,
		"directoryRows", result.DirectoryRowsScanned,
		"directoryDeleted", result.DirectoryRowsDeleted,
		"elapsed", time.Since(started))
	return &result, nil
}
