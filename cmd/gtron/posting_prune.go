package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/pruning"
)

func postingPruneEnabled(value string) (bool, error) {
	switch value {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("GTRON_POSTING_PRUNE must be 0 or 1")
	}
}

func runtimePostingPruneChunk(bc *core.BlockChain) pruning.PostingPruneChunkFunc {
	return func(ctx context.Context, boundary pruning.PostingPruneBoundary, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (pruning.PostingPruneChunkOutcome, error) {
		result, nextAnchor, err := bc.PruneStateChangePostingChunk(ctx, boundary.PrunedThrough, boundary.ProofHead, boundary.ProofHash, anchor, cursor, limits)
		out := pruning.PostingPruneChunkOutcome{Result: result, Anchor: nextAnchor}
		if errors.Is(err, core.ErrStateChangePostingPruneDeferred) {
			out.Deferred = true
			return out, nil
		}
		if errors.Is(err, core.ErrStateChangePostingPruneBoundaryChanged) {
			out.BoundaryChanged = true
			return out, nil
		}
		return out, err
	}
}
