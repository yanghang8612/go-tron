package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/maintenance"
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

type postingPruneChain interface {
	PruneStateChangePostingChunkWithAdmission(context.Context, uint64, uint64, common.Hash, common.Hash, []byte, rawdb.StateChangePostingPruneLimits, core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error)
}

func runtimePostingPruneChunk(bc postingPruneChain, gate *maintenance.HeavyWorkGate, lockedLoad func() maintenance.StoragePressure) pruning.PostingPruneChunkFunc {
	return func(ctx context.Context, boundary pruning.PostingPruneBoundary, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (pruning.PostingPruneChunkOutcome, error) {
		out := pruning.PostingPruneChunkOutcome{}
		admit := func() (func(), bool) {
			if lockedLoad == nil || !pruning.PostingPrunePressureReady(lockedLoad(), time.Now()) {
				out.PressureDeferred = true
				return nil, false
			}
			release, ok := gate.TryAcquire()
			if !ok {
				out.GateDeferred = true
			}
			return release, ok
		}
		result, nextAnchor, timing, err := bc.PruneStateChangePostingChunkWithAdmission(ctx, boundary.PrunedThrough, boundary.ProofHead, boundary.ProofHash, anchor, cursor, limits, admit)
		out.Result, out.Anchor = result, nextAnchor
		out.Timings = pruning.PostingPruneTimings{ChainWait: timing.ChainWait, ChainHeld: timing.ChainHeld,
			Admission: timing.Admission, Proof: timing.Proof, GateHeld: timing.GateHeld}
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
