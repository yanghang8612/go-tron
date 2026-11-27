package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/log"
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

func runtimePostingPruneLimits() rawdb.StateChangePostingPruneLimits {
	return rawdb.StateChangePostingPruneLimits{
		MaxScannedRows:  16384,
		MaxScannedBytes: 4 << 20,
		MaxDeleteBytes:  1 << 20,
		MaxDuration:     10 * time.Millisecond,
	}
}

type postingPruneChain interface {
	PruneStateChangePostingChunkWithAdmission(context.Context, uint64, uint64, common.Hash, common.Hash, []byte, rawdb.StateChangePostingPruneLimits, core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error)
}

type directoryPruneChain interface {
	PruneStateChangeDirectoryChunkWithAdmission(context.Context, uint64, uint64, common.Hash, common.Hash, []byte, rawdb.StateChangePostingPruneLimits, core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error)
}

func runtimePostingPruneChunk(bc postingPruneChain, gate *maintenance.HeavyWorkGate, lockedLoad func() maintenance.StoragePressure) pruning.PostingPruneChunkFunc {
	return runtimePostingPruneModeChunk(bc.PruneStateChangePostingChunkWithAdmission, gate, lockedLoad)
}

func runtimeDirectoryPruneChunk(bc directoryPruneChain, gate *maintenance.HeavyWorkGate, lockedLoad func() maintenance.StoragePressure) pruning.PostingPruneChunkFunc {
	return runtimePostingPruneModeChunk(bc.PruneStateChangeDirectoryChunkWithAdmission, gate, lockedLoad)
}

type postingModePruneCall func(context.Context, uint64, uint64, common.Hash, common.Hash, []byte, rawdb.StateChangePostingPruneLimits, core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error)

func runtimePostingPruneModeChunk(call postingModePruneCall, gate *maintenance.HeavyWorkGate, lockedLoad func() maintenance.StoragePressure) pruning.PostingPruneChunkFunc {
	return func(ctx context.Context, boundary pruning.PostingPruneBoundary, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (pruning.PostingPruneChunkOutcome, error) {
		out := pruning.PostingPruneChunkOutcome{}
		admit := func() (func(), bool) {
			if lockedLoad == nil {
				out.PressureDeferred = true
				return nil, false
			}
			ready, relief := pruning.PostingPrunePressureAdmission(lockedLoad(), time.Now())
			if !ready {
				out.PressureDeferred = true
				return nil, false
			}
			release, ok := gate.TryAcquire()
			if !ok {
				out.GateDeferred = true
			} else {
				out.DebtRelief = relief
			}
			return release, ok
		}
		result, nextAnchor, timing, err := call(ctx, boundary.PrunedThrough, boundary.ProofHead, boundary.ProofHash, anchor, cursor, limits, admit)
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
		if errors.Is(err, core.ErrStateChangeDirectoryPruneUnsupported) {
			out.DirectoryUnsupported = true
			return out, nil
		}
		return out, err
	}
}

func loadPostingPruneCursorHint(path string) []byte {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Debug("Ignoring unreadable posting prune cursor hint", "path", path, "err", err)
		}
		return nil
	}
	if !rawdb.IsValidStateChangePostingCursor(data) {
		log.Debug("Ignoring malformed posting prune cursor hint", "path", path, "len", len(data))
		return nil
	}
	return data
}

func savePostingPruneCursorHint(path string, cursor []byte) {
	if path == "" {
		return
	}
	if len(cursor) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Debug("Failed to remove posting prune cursor hint", "path", path, "err", err)
		}
		return
	}
	if !rawdb.IsValidStateChangePostingCursor(cursor) {
		return
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		log.Debug("Failed to stage posting prune cursor hint", "path", path, "err", err)
		return
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(cursor); err != nil {
		_ = tmp.Close()
		log.Debug("Failed to write posting prune cursor hint", "path", path, "err", err)
		return
	}
	if err := tmp.Close(); err != nil {
		log.Debug("Failed to close posting prune cursor hint", "path", path, "err", err)
		return
	}
	if err := os.Rename(tmpName, path); err != nil {
		log.Debug("Failed to replace posting prune cursor hint", "path", path, "err", err)
		return
	}
	cleanup = false
}
