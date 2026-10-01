package core

import (
	"context"
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type stagedHistorySourceView struct {
	snapshots.AggregatorDB
	manifest *snapshots.Manifest
}

func (v stagedHistorySourceView) IsPinnedKeyValueView() bool              { return true }
func (v stagedHistorySourceView) PinnedColdManifest() *snapshots.Manifest { return v.manifest }
func (v stagedHistorySourceView) RequireHotRange(first, last uint64) error {
	routed, ok := v.AggregatorDB.(interface{ RequireHotRange(uint64, uint64) error })
	if !ok {
		return errors.New("history staging: routed view cannot prove hot range")
	}
	return routed.RequireHotRange(first, last)
}

// SetHistoryStagingManager installs the already opened and reconciled staging
// store before archive, snapshot, pruning, or sync lifecycles start. Canonical
// block execution continues to write only to chaindata.
func (bc *BlockChain) SetHistoryStagingManager(manager *rawdb.HistoryStagingManager) error {
	if bc == nil || manager == nil {
		return errors.New("history staging: nil blockchain or manager")
	}
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}
	if !bc.historyStaging.CompareAndSwap(nil, manager) {
		return errors.New("history staging: manager already installed")
	}
	return nil
}

// HistoryStagingManager returns the store installed for this process. A nil
// result means the node is using its original single-store history layout.
func (bc *BlockChain) HistoryStagingManager() *rawdb.HistoryStagingManager {
	if bc == nil {
		return nil
	}
	return bc.historyStaging.Load()
}

// AcquireStateHistorySourceView pins the history-source generation used by a
// cold build. The short chain lock serializes the hot snapshot with the route
// lease; the returned view stays fixed while the costly build runs unlocked.
func (bc *BlockChain) AcquireStateHistorySourceView(ctx context.Context) (snapshots.AggregatorDB, func() error, error) {
	if bc == nil || bc.db == nil {
		return nil, nil, errors.New("history staging: unavailable blockchain database")
	}
	var releasePublication func()
	if bc.historyStaging.Load() != nil {
		coldManager, ok := bc.stateCodeColdHistory.(*snapshots.Manager)
		if !ok || coldManager == nil {
			return nil, nil, errors.New("history staging: cold manager is not pinnable")
		}
		var err error
		releasePublication, err = snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
		if err != nil {
			return nil, nil, err
		}
		defer releasePublication()
	}
	if err := lockMutexContext(ctx, &bc.chainmu); err != nil {
		return nil, nil, err
	}
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return nil, nil, ErrBlockChainClosed
	}
	if manager := bc.historyStaging.Load(); manager != nil {
		coldManager, ok := bc.stateCodeColdHistory.(*snapshots.Manager)
		if !ok || coldManager == nil {
			return nil, nil, errors.New("history staging: cold manager is not pinnable")
		}
		var pinnedCold *snapshots.Manager
		view, err := manager.AcquireView(func() (rawdb.StateHistoryReadView, func() error, error) {
			return rawdb.AcquireStateHistoryReadView(bc.db)
		}, func() (func() error, error) {
			pinned, release, err := coldManager.PinHistoryReadView()
			if err != nil {
				return nil, err
			}
			pinnedCold = pinned
			return func() error { release(); return nil }, nil
		})
		if err != nil {
			return nil, nil, err
		}
		var manifest *snapshots.Manifest
		if pinnedCold != nil {
			manifest = pinnedCold.Manifest()
		}
		return stagedHistorySourceView{AggregatorDB: view, manifest: manifest}, view.Close, nil
	}
	return rawdb.AcquireStateHistoryReadView(bc.db)
}
