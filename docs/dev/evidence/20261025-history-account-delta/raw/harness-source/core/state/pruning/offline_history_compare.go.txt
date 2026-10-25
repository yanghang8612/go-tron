package pruning

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// compareOfflineHistoryToHot proves that every remaining hot previous image
// occurs unchanged in the already authenticated cold range. A cold stream may
// contain additional rows after an interrupted deletion; requiring identical
// counts would prevent safe retries of partially completed block deletion.
// V6 reconstructs Seq from transaction ordinal and segment record ordinal;
// it does not retain the hot sequence number. Compare the ordered rows within
// each transaction instead, without skipping a different row in the same tx.
// Only one cold record is borrowed at a time. Every exit cancels and joins the
// producer, so its readers never outlive the caller's exclusive file ownership.
func compareOfflineHistoryToHot(ctx context.Context, db ethdb.Iteratee, dir string, manifest *snapshots.Manifest, plan OfflineHistoryPlan) (uint64, error) {
	if err := snapshots.VerifyOfflineHistoryBlockRangesContext(ctx, dir, manifest.Segments, plan.rows); err != nil {
		return 0, err
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rows := make(chan *rawdb.StateDomainChange)
	consumed := make(chan struct{})
	done := make(chan error, 1)
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok || cfg.IterateHistoryRange == nil || cfg.IterateHotHistoryBlockTxBorrowed == nil {
		return 0, errors.New("pruning: offline comparison requires streaming hot and cold history readers")
	}
	go func() {
		var runErr error
		defer func() { close(rows); done <- runErr }()
		for _, ref := range offlineHistoryRefs(manifest) {
			if ref.ToTxNum < plan.FromTxNum || ref.FromTxNum > plan.ToTxNum {
				continue
			}
			runErr = cfg.IterateHistoryRange(dir, manifest, ref, plan.FromTxNum, plan.ToTxNum, func(row *rawdb.StateDomainChange) (bool, error) {
				select {
				case <-workCtx.Done():
					return false, workCtx.Err()
				case rows <- row:
				}
				select {
				case <-workCtx.Done():
					return false, workCtx.Err()
				case <-consumed:
					return true, nil
				}
			})
			if runErr != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	var current *rawdb.StateDomainChange
	advance := func() error {
		if current != nil {
			select {
			case consumed <- struct{}{}:
			case <-workCtx.Done():
				return workCtx.Err()
			}
			current = nil
		}
		select {
		case <-workCtx.Done():
			return workCtx.Err()
		case row, ok := <-rows:
			if !ok {
				return errors.New("pruning: cold history ended before a remaining hot previous image")
			}
			current = row
			return nil
		}
	}
	var compared uint64
	matched := false
	err := cfg.IterateHotHistoryBlockTxBorrowed(db, plan.FromBlock, plan.ToBlock, plan.FromTxNum, plan.ToTxNum, func(hot *rawdb.StateDomainChange) (bool, error) {
		if err := workCtx.Err(); err != nil {
			return false, err
		}
		if hot == nil {
			return false, errors.New("pruning: nil hot previous image")
		}
		if matched {
			if err := advance(); err != nil {
				return false, err
			}
			matched = false
		}
		for current == nil || current.TxNum < hot.TxNum {
			if err := advance(); err != nil {
				return false, err
			}
		}
		if current.BlockNum != hot.BlockNum || current.BlockHash != hot.BlockHash || current.TxNum != hot.TxNum ||
			current.FlatDomain != hot.FlatDomain || current.Owner != hot.Owner || current.Generation != hot.Generation || current.Domain != hot.Domain ||
			!bytes.Equal(current.Key, hot.Key) || current.PrevExists != hot.PrevExists || !bytes.Equal(current.Prev, hot.Prev) {
			return false, fmt.Errorf("pruning: cold/hot previous image mismatch at block %d tx %d seq %d", hot.BlockNum, hot.TxNum, hot.Seq)
		}
		compared++
		matched = true
		return true, nil
	})
	return compared, err
}
