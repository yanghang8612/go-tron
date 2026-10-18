package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// AcquireStateLatestSnapshot drains canonical writers and pins the durable base
// plus immutable committed layers at one coherent source/HEAD cut. Import
// may resume after return; the caller owns Close on the returned view.
func (bc *BlockChain) AcquireStateLatestSnapshot(ctx context.Context) (pointread.KeyValueSnapshot, rawdb.StateCommitmentHead, error) {
	var empty rawdb.StateCommitmentHead
	if err := ctx.Err(); err != nil {
		return nil, empty, err
	}
	if bc == nil || bc.db == nil {
		return nil, empty, errors.New("nil blockchain/database")
	}
	bc.insertSessionGate.Lock()
	defer bc.insertSessionGate.Unlock()
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if err := ctx.Err(); err != nil {
		return nil, empty, err
	}
	if p := bc.commitErr.Load(); p != nil {
		return nil, empty, *p
	}
	if p := bc.flushErr.Load(); p != nil {
		return nil, empty, *p
	}
	current := bc.CurrentBlock()
	if current == nil {
		return nil, empty, errors.New("canonical head missing")
	}
	view, err := bc.buffer.NewReadSnapshotThrough(current.Number())
	if err != nil {
		return nil, empty, err
	}
	head, present, err := rawdb.ReadStateCommitmentHead(view)
	if err != nil || !present {
		view.Close()
		if err == nil {
			err = rawdb.ErrStateCommitmentHead
		}
		return nil, empty, err
	}
	if current == nil || head.BlockNum != current.Number() || head.BlockHash != current.Hash() {
		view.Close()
		return nil, empty, fmt.Errorf("pinned commitment HEAD does not match canonical head")
	}
	tr, hasRange, err := rawdb.ReadStateTxRange(view, head.BlockNum)
	if err != nil {
		view.Close()
		return nil, empty, err
	}
	if hasRange && (tr.BlockHash != head.BlockHash || tr.EndTxNum != head.TxNum) {
		view.Close()
		return nil, empty, errors.New("pinned commitment HEAD transaction cut mismatch")
	}
	if !hasRange && head.TxNum != 0 {
		view.Close()
		return nil, empty, errors.New("pinned commitment HEAD transaction cut missing")
	}
	root, present, err := rawdb.ReadLatestDomainCommitmentRoot(view)
	if err != nil || !present || root != head.Root {
		view.Close()
		if err == nil {
			err = errors.New("pinned commitment HEAD/root mismatch")
		}
		return nil, empty, err
	}
	return view, head, nil
}
