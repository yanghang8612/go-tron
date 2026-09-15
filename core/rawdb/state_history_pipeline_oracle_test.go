package rawdb

// Frozen verbatim from 3fd7eeddfd9889ab33e167a16d04f588d6175962,
// core/rawdb/accessors_state_changeset_borrowed.go. Only these two function
// names/calls were mechanically renamed. The ordinary materializer and scalar
// decoders are unchanged in this candidate; no pipeline helper is used here.
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
)

func frozen3fdIterateHistoryRange(db ethdb.Iteratee, fromBlock, toBlock, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	historyView, releaseHistoryView, viewErr := AcquireStateHistoryReadView(db)
	if viewErr != nil {
		return viewErr
	}
	defer func() { _ = releaseHistoryView() }()
	db = historyView

	if db == nil {
		return errors.New("rawdb: nil state domain change database")
	}
	if fn == nil {
		return errors.New("rawdb: nil borrowed state domain change callback")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("rawdb: inverted state domain change block range [%d,%d]", fromBlock, toBlock)
	}
	if toTxNum < fromTxNum {
		return fmt.Errorf("rawdb: inverted state domain change tx range [%d,%d]", fromTxNum, toTxNum)
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	rangeIt := db.NewIterator(stateTxRangePrefix, start[:])
	defer rangeIt.Release()
	changeIt := db.NewIterator(stateChangeSetPrefix, start[:])
	defer changeIt.Release()
	var (
		haveRange     bool
		rangesDone    bool
		rangeBlock    uint64
		rangeHash     common.Hash
		rangeBegin    uint64
		rangeEnd      uint64
		havePrevious  bool
		previousTxNum uint64
		previousSeq   uint64
		previousBlock uint64
	)
	var scratch StateDomainChange
	visit := func(change *StateDomainChange) (bool, error) {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return true, nil
		}
		if havePrevious && (change.TxNum < previousTxNum ||
			(change.TxNum == previousTxNum && (change.Seq < previousSeq ||
				(change.Seq == previousSeq && change.BlockNum <= previousBlock)))) {
			return false, fmt.Errorf("rawdb: borrowed state domain changes are not ordered at block %d sequence %d txNum %d", change.BlockNum, change.Seq, change.TxNum)
		}
		change.BlockHash = rangeHash
		cont, err := fn(change)
		if err == nil && cont {
			havePrevious = true
			previousTxNum = change.TxNum
			previousSeq = change.Seq
			previousBlock = change.BlockNum
		}
		return cont, err
	}
	for changeIt.Next() {
		key := changeIt.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		blockNum := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if blockNum < fromBlock {
			continue
		}
		if blockNum > toBlock {
			break
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq != 0 {
			return fmt.Errorf("%w at block %d sequence %d", ErrStateDomainChangeBorrowedLegacyRows, blockNum, seq)
		}
		for !haveRange || rangeBlock < blockNum {
			if !rangeIt.Next() {
				rangesDone = true
				break
			}
			rangeKey := rangeIt.Key()
			if !bytes.HasPrefix(rangeKey, stateTxRangePrefix) || len(rangeKey) != len(stateTxRangePrefix)+8 {
				continue
			}
			candidate := binary.BigEndian.Uint64(rangeKey[len(stateTxRangePrefix):])
			if candidate < fromBlock {
				continue
			}
			if candidate > toBlock {
				rangeBlock = candidate
				rangeHash = common.Hash{}
				rangeBegin = 0
				rangeEnd = 0
				haveRange = true
				break
			}
			var err error
			rangeHash, rangeBegin, rangeEnd, err = decodeBorrowedStateTxRange(rangeIt.Value(), candidate)
			if err != nil {
				return err
			}
			rangeBlock = candidate
			haveRange = true
		}
		if rangesDone {
			break
		}
		if rangeBlock != blockNum || rangeEnd < fromTxNum || rangeBegin > toTxNum {
			continue
		}
		value := changeIt.Value()
		cont, err := frozen3fdIterateHistoryBlock(value, blockNum, &scratch, visit, historyView)
		if err != nil {
			if isStateHistorySharedPack(value) {
				return err
			}
			// Sequence zero was an ordinary row before block packs reserved it.
			// Probe the owning transition decoder only on this exceptional path
			// so current-schema corruption remains distinguishable from legacy
			// input and is never silently retried.
			if _, legacyErr := decodePersistedStateDomainChange(value, blockNum, 0); legacyErr == nil {
				return fmt.Errorf("%w at block %d sequence 0", ErrStateDomainChangeBorrowedLegacyRows, blockNum)
			}
			return err
		}
		if !cont {
			return nil
		}
	}
	if err := changeIt.Error(); err != nil {
		return err
	}
	return rangeIt.Error()
}

func frozen3fdIterateHistoryBlock(data []byte, blockNum uint64, scratch *StateDomainChange, fn func(*StateDomainChange) (bool, error), readers ...ethdb.KeyValueReader) (bool, error) {
	if scratch == nil {
		return false, errors.New("rawdb: nil borrowed state domain change scratch")
	}
	if fn == nil {
		return false, errors.New("rawdb: nil borrowed state domain change block callback")
	}
	var materializeErr error
	data, materializeErr = materializeStateHistorySharedPack(data, blockNum, readers)
	if materializeErr != nil {
		return false, materializeErr
	}
	decoded, pooled, err := borrowStateDomainChangeBlockPayload(data)
	if err != nil {
		return false, err
	}
	if pooled != nil {
		defer releaseBorrowedStateDomainChangeBlockPayload(pooled)
	}

	block, trailing, err := rlp.SplitList(decoded)
	if err != nil {
		return false, fmt.Errorf("rawdb: decode borrowed state domain change block: %w", err)
	}
	if len(trailing) != 0 {
		return false, fmt.Errorf("rawdb: borrowed state domain change block has %d trailing bytes", len(trailing))
	}
	version, block, err := splitBorrowedStateDomainChangeUint(block, "version", ^uint64(0))
	if err != nil {
		return false, err
	}
	if version != uint64(persistedStateDomainChangeBlockVersion) {
		return false, fmt.Errorf("rawdb: unsupported state domain change block version %d", version)
	}
	firstSeq, block, err := splitBorrowedStateDomainChangeUint(block, "first sequence", ^uint64(0))
	if err != nil {
		return false, err
	}
	if firstSeq == 0 {
		return false, fmt.Errorf("rawdb: invalid state domain change block first sequence for block %d", blockNum)
	}
	rows, block, err := rlp.SplitList(block)
	if err != nil {
		return false, fmt.Errorf("rawdb: decode borrowed state domain change rows: %w", err)
	}
	if len(block) != 0 {
		return false, fmt.Errorf("rawdb: borrowed state domain change block header has %d trailing bytes", len(block))
	}
	if len(rows) == 0 {
		return false, fmt.Errorf("rawdb: empty state domain change block pack for block %d", blockNum)
	}

	var (
		rowIndex      uint64
		previousTxNum uint64
		havePrevious  bool
	)
	for len(rows) > 0 {
		row, remainingRows, err := rlp.SplitList(rows)
		if err != nil {
			return false, fmt.Errorf("rawdb: decode borrowed state domain change row %d: %w", rowIndex, err)
		}
		if rowIndex > ^uint64(0)-firstSeq {
			return false, fmt.Errorf("rawdb: state domain change block sequence overflows at row %d", rowIndex)
		}
		if err := decodeBorrowedStateDomainChangeRow(row, blockNum, firstSeq+rowIndex, scratch); err != nil {
			return false, fmt.Errorf("rawdb: decode borrowed state domain change row %d: %w", rowIndex, err)
		}
		if havePrevious && scratch.TxNum < previousTxNum {
			return false, fmt.Errorf("rawdb: state domain change block %d txNum %d at sequence %d follows txNum %d", blockNum, scratch.TxNum, scratch.Seq, previousTxNum)
		}
		cont, err := fn(scratch)
		if err != nil || !cont {
			return cont, err
		}
		previousTxNum = scratch.TxNum
		havePrevious = true
		rowIndex++
		rows = remainingRows
	}
	return true, nil
}
