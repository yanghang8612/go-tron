package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// HistoryStagingBlockComplete is written in the canonical block batch. Its
// row count and digest distinguish a proven legal zero-change block from a
// missing/pruned history payload. Old blocks without this receipt cannot be
// inferred complete from the presence of StateTxRange alone.
type HistoryStagingBlockComplete struct {
	Version       uint8
	Epoch         uint64
	BlockNum      uint64
	BlockHash     common.Hash
	BeginTxNum    uint64
	EndTxNum      uint64
	PhysicalRows  uint64
	PayloadDigest [32]byte
}

type HistoryStagingBlockHasher struct {
	block   uint64
	h       hash.Hash
	rows    uint64
	lastSeq uint64
	seen    bool
}

func NewHistoryStagingBlockHasher(block uint64) *HistoryStagingBlockHasher {
	h := sha256.New()
	_, _ = h.Write([]byte("go-tron-history-staging-block-v1"))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], block)
	_, _ = h.Write(encoded[:])
	return &HistoryStagingBlockHasher{block: block, h: h}
}

// Add accepts only this block's exact physical changeset rows in increasing
// sequence order. Shared chunks remain bucket-scoped and are authenticated by
// the pack decoder; including mutable bucket metadata here would make an older
// block receipt change when a later block appends a chunk.
func (h *HistoryStagingBlockHasher) Add(key, value []byte) error {
	if h == nil || len(key) != len(stateChangeSetPrefix)+16 {
		return errors.New("rawdb: invalid staged block hash row")
	}
	for i := range stateChangeSetPrefix {
		if key[i] != stateChangeSetPrefix[i] {
			return errors.New("rawdb: wrong staged block hash prefix")
		}
	}
	if binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) != h.block {
		return errors.New("rawdb: staged block hash crosses blocks")
	}
	seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
	if h.seen && seq <= h.lastSeq {
		return errors.New("rawdb: staged block hash sequence is not increasing")
	}
	if h.rows == ^uint64(0) {
		return errors.New("rawdb: staged block hash row count overflow")
	}
	historyStagingHashRow(h.h, key, value)
	h.rows++
	h.lastSeq = seq
	h.seen = true
	return nil
}

// MaybeAdd accepts every Put from a canonical block writer and hashes only
// physical changeset rows for this block. Non-history keys are ignored.
func (h *HistoryStagingBlockHasher) MaybeAdd(key, value []byte) (bool, error) {
	if h == nil {
		return false, errors.New("rawdb: nil staged block hasher")
	}
	if len(key) < len(stateChangeSetPrefix) || !bytes.HasPrefix(key, stateChangeSetPrefix) {
		return false, nil
	}
	if len(key) != len(stateChangeSetPrefix)+16 {
		return false, ErrHistoryStagingConflict
	}
	if binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) != h.block {
		return false, ErrHistoryStagingConflict
	}
	return true, h.Add(key, value)
}

func (h *HistoryStagingBlockHasher) Finish(blockHash common.Hash, epoch, beginTx, endTx uint64) (HistoryStagingBlockComplete, error) {
	if h == nil || epoch == 0 || blockHash == (common.Hash{}) || endTx < beginTx {
		return HistoryStagingBlockComplete{}, errors.New("rawdb: invalid staged block completion")
	}
	var digest [32]byte
	copy(digest[:], h.h.Sum(nil))
	return HistoryStagingBlockComplete{Version: HistoryStagingFormatVersion, Epoch: epoch, BlockNum: h.block, BlockHash: blockHash, BeginTxNum: beginTx, EndTxNum: endTx, PhysicalRows: h.rows, PayloadDigest: digest}, nil
}

// BuildHistoryStagingBlockComplete scans the current writer-visible block
// image. It is intended for a blockbuffer view that includes the pending
// canonical block; the caller must pass the same batch for Write below.
func BuildHistoryStagingBlockComplete(ctx context.Context, view StateHistoryReadView, epoch, block uint64, blockHash common.Hash) (HistoryStagingBlockComplete, error) {
	if ctx == nil || view == nil || !view.IsPinnedKeyValueView() {
		return HistoryStagingBlockComplete{}, ErrStateHistoryReadViewUnpinned
	}
	rangeRow, present, err := ReadStateTxRange(view, block)
	if err != nil || !present || rangeRow.BlockHash != blockHash {
		return HistoryStagingBlockComplete{}, fmt.Errorf("rawdb: block %d tx-range is not writer-visible: %w", block, err)
	}
	h := NewHistoryStagingBlockHasher(block)
	it := view.NewIterator(stateChangeSetBlockPrefix(block), nil)
	defer it.Release()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return HistoryStagingBlockComplete{}, err
		}
		if err := h.Add(it.Key(), it.Value()); err != nil {
			return HistoryStagingBlockComplete{}, err
		}
	}
	if err := it.Error(); err != nil {
		return HistoryStagingBlockComplete{}, err
	}
	return h.Finish(blockHash, epoch, rangeRow.BeginTxNum, rangeRow.EndTxNum)
}

func WriteHistoryStagingBlockComplete(batch ethdb.KeyValueWriter, row HistoryStagingBlockComplete) error {
	if batch == nil || row.Version != HistoryStagingFormatVersion || row.Epoch == 0 || row.BlockHash == (common.Hash{}) || row.EndTxNum < row.BeginTxNum || row.PayloadDigest == ([32]byte{}) {
		return errors.New("rawdb: invalid staged block completion row")
	}
	return writeHistoryStagingValue(batch, historyStagingBlockCompleteKey(row.Epoch, row.BlockNum), row)
}

func ReadHistoryStagingBlockComplete(reader ethdb.KeyValueReader, epoch, block uint64) (HistoryStagingBlockComplete, bool, error) {
	var row HistoryStagingBlockComplete
	if reader == nil || epoch == 0 {
		return row, false, errors.New("rawdb: invalid staged block completion read")
	}
	present, err := readHistoryStagingValue(reader, historyStagingBlockCompleteKey(epoch, block), &row)
	if err != nil || !present {
		return row, present, err
	}
	if row.Version != HistoryStagingFormatVersion || row.Epoch != epoch || row.BlockNum != block || row.BlockHash == (common.Hash{}) || row.EndTxNum < row.BeginTxNum || row.PayloadDigest == ([32]byte{}) {
		return HistoryStagingBlockComplete{}, false, ErrHistoryStagingConflict
	}
	return row, true, nil
}

func DeleteHistoryStagingBlockComplete(batch ethdb.KeyValueWriter, epoch, block uint64) error {
	if batch == nil || epoch == 0 {
		return errors.New("rawdb: invalid staged block completion delete")
	}
	return batch.Delete(historyStagingBlockCompleteKey(epoch, block))
}
