package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// ErrStateDomainChangeBorrowedLegacyRows reports that a borrowed block-pack
// scan encountered a retired standalone history row. Callers that must support
// databases written by the transition schema can fall back to the owning
// iterator without hiding corruption or callback errors.
var ErrStateDomainChangeBorrowedLegacyRows = errors.New("rawdb: borrowed state domain change scan encountered legacy rows")

// IterateStateTxRangesByBlockRangeBorrowed walks the current fixed-field
// tx-range rows without reflection decoding. The callback pointer is reused and
// is valid only until fn returns.
func IterateStateTxRangesByBlockRangeBorrowed(db ethdb.Iteratee, fromBlock, toBlock uint64, fn func(*StateTxRange) (bool, error)) error {
	if db == nil {
		return errors.New("rawdb: nil state tx range database")
	}
	if fn == nil {
		return errors.New("rawdb: nil borrowed state tx range callback")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("rawdb: inverted state tx block range [%d,%d]", fromBlock, toBlock)
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	it := db.NewIterator(stateTxRangePrefix, start[:])
	defer it.Release()
	var scratch StateTxRange
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, stateTxRangePrefix) || len(key) != len(stateTxRangePrefix)+8 {
			continue
		}
		blockNum := binary.BigEndian.Uint64(key[len(stateTxRangePrefix):])
		if blockNum < fromBlock {
			continue
		}
		if blockNum > toBlock {
			return nil
		}
		hash, begin, end, err := decodeBorrowedStateTxRange(it.Value(), blockNum)
		if err != nil {
			return err
		}
		scratch = StateTxRange{BlockNum: blockNum, BlockHash: hash, BeginTxNum: begin, EndTxNum: end}
		cont, err := fn(&scratch)
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
}

// IterateStateDomainChangesByBlockTxRangeBorrowed is the allocation-bounded
// cold-build iterator for the current block-pack schema. Key and Prev in each
// callback argument alias the Pebble value or a pooled decompression buffer and
// are valid only until fn returns. Callers that retain a change must copy it.
//
// Fresh canonical execution writes exactly one sequence-zero block pack per
// block. Standalone rows belong to the retired transition schema and return
// ErrStateDomainChangeBorrowedLegacyRows; owning history readers retain their
// compatibility and repair-overwrite behavior for callers that fall back.
func IterateStateDomainChangesByBlockTxRangeBorrowed(db ethdb.Iteratee, fromBlock, toBlock, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) error {
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
		cont, err := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(value, blockNum, &scratch, visit)
		if err != nil {
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

func decodeBorrowedStateTxRange(data []byte, physicalBlock uint64) (common.Hash, uint64, uint64, error) {
	row, trailing, err := rlp.SplitList(data)
	if err != nil {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: decode borrowed state tx range for block %d: %w", physicalBlock, err)
	}
	if len(trailing) != 0 {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: borrowed state tx range for block %d has %d trailing bytes", physicalBlock, len(trailing))
	}
	blockNum, row, err := splitBorrowedStateDomainChangeUint(row, "tx range block", ^uint64(0))
	if err != nil {
		return common.Hash{}, 0, 0, err
	}
	if blockNum != physicalBlock {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: state tx range key block %d contains block %d", physicalBlock, blockNum)
	}
	hashBytes, row, err := splitBorrowedStateDomainChangeString(row, "tx range block hash")
	if err != nil {
		return common.Hash{}, 0, 0, err
	}
	if len(hashBytes) != common.HashLength {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: state tx range block %d hash has %d bytes, want %d", physicalBlock, len(hashBytes), common.HashLength)
	}
	var hash common.Hash
	copy(hash[:], hashBytes)
	begin, row, err := splitBorrowedStateDomainChangeUint(row, "tx range begin", ^uint64(0))
	if err != nil {
		return common.Hash{}, 0, 0, err
	}
	end, row, err := splitBorrowedStateDomainChangeUint(row, "tx range end", ^uint64(0))
	if err != nil {
		return common.Hash{}, 0, 0, err
	}
	if len(row) != 0 {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: state tx range block %d has %d trailing fields", physicalBlock, len(row))
	}
	if end < begin {
		return common.Hash{}, 0, 0, fmt.Errorf("rawdb: invalid state tx range for block %d: [%d,%d]", physicalBlock, begin, end)
	}
	return hash, begin, end, nil
}

// iteratePersistedStateDomainChangeBlockBorrowed decodes the current RLP block
// pack without allocating row structs or copying variable-width byte fields.
// The returned callback view is invalid as soon as the callback returns.
func iteratePersistedStateDomainChangeBlockBorrowed(data []byte, blockNum uint64, fn func(*StateDomainChange) (bool, error)) (bool, error) {
	var scratch StateDomainChange
	return iteratePersistedStateDomainChangeBlockBorrowedWithScratch(data, blockNum, &scratch, fn)
}

func iteratePersistedStateDomainChangeBlockBorrowedWithScratch(data []byte, blockNum uint64, scratch *StateDomainChange, fn func(*StateDomainChange) (bool, error)) (bool, error) {
	if scratch == nil {
		return false, errors.New("rawdb: nil borrowed state domain change scratch")
	}
	if fn == nil {
		return false, errors.New("rawdb: nil borrowed state domain change block callback")
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

func decodeBorrowedStateDomainChangeRow(row []byte, blockNum, seq uint64, change *StateDomainChange) error {
	*change = StateDomainChange{BlockNum: blockNum, Seq: seq}

	var err error
	if change.TxNum, row, err = splitBorrowedStateDomainChangeUint(row, "txNum", ^uint64(0)); err != nil {
		return err
	}
	flatDomain, rest, err := splitBorrowedStateDomainChangeUint(row, "flat domain", uint64(^uint8(0)))
	if err != nil {
		return err
	}
	change.FlatDomain = StateFlatDomain(flatDomain)
	row = rest
	owner, rest, err := rlp.SplitString(row)
	if err != nil {
		return fmt.Errorf("owner: %w", err)
	}
	if len(owner) != common.AddressLength {
		return fmt.Errorf("owner has %d bytes, want %d", len(owner), common.AddressLength)
	}
	copy(change.Owner[:], owner)
	row = rest
	if change.Generation, row, err = splitBorrowedStateDomainChangeUint(row, "generation", ^uint64(0)); err != nil {
		return err
	}
	domain, rest, err := splitBorrowedStateDomainChangeUint(row, "domain", uint64(^uint16(0)))
	if err != nil {
		return err
	}
	change.Domain = kvdomains.KVDomain(domain)
	row = rest
	if change.Key, row, err = splitBorrowedStateDomainChangeString(row, "key"); err != nil {
		return err
	}
	prevExists, rest, err := splitBorrowedStateDomainChangeUint(row, "previous presence", 1)
	if err != nil {
		return err
	}
	change.PrevExists = prevExists == 1
	row = rest
	if change.Prev, row, err = splitBorrowedStateDomainChangeString(row, "previous value"); err != nil {
		return err
	}
	if len(row) != 0 {
		return fmt.Errorf("row has %d trailing bytes", len(row))
	}
	return nil
}

func splitBorrowedStateDomainChangeUint(data []byte, field string, max uint64) (uint64, []byte, error) {
	value, rest, err := rlp.SplitUint64(data)
	if err != nil {
		return 0, data, fmt.Errorf("%s: %w", field, err)
	}
	if value > max {
		return 0, data, fmt.Errorf("%s value %d overflows", field, value)
	}
	return value, rest, nil
}

func splitBorrowedStateDomainChangeString(data []byte, field string) ([]byte, []byte, error) {
	value, rest, err := rlp.SplitString(data)
	if err != nil {
		return nil, data, fmt.Errorf("%s: %w", field, err)
	}
	return value, rest, nil
}

func borrowStateDomainChangeBlockPayload(data []byte) ([]byte, *[]byte, error) {
	payload, decodedLen, compressed, err := stateDomainChangeBlockCompressionPayload(data)
	if err != nil || !compressed {
		return payload, nil, err
	}
	pooled := stateChangeBlockDecodeBufferPool.Get().(*[]byte)
	if cap(*pooled) < decodedLen {
		*pooled = make([]byte, decodedLen)
	} else {
		*pooled = (*pooled)[:decodedLen]
	}
	decoded, err := snappy.Decode(*pooled, payload)
	if err != nil {
		releaseBorrowedStateDomainChangeBlockPayload(pooled)
		return nil, nil, fmt.Errorf("rawdb: decode compressed state domain change block: %w", err)
	}
	return decoded, pooled, nil
}

func releaseBorrowedStateDomainChangeBlockPayload(pooled *[]byte) {
	if pooled == nil || cap(*pooled) > stateDomainChangeBlockPooledBufferMax {
		return
	}
	*pooled = (*pooled)[:0]
	stateChangeBlockDecodeBufferPool.Put(pooled)
}
