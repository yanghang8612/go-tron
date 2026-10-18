package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// This transport record authenticates execution metadata excluded from MuHash.
// An explicit absent record is required even when the cycle has no pending rewards.
var executionCheckpointMagic = [8]byte{'G', 'T', 'E', 'X', 'E', 'C', 0, 1}

const executionCheckpointHeader = 8 + 32 + 8 + 24 + 1 + 16 + 1 + 8

type executionCheckpoint struct {
	BlockHash  common.Hash
	TxNum      uint64
	Present    bool
	Pending    []byte
	Properties rawdb.StateExecutionProperties
	Range      *rawdb.StateTxRange
}

func encodeExecutionCheckpoint(reader ethdb.KeyValueReader, head rawdb.StateCommitmentHead) ([]byte, error) {
	properties, err := rawdb.ReadStateExecutionProperties(reader)
	if err != nil {
		return nil, err
	}
	if uint64(properties.BlockNum) != head.BlockNum || properties.BlockHash != head.BlockHash {
		return nil, errors.New("snapshots: execution properties / HEAD identity mismatch")
	}
	_, _, present, err := rawdb.ReadCycleRewardPending(reader)
	if err != nil {
		return nil, err
	}
	var pending []byte
	if present {
		pending, err = reader.Get(rawdb.CycleRewardPendingKey())
		if err != nil {
			return nil, err
		}
	}
	row, rangePresent, err := rawdb.ReadStateTxRange(reader, head.BlockNum)
	if err != nil {
		return nil, err
	}
	if head.TxNum > 0 && (!rangePresent || row.BlockNum != head.BlockNum || row.BeginTxNum > row.EndTxNum || row.BlockHash != head.BlockHash || row.EndTxNum != head.TxNum) {
		return nil, errors.New("snapshots: execution allocator range / HEAD cut mismatch")
	}
	if head.TxNum == 0 && rangePresent {
		return nil, errors.New("snapshots: zero history cut has allocator range")
	}
	out := make([]byte, executionCheckpointHeader+len(pending))
	copy(out, executionCheckpointMagic[:])
	copy(out[8:40], head.BlockHash[:])
	binary.BigEndian.PutUint64(out[40:48], head.TxNum)
	if present {
		out[89] = 1
	}
	binary.BigEndian.PutUint64(out[90:98], uint64(len(pending)))
	binary.BigEndian.PutUint64(out[48:56], uint64(properties.BlockNum))
	binary.BigEndian.PutUint64(out[56:64], uint64(properties.Timestamp))
	binary.BigEndian.PutUint64(out[64:72], uint64(properties.SolidifiedBlockNum))
	if rangePresent {
		out[72] = 1
		binary.BigEndian.PutUint64(out[73:81], row.BeginTxNum)
		binary.BigEndian.PutUint64(out[81:89], row.EndTxNum)
	}
	copy(out[98:], pending)
	return out, nil
}
func decodeExecutionCheckpoint(value []byte) (executionCheckpoint, error) {
	var out executionCheckpoint
	if len(value) < executionCheckpointHeader || !bytes.Equal(value[:8], executionCheckpointMagic[:]) || value[72] > 1 || value[89] > 1 || binary.BigEndian.Uint64(value[90:98]) != uint64(len(value)-executionCheckpointHeader) {
		return out, errors.New("snapshots: invalid execution checkpoint schema")
	}
	copy(out.BlockHash[:], value[8:40])
	out.TxNum = binary.BigEndian.Uint64(value[40:48])
	out.Properties = rawdb.StateExecutionProperties{BlockNum: int64(binary.BigEndian.Uint64(value[48:56])), Timestamp: int64(binary.BigEndian.Uint64(value[56:64])), SolidifiedBlockNum: int64(binary.BigEndian.Uint64(value[64:72])), BlockHash: out.BlockHash}
	if out.Properties.BlockNum < 0 || out.Properties.Timestamp < 0 || out.Properties.SolidifiedBlockNum < 0 || out.Properties.SolidifiedBlockNum > out.Properties.BlockNum {
		return out, errors.New("snapshots: invalid execution checkpoint properties")
	}
	if value[72] == 1 {
		out.Range = &rawdb.StateTxRange{BlockNum: uint64(out.Properties.BlockNum), BlockHash: out.BlockHash, BeginTxNum: binary.BigEndian.Uint64(value[73:81]), EndTxNum: binary.BigEndian.Uint64(value[81:89])}
		if out.Range.BeginTxNum > out.Range.EndTxNum || out.Range.EndTxNum != out.TxNum {
			return out, errors.New("snapshots: execution allocator range / HEAD cut mismatch")
		}
	} else if out.TxNum != 0 || binary.BigEndian.Uint64(value[73:81]) != 0 || binary.BigEndian.Uint64(value[81:89]) != 0 {
		return out, errors.New("snapshots: missing execution allocator range")
	}
	out.Present = value[89] == 1
	out.Pending = bytes.Clone(value[98:])
	if !out.Present && len(out.Pending) != 0 {
		return out, errors.New("snapshots: absent execution checkpoint has payload")
	}
	if out.Present {
		if _, _, _, err := rawdb.ReadCycleRewardPending(pendingValueReader{out.Pending}); err != nil {
			return out, err
		}
	}
	return out, nil
}

type pendingValueReader struct{ value []byte }

func (p pendingValueReader) Has([]byte) (bool, error)   { return true, nil }
func (p pendingValueReader) Get([]byte) ([]byte, error) { return p.value, nil }

func buildExecutionCheckpointSegments(ctx context.Context, db AggregatorDB, dir string, _ kvdomains.KVDomain, from, to uint64, path string) ([]SegmentRef, error) {
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return nil, err
	}
	if !ok || head.TxNum != to {
		return nil, errors.New("snapshots: execution checkpoint requires same-cut HEAD")
	}
	value, err := encodeExecutionCheckpoint(db, head)
	if err != nil {
		return nil, err
	}
	ref, _, btree, err := writeLatestBinarySegmentWithCompanionsContext(ctx, dir, SegmentRef{Dataset: SegmentDatasetExecutionCheckpoint, Kind: SegmentLatest, FromTxNum: from, ToTxNum: to, Path: path}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.CycleRewardPendingKey(), Value: value})
	}, false)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{ref, btree}, nil
}
func (s rawDBLatestHotStore) WriteExecutionCheckpoint(value []byte) error {
	record, err := decodeExecutionCheckpoint(value)
	if err != nil {
		return err
	}
	if s.writer == nil {
		return errors.New("snapshots: nil execution checkpoint writer")
	}
	if record.Range != nil {
		if deferred, ok := s.writer.(interface {
			WriteSnapshotTxRange(rawdb.StateTxRange) error
		}); ok {
			if err := deferred.WriteSnapshotTxRange(*record.Range); err != nil {
				return err
			}
		} else if err := rawdb.WriteStateTxRange(s.writer, record.Range.BlockNum, record.Range.BlockHash, record.Range.BeginTxNum, record.Range.EndTxNum); err != nil {
			return err
		}
	}
	if err := rawdb.WriteStateExecutionProperties(s.writer, record.Properties); err != nil {
		return err
	}
	if record.Present {
		return s.writer.Put(rawdb.CycleRewardPendingKey(), record.Pending)
	}
	return rawdb.DeleteCycleRewardPending(s.writer)
}
func verifyRestoredExecutionCheckpoint(db ethdb.KeyValueReader, mgr *Manager, head rawdb.StateCommitmentHead) error {
	value, ok, err := mgr.getLatestValue(SegmentDatasetExecutionCheckpoint, 0, rawdb.CycleRewardPendingKey(), head.TxNum)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("snapshots: missing execution checkpoint at latest source cut")
	}
	record, err := decodeExecutionCheckpoint(value)
	if err != nil {
		return err
	}
	if record.BlockHash != head.BlockHash || record.TxNum != head.TxNum || uint64(record.Properties.BlockNum) != head.BlockNum {
		return errors.New("snapshots: execution checkpoint / HEAD identity mismatch")
	}
	actual, err := encodeExecutionCheckpoint(db, head)
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, value) {
		return errors.New("snapshots: restored execution checkpoint differs from pinned cut")
	}
	return nil
}
