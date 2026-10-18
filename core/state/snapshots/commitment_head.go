package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// A single complete normalized state is retained at a real snapshot cut. The
// per-block CommitmentCheckpoint family stays lightweight.
func buildCommitmentHeadSegments(ctx context.Context, db AggregatorDB, dir string, _ kvdomains.KVDomain, from, to uint64, path string) ([]SegmentRef, error) {
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("snapshots: missing MuHash HEAD")
	}
	if head.TxNum != to {
		return nil, fmt.Errorf("snapshots: HEAD tx %d differs from source cut %d", head.TxNum, to)
	}
	root, present, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return nil, err
	}
	if !present || root != head.Root {
		return nil, errors.New("snapshots: HEAD/root source cut mismatch")
	}
	value, err := rawdb.EncodeStateCommitmentHead(head)
	if err != nil {
		return nil, err
	}
	ref, _, btree, err := writeLatestBinarySegmentWithCompanionsContext(ctx, dir, SegmentRef{Dataset: SegmentDatasetCommitmentHead, Kind: SegmentLatest, FromTxNum: from, ToTxNum: to, Path: path}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.StateCommitmentHeadKey(), Value: value})
	}, false)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{ref, btree}, nil
}

func (m *Manager) GetCommitmentHead(txNum uint64) (rawdb.StateCommitmentHead, bool, error) {
	value, ok, err := m.getLatestValue(SegmentDatasetCommitmentHead, 0, rawdb.StateCommitmentHeadKey(), txNum)
	if err != nil || !ok {
		return rawdb.StateCommitmentHead{}, ok, err
	}
	head, err := rawdb.DecodeStateCommitmentHead(value)
	if err != nil {
		return rawdb.StateCommitmentHead{}, false, err
	}
	if head.TxNum != txNum {
		return rawdb.StateCommitmentHead{}, false, fmt.Errorf("snapshots: HEAD tx %d differs from requested cut %d", head.TxNum, txNum)
	}
	return head, true, nil
}

func (s rawDBLatestHotStore) WriteCommitmentHead(value []byte) error {
	head, err := rawdb.DecodeStateCommitmentHead(value)
	if err != nil {
		return err
	}
	if s.writer == nil {
		return errors.New("snapshots: nil HEAD writer")
	}
	return rawdb.WriteStateCommitmentHead(s.writer, head)
}

func verifyRestoredCommitmentHead(db ethdb.KeyValueReader, mgr *Manager, manifest *Manifest) error {
	txNum := manifest.LatestStateTxNum
	expected, ok, err := mgr.GetCommitmentHead(txNum)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("snapshots: missing complete HEAD at snapshot cut")
	}
	got, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("snapshots: restored source has no MuHash HEAD")
	}
	a, err := rawdb.EncodeStateCommitmentHead(expected)
	if err != nil {
		return err
	}
	b, err := rawdb.EncodeStateCommitmentHead(got)
	if err != nil {
		return err
	}
	if !bytes.Equal(a, b) {
		return errors.New("snapshots: restored complete HEAD differs from source cut")
	}
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return err
	}
	if !ok || root != got.Root {
		return errors.New("snapshots: restored latest commitment root / HEAD mismatch")
	}
	checkpoint, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(db)
	if err != nil {
		return err
	}
	if !ok || checkpoint.BlockNum != got.BlockNum || checkpoint.BlockHash != got.BlockHash || checkpoint.Root != got.Root || checkpoint.Scheme != got.Scheme {
		return errors.New("snapshots: commitment checkpoint differs from complete HEAD cut")
	}
	source, ok := db.(AggregatorDB)
	if !ok {
		return errors.New("snapshots: source enumeration required for restored HEAD verification")
	}
	rebuilt, counts, err := rebuildLatestMuHashCounted(source)
	if err != nil {
		return err
	}
	if common.Hash(rebuilt.Digest) != got.Root || rebuilt.State != got.State || counts != manifest.SourceRows {
		return errors.New("snapshots: restored source collection differs from complete MuHash HEAD")
	}
	return verifyRestoredExecutionCheckpoint(db, mgr, got)
}

// RebuildLatestMuHash is an explicit offline/snapshot-install authentication
// pass. Ordinary startup validates only the constant-size HEAD and identity.
func RebuildLatestMuHash(db AggregatorDB) (muhash3072.Finalized, error) {
	value, _, err := rebuildLatestMuHashCounted(db)
	return value, err
}
func rebuildLatestMuHashCounted(db AggregatorDB) (muhash3072.Finalized, [3]uint64, error) {
	acc := muhash3072.NewAccumulator()
	var counts [3]uint64
	for family, prefix := range rawdb.StateCommitmentSourcePrefixes() {
		it := db.NewIterator(prefix, nil)
		var err error
		for it.Next() {
			element, e := rawdb.EncodeStateCommitmentElement(it.Key(), it.Value())
			if e != nil {
				err = e
				break
			}
			if e = acc.Insert(element); e != nil {
				err = e
				break
			}
			counts[family]++
		}
		err = errors.Join(err, it.Error())
		it.Release()
		err = errors.Join(err, it.Error())
		if err != nil {
			return muhash3072.Finalized{}, counts, err
		}
	}
	value, err := acc.Finalize()
	return value, counts, err
}
func latestSourceRowCounts(refs []SegmentRef, txNum uint64) (counts [3]uint64) {
	if txNum == 0 {
		return
	}
	for _, ref := range refs {
		if ref.Kind != SegmentLatest || ref.ToTxNum != txNum {
			continue
		}
		switch ref.Dataset {
		case SegmentDatasetAccountLatest:
			counts[0] += ref.Rows
		case SegmentDatasetKVGeneration:
			counts[1] += ref.Rows
		case SegmentDatasetKVLatest:
			counts[2] += ref.Rows
		}
	}
	return
}

func validateMuHashLatestCut(manifest *Manifest) error {
	if manifest.LatestStateTxNum == 0 {
		if manifestHasLatestDataset(manifest, SegmentDatasetCommitmentHead) {
			return errors.New("snapshots: complete HEAD without explicit latest source cut")
		}
		return nil
	}
	var heads, roots, accounts, generations, executions, checkpoints int
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentLatest {
			continue
		}
		switch ref.Dataset {
		case SegmentDatasetCommitmentHead:
			heads++
		case SegmentDatasetExecutionCheckpoint:
			executions++
		case SegmentDatasetCommitmentCheckpoint:
			checkpoints++
		case SegmentDatasetCommitmentRoot:
			roots++
		case SegmentDatasetAccountLatest:
			accounts++
		case SegmentDatasetKVGeneration:
			generations++
		case SegmentDatasetKVLatest:
		default:
			continue
		}
		if ref.ToTxNum != manifest.LatestStateTxNum {
			return errors.New("snapshots: latest source/HEAD families span different cuts")
		}
	}
	if heads != 1 || roots != 1 || accounts != 1 || generations != 1 || executions != 1 || checkpoints != 1 {
		return errors.New("snapshots: incomplete latest source/HEAD cut")
	}
	if counts := latestSourceRowCounts(manifest.Segments, manifest.LatestStateTxNum); counts != manifest.SourceRows {
		return errors.New("snapshots: latest source row census differs from manifest")
	}
	return nil
}

// A lightweight checkpoint is synthesized only at the actual snapshot cut.
// Production need not maintain a historical per-block checkpoint table.
func buildCutCommitmentCheckpointSegments(ctx context.Context, db AggregatorDB, dir string, _ kvdomains.KVDomain, from, to uint64, path string) ([]SegmentRef, error) {
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return nil, err
	}
	if !present || head.TxNum != to {
		return nil, errors.New("snapshots: cut checkpoint requires same-cut HEAD")
	}
	value, err := rawdb.EncodeStateCommitmentCheckpointValue(&rawdb.StateCommitmentCheckpoint{BlockNum: head.BlockNum, BlockHash: head.BlockHash, Root: head.Root, Scheme: head.Scheme})
	if err != nil {
		return nil, err
	}
	ref, _, btree, err := writeLatestBinarySegmentWithCompanionsContext(ctx, dir, SegmentRef{Dataset: SegmentDatasetCommitmentCheckpoint, Kind: SegmentLatest, FromTxNum: from, ToTxNum: to, Path: path}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.LatestStateCommitmentCheckpointLogicalKey(), Value: value})
	}, false)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{ref, btree}, nil
}
