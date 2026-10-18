package snapshots

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// ErrRestorePublicationUncertain requires reopening the exclusive target and
// checking marker/HEAD/current identity before any further action. It never
// authorizes replay of payload batches.
var ErrRestorePublicationUncertain = errors.New("snapshots: final publication outcome uncertain; reopen and inspect marker/HEAD/current identity before proceeding")

// Payload batches may populate an offline empty target, but publication keys
// stay in this constant-size overlay until all verification succeeds.
type unpublishedRestore struct {
	ethdb.KeyValueStore
	anchors    map[string][]byte
	anchorKeys map[string]bool
}
type keyCapture struct{ key []byte }

func (c *keyCapture) Put(k, v []byte) error { c.key = bytes.Clone(k); return nil }
func (c *keyCapture) Delete([]byte) error   { return errors.New("unexpected key capture delete") }
func newUnpublishedRestore(db ethdb.KeyValueWriter) (*unpublishedRestore, error) {
	store, ok := db.(ethdb.KeyValueStore)
	if !ok {
		return nil, errors.New("snapshots: fresh restore requires a batch-capable readable target")
	}
	if active, err := rawdb.StateCommitmentRestoreInProgress(store); err != nil {
		return nil, err
	} else if active {
		return nil, errors.New("snapshots: residual incomplete restore; fresh empty target required")
	}
	if _, ok, err := rawdb.ReadStateCommitmentHead(store); err != nil {
		return nil, err
	} else if ok {
		return nil, errors.New("snapshots: restore target already has HEAD; fresh state required")
	}
	if _, ok, err := rawdb.ReadLatestDomainCommitmentRoot(store); err != nil {
		return nil, err
	} else if ok {
		return nil, errors.New("snapshots: restore target already has current root")
	}
	if _, ok, err := rawdb.ReadHeadBlockHashStrict(store); err != nil {
		return nil, err
	} else if ok {
		return nil, errors.New("snapshots: restore target already has current chain anchor")
	}
	for _, prefix := range rawdb.StateCommitmentSourcePrefixes() {
		it := store.NewIterator(prefix, nil)
		exists := it.Next()
		err := it.Error()
		it.Release()
		err = errors.Join(err, it.Error())
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("snapshots: restore target has residual source rows for %x", prefix)
		}
	}
	if err := rawdb.WriteStateCommitmentRestoreMarker(store); err != nil {
		return nil, err
	}
	syncer, ok := store.(interface{ SyncKeyValue() error })
	if !ok {
		return nil, errors.New("snapshots: durable restore marker requires SyncKeyValue capability")
	}
	if err := syncer.SyncKeyValue(); err != nil {
		return nil, err
	}
	c := new(keyCapture)
	if err := rawdb.WriteLatestDomainCommitmentRoot(c, common.Hash{}); err != nil {
		return nil, err
	}
	out := &unpublishedRestore{KeyValueStore: store, anchors: map[string][]byte{}, anchorKeys: map[string]bool{string(rawdb.StateCommitmentHeadKey()): true, string(rawdb.CycleRewardPendingKey()): true, string(c.key): true}}
	capture := &executionKeyCapture{keys: out.anchorKeys}
	if err := rawdb.WriteStateExecutionProperties(capture, rawdb.StateExecutionProperties{}); err != nil {
		return nil, err
	}
	return out, nil
}
func (s *unpublishedRestore) Put(k, v []byte) error {
	if s.anchorKeys[string(k)] {
		s.anchors[string(k)] = bytes.Clone(v)
		if rawdb.IsStateCommitmentHeadKey(k) {
			head, err := rawdb.DecodeStateCommitmentHead(v)
			if err != nil {
				return err
			}
			capture := new(keyCapture)
			if err := rawdb.WriteBlockNumber(capture, head.BlockHash, head.BlockNum); err != nil {
				return err
			}
			s.anchorKeys[string(capture.key)] = true
			return rawdb.WriteBlockNumber(s, head.BlockHash, head.BlockNum)
		}
		return nil
	}
	return s.KeyValueStore.Put(k, v)
}
func (s *unpublishedRestore) Get(k []byte) ([]byte, error) {
	if v, ok := s.anchors[string(k)]; ok {
		return bytes.Clone(v), nil
	}
	return s.KeyValueStore.Get(k)
}
func (s *unpublishedRestore) Has(k []byte) (bool, error) {
	if _, ok := s.anchors[string(k)]; ok {
		return s.anchors[string(k)] != nil, nil
	}
	return s.KeyValueStore.Has(k)
}
func (s *unpublishedRestore) Delete(k []byte) error {
	if s.anchorKeys[string(k)] {
		s.anchors[string(k)] = nil
		return nil
	}
	return s.KeyValueStore.Delete(k)
}
func (s *unpublishedRestore) NewBatch() ethdb.Batch {
	return &unpublishedBatch{Batch: s.KeyValueStore.NewBatch(), owner: s}
}
func (s *unpublishedRestore) NewBatchWithSize(n int) ethdb.Batch {
	return &unpublishedBatch{Batch: s.KeyValueStore.NewBatchWithSize(n), owner: s}
}

type unpublishedBatch struct {
	ethdb.Batch
	owner *unpublishedRestore
}

func (b *unpublishedBatch) Put(k, v []byte) error {
	if b.owner.anchorKeys[string(k)] {
		return b.owner.Put(k, v)
	}
	return b.Batch.Put(k, v)
}
func (b *unpublishedBatch) Delete(k []byte) error {
	if b.owner.anchorKeys[string(k)] {
		b.owner.anchors[string(k)] = nil
		return nil
	}
	return b.Batch.Delete(k)
}
func (s *unpublishedRestore) publish(manifest *Manifest, finalMetadata func(ethdb.KeyValueWriter, rawdb.StateCommitmentHead) error) (*Progress, error) {
	head, ok, err := rawdb.ReadStateCommitmentHead(s)
	if err != nil {
		return nil, err
	}
	if !ok || head.TxNum != manifest.LatestStateTxNum {
		return nil, errors.New("snapshots: complete authenticated HEAD required before publication")
	}
	b := s.KeyValueStore.NewBatch()
	defer b.Close()
	for key, value := range s.anchors {
		var err error
		if value == nil {
			err = b.Delete([]byte(key))
		} else {
			err = b.Put([]byte(key), value)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := rawdb.WriteBlockStateRoot(b, head.BlockHash, head.Root); err != nil {
		return nil, err
	}
	if err := rawdb.WriteBlockNumber(b, head.BlockHash, head.BlockNum); err != nil {
		return nil, err
	}
	if err := rawdb.WriteCanonicalStageProgressWithHash(b, head.BlockNum, head.BlockHash); err != nil {
		return nil, err
	}
	capture := new(keyCapture)
	rawdb.WriteHeadBlockHash(capture, head.BlockHash)
	if err := b.Put(capture.key, head.BlockHash.Bytes()); err != nil {
		return nil, err
	}
	progress := progressForInstalledManifest(manifest)
	rows, err := snapshotInstallProgressRows(s, progress, manifest)
	if err != nil {
		return nil, err
	}
	if err := rawdb.WriteStageProgressRows(b, rows); err != nil {
		return nil, err
	}
	if finalMetadata != nil {
		if err := finalMetadata(b, head); err != nil {
			return nil, err
		}
	}
	if err := rawdb.DeleteStateCommitmentRestoreMarker(b); err != nil {
		return nil, err
	}
	if err := b.Write(); err != nil {
		return nil, errors.Join(ErrRestorePublicationUncertain, err)
	}
	if syncer, ok := s.KeyValueStore.(interface{ SyncKeyValue() error }); ok {
		if err := syncer.SyncKeyValue(); err != nil {
			return nil, errors.Join(ErrRestorePublicationUncertain, err)
		}
	}
	return progress, nil
}

type executionKeyCapture struct{ keys map[string]bool }

func (c *executionKeyCapture) Put(key, value []byte) error { c.keys[string(key)] = true; return nil }
func (c *executionKeyCapture) Delete(key []byte) error     { c.keys[string(key)] = true; return nil }

func (s *unpublishedRestore) WriteSnapshotTxRange(row rawdb.StateTxRange) error {
	capture := new(keyCapture)
	if err := rawdb.WriteStateTxRange(capture, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
		return err
	}
	s.anchorKeys[string(capture.key)] = true
	return rawdb.WriteStateTxRange(s, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum)
}
func (b *unpublishedBatch) WriteSnapshotTxRange(row rawdb.StateTxRange) error {
	return b.owner.WriteSnapshotTxRange(row)
}
