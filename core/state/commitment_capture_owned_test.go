package state

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func assertCapturedPhysicalOracle(t *testing.T, db domains.CommitmentDB, root common.Hash) {
	t.Helper()
	oracle := muhash3072.NewAccumulator()
	if err := rawdb.IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
		el, e := rawdb.EncodeStateCommitmentElement(k, v)
		if e == nil {
			e = oracle.Insert(el)
		}
		return e == nil, e
	}); err != nil {
		t.Fatal(err)
	}
	final, err := oracle.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil || !present || head.State != final.State || head.Root != common.Hash(final.Digest) || root != head.Root {
		t.Fatal("captured source/HEAD/root mismatch", err)
	}
}

func TestCommitmentBeforeSurvivesCollectorAndBatchReuse(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		exists bool
		raw    []byte
	}{{"missing", false, nil}, {"present-empty", true, rawdb.EncodeStateKVLatestValue(nil)}, {"physical-zero", true, rawdb.EncodeStateKVLatestValue(make([]byte, 32))}} {
		t.Run(fixture.name, func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			if err := rawdb.InitializeStateCommitment(disk); err != nil {
				t.Fatal(err)
			}
			owner := testAddr(0xac)
			slot := make([]byte, 32)
			slot[31] = 7
			key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, slot)
			if fixture.exists {
				if err := disk.Put(key, fixture.raw); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := domains.RebuildMuHashCommitment(disk); err != nil {
				t.Fatal(err)
			}
			collector := NewDomainCommitmentState(&StateDB{})
			capture := func(before []byte, exists bool, after byte, id domains.CommitmentIdentity) *CapturedCommit {
				t.Helper()
				input := bytes.Clone(before)
				if err := collector.recordPhysicalBefore(key, input, exists); err != nil {
					t.Fatal(err)
				}
				clear(input) // first capture must own its bytes independently.
				if err := collector.recordPhysicalBefore(key, []byte("later same-key write"), true); err != nil {
					t.Fatal(err)
				}
				collector.recordKVLatestValue(owner, 0, kvdomains.ContractStorage, slot, []byte{after}, true, false)
				batch, err := collector.latestUpdateBatchFromTouches()
				if err != nil {
					t.Fatal(err)
				}
				return &CapturedCommit{batch: batch, identity: id}
			}
			a := capture(fixture.raw, fixture.exists, 1, domains.CommitmentIdentity{BlockNum: 1, BlockHash: common.Hash{1}})
			defer a.Release()
			collector.resetForCommit(&StateDB{}, nil)
			b := capture(rawdb.EncodeStateKVLatestValue([]byte{1}), true, 2, domains.CommitmentIdentity{BlockNum: 2, BlockHash: common.Hash{2}, ParentHash: common.Hash{1}})
			defer b.Release()
			collector.release()
			root, err := a.Fold(disk)
			if err != nil {
				t.Fatal(err)
			}
			assertCapturedPhysicalOracle(t, disk, root)
			// Reborrow and overwrite pooled descriptors/key storage after A releases;
			// B still owns its captured before/after references.
			reuse := borrowCommitmentUpdateBatch(8, 1024)
			reuse.keyArena = append(reuse.keyArena, bytes.Repeat([]byte{0xee}, 1024)...)
			for i := range reuse.updates {
				reuse.updates[i] = rawdb.StateCommitmentUpdate{Before: []byte("overwrite"), Value: []byte("overwrite")}
			}
			root, err = b.Fold(disk)
			if err != nil {
				t.Fatal(err)
			}
			assertCapturedPhysicalOracle(t, disk, root)
			reuse.release()
			if _, err = a.Fold(disk); err == nil {
				t.Fatal("released capture reused")
			}
		})
	}
}

func TestCommitmentBorrowCaptureOwnsBytesWithoutSLOADCount(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		value   []byte
		deleted bool
	}{{"missing", nil, true}, {"empty", []byte{}, false}, {"zero", make([]byte, 32), false}, {"nonzero-overwrite", bytes.Repeat([]byte{0x12}, 32), false}} {
		t.Run(fixture.name, func(t *testing.T) {
			owner := testAddr(0xad)
			slot := make([]byte, 32)
			slot[31] = 8
			count := int64(19)
			collector := NewDomainCommitmentState(&StateDB{})
			pending := bytes.Clone(fixture.value)
			w := &accountKVLatestBatch{commitment: collector, storagePendingResolved: &count, pending: map[accountKVLatestPendingMapKey]accountKVLatestPending{accountKVLatestPendingLookupKey(owner, 0, kvdomains.ContractStorage, slot): {value: pending, deleted: fixture.deleted}}}
			if err := w.captureKVBefore(owner, 0, kvdomains.ContractStorage, slot); err != nil {
				t.Fatal(err)
			}
			if count != 19 {
				t.Fatal("commitment capture incremented SLOAD pending count", count)
			}
			clear(pending)
			key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, slot)
			update, err := collector.attachPhysicalBefore(rawdb.StateCommitmentUpdate{Key: key})
			if err != nil {
				t.Fatal(err)
			}
			want := rawdb.EncodeStateKVLatestValue(fixture.value)
			if update.BeforeExists == fixture.deleted || (!fixture.deleted && !bytes.Equal(update.Before, want)) || (fixture.deleted && update.Before != nil) {
				t.Fatal("capture borrowed/changed pending before")
			}
			if _, _, err = w.readLatestForDecoding(owner, 0, kvdomains.ContractStorage, slot); err != nil {
				t.Fatal(err)
			}
			if count != 20 {
				t.Fatal("execution SLOAD pending count changed", count)
			}
		})
	}
}
