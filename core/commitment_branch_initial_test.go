package core

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type initialBranchFaultDB struct {
	ethdb.Database
	syncCalls, failSync int
	failDelete          bool
	hasHook             func([]byte) (bool, error)
}

func (d *initialBranchFaultDB) Has(key []byte) (bool, error) {
	if d.hasHook != nil {
		return d.hasHook(key)
	}
	return d.Database.Has(key)
}

func (d *initialBranchFaultDB) SyncKeyValue() error {
	d.syncCalls++
	if d.failSync > 0 && d.syncCalls == d.failSync {
		return errors.New("initial branch sync injected")
	}
	return nil
}
func (d *initialBranchFaultDB) DeleteRange(start, end []byte) error {
	if d.failDelete {
		return errors.New("initial branch delete injected")
	}
	return d.Database.DeleteRange(start, end)
}
func coreInitialBranchFixture(t *testing.T) (*BlockChain, *initialBranchFaultDB, string, rawdb.CommitmentBranchRotation, *snapshots.VerifiedCommitmentBranchBase) {
	t.Helper()
	db := &initialBranchFaultDB{Database: ethrawdb.NewMemoryDatabase()}
	witness := testInsertAddr(0xc3)
	bc := newAsyncFlushChainOn(t, db, witness)
	t.Cleanup(func() { db.failSync = 0; db.failDelete = false; bc.Close() })
	block := buildTestBlock(bc, witness, 3000)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if err := rawdb.WriteStateTxRange(db, block.Number(), block.Hash(), 1, 1); err != nil {
		t.Fatal(err)
	}
	rotation, ok, err := bc.BeginInitialCommitmentBranchRotation()
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	dir := t.TempDir()
	if _, err := snapshots.NewAggregator(dir).BuildCommitmentBranchBaseContext(context.Background(), db, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: rotation.SnapshotTxNum}); err != nil {
		t.Fatal(err)
	}
	proof, err := snapshots.VerifyCommitmentBranchBase(context.Background(), dir, rotation)
	if err != nil {
		t.Fatal(err)
	}
	return bc, db, dir, rotation, proof
}
func TestInitialCommitmentAcceptedCleanupDurabilityAndRetry(t *testing.T) {
	for _, fault := range []string{"first-sync", "delete", "last-sync"} {
		t.Run(fault, func(t *testing.T) {
			bc, db, _, rotation, proof := coreInitialBranchFixture(t)
			if err := rawdb.WriteCommitmentBranchBase(db, rawdb.CommitmentBranchBase{Generation: 1, SnapshotTxNum: rotation.SnapshotTxNum, Root: rotation.Root, BlockNum: rotation.BlockNum, BlockHash: rotation.BlockHash}); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.DeleteCommitmentBranchRotation(db); err != nil {
				t.Fatal(err)
			}
			db.syncCalls = 0
			switch fault {
			case "first-sync":
				db.failSync = 1
			case "delete":
				db.failDelete = true
			case "last-sync":
				db.failSync = 2
			}
			if _, err := bc.CleanupAcceptedInitialCommitmentBranchBase(context.Background(), proof); err == nil {
				t.Fatal("fault accepted")
			}
			present, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(db)
			if err != nil || present != (fault != "last-sync") {
				t.Fatalf("unsafe deletion fault=%s present=%v err=%v", fault, present, err)
			}
			db.failSync = 0
			db.failDelete = false
			if _, err := bc.CleanupAcceptedInitialCommitmentBranchBase(context.Background(), proof); err != nil {
				t.Fatal("cleanup retry", err)
			}
			if rows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(db); err != nil || rows {
				t.Fatal("legacy remains", rows, err)
			}
			if _, active, err := bc.BeginInitialCommitmentBranchRotation(); err != nil || active {
				t.Fatal("successor rotation started", active, err)
			}
		})
	}
}

func TestInitialCommitmentRejectsChangedProofAndCancelledCompletion(t *testing.T) {
	for _, fault := range []string{"missing-accessor", "same-bytes-replacement", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			bc, db, dir, rotation, proof := coreInitialBranchFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if fault == "cancel" {
				cancel()
			} else {
				manifest, err := snapshots.LoadProductionManifest(dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, ref := range manifest.Segments {
					if ref.NormalizedDataset() != snapshots.SegmentDatasetCommitmentBranch || ref.Kind != snapshots.SegmentAccessor {
						continue
					}
					path := filepath.Join(dir, ref.Path)
					if fault == "missing-accessor" {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
					} else {
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path+".swap", data, 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.Rename(path+".swap", path); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := bc.CompleteInitialCommitmentBranchRotation(ctx, rotation, proof); err == nil {
				t.Fatal("invalid proof accepted")
			}
			if _, based, err := rawdb.ReadCommitmentBranchBase(db); err != nil || based {
				t.Fatal("base published", based, err)
			}
			if rows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(db); err != nil || !rows {
				t.Fatal("legacy deleted", rows, err)
			}
		})
	}
}

func TestInitialCommitmentCancellationBeforeRejectedBoundaryRebuild(t *testing.T) {
	bc, db, _, rotation, proof := coreInitialBranchFixture(t)
	props := bc.cachedDynProps().Copy()
	props.SetLatestSolidifiedBlockNum(int64(rotation.BlockNum))
	bc.storeDynPropsCache(props)
	canonicalRaw, ok, err := rawdb.ReadBlockRawStrict(bc.chaindb, rotation.BlockNum)
	if err != nil || !ok {
		t.Fatal("canonical fixture", ok, err)
	}
	var canonicalKey []byte
	it := db.NewIterator(nil, nil)
	for it.Next() {
		if bytes.Equal(it.Value(), canonicalRaw) {
			canonicalKey = append([]byte(nil), it.Key()...)
			break
		}
	}
	iterErr := it.Error()
	it.Release()
	if iterErr != nil || canonicalKey == nil {
		t.Fatal("canonical key fixture", iterErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reached := false
	// Report an absent canonical row and cancel during the boundary lookup,
	// after completion's initial context checks and async flush barriers.
	db.hasHook = func(key []byte) (bool, error) {
		if bytes.Equal(key, canonicalKey) {
			reached = true
			cancel()
			return false, nil
		}
		return db.Database.Has(key)
	}
	if err := bc.CompleteInitialCommitmentBranchRotation(ctx, rotation, proof); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled completion rebuilt hot branches", err)
	}
	db.hasHook = nil
	if !reached {
		t.Fatal("late cancellation was not exercised")
	}
	if marker, ok, err := rawdb.ReadCommitmentBranchRotation(db); err != nil || !ok || marker != rotation {
		t.Fatal("cancelled completion changed rotation", marker, ok, err)
	}
	if rows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(db); err != nil || !rows {
		t.Fatal("cancelled completion removed legacy", rows, err)
	}
}
