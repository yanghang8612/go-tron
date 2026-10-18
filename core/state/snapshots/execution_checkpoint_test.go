package snapshots

import (
	"bytes"
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"testing"
)

func TestExecutionCheckpointSameCutAtomicRestore(t *testing.T) {
	for _, present := range []bool{false, true} {
		for _, abort := range []bool{false, true} {
			source := rawdb.NewMemoryDatabase()
			owner := coldBuilderOwner(7)
			if present {
				if err := rawdb.WriteCycleRewardPending(source, 8, map[common.Address]int64{owner: 91}); err != nil {
					t.Fatal(err)
				}
			}
			seedTestMuHashHead(t, source, 9, 12, common.Hash{9})
			dir := t.TempDir()
			built, err := NewAggregator(dir).BuildLatest(source, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 12})
			if err != nil {
				t.Fatal(err)
			}
			identity := chainFreezerTestIdentity()
			built.Manifest.Chain = &identity
			if err := PublishManifest(dir, built.Manifest); err != nil {
				t.Fatal(err)
			}
			db := rawdb.NewMemoryDatabase()
			opts := RestoreVerifiedSnapshotOptions{BeforePublish: func(staged ethdb.KeyValueStore, m *Manifest) error {
				if _, ok, err := rawdb.ReadStateCommitmentHead(db); err != nil || ok {
					t.Fatalf("HEAD exposed before publish %v %v", ok, err)
				}
				if _, _, ok, err := rawdb.ReadCycleRewardPending(db); err != nil || ok {
					t.Fatalf("pending exposed before publish %v %v", ok, err)
				}
				_, rewards, ok, err := rawdb.ReadCycleRewardPending(staged)
				if err != nil || ok != present || (present && rewards[owner] != 91) {
					t.Fatalf("overlay pending %v %v %v", rewards, ok, err)
				}
				if abort {
					return errors.New("prepublish chain failure")
				}
				return nil
			}}
			_, err = RestoreLatestFromVerifiedManifestWithOptions(db, dir, identity, opts)
			marker, markerErr := rawdb.StateCommitmentRestoreInProgress(db)
			if markerErr != nil {
				t.Fatal(markerErr)
			}
			if abort {
				if err == nil || !marker {
					t.Fatalf("abort=%v marker=%v", err, marker)
				}
			} else {
				if err != nil || marker {
					t.Fatalf("restore=%v marker=%v", err, marker)
				}
				_, rewards, ok, err := rawdb.ReadCycleRewardPending(db)
				if err != nil || ok != present || (present && rewards[owner] != 91) {
					t.Fatalf("pending %v %v %v", rewards, ok, err)
				}
			}
		}
	}
}

func TestExecutionCheckpointRejectsMissingOrWrongCut(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	head := seedTestMuHashHead(t, db, 1, 1, common.Hash{1})
	dir := t.TempDir()
	value, err := encodeExecutionCheckpoint(db, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 40, 72, 97} {
		bad := bytes.Clone(value)
		bad[offset] ^= 1
		if _, err := decodeExecutionCheckpoint(bad); err == nil {
			t.Fatalf("invalid field%d accepted", offset)
		}
	}
	refs, err := NewAggregator(dir).buildLatestSegments(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	kept := refs[:0]
	for _, ref := range refs {
		if ref.Dataset != SegmentDatasetExecutionCheckpoint {
			kept = append(kept, ref)
		}
	}
	if err := NewManifest(1, 1, kept).Validate(); err == nil {
		t.Fatal("missing execution checkpoint accepted")
	}
	// A record with a different HEAD identity remains valid bytes but fails cut authentication.
	bad := bytes.Clone(value)
	bad[8] ^= 1
	ref, _, btree, err := writeLatestBinarySegmentWithCompanionsContext(context.Background(), dir, SegmentRef{Dataset: SegmentDatasetExecutionCheckpoint, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: "bad-execution.seg"}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.CycleRewardPendingKey(), Value: bad})
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	kept = append(kept, ref, btree)
	identity := chainFreezerTestIdentity()
	if err := PublishManifest(dir, NewManifestForChain(1, 1, kept, identity)); err != nil {
		t.Fatal(err)
	}
	target := rawdb.NewMemoryDatabase()
	if _, err := RestoreLatestFromVerifiedManifest(target, dir, identity); err == nil {
		t.Fatal("execution checkpoint with wrong HEAD identity published")
	}
	if _, present, err := rawdb.ReadStateCommitmentHead(target); err != nil || present {
		t.Fatalf("bad cut published HEAD %v %v", present, err)
	}
}
