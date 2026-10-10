package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func initialBranchFixture(t *testing.T) (string, ethdb.KeyValueStore, rawdb.CommitmentBranchRotation, *AggregatorBuildResult) {
	t.Helper()
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	root := seedStagedBranchRows(t, db)
	rotation := rawdb.CommitmentBranchRotation{Generation: 1, SnapshotTxNum: 20, Root: root, BlockNum: 10, BlockHash: common.Hash{1}}
	if err := rawdb.WriteCommitmentBranchRotation(db, rotation); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	result, err := NewAggregator(dir).BuildCommitmentBranchBaseContext(context.Background(), db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 20})
	if err != nil {
		t.Fatal(err)
	}
	return dir, db, rotation, result
}

func TestInitialCommitmentBaseReuseAuthenticatesEveryFile(t *testing.T) {
	for _, fault := range []string{"none", "missing", "corrupt"} {
		for index := 0; index < 5; index++ {
			t.Run(fmt.Sprintf("%s/%d", fault, index), func(t *testing.T) {
				dir, db, rotation, first := initialBranchFixture(t)
				refs := first.Manifest.Segments
				if len(refs) != 5 {
					t.Fatalf("family files=%d", len(refs))
				}
				ref := refs[index]
				path := filepath.Join(dir, ref.Path)
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "missing":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				case "corrupt":
					f, err := os.OpenFile(path, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					var b [1]byte
					if _, err = f.ReadAt(b[:], before.Size()-1); err != nil {
						t.Fatal(err)
					}
					b[0] ^= 0x40
					if _, err = f.WriteAt(b[:], before.Size()-1); err != nil {
						t.Fatal(err)
					}
					f.Close()
				}
				result, err := NewAggregator(dir).BuildCommitmentBranchBaseContext(context.Background(), db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: rotation.SnapshotTxNum})
				if fault != "none" {
					if err == nil {
						t.Fatal("damaged published family accepted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Segments) != 0 || result.Manifest.Generation != first.Manifest.Generation || result.commitmentBaseProof == nil {
					t.Fatal("published family was rebuilt")
				}
				after, err := os.Stat(path)
				if err != nil || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
					t.Fatal("reused original changed", err)
				}
				if err := result.commitmentBaseProof.Recheck(rotation); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestInitialCommitmentBaseProofRequiresDurableUnchangedPublication(t *testing.T) {
	dir, _, rotation, _ := initialBranchFixture(t)
	proof, err := VerifyCommitmentBranchBase(context.Background(), dir, rotation)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	noFail := func(string) error { calls++; return nil }
	if err := proof.prepareDurable(context.Background(), noFail, noFail); err != nil {
		t.Fatal(err)
	}
	total := calls
	for fail := 1; fail <= total; fail++ {
		calls = 0
		sync := func(string) error {
			calls++
			if calls == fail {
				return errors.New("sync injected")
			}
			return nil
		}
		if err := proof.prepareDurable(context.Background(), sync, sync); err == nil {
			t.Fatalf("sync failure %d accepted", fail)
		}
		if err := proof.Recheck(rotation); err == nil {
			t.Fatal("failed cold barrier granted deletion authority")
		}
		if err := proof.PrepareDurable(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyCommitmentBranchBase(ctx, dir, rotation); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := proof.Recheck(rawdb.CommitmentBranchRotation{}); err == nil {
		t.Fatal("wrong rotation accepted")
	}
	for ref := range proof.facts {
		data, err := os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		replacement := filepath.Join(dir, ref.Path) + ".replacement"
		if err := os.WriteFile(replacement, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, filepath.Join(dir, ref.Path)); err != nil {
			t.Fatal(err)
		}
		if err := proof.Recheck(rotation); err == nil {
			t.Fatal("same bytes replacement reused")
		}
		break
	}
}
