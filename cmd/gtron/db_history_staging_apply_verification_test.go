package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestHistoryStagingApplyCompletedStillRequiresFullVerification(t *testing.T) {
	for _, corruption := range []string{"target-payload", "cold-file", "receipt"} {
		t.Run(corruption, func(t *testing.T) {
			f := newHistoryStagingE2EFixture(t)
			plan, err := f.command(t, "migrate", "--max-buckets", "0")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.command(t, "apply", "--plan-id", plan.PlanID, "--apply-workers", "8"); err != nil {
				t.Fatal(err)
			}
			if corruption == "cold-file" {
				manifest, err := statesnapshots.LoadProductionManifest(f.cold)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(f.cold, manifest.Segments[0].Path)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)-1] ^= 1
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				stage, err := rawdb.NewHistoryStagingPebbleDB(defaultHistoryStagingDir(f.datadir), 16, 16, false)
				if err != nil {
					t.Fatal(err)
				}
				it := stage.NewIterator(nil, nil)
				var key, value []byte
				for it.Next() {
					if corruption == "target-payload" && len(it.Value()) > 64<<10 {
						key = bytes.Clone(it.Key())
						break
					}
					if corruption == "receipt" {
						var receipt rawdb.HistoryStagingReceipt
						if rlp.DecodeBytes(it.Value(), &receipt) == nil && receipt.ProofDigest != ([32]byte{}) && receipt.DataDigest != ([32]byte{}) {
							receipt.DataDigest[0] ^= 1
							key = bytes.Clone(it.Key())
							value, err = rlp.EncodeToBytes(receipt)
							if err != nil {
								t.Fatal(err)
							}
							break
						}
					}
				}
				iterErr := it.Error()
				it.Release()
				if iterErr != nil || key == nil {
					stage.Close()
					t.Fatalf("corruption fixture key unavailable: %v", iterErr)
				}
				if corruption == "target-payload" {
					err = stage.Delete(key)
				} else {
					err = stage.Put(key, value)
				}
				if err != nil {
					stage.Close()
					t.Fatal(err)
				}
				if err := stage.SyncKeyValue(); err != nil {
					stage.Close()
					t.Fatal(err)
				}
				if err := stage.Close(); err != nil {
					t.Fatal(err)
				}
			}
			_, resumeErr := f.command(t, "resume", "--plan-id", plan.PlanID, "--apply-workers", "8")
			if corruption == "receipt" && resumeErr == nil {
				t.Fatal("completed receipt tampering skipped")
			}
			if corruption == "target-payload" && resumeErr != nil {
				t.Fatalf("metadata-only resume unexpectedly rescanned payload: %v", resumeErr)
			}
			if _, err := f.command(t, "inspect", "--verify-complete", "--plan-id", plan.PlanID, "--apply-workers", "8"); err == nil {
				t.Fatal("corruption passed final verification")
			}
			source, err := rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			stage, err := rawdb.NewHistoryStagingPebbleDB(defaultHistoryStagingDir(f.datadir), 16, 16, false)
			if err != nil {
				t.Fatal(err)
			}
			defer stage.Close()
			planData, err := os.ReadFile(filepath.Join(historyStagingPlanDirectory(f.datadir), plan.PlanID+".jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var header historyStagingPlanHeader
			if err := json.Unmarshal(bytes.SplitN(planData, []byte{'\n'}, 2)[0], &header); err != nil {
				t.Fatal(err)
			}
			manager, err := rawdb.NewHistoryStagingManager(source, stage, historyStagingIdentity(header.Paths, header.GenesisHash, header.NetworkID))
			if err != nil {
				t.Fatal(err)
			}
			if _, present, err := manager.ReadHistoryStagingRouteBarrier(); err != nil || present {
				t.Fatalf("barrier published after failed verification: present=%v err=%v", present, err)
			}
		})
	}
}

func TestHistoryStagingApplyRejectsWorkerOverflowBeforeMutation(t *testing.T) {
	f := newHistoryStagingE2EFixture(t)
	plan, err := f.command(t, "migrate", "--max-buckets", "0")
	if err != nil {
		t.Fatal(err)
	}
	target := defaultHistoryStagingDir(f.datadir)
	_, before := os.Stat(target)
	if _, err := f.command(t, "apply", "--plan-id", plan.PlanID, "--apply-workers", "9"); err == nil {
		t.Fatal("worker overflow accepted")
	}
	if os.IsNotExist(before) {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatal("invalid worker count opened or initialized target")
		}
	}
}
