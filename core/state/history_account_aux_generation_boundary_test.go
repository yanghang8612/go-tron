package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestHistoricalAccountAuxGenerationBoundaries(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "deleted-at-head"
		if recreate {
			name = "recreated-gen1"
		}
		t.Run(name, func(t *testing.T) {
			f := newHistoryFixture(t)
			owner, other := testAddr(0xe6), testAddr(0xe7)
			active3 := types.MakeDefaultActivePermission(owner, make([]byte, 32))
			active3.Id, active3.PermissionName = 3, "unchanged-custom-active-3"
			oldOwner := types.MakeDefaultOwnerPermission(owner)
			oldWitness := types.MakeDefaultWitnessPermission(owner)
			f.applyBlock(common.Hash{1}, func(s *StateDB) {
				s.CreateAccount(owner, corepb.AccountType_Normal)
				s.SetPermissions(owner, oldOwner, oldWitness, []*corepb.Permission{active3})
				for key, value := range map[string]int64{"old-only": 11, "shared": 12} {
					if err := s.SetAccountKV(owner, kvdomains.AccountAssetV2, []byte(key), encodeAccountAuxInt64(value)); err != nil {
						t.Fatal(err)
					}
				}
			})
			f.applyBlock(common.Hash{2}, func(s *StateDB) { s.AddBalance(other, 1) })
			f.applyBlock(common.Hash{3}, func(s *StateDB) { s.DeleteAccount(owner) })
			if recreate {
				f.applyBlock(common.Hash{4}, func(s *StateDB) {
					s.CreateAccount(owner, corepb.AccountType_Normal)
					s.SetPermissions(owner, types.MakeDefaultOwnerPermission(owner), nil,
						[]*corepb.Permission{types.MakeDefaultActivePermission(owner, make([]byte, 32))})
					for key, value := range map[string]int64{"new-only": 21, "shared": 22} {
						if err := s.SetAccountKV(owner, kvdomains.AccountAssetV2, []byte(key), encodeAccountAuxInt64(value)); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
			f.applyBlock(common.Hash{byte(f.head + 1)}, func(s *StateDB) { s.AddBalance(other, 1) })
			if _, exists, err := rawdb.ReadStateKVLatest(f.disk, owner, 0, kvdomains.AccountPermissionAux, accountActivePermissionKey(3)); err != nil || !exists {
				t.Fatal("original gen0 physical row must remain", exists, err)
			}
			futurePermissionChanges, futureAccountChanges, futureGenerationChanges := 0, 0, 0
			for h := uint64(3); h <= f.head; h++ {
				if err := rawdb.IterateStateDomainChanges(f.disk, h, func(c *rawdb.StateDomainChange) (bool, error) {
					if c.Owner == owner {
						switch c.FlatDomain {
						case rawdb.StateFlatDomainKVLatest:
							if c.Domain == kvdomains.AccountPermissionAux && c.Generation == 0 {
								futurePermissionChanges++
							}
						case rawdb.StateFlatDomainAccountLatest:
							futureAccountChanges++
						case rawdb.StateFlatDomainKVGeneration:
							futureGenerationChanges++
						}
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			check := func(label string, reader *PersistentHistoryReader) {
				t.Helper()
				for h := uint64(1); h <= 2; h++ {
					acc, err := reader.AccountAt(owner, h)
					if err != nil || acc == nil {
						t.Fatalf("%s AccountAt(%d): %v/%v", label, h, acc, err)
					}
					if !proto.Equal(acc.OwnerPermission(), oldOwner) || !proto.Equal(acc.WitnessPermission(), oldWitness) || len(acc.ActivePermission()) != 1 || !proto.Equal(acc.ActivePermission()[0], active3) {
						t.Fatalf("%s block %d lost old-generation permissions or leaked new keys: %v", label, h, acc.Proto())
					}
					assets := acc.Proto().GetAssetV2()
					if len(assets) != 2 || assets["old-only"] != 11 || assets["shared"] != 12 {
						t.Fatalf("%s block %d old generation assets=%v", label, h, assets)
					}
				}
				if acc, err := reader.AccountAt(owner, 3); err != nil || acc != nil {
					t.Fatalf("%s deleted target fabricated an account: %v/%v", label, acc, err)
				}
				if recreate {
					acc, err := reader.AccountAt(owner, 4)
					if err != nil || acc == nil {
						t.Fatalf("%s recreated AccountAt(4): %v/%v", label, acc, err)
					}
					if len(acc.ActivePermission()) != 1 || acc.ActivePermission()[0].Id != 2 || acc.WitnessPermission() != nil {
						t.Fatalf("%s old generation permissions leaked into recreation: %v", label, acc.Proto())
					}
					assets := acc.Proto().GetAssetV2()
					if len(assets) != 2 || assets["new-only"] != 21 || assets["shared"] != 22 {
						t.Fatalf("%s recreated assets=%v", label, assets)
					}
				}
			}
			t.Logf("future gen0 permission rows=%d account rows=%d generation rows=%d", futurePermissionChanges, futureAccountChanges, futureGenerationChanges)
			if futurePermissionChanges != 0 {
				t.Fatal("fixture must exercise missing per-key before-images across deletion/recreation")
			}
			check("hot", f.reader())
			from, _, err := rawdb.ReadStateTxRange(f.disk, 1)
			if err != nil {
				t.Fatal(err)
			}
			to, _, err := rawdb.ReadStateTxRange(f.disk, f.head)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, from.BeginTxNum, to.EndTxNum, "history/state-domain-change-generation-probe.seg")
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.PublishManifest(dir, snapshots.NewManifest(from.BeginTxNum, to.EndTxNum, refs)); err != nil {
				t.Fatal(err)
			}
			manager, err := snapshots.OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			f.pruneHotStateDomainHistory()
			check("cold-history-only", NewPersistentHistoryReaderWithColdHistory(f.disk, f.state, f.head, manager))
			latest, accessor, btree, err := snapshots.BuildLatestDomainSegmentFilesFromDB(f.disk, dir, kvdomains.AccountPermissionAux, from.BeginTxNum, to.EndTxNum, "latest/permission-generation-probe.seg")
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, latest, accessor, btree)
			latest, accessor, btree, err = snapshots.BuildLatestDomainSegmentFilesFromDB(f.disk, dir, kvdomains.AccountAssetV2, from.BeginTxNum, to.EndTxNum, "latest/asset-generation-probe.seg")
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, latest, accessor, btree)
			if err := snapshots.PublishManifest(dir, snapshots.NewManifest(from.BeginTxNum, to.EndTxNum, refs)); err != nil {
				t.Fatal(err)
			}
			withLatest, err := snapshots.OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			check("cold-with-head-latest", NewPersistentHistoryReaderWithColdHistory(f.disk, f.state, f.head, withLatest))
		})
	}
}
