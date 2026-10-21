package rawdb

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestQueryTailPlanOneScanForRepairedOwners(t *testing.T) {
	f, owner := queryTailFixture(t)
	alias := owner
	alias[0] = 0
	if err := WriteStateDomainChangeRow(f, &StateDomainChange{BlockNum: 9, Seq: 80, TxNum: 980, FlatDomain: StateFlatDomainAccountLatest, Owner: alias, Prev: []byte("alias")}); err != nil {
		t.Fatal(err)
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryTailCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	owners := []common.Address{owner, {0x41, 0x33}, alias, {0x41, 0x99}}
	PlanStateHistoryQueryTailOwners(cache, owners)
	for pass := 0; pass < 2; pass++ {
		for _, who := range owners {
			var want, got []*StateDomainChange
			if err := IterateStateDomainChangesByBlockRange(view, 2, 9, func(row *StateDomainChange) (bool, error) {
				if row.Owner == who {
					want = append(want, cloneStateDomainChange(row))
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, who, func(row *StateDomainChange) (bool, error) {
				got = append(got, cloneStateDomainChange(row))
				if len(row.Prev) > 0 {
					row.Prev[0] ^= 1
				}
				return true, nil
			}); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("owner %x pass %d mismatch: %v", who, pass, err)
			}
		}
	}
	if counted.scans != 1 || len(cache.entries) != len(owners) || cache.used > cache.budget || cache.rows > cache.maxRows {
		t.Fatal("plan did not share the bounded full scan", counted.scans, len(cache.entries), cache.used, cache.rows)
	}
}

func TestQueryTailPlanIncompleteScansNeverProveAbsence(t *testing.T) {
	for _, mode := range []string{"early", "cancel", "error", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			f, owner := queryTailFixture(t)
			if mode == "corrupt" {
				if err := f.Put(stateChangeSetKey(2, 0), append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), stateDomainChangeBlockSharedVersion, 0xff)); err != nil {
					t.Fatal(err)
				}
			}
			view, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			cache := WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
			PlanStateHistoryQueryTailOwners(cache, []common.Address{owner, {0x41, 0x99}})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err = iterateStateHistoryTailByOwner(ctx, cache, 2, 9, owner, func(*StateDomainChange) (bool, error) {
				if mode == "cancel" {
					cancel()
				}
				if mode == "error" {
					return false, errors.New("callback")
				}
				return mode != "early", nil
			})
			if mode == "cancel" && !errors.Is(err, context.Canceled) || (mode == "error" || mode == "corrupt") && err == nil {
				t.Fatal(err)
			}
			if len(cache.entries) != 0 || len(cache.rejected) != 0 || cache.used != 0 || cache.rows != 0 {
				t.Fatal("partial planned scan changed caches")
			}
		})
	}
}

func TestQueryTailPlanBudgetAndOwnerCap(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cache := WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
	absent := common.Address{0x41, 0x99}
	PlanStateHistoryQueryTailOwners(cache, []common.Address{owner, {0x41, 0x33}, absent})
	cache.budget = 512
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, owner, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if cache.used > cache.budget || cache.rows > cache.maxRows || !cache.rejected[owner] || cache.entries[absent] == nil {
		t.Fatal("dense owner prevented bounded absent-owner proof", cache.used, cache.rows)
	}
	// Unplanned reads may consume slots after registration. Planning must not
	// bypass the shared retained-owner cap when those owners are later requested.
	cache = WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
	PlanStateHistoryQueryTailOwners(cache, []common.Address{owner, absent})
	for i := 0; i < stateHistoryQueryTailOwners; i++ {
		who := common.Address{0x41, 0xa0, byte(i)}
		if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, who, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, owner, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries)+len(cache.rejected) > stateHistoryQueryTailOwners {
		t.Fatal("planned owners exceeded shared cap")
	}
}
