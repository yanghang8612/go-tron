package rawdb

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStatePrefetchPlanBoundDedupAndLogicalValues(t *testing.T) {
	db := NewMemoryDatabase()
	owner := common.BytesToAddress([]byte{0x41, 1})
	if err := WriteStateAccountLatest(db, owner, []byte("envelope")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVLatest(db, owner, 7, kvdomains.AccountResourceAux, []byte{0}, []byte("resource")); err != nil {
		t.Fatal(err)
	}
	plan := NewStatePrefetchPlan(3)
	first := plan.AddAccount(owner)
	second := plan.AddAccount(owner)
	if first != 0 || second != first {
		t.Fatal("account dedup")
	}
	if plan.AddKV(owner, 7, kvdomains.AccountResourceAux, []byte{0}) != 1 {
		t.Fatal("KV ID")
	}
	if plan.AddKV(owner, 8, kvdomains.AccountResourceAux, []byte{0}) != 2 {
		t.Fatal("generation not distinguished")
	}
	if plan.AddCode(common.Hash{1}) != -1 || plan.AddCode(common.Hash{}) != -1 {
		t.Fatal("plan limit or zero code hash")
	}
	if plan.AddAccount(owner) != 0 {
		t.Fatal("dedup at capacity")
	}
	seen := 0
	err := plan.Execute(db, func(i int, value []byte, present bool, err error) error {
		seen++
		want := []string{"envelope", "resource", ""}[i]
		if err != nil || string(value) != want || present != (i != 2) {
			t.Fatalf("row %d: %q/%v/%v", i, value, present, err)
		}
		return nil
	})
	if err != nil || seen != 3 {
		t.Fatalf("execute = %v visits=%d", err, seen)
	}
	if err := db.Put(stateKVLatestKey(owner, 7, kvdomains.AccountResourceAux, []byte{0}), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	var decodeError bool
	if err := plan.Execute(db, func(i int, _ []byte, present bool, err error) error {
		if i == 1 {
			decodeError = err != nil && !present
		}
		return nil
	}); err != nil || !decodeError {
		t.Fatalf("corrupt KV accepted: %v", err)
	}
}

type reversePlanReader struct{ ethdb.KeyValueReader }

func (r reversePlanReader) PrefetchBatch(keys [][]byte, visit func(int, []byte, bool, error) error) error {
	for i := len(keys) - 1; i >= 0; i-- {
		value, present, err := readStatePresentNoCopy(r.KeyValueReader, keys[i], "test")
		if err := visit(i, value, present, err); err != nil {
			return err
		}
	}
	return nil
}

func TestStatePrefetchPlanUnorderedCallbacksAndCancellation(t *testing.T) {
	db := NewMemoryDatabase()
	plan := NewStatePrefetchPlan(4)
	for i := 0; i < 4; i++ {
		plan.AddCode(common.Hash{byte(i + 1)})
	}
	stop := errors.New("cancel")
	var ids []int
	err := plan.Execute(reversePlanReader{db}, func(i int, _ []byte, _ bool, _ error) error {
		ids = append(ids, i)
		if len(ids) == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || len(ids) != 2 || ids[0] != 3 || ids[1] != 2 {
		t.Fatalf("visit order/cancel = %v/%v", ids, err)
	}
}
