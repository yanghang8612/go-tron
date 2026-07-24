package domains

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestOverlayPutDeleteAndParentReadThrough(t *testing.T) {
	owner := testAddress(0x21)
	parent := NewMemoryStore()
	if err := parent.DomainPut(owner, kvdomains.SystemDynamicProperty, []byte("k"), []byte("parent")); err != nil {
		t.Fatal(err)
	}
	overlay := NewOverlay(parent)

	got, ok, err := overlay.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "parent" {
		t.Fatalf("parent read-through = %q ok=%v err=%v", got, ok, err)
	}
	if err := overlay.DomainPut(owner, kvdomains.SystemDynamicProperty, []byte("k"), []byte("overlay")); err != nil {
		t.Fatal(err)
	}
	got, ok, err = overlay.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "overlay" {
		t.Fatalf("overlay put = %q ok=%v err=%v", got, ok, err)
	}
	if err := overlay.DomainDel(owner, kvdomains.SystemDynamicProperty, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if got, ok, err = overlay.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || ok {
		t.Fatalf("overlay delete = %q ok=%v err=%v", got, ok, err)
	}
}

func TestOverlayPrefixDeleteOrdersAgainstExactWrites(t *testing.T) {
	owner := testAddress(0x22)
	parent := NewMemoryStore()
	_ = parent.DomainPut(owner, kvdomains.SystemDelegation, []byte("aa/1"), []byte("parent-1"))
	_ = parent.DomainPut(owner, kvdomains.SystemDelegation, []byte("bb/1"), []byte("parent-2"))
	overlay := NewOverlay(parent)

	if err := overlay.DomainPut(owner, kvdomains.SystemDelegation, []byte("aa/new-before"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := overlay.DomainDelPrefix(owner, kvdomains.SystemDelegation, []byte("aa/")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("aa/1")); err != nil || ok {
		t.Fatalf("prefix delete should hide parent key: ok=%v err=%v", ok, err)
	}
	if _, ok, err := overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("aa/new-before")); err != nil || ok {
		t.Fatalf("prefix delete should hide earlier overlay key: ok=%v err=%v", ok, err)
	}
	got, ok, err := overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("bb/1"))
	if err != nil || !ok || string(got) != "parent-2" {
		t.Fatalf("unmatched parent key = %q ok=%v err=%v", got, ok, err)
	}

	if err := overlay.DomainPut(owner, kvdomains.SystemDelegation, []byte("aa/new-after"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	got, ok, err = overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("aa/new-after"))
	if err != nil || !ok || string(got) != "new" {
		t.Fatalf("post-prefix put = %q ok=%v err=%v", got, ok, err)
	}
}

func TestOverlayFlushAndDiscard(t *testing.T) {
	owner := testAddress(0x23)
	dst := NewMemoryStore()
	overlay := NewOverlay(nil)
	_ = overlay.DomainPut(owner, kvdomains.SystemReward, []byte("cycle/1"), []byte("one"))
	_ = overlay.DomainPut(owner, kvdomains.SystemReward, []byte("cycle/2"), []byte("two"))
	_ = overlay.DomainDelPrefix(owner, kvdomains.SystemReward, []byte("cycle/1"))

	if err := overlay.FlushTo(dst); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := dst.GetLatest(owner, kvdomains.SystemReward, []byte("cycle/1")); err != nil || ok {
		t.Fatalf("flushed prefix delete left cycle/1: ok=%v err=%v", ok, err)
	}
	got, ok, err := dst.GetLatest(owner, kvdomains.SystemReward, []byte("cycle/2"))
	if err != nil || !ok || string(got) != "two" {
		t.Fatalf("flushed cycle/2 = %q ok=%v err=%v", got, ok, err)
	}
	if len(overlay.Mutations()) != 0 {
		t.Fatal("FlushTo must clear overlay mutations")
	}

	_ = overlay.DomainPut(owner, kvdomains.SystemReward, []byte("x"), []byte("y"))
	overlay.Discard()
	if _, ok, err := overlay.GetLatest(owner, kvdomains.SystemReward, []byte("x")); err != nil || ok {
		t.Fatalf("discarded overlay key visible: ok=%v err=%v", ok, err)
	}
}

func TestOverlayHooksAndMetrics(t *testing.T) {
	owner := testAddress(0x24)
	var mutations []Mutation
	var gets []GetEvent
	overlay := NewOverlay(NewMemoryStore(), WithHooks(Hooks{
		OnMutation: func(m Mutation) { mutations = append(mutations, m) },
		OnGetLatest: func(e GetEvent) {
			gets = append(gets, e)
		},
	}))
	if err := overlay.DomainPut(owner, kvdomains.ContractABI, []byte("abi"), []byte{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := overlay.GetLatest(owner, kvdomains.ContractABI, []byte("abi"))
	if err != nil || !ok || len(got) != 0 {
		t.Fatalf("empty value should be present: got=%x ok=%v err=%v", got, ok, err)
	}
	if len(mutations) != 1 || mutations[0].Kind != MutationPut {
		t.Fatalf("mutation hook = %+v", mutations)
	}
	if len(gets) != 1 || gets[0].Source != GetSourceOverlay || !gets[0].Found {
		t.Fatalf("get hook = %+v", gets)
	}
	metrics := overlay.Metrics()
	if metrics.Gets != 1 || metrics.Puts != 1 || metrics.OverlayHits != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}

	mutations[0].Key[0] = 'x'
	if bytes.Equal(overlay.Mutations()[0].Key, mutations[0].Key) {
		t.Fatal("mutation hook leaked mutable overlay key storage")
	}
}

func TestOverlayOwnsMutationInputs(t *testing.T) {
	owner := testAddress(0x26)
	overlay := NewOverlay(nil)
	key := []byte("key")
	value := []byte("value")
	if err := overlay.DomainPut(owner, kvdomains.ContractStorage, key, value); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	value[0] = 'x'

	got, ok, err := overlay.GetLatest(owner, kvdomains.ContractStorage, []byte("key"))
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("owned mutation = %q ok=%v err=%v", got, ok, err)
	}
	mutations := overlay.Mutations()
	if len(mutations) != 1 || string(mutations[0].Key) != "key" || string(mutations[0].Value) != "value" {
		t.Fatalf("mutations = %+v", mutations)
	}
}

type ownedFlushWriter struct {
	key   []byte
	value []byte
}

func (*ownedFlushWriter) DomainPut(common.Address, kvdomains.KVDomain, []byte, []byte) error {
	return errors.New("unexpected defensive put")
}
func (*ownedFlushWriter) DomainDel(common.Address, kvdomains.KVDomain, []byte) error {
	return errors.New("unexpected defensive delete")
}
func (*ownedFlushWriter) DomainDelPrefix(common.Address, kvdomains.KVDomain, []byte) error {
	return nil
}
func (w *ownedFlushWriter) DomainPutOwned(_ common.Address, _ kvdomains.KVDomain, key, value []byte) error {
	w.key, w.value = key, value
	return nil
}
func (*ownedFlushWriter) DomainDelOwned(common.Address, kvdomains.KVDomain, []byte) error {
	return nil
}

func TestOverlayFlushTransfersOwnedMutationStorage(t *testing.T) {
	overlay := NewOverlay(nil)
	if err := overlay.DomainPut(testAddress(0x29), kvdomains.ContractStorage, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	view := overlay.mutationsView()[0]
	writer := new(ownedFlushWriter)
	if err := overlay.FlushTo(writer); err != nil {
		t.Fatal(err)
	}
	if &writer.key[0] != &view.Key[0] || &writer.value[0] != &view.Value[0] {
		t.Fatal("FlushTo cloned storage before owned writer")
	}
}

func TestOverlayFlushTransferPreservesPublicInputOwnership(t *testing.T) {
	overlay := NewOverlay(nil)
	key := []byte("public-key")
	value := []byte("public-value")
	if err := overlay.DomainPut(testAddress(0x2a), kvdomains.ContractStorage, key, value); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	value[0] = 'X'
	writer := new(ownedFlushWriter)
	if err := overlay.FlushTo(writer); err != nil {
		t.Fatal(err)
	}
	if string(writer.key) != "public-key" || string(writer.value) != "public-value" {
		t.Fatalf("public mutation input leaked through ownership transfer: key=%q value=%q", writer.key, writer.value)
	}
}

func TestOverlayRejectsUnregisteredDomain(t *testing.T) {
	owner := testAddress(0x25)
	overlay := NewOverlay(nil)
	if err := overlay.DomainPut(owner, kvdomains.KVDomain(0x0099), []byte("k"), []byte("v")); err == nil {
		t.Fatal("unregistered domain accepted")
	}
	if _, _, err := overlay.GetLatest(owner, kvdomains.KVDomain(0x0099), []byte("k")); err == nil {
		t.Fatal("unregistered domain read accepted")
	}
}
