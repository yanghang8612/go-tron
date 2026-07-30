package vm

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

type recordingRuntimePrefetcher struct {
	keys []state.PrefetchKey
}

func (p *recordingRuntimePrefetcher) Enqueue(keys []state.PrefetchKey) int {
	p.keys = append(p.keys, keys...)
	return len(keys)
}

func TestTVMRuntimePrefetchesCallTargetCodeAndMetadata(t *testing.T) {
	p := &recordingRuntimePrefetcher{}
	evm := newTestEVMWithConfig(t, TVMConfig{RuntimePrefetcher: p})
	caller := tcommon.Address{0x41, 0x01}
	target := tcommon.Address{0x41, 0x02}

	if _, _, err := evm.Call(caller, target, nil, 100_000, 0); err != nil {
		t.Fatalf("Call: %v", err)
	}

	assertRuntimePrefetchHas(t, p.keys, state.ContractCodePrefetchKey(target))
	assertRuntimePrefetchHas(t, p.keys, state.ContractMetadataPrefetchKey(target))
}

func TestTVMRuntimePrefetchesStorageSlot(t *testing.T) {
	p := &recordingRuntimePrefetcher{}
	evm := newTestEVMWithConfig(t, TVMConfig{RuntimePrefetcher: p})
	contractAddr := tcommon.Address{0x41, 0x03}
	slot := tcommon.Hash{31: 0x07}
	code := []byte{
		byte(PUSH1), 0x07,
		byte(SLOAD),
		byte(STOP),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, contractAddr, 0, 100_000)
	contract.SetCode(contractAddr, code)

	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRuntimePrefetchHas(t, p.keys, state.ContractStoragePrefetchKey(contractAddr, slot))
}

func assertRuntimePrefetchHas(t *testing.T, keys []state.PrefetchKey, want state.PrefetchKey) {
	t.Helper()
	for _, got := range keys {
		if runtimePrefetchKeyEqual(got, want) {
			return
		}
	}
	t.Fatalf("runtime prefetch keys missing %+v in %+v", want, keys)
}

func runtimePrefetchKeyEqual(a, b state.PrefetchKey) bool {
	return a.Kind == b.Kind &&
		a.Owner == b.Owner &&
		a.Domain == b.Domain &&
		a.Slot == b.Slot &&
		a.Generation == b.Generation &&
		a.HasGeneration == b.HasGeneration &&
		bytes.Equal(a.Key, b.Key)
}
