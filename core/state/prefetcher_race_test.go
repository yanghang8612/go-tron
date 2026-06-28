//go:build race

package state

import (
	"sync"
	"sync/atomic"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestStatePrefetcherConcurrentRawReadsAndMutations(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	contract := testAddr(0x90)
	origin := testAddr(0x91)
	slot := tcommon.Hash{0x01}
	rewardKey := []byte("reward")
	metaBytes := mustMarshalPrefetchContractMetadata(t, &contractpb.SmartContract{
		Version:       1,
		OriginAddress: origin.Bytes(),
		TrxHash:       []byte{0xaa, 0xbb, 0xcc},
	})
	meta := &contractpb.SmartContract{
		Version: 1,
		TrxHash: []byte{
			0xaa, 0xbb, 0xcc,
		},
	}
	storageKey := javaStorageRowKey(contract, slot, meta).Bytes()

	if err := rawdb.WriteStateKVGeneration(db, contract, 0); err != nil {
		t.Fatalf("WriteStateKVGeneration contract: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, origin, []byte("origin")); err != nil {
		t.Fatalf("WriteStateAccountLatest origin: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
		t.Fatalf("WriteStateKVLatest metadata: %v", err)
	}

	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 4, Queue: 256})
	p.Start()

	var done atomic.Bool
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if done.Load() {
					return
				}
				rawRewardKey := append([]byte(nil), rewardKey...)
				p.Enqueue([]PrefetchKey{
					AccountPrefetchKey(contract),
					{
						Kind:   PrefetchAccountKVLatest,
						Owner:  contract,
						Domain: kvdomains.SystemReward,
						Key:    rawRewardKey,
					},
					ContractCodePrefetchKey(contract),
					ContractOriginAccountPrefetchKey(contract),
					ContractStoragePrefetchKey(contract, slot),
				})
				rawRewardKey[0] ^= byte(worker + 1)
			}
		}(worker)
	}

	for i := 0; i < 200; i++ {
		code := []byte{0x60, byte(i), 0x56}
		codeHash := tcommon.Keccak256(code)
		account := &StateAccountV2{
			Version:  StateAccountVersion,
			CodeHash: codeHash,
		}
		accountBytes, err := account.Encode()
		if err != nil {
			t.Fatalf("Encode account: %v", err)
		}
		if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
			t.Fatalf("WriteStateCode: %v", err)
		}
		if err := rawdb.WriteStateAccountLatest(db, contract, accountBytes); err != nil {
			t.Fatalf("WriteStateAccountLatest contract: %v", err)
		}
		if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.SystemReward, rewardKey, []byte{byte(i)}); err != nil {
			t.Fatalf("WriteStateKVLatest reward: %v", err)
		}
		if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractStorage, storageKey, []byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatalf("WriteStateKVLatest storage: %v", err)
		}
		if _, _, err := rawdb.ReadStateAccountLatest(db, contract); err != nil {
			t.Fatalf("ReadStateAccountLatest contract: %v", err)
		}
		if _, _, err := rawdb.ReadStateKVLatest(db, contract, 0, kvdomains.SystemReward, rewardKey); err != nil {
			t.Fatalf("ReadStateKVLatest reward: %v", err)
		}
	}

	done.Store(true)
	p.Stop()
	wg.Wait()

	stats := p.Stats()
	if stats.Processed == 0 {
		t.Fatalf("stats = %+v, want processed prefetch work", stats)
	}
	if stats.Errors != 0 {
		t.Fatalf("stats = %+v, want no prefetch errors", stats)
	}
}
