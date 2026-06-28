package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestStatePrefetcherWarmsRawLatestRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x44)
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatalf("WriteStateAccountLatest: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatalf("WriteStateKVGeneration: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemReward, []byte("reward"), []byte("value")); err != nil {
		t.Fatalf("WriteStateKVLatest reward: %v", err)
	}

	slot := tcommon.Hash{0x01}
	meta := &contractpb.SmartContract{
		Version: 1,
		TrxHash: []byte{
			0xaa, 0xbb, 0xcc,
		},
	}
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
		t.Fatalf("WriteStateKVLatest metadata: %v", err)
	}
	storageRowKey := javaStorageRowKey(owner, slot, meta)
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, storageRowKey.Bytes(), []byte("storage")); err != nil {
		t.Fatalf("WriteStateKVLatest storage: %v", err)
	}

	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 2, Queue: 8})
	p.Start()
	if accepted := p.Enqueue([]PrefetchKey{
		AccountPrefetchKey(owner),
		AccountKVPrefetchKey(owner, kvdomains.SystemReward, []byte("reward")),
		ContractStoragePrefetchKey(owner, slot),
	}); accepted != 3 {
		t.Fatalf("accepted = %d, want 3", accepted)
	}
	p.Stop()

	stats := p.Stats()
	if stats.Enqueued != 3 || stats.Processed != 3 || stats.Hits != 3 || stats.Misses != 0 || stats.Errors != 0 || stats.Dropped != 0 {
		t.Fatalf("stats = %+v, want 3 hits and no misses/errors/drops", stats)
	}
}

func TestStatePrefetcherWarmsContractCodeFromAccountEnvelope(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x49)
	code := []byte{0x60, 0x00, 0x56}
	codeHash := tcommon.Keccak256(code)
	account := &StateAccountV2{
		Version:  StateAccountVersion,
		CodeHash: codeHash,
	}
	accountBytes, err := account.Encode()
	if err != nil {
		t.Fatalf("Encode account: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, owner, accountBytes); err != nil {
		t.Fatalf("WriteStateAccountLatest: %v", err)
	}
	if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
		t.Fatalf("WriteStateCode: %v", err)
	}

	hit, err := prefetchLatest(db, ContractCodePrefetchKey(owner))
	if err != nil {
		t.Fatalf("prefetch contract code: %v", err)
	}
	if !hit {
		t.Fatal("prefetch contract code hit = false, want true")
	}
}

func TestStatePrefetcherContractCodeMissesWithoutChangingSemantics(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, ethdb.KeyValueWriter, tcommon.Address)
	}{
		{
			name: "missing account envelope",
		},
		{
			name: "malformed account envelope",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, owner tcommon.Address) {
				t.Helper()
				if err := rawdb.WriteStateAccountLatest(db, owner, []byte("not-state-account-v2")); err != nil {
					t.Fatalf("WriteStateAccountLatest malformed: %v", err)
				}
			},
		},
		{
			name: "zero code hash",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, owner tcommon.Address) {
				t.Helper()
				account := &StateAccountV2{Version: StateAccountVersion}
				accountBytes, err := account.Encode()
				if err != nil {
					t.Fatalf("Encode account: %v", err)
				}
				if err := rawdb.WriteStateAccountLatest(db, owner, accountBytes); err != nil {
					t.Fatalf("WriteStateAccountLatest zero code hash: %v", err)
				}
			},
		},
		{
			name: "missing code row",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, owner tcommon.Address) {
				t.Helper()
				account := &StateAccountV2{
					Version:  StateAccountVersion,
					CodeHash: tcommon.Keccak256([]byte{0x60, 0x01}),
				}
				accountBytes, err := account.Encode()
				if err != nil {
					t.Fatalf("Encode account: %v", err)
				}
				if err := rawdb.WriteStateAccountLatest(db, owner, accountBytes); err != nil {
					t.Fatalf("WriteStateAccountLatest missing code row: %v", err)
				}
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			owner := testAddr(byte(0x50 + i))
			if tt.setup != nil {
				tt.setup(t, db, owner)
			}
			hit, err := prefetchLatest(db, ContractCodePrefetchKey(owner))
			if err != nil {
				t.Fatalf("prefetch contract code: %v", err)
			}
			if hit {
				t.Fatal("prefetch contract code hit = true, want false")
			}
		})
	}
}

func TestStatePrefetcherWarmsContractOriginAccountFromMetadata(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	contract := testAddr(0x60)
	origin := testAddr(0x61)
	metaBytes := mustMarshalPrefetchContractMetadata(t, &contractpb.SmartContract{
		OriginAddress: origin.Bytes(),
	})
	if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
		t.Fatalf("WriteStateKVLatest metadata: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, origin, []byte("origin-account")); err != nil {
		t.Fatalf("WriteStateAccountLatest origin: %v", err)
	}

	hit, err := prefetchLatest(db, ContractOriginAccountPrefetchKey(contract))
	if err != nil {
		t.Fatalf("prefetch contract origin account: %v", err)
	}
	if !hit {
		t.Fatal("prefetch contract origin account hit = false, want true")
	}
}

func TestStatePrefetcherWarmsGovernanceWitnessRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	witness := testAddr(0x62)
	voter := testAddr(0x63)
	keys := []PrefetchKey{
		WitnessCapsulePrefetchKey(witness),
		WitnessBrokeragePrefetchKey(witness),
		WitnessIndexPrefetchKey(),
		ProposalPrefetchKey(7),
		ProposalIndexPrefetchKey(),
		PendingVotesPrefetchKey(voter),
		PendingVotesIndexPrefetchKey(),
	}
	for i, key := range keys {
		if err := rawdb.WriteStateKVLatest(db, key.Owner, 0, key.Domain, key.Key, []byte{byte(i + 1)}); err != nil {
			t.Fatalf("WriteStateKVLatest key %d: %v", i, err)
		}
	}
	for _, key := range keys {
		hit, err := prefetchLatest(db, key)
		if err != nil {
			t.Fatalf("prefetch governance/witness key %#v: %v", key, err)
		}
		if !hit {
			t.Fatalf("prefetch governance/witness key %#v hit = false, want true", key)
		}
	}
}

func TestStatePrefetcherContractOriginAccountMissesWithoutChangingSemantics(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, ethdb.KeyValueWriter, tcommon.Address)
	}{
		{
			name: "missing metadata",
		},
		{
			name: "malformed metadata",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, contract tcommon.Address) {
				t.Helper()
				if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0xff}); err != nil {
					t.Fatalf("WriteStateKVLatest malformed metadata: %v", err)
				}
			},
		},
		{
			name: "invalid origin address",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, contract tcommon.Address) {
				t.Helper()
				metaBytes := mustMarshalPrefetchContractMetadata(t, &contractpb.SmartContract{
					OriginAddress: []byte{tcommon.AddressPrefixMainnet, 0x01},
				})
				if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
					t.Fatalf("WriteStateKVLatest invalid origin metadata: %v", err)
				}
			},
		},
		{
			name: "missing origin account",
			setup: func(t *testing.T, db ethdb.KeyValueWriter, contract tcommon.Address) {
				t.Helper()
				metaBytes := mustMarshalPrefetchContractMetadata(t, &contractpb.SmartContract{
					OriginAddress: testAddr(0x6f).Bytes(),
				})
				if err := rawdb.WriteStateKVLatest(db, contract, 0, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
					t.Fatalf("WriteStateKVLatest missing origin metadata: %v", err)
				}
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			contract := testAddr(byte(0x70 + i))
			if tt.setup != nil {
				tt.setup(t, db, contract)
			}
			hit, err := prefetchLatest(db, ContractOriginAccountPrefetchKey(contract))
			if err != nil {
				t.Fatalf("prefetch contract origin account: %v", err)
			}
			if hit {
				t.Fatal("prefetch contract origin account hit = true, want false")
			}
		})
	}
}

func TestStatePrefetcherDropsWhenQueueFull(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x45)
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 1})

	accepted := p.Enqueue([]PrefetchKey{
		AccountPrefetchKey(owner),
		AccountPrefetchKey(testAddr(0x46)),
	})
	p.Stop()

	stats := p.Stats()
	if accepted != 1 || stats.Enqueued != 1 || stats.Dropped != 1 || stats.Processed != 0 {
		t.Fatalf("accepted=%d stats=%+v, want one queued, one dropped, none processed before Start", accepted, stats)
	}
}

func TestStatePrefetcherRecordsErrors(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x47)
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 2})
	p.Start()
	p.Enqueue([]PrefetchKey{
		AccountKVPrefetchKey(owner, kvdomains.KVDomain(0xffff), []byte("bad-domain")),
	})
	p.Stop()

	stats := p.Stats()
	if stats.Processed != 1 || stats.Errors != 1 || stats.Hits != 0 {
		t.Fatalf("stats = %+v, want one processed error", stats)
	}
}

func TestStatePrefetcherStopIsIdempotentAndDropsAfterStop(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 2})
	p.Start()
	p.Stop()
	p.Stop()

	if accepted := p.Enqueue([]PrefetchKey{AccountPrefetchKey(testAddr(0x48))}); accepted != 0 {
		t.Fatalf("accepted after Stop = %d, want 0", accepted)
	}
	if stats := p.Stats(); stats.Dropped != 1 {
		t.Fatalf("stats after enqueue stopped = %+v, want one drop", stats)
	}
}

func mustMarshalPrefetchContractMetadata(t *testing.T, meta *contractpb.SmartContract) []byte {
	t.Helper()
	data, err := proto.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	return data
}
