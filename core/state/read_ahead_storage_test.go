package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"golang.org/x/crypto/sha3"
	"google.golang.org/protobuf/types/known/anypb"
)

func testTRC20Calldata(selector [4]byte, addrs ...[20]byte) []byte {
	data := make([]byte, 4+32*(len(addrs)+1))
	copy(data[:4], selector[:])
	for i, a := range addrs {
		copy(data[4+32*i+12:4+32*(i+1)], a[:])
	}
	return data
}

func testMapSlot(key20 [20]byte, slot uint8) tcommon.Hash {
	h := sha3.NewLegacyKeccak256()
	var buf [64]byte
	return computeMappingSlotWithHasher(h, &buf, key20, slot)
}

func testNestedMapSlot(outer20, inner20 [20]byte, slot uint8) tcommon.Hash {
	h := sha3.NewLegacyKeccak256()
	var buf [64]byte
	outer := computeMappingSlotWithHasher(h, &buf, outer20, slot)
	return computeNestedMappingSlotWithHasher(h, &buf, inner20, outer)
}

func testTriggerBlock(t *testing.T, num int64, owner, contract tcommon.Address, data []byte) *types.Block {
	t.Helper()
	param, err := anypb.New(&contractpb.TriggerSmartContract{
		OwnerAddress:    owner.Bytes(),
		ContractAddress: contract.Bytes(),
		Data:            data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: num},
		},
		Transactions: []*corepb.Transaction{{
			RawData: &corepb.TransactionRaw{
				Contract: []*corepb.Transaction_Contract{{
					Type:      corepb.Transaction_Contract_TriggerSmartContract,
					Parameter: param,
				}},
			},
		}},
	})
}

func TestExtractTriggerStorageCall(t *testing.T) {
	owner := readAheadAddress(0x11)
	arg0 := [20]byte{0: 0xaa, 19: 0x01}
	arg1 := [20]byte{0: 0xbb, 19: 0x02}

	sel, caller, a0, a1, ok := ExtractTriggerStorageCall(owner[:], testTRC20Calldata(selectorERC20Transfer, arg0))
	if !ok || sel != selectorERC20Transfer || caller != [20]byte(owner[1:21]) || a0 != arg0 || a1 != ([20]byte{}) {
		t.Fatalf("transfer extract = (ok=%v sel=%x caller=%x a0=%x a1=%x)", ok, sel, caller, a0, a1)
	}

	sel, caller, a0, a1, ok = ExtractTriggerStorageCall(owner[:], testTRC20Calldata(selectorERC20TransferFrom, arg0, arg1))
	if !ok || sel != selectorERC20TransferFrom || caller != [20]byte(owner[1:21]) || a0 != arg0 || a1 != arg1 {
		t.Fatalf("transferFrom extract = (ok=%v sel=%x caller=%x a0=%x a1=%x)", ok, sel, caller, a0, a1)
	}

	if _, _, _, _, ok := ExtractTriggerStorageCall(owner[:], []byte{0xa9, 0x05, 0x9c, 0xbb}); ok {
		t.Fatal("short calldata accepted")
	}
}

func TestSlotLearnerLearnsAndBecomesUnlearnable(t *testing.T) {
	learner := newContractSlotLearner()
	contract := readAheadAddress(0x31)

	caller1 := [20]byte{19: 0x01}
	to1 := [20]byte{19: 0x02}
	caller2 := [20]byte{19: 0x03}
	to2 := [20]byte{19: 0x04}

	// Sample 1: transfer reads balance[caller] @ slot 2, balance[to] @ slot 2, and paused @ slot 10.
	if !learner.needsObservation(contract, 0, selectorERC20Transfer) {
		t.Fatal("expected initial observation needed")
	}
	if learned := learner.observe(contract, 0, selectorERC20Transfer, caller1, to1, [20]byte{}, []tcommon.Hash{
		{31: 10},
		testMapSlot(caller1, 2),
		testMapSlot(to1, 2),
		testMapSlot(caller1, 7), // spurious slot only present in tx1
	}); learned {
		t.Fatal("learned after only 1 sample")
	}
	if !learner.needsObservation(contract, 0, selectorERC20Transfer) {
		t.Fatal("expected second observation needed")
	}
	// Sample 2 confirms slot 2 + const slot 10, intersects away slot 7.
	if learned := learner.observe(contract, 0, selectorERC20Transfer, caller2, to2, [20]byte{}, []tcommon.Hash{
		{31: 10},
		testMapSlot(caller2, 2),
		testMapSlot(to2, 2),
	}); !learned {
		t.Fatal("expected template to be learned after 2 consistent samples")
	}
	if learner.needsObservation(contract, 0, selectorERC20Transfer) {
		t.Fatal("expected NeedsObservation=false once learned")
	}
	// Different generation still needs learning.
	if !learner.needsObservation(contract, 1, selectorERC20Transfer) {
		t.Fatal("generation bump must require new learning")
	}
	tpl, ok := learner.learnedTemplate(contract, 0, selectorERC20Transfer)
	if !ok {
		t.Fatal("missing learned template")
	}
	predicted := appendPredictedStorageSlots(nil, tpl, caller1, to1, [20]byte{})
	wantSet := map[tcommon.Hash]bool{
		testMapSlot(caller1, 2): false,
		testMapSlot(to1, 2):     false,
		{31: 10}:                false,
	}
	for _, s := range predicted {
		if _, exists := wantSet[s]; exists {
			wantSet[s] = true
		}
	}
	for s, seen := range wantSet {
		if !seen {
			t.Fatalf("predicted slots %x missing %x", predicted, s)
		}
	}

	// Non-standard contract transitions to unlearnable after 4 inconsistent samples.
	weird := readAheadAddress(0x32)
	for i := range 4 {
		learner.observe(weird, 0, selectorERC20Transfer, caller1, to1, [20]byte{}, []tcommon.Hash{{0: byte(i + 1), 31: 99}})
	}
	if learner.needsObservation(weird, 0, selectorERC20Transfer) {
		t.Fatal("expected unlearnable contract to stop observing after 4 misses")
	}

	// Test nested allowance slot learning for approve & transferFrom.
	if !learner.needsObservation(contract, 0, selectorERC20Approve) {
		t.Fatal("approve needs observation")
	}
	learner.observe(contract, 0, selectorERC20Approve, caller1, to1, [20]byte{}, []tcommon.Hash{testNestedMapSlot(caller1, to1, 3)})
	if !learner.observe(contract, 0, selectorERC20Approve, caller2, to2, [20]byte{}, []tcommon.Hash{testNestedMapSlot(caller2, to2, 3)}) {
		t.Fatal("expected approve nested slot to be learned")
	}
	approveTpl, ok := learner.learnedTemplate(contract, 0, selectorERC20Approve)
	if !ok {
		t.Fatal("missing approve template")
	}
	approvePredicted := appendPredictedStorageSlots(nil, approveTpl, caller1, to1, [20]byte{})
	if len(approvePredicted) != 1 || approvePredicted[0] != testNestedMapSlot(caller1, to1, 3) {
		t.Fatalf("approve predicted = %x, want [%x]", approvePredicted, testNestedMapSlot(caller1, to1, 3))
	}
}

func TestStateReadAheadPrefetchesLearnedTRC20Storage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		version   int32
		trxHash   []byte
		optimized bool
	}{
		{name: "legacy_v0", version: 0, trxHash: []byte{1, 2, 3}, optimized: false},
		{name: "legacy_v1_hashed_slot", version: 1, trxHash: []byte{4, 5, 6}, optimized: false},
		{name: "optimized_and_legacy", version: 0, trxHash: []byte{7, 8, 9}, optimized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			contract := readAheadAddress(0x61)
			owner1 := readAheadAddress(0x62)
			to1 := readAheadAddress(0x63)
			owner2 := readAheadAddress(0x64)
			to2 := readAheadAddress(0x65)

			meta := &contractpb.SmartContract{
				OriginAddress: owner1.Bytes(),
				TrxHash:       tc.trxHash,
				Version:       tc.version,
			}
			metaEncoded, err := statecodec.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
			contractEnv, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: 3}).Encode()
			if err != nil {
				t.Fatal(err)
			}
			ownerEnv, err := (&StateAccountV3{Version: StateAccountVersion}).Encode()
			if err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateAccountLatest(disk, contract, contractEnv); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateAccountLatest(disk, owner1, ownerEnv); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateAccountLatest(disk, owner2, ownerEnv); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateKVLatest(disk, contract, 3, kvdomains.ContractMetadata, contractMetaKVKey, metaEncoded); err != nil {
				t.Fatal(err)
			}

			// Pre-populate storage rows for tx2 (both legacy and/or optimized depending on mode).
			caller20_2 := [20]byte(owner2[1:21])
			to20_2 := [20]byte(to2[1:21])
			slotFrom := testMapSlot(caller20_2, 2)
			slotTo := testMapSlot(to20_2, 2)
			valFrom := tcommon.Hash{31: 0x11}
			valTo := tcommon.Hash{31: 0x22}

			legacyKeyFrom := LegacyStorageRowKey(contract, slotFrom, meta)
			if err := rawdb.WriteStateKVLatest(disk, contract, 3, kvdomains.ContractStorage, legacyKeyFrom[:], valFrom[:]); err != nil {
				t.Fatal(err)
			}
			if tc.optimized {
				optKeyTo := OptimizedStorageRowKey(contract, slotTo, meta)
				if err := rawdb.WriteStateKVLatest(disk, contract, 3, kvdomains.ContractStorage, optKeyTo, valTo[:]); err != nil {
					t.Fatal(err)
				}
			} else {
				legacyKeyTo := LegacyStorageRowKey(contract, slotTo, meta)
				if err := rawdb.WriteStateKVLatest(disk, contract, 3, kvdomains.ContractStorage, legacyKeyTo[:], valTo[:]); err != nil {
					t.Fatal(err)
				}
			}

			base := &readAheadCountingReader{KeyValueReader: disk}
			buffer := blockbuffer.New(base)
			buffer.SetBaseReadCacheSize(1 << 20)
			prefetcher := NewStateReadAhead(buffer, StateReadAheadConfig{
				Workers:             1,
				QueueBlocks:         4,
				QueueBytes:          1 << 20,
				UseOptimizedStorage: func() bool { return tc.optimized },
			})
			prefetcher.Start()
			defer prefetcher.Close()

			// Block 1 warms ContractMetadata into layoutCache while canonical execution learns slot 2.
			b1 := testTriggerBlock(t, 1, owner1, contract, testTRC20Calldata(selectorERC20Transfer, [20]byte(to1[1:21])))
			prefetcher.EnqueueBlocks([]*types.Block{b1})
			prefetcher.Wait()

			caller20_1 := [20]byte(owner1[1:21])
			to20_1 := [20]byte(to1[1:21])
			prefetcher.ObserveContractSlots(contract, 3, selectorERC20Transfer, caller20_1, to20_1, [20]byte{}, []tcommon.Hash{
				testMapSlot(caller20_1, 2),
				testMapSlot(to20_1, 2),
			})
			prefetcher.ObserveContractSlots(contract, 3, selectorERC20Transfer, caller20_2, to20_2, [20]byte{}, []tcommon.Hash{
				slotFrom,
				slotTo,
			})

			// Block 2 should now prefetch both sender & recipient storage slots via Stage 2.
			b2 := testTriggerBlock(t, 2, owner2, contract, testTRC20Calldata(selectorERC20Transfer, to20_2))
			prefetcher.EnqueueBlocks([]*types.Block{b2})
			prefetcher.Wait()

			stats := prefetcher.Stats()
			if stats.StorageLearnedContracts != 1 {
				t.Fatalf("StorageLearnedContracts = %d, want 1", stats.StorageLearnedContracts)
			}
			if stats.StorageRows == 0 || stats.StoragePresent < 2 {
				t.Fatalf("expected prefetched storage rows present >= 2, got stats = %+v", stats)
			}

			// Verify canonical reads now hit buffer cache with zero additional durable base reads!
			readsBeforeCanonical := base.gets.Load()
			if got, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, contract, 3, kvdomains.ContractStorage, legacyKeyFrom[:]); err != nil || !ok || tcommon.BytesToHash(got) != valFrom {
				t.Fatalf("prefetched legacy row = (%x, ok=%v, err=%v), want %x", got, ok, err, valFrom)
			}
			if tc.optimized {
				optKeyTo := OptimizedStorageRowKey(contract, slotTo, meta)
				if got, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, contract, 3, kvdomains.ContractStorage, optKeyTo); err != nil || !ok || tcommon.BytesToHash(got) != valTo {
					t.Fatalf("prefetched optimized row = (%x, ok=%v, err=%v), want %x", got, ok, err, valTo)
				}
			}
			if after := base.gets.Load(); after != readsBeforeCanonical {
				t.Fatalf("canonical storage reads missed cache after prefetch: before=%d after=%d", readsBeforeCanonical, after)
			}
		})
	}
}
