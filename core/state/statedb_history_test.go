package state

import (
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// TestAccumulateHistory_DisabledIsNoOp asserts that with capture off the
// StateDB writes zero history rows even after multiple mutations — the
// zero-overhead promise for non-archive operators.
func TestAccumulateHistory_DisabledIsNoOp(t *testing.T) {
	sdb := newTestStateDB(t)
	// Default: historyEnabled = false.

	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{0x02})
	sdb.SetCode(addr, []byte{0xAB, 0xCD})

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 42, tcommon.Hash{0xCA, 0xFE}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	if rawdb.HasAccountDelta(buf, 42, addr) {
		t.Error("expected no sh-a- row when history disabled")
	}
	if rawdb.HasSlotDelta(buf, 42, addr, tcommon.Hash{0x01}) {
		t.Error("expected no sh-s- row when history disabled")
	}
	if rawdb.HasHistoryMeta(buf, 42) {
		t.Error("expected no sh-m- row when history disabled")
	}
	if rawdb.HasAddrInverse(buf, addr, 42) {
		t.Error("expected no sh-i-a- row when history disabled")
	}
	if rawdb.HasSlotInverse(buf, addr, tcommon.Hash{0x01}, 42) {
		t.Error("expected no sh-i-s- row when history disabled")
	}
}

// TestAccumulateHistory_BlobCapturesAccount verifies AccountProtoPre carries
// the FULL pre-block account proto — balance, nonce-equivalent state, asset
// map — exactly as it was before the block-level mutations.
func TestAccumulateHistory_BlobCapturesAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(2)

	// Seed pre-block state: balance, an asset entry, and an account name.
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 555)
	sdb.SetTRC10Balance(addr, 1001, 77)
	sdb.SetAccountName(addr, "pre-block-name")
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	// Begin "block" — flip the capture flag and mutate.
	sdb.SetHistoryEnabled(true)
	sdb.AddBalance(addr, 100) // 555 → 655
	sdb.SetTRC10Balance(addr, 1001, 999)
	sdb.SetAccountName(addr, "post-block-name")

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 10, tcommon.Hash{0xAB}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	delta := rawdb.ReadAccountDelta(buf, 10, addr)
	if delta == nil {
		t.Fatal("AccountDelta missing")
	}
	if !delta.ExistedPre {
		t.Fatal("ExistedPre should be true for pre-existing account")
	}
	var pre corepb.Account
	if err := proto.Unmarshal(delta.AccountProtoPre, &pre); err != nil {
		t.Fatalf("unmarshal AccountProtoPre: %v", err)
	}
	if pre.Balance != 555 {
		t.Errorf("captured Balance = %d, want 555 (pre-mutation)", pre.Balance)
	}
	if got := pre.AssetV2[strconv.FormatInt(1001, 10)]; got != 77 {
		t.Errorf("captured AssetV2[1001] = %d, want 77", got)
	}
	if string(pre.AccountName) != "pre-block-name" {
		t.Errorf("captured AccountName = %q, want pre-block-name", pre.AccountName)
	}
}

// TestAccumulateHistory_StorageFirstWriteSticks asserts only the FIRST
// pre-mutation slot value lands in sh-s-, even after multiple in-block
// writes to the same slot.
func TestAccumulateHistory_StorageFirstWriteSticks(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(3)
	slot := tcommon.HexToHash("0xdeadbeef")

	// Seed: slot already holds value A pre-block.
	sdb.GetOrCreateAccount(addr)
	sdb.SetState(addr, slot, tcommon.HexToHash("0xAAAA"))
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	sdb.SetHistoryEnabled(true)
	// Pre-warm the in-memory storage cache from disk so the journal
	// records the real pre-block value, not a zero. opSstore in the VM
	// always pre-warms via opSload; tests must mirror that contract.
	_ = sdb.GetState(addr, slot)

	// Two writes in the same block: A → B → C. Only A should land.
	sdb.SetState(addr, slot, tcommon.HexToHash("0xBBBB"))
	sdb.SetState(addr, slot, tcommon.HexToHash("0xCCCC"))

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 20, tcommon.Hash{0xBE, 0xEF}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	got, ok := rawdb.ReadSlotDelta(buf, 20, addr, slot)
	if !ok {
		t.Fatal("SlotDelta missing")
	}
	if got != tcommon.HexToHash("0xAAAA") {
		t.Errorf("captured slot pre-value = %s, want 0xAAAA (first overwrite)", got.Hex())
	}
}

// TestAccumulateHistory_StorageEmptyPreValue verifies the zero-hash sentinel
// path: a slot that was empty pre-block must show up with the "found,
// zero-hash" reading from ReadSlotDelta.
func TestAccumulateHistory_StorageEmptyPreValue(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(4)
	slot := tcommon.HexToHash("0xfeed")

	sdb.GetOrCreateAccount(addr)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	sdb.SetHistoryEnabled(true)
	// First write to a previously-empty slot.
	sdb.SetState(addr, slot, tcommon.HexToHash("0x1234"))

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 30, tcommon.Hash{0xC0, 0xDE}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	got, ok := rawdb.ReadSlotDelta(buf, 30, addr, slot)
	if !ok {
		t.Fatal("SlotDelta missing for previously-empty slot")
	}
	if got != (tcommon.Hash{}) {
		t.Errorf("captured slot pre-value = %s, want zero hash", got.Hex())
	}
}

// TestAccumulateHistory_InverseIndexWritten confirms both sh-i-a- and
// sh-i-s- rows land for every touched addr/slot.
func TestAccumulateHistory_InverseIndexWritten(t *testing.T) {
	sdb := newTestStateDB(t)
	addrA := testAddr(5)
	addrB := testAddr(6)
	slot := tcommon.HexToHash("0xa1")

	sdb.GetOrCreateAccount(addrA)
	sdb.GetOrCreateAccount(addrB)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	sdb.SetHistoryEnabled(true)
	sdb.AddBalance(addrA, 1)
	sdb.AddBalance(addrB, 2)
	sdb.SetState(addrA, slot, tcommon.HexToHash("0x99"))

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 40, tcommon.Hash{0xAA}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	if !rawdb.HasAddrInverse(buf, addrA, 40) {
		t.Error("sh-i-a- for addrA missing")
	}
	if !rawdb.HasAddrInverse(buf, addrB, 40) {
		t.Error("sh-i-a- for addrB missing")
	}
	if !rawdb.HasSlotInverse(buf, addrA, slot, 40) {
		t.Error("sh-i-s- for (addrA, slot) missing")
	}
}

// TestAccumulateHistory_MetaCorrect asserts the per-block manifest counts
// touched addrs and total slots correctly.
func TestAccumulateHistory_MetaCorrect(t *testing.T) {
	sdb := newTestStateDB(t)
	addrA := testAddr(7)
	addrB := testAddr(8)

	sdb.GetOrCreateAccount(addrA)
	sdb.GetOrCreateAccount(addrB)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	sdb.SetHistoryEnabled(true)
	sdb.AddBalance(addrA, 1)
	sdb.AddBalance(addrB, 2)
	sdb.SetState(addrA, tcommon.Hash{0x01}, tcommon.Hash{0x11})
	sdb.SetState(addrA, tcommon.Hash{0x02}, tcommon.Hash{0x22})
	sdb.SetState(addrB, tcommon.Hash{0x03}, tcommon.Hash{0x33})

	buf := memorydb.New()
	blockHash := tcommon.Hash{0xDE, 0xAD}
	if err := sdb.AccumulateHistory(buf, 55, blockHash); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	meta := rawdb.ReadHistoryMeta(buf, 55)
	if meta == nil {
		t.Fatal("StateHistoryMeta missing")
	}
	if meta.BlockNum != 55 {
		t.Errorf("BlockNum = %d, want 55", meta.BlockNum)
	}
	if string(meta.BlockHash) != string(blockHash.Bytes()) {
		t.Errorf("BlockHash mismatch: got %x", meta.BlockHash)
	}
	if meta.NumAddrs != 2 {
		t.Errorf("NumAddrs = %d, want 2", meta.NumAddrs)
	}
	if meta.NumSlots != 3 {
		t.Errorf("NumSlots = %d, want 3", meta.NumSlots)
	}
	if meta.SchemaVer != rawdb.HistorySchemaVersion {
		t.Errorf("SchemaVer = %d, want %d", meta.SchemaVer, rawdb.HistorySchemaVersion)
	}
}

// TestAccumulateHistory_NewAccountHasNoSnap asserts that an account created
// in this block writes an AccountDelta with ExistedPre=false and empty
// pre-image blobs — the row is still emitted so readers can distinguish
// "created" from "untouched".
func TestAccumulateHistory_NewAccountHasNoSnap(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(9)

	sdb.SetHistoryEnabled(true)
	sdb.GetOrCreateAccount(addr) // FIRST mutation creates the account.
	sdb.AddBalance(addr, 42)     // Adds another accountChange entry; ignored by first-seen.

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 60, tcommon.Hash{0xBB}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	delta := rawdb.ReadAccountDelta(buf, 60, addr)
	if delta == nil {
		t.Fatal("AccountDelta missing for newly-created account")
	}
	if delta.ExistedPre {
		t.Error("ExistedPre should be false for an account created this block")
	}
	if len(delta.AccountProtoPre) != 0 {
		t.Errorf("AccountProtoPre should be empty, got %d bytes", len(delta.AccountProtoPre))
	}
	if len(delta.CodePre) != 0 {
		t.Errorf("CodePre should be empty for new account, got %d bytes", len(delta.CodePre))
	}
	if len(delta.ContractMetaPre) != 0 {
		t.Errorf("ContractMetaPre should be empty for new account, got %d bytes", len(delta.ContractMetaPre))
	}
}

// TestAccumulateHistory_CodeAndContractMetaCaptured verifies that the
// pre-block code and ContractMeta survive into AccountDelta when a contract
// is mutated by SetCode / SetContract.
func TestAccumulateHistory_CodeAndContractMetaCaptured(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(10)

	// Seed: contract has pre-block code and contract metadata.
	sdb.GetOrCreateAccount(addr)
	sdb.SetCode(addr, []byte{0x60, 0x01, 0x60, 0x02})
	sdb.SetContract(addr, &contractpb.SmartContract{Name: "pre", ConsumeUserResourcePercent: 50})
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	sdb.SetHistoryEnabled(true)
	// In-block: overwrite code and meta.
	sdb.SetCode(addr, []byte{0xAA, 0xBB})
	sdb.SetContract(addr, &contractpb.SmartContract{Name: "post", ConsumeUserResourcePercent: 100})

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 70, tcommon.Hash{0xCC}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	delta := rawdb.ReadAccountDelta(buf, 70, addr)
	if delta == nil {
		t.Fatal("AccountDelta missing")
	}
	if string(delta.CodePre) != string([]byte{0x60, 0x01, 0x60, 0x02}) {
		t.Errorf("CodePre = %x, want 60016002", delta.CodePre)
	}
	if len(delta.ContractMetaPre) == 0 {
		t.Fatal("ContractMetaPre is empty")
	}
	var preMeta contractpb.SmartContract
	if err := proto.Unmarshal(delta.ContractMetaPre, &preMeta); err != nil {
		t.Fatalf("unmarshal ContractMetaPre: %v", err)
	}
	if preMeta.Name != "pre" || preMeta.ConsumeUserResourcePercent != 50 {
		t.Errorf("ContractMetaPre = %+v, want pre/50", &preMeta)
	}
}

// BenchmarkAccumulateHistory_Disabled measures the cost of calling
// AccumulateHistory after a typical block of mutations when capture is
// off. The body has to return on the first bool check; the only steady
// cost is the function-call epilogue. Used to verify the zero-overhead
// promise in CI spot-checks.
func BenchmarkAccumulateHistory_Disabled(b *testing.B) {
	sdb := newTestStateDB(&testing.T{})
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{0x02})
	buf := memorydb.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sdb.AccumulateHistory(buf, uint64(i), tcommon.Hash{})
	}
}

// TestAccumulateHistory_RevertedMutationNotCaptured verifies that journal
// truncation by RevertToSnapshot causes the reverted mutation to be invisible
// to AccumulateHistory — the row is never written.
func TestAccumulateHistory_RevertedMutationNotCaptured(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(11)

	sdb.SetHistoryEnabled(true)
	snap := sdb.Snapshot()
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 9999)
	sdb.RevertToSnapshot(snap)

	buf := memorydb.New()
	if err := sdb.AccumulateHistory(buf, 80, tcommon.Hash{0xDD}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	if rawdb.HasAccountDelta(buf, 80, addr) {
		t.Error("reverted account should not appear in history")
	}
	if !rawdb.HasHistoryMeta(buf, 80) {
		t.Error("StateHistoryMeta should still land even for zero-touch blocks")
	}
	meta := rawdb.ReadHistoryMeta(buf, 80)
	if meta.NumAddrs != 0 || meta.NumSlots != 0 {
		t.Errorf("counts after full revert: addrs=%d slots=%d, want 0/0", meta.NumAddrs, meta.NumSlots)
	}
}
