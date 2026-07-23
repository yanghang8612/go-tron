package state

import (
	"bytes"
	"strconv"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var (
	benchmarkAccountBytes []byte
	benchmarkAccountProto *corepb.Account
)

func newMapRichJournalAccount(addr tcommon.Address, entries int) *stateObject {
	asset := make(map[string]int64, entries)
	assetV2 := make(map[string]int64, entries)
	latest := make(map[string]int64, entries)
	latestV2 := make(map[string]int64, entries)
	freeUsage := make(map[string]int64, entries)
	freeUsageV2 := make(map[string]int64, entries)
	for i := 0; i < entries; i++ {
		legacyKey := "asset-" + strconv.Itoa(i)
		v2Key := strconv.Itoa(1_000_000 + i)
		value := int64(i + 1)
		asset[legacyKey] = value
		assetV2[v2Key] = value
		latest[legacyKey] = value * 10
		latestV2[v2Key] = value * 10
		freeUsage[legacyKey] = value * 100
		freeUsageV2[v2Key] = value * 100
	}
	account := types.NewAccountFromPB(&corepb.Account{
		Address:                    addr.Bytes(),
		Balance:                    1_000_000_000,
		Asset:                      asset,
		AssetV2:                    assetV2,
		LatestAssetOperationTime:   latest,
		LatestAssetOperationTimeV2: latestV2,
		FreeAssetNetUsage:          freeUsage,
		FreeAssetNetUsageV2:        freeUsageV2,
	})
	obj := newStateObject(addr, account)
	obj.accountKVRoot = tcommon.BytesToHash([]byte{0xaa})
	obj.accountKVGeneration = 17
	obj.codeHash = tcommon.BytesToHash([]byte{0xbb})
	return obj
}

func TestJournalAccountLatestEnvelopeContainsSameAccountProto(t *testing.T) {
	addr := testAddr(0x7a)
	obj := newMapRichJournalAccount(addr, 8)
	sdb := &StateDB{journal: newJournal()}

	sdb.journalAccount(addr, obj)
	if len(sdb.journal.entries) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(sdb.journal.entries))
	}
	change, ok := sdb.journal.entries[0].(accountChange)
	if !ok {
		t.Fatalf("journal entry type = %T, want accountChange", sdb.journal.entries[0])
	}
	envelope, err := DecodeStateAccountV2(change.prevLatest)
	if err != nil {
		t.Fatalf("decode previous account latest: %v", err)
	}
	if !bytes.Equal(envelope.AccountProto, change.prev) {
		t.Fatal("account protobuf differs between revert and latest-domain pre-images")
	}
	if envelope.AccountKVRoot != EmptyKVRoot {
		t.Fatalf("flat account KV root = %x, want %x", envelope.AccountKVRoot, EmptyKVRoot)
	}
	if envelope.AccountKVGeneration != obj.accountKVGeneration {
		t.Fatalf("account KV generation = %d, want %d", envelope.AccountKVGeneration, obj.accountKVGeneration)
	}
	if envelope.CodeHash != obj.codeHash {
		t.Fatalf("code hash = %x, want %x", envelope.CodeHash, obj.codeHash)
	}
}

func TestJournalAccountDeletedObjectOmitsLatestEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deleted      bool
		selfDestruct bool
	}{
		{name: "deleted", deleted: true},
		{name: "self-destructed", selfDestruct: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := testAddr(0x7c)
			obj := newMapRichJournalAccount(addr, 2)
			obj.deleted = tc.deleted
			obj.selfDestructed = tc.selfDestruct
			sdb := &StateDB{journal: newJournal()}

			sdb.journalAccount(addr, obj)
			change := sdb.journal.entries[0].(accountChange)
			if len(change.prev) == 0 {
				t.Fatal("revert pre-image must retain the account protobuf")
			}
			if change.prevLatest != nil {
				t.Fatalf("latest-domain pre-image = %x, want nil", change.prevLatest)
			}
		})
	}
}

func TestJournalAccountCoalescesWithinSnapshot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x7d)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	sdb.resetJournal()

	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 10)
	sdb.AddBalance(addr, 20)
	sdb.SetAllowance(addr, 30)
	if got := len(sdb.journal.entries); got != 1 {
		t.Fatalf("journal entries = %d, want one account pre-image", got)
	}
	if got := sdb.GetBalance(addr); got != 130 {
		t.Fatalf("balance before revert = %d, want 130", got)
	}
	sdb.RevertToSnapshot(snap)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("balance after revert = %d, want 100", got)
	}
	if got := sdb.GetAllowance(addr); got != 0 {
		t.Fatalf("allowance after revert = %d, want 0", got)
	}
}

func TestJournalAccountNestedSnapshotsKeepIndependentPreimages(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x7e)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	sdb.resetJournal()

	outer := sdb.Snapshot()
	sdb.AddBalance(addr, 10)
	inner := sdb.Snapshot()
	sdb.AddBalance(addr, 20)
	sdb.SetAllowance(addr, 30)
	if got := len(sdb.journal.entries); got != 2 {
		t.Fatalf("journal entries = %d, want one per snapshot", got)
	}

	sdb.RevertToSnapshot(inner)
	if got := sdb.GetBalance(addr); got != 110 {
		t.Fatalf("balance after inner revert = %d, want 110", got)
	}
	if got := sdb.GetAllowance(addr); got != 0 {
		t.Fatalf("allowance after inner revert = %d, want 0", got)
	}
	sdb.RevertToSnapshot(outer)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("balance after outer revert = %d, want 100", got)
	}
}

func TestJournalAccountRejectsStalePositionAfterRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addrA := testAddr(0x7f)
	addrB := testAddr(0x80)
	for _, addr := range []tcommon.Address{addrA, addrB} {
		sdb.GetOrCreateAccount(addr)
		sdb.AddBalance(addr, 100)
	}
	sdb.resetJournal()

	outer := sdb.Snapshot()
	sdb.AddBalance(addrA, 1)
	inner := sdb.Snapshot()
	sdb.AddBalance(addrA, 2)
	sdb.RevertToSnapshot(inner)

	// Reuse addrA's now-stale journal slot with another account. The position
	// cache must validate the entry's address instead of treating the numeric
	// slot as a hit after the journal grows again.
	sdb.AddBalance(addrB, 3)
	sdb.AddBalance(addrA, 4)
	if got := len(sdb.journal.entries); got != 3 {
		t.Fatalf("journal entries after stale-slot reuse = %d, want 3", got)
	}
	sdb.RevertToSnapshot(outer)
	if got := sdb.GetBalance(addrA); got != 100 {
		t.Fatalf("addrA balance after revert = %d, want 100", got)
	}
	if got := sdb.GetBalance(addrB); got != 100 {
		t.Fatalf("addrB balance after revert = %d, want 100", got)
	}
}

func TestJournalAccountDomainFlushStartsNewPreimageInterval(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x81)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	sdb.resetJournal()
	sdb.changeSet.enabled = true
	sdb.changeSet.captureAtCommit = false

	_ = sdb.Snapshot()
	sdb.AddBalance(addr, 10)
	sdb.changeSet.journalMark = sdb.journal.length() // prior tx published
	sdb.AddBalance(addr, 20)
	if got := len(sdb.journal.entries); got != 2 {
		t.Fatalf("journal entries across domain flush = %d, want 2", got)
	}
	second := sdb.journal.entries[1].(accountChange)
	prev, err := types.UnmarshalAccount(second.prev)
	if err != nil {
		t.Fatalf("decode second pre-image: %v", err)
	}
	if got := prev.Balance(); got != 110 {
		t.Fatalf("second interval pre-image balance = %d, want 110", got)
	}
}

func TestJournalAccountReusesAndInvalidatesDeterministicProto(t *testing.T) {
	addr := testAddr(0x84)
	obj := newMapRichJournalAccount(addr, 8)
	cached, err := obj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	sdb := &StateDB{journal: newJournal()}

	sdb.journalAccount(addr, obj)
	change := sdb.journal.entries[0].(accountChange)
	if len(change.prev) == 0 || &change.prev[0] != &cached[0] {
		t.Fatal("journal did not reuse the cached deterministic account bytes")
	}
	if obj.accountProto != nil {
		t.Fatal("journal left the account proto cache valid across a mutation")
	}
}

func TestAccountProtoCacheTracksEncodeAndRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x85)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	sdb.resetJournal()
	obj := sdb.getStateObject(addr)

	if _, exists, err := encodeAccountLatestObject(obj, true); err != nil || !exists {
		t.Fatalf("prime account proto cache: exists=%v err=%v", exists, err)
	}
	if obj.accountProto == nil {
		t.Fatal("account encoding did not populate the proto cache")
	}

	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 10)
	if obj.accountProto != nil {
		t.Fatal("account mutation did not invalidate the proto cache")
	}
	sdb.RevertToSnapshot(snap)
	obj = sdb.getStateObject(addr)
	if obj.accountProto == nil {
		t.Fatal("account revert did not restore the deterministic proto cache")
	}
	restored, err := types.UnmarshalAccount(obj.accountProto)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Balance() != 100 || obj.account.Balance() != 100 {
		t.Fatalf("restored balances proto=%d object=%d, want 100", restored.Balance(), obj.account.Balance())
	}
}

func TestAccountScalarJournalNestedSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x88)
	obj := newMapRichJournalAccount(addr, 8)
	obj.dirtySet = sdb.dirtyObjects
	sdb.stateObjects[addr] = obj
	original, err := obj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}

	outer := sdb.Snapshot()
	sdb.AddBalance(addr, 10)
	sdb.SetAllowance(addr, 20)
	sdb.SetNetUsage(addr, 30)
	sdb.SetLatestOperationTime(addr, 40)
	sdb.SetLatestConsumeTime(addr, 50)
	sdb.SetFreeNetUsage(addr, 60)
	sdb.SetLatestConsumeFreeTime(addr, 70)
	sdb.SetNetWindow(addr, 80, true)
	sdb.SetEnergyUsage(addr, 90)
	sdb.SetLatestConsumeTimeForEnergy(addr, 100)
	sdb.SetEnergyWindow(addr, 110, true)
	if got := len(sdb.journal.entries); got != 1 {
		t.Fatalf("outer scalar journal entries = %d, want 1", got)
	}
	if _, ok := sdb.journal.entries[0].(*accountScalarChange); !ok {
		t.Fatalf("outer journal type = %T, want accountScalarChange", sdb.journal.entries[0])
	}

	inner := sdb.Snapshot()
	sdb.AddBalance(addr, 1)
	sdb.SetEnergyUsage(addr, 2)
	sdb.SetTRC10Balance(addr, 1_000_001, 999) // forces a full Account pre-image
	sdb.SetNetUsage(addr, 3)                  // covered by the full pre-image
	if got := len(sdb.journal.entries); got != 3 {
		t.Fatalf("nested mixed journal entries = %d, want scalar+scalar+full", got)
	}
	sdb.RevertToSnapshot(inner)
	if got := sdb.GetBalance(addr); got != 1_000_000_010 {
		t.Fatalf("balance after inner revert = %d", got)
	}
	if got := sdb.GetNetUsage(addr); got != 30 {
		t.Fatalf("net usage after inner revert = %d, want 30", got)
	}
	if got := sdb.GetEnergyUsage(addr); got != 90 {
		t.Fatalf("energy usage after inner revert = %d, want 90", got)
	}
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 2 {
		t.Fatalf("TRC10 balance after inner revert = %d, want 2", got)
	}

	sdb.RevertToSnapshot(outer)
	restored := sdb.getStateObject(addr)
	got, err := restored.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("outer revert did not restore the exact deterministic Account bytes")
	}
	if restored.account.Proto().AccountResource != nil {
		t.Fatal("outer revert did not restore nil AccountResource presence")
	}
}

func TestAccountScalarJournalTransactionBoundaries(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x89)
	obj := newMapRichJournalAccount(addr, 8)
	obj.dirtySet = sdb.dirtyObjects
	sdb.stateObjects[addr] = obj
	original, err := obj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}

	block := sdb.Snapshot()
	for tx := int64(1); tx <= 3; tx++ {
		sdb.Snapshot()
		sdb.AddBalance(addr, -tx)
		sdb.SetNetUsage(addr, tx*10)
		sdb.SetEnergyUsage(addr, tx*100)
		sdb.FinalizeTransaction()
	}
	if got := len(sdb.journal.entries); got != 3 {
		t.Fatalf("journal entries across tx boundaries = %d, want 3", got)
	}
	for i, entry := range sdb.journal.entries {
		if _, ok := entry.(*accountScalarChange); !ok {
			t.Fatalf("journal entry %d type = %T, want accountScalarChange", i, entry)
		}
	}
	sdb.RevertToSnapshot(block)
	got, err := sdb.getStateObject(addr).deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("block revert across transaction boundaries changed Account bytes")
	}
}

func TestAccountScalarJournalHistoryEnabledKeepsFullPreimage(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x8a)
	obj := newMapRichJournalAccount(addr, 8)
	obj.dirtySet = sdb.dirtyObjects
	sdb.stateObjects[addr] = obj
	original, err := obj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	sdb.changeSet.enabled = true
	sdb.changeSet.captureAtCommit = false

	sdb.Snapshot()
	sdb.AddBalance(addr, -1)
	sdb.SetEnergyUsage(addr, 123)
	if got := len(sdb.journal.entries); got != 1 {
		t.Fatalf("history journal entries = %d, want 1", got)
	}
	change, ok := sdb.journal.entries[0].(accountChange)
	if !ok {
		t.Fatalf("history journal type = %T, want accountChange", sdb.journal.entries[0])
	}
	if !bytes.Equal(change.prev, original) {
		t.Fatal("history-enabled scalar write did not retain exact Account pre-image")
	}
	envelope, err := DecodeStateAccountV2(change.prevLatest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(envelope.AccountProto, original) {
		t.Fatal("history-enabled latest pre-image did not retain exact Account bytes")
	}
}

func TestAccountScalarJournalCommitAndRootsMatchFullJournal(t *testing.T) {
	setup := func(t *testing.T) (*StateDB, tcommon.Address, *stateObject) {
		t.Helper()
		sdb := newTestStateDB(t)
		addr := testAddr(0x8b)
		obj := sdb.GetOrCreateAccount(addr)
		obj.account = newMapRichJournalAccount(addr, 8).account.Copy()
		obj.accountDirty = true
		obj.markDirty()
		sdb.resetJournal()
		return sdb, addr, obj
	}
	applyDirect := func(sdb *StateDB, addr tcommon.Address, obj *stateObject) {
		sdb.journalAccount(addr, obj)
		obj.account.SetBalance(obj.account.Balance() - 123)
		obj.account.SetAllowance(456)
		obj.account.SetNetUsage(789)
		obj.account.SetLatestOperationTime(10)
		obj.account.SetLatestConsumeTime(11)
		obj.account.SetFreeNetUsage(12)
		obj.account.SetLatestConsumeFreeTime(13)
		obj.account.SetNetWindow(14, true)
		obj.account.SetEnergyUsage(15)
		obj.account.SetLatestConsumeTimeForEnergy(16)
		obj.account.SetEnergyWindow(17, true)
		obj.markDirty()
	}

	optimized, addr, optimizedObj := setup(t)
	full, _, fullObj := setup(t)
	optimized.Snapshot()
	full.Snapshot()
	optimizedMark := optimized.JournalMark()
	fullMark := full.JournalMark()
	optimized.AddBalance(addr, -123)
	optimized.SetAllowance(addr, 456)
	optimized.SetNetUsage(addr, 789)
	optimized.SetLatestOperationTime(addr, 10)
	optimized.SetLatestConsumeTime(addr, 11)
	optimized.SetFreeNetUsage(addr, 12)
	optimized.SetLatestConsumeFreeTime(addr, 13)
	optimized.SetNetWindow(addr, 14, true)
	optimized.SetEnergyUsage(addr, 15)
	optimized.SetLatestConsumeTimeForEnergy(addr, 16)
	optimized.SetEnergyWindow(addr, 17, true)
	applyDirect(full, addr, fullObj)

	optimizedBytes, err := optimizedObj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	fullBytes, err := fullObj.deterministicAccountProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(optimizedBytes, fullBytes) {
		t.Fatal("optimized and full journal paths produced different Account bytes")
	}
	optimizedJavaRoot, err := optimized.JavaAccountStateRoot(tcommon.Hash{}, optimizedMark)
	if err != nil {
		t.Fatal(err)
	}
	fullJavaRoot, err := full.JavaAccountStateRoot(tcommon.Hash{}, fullMark)
	if err != nil {
		t.Fatal(err)
	}
	if optimizedJavaRoot != fullJavaRoot {
		t.Fatalf("java account roots differ: optimized=%x full=%x", optimizedJavaRoot, fullJavaRoot)
	}
	optimizedRoot, err := optimized.Commit()
	if err != nil {
		t.Fatal(err)
	}
	fullRoot, err := full.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if optimizedRoot != fullRoot {
		t.Fatalf("commitment roots differ: optimized=%x full=%x", optimizedRoot, fullRoot)
	}
}

func BenchmarkJournalAccountMapRich(b *testing.B) {
	addr := testAddr(0x7b)
	obj := newMapRichJournalAccount(addr, 64)
	sdb := &StateDB{journal: newJournal()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.journal.entries = sdb.journal.entries[:0]
		sdb.journalAccount(addr, obj)
	}
}

func BenchmarkJournalAccountMapRichRepeatedSnapshot(b *testing.B) {
	addr := testAddr(0x82)
	obj := newMapRichJournalAccount(addr, 64)
	sdb := &StateDB{journal: newJournal()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.journal.entries = sdb.journal.entries[:0]
		sdb.snapshots = append(sdb.snapshots[:0], 0)
		clear(sdb.accountJournalPos)
		for range 8 {
			sdb.journalAccount(addr, obj)
		}
	}
}

func BenchmarkJournalAccountMapRichCached(b *testing.B) {
	addr := testAddr(0x86)
	obj := newMapRichJournalAccount(addr, 64)
	cached, err := obj.account.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	sdb := &StateDB{journal: newJournal()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.journal.entries = sdb.journal.entries[:0]
		obj.accountProto = cached
		sdb.journalAccount(addr, obj)
	}
}

// BenchmarkJournalAccountMapRichSmartContractBlock models the account writes
// made by a block of TriggerSmartContract transactions: every transaction has
// its own rollback boundary and updates balance plus bandwidth/energy recovery
// scalars on the same map-rich account.
func BenchmarkJournalAccountMapRichSmartContractBlock(b *testing.B) {
	const txsPerBlock = 32
	addr := testAddr(0x87)
	obj := newMapRichJournalAccount(addr, 64)
	sdb := &StateDB{
		stateObjects:    map[tcommon.Address]*stateObject{addr: obj},
		dirtyObjects:    make(map[tcommon.Address]struct{}),
		txFinalizeDirty: make(map[tcommon.Address]struct{}),
		journal:         newJournal(),
	}
	obj.dirtySet = sdb.dirtyObjects
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sdb.resetJournal()
		obj.account.SetBalance(1_000_000_000)
		obj.account.SetNetUsage(0)
		obj.account.SetLatestConsumeTime(0)
		obj.account.SetLatestOperationTime(0)
		obj.account.SetFreeNetUsage(0)
		obj.account.SetLatestConsumeFreeTime(0)
		obj.account.SetEnergyUsage(0)
		obj.account.SetLatestConsumeTimeForEnergy(0)
		obj.account.SetNetWindow(0, false)
		obj.account.SetEnergyWindow(0, false)
		obj.accountProto = nil
		for tx := int64(1); tx <= txsPerBlock; tx++ {
			sdb.Snapshot()
			sdb.AddBalance(addr, -1)
			sdb.SetNetUsage(addr, tx*100)
			sdb.SetLatestConsumeTime(addr, tx)
			sdb.SetLatestOperationTime(addr, tx)
			sdb.SetFreeNetUsage(addr, tx*10)
			sdb.SetLatestConsumeFreeTime(addr, tx)
			sdb.SetEnergyUsage(addr, tx*1_000)
			sdb.SetLatestConsumeTimeForEnergy(addr, tx)
			sdb.SetNetWindow(addr, tx*10_000, true)
			sdb.SetEnergyWindow(addr, tx*20_000, true)
			sdb.FinalizeTransaction()
		}
	}
}

func BenchmarkAccountSnapshotStrategies(b *testing.B) {
	obj := newMapRichJournalAccount(testAddr(0x83), 64)
	b.Run("deterministic-marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkAccountBytes, _ = obj.account.Marshal()
		}
	})
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkAccountBytes, _ = proto.Marshal(obj.account.Proto())
		}
	})
	b.Run("clone", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkAccountProto = proto.Clone(obj.account.Proto()).(*corepb.Account)
		}
	})
}
