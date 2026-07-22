package state

import (
	"bytes"
	"strconv"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
