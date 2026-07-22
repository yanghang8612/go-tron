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
