package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestTRC10MapsPersistOutsideAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x91)
	sdb.SetTRC10BalanceLegacyAndV2(addr, []byte("TOKEN"), 1_000_001, 77)
	sdb.SetFreeAssetNetUsage(addr, "TOKEN", 12)
	sdb.SetLatestAssetOperationTimeV2(addr, "1000001", 34)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.accountKVIndex(), addr)
	if err != nil || !ok {
		t.Fatalf("read account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 3 {
		t.Fatalf("account version = %d, want 3", envelope.Version)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Asset)+len(stored.AssetV2)+len(stored.FreeAssetNetUsage)+len(stored.FreeAssetNetUsageV2)+len(stored.LatestAssetOperationTime)+len(stored.LatestAssetOperationTimeV2) != 0 {
		t.Fatalf("split maps leaked into account envelope: %+v", &stored)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.GetTRC10Balance(addr, 1_000_001); got != 77 {
		t.Fatalf("split balance = %d, want 77", got)
	}
	if got := reopened.GetAccount(addr); got == nil || got.Proto().AssetV2["1000001"] != 77 || got.Proto().Asset["TOKEN"] != 77 {
		t.Fatalf("materialized account = %+v", got)
	}
	value, ok, err := reopened.GetAccountKV(addr, kvdomains.AccountFreeAssetNetUsage, []byte("TOKEN"))
	if err != nil || !ok {
		t.Fatalf("read split usage: ok=%v err=%v", ok, err)
	}
	if decoded, err := decodeAccountAuxInt64(value); err != nil || decoded != 12 {
		t.Fatalf("split usage = %d err=%v, want 12", decoded, err)
	}
}

func TestStateAccountV3RejectsV2(t *testing.T) {
	legacy := &StateAccountV3{Version: 2, AccountProto: []byte{1}}
	data, err := legacy.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeStateAccountV3(data); err == nil {
		t.Fatal("expected v2 account envelope to be rejected")
	}
}

func TestTRC10MapSnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x94)
	sdb.SetTRC10Balance(addr, 1_000_001, 11)
	if got := sdb.GetAccount(addr); got == nil || got.Proto().AssetV2["1000001"] != 11 {
		t.Fatalf("initial materialized map = %+v", got)
	}

	snapshot := sdb.Snapshot()
	sdb.SetTRC10Balance(addr, 1_000_001, 22)
	if got := sdb.GetAccount(addr); got == nil || got.Proto().AssetV2["1000001"] != 22 {
		t.Fatalf("updated materialized map = %+v", got)
	}
	sdb.RevertToSnapshot(snapshot)
	if got := sdb.GetAccount(addr); got == nil || got.Proto().AssetV2["1000001"] != 11 {
		t.Fatalf("materialized map after revert = %+v", got)
	}
}

func TestAccountSplitDomainsRemainContiguousForHotPathGuard(t *testing.T) {
	wantCount := int(kvdomains.AccountTronPowerAux-kvdomains.AccountPermissionAux) + 1
	if len(accountSplitDomains) != wantCount {
		t.Fatalf("split domain count = %d, want contiguous range size %d", len(accountSplitDomains), wantCount)
	}
	for domain := kvdomains.AccountPermissionAux; domain <= kvdomains.AccountTronPowerAux; domain++ {
		if !isAccountSplitDomain(domain) {
			t.Fatalf("split account domain range has gap at %s (%#04x)", kvdomains.Name(domain), uint16(domain))
		}
	}
}
