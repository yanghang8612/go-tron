package state

import (
	"strconv"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func testAssetIssue(owner tcommon.Address, id int64, name string, supply int64) *contractpb.AssetIssueContract {
	return &contractpb.AssetIssueContract{
		Id:           strconv.FormatInt(id, 10),
		OwnerAddress: owner[:],
		Name:         []byte(name),
		TotalSupply:  supply,
		TrxNum:       1,
		Num:          1,
	}
}

func TestAssetStoreStrictRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x71)
	const tokenID int64 = 1_000_001
	v2 := testAssetIssue(owner, tokenID, "TOKEN", 101)
	legacy := testAssetIssue(owner, tokenID, "TOKEN", 100)

	if err := sdb.WriteAssetIssue(tokenID, v2); err != nil {
		t.Fatalf("WriteAssetIssue: %v", err)
	}
	if err := sdb.WriteAssetIssueByName([]byte("TOKEN"), legacy); err != nil {
		t.Fatalf("WriteAssetIssueByName: %v", err)
	}
	if err := sdb.WriteAssetNameIndex([]byte("TOKEN"), tokenID); err != nil {
		t.Fatalf("WriteAssetNameIndex: %v", err)
	}
	if err := sdb.WriteAssetOwnerIndex(owner[:], tokenID); err != nil {
		t.Fatalf("WriteAssetOwnerIndex: %v", err)
	}
	if err := sdb.WriteAssetIssueTime(tokenID, 1234); err != nil {
		t.Fatalf("WriteAssetIssueTime: %v", err)
	}

	gotV2, ok, err := sdb.ReadAssetIssueStrict(tokenID)
	if err != nil || !ok || gotV2 == nil || gotV2.GetTotalSupply() != 101 {
		t.Fatalf("ReadAssetIssueStrict = %+v/%v/%v, want V2 supply 101", gotV2, ok, err)
	}
	gotLegacy, ok, err := sdb.ReadAssetIssueByNameStrict([]byte("TOKEN"))
	if err != nil || !ok || gotLegacy == nil || gotLegacy.GetTotalSupply() != 100 {
		t.Fatalf("ReadAssetIssueByNameStrict = %+v/%v/%v, want legacy supply 100", gotLegacy, ok, err)
	}
	if id, ok, err := sdb.ReadAssetNameIndexStrict([]byte("TOKEN")); err != nil || !ok || id != tokenID {
		t.Fatalf("ReadAssetNameIndexStrict = %d/%v/%v, want %d/true/nil", id, ok, err, tokenID)
	}
	if id, ok, err := sdb.ReadAssetOwnerIndexStrict(owner[:]); err != nil || !ok || id != tokenID {
		t.Fatalf("ReadAssetOwnerIndexStrict = %d/%v/%v, want %d/true/nil", id, ok, err, tokenID)
	}
	if ts, ok, err := sdb.ReadAssetIssueTimeStrict(tokenID); err != nil || !ok || ts != 1234 {
		t.Fatalf("ReadAssetIssueTimeStrict = %d/%v/%v, want 1234/true/nil", ts, ok, err)
	}
	v2List, err := sdb.ListAssetsV2Strict(tokenID, tokenID)
	if err != nil || len(v2List) != 1 || v2List[0].GetTotalSupply() != 101 {
		t.Fatalf("ListAssetsV2Strict = %+v/%v, want V2 supply 101", v2List, err)
	}
	legacyList, err := sdb.ListAssetsLegacyStrict(tokenID, tokenID)
	if err != nil || len(legacyList) != 1 || legacyList[0].GetTotalSupply() != 100 {
		t.Fatalf("ListAssetsLegacyStrict = %+v/%v, want legacy supply 100", legacyList, err)
	}

	if got, ok, err := sdb.ReadAssetIssueStrict(tokenID + 1); got != nil || ok || err != nil {
		t.Fatalf("ReadAssetIssueStrict missing = %+v/%v/%v, want nil/false/nil", got, ok, err)
	}
	missingOwner := testAddr(0x72)
	if id, ok, err := sdb.ReadAssetOwnerIndexStrict(missingOwner[:]); id != 0 || ok || err != nil {
		t.Fatalf("ReadAssetOwnerIndexStrict missing = %d/%v/%v, want 0/false/nil", id, ok, err)
	}
}

func TestAssetIssueAtSurfacesCorruptMetadata(t *testing.T) {
	f := newHistoryFixture(t)
	const tokenID int64 = 1_000_001

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetV2Tag, tokenID), []byte{0x80}); err != nil {
			t.Fatalf("write corrupt asset metadata: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	got, err := f.reader().AssetIssueAt(tokenID, 1)
	if err == nil {
		t.Fatal("AssetIssueAt corrupt metadata error = nil")
	}
	if got != nil {
		t.Fatalf("AssetIssueAt corrupt metadata asset = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode asset metadata at block 1") {
		t.Fatalf("AssetIssueAt corrupt metadata error = %v, want decode asset metadata context", err)
	}
}

func TestAssetStoreStrictSurfacesCorruptMetadata(t *testing.T) {
	sdb := newTestStateDB(t)
	const tokenID int64 = 1_000_001
	corruptProto := []byte{0x80}
	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetV2Tag, tokenID), corruptProto); err != nil {
		t.Fatalf("write corrupt V2 asset: %v", err)
	}

	if got := sdb.ReadAssetIssue(tokenID); got != nil {
		t.Fatalf("compat ReadAssetIssue corrupt metadata = %+v, want nil", got)
	}
	if got, ok, err := sdb.ReadAssetIssueStrict(tokenID); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode asset issue id 1000001") {
		t.Fatalf("ReadAssetIssueStrict corrupt metadata = %+v/%v/%v, want decode error", got, ok, err)
	}
	if _, err := sdb.ListAssetsV2Strict(tokenID, tokenID); err == nil || !strings.Contains(err.Error(), "decode asset issue id 1000001") {
		t.Fatalf("ListAssetsV2Strict corrupt metadata error = %v, want decode asset issue", err)
	}

	owner := testAddr(0x73)
	validV2 := testAssetIssue(owner, tokenID, "TOKEN", 101)
	if err := sdb.WriteAssetIssue(tokenID, validV2); err != nil {
		t.Fatalf("replace V2 asset: %v", err)
	}
	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetLegacyTag, []byte("TOKEN")), corruptProto); err != nil {
		t.Fatalf("write corrupt legacy asset: %v", err)
	}
	if got := sdb.ReadAssetIssueByName([]byte("TOKEN")); got != nil {
		t.Fatalf("compat ReadAssetIssueByName corrupt metadata = %+v, want nil", got)
	}
	if got, ok, err := sdb.ReadAssetIssueByNameStrict([]byte("TOKEN")); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode legacy asset issue name") {
		t.Fatalf("ReadAssetIssueByNameStrict corrupt metadata = %+v/%v/%v, want decode error", got, ok, err)
	}
	if _, err := sdb.ListAssetsLegacyStrict(tokenID, tokenID); err == nil || !strings.Contains(err.Error(), "decode legacy asset issue name") {
		t.Fatalf("ListAssetsLegacyStrict corrupt metadata error = %v, want decode legacy asset", err)
	}
}

func TestAssetStoreStrictSurfacesMalformedScalars(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x74)
	const tokenID int64 = 1_000_001

	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetNameIndexTag, []byte("TOKEN")), []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write malformed name index: %v", err)
	}
	if id, ok := sdb.ReadAssetNameIndex([]byte("TOKEN")); id != 0 || ok {
		t.Fatalf("compat ReadAssetNameIndex malformed = %d/%v, want 0/false", id, ok)
	}
	if id, ok, err := sdb.ReadAssetNameIndexStrict([]byte("TOKEN")); err == nil || !ok || id != 0 || !strings.Contains(err.Error(), "decode asset name index") {
		t.Fatalf("ReadAssetNameIndexStrict malformed = %d/%v/%v, want length error", id, ok, err)
	}

	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetOwnerIndexTag, owner[:]), []byte{0x01}); err != nil {
		t.Fatalf("write malformed owner index: %v", err)
	}
	if id, ok := sdb.ReadAssetOwnerIndex(owner[:]); id != 0 || ok {
		t.Fatalf("compat ReadAssetOwnerIndex malformed = %d/%v, want 0/false", id, ok)
	}
	if id, ok, err := sdb.ReadAssetOwnerIndexStrict(owner[:]); err == nil || !ok || id != 0 || !strings.Contains(err.Error(), "decode asset owner index") {
		t.Fatalf("ReadAssetOwnerIndexStrict malformed = %d/%v/%v, want length error", id, ok, err)
	}

	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetIssueTimeTag, tokenID), []byte{0x01}); err != nil {
		t.Fatalf("write malformed issue time: %v", err)
	}
	if got := sdb.ReadAssetIssueTime(tokenID); got != 0 {
		t.Fatalf("compat ReadAssetIssueTime malformed = %d, want 0", got)
	}
	if ts, ok, err := sdb.ReadAssetIssueTimeStrict(tokenID); err == nil || !ok || ts != 0 || !strings.Contains(err.Error(), "decode asset issue time") {
		t.Fatalf("ReadAssetIssueTimeStrict malformed = %d/%v/%v, want length error", ts, ok, err)
	}
}
