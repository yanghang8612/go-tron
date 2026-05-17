package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestWriteReadAssetIssue(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	c := &contractpb.AssetIssueContract{
		Name:        []byte("MYTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
	}
	if err := WriteAssetIssue(db, 1_000_001, c); err != nil {
		t.Fatal(err)
	}
	got := ReadAssetIssue(db, 1_000_001)
	if got == nil {
		t.Fatal("expected asset to be found")
	}
	if string(got.Name) != "MYTOKEN" {
		t.Fatalf("name: want MYTOKEN, got %s", got.Name)
	}
}

func TestWriteReadAssetIssueByName(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	c := &contractpb.AssetIssueContract{
		Name:      []byte("MYTOKEN"),
		Precision: 6,
		Id:        "1000001",
	}
	if err := WriteAssetIssueByName(db, []byte("MYTOKEN"), c); err != nil {
		t.Fatal(err)
	}
	got := ReadAssetIssueByName(db, []byte("MYTOKEN"))
	if got == nil {
		t.Fatal("expected legacy asset to be found")
	}
	if got.Precision != 6 {
		t.Fatalf("precision: want 6, got %d", got.Precision)
	}
	if ReadAssetIssueByName(db, []byte("UNKNOWN")) != nil {
		t.Fatal("expected nil for unknown legacy asset")
	}
}

func TestReadAssetIssue_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if got := ReadAssetIssue(db, 9_999_999); got != nil {
		t.Fatal("expected nil for unknown token")
	}
}

func TestAssetNameIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteAssetNameIndex(db, []byte("MYTOKEN"), 1_000_001); err != nil {
		t.Fatal(err)
	}
	id, ok := ReadAssetNameIndex(db, []byte("MYTOKEN"))
	if !ok {
		t.Fatal("expected name index to be found")
	}
	if id != 1_000_001 {
		t.Fatalf("tokenID: want 1000001, got %d", id)
	}
	_, ok2 := ReadAssetNameIndex(db, []byte("UNKNOWN"))
	if ok2 {
		t.Fatal("expected not-found for unknown name")
	}
}

func TestAssetOwnerIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	if err := WriteAssetOwnerIndex(db, owner[:], 1_000_001); err != nil {
		t.Fatal(err)
	}
	id, ok := ReadAssetOwnerIndex(db, owner[:])
	if !ok {
		t.Fatal("expected owner index to be found")
	}
	if id != 1_000_001 {
		t.Fatalf("tokenID: want 1000001, got %d", id)
	}
	other := common.Address{0x41, 0x02}
	_, ok2 := ReadAssetOwnerIndex(db, other[:])
	if ok2 {
		t.Fatal("expected not-found for other address")
	}
}

func TestAssetIssueTime(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteAssetIssueTime(db, 1_000_001, 1_713_000_000_000); err != nil {
		t.Fatal(err)
	}
	got := ReadAssetIssueTime(db, 1_000_001)
	if got != 1_713_000_000_000 {
		t.Fatalf("issueTime: want 1713000000000, got %d", got)
	}
	if ReadAssetIssueTime(db, 9_999_999) != 0 {
		t.Fatal("expected 0 for unknown token")
	}
}

func TestListAllAssets(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{Name: []byte("AAA")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteAssetIssue(db, 1_000_002, &contractpb.AssetIssueContract{Name: []byte("BBB")}); err != nil {
		t.Fatal(err)
	}
	all := ListAllAssets(db)
	if len(all) != 2 {
		t.Fatalf("expected 2 assets, got %d", len(all))
	}
}

func TestListAllLegacyAssets(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteAssetIssueByName(db, []byte("AAA"), &contractpb.AssetIssueContract{Name: []byte("AAA")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteAssetIssueByName(db, []byte("BBB"), &contractpb.AssetIssueContract{Name: []byte("BBB")}); err != nil {
		t.Fatal(err)
	}
	all := ListAllLegacyAssets(db)
	if len(all) != 2 {
		t.Fatalf("expected 2 legacy assets, got %d", len(all))
	}
}

func TestListAssetsPaginated(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for i := int64(0); i < 5; i++ {
		if err := WriteAssetIssue(db, 1_000_001+i, &contractpb.AssetIssueContract{}); err != nil {
			t.Fatalf("WriteAssetIssue failed: %v", err)
		}
	}
	page := ListAssetsPaginated(db, 2, 2)
	if len(page) != 2 {
		t.Fatalf("expected 2 paginated assets, got %d", len(page))
	}
	all := ListAssetsPaginated(db, 0, 100)
	if len(all) != 5 {
		t.Fatalf("expected 5 for limit>total, got %d", len(all))
	}
}
