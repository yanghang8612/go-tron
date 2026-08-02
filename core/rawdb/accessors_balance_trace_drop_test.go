package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDropBalanceTraceKeyspacesPreservesOtherData(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	if err := WriteBlockBalanceTrace(db, 7, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 7},
	}); err != nil {
		t.Fatal(err)
	}
	owner := []byte{1, 2, 3}
	if err := WriteAccountTrace(db, owner, 7, 99); err != nil {
		t.Fatal(err)
	}
	txHash := make([]byte, 32)
	txHash[0] = 0xaa
	if err := WriteTransactionIndex(db, txHash, 7); err != nil {
		t.Fatal(err)
	}
	if err := DropBalanceTraceKeyspaces(db); err != nil {
		t.Fatal(err)
	}
	if trace := ReadBlockBalanceTrace(db, 7); trace != nil {
		t.Fatalf("block balance trace survived drop: %+v", trace)
	}
	if _, ok := ReadAccountTrace(db, owner, 7); ok {
		t.Fatal("account trace survived drop")
	}
	if blockNum := ReadTransactionIndex(NewChainDB(db, NoopAncient{}), txHash); blockNum == nil || *blockNum != 7 {
		t.Fatalf("transaction index changed by trace drop: %v", blockNum)
	}
}
