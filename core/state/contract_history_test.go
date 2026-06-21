package state

import (
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestContractAtSurfacesCorruptMetadata(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x71)

	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("write block 1 tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write block 2 tx range: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, addr, 0); err != nil {
		t.Fatalf("write kv generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, addr, 0, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0x80}); err != nil {
		t.Fatalf("write corrupt contract metadata: %v", err)
	}

	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	got, err := reader.ContractAt(addr, 1)
	if err == nil {
		t.Fatal("ContractAt corrupt metadata error = nil")
	}
	if got != nil {
		t.Fatalf("ContractAt corrupt metadata contract = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode contract metadata at block 1") {
		t.Fatalf("ContractAt corrupt metadata error = %v, want decode contract metadata context", err)
	}
}
