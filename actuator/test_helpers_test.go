package actuator

import (
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func makeTestAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func setupStateDB(t *testing.T) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func setupContext(t *testing.T, statedb *state.StateDB, tx *types.Transaction) *Context {
	t.Helper()
	return &Context{
		State:         statedb,
		DynProps:      state.NewDynamicProperties(),
		Tx:            tx,
		BlockTime:     1000000,
		PrevBlockTime: 1000000,
		BlockNumber:   1,
		DB:            ethrawdb.NewMemoryDatabase(),
	}
}

func seedAccount(statedb *state.StateDB, addr tcommon.Address, balance int64) {
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddBalance(addr, balance)
}
