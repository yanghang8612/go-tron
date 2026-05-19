package core

import (
	"encoding/binary"
	"encoding/hex"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const (
	historyServeWindow = uint64(8191)
	historyStorageName = "BlockHashHistory"
)

var (
	historyStorageAddress  = tcommon.BytesToAddress(mustDecodeHex("410000f90827f1c53a10cb7a02335b175320002935"))
	historyDeployerAddress = mustDecodeHex("413462413af4609098e1e27a490f554f260213d685")
	historyStorageCode     = mustDecodeHex("3373fffffffffffffffffffffffffffffffffffffffe14604657602036036042575f35600143038111604257611fff81430311604257611fff9006545f5260205ff35b5f5ffd5b5f35611fff60014303065500")
)

func mustDecodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func deployHistoryBlockHash(db kvReadWriter, statedb *state.StateDB, dynProps *state.DynamicProperties) {
	if statedb == nil || dynProps == nil {
		return
	}
	if len(statedb.GetCode(historyStorageAddress)) > 0 ||
		statedb.GetContract(historyStorageAddress) != nil ||
		(db != nil && (len(rawdb.ReadCode(db, historyStorageAddress)) > 0 ||
			len(rawdb.ReadContract(db, historyStorageAddress)) > 0)) {
		return
	}

	accountExisting := statedb.AccountExists(historyStorageAddress)
	statedb.SetCode(historyStorageAddress, append([]byte(nil), historyStorageCode...))
	statedb.SetContract(historyStorageAddress, &contractpb.SmartContract{
		Name:                       historyStorageName,
		ContractAddress:            historyStorageAddress.Bytes(),
		OriginAddress:              append([]byte(nil), historyDeployerAddress...),
		ConsumeUserResourcePercent: 100,
	})
	statedb.CreateAccount(historyStorageAddress, corepb.AccountType_Contract)
	if !accountExisting {
		statedb.SetAccountName(historyStorageAddress, historyStorageName)
	} else {
		statedb.ClearAcquiredDelegatedResource(historyStorageAddress)
	}
	dynProps.SetBlockHashHistoryInstalled(true)
}

func writeHistoryBlockHash(statedb *state.StateDB, dynProps *state.DynamicProperties, blockNum uint64, parentHash tcommon.Hash) {
	if statedb == nil || dynProps == nil || !dynProps.BlockHashHistoryInstalled() || blockNum == 0 {
		return
	}
	slot := (blockNum - 1) % historyServeWindow
	slotKey := uint64ToDataWord(slot)
	// Pre-warm the storage cache so that the SetState journal entry captures
	// the real disk pre-value rather than zero. The TVM normally pre-warms
	// via opSload before opSstore; this direct write path bypasses that.
	// Without this pre-warm, once the BlockHashHistory ring wraps
	// (block ≥ 8192), the State History Index would record zero pre-values
	// for ring slots instead of the prior-cycle hash.
	_ = statedb.GetState(historyStorageAddress, slotKey)
	statedb.SetState(historyStorageAddress, slotKey, parentHash)
}

func uint64ToDataWord(v uint64) tcommon.Hash {
	var h tcommon.Hash
	binary.BigEndian.PutUint64(h[24:], v)
	return h
}
