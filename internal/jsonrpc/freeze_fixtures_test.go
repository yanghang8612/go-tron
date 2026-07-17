package jsonrpc_test

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// freeze_fixtures_test.go builds the FIXED, deterministic backend state that
// the freeze corpus is captured against. Everything here is hand-pinned so
// that the block hash, tx hash, balances, code, storage, logs and receipt are
// byte-stable across runs — block/tx hashes derive from proto.Marshal of the
// header / raw-data, which is deterministic for these map-free messages.
//
// The reflection-based dispatch migration (next slice) must reproduce the
// exact same JSON for every request in fixtures/jsonrpc-corpus, so the data
// below must never change once the corpus is generated.

const (
	// freezeChainID is Nile's chain id (728126428 = 0x2b6653dc).
	freezeChainID = int64(728126428)
	// freezeBlockNumber is the chain head reported by the backend (0x64 = 100).
	freezeBlockNumber = uint64(100)

	// 21-byte TRON addresses (0x41 prefix). The handler's hex20 helper renders
	// only the trailing 20 bytes; the corpus uses the 20-byte form in requests.
	freezeOwnerAddr21    = "0x411020304050607080900010203040506070809000"
	freezeContractAddr21 = "0x41a0b0c0d0e0f000102030405060708090a0b0c0d0"
	freezeContractAddr20 = "0xa0b0c0d0e0f000102030405060708090a0b0c0d0"

	// freezeAccountHex (20-byte form) is used for getBalance/getCode/getStorageAt.
	freezeAccountHex = "0x4101020304050607080900010203040506070809"

	// freezeBalanceSUN is the live balance in SUN; the handler multiplies by 1e12.
	freezeBalanceSUN = int64(1_000_000)
	// freezeGasPrice is the energy fee in SUN per energy unit (0x1a4 = 420).
	freezeGasPrice = int64(420)
	// freezePeerCount drives net_peerCount and net_listening.
	freezePeerCount = 3
)

// hex0x renders a full byte slice as "0x"+lowercase-hex, matching the handler's
// fmt.Sprintf("0x%x", ...) output for hashes.
func hex0x(b []byte) string { return "0x" + common.ToHex(b) }

// buildFreezeBlock constructs a deterministic block at freezeBlockNumber that
// carries a single TriggerSmartContract transaction. Block.Hash() and
// tx.Hash() are pure functions of the marshaled protos, so both are stable.
func buildFreezeBlock() *types.Block {
	trigger := &contractpb.TriggerSmartContract{
		OwnerAddress:    common.FromHex(freezeOwnerAddr21),
		ContractAddress: common.FromHex(freezeContractAddr21),
		CallValue:       0x10,
		Data:            []byte{0x70, 0xa0, 0x82, 0x31},
	}
	param, err := anypb.New(trigger)
	if err != nil {
		panic("freeze fixture: anypb.New(trigger): " + err.Error())
	}

	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  1_700_000_000_000,
			Expiration: 1_700_000_060_000,
			FeeLimit:   1_000_000_000,
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TriggerSmartContract,
					Parameter: param,
				},
			},
		},
		Signature: [][]byte{make([]byte, 65)},
	}

	blockPB := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Timestamp:        1_700_000_000_000, // ms; handler emits /1000
				Number:           int64(freezeBlockNumber),
				ParentHash:       common.FromHex("0x000000000000006300000000000000000000000000000000000000000000aabb"),
				WitnessAddress:   common.FromHex(freezeOwnerAddr21),
				Version:          30,
				AccountStateRoot: common.FromHex("0x1111111111111111111111111111111111111111111111111111111111111111"),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	}
	return types.NewBlockFromPB(blockPB)
}

// buildFreezeTxInfo builds the TransactionInfo (receipt source) matching the
// single tx in the freeze block: SUCCESS result, one log, fixed energy usage.
func buildFreezeTxInfo() *corepb.TransactionInfo {
	return &corepb.TransactionInfo{
		Result:         corepb.TransactionInfo_SUCESS,
		BlockNumber:    int64(freezeBlockNumber),
		BlockTimeStamp: 1_700_000_000_000,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsageTotal: 31000,
		},
		Log: []*corepb.TransactionInfo_Log{
			{
				Address: common.FromHex(freezeContractAddr21),
				Topics: [][]byte{
					common.FromHex("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
				},
				Data: []byte{0x01, 0x02, 0x03, 0x04},
			},
		},
	}
}

// buildFreezeLogs returns the eth_getLogs / eth_getFilterLogs result set, in
// the RPCLog shape the handler returns to clients. The hashes are derived from
// the freeze block so the log entry is internally consistent with it.
func buildFreezeLogs(block *types.Block) []*jsonrpc.RPCLog {
	blockHash := block.Hash()
	txHash := block.Transactions()[0].Hash()
	// hex20 of the 21-byte contract address = trailing 20 bytes.
	contract21 := common.FromHex(freezeContractAddr21)
	return []*jsonrpc.RPCLog{
		{
			Address:          hex0x(contract21[len(contract21)-20:]),
			Topics:           []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
			Data:             "0x01020304",
			BlockNumber:      "0x64",
			BlockTimestamp:   "0x6553f100",
			TransactionHash:  hex0x(txHash[:]),
			TransactionIndex: "0x0",
			BlockHash:        hex0x(blockHash[:]),
			LogIndex:         "0x0",
			Removed:          false,
		},
	}
}

// newFreezeBackend assembles the fully-seeded stubBackend used by the freeze
// corpus. Reuses the existing test double (api_test.go) verbatim.
func newFreezeBackend() *stubBackend {
	block := buildFreezeBlock()
	tx := block.Transactions()[0]
	return &stubBackend{
		chainID:     freezeChainID,
		blockNumber: freezeBlockNumber,
		block:       block,
		balance:     freezeBalanceSUN,
		code:        []byte{0x60, 0x80, 0x60, 0x40, 0x52},
		storage:     common.BytesToHash(common.FromHex("0x00000000000000000000000000000000000000000000000000000000deadbeef")),
		tx:          tx.Proto(),
		txBlock:     block,
		txIndex:     0,
		txInfo:      buildFreezeTxInfo(),
		callResult:  common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000001"),
		logs:        buildFreezeLogs(block),
		gasPrice:    freezeGasPrice,
		peerCount:   freezePeerCount,
	}
}

// freezeBlockHashHex / freezeTxHashHex expose the derived hashes so the corpus
// generator can build by-hash requests that actually resolve against the block.
func freezeBlockHashHex() string {
	b := buildFreezeBlock()
	h := b.Hash()
	return hex0x(h[:])
}

func freezeTxHashHex() string {
	b := buildFreezeBlock()
	txHash := b.Transactions()[0].Hash()
	return hex0x(txHash[:])
}
