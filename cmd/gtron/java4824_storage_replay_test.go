package main

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestJava4824StorageProposalReplay(t *testing.T) {
	const dir = "testdata/java-4.8.2.4-storage"
	var oracle struct {
		BlockIDs        []string         `json:"block_ids"`
		Contract        string           `json:"contract"`
		ActivationBlock uint64           `json:"activation_block"`
		Properties      map[string]int64 `json:"properties"`
		Storage         []struct {
			Block uint64 `json:"block"`
			Slot1 byte   `json:"slot_1"`
		} `json:"storage"`
		Receipts []struct {
			Name           string   `json:"name"`
			ID             string   `json:"id"`
			Block          int64    `json:"block"`
			Fee            int64    `json:"fee"`
			Energy         int64    `json:"energy"`
			Result         string   `json:"result"`
			ContractResult []string `json:"contract_result"`
		} `json:"receipts"`
	}
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if err := json.Unmarshal(read("oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if len(oracle.BlockIDs) != 58 || len(oracle.Receipts) != 7 || len(oracle.Storage) != 4 {
		t.Fatal("incomplete Java oracle")
	}
	genesis, err := loadGenesisFile(filepath.Join(dir, "genesis.json"))
	if err != nil {
		t.Fatal(err)
	}
	db := ethrawdb.NewMemoryDatabase()
	if _, _, err := core.SetupGenesisBlock(db, genesis); err != nil {
		t.Fatal(err)
	}
	chain, err := core.NewBlockChain(db, state.NewDatabase(db), genesis.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer chain.Close()
	chain.SetEngine(dpos.New(chain))
	backend := core.NewTronBackend(chain, nil)
	if got := chain.CurrentBlock().Hash().Hex(); got != oracle.BlockIDs[0] {
		t.Fatalf("genesis ID = %s, Java = %s", got, oracle.BlockIDs[0])
	}

	addr, err := decodeAddress(oracle.Contract)
	if err != nil {
		t.Fatal(err)
	}
	slot := common.Hash{31: 1}
	wire := read("blocks.bin")
	for height := uint64(1); height < uint64(len(oracle.BlockIDs)); height++ {
		raw, size := protowire.ConsumeBytes(wire)
		if size < 0 {
			t.Fatalf("parse block %d: %v", height, protowire.ParseError(size))
		}
		wire = wire[size:]
		var pb corepb.Block
		if err := proto.Unmarshal(raw, &pb); err != nil {
			t.Fatal(err)
		}
		block := types.NewBlockFromPB(&pb)
		if block.Number() != height || block.Hash().Hex() != oracle.BlockIDs[height] {
			t.Fatalf("block %d ID = %s, Java = %s", height, block.Hash(), oracle.BlockIDs[height])
		}
		if err := chain.InsertBlock(block); err != nil {
			t.Fatalf("replay block %d: %v", height, err)
		}
		if got := chain.CurrentBlock().Hash(); got != block.Hash() {
			t.Fatalf("replayed head at block %d = %s, Java = %s", height, got, block.Hash())
		}
		dp := chain.DynProps()
		wantKeyProtection := height >= oracle.ActivationBlock
		if dp.AllowOptimizeTvmStorage() || dp.AllowOptimizeTvmStorageKey() != wantKeyProtection {
			t.Fatalf("block %d: proposal 99=%t 102=%t, want 99=false 102=%t", height,
				dp.AllowOptimizeTvmStorage(), dp.AllowOptimizeTvmStorageKey(), wantKeyProtection)
		}
		for _, want := range oracle.Storage {
			if want.Block == height {
				value, err := backend.GetStorageAt(addr, slot)
				if err != nil || value != (common.Hash{31: want.Slot1}) {
					t.Fatalf("block %d slot 1 = %s (%v), want %d", height, value, err, want.Slot1)
				}
			}
		}
	}
	if len(wire) != 0 {
		t.Fatalf("fixture includes %d trailing bytes", len(wire))
	}
	for key, want := range oracle.Properties {
		if got := chain.DynProps().All()[key]; got != want {
			t.Errorf("%s = %d, Java = %d", key, got, want)
		}
	}
	for _, want := range oracle.Receipts {
		id, err := hex.DecodeString(want.ID)
		if err != nil {
			t.Fatal(err)
		}
		info, err := backend.GetTransactionInfoByID(common.Hash(id))
		if err != nil || info == nil {
			t.Fatalf("%s receipt: %v", want.Name, err)
		}
		result := make([]string, len(info.GetContractResult()))
		for i, bytes := range info.GetContractResult() {
			result[i] = hex.EncodeToString(bytes)
		}
		if got := info.GetReceipt(); info.GetBlockNumber() != want.Block || info.GetFee() != want.Fee ||
			got.GetEnergyUsageTotal() != want.Energy || got.GetResult().String() != want.Result ||
			!slices.Equal(result, want.ContractResult) {
			t.Errorf("%s receipt: block=%d fee=%d energy=%d result=%s output=%q, Java=%+v",
				want.Name, info.GetBlockNumber(), info.GetFee(), got.GetEnergyUsageTotal(), got.GetResult(), result, want)
		}
	}
}
