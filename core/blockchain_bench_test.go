package core

import (
	"encoding/binary"
	"math/rand"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func benchAccount(i int) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	binary.BigEndian.PutUint64(a[1:9], uint64(i+1))
	return a
}

// buildTransferChain returns a funded genesis plus a factory that builds
// numBlocks chained blocks (parent-linked from genHash), each with txPerBlock
// transfer txs spread across numAccounts funded accounts (each account sends
// rarely so free bandwidth mostly covers it).
func buildTransferChain(numAccounts, numBlocks, txPerBlock int) (*params.Genesis, func(tcommon.Hash) []*types.Block) {
	genAccounts := make([]params.GenesisAccount, numAccounts)
	for i := range genAccounts {
		genAccounts[i] = params.GenesisAccount{Address: benchAccount(i), Balance: 1_000_000_000_000}
	}
	genesis := &params.Genesis{Config: params.MainnetChainConfig, Accounts: genAccounts, DynamicProperties: map[string]int64{}}

	mk := func(genHash tcommon.Hash) []*types.Block {
		rng := rand.New(rand.NewSource(1))
		blocks := make([]*types.Block, 0, numBlocks)
		parent := genHash
		for blk := 1; blk <= numBlocks; blk++ {
			txs := make([]*corepb.Transaction, 0, txPerBlock)
			for j := 0; j < txPerBlock; j++ {
				fromIdx := (blk*txPerBlock + j) % numAccounts
				toIdx := rng.Intn(numAccounts)
				if toIdx == fromIdx {
					toIdx = (toIdx + 1) % numAccounts
				}
				from := benchAccount(fromIdx)
				to := benchAccount(toIdx)
				param, _ := anypb.New(&contractpb.TransferContract{
					OwnerAddress: from.Bytes(), ToAddress: to.Bytes(), Amount: 1,
				})
				txs = append(txs, &corepb.Transaction{RawData: &corepb.TransactionRaw{
					Expiration: int64(blk)*3000 + 600000,
					Contract:   []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: param}},
				}})
			}
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: int64(blk), Timestamp: int64(blk) * 3000, ParentHash: parent.Bytes(),
				}},
				Transactions: txs,
			})
			blocks = append(blocks, block)
			parent = block.Hash()
		}
		return blocks
	}
	return genesis, mk
}

// BenchmarkInsertBlocksTransfers profiles the production block-insertion path
// (validate → execute → commit/commitment-fold → flush) on an account-heavy
// transfer workload, to find the post-parallel-fold bottleneck:
//
//	go test ./core -run '^$' -bench BenchmarkInsertBlocksTransfers -benchtime=4x -cpuprofile /tmp/insert.prof
func BenchmarkInsertBlocksTransfers(b *testing.B) {
	benchInsertBlocks(b, statedomains.ParallelFoldMinOps)
}
func BenchmarkInsertBlocksTransfersSeqFold(b *testing.B) { benchInsertBlocks(b, 1<<30) }

func benchInsertBlocks(b *testing.B, foldMinOps int) {
	prev := statedomains.ParallelFoldMinOps
	statedomains.ParallelFoldMinOps = foldMinOps
	defer func() { statedomains.ParallelFoldMinOps = prev }()

	const numAccounts, numBlocks, txPerBlock = 3000, 150, 80
	genesis, mk := buildTransferChain(numAccounts, numBlocks, txPerBlock)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		diskdb := ethrawdb.NewMemoryDatabase()
		_, genHash, err := SetupGenesisBlock(diskdb, genesis)
		if err != nil {
			b.Fatal(err)
		}
		blocks := mk(genHash)
		bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := bc.InsertBlocks(blocks); err != nil {
			b.Fatalf("InsertBlocks: %v", err)
		}
	}
}
