package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBlockChainRestartSyncFromHeightRebuildsMaterializedState(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witness := testInsertAddr(1)
	owner := testInsertAddr(2)
	receiver := testInsertAddr(3)
	genesisBalance := int64(100_000_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witness, Balance: 1_000_000},
			{Address: owner, Balance: genesisBalance},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witness, VoteCount: 1000, URL: "http://w"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}

	blocks := make([]*types.Block, 6)
	blocks[0] = bc.CurrentBlock()
	var tx4Hash tcommon.Hash
	for i := uint64(1); i <= 5; i++ {
		parent := bc.CurrentBlock()
		var txs []*corepb.Transaction
		if i == 4 {
			tx := testRestartTransferTx(t, owner, receiver, 7_000_000)
			tx4Hash = types.NewTransactionFromPB(tx).Hash()
			txs = append(txs, tx)
		}
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witness.Bytes(),
					Version:        params.BlockVersion,
				},
			},
			Transactions: txs,
		})
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("InsertBlock(%d): %v", i, err)
		}
		blocks[i] = block
	}
	if got := bc.CurrentBlock().Number(); got != 5 {
		t.Fatalf("precondition head = %d, want 5", got)
	}
	if got := readWitnessLatestBlockAtHead(t, bc, witness); got != 5 {
		t.Fatalf("precondition witness latest = %d, want 5", got)
	}
	if info := rawdb.ReadTransactionInfo(bc.ChainDB(), tx4Hash[:]); info == nil {
		t.Fatal("precondition tx4 info missing")
	}
	rawdb.WriteLatestPbftBlockNum(diskdb, 5)
	if got := rawdb.ReadLatestPbftBlockNum(diskdb); got != 5 {
		t.Fatalf("precondition latest PBFT = %d, want 5", got)
	}

	var progress []coreRestartEvent
	err = bc.RestartSyncFromHeight(2, genesis, nil, func(p RestartSyncProgress) {
		progress = append(progress, coreRestartEvent{phase: p.Phase, block: p.Block})
	})
	if err != nil {
		t.Fatalf("RestartSyncFromHeight: %v", err)
	}

	if got := bc.CurrentBlock().Number(); got != 2 {
		t.Fatalf("head number = %d, want 2", got)
	}
	if got := bc.CurrentBlock().Hash(); got != blocks[2].Hash() {
		t.Fatalf("head hash = %x, want block2 %x", got, blocks[2].Hash())
	}
	if got := rawdb.ReadHeadBlockHash(diskdb); got != blocks[2].Hash() {
		t.Fatalf("disk head hash = %x, want %x", got, blocks[2].Hash())
	}
	if got := bc.GetBlockByNumber(3); got != nil {
		t.Fatalf("block 3 should be hidden above rewound head, got %x", got.Hash())
	}
	if got := bc.DynProps().LatestBlockHeaderNumber(); got != 2 {
		t.Fatalf("dynprops latest block = %d, want 2", got)
	}
	if got := readWitnessLatestBlockAtHead(t, bc, witness); got != 2 {
		t.Fatalf("witness latest after rewind = %d, want 2", got)
	}
	w := readWitnessAtHead(t, bc, witness)
	if got := w.TotalProduced(); got != 2 {
		t.Fatalf("witness total produced after rewind = %d, want 2", got)
	}
	if info := rawdb.ReadTransactionInfo(bc.ChainDB(), tx4Hash[:]); info != nil {
		t.Fatalf("future tx info survived rewind: block=%d", info.BlockNumber)
	}
	if idx := rawdb.ReadTransactionIndex(bc.ChainDB(), tx4Hash[:]); idx != nil {
		t.Fatalf("future tx index survived rewind: %d", *idx)
	}
	if got := rawdb.ReadLatestPbftBlockNum(diskdb); got != -1 {
		t.Fatalf("future latest PBFT survived rewind: %d", got)
	}
	headState, err := state.New(bc.HeadStateRoot(), sdb)
	if err != nil {
		t.Fatalf("open rewound state: %v", err)
	}
	if got := headState.GetBalance(owner); got != genesisBalance {
		t.Fatalf("owner balance after rewind = %d, want genesis balance %d", got, genesisBalance)
	}
	if len(progress) == 0 || progress[len(progress)-1] != (coreRestartEvent{phase: "done", block: 2}) {
		t.Fatalf("progress did not finish at done/2: %+v", progress)
	}
}

type coreRestartEvent struct {
	phase string
	block uint64
}

func testRestartTransferTx(t *testing.T, from, to tcommon.Address, amount int64) *corepb.Transaction {
	t.Helper()
	tc := &contractpb.TransferContract{
		OwnerAddress: from.Bytes(),
		ToAddress:    to.Bytes(),
		Amount:       amount,
	}
	param, err := anypb.New(tc)
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	return &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	}
}
