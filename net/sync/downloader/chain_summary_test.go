package downloader

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newTestChain spins up an in-memory chain seeded with `numBlocks`
// trivially-linked blocks on top of genesis. Mirrors makeTestChain /
// makeChainWithBlocks in net/ but kept local so the package has no test
// dependency on the net package.
func newTestChain(t *testing.T, numBlocks int) *core.BlockChain {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: tcommon.Address{0x41, 1}, Balance: 1_000_000},
		},
	}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= numBlocks; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlockWithoutVerify(block); err != nil {
			t.Fatalf("insert #%d: %v", i, err)
		}
	}
	return bc
}

func TestBuildChainSummaryGenesisOnly(t *testing.T) {
	bc := newTestChain(t, 0)
	summary := BuildChainSummary(bc)
	if len(summary) != 1 {
		t.Fatalf("expected 1 entry in chain summary, got %d", len(summary))
	}
	if summary[0].Number() != 0 {
		t.Fatalf("expected genesis in summary, got block #%d", summary[0].Number())
	}
}

func TestBuildChainSummaryAscendingOrder(t *testing.T) {
	bc := newTestChain(t, 10)
	summary := BuildChainSummary(bc)
	// Ascending order — java-tron's SyncBlockChainMsgHandler.check enforces
	// summary[last].num >= peer.lastSyncBlockId.num, so the head must be
	// last and genesis must be first.
	if got := summary[0].Number(); got != 0 {
		t.Fatalf("first summary entry should be genesis (#0), got #%d", got)
	}
	last := summary[len(summary)-1]
	if last.Number() != 10 {
		t.Fatalf("last summary entry should be head (#10), got #%d", last.Number())
	}
	for i := 1; i < len(summary); i++ {
		if summary[i].Number() <= summary[i-1].Number() {
			t.Fatalf("summary not strictly ascending at i=%d: %d -> %d",
				i, summary[i-1].Number(), summary[i].Number())
		}
	}
}

func TestBuildChainSummaryExponentialSpacing(t *testing.T) {
	bc := newTestChain(t, 32)
	summary := BuildChainSummary(bc)
	// After reversal the gap between consecutive entries (starting from the
	// head) doubles: head, head-1, head-2, head-4, head-8, ..., 0.
	// Walking back-to-front, gaps must form 1,2,4,8,...
	n := len(summary)
	if n < 3 {
		t.Fatalf("expected at least 3 entries with 32 blocks, got %d", n)
	}
	step := uint64(1)
	for i := n - 2; i >= 0; i-- {
		gap := summary[i+1].Number() - summary[i].Number()
		// The final hop to genesis can be shorter than `step` because the
		// loop in BuildChainSummary clamps `num` to 0 when num < step.
		if i == 0 {
			if gap > step {
				t.Fatalf("genesis hop too large: gap=%d step=%d", gap, step)
			}
			break
		}
		if gap != step {
			t.Fatalf("gap[%d]=%d, want %d (step)", i, gap, step)
		}
		step *= 2
	}
}

func TestFindCommonBlockSharedPrefix(t *testing.T) {
	bc := newTestChain(t, 5)
	block3 := bc.GetBlockByNumber(3)
	block0 := bc.GetBlockByNumber(0)
	peerSummary := []types.BlockID{block3.ID(), block0.ID()}
	commonNum := FindCommonBlock(bc, peerSummary)
	if commonNum != 3 {
		t.Fatalf("expected common block #3, got #%d", commonNum)
	}
}

func TestFindCommonBlockNoMatch(t *testing.T) {
	bc := newTestChain(t, 0)
	fakeID := types.BlockID{Hash: tcommon.Hash{0xFF}, Num: 100}
	commonNum := FindCommonBlock(bc, []types.BlockID{fakeID})
	if commonNum != 0 {
		t.Fatalf("expected common block #0 (genesis fallback), got #%d", commonNum)
	}
}

func TestFindCommonBlockSummaryContainsHead(t *testing.T) {
	bc := newTestChain(t, 7)
	head := bc.CurrentBlock()
	peerSummary := []types.BlockID{head.ID()}
	if got := FindCommonBlock(bc, peerSummary); got != head.Number() {
		t.Fatalf("expected head #%d, got #%d", head.Number(), got)
	}
}

func TestFindCommonBlockSkipsBlocksAboveHead(t *testing.T) {
	bc := newTestChain(t, 3)
	// A summary entry numbered above our head should be ignored; the
	// genesis fallback returns 0 if nothing else matches.
	future := types.BlockID{Hash: tcommon.Hash{0xAB}, Num: 100}
	if got := FindCommonBlock(bc, []types.BlockID{future}); got != 0 {
		t.Fatalf("expected 0 when summary entry above head, got #%d", got)
	}
	// But a later entry that does match should win.
	block2 := bc.GetBlockByNumber(2)
	if got := FindCommonBlock(bc, []types.BlockID{future, block2.ID()}); got != 2 {
		t.Fatalf("expected #2 (skipping future), got #%d", got)
	}
}
