package net

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSyncServiceImportsTailBlockAfterSnapshotFreezerBoundary(t *testing.T) {
	restored, fz, storedBlock1 := newNetSyncSnapshotRestoredBoundary(t, 1)
	defer restored.Close()
	defer fz.Close()
	if got := restored.CurrentBlock(); got == nil || got.Hash() != storedBlock1.Hash() {
		t.Fatalf("restored head = %v, want freezer boundary %x", got, storedBlock1.Hash())
	}

	ss := NewSyncService(restored, nil)
	peer := p2p.NewPeer(nil, "snapshot-tail-peer", false, nil)
	block2 := syncSnapshotTestBlock(2, storedBlock1.Hash())
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	markPendingLocked(ss, ps, block2.ID())
	ss.mu.Unlock()

	if !ss.HandleBlock(peer, block2, nil) {
		t.Fatal("snapshot-boundary tail block was not consumed by sync")
	}
	if got := restored.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("restored sync head = %v, want block2 %x", got, block2.Hash())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(restored.DB(), rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != block2.Number() || !row.HasBlockHash || row.BlockHash != block2.Hash() {
		t.Fatalf("snapshot-boundary sync import stage = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if paused, at, _, err := ss.PausedStatus(); paused || err != nil {
		t.Fatalf("sync paused after boundary tail import: paused=%v at=%d err=%v", paused, at, err)
	}
}

func TestTwoNodeSyncFromSnapshotFreezerBoundary(t *testing.T) {
	bcA := newNetSyncSnapshotExecutedChain(t, 3)
	defer bcA.Close()
	bcB, fzB, boundary := newNetSyncSnapshotRestoredBoundary(t, 1)
	defer bcB.Close()
	defer fzB.Close()
	if got := bcA.GetBlockByNumber(boundary.Number()); got == nil || got.Hash() != boundary.Hash() {
		t.Fatalf("source boundary = %v, want %x", got, boundary.Hash())
	}

	broadcasterA := NewBroadcastService(nil)
	broadcasterB := NewBroadcastService(nil)
	handlerA := NewTronHandler(bcA, txpool.New(), broadcasterA)
	handlerB := NewTronHandler(bcB, txpool.New(), broadcasterB)
	syncA := NewSyncService(bcA, handlerA)
	syncB := NewSyncService(bcB, handlerB)
	handlerA.SetSyncService(syncA)
	handlerB.SetSyncService(syncB)

	srvA := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerA)
	srvB := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerB)
	handlerA.SetServer(srvA)
	handlerB.SetServer(srvB)
	broadcasterA.SetPeersFunc(handlerA.HandshakedPeers)
	broadcasterB.SetPeersFunc(handlerB.HandshakedPeers)
	srvA.Start()
	defer srvA.Stop()
	srvB.Start()
	defer srvB.Stop()

	srvB.AddPeer(srvA.ListenAddr())
	want := bcA.CurrentBlock()
	// Full-repository tests run many CPU- and disk-heavy packages concurrently;
	// leave enough wall time for the two loopback servers to handshake under
	// scheduler pressure while keeping the poll interval and correctness gate.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if got := bcB.CurrentBlock(); got != nil && got.Hash() == want.Hash() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := bcB.CurrentBlock(); got == nil || got.Hash() != want.Hash() {
		t.Fatalf("snapshot-boundary node synced to %v, want source tip #%d %x", got, want.Number(), want.Hash())
	}
}

func newNetSyncSnapshotGenesis() *params.Genesis {
	return &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: tcommon.Address{0x41, 1}, Balance: 1_000_000},
		},
	}
}

func newNetSyncSnapshotExecutedChain(t *testing.T, blocks uint64) *core.BlockChain {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	if _, _, err := core.SetupGenesisBlock(db, newNetSyncSnapshotGenesis()); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := core.NewBlockChain(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	for n := uint64(1); n <= blocks; n++ {
		block := syncSnapshotTestBlock(n, bc.CurrentBlock().Hash())
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("InsertBlock(%d): %v", n, err)
		}
	}
	bc.WaitForFlushSettled()
	return bc
}

func newNetSyncSnapshotRestoredBoundary(t *testing.T, boundary uint64) (*core.BlockChain, *rawdbfreezer.Freezer, *types.Block) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	if _, _, err := core.SetupGenesisBlock(db, newNetSyncSnapshotGenesis()); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	source, err := core.NewBlockChain(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain(source): %v", err)
	}
	for n := uint64(1); n <= boundary; n++ {
		block := syncSnapshotTestBlock(n, source.CurrentBlock().Hash())
		if err := source.InsertBlock(block); err != nil {
			t.Fatalf("InsertBlock(%d source): %v", n, err)
		}
	}
	source.WaitForFlushSettled()
	if err := source.Close(); err != nil {
		t.Fatalf("close source chain: %v", err)
	}

	hotOnly := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	rows := make([]netSyncSnapshotFreezerRow, 0, boundary+1)
	for n := uint64(0); n <= boundary; n++ {
		block := rawdb.ReadBlock(hotOnly, n)
		if block == nil {
			t.Fatalf("source block %d missing after insert", n)
		}
		var root tcommon.Hash
		if n == 0 {
			root = rawdb.ReadGenesisStateRoot(db)
		} else {
			root = rawdb.ReadBlockStateRoot(hotOnly, block.Hash())
		}
		if root == (tcommon.Hash{}) {
			t.Fatalf("source block %d state root missing", n)
		}
		rows = append(rows, netSyncSnapshotFreezerRow{block: block, stateRoot: root.Bytes()})
	}
	fz := openNetSyncSnapshotFreezer(t, filepath.Join(t.TempDir(), "freezer"))
	appendNetSyncSnapshotFreezerRows(t, fz, rows)
	boundaryBlock := rows[len(rows)-1].block
	if err := rawdb.WriteStateTxRange(db, boundaryBlock.Number(), boundaryBlock.Hash(), boundary, boundary); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	if _, err := statesnapshots.InstallCanonicalBoundaryFromVerifiedSnapshot(db, rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz)), statesnapshots.NewManifest(boundary, boundary, nil)); err != nil {
		t.Fatalf("InstallCanonicalBoundaryFromVerifiedSnapshot: %v", err)
	}
	restored, err := core.NewBlockChainWithAncient(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChain(restored): %v", err)
	}
	return restored, fz, boundaryBlock
}

func syncSnapshotTestBlock(number uint64, parent tcommon.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(number),
				Timestamp:  int64(number) * 3000,
				ParentHash: parent.Bytes(),
			},
			WitnessSignature: make([]byte, 65),
		},
	})
}

type netSyncSnapshotFreezerRow struct {
	block     *types.Block
	stateRoot []byte
}

func openNetSyncSnapshotFreezer(t *testing.T, dir string) *rawdbfreezer.Freezer {
	t.Helper()
	fz, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	return fz
}

func appendNetSyncSnapshotFreezerRows(t *testing.T, fz *rawdbfreezer.Freezer, rows []netSyncSnapshotFreezerRow) {
	t.Helper()
	if _, err := fz.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for i, row := range rows {
			if row.block == nil {
				return fmt.Errorf("nil block at row %d", i)
			}
			n := row.block.Number()
			if n != uint64(i) {
				return fmt.Errorf("row %d has block number %d", i, n)
			}
			blockRaw, err := row.block.Marshal()
			if err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, blockRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, nil); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, row.stateRoot); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := fz.Sync(); err != nil {
		t.Fatalf("Sync freezer: %v", err)
	}
}
