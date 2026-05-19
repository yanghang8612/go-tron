package core

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethdb "github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// Slice 6 of the State History Index: operator-recovery backfill.
//
// These tests lock in the four Slice-6 acceptance properties:
//
//   - MatchesLiveCapture: backfilling a chain that was synced WITHOUT history
//     capture produces byte-identical sh-* rows to a chain synced WITH live
//     capture. This is the central parity assertion — it proves the backfill
//     drives the same AccumulateHistory journal path as live applyBlock.
//   - Idempotent: re-running backfill over an already-indexed range overwrites
//     every row with byte-identical content.
//   - Resumable: an interrupted backfill resumes from the persisted cursor and
//     ends with full coverage.
//   - RefusesUnreconstructible: a range whose parent state can't be opened
//     (pruned / unknown root) is refused with a clear typed error before any
//     row is written.
//
// Fixture: a single-witness chain with far-future maintenance time so
// solidified == head (buffer flushes to disk on every insert) and no
// maintenance-boundary mutations fire. The maintenance caveat documented on
// BackfillHistory means a maintenance-crossing range would NOT byte-match;
// these fixtures deliberately suppress maintenance, matching slice 2/4.

// shPrefixes enumerates every State History Index key family for full-DB
// row snapshots. sh-cfg-/sh-bf-cursor- are intentionally EXCLUDED: the config
// sentinel is owned by the live writer / pruner (absent on a backfill-only
// chain) and the cursor is backfill bookkeeping, not a derived history row.
var shPrefixes = [][]byte{
	[]byte("sh-m-"),
	[]byte("sh-a-"),
	[]byte("sh-s-"),
	[]byte("sh-i-a-"),
	[]byte("sh-i-s-"),
}

// snapshotHistoryRows reads every sh-* derived row from db into a sorted,
// stable key->value map suitable for byte-for-byte comparison.
//
// Callers pass the chain's buffer overlay (bc.buffer), NOT the bare disk db:
// in the single-witness fixture solidified stays at 0, so live-captured sh-*
// rows live in the in-memory buffer layers and never flush to disk. The
// buffer's NewIterator merges its pending layers over the disk base, so a
// buffer snapshot sees the logically-complete set regardless of flush state.
// The backfill chain writes sh-* rows straight to disk; reading through its
// buffer falls through to the base and finds them just the same — so the
// comparison is symmetric (logical rows vs logical rows).
func snapshotHistoryRows(t *testing.T, db ethdb.Iteratee) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	for _, prefix := range shPrefixes {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			k := string(append([]byte(nil), it.Key()...))
			v := append([]byte(nil), it.Value()...)
			out[k] = v
		}
		it.Release()
	}
	return out
}

// assertRowsEqual compares two history-row snapshots key-by-key and value-by-
// value, reporting the first divergence in each direction.
func assertRowsEqual(t *testing.T, want, got map[string][]byte, ctx string) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf("%s: row count mismatch: want %d, got %d", ctx, len(want), len(got))
	}
	// Sort keys for deterministic first-divergence reporting.
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := want[k]
		g, ok := got[k]
		if !ok {
			t.Errorf("%s: key %x present in want, missing in got", ctx, k)
			continue
		}
		if !bytes.Equal(w, g) {
			t.Errorf("%s: value mismatch at key %x: want %x, got %x", ctx, k, w, g)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s: key %x present in got, missing in want", ctx, k)
		}
	}
}

// backfillChainSpec describes a deterministic synthetic chain the backfill
// tests replay. Each entry is a single transfer; block numbers are 1-based.
type backfillTransfer struct {
	number int64
	ts     int64
	amount int64
}

// defaultBackfillChain is a short chain that touches a stable account set so
// the sh-* rows are non-trivial (sender + receiver per block) yet small.
var defaultBackfillChain = []backfillTransfer{
	{number: 1, ts: 3000, amount: 5_000_000},
	{number: 2, ts: 6000, amount: 1_500_000},
	{number: 3, ts: 9000, amount: 2_250_000},
}

// buildBackfillGenesis returns a single-witness genesis with HistoryEnabled
// set per the argument and far-future maintenance.
func buildBackfillGenesis(history bool) (*params.Genesis, *params.ChainConfig) {
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = history
	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance noise
		},
	}
	return genesis, cfg
}

// buildAndInsertChain stands up a blockchain on a fresh memorydb, inserts the
// transfer chain, and returns the disk db + chain. When history is true the
// live-capture path runs; when false no sh-* rows are written during insert.
func buildAndInsertChain(t *testing.T, history bool, chain []backfillTransfer) (ethdb.Database, *BlockChain) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis, cfg := buildBackfillGenesis(history)

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatalf("setup genesis: %v", err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatalf("new blockchain: %v", err)
	}

	parentHash := genesisHash
	for _, e := range chain {
		block := buildTransferBlock(t, e.number, e.ts, parentHash, testInsertAddr(1), e.amount)
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("insert block %d: %v", e.number, err)
		}
		parentHash = block.Hash()
	}
	// Drain async buffer flushes so all sh-* rows (when history is on) and all
	// block-state-root side-store entries are durably on disk before backfill
	// reads them.
	bc.WaitForFlushSettled()
	return diskdb, bc
}

// TestBackfill_MatchesLiveCapture is the central parity assertion. A chain
// synced WITH live capture and the SAME chain synced WITHOUT capture +
// backfilled must produce byte-identical sh-* rows.
func TestBackfill_MatchesLiveCapture(t *testing.T) {
	// Live-capture reference chain. Snapshot through the buffer overlay: the
	// single-witness fixture keeps solidified at 0, so live-captured rows are
	// in the buffer layers, not yet on disk.
	_, liveBC := buildAndInsertChain(t, true, defaultBackfillChain)
	wantRows := snapshotHistoryRows(t, liveBC.buffer)
	if len(wantRows) == 0 {
		t.Fatal("live-capture chain wrote no sh-* rows; fixture broken")
	}

	// Backfill chain: synced WITHOUT capture, so no sh-* rows yet (on disk or
	// in the buffer).
	bfDB, bfBC := buildAndInsertChain(t, false, defaultBackfillChain)
	preRows := snapshotHistoryRows(t, bfBC.buffer)
	if len(preRows) != 0 {
		t.Fatalf("backfill chain has %d sh-* rows before backfill; capture leaked", len(preRows))
	}

	to := uint64(len(defaultBackfillChain))
	if err := bfBC.BackfillHistory(1, to, false, nil); err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	bfBC.WaitForFlushSettled()

	// Backfill writes straight to disk; reading through the buffer falls
	// through to the base and finds them, so the comparison is symmetric.
	gotRows := snapshotHistoryRows(t, bfBC.buffer)
	assertRowsEqual(t, wantRows, gotRows, "backfill-vs-live")

	// Sanity: backfill rows are genuinely on disk (not merely buffered).
	if len(snapshotHistoryRows(t, bfDB)) != len(gotRows) {
		t.Errorf("backfill rows not durable on disk: disk=%d buffer=%d",
			len(snapshotHistoryRows(t, bfDB)), len(gotRows))
	}
}

// TestBackfill_Idempotent runs the backfill twice over the same range and
// asserts the second run leaves byte-identical rows (deterministic capture).
func TestBackfill_Idempotent(t *testing.T) {
	bfDB, bfBC := buildAndInsertChain(t, false, defaultBackfillChain)
	to := uint64(len(defaultBackfillChain))

	if err := bfBC.BackfillHistory(1, to, false, nil); err != nil {
		t.Fatalf("first BackfillHistory: %v", err)
	}
	bfBC.WaitForFlushSettled()
	firstRows := snapshotHistoryRows(t, bfDB)
	if len(firstRows) == 0 {
		t.Fatal("first backfill wrote no rows")
	}

	if err := bfBC.BackfillHistory(1, to, false, nil); err != nil {
		t.Fatalf("second BackfillHistory: %v", err)
	}
	bfBC.WaitForFlushSettled()
	secondRows := snapshotHistoryRows(t, bfDB)

	assertRowsEqual(t, firstRows, secondRows, "idempotent-rerun")
}

// TestBackfill_Resumable simulates an interrupt: it manually sets the resume
// cursor to a mid-range block, runs a resume backfill, and asserts the loop
// covers the whole range. It also verifies that a resumed run produces rows
// identical to a clean full run.
func TestBackfill_Resumable(t *testing.T) {
	// Reference: clean full backfill on its own chain.
	refDB, refBC := buildAndInsertChain(t, false, defaultBackfillChain)
	to := uint64(len(defaultBackfillChain))
	if err := refBC.BackfillHistory(1, to, false, nil); err != nil {
		t.Fatalf("reference BackfillHistory: %v", err)
	}
	refBC.WaitForFlushSettled()
	refRows := snapshotHistoryRows(t, refDB)

	// Subject chain: backfill blocks 1..1, then simulate a crash by leaving
	// the cursor at 1, then resume — the resume must cover 2..to.
	bfDB, bfBC := buildAndInsertChain(t, false, defaultBackfillChain)
	if err := bfBC.BackfillHistory(1, 1, false, nil); err != nil {
		t.Fatalf("partial BackfillHistory [1,1]: %v", err)
	}
	bfBC.WaitForFlushSettled()

	cursor, ok := rawdb.ReadHistoryBackfillCursor(bfDB)
	if !ok || cursor != 1 {
		t.Fatalf("after partial backfill, cursor = %d (ok=%v), want 1", cursor, ok)
	}

	// Resume over the full range; the cursor must advance us to start at 2.
	var visited []uint64
	if err := bfBC.BackfillHistory(1, to, true, func(p BackfillProgress) {
		visited = append(visited, p.Block)
	}); err != nil {
		t.Fatalf("resume BackfillHistory: %v", err)
	}
	bfBC.WaitForFlushSettled()

	// The resume must have processed exactly blocks 2..to (not block 1 again).
	wantVisited := make([]uint64, 0, to-1)
	for n := uint64(2); n <= to; n++ {
		wantVisited = append(wantVisited, n)
	}
	if len(visited) != len(wantVisited) {
		t.Fatalf("resume visited %v, want %v", visited, wantVisited)
	}
	for i := range wantVisited {
		if visited[i] != wantVisited[i] {
			t.Fatalf("resume visited %v, want %v", visited, wantVisited)
		}
	}

	finalCursor, _ := rawdb.ReadHistoryBackfillCursor(bfDB)
	if finalCursor != to {
		t.Errorf("final cursor = %d, want %d", finalCursor, to)
	}

	// Full coverage: resumed rows match the clean full-run rows.
	gotRows := snapshotHistoryRows(t, bfDB)
	assertRowsEqual(t, refRows, gotRows, "resume-coverage")
}

// TestBackfill_RefusesUnreconstructible asserts the reconstructibility guard
// fires with a clear typed error when the parent state root for the requested
// range can't be opened. We synthesise the "state pruned below the range"
// condition by overwriting block 1's parent (genesis) state-root side entry
// with a root whose trie nodes don't exist — state.New then fails with a
// "missing trie node" error, exactly as it would on a node that pruned state
// below the requested floor. The pre-flight guard must refuse before writing
// any row.
func TestBackfill_RefusesUnreconstructible(t *testing.T) {
	bfDB, bfBC := buildAndInsertChain(t, false, defaultBackfillChain)
	to := uint64(len(defaultBackfillChain))

	// Genesis is block 1's parent. Point its state-root side entry at a root
	// that has no backing trie nodes. ReadBlockStateRoot returns this non-zero
	// bogus root (so the genesis-root and proto fallbacks are skipped), and
	// state.New(bogusRoot) fails to open the trie.
	genesis := bfBC.GetBlockByNumber(0)
	if genesis == nil {
		t.Fatal("genesis block missing")
	}
	var bogus tcommon.Hash
	bogus[0] = 0xDE
	bogus[31] = 0xAD
	rawdb.WriteBlockStateRoot(bfDB, genesis.Hash(), bogus)

	err := bfBC.BackfillHistory(1, to, false, nil)
	if err == nil {
		t.Fatal("expected unreconstructible error, got nil")
	}
	var unrec *errBackfillUnreconstructible
	if !errors.As(err, &unrec) {
		t.Fatalf("expected *errBackfillUnreconstructible, got %T: %v", err, err)
	}

	// No rows should have been written when the pre-flight guard refused.
	rows := snapshotHistoryRows(t, bfDB)
	if len(rows) != 0 {
		t.Errorf("unreconstructible backfill wrote %d rows; guard should refuse before writing", len(rows))
	}
}
