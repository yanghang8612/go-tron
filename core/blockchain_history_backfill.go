package core

import (
	"errors"
	"fmt"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

// BackfillProgress is the per-callback snapshot the backfill loop hands to a
// caller-supplied progress function. Block is the block just re-derived;
// From/To bound the requested range; Done is the count of blocks completed
// this run (resume-relative, not range-relative). Rate is blocks/sec since
// the run started.
type BackfillProgress struct {
	Block uint64
	From  uint64
	To    uint64
	Done  uint64
	Rate  float64
}

// errBackfillUnreconstructible wraps the reconstructibility-guard failure so
// the CLI can present a clean "state not available" message without leaking
// the underlying trie-open error shape.
type errBackfillUnreconstructible struct {
	block uint64
	root  tcommon.Hash
	cause error
}

func (e *errBackfillUnreconstructible) Error() string {
	return fmt.Sprintf("cannot backfill: parent state for block %d (root %x) is not available — "+
		"the node likely pruned state below this range; re-sync in archive mode or restore a snapshot that covers it: %v",
		e.block, e.root, e.cause)
}

func (e *errBackfillUnreconstructible) Unwrap() error { return e.cause }

// BackfillHistory re-derives the State History Index (sh-* rows) for every
// block in [from, to] by replaying each block's state transition over the
// canonical chain data and capturing the StateDB journal via the SAME
// AccumulateHistory path that live applyBlock uses.
//
// Option A (ProcessBlock-path replay), per the Slice 6 plan: for each block N
// we open a StateDB at N-1's post-state root, run ProcessBlock to repopulate
// the per-block journal, then call statedb.AccumulateHistory to write the
// pre-block deltas. We do NOT commit a new canonical state, advance the head,
// or touch bc.buffer — backfill is read-only with respect to the live chain
// and only appends sh-* index rows to disk.
//
// Isolation of non-history writes: ProcessBlock writes non-history rows
// (asset issues, proposals, contract state, nullifiers, block-reward
// allowances) through its BufferedKVStore argument. Re-running those against
// the real disk would double-increment counters and trip "already exists"
// guards on subsequent blocks. We therefore route every ProcessBlock write
// into a throwaway *blockbuffer.Buffer overlaid on bc.db (reads fall through
// to disk, writes stay in the scratch layer) and discard it after each block.
// Only AccumulateHistory writes land on real disk.
//
// Maintenance caveat: the maintenance-boundary mutations applyBlock performs
// AFTER ProcessBlock (ProcessProposals, applyRewardVI, applyPendingVotes,
// tryRemoveThePowerOfTheGr) are NOT replayed here. Those read witness vote
// tallies / proposal state / the active-witness set from flat-KV stores that
// hold only the CURRENT (post-HEAD) value — the as-of-N snapshot is not
// recoverable from disk, so replaying them would produce deltas computed
// against the wrong inputs rather than matching the live capture. A backfill
// over a range that crosses a maintenance boundary therefore reconstructs the
// per-tx and per-block-reward account/slot deltas exactly but omits the
// maintenance-only AccountDelta rows for the boundary block. Operators who
// need a byte-exact archive across maintenance boundaries must re-sync in
// archive mode. (The plan scopes Slice 6 to the ProcessBlock path for exactly
// this reason.)
//
// Read-side flat-KV caveat: the trie side is opened at the correct historical
// root via state.New(parentRoot), but ProcessBlock also reads NON-trie state
// (asset-issue / proposal / contract-meta / shielded-nullifier / exchange
// stores) through the scratch overlay, which falls through to bc.db — and
// bc.db's flat-KV holds HEAD state, not block N-1's snapshot. Trie-only txs
// (TransferContract, Freeze/Unfreeze, VoteWitness, WithdrawBalance — the vast
// majority of TRON traffic) replay cleanly. But an actuator that checks
// flat-KV for collision/dedup (AssetIssue, ProposalCreate, CreateSmartContract,
// ExchangeCreate, a shielded transfer's nullifier-seen guard) may reject the
// replay with an "already exists" / "already seen" error because live-apply
// long ago persisted that entity into the HEAD-state flat-KV. When that
// happens BackfillHistory returns the replay error for that block (it does NOT
// silently emit wrong rows). Backfilling a range dense with creation txs is
// therefore best done from a snapshot taken at `from-1`, or via a full archive
// re-sync. This is the same ProcessBlock-path limitation as the maintenance
// caveat, surfaced on the read side.
//
// Concurrency: BackfillHistory does not take bc.chainmu and must run with the
// node stopped. CLI callers are safe by construction — Pebble's exclusive
// datadir lock prevents a running gtron from opening the same database — so
// the tool cannot race a live applyBlock over bc.stateDB or the journal.
//
// Idempotency: the capture path is deterministic given the same block and the
// same parent state, so re-running over an already-indexed range overwrites
// each row with byte-identical content (WriteAccountDelta/WriteSlotDelta/...
// are Puts keyed by (block, addr[, slot])).
//
// Resumability: when resume is true the loop starts at max(from, cursor+1)
// where cursor is the persisted last-completed block. After every block the
// cursor advances on disk so an interrupt mid-range resumes without redoing
// completed work.
//
// progressFn, if non-nil, is invoked after each block completes. It must not
// block for long — it runs on the backfill goroutine.
func (bc *BlockChain) BackfillHistory(from, to uint64, resume bool, progressFn func(BackfillProgress)) error {
	if from == 0 {
		// Block 0 is genesis: it has no parent block to open a pre-state from
		// and the genesis state is materialised directly, not via a block
		// transition. There are no sh-* rows to derive for it.
		from = 1
	}
	if to < from {
		return fmt.Errorf("backfill: empty range [%d, %d] (to < from)", from, to)
	}

	head := bc.CurrentBlock()
	if head == nil {
		return errors.New("backfill: chain has no head block")
	}
	if to > head.Number() {
		return fmt.Errorf("backfill: range upper bound %d exceeds chain head %d", to, head.Number())
	}

	start := from
	if resume {
		if cursor, ok := rawdb.ReadHistoryBackfillCursor(bc.db); ok && cursor+1 > start {
			start = cursor + 1
		}
	}
	if start > to {
		// Everything in the requested range is already covered by the
		// persisted cursor — nothing to do. Not an error.
		log.Info("History backfill: range already covered by resume cursor",
			"from", from, "to", to, "cursor", start-1)
		return nil
	}

	log.Info("History backfill starting",
		"from", from, "to", to, "start", start, "resume", resume)

	// Pre-flight reconstructibility guard on the FIRST block we will replay.
	// Opening the parent StateDB is the authoritative check (a pruned state
	// floor surfaces as an OpenTrie error); doing it up front lets us refuse
	// the whole job with a clear message before writing any rows. Per-block
	// opens below re-run the same guard so a mid-range gap is also caught.
	if _, _, err := bc.openParentStateForBackfill(start); err != nil {
		return err
	}

	runStart := time.Now()
	var done uint64
	for n := start; n <= to; n++ {
		if err := bc.backfillOneBlock(n); err != nil {
			return err
		}
		// Persist the resume cursor AFTER the history rows for this block are
		// durably written, so a crash between rows and cursor re-does the
		// block (idempotent) rather than skipping it.
		if err := rawdb.WriteHistoryBackfillCursor(bc.db, n); err != nil {
			return fmt.Errorf("backfill: persist cursor at block %d: %w", n, err)
		}
		done++

		if progressFn != nil {
			elapsed := time.Since(runStart).Seconds()
			rate := 0.0
			if elapsed > 0 {
				rate = float64(done) / elapsed
			}
			progressFn(BackfillProgress{Block: n, From: from, To: to, Done: done, Rate: rate})
		}
	}

	log.Info("History backfill complete",
		"from", from, "to", to, "blocks", done,
		"elapsed", time.Since(runStart))
	return nil
}

// openParentStateForBackfill resolves block n's canonical record and opens a
// StateDB at its parent's post-state root. Returns the block, the opened
// StateDB (history capture already enabled), and a typed unreconstructible
// error if the parent state can't be opened.
func (bc *BlockChain) openParentStateForBackfill(n uint64) (*blockForBackfill, *state.StateDB, error) {
	block := bc.GetBlockByNumber(n)
	if block == nil {
		return nil, nil, fmt.Errorf("backfill: canonical block %d not found", n)
	}

	// Resolve the parent's post-state root. State roots live in a side store
	// keyed by block hash (java-tron parity: the block proto carries no
	// account_state_root), with the genesis-state-root fallback for block 1.
	parentHash := block.ParentHash()
	parentRoot := rawdb.ReadBlockStateRoot(bc.chaindb, parentHash)
	if parentRoot == (tcommon.Hash{}) && n == 1 {
		parentRoot = rawdb.ReadGenesisStateRoot(bc.db)
	}
	if parentRoot == (tcommon.Hash{}) {
		// Backwards-compat fallback for chain databases written before
		// blockStateRootPrefix existed: read it off the parent block proto.
		if parent := bc.GetBlockByNumber(n - 1); parent != nil {
			parentRoot = parent.AccountStateRoot()
		}
	}
	if parentRoot == (tcommon.Hash{}) {
		return nil, nil, &errBackfillUnreconstructible{
			block: n,
			root:  parentRoot,
			cause: errors.New("parent state root unknown (no sh side-store entry, no genesis root, no proto fallback)"),
		}
	}

	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return nil, nil, &errBackfillUnreconstructible{block: n, root: parentRoot, cause: err}
	}
	statedb.SetHistoryEnabled(true)

	bf := &blockForBackfill{block: block, parentRoot: parentRoot}
	return bf, statedb, nil
}

type blockForBackfill struct {
	block      *types.Block
	parentRoot tcommon.Hash
}

// backfillOneBlock replays block n's state transition and writes its sh-*
// rows. It mirrors applyBlock's StateDB-open + ProcessBlock + AccumulateHistory
// sequence, but omits every persist / head-advance / buffer-commit step:
//   - no statedb.Commit / WriteBlockStateRoot
//   - no updateSolidifiedBlock / DP flush
//   - no bc.buffer.BeginBlock / CommitBlock
//   - no currentBlock.Store / shielded-merkle save / FlushWitnesses
func (bc *BlockChain) backfillOneBlock(n uint64) error {
	bf, statedb, err := bc.openParentStateForBackfill(n)
	if err != nil {
		return err
	}
	block := bf.block

	// Throwaway overlay for ProcessBlock's NON-history writes. Reads fall
	// through to bc.db; writes (asset issues, proposals, contract state,
	// nullifiers, the block-reward allowance ledger) stay in this scratch
	// layer and are dropped when the function returns. This is what makes a
	// re-run over an already-applied block safe: actuator counters and
	// dedup guards see the on-disk state, never a double-applied copy.
	scratch := blockbuffer.New(bc.db)
	scratch.BeginBlock(block.Hash())

	// Dynamic properties drive the same DP-dependent branches ProcessBlock
	// takes during live apply (energy limit, consensus-logic optimisation,
	// reward params). Load them through the scratch overlay so they reflect
	// disk state without being mutable on disk.
	dynProps := state.LoadDynamicProperties(scratch)

	// Rewind the DP header fields to the PARENT block (n-1). ProcessBlock
	// derives prevBlockTime from dynProps.LatestBlockHeaderTimestamp()
	// (state_processor.go) and feeds it to ValidateTxCommon's expiration
	// check (expiration <= prevBlockTime → expired). The DP loaded from
	// disk carries the HEAD timestamp, so without this rewind every
	// historical tx in block n would be judged expired against a
	// far-future head time and the replay would fail wholesale. Live
	// applyBlock(n) sees DP still holding n-1's header (it advances the
	// header only after processing), so mirror that snapshot here.
	//
	// This rewinds only the HEADER fields. The rest of DP (proposal/energy
	// params AND the fork-version vote bitmaps that core/forks resolves
	// Pass(version) from) still reflects HEAD: a fork gate that is ON at
	// HEAD but was OFF at block n makes ProcessBlock take the post-fork
	// path and can touch a *different set* of accounts than the live
	// capture did, drifting the sh-* row set (per-row pre-values stay
	// trie-correct). That's the documented maintenance/flat-KV caveat
	// above — backfill is byte-faithful only for ranges with no
	// maintenance boundary or fork activation between n and HEAD; the
	// header rewind is the bounded fix for the universal expiration
	// blocker, not a general as-of-n DP reconstruction (which isn't
	// recoverable from disk).
	parentBlock := bc.GetBlockByNumber(n - 1)
	if parentBlock != nil {
		dynProps.SetLatestBlockHeaderTimestamp(parentBlock.Timestamp())
		dynProps.SetLatestBlockHeaderNumber(int64(parentBlock.Number()))
		dynProps.SetLatestBlockHeaderHash(parentBlock.Hash())
	}

	// Hydrate witnesses for the reward/standby paths inside ProcessBlock.
	// Read-only LoadWitness (does not mark dirty) — backfill never flushes
	// witnesses, so this only feeds the in-block reward computation.
	for _, addr := range rawdb.ReadWitnessIndex(scratch) {
		if statedb.GetWitness(addr) == nil {
			statedb.LoadWitness(rawdb.ReadWitness(scratch, addr))
		}
	}

	// Choose the same ProcessBlock variant applyBlock would: the
	// java-account-state-root path when the block carries a root (post-fork
	// java-tron blocks), the plain path otherwise. validateEnvelope is false:
	// backfill replays already-canonical blocks, so re-running signature /
	// permission envelope checks is redundant work (and would need the engine
	// wired). The energy-fork block number mirrors the live config.
	energyLimitForkBlockNum := bc.config.EnergyLimitForkBlockNum()
	blockRoot := block.AccountStateRoot()
	parentAccountStateRoot := tcommon.Hash{}
	if parentBlock != nil {
		parentAccountStateRoot = parentBlock.AccountStateRoot()
	}
	if blockRoot != (tcommon.Hash{}) {
		_, _, err = ProcessBlockWithJavaAccountStateRootAndEnergyFork(
			statedb, dynProps, block, scratch, bc.ActiveWitnesses(),
			bc.GenesisTimestamp(), energyLimitForkBlockNum, false, parentAccountStateRoot)
	} else {
		_, err = ProcessBlockWithEnergyFork(
			statedb, dynProps, block, scratch, bc.ActiveWitnesses(),
			bc.GenesisTimestamp(), energyLimitForkBlockNum, false)
	}
	if err != nil {
		return fmt.Errorf("backfill: replay block %d: %w", n, err)
	}

	// Capture the journal into sh-* rows. Writes go DIRECTLY to disk (not the
	// scratch overlay) via a batch so the rows persist. AccumulateHistory must
	// run before any Commit truncates the journal — backfill never commits, so
	// the journal is intact here.
	batch := bc.db.NewBatch()
	if err := statedb.AccumulateHistory(batch, n, block.Hash()); err != nil {
		return fmt.Errorf("backfill: accumulate history for block %d: %w", n, err)
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("backfill: write history rows for block %d: %w", n, err)
	}

	// scratch + statedb go out of scope here; neither is committed, so the
	// live chain state and the canonical head are untouched.
	return nil
}
