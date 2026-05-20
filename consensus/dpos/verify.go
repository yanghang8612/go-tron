package dpos

import (
	"crypto/sha256"
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidBlockNumber = errors.New("invalid block number")
	ErrInvalidParentHash  = errors.New("parent hash mismatch")
	ErrInvalidTimestamp   = errors.New("invalid timestamp")
	ErrInvalidWitness     = errors.New("not the scheduled witness")
	ErrInvalidSignature   = errors.New("invalid block signature")
)

// VerifyHeader is the back-compat entry point that loads dp via
// chain.DynProps() (which reads through the buffer overlay). Hot-path callers
// inside applyBlock should call VerifyHeaderWithDynProps directly with a dp
// they have already loaded to avoid a redundant LoadDynamicProperties pass.
func VerifyHeader(chain consensus.ChainReader, block *types.Block) error {
	return VerifyHeaderWithDynProps(chain, block, chain.DynProps())
}

// VerifyHeaderWithDynProps verifies a block header against the supplied
// dynamic-properties snapshot. The caller owns the load — applyBlock reads dp
// from the buffer overlay once and threads it here, removing the duplicate
// LoadDynamicProperties that the chain.DynProps() fallback in VerifyHeader
// would otherwise perform.
func VerifyHeaderWithDynProps(chain consensus.ChainReader, block *types.Block, dp *state.DynamicProperties) error {
	parent := chain.CurrentBlock()
	if parent == nil {
		return errors.New("parent block not found")
	}
	if block.Number() != parent.Number()+1 {
		return ErrInvalidBlockNumber
	}
	if block.ParentHash() != parent.Hash() {
		return ErrInvalidParentHash
	}
	genesisTime := chain.GenesisTimestamp()
	// [java DposService.validBlock:126-131 — Check B, UNGATED] the block's
	// absolute slot must be strictly after the parent's. java computes
	// bSlot = getAbSlot(blockTs), hSlot = getAbSlot(parentTs) where
	// getAbSlot(t) = (t - genesis) / interval, and rejects bSlot <= hSlot.
	// This subsumes a plain blockTime <= parentTime monotonicity guard AND
	// additionally rejects a mod-3000-misaligned block that lands in the
	// parent's abs-slot (only reachable pre-#88, when misaligned timestamps
	// pass Check A). The earlier raw-timestamp compare missed that same-slot
	// case — a misaligned block with blockTime > parentTime but the same
	// abs-slot was accepted here while java rejected it, a latent fork vector.
	if AbsoluteSlot(block.Timestamp(), genesisTime) <= AbsoluteSlot(parent.Timestamp(), genesisTime) {
		return ErrInvalidTimestamp
	}
	// mod-3000 alignment + slot==0 rejection were unconditional in early
	// gtron but java-tron gates both on proposal #88 (`DposService.java:120,
	// 134`). Pre-#88, java accepts misaligned timestamps and slot-0 blocks;
	// gtron must do the same for replay parity. In practice real producers
	// only mint aligned slots, so the loosening is theoretical.
	//
	// NOTE: java's validBlock additionally returns true immediately when the
	// parent is genesis (number 0), skipping Checks A–D for block 1. gtron
	// deliberately does NOT replicate that bypass: it runs the full check set
	// for block 1 too. This is stricter than java but safe — the canonical
	// block 1 is always well-formed (aligned, scheduled witness, valid sig) so
	// it passes both, and gtron rejects only malformed block-1s that never
	// appear on the real chain. Matching java's bypass would weaken block-1
	// validation (e.g. drop the witness-schedule check that
	// TestInsertBlock_RejectsUnscheduledWitness pins) for zero real-world
	// parity benefit. Tracked as V-1 in docs/dev/cross-impl-audit-2026-05-19.md.
	if dp.ConsensusLogicOptimization() {
		if block.Timestamp()%int64(params.BlockProducedInterval) != 0 {
			return ErrInvalidTimestamp
		}
		isMaintenance := dp.StateFlag() == 1
		if SlotForTime(block.Timestamp(), parent.Timestamp(), genesisTime, isMaintenance, int64(params.MaintenanceSkipSlots)) == 0 {
			return ErrInvalidTimestamp
		}
	}

	// Recover through the block's memo so a parallel pre-verification pass can
	// move this ECDSA recovery off the serial critical path. On a cache miss
	// (e.g. fork-replay blocks the pre-pass never saw) this computes inline via
	// recoverWitness — identical result either way.
	witness, err := block.CachedRecoveredWitness(recoverWitness)
	if err != nil {
		return ErrInvalidSignature
	}
	// java-tron's Manager.pushBlock → validateSignature compares the
	// recovered signer to BlockHeader.raw.witness_address (not the schedule).
	// Without this, a producer with a leaked SR key could mint a block with
	// the SR's address as the schedule-side witness while stamping a
	// different attacker-controlled address into block.witness_address —
	// applyBlock's downstream calls (payBlockReward, updateSolidifiedBlock,
	// flipWitnessIsJobs) all key off block.WitnessAddress(), so the reward
	// and is_jobs flip would route to the attacker.
	if witness != block.WitnessAddress() {
		return ErrInvalidSignature
	}

	// Schedule slot for this block. java-tron's DposService.validBlock calls
	// `dposSlot.getScheduledWitness(dposSlot.getSlot(timestamp))` where:
	//   getSlot(ts)    = (ts - getTime(1)) / interval + 1
	//   getTime(1)     = parent_aligned_time + interval * (1 + skip if parent_was_maintenance)
	//   getScheduledWitness(slot) = activeWitnesses[(getAbSlot(parent.timestamp) + slot) % N]
	// After substitution and cancelling intervals, the schedule index is
	//   (parent_absSlot + (ts - parent_aligned)/interval - skip*isMaintenance) % N
	// which equals AbsoluteSlot(ts) - skip*isMaintenance for aligned timestamps.
	// The maintenance branch matters because after a maintenance block, java
	// advances the wall clock by (1 + skip) slots before producing the next
	// block, but the schedule index only advances by 1 — the skipped slots are
	// NOT consumed by the rotation. Without the correction here, gtron rejects
	// every block that follows a maintenance with ErrInvalidWitness once there
	// are multiple SRs registered (the 1-SR fixture hid this because every idx
	// resolves to the same witness).
	slot := AbsoluteSlot(block.Timestamp(), genesisTime)
	if dp.StateFlag() == 1 {
		slot -= int64(params.MaintenanceSkipSlots)
	}
	witnesses := chain.ActiveWitnesses()
	if len(witnesses) == 0 {
		log.Warn("DPoS scheduled witness check failed: no active witnesses",
			"number", block.Number(), "slot", slot, "stateFlag", dp.StateFlag(),
			"parent", parent.Number(), "parentTs", parent.Timestamp(), "blockTs", block.Timestamp())
		return ErrInvalidWitness
	}
	idx := WitnessIndex(slot, len(witnesses))
	if idx < 0 || idx >= len(witnesses) {
		log.Warn("DPoS scheduled witness index out of range",
			"number", block.Number(), "slot", slot, "idx", idx, "activeWitnesses", len(witnesses),
			"stateFlag", dp.StateFlag(), "parent", parent.Number(), "parentTs", parent.Timestamp(), "blockTs", block.Timestamp())
		return ErrInvalidWitness
	}
	// Match java-tron DposService.validBlock: compare the schedule against
	// block.witness_address (transitively, since we just enforced signer ==
	// block.witness_address). This phrasing yields the more intuitive
	// ErrInvalidWitness when an SR mints in a slot that doesn't belong to it.
	if witnesses[idx] != block.WitnessAddress() {
		log.Warn("DPoS scheduled witness mismatch",
			"number", block.Number(), "slot", slot, "idx", idx, "activeWitnesses", len(witnesses),
			"scheduled", witnesses[idx], "actual", block.WitnessAddress(),
			"stateFlag", dp.StateFlag(), "parent", parent.Number(), "parentTs", parent.Timestamp(), "blockTs", block.Timestamp())
		return ErrInvalidWitness
	}
	return nil
}

func recoverWitness(block *types.Block) (common.Address, error) {
	sig := block.WitnessSignature()
	if len(sig) != 65 {
		return common.Address{}, ErrInvalidSignature
	}
	headerRaw := block.Proto().BlockHeader.RawData
	data, err := proto.Marshal(headerRaw)
	if err != nil {
		return common.Address{}, err
	}
	hash := sha256.Sum256(data)

	pub, err := crypto.SigToPub(hash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(pub), nil
}
