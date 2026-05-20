package dpos

import (
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// buildBlock constructs a minimal types.Block with the given header fields
// and a deliberately invalid 65-byte signature so recoverWitness returns
// ErrInvalidSignature when reached. Tests use this to distinguish "rejected
// early by timestamp check" from "rejected later by signature check".
func buildBlock(number uint64, timestamp int64, parentHash [32]byte) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(number),
				Timestamp:  timestamp,
				ParentHash: parentHash[:],
			},
			WitnessSignature: make([]byte, 65), // invalid (all zeros)
		},
	})
}

// genesisAt builds a number=0 block at timestamp=genesisTime to serve as
// the parent for VerifyHeader's CurrentBlock() call.
func genesisAt(genesisTime int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 0, Timestamp: genesisTime},
		},
	})
}

// Pre-fork (ConsensusLogicOptimization=false): a mod-3000-misaligned
// timestamp must not trigger the timestamp check — verification proceeds
// to signature recovery and fails there. Mirrors java-tron's behavior
// before proposal #88 activates.
func TestVerifyHeader_PreFork_AcceptsMisalignedTimestamp(t *testing.T) {
	genesis := genesisAt(0)
	chain := &mockChainReader{
		currentBlock: genesis,
		genesisTime:  0,
		dp:           state.NewDynamicProperties(),
	}
	// ConsensusLogicOptimization left false (default).

	block := buildBlock(1, 5000 /* not 3000-aligned */, genesis.Hash())
	err := VerifyHeader(chain, block)
	if err == nil {
		t.Fatal("expected error (invalid signature) but got nil")
	}
	if errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("pre-fork must not reject on mod-3000 mismatch; got %v", err)
	}
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature (proves we got past timestamp gate), got %v", err)
	}
}

// Post-fork (ConsensusLogicOptimization=true): same misaligned timestamp
// must short-circuit with ErrInvalidTimestamp before signature recovery.
func TestVerifyHeader_PostFork_RejectsMisalignedTimestamp(t *testing.T) {
	genesis := genesisAt(0)
	dp := state.NewDynamicProperties()
	dp.SetConsensusLogicOptimization(true)
	chain := &mockChainReader{
		currentBlock: genesis,
		genesisTime:  0,
		dp:           dp,
	}

	block := buildBlock(1, 5000 /* not 3000-aligned */, genesis.Hash())
	if err := VerifyHeader(chain, block); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("expected ErrInvalidTimestamp, got %v", err)
	}
}

// Slot==0 path. To exercise Check C (slot==0) in isolation, the scenario must
// pass the ungated Check B (abs-slot ordering) first. After a maintenance
// block, getTime(1) is pushed out by MaintenanceSkipSlots (=2), so a block one
// abs-slot past the parent (Check B ok) can still fall before the first
// schedulable slot ⇒ SlotForTime returns 0. This needs a non-genesis parent
// (number ≥ 1) carrying the maintenance state-flag — the earlier genesis-offset
// construction put the block in the parent's own abs-slot, which Check B now
// (correctly, matching java) rejects before Check C is reached.
//
// Pre-fork (ConsensusLogicOptimization=false): Check C is skipped, so a slot==0
// block proceeds past the timestamp gates to signature recovery and fails
// there. Mirrors java before proposal #88.
func TestVerifyHeader_PreFork_AcceptsSlotZero(t *testing.T) {
	parent := buildBlock(1, 3000, [32]byte{}) // number ≥ 1, abs-slot 1
	dp := state.NewDynamicProperties()
	dp.SetStateFlag(1) // parent was a maintenance block
	chain := &mockChainReader{
		currentBlock: parent,
		genesisTime:  0,
		dp:           dp,
	}
	// firstSlotTime = SlotTime(1, 3000, 0, maint, skip=2) = 3000 + 3000*3 = 12000.
	// block ts 6000: abs-slot 2 > parent abs-slot 1 (Check B passes), and
	// 6000 < 12000 ⇒ SlotForTime returns 0.
	block := buildBlock(2, 6000, parent.Hash())

	err := VerifyHeader(chain, block)
	if errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("pre-fork must not reject slot==0; got %v", err)
	}
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestVerifyHeader_PostFork_RejectsSlotZero(t *testing.T) {
	parent := buildBlock(1, 3000, [32]byte{})
	dp := state.NewDynamicProperties()
	dp.SetConsensusLogicOptimization(true)
	dp.SetStateFlag(1)
	chain := &mockChainReader{
		currentBlock: parent,
		genesisTime:  0,
		dp:           dp,
	}

	block := buildBlock(2, 6000, parent.Hash())
	if err := VerifyHeader(chain, block); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("expected ErrInvalidTimestamp (slot==0 reject), got %v", err)
	}
}

// TestVerifyHeader_RejectsSameAbsSlot pins java DposService.validBlock's ungated
// Check B (bSlot <= hSlot). A mod-3000-misaligned block whose timestamp is
// strictly greater than its parent's but lands in the SAME absolute slot must
// be rejected. The earlier blockTime<=parentTime guard accepted it (4000 >
// 3000) while java rejected it (getAbSlot(4000)==getAbSlot(3000)==1) — the
// latent fork vector this check closes. Pre-fork (CLO off) so Check A's
// mod-3000 gate does not mask it.
func TestVerifyHeader_RejectsSameAbsSlot(t *testing.T) {
	parent := buildBlock(1, 3000, [32]byte{}) // abs-slot 1
	chain := &mockChainReader{
		currentBlock: parent,
		genesisTime:  0,
		dp:           state.NewDynamicProperties(), // CLO false
	}
	// block ts 4000: abs-slot (4000-0)/3000 = 1, same as parent's abs-slot 1.
	block := buildBlock(2, 4000, parent.Hash())
	if err := VerifyHeader(chain, block); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("expected ErrInvalidTimestamp (same abs-slot as parent), got %v", err)
	}
}
