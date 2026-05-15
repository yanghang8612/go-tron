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

// Slot==0 path: with a non-3000-aligned genesis time, there exist
// timestamps that are absolute-mod-3000 aligned but still fall before
// firstSlotTime (= genesisTime + 3000). Pre-fork java accepts these and
// reaches signature recovery; post-fork java rejects.
func TestVerifyHeader_PreFork_AcceptsSlotZero(t *testing.T) {
	const gts = 1500 // genesis not aligned to BlockProducedInterval
	chain := &mockChainReader{
		currentBlock: genesisAt(gts),
		genesisTime:  gts,
		dp:           state.NewDynamicProperties(),
	}
	// firstSlotTime = SlotTime(1, gts, gts, false, …) = gts + 3000 = 4500.
	// Block timestamp 3000 is > parent (gts=1500), mod-3000-aligned, but
	// 3000 < 4500 ⇒ SlotForTime returns 0.
	block := buildBlock(1, 3000, chain.currentBlock.Hash())

	err := VerifyHeader(chain, block)
	if errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("pre-fork must not reject slot==0; got %v", err)
	}
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestVerifyHeader_PostFork_RejectsSlotZero(t *testing.T) {
	const gts = 1500
	dp := state.NewDynamicProperties()
	dp.SetConsensusLogicOptimization(true)
	chain := &mockChainReader{
		currentBlock: genesisAt(gts),
		genesisTime:  gts,
		dp:           dp,
	}

	block := buildBlock(1, 3000, chain.currentBlock.Hash())
	if err := VerifyHeader(chain, block); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("expected ErrInvalidTimestamp (slot==0 reject), got %v", err)
	}
}
