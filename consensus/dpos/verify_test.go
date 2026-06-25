package dpos

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
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

// signedHeaderBlock builds a slot-aligned block at the given height/timestamp,
// stamps witnessAddr into the header, and signs the canonical header hash
// (sha256 of the marshaled BlockHeaderRaw) with key — exactly as
// core.SignBlock / recoverWitness do. The recovered signer is therefore
// crypto.PubkeyToAddress(key.PublicKey), which may differ from witnessAddr (the
// delegated-signing case).
func signedHeaderBlock(t *testing.T, number uint64, ts int64, parentHash [32]byte, witnessAddr common.Address, key *ecdsa.PrivateKey) *types.Block {
	t.Helper()
	raw := &corepb.BlockHeaderRaw{
		Number:         int64(number),
		Timestamp:      ts,
		ParentHash:     parentHash[:],
		WitnessAddress: witnessAddr.Bytes(),
	}
	data, err := proto.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	hash := sha256.Sum256(data)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatalf("sign header: %v", err)
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: raw, WitnessSignature: sig},
	})
}

// TestVerifyHeader_MultiSignWitnessPermission pins java-tron
// BlockCapsule.validateSignature: when AllowMultiSign is active, the recovered
// block signer is compared to the witness account's witness-permission address
// (AccountCapsule.getWitnessPermissionAddress — a key the SR may delegate block
// signing to), NOT block.witness_address. Pre-AllowMultiSign (and for witnesses
// that never delegated) it is still compared to block.witness_address.
//
// This is the Nile 45,490,765 stall: SR 417d6fd4 delegated block signing to
// 415624c1 via its witness permission, so a block signed by 415624c1 was valid
// on-chain, but gtron rejected it for not being signed by 417d6fd4.
func TestVerifyHeader_MultiSignWitnessPermission(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	delegKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	delegAddr := crypto.PubkeyToAddress(&delegKey.PublicKey)

	genesis := genesisAt(0)
	// Resolver mirroring an SR that delegated block signing to delegAddr.
	delegated := func(addr common.Address) common.Address {
		if addr == witnessAddr {
			return delegAddr
		}
		return addr
	}
	// run verifies a block produced by witnessAddr in slot 1 (ts 3000). CLO is
	// left off so the timestamp gates are skipped and verification reaches the
	// signature check, then the schedule check (which witnessAddr — the sole
	// active SR — satisfies). The error (or nil) is thus determined solely by
	// the signature comparison under test.
	run := func(multiSign bool, permSigner func(common.Address) common.Address, b *types.Block) error {
		dp := state.NewDynamicProperties()
		dp.SetAllowMultiSign(multiSign)
		chain := &mockChainReader{
			currentBlock: genesis,
			genesisTime:  0,
			witnesses:    []common.Address{witnessAddr},
			dp:           dp,
			permSigner:   permSigner,
		}
		return VerifyHeaderWithDynProps(chain, b, dp)
	}

	delegBlock := signedHeaderBlock(t, 1, 3000, genesis.Hash(), witnessAddr, delegKey)
	ownBlock := signedHeaderBlock(t, 1, 3000, genesis.Hash(), witnessAddr, witnessKey)
	otherBlock := signedHeaderBlock(t, 1, 3000, genesis.Hash(), witnessAddr, otherKey)

	// 1. AllowMultiSign on + delegation to delegAddr + signed by delegKey ⇒ valid.
	if err := run(true, delegated, delegBlock); err != nil {
		t.Fatalf("delegated-key block must be accepted under AllowMultiSign: %v", err)
	}
	// 2. Same delegated-key block, AllowMultiSign OFF ⇒ compared to
	//    witness_address (≠ delegAddr) ⇒ rejected.
	if err := run(false, delegated, delegBlock); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("delegated-key block must be rejected when AllowMultiSign is off, got %v", err)
	}
	// 3. AllowMultiSign on but witness did NOT delegate (resolver returns
	//    witness_address) ⇒ delegated-key signature rejected.
	if err := run(true, nil, delegBlock); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("delegated-key block must be rejected without a matching witness permission, got %v", err)
	}
	// 4. Witness signs with its own key under AllowMultiSign (no delegation) ⇒
	//    getWitnessPermissionAddress returns witness_address ⇒ valid.
	if err := run(true, nil, ownBlock); err != nil {
		t.Fatalf("own-key block must remain valid under AllowMultiSign: %v", err)
	}
	// 5. Default path unchanged: own-key block with AllowMultiSign off ⇒ valid.
	if err := run(false, nil, ownBlock); err != nil {
		t.Fatalf("own-key block must remain valid pre-AllowMultiSign: %v", err)
	}
	// 6. An unauthorized third key is rejected even with delegation in effect
	//    (only the delegated key — not any key — is accepted).
	if err := run(true, delegated, otherBlock); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("unauthorized signer must be rejected under AllowMultiSign, got %v", err)
	}
}
