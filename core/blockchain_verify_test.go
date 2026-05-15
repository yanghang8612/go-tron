package core

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

// setupVerifyChain stands up a single-witness chain wired with a real DPoS
// engine for header verification. The returned key signs blocks attributed to
// witnessAddr; tests that need an invalid signer generate a second key.
func setupVerifyChain(t *testing.T) (*BlockChain, *ecdsa.PrivateKey, error) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	key, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://sr1"},
		},
		// Push maintenance far out so block #1 doesn't trigger rotation.
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 9_000_000_000,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		return nil, nil, err
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		return nil, nil, err
	}
	bc.SetEngine(dpos.New(bc))
	return bc, key, nil
}

// TestInsertBlock_RejectsWrongSigner is the P0-1 regression test for the
// java-tron `validateSignature` parity: when block.WitnessAddress names the
// legit scheduled witness but the signature was produced by a different key,
// the recovered signer ≠ block.WitnessAddress, and the block must be
// rejected before any state mutation.
func TestInsertBlock_RejectsWrongSigner(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	wrongKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	// Block witness is the legitimate SR; signature comes from the wrong key.
	witnessAddr := bc.ActiveWitnesses()[0]
	pool := txpool.New()
	result, err := BuildBlock(bc, pool, witnessAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	if err := SignBlock(block, wrongKey); err != nil {
		t.Fatal(err)
	}

	err = bc.InsertBlock(block)
	if err == nil {
		t.Fatal("expected InsertBlock to reject a block signed by an out-of-set key")
	}
	if !errors.Is(err, dpos.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature (signer ≠ block.WitnessAddress), got %v", err)
	}
	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("chain head advanced past genesis on a rejected block: got %d", got)
	}
}

// TestInsertBlock_RejectsUnscheduledWitness covers the ErrInvalidWitness
// branch: a self-consistent block (signer == block.WitnessAddress) from an
// address that simply isn't in the active witness set / isn't the witness
// scheduled for this slot. Matches java-tron DposService.validBlock's
// scheduledWitness check.
func TestInsertBlock_RejectsUnscheduledWitness(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh key — its address is NOT in the active witness set seeded by
	// setupVerifyChain (which only registers the genesis SR).
	rogueKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	rogueAddr := crypto.PubkeyToAddress(&rogueKey.PublicKey)
	if bc.ActiveWitnesses()[0] == rogueAddr {
		t.Skip("rogue key collided with genesis SR address; rerun")
	}

	pool := txpool.New()
	// BuildBlock attributes the block to rogueAddr (the would-be witness),
	// signed correctly by rogueKey. VerifyHeader passes the signer-matches
	// check but fails on schedule lookup.
	result, err := BuildBlock(bc, pool, rogueAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	if err := SignBlock(block, rogueKey); err != nil {
		t.Fatal(err)
	}

	err = bc.InsertBlock(block)
	if err == nil {
		t.Fatal("expected InsertBlock to reject a block from an unscheduled witness")
	}
	if !errors.Is(err, dpos.ErrInvalidWitness) {
		t.Fatalf("expected ErrInvalidWitness (block.WitnessAddress ≠ schedule), got %v", err)
	}
}

// TestInsertBlock_RejectsZeroSignature: a block with the 65-byte all-zero
// signature payload (default for the unsigned tests in this package) must be
// rejected by VerifyHeader as malformed before any state writes happen.
// Mirrors java-tron's ECDSA recover failure path.
func TestInsertBlock_RejectsZeroSignature(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	witnessAddr := bc.ActiveWitnesses()[0]
	pool := txpool.New()
	result, err := BuildBlock(bc, pool, witnessAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	// Leave WitnessSignature empty / unset — VerifyHeader → recoverWitness
	// returns ErrInvalidSignature on the length check.

	err = bc.InsertBlock(block)
	if err == nil {
		t.Fatal("expected InsertBlock to reject a block with no signature")
	}
	if !errors.Is(err, dpos.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

// TestInsertBlock_RejectsForgedWitnessAddress is the parity test for the
// "leaked-key reward redirect" attack: a legit SR signs the block (so the
// recovered signer is in the active set) but mutates BlockHeader.raw
// .witness_address after signing to point at an attacker-controlled
// account. applyBlock's downstream payBlockReward / updateSolidifiedBlock /
// flipWitnessIsJobs all key off block.WitnessAddress(), so without the
// `signer == block.WitnessAddress` check the reward and is_jobs flip route
// to the attacker. java-tron Manager.pushBlock → validateSignature rejects
// identically.
func TestInsertBlock_RejectsForgedWitnessAddress(t *testing.T) {
	bc, key, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	witnessAddr := bc.ActiveWitnesses()[0]
	pool := txpool.New()
	result, err := BuildBlock(bc, pool, witnessAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	// Sign with the legit key first so the signature covers the legit
	// WitnessAddress; then stomp WitnessAddress with an attacker address.
	// The signature still recovers to the legit witness, but the new field
	// value diverges — VerifyHeader must reject.
	if err := SignBlock(block, key); err != nil {
		t.Fatal(err)
	}
	var attacker [21]byte
	attacker[0] = 0x41
	attacker[20] = 0xEE
	block.Proto().BlockHeader.RawData.WitnessAddress = attacker[:]

	err = bc.InsertBlock(block)
	if err == nil {
		t.Fatal("expected InsertBlock to reject a block whose WitnessAddress field doesn't match the signer")
	}
	if !errors.Is(err, dpos.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature (signer ≠ block.WitnessAddress), got %v", err)
	}
}

// TestInsertBlock_AcceptsValidSignedBlock: positive control — the same chain
// accepts a block signed by the scheduled witness's key, demonstrating that
// the verification path is gated on signer identity and not unconditionally
// rejecting.
func TestInsertBlock_AcceptsValidSignedBlock(t *testing.T) {
	bc, key, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	witnessAddr := bc.ActiveWitnesses()[0]
	pool := txpool.New()
	result, err := BuildBlock(bc, pool, witnessAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	if err := SignBlock(block, key); err != nil {
		t.Fatal(err)
	}

	if err := bc.InsertBlock(block); err != nil {
		t.Fatalf("InsertBlock: expected accept, got %v", err)
	}
	if got := bc.CurrentBlock().Number(); got != 1 {
		t.Fatalf("chain head: got %d, want 1", got)
	}
}
