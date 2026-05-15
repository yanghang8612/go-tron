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

// TestChain_RejectsBlockWithUnsignedTx: P0-2b chain-level test.
// applyBlock must reject a block whose body contains a tx with no
// signature, even if the block header itself was correctly signed by the
// scheduled witness. Without the envelope-validation loop in applyBlock a
// malicious peer can inject malformed txs through a legit-looking block.
func TestChain_RejectsBlockWithUnsignedTx(t *testing.T) {
	bc, key, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := bc.ActiveWitnesses()[0]

	// Build a malformed transfer tx (no signature) bound for the chain.
	_, recipient := keyAndAddr(t)
	tx := buildTransferTx(t, witnessAddr, recipient, 100, 0 /* no signers */)

	pool := txpool.New()
	pool.Add(tx) // bypass envelope gate — pool.Add itself doesn't check
	result, err := BuildBlock(bc, pool, witnessAddr, int64(params.BlockProducedInterval))
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block
	if err := SignBlock(block, key); err != nil {
		t.Fatal(err)
	}

	err = bc.InsertBlock(block)
	if err == nil {
		t.Fatal("expected InsertBlock to reject a block whose tx has no signature")
	}
	if !errors.Is(err, ErrNoSignature) {
		t.Fatalf("expected ErrNoSignature for the empty-sig tx, got %v", err)
	}
}

// TestChain_ValidateTransaction_RejectsBadSigner: the txpool admission gate
// uses bc.ValidateTransaction; signing with a key that isn't in the owner's
// default permission must yield ErrUnauthorizedSigner.
func TestChain_ValidateTransaction_RejectsBadSigner(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	_, owner := keyAndAddr(t)
	wrongKey, _ := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	tx := buildTransferTx(t, owner, recipient, 100, 0, wrongKey)

	if err := bc.ValidateTransaction(tx); !errors.Is(err, ErrUnauthorizedSigner) {
		t.Fatalf("expected ErrUnauthorizedSigner, got %v", err)
	}
}

// TestChain_ValidateTransaction_AcceptsOwnerSig: matching default permission
// → accept. Pairs with the rejection test above for the pool admission
// gate. Production binaries (cmd/gtron) wire engine, so this is the live
// behavior; tests that skip SetEngine bypass entirely.
func TestChain_ValidateTransaction_AcceptsOwnerSig(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	// Point ref_block at genesis (in the tapos ring after SetupGenesisBlock).
	// Without this the chain-level path's TAPOS check rejects the tx.
	refBytes, refHash := genesisTaposRef(t, bc)
	tx := buildTransferTxWithRef(t, owner, recipient, 100, 0, refBytes, refHash, ownerKey)

	if err := bc.ValidateTransaction(tx); err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
}

// genesisTaposRef returns ref_block_bytes and ref_block_hash for a tx that
// references genesis. Production wallets pick a recent block; for tests on
// a 1-block chain only block #0 is available, so we point at that.
func genesisTaposRef(t *testing.T, bc *BlockChain) ([]byte, []byte) {
	t.Helper()
	genesis := bc.GetBlockByNumber(0)
	if genesis == nil {
		t.Fatal("genesis block missing")
	}
	h := genesis.Hash()
	return []byte{0, 0}, h[8:16]
}

// TestChain_ValidateTransaction_RejectsBadTapos: a syntactically correct
// tx (good signature, real account) but with a TAPOS reference that
// doesn't match any recent block must be rejected. Mirrors java-tron's
// TaposException("different block hash") / ("No reference block found").
func TestChain_ValidateTransaction_RejectsBadTapos(t *testing.T) {
	bc, _, err := setupVerifyChain(t)
	if err != nil {
		t.Fatal(err)
	}

	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	// Real ref_block_bytes (slot 0 is populated by genesis) but a wrong
	// hash. Differentiates "slot empty" from "slot occupied but hash
	// diverges" — the latter is the chain-fork shape worth catching.
	wrongHash := []byte{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef}
	tx := buildTransferTxWithRef(t, owner, recipient, 100, 0, []byte{0, 0}, wrongHash, ownerKey)

	if err := bc.ValidateTransaction(tx); !errors.Is(err, ErrTaposHashMismatch) {
		t.Fatalf("expected ErrTaposHashMismatch, got %v", err)
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
