package net

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/core/types"
	"google.golang.org/protobuf/proto"
)

func makeDataSyncHandler(t *testing.T) (*PbftDataSyncHandler, *core.BlockChain) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{Config: params.MainnetChainConfig, Timestamp: 0}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.LoadDynamicProperties(diskdb)
	dp.Set("allow_pbft", 1)
	dp.Flush(diskdb)

	h := NewPbftDataSyncHandler(bc, bc.DB())
	return h, bc
}

// buildCommitResult creates a PBFTCommitResult for blockNum signed by the given keys.
func buildCommitResult(t *testing.T, keys []*ecdsa.PrivateKey, viewN int64, dt corepb.PBFTMessage_DataType) ([]byte, *corepb.PBFTCommitResult) {
	t.Helper()
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_COMMIT,
		DataType: dt,
		ViewN:    viewN,
		Epoch:    0,
		Data:     []byte("blockid"),
	}
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	hashArr := sha256.Sum256(rawBytes)
	sigs := make([][]byte, len(keys))
	for i, k := range keys {
		sig, err := crypto.Sign(hashArr[:], k)
		if err != nil {
			t.Fatal(err)
		}
		sigs[i] = sig
	}
	result := &corepb.PBFTCommitResult{Data: rawBytes, Signature: sigs}
	payload, err := proto.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return payload, result
}

func registerSRs(t *testing.T, db interface{ DB() interface{ Put([]byte, []byte) error } }, keys []*ecdsa.PrivateKey) []tcommon.Address {
	addrs := make([]tcommon.Address, len(keys))
	for i, k := range keys {
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	return addrs
}

func genesisBlock(bc *core.BlockChain) *types.Block {
	return bc.GetBlockByNumber(0)
}

func TestPbftDataSyncValid(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0) // genesis block number
	payload, _ := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)

	h.HandleCommitMsg(nil, payload)

	// Trigger on genesis block insert
	h.ProcessOnBlock(genesisBlock(bc))

	result := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if result == nil {
		t.Fatal("expected WriteBlockSignData after valid 19-sig commit result, got nil")
	}
}

func TestPbftDataSyncInvalidSig(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0)
	payload, result := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)
	_ = result

	// Tamper the last signature — replace byte 0 with 0xFF
	var cr corepb.PBFTCommitResult
	proto.Unmarshal(payload, &cr)
	if len(cr.Signature) > 0 {
		cr.Signature[len(cr.Signature)-1][0] ^= 0xFF
	}
	tampered, _ := proto.Marshal(&cr)

	h.HandleCommitMsg(nil, tampered)
	h.ProcessOnBlock(genesisBlock(bc))

	r := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if r != nil {
		t.Error("expected no rawdb write for tampered signature")
	}
}

// TestPbftDataSync_SRLEpochUsesDefaultInterval guards against a regression
// flagged by adversarial review: the PBFT BlockHook used to call
// state.LoadDynamicProperties (which seeds defaults) and then read
// MaintenanceTimeInterval(). Replacing that with a direct rawdb point read
// would return 0 on chains whose genesis omits the key — mainnet ships
// DynamicProperties as an empty map, and dp.Flush only writes dirty keys, so
// maintenance_time_interval (default 21_600_000) never lands on disk. The
// epoch math then collapses to `nextMaint - 0` and SRL commit results cached
// by epoch are silently lost.
//
// This test simulates that scenario: install genesis with an empty DP map,
// seed only allow_pbft + next_maintenance_time (the keys mainnet would have
// after genesis init), cache an SRL result at the expected epoch, and
// verify ProcessOnBlock finds it and persists.
func TestPbftDataSync_SRLEpochUsesDefaultInterval(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{Config: params.MainnetChainConfig, Timestamp: 0}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	// Mainnet-shaped DP state: only the keys that mainnet genesis Set's land
	// on disk. allow_pbft is flipped on by a proposal in real life; we Set
	// it directly here so allowPBFT() short-circuits to true. Critically we
	// do NOT Set maintenance_time_interval — the default 21_600_000 must
	// only live in memory, matching mainnet's empty-map genesis.
	dp := state.LoadDynamicProperties(diskdb)
	dp.Set("allow_pbft", 1)
	dp.Flush(diskdb)

	// Sanity check: ReadDynamicProperty must return absent for the
	// maintenance interval — that's the precondition this test guards.
	if got := rawdb.ReadDynamicProperty(diskdb, "maintenance_time_interval"); len(got) != 0 {
		t.Fatalf("test precondition broken: maintenance_time_interval present on disk (%d bytes)", len(got))
	}

	h := NewPbftDataSyncHandler(bc, bc.DB())

	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	// SRL messages are cached at viewN = epoch = nextMaint - interval. With
	// next_maintenance_time absent (default 0) and the in-memory interval
	// default 21_600_000, epoch should be -21_600_000.
	const wantEpoch = int64(0) - 21_600_000
	payload, _ := buildCommitResult(t, keys, wantEpoch, corepb.PBFTMessage_SRL)
	h.HandleCommitMsg(nil, payload)

	// ProcessOnBlock on genesis (block #0) misses the per-block cache and
	// falls into the SRL epoch lookup. With the fallback to defaults the
	// epoch must match wantEpoch and the SRL sign data must be persisted.
	h.ProcessOnBlock(genesisBlock(bc))

	if got := rawdb.ReadSrSignData(bc.DB(), 0); got == nil {
		t.Fatal("expected WriteSrSignData via SRL epoch lookup, got nil — readDPInt64 fallback to defaultProps regressed")
	}
}

func TestPbftDataSyncInsufficientSigs(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 18) // only 18
	addrs := make([]tcommon.Address, 18)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0)
	payload, _ := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)

	h.HandleCommitMsg(nil, payload)
	h.ProcessOnBlock(genesisBlock(bc))

	r := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if r != nil {
		t.Error("expected no rawdb write for insufficient sigs (18 < 19)")
	}
}
