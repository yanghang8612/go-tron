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
	// allow_pbft is a rooted DP key (Phase 3b); stage it into the head cache
	// the handler reads, not flat dp-.
	dp := bc.DynProps()
	dp.Set("allow_pbft", 1)
	bc.SetDynPropsCacheForTest(dp)

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
	// allow_pbft is a rooted DP key (Phase 3b); stage it into the head cache
	// the handler reads, not flat dp-.
	dp := bc.DynProps()
	dp.Set("allow_pbft", 1)
	bc.SetDynPropsCacheForTest(dp)

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

// TestPbftDataSync_SRLEpochUsesBufferedDP locks the buffer-overlay reroute
// of the PBFT BlockHook's DP reads. The hook fires synchronously from
// BlockChain.applyBlock after each successful block — at that moment
// bc.buffer still holds the just-applied block's DP writes, because
// bc.buffer.FlushUpTo(solidified, ...) only persists layers up to the
// solidified boundary (which lags head by ~19 blocks on mainnet 27-SR DPoS).
//
// Reading the DP keys directly off h.db inside the hook therefore sees a
// stale image, by up to that ~19-block window. The worst-case symptom is a
// maintenance-boundary block: applyBlock writes the advanced
// next_maintenance_time into bc.buffer, PBFT SRL commit messages arriving
// over the wire cache themselves under the NEW epoch, and the hook —
// reading the OLD next_maintenance_time off disk — looks up the wrong
// epoch and silently drops every cached SR-list signature.
//
// Scenario:
//   - Genesis seeds next_maintenance_time=3000 (Set'd → lands on disk via
//     dp.Flush) plus the default maintenance_time_interval=21_600_000
//     (NOT Set'd → stays in-memory only, matching mainnet's empty-map
//     genesis).
//   - Block #1 has timestamp 3000, so applyBlock crosses the boundary and
//     calls dp.SetNextMaintenanceTime(3000 + 21_600_000) = 21_603_000.
//     That write goes through bc.buffer; solidified is still 0, so
//     flushBufferUpToSolidified is a no-op and the new value never
//     reaches disk.
//   - An SRL commit message is cached at viewN = 21_603_000 - 21_600_000
//     = 3000, the epoch a buffered read computes.
//   - ProcessOnBlock(block1) must see the cached entry. A disk read of
//     next_maintenance_time would still return 3000, the old value, and
//     compute epoch = -21_597_000 — missing the cache and dropping the
//     SRL signature on the floor.
func TestPbftDataSync_SRLEpochUsesBufferedDP(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	// Two genesis witnesses — single-witness chains compute solidified=1
	// after block #1 (the 30%-quantile lands on the only witness's height),
	// which drains the buffer back to disk before the assertion below can
	// observe the buffer/disk delta. A second witness that has not yet
	// produced keeps the quantile at 0 → solidified stays 0 → no flush.
	witnessKey, _ := ethcrypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	idleKey, _ := ethcrypto.GenerateKey()
	idleAddr := crypto.PubkeyToAddress(&idleKey.PublicKey)

	const oldNextMaint = int64(3000)
	const interval = int64(21_600_000) // default; intentionally NOT in the map
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 2, URL: "http://w1"},
			{Address: idleAddr, VoteCount: 1, URL: "http://w2"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": oldNextMaint,
		},
	}
	_, genesisHash, err := core.SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Flip allow_pbft on disk so allowPBFT() short-circuits — what matters
	// here is the SRL epoch computation, not gate activation.
	// allow_pbft is a rooted DP key (Phase 3b); stage it into the head cache
	// the handler reads, not flat dp-.
	dp := bc.DynProps()
	dp.Set("allow_pbft", 1)
	bc.SetDynPropsCacheForTest(dp)

	h := NewPbftDataSyncHandler(bc, bc.DB())

	// Apply block #1 with timestamp == oldNextMaint to cross the boundary.
	// applyBlock will call dp.SetNextMaintenanceTime(3000 + 21_600_000)
	// and route the write through bc.buffer.
	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      oldNextMaint,
				ParentHash:     genesisHash.Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
	// InsertBlock (not InsertBlockWithoutVerify) is required: the
	// "WithoutVerify" path is a test shortcut that writes the block
	// straight to disk and skips applyBlock entirely. The buffer-vs-disk
	// delta this test depends on is only produced when applyBlock runs
	// (it routes DP writes through bc.buffer and flushes only up to
	// solidified). Verification is fine here because bc.engine is nil.
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock(block1): %v", err)
	}

	// next_maintenance_time is a rooted DP key (Phase 3b): it lives in the
	// system-account KV, never in flat dp-, so there is no buffer/disk delta to
	// assert anymore. The invariant that still matters — and the whole point of
	// this test — is that the SRL epoch computation reads the POST-block value
	// (via the in-memory head snapshot), not a stale one.
	const newNextMaint = oldNextMaint + interval
	if got := bc.BufferedDPInt64("next_maintenance_time"); got != newNextMaint {
		t.Fatalf("BufferedDPInt64(next_maintenance_time) = %d, want %d", got, newNextMaint)
	}

	// 19 SR witnesses for the SRL signature quorum.
	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	// Cache the SRL result under the BUFFERED epoch. The fix must compute
	// this same number; the broken disk-only path computes oldNextMaint -
	// interval = -21_597_000 and misses the cache.
	bufferedEpoch := newNextMaint - interval
	payload, _ := buildCommitResult(t, keys, bufferedEpoch, corepb.PBFTMessage_SRL)
	h.HandleCommitMsg(nil, payload)

	h.ProcessOnBlock(block1)

	if got := rawdb.ReadSrSignData(bc.DB(), 0); got == nil {
		t.Fatal("expected WriteSrSignData via buffered next_maintenance_time, got nil — BlockHook still reading stale disk DP image")
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
