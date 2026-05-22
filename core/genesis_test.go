package core

import (
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

func TestGenesisToBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
		DynamicProperties: map[string]int64{
			"witness_pay_per_block": 16000000,
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	block, err := GenesisToBlock(genesis, sdb)
	if err != nil {
		t.Fatal(err)
	}

	if block.Number() != 0 {
		t.Fatalf("genesis block number: want 0, got %d", block.Number())
	}
	if block.ParentHash() != (common.Hash{}) {
		t.Fatal("genesis parent hash should be zero")
	}
	if block.AccountStateRoot() != (common.Hash{}) {
		t.Fatalf("genesis accountStateRoot should be zero (java-tron parity), got %x", block.AccountStateRoot())
	}
	if len(block.Transactions()) != len(genesis.Accounts) {
		t.Fatalf("genesis tx count: want %d, got %d", len(genesis.Accounts), len(block.Transactions()))
	}
	if string(block.Proto().GetBlockHeader().GetRawData().GetWitnessAddress()) !=
		"A new system must allow existing systems to be linked together without requiring any central control or coordination" {
		t.Fatal("genesis witness_address should be the famous-quote bytes (java-tron parity)")
	}
}

// TestDefaultNileGenesis_HashByteEqual locks gtron's Nile genesis blockID
// against the live Nile testnet value. Confirmed via wallet/getblockbynum
// {"num":0} on https://nile.trongrid.io (2026-05-09). Source for the
// underlying genesis fields: java-tron `nile/Nile` branch
// `framework/src/main/resources/config-nile.conf`.
func TestDefaultNileGenesis_HashByteEqual(t *testing.T) {
	const wantHex = "0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc"
	g := params.DefaultNileGenesis()
	diskdb := ethrawdb.NewMemoryDatabase()
	block, err := GenesisToBlock(g, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	gotHex := hex.EncodeToString(block.Hash().Bytes())
	if gotHex != wantHex {
		t.Fatalf("Nile genesis blockID drift:\n  want: %s\n  got:  %s\n  (any change to params/nile.go genesis fields breaks Nile P2P sync)", wantHex, gotHex)
	}
}

// TestDefaultMainnetGenesis_HashByteEqual locks gtron's mainnet genesis
// blockID against the live mainnet value. java-tron seed nodes drop our
// connection at TRON Hello when the genesisBlockId we advertise differs.
// Real mainnet blockID confirmed via wallet/getblockbynum {"num":0} on
// 3.12.206.71:8088 (java-tron 4.8.1, 2026-05-08).
func TestDefaultMainnetGenesis_HashByteEqual(t *testing.T) {
	const wantHex = "00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc"
	g := params.DefaultMainnetGenesis()
	diskdb := ethrawdb.NewMemoryDatabase()
	block, err := GenesisToBlock(g, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	gotHex := hex.EncodeToString(block.Hash().Bytes())
	if gotHex != wantHex {
		t.Fatalf("mainnet genesis blockID drift:\n  want: %s\n  got:  %s\n  (any change to params/mainnet.go genesis fields breaks java-tron P2P sync)", wantHex, gotHex)
	}
}

// TestSetupGenesisBlock_NextMaintenanceTimeSeeded locks the
// `Manager.initGenesis`-equivalent fix in core/genesis.go: when the genesis
// config doesn't seed `next_maintenance_time` (mainnet/nile defaults), the
// bootstrap layer must derive it from `genesis.Timestamp +
// maintenance_time_interval`. Without this, the applyBlock gate
// `NextMaintenanceTime() > 0` stays false forever, DoMaintenance never runs,
// and every standby-witness allowance reward + active-set rotation silently
// drops on the floor — observed empirically on a 37k-block mainnet sync
// before the fix landed (2026-05-08).
func TestSetupGenesisBlock_NextMaintenanceTimeSeeded(t *testing.T) {
	genesis := params.DefaultMainnetGenesis()
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	dp := loadGenesisDP(t, diskdb)
	if dp.NextMaintenanceTime() <= 0 {
		t.Fatalf("NextMaintenanceTime must be > 0 after genesis bootstrap (mainnet); got %d", dp.NextMaintenanceTime())
	}
	wantNext := genesis.Timestamp + dp.MaintenanceTimeInterval()
	if dp.NextMaintenanceTime() != wantNext {
		t.Fatalf("NextMaintenanceTime: got %d, want %d (= genesis.Timestamp + MaintenanceTimeInterval)",
			dp.NextMaintenanceTime(), wantNext)
	}
}

// TestSetupGenesisBlock_RootedDynPropsDeterministic guards consensus safety of
// the Phase 3b rooted-dynprops seed: FlushRooted iterates a Go map (randomized
// order), so two nodes booting the same genesis must still derive the IDENTICAL
// genesis state root — otherwise they'd disagree on state from block #1. Also
// confirms a rooted value round-trips out of the genesis root.
func TestSetupGenesisBlock_RootedDynPropsDeterministic(t *testing.T) {
	var firstRoot common.Hash
	for run := 0; run < 4; run++ {
		diskdb := ethrawdb.NewMemoryDatabase()
		if _, _, err := SetupGenesisBlock(diskdb, params.DefaultMainnetGenesis()); err != nil {
			t.Fatal(err)
		}
		root := rawdb.ReadGenesisStateRoot(diskdb)
		if root == (common.Hash{}) {
			t.Fatal("genesis state root not persisted")
		}
		if run == 0 {
			firstRoot = root
		} else if root != firstRoot {
			t.Fatalf("genesis state root non-deterministic across runs: run %d = %x, run 0 = %x", run, root, firstRoot)
		}
		// A rooted dynprop must be recoverable from the genesis root.
		if got := loadGenesisDP(t, diskdb).MaintenanceTimeInterval(); got != 21600000 {
			t.Fatalf("rooted maintenance_time_interval at genesis root: got %d, want 21600000", got)
		}
	}
}

// TestSetupGenesisBlock_WitnessAccountsCreated locks java-tron `Manager
// .initWitness` parity: every genesis witness must have an Account record
// (created with AccountType=AssetIssue if absent) and IsWitness=true on
// that account, after SetupGenesisBlock returns. Without this, AddAllowance
// on a GR address silently no-ops (statedb.go: `if obj == nil { return }`),
// killing payBlockReward and distributeLegacyStandby for every GR witness
// — observed empirically before the fix landed (2026-05-09 follow-up to
// the next_maintenance_time fix in 4a4188a). Reads through a fresh
// state.New on the persisted state root to also verify the witness flags
// survived the Commit, not just the in-memory bootstrap statedb.
func TestSetupGenesisBlock_WitnessAccountsCreated(t *testing.T) {
	genesis := params.DefaultMainnetGenesis()
	if len(genesis.Witnesses) == 0 {
		t.Fatalf("mainnet genesis has no witnesses; test fixture broken")
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	root := rawdb.ReadGenesisStateRoot(diskdb)
	if root == (common.Hash{}) {
		t.Fatalf("genesis state root not persisted")
	}
	sdb, err := state.New(root, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatalf("open state at genesis root: %v", err)
	}

	for _, gw := range genesis.Witnesses {
		if !sdb.AccountExists(gw.Address) {
			t.Errorf("witness %s: no Account record after genesis", gw.Address.Hex())
			continue
		}
		if !sdb.IsWitness(gw.Address) {
			t.Errorf("witness %s: account.IsWitness=false after genesis", gw.Address.Hex())
		}
	}
}

// TestSetupGenesisBlock_WitnessIsJobsSet locks java-tron Manager.initWitness
// parity (Manager.java:725): every genesis witness's persisted WitnessCapsule
// must have is_jobs=true after SetupGenesisBlock. gtron never calls SetIsJobs
// on any other genesis path, so without this gRPC wallet.listwitnesses and the
// conformance digest report is_jobs=false for every witness forever.
func TestSetupGenesisBlock_WitnessIsJobsSet(t *testing.T) {
	genesis := params.DefaultMainnetGenesis()
	if len(genesis.Witnesses) == 0 {
		t.Fatalf("mainnet genesis has no witnesses; test fixture broken")
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	for _, gw := range genesis.Witnesses {
		w := rawdb.ReadWitness(diskdb, gw.Address)
		if w == nil {
			t.Errorf("witness %s: no Witness record after genesis", gw.Address.Hex())
			continue
		}
		if !w.IsJobs() {
			t.Errorf("witness %s: IsJobs=false after genesis, want true", gw.Address.Hex())
		}
	}
}

// TestSetupGenesisBlock_NextMaintenanceTimeRespectsExplicit verifies that
// when the genesis config DOES seed `next_maintenance_time`, the bootstrap
// fallback does not clobber it.
func TestSetupGenesisBlock_NextMaintenanceTimeRespectsExplicit(t *testing.T) {
	const explicit = int64(1700000000000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1}), Balance: 1},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": 21600000,
			"next_maintenance_time":     explicit,
		},
	}
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	dp := loadGenesisDP(t, diskdb)
	if dp.NextMaintenanceTime() != explicit {
		t.Fatalf("explicit next_maintenance_time clobbered: got %d, want %d", dp.NextMaintenanceTime(), explicit)
	}
}

func TestSetupGenesisBlock_EnergyFeeSeedsPriceHistory(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1}), Balance: 1},
		},
		DynamicProperties: map[string]int64{
			"energy_fee": 420,
		},
	}
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	dp := loadGenesisDP(t, diskdb)
	if got := dp.EnergyFee(); got != 420 {
		t.Fatalf("energy_fee: got %d, want 420", got)
	}
	if got := dp.EnergyPriceHistory(); got != "0:420" {
		t.Fatalf("energy_price_history: got %q, want %q", got, "0:420")
	}
}

func TestSetupGenesisBlock_ConstantinopleConfigAddsClearABIOperation(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1}), Balance: 1},
		},
		DynamicProperties: map[string]int64{
			"allow_tvm_constantinople": 1,
		},
	}
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	dp := loadGenesisDP(t, diskdb)
	if !dp.IsContractTypeAvailable(48) {
		t.Fatal("ClearABIContract bit 48 not set in available_contract_type")
	}
	if dp.ActiveDefaultOperations()[48/8]&(1<<(48%8)) == 0 {
		t.Fatal("ClearABIContract bit 48 not set in active_default_operations")
	}
}

func TestGenesisHashDeterministic(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 500},
		},
	}

	diskdb1 := ethrawdb.NewMemoryDatabase()
	block1, _ := GenesisToBlock(genesis, state.NewDatabase(diskdb1))

	diskdb2 := ethrawdb.NewMemoryDatabase()
	block2, _ := GenesisToBlock(genesis, state.NewDatabase(diskdb2))

	if block1.Hash() != block2.Hash() {
		t.Fatalf("genesis hash not deterministic: %x vs %x", block1.Hash(), block2.Hash())
	}
}

func TestSetupGenesisBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()

	config, hash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if config == nil {
		t.Fatal("config should not be nil")
	}
	if hash == (common.Hash{}) {
		t.Fatal("genesis hash should not be zero")
	}

	// Verify genesis block is stored
	block := rawdb.ReadBlock(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), 0)
	if block == nil {
		t.Fatal("genesis block not found in DB")
	}
	if block.Hash() != hash {
		t.Fatal("stored genesis hash mismatch")
	}

	// Second call should succeed with same genesis
	config2, hash2, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if hash2 != hash {
		t.Fatal("second SetupGenesisBlock returned different hash")
	}
	if config2.ChainID != config.ChainID {
		t.Fatal("config mismatch")
	}
}

func TestSetupGenesisBlockWithAncientFindsFrozenGenesis(t *testing.T) {
	genesis := params.DefaultMainnetGenesis()
	diskdb := ethrawdb.NewMemoryDatabase()
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatalf("setup genesis: %v", err)
	}
	blockRaw := rawdb.ReadBlockRaw(diskdb, 0)
	if len(blockRaw) == 0 {
		t.Fatal("genesis block raw bytes missing before freeze")
	}

	fz, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	defer fz.Close()
	if _, err := fz.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		if err := op.AppendRaw("bodies", 0, blockRaw); err != nil {
			return err
		}
		if err := op.AppendRaw("tx_infos", 0, nil); err != nil {
			return err
		}
		return op.AppendRaw("state_roots", 0, nil)
	}); err != nil {
		t.Fatalf("append ancient genesis: %v", err)
	}
	if err := rawdb.DeleteFrozenBlockRange(diskdb, 0, 0); err != nil {
		t.Fatalf("delete hot genesis: %v", err)
	}
	if got := rawdb.ReadBlock(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), 0); got != nil {
		t.Fatal("test setup failed: hot genesis still readable without ancient")
	}

	_, gotHash, err := SetupGenesisBlockWithAncient(diskdb, rawdb.NewFreezerReader(fz), genesis)
	if err != nil {
		t.Fatalf("SetupGenesisBlockWithAncient: %v", err)
	}
	if gotHash != genesisHash {
		t.Fatalf("genesis hash = %x, want %x", gotHash, genesisHash)
	}
}

// TestGenesisToBlock_MatchesJavaTronPrivateChain pins the genesis block hash
// against a live java-tron private chain at /Users/asuka/Works/Tests/TVM/run.
//
// The expected hash was captured 2026-05-02 by the diagnostic
// p2p.TestProbeJavaTronGenesis, which extracts the genesis BlockID from
// the peer's app-layer P2P_HELLO. The first 8 bytes of the BlockID encode
// the block number (zero for genesis); the remaining 24 bytes are the
// trailing 24 bytes of SHA256(BlockHeaderRaw proto bytes).
//
// Concretely, java-tron reported genesis ID
//
//	000000000000000075da3fe749503edb5d6121d96d450b980294a03648934988
//
// and gtron's `Block.ID()` overwrites the leading 8 bytes with the
// big-endian block number, so we compare on the BlockID, not the raw hash.
// TestGenesisCreatesSystemAccount verifies that the reserved system account
// (common.SystemAccountAddress) exists in the persisted post-genesis state so
// it can own chain-global KV entries from block #1 onward.
func TestGenesisCreatesSystemAccount(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1}), Balance: 1},
		},
	}
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	root := rawdb.ReadGenesisStateRoot(diskdb)
	if root == (common.Hash{}) {
		t.Fatalf("genesis state root not persisted")
	}
	sdb, err := state.New(root, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatalf("open state at genesis root: %v", err)
	}
	if !sdb.AccountExists(common.SystemAccountAddress) {
		t.Fatal("genesis must create the reserved system account")
	}
}

// TestGenesisToBlock_MatchesJavaTronPrivateChain pins the genesis block hash
// against a live java-tron private chain at /Users/asuka/Works/Tests/TVM/run.
//
// The expected hash was captured 2026-05-02 by the diagnostic
// p2p.TestProbeJavaTronGenesis, which extracts the genesis BlockID from
// the peer's app-layer P2P_HELLO. The first 8 bytes of the BlockID encode
// the block number (zero for genesis); the remaining 24 bytes are the
// trailing 24 bytes of SHA256(BlockHeaderRaw proto bytes).
//
// Concretely, java-tron reported genesis ID
//
//	000000000000000075da3fe749503edb5d6121d96d450b980294a03648934988
//
// and gtron's `Block.ID()` overwrites the leading 8 bytes with the
// big-endian block number, so we compare on the BlockID, not the raw hash.
func TestGenesisToBlock_MatchesJavaTronPrivateChain(t *testing.T) {
	zion, err := crypto.Base58ToAddress("TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC")
	if err != nil {
		t.Fatalf("decode Zion: %v", err)
	}
	blackhole, err := crypto.Base58ToAddress("TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy")
	if err != nil {
		t.Fatalf("decode Blackhole: %v", err)
	}
	parentBytes, err := hex.DecodeString("e58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f")
	if err != nil {
		t.Fatalf("decode parent: %v", err)
	}

	g := &params.Genesis{
		Config:     params.MainnetChainConfig,
		Timestamp:  0,
		ParentHash: common.BytesToHash(parentBytes),
		Accounts: []params.GenesisAccount{
			{Address: zion, Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: blackhole, Balance: -9_223_372_036_854_775_808, AccountName: "Blackhole"},
		},
		Witnesses: []params.GenesisWitness{
			{Address: zion, VoteCount: 100, URL: "http://test.io"},
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	block, err := GenesisToBlock(g, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatalf("GenesisToBlock: %v", err)
	}

	const wantHex = "000000000000000075da3fe749503edb5d6121d96d450b980294a03648934988"
	id := block.ID()
	gotHex := hex.EncodeToString(id.Hash[:])
	if gotHex != wantHex {
		t.Fatalf("genesis BlockID mismatch:\n  want %s\n  got  %s", wantHex, gotHex)
	}
	if block.AccountStateRoot() != (common.Hash{}) {
		t.Fatalf("genesis accountStateRoot must be zero, got %x", block.AccountStateRoot())
	}
	if len(block.Transactions()) != 2 {
		t.Fatalf("expected 2 genesis txs, got %d", len(block.Transactions()))
	}
}
