package core

import (
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	dp := state.LoadDynamicProperties(diskdb)
	if dp.NextMaintenanceTime() <= 0 {
		t.Fatalf("NextMaintenanceTime must be > 0 after genesis bootstrap (mainnet); got %d", dp.NextMaintenanceTime())
	}
	wantNext := genesis.Timestamp + dp.MaintenanceTimeInterval()
	if dp.NextMaintenanceTime() != wantNext {
		t.Fatalf("NextMaintenanceTime: got %d, want %d (= genesis.Timestamp + MaintenanceTimeInterval)",
			dp.NextMaintenanceTime(), wantNext)
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
	dp := state.LoadDynamicProperties(diskdb)
	if dp.NextMaintenanceTime() != explicit {
		t.Fatalf("explicit next_maintenance_time clobbered: got %d, want %d", dp.NextMaintenanceTime(), explicit)
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
	block := rawdb.ReadBlock(diskdb, 0)
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
