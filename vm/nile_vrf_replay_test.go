package vm

// Replay of the Nile block 16,745,722 stall (2021-06-10):
// tx a79813c810e821b266cf9af38f9baa39f3e27020613bcdef0042047c5ac5f496 —
// JustLink (Chainlink port) VRFCoordinator.fulfillRandomnessRequest(proof),
// java SUCCESS energy 52,909 / 2 logs / 1 internal call; gtron REVERTed.
// The proof embeds block number 16,745,511 whose blockhash the coordinator
// mixes into the VRF seed ("please prove blockhash"-style require). The
// slice-3 freezer had already pruned that block's hot b-<num> row (head-211
// is past solidified-128), the KV-only BLOCKHASH read returned 0, and the
// coordinator reverted at ~2.7k energy. With the hash resolvable the full
// VRF verification (bigModExp + ecrecover ecmul trick) matches the java
// receipt exactly. Storage slots seeded from the stalled node's head state;
// block 16,745,511 rebuilt from its canonical header fields.

import (
	"bytes"
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	vrfCoordAddr  = "41cce04c0ae6d7691faee4dcfe2ceca4be7fda0933"
	vrfD20Addr    = "41f4e0e122d0776779d50b12e94ba230db4932bb5f"
	vrfOracleAddr = "4197e3c84e3f402c963264369a098bb2f0922cb125"
	// canonical blockID of Nile block 16,745,511 (the proof's blockNum)
	vrfBlockHash = "0000000000ff842737eddf96785498c824c9752d49254b49887160f4ea17c7b6"
	requestIDHex = "8f404110e3832bb680ee3990798af41ba88a086af889c216325d98cbfd234199"
	keyHashHex   = "e4f280f6d621db4bccd8568197e3c84e3f402c963264369a098bb2f0922cb125"
)

// nileBlock16745511 rebuilds the real Nile block header so that
// types.Block.Hash() reproduces the canonical blockID.
func nileBlock16745511(t *testing.T) *types.Block {
	t.Helper()
	txTrie, _ := hex.DecodeString("e32852f952fdb17f76eea7dc0e3a90cc18ce52aa4930cb1bf4fffcd9b144028e")
	parent, _ := hex.DecodeString("0000000000ff84260e74af2233de5e5d9040298c37026c6feeed5c37c1f31d5a")
	witness, _ := hex.DecodeString("4110cc4eed4e2365c4e57d3f60913c895d1187656e")
	blk := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         16745511,
				Timestamp:      1623313845000,
				TxTrieRoot:     txTrie,
				ParentHash:     parent,
				WitnessAddress: witness,
				Version:        21,
			},
		},
	})
	want, _ := hex.DecodeString(vrfBlockHash)
	if !bytes.Equal(blk.Hash().Bytes(), want) {
		t.Fatalf("rebuilt block 16745511 hash %x != canonical %s", blk.Hash().Bytes(), vrfBlockHash)
	}
	return blk
}

func newVRFState(t *testing.T) (*state.StateDB, *TVM, ethdb.Database) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{vrfCoordAddr, "testdata/nile_vrf16745722/rt_vrf_coord.hex"},
		{vrfD20Addr, "testdata/nile_vrf16745722/rt_vrfd20.hex"},
	} {
		addr := hexAddr(t, pair[0])
		code := mustHexFile(t, pair[1])
		sdb.CreateAccount(addr, corepb.AccountType_Contract)
		sdb.SetCode(addr, code)
	}
	cfg := TVMConfig{
		TransferTrc10:  true,
		Constantinople: true,
		Solidity059:    true,
		ShieldedToken:  true,
		Istanbul:       true,
	}
	// Block context of the failing tx 16,745,722.
	tvm := NewTVM(sdb, nil, hexAddr(t, vrfOracleAddr), 16745722, 1623314484000,
		hexAddr(t, "41a234e405a2c6fd67cdd4d0ea2f6188f65534c8b1"), 1, cfg)
	tvm.SetRootTransactionID(tcommon.HexToHash("a79813c810e821b266cf9af38f9baa39f3e27020613bcdef0042047c5ac5f496"))
	return sdb, tvm, diskdb
}

func seedVRFCoordinatorState(t *testing.T, sdb *state.StateDB) {
	t.Helper()
	coord := hexAddr(t, vrfCoordAddr)
	set := func(key, val string) {
		sdb.SetState(coord, tcommon.HexToHash(key), tcommon.HexToHash(val))
	}
	// callbacks[requestId]: slot0 = randomnessFee(uint96)<<160 | callbackContract,
	// slot1 = seedAndBlockNum (values read from the stalled gtron node head).
	set("7c1815a18257598fe8810c4cbd10daf780b01ada30cce46379d3b8ff8ae287e8",
		"00000000000000000000000cf4e0e122d0776779d50b12e94ba230db4932bb5f")
	set("7c1815a18257598fe8810c4cbd10daf780b01ada30cce46379d3b8ff8ae287e9",
		"5bc26acd4c5d024f31842b85a285ce07f0b2dcbadbfe71b77076b433aee35f28")
	// serviceAgreements[keyHash]: slot0 = fee(uint96)<<160 | vRFOracle, slot1 = jobID.
	set("44ac7b32fbdbe1f1e9ee1c9e003896956f8cfea50211329a364cb5f187a726de",
		"00000000000000000000000a97e3c84e3f402c963264369a098bb2f0922cb125")
	set("44ac7b32fbdbe1f1e9ee1c9e003896956f8cfea50211329a364cb5f187a726df",
		"3034643737333839306263333437663838353434646338356265613234393835")

	// withdrawableTokens[vRFOracle]: the oracle's accrued fees (0x24 read
	// from the stalled node). Without it the fee credit becomes a fresh
	// SSTORE (SET 20000) instead of java's RESET 5000.
	set("248fbc7676d6b7080cec417c8ee5127974539c535102acb557f4cf1492537dc6",
		"0000000000000000000000000000000000000000000000000000000000000024")

	// VRFD20 slot3 = vrfCoordinator address (the callback's "Only
	// VRFCoordinator can fulfill" require reads it).
	sdb.SetState(hexAddr(t, vrfD20Addr),
		tcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000003"),
		tcommon.HexToHash("000000000000000000000000cce04c0ae6d7691faee4dcfe2ceca4be7fda0933"))
}

func callVRFGetter(t *testing.T, tvm *TVM, selectorAndArg string) []byte {
	t.Helper()
	data, err := hex.DecodeString(selectorAndArg)
	if err != nil {
		t.Fatal(err)
	}
	ret, _, callErr := tvm.Call(hexAddr(t, vrfOracleAddr), hexAddr(t, vrfCoordAddr), data, 1_000_000, 0)
	if callErr != nil {
		t.Fatalf("getter %s: %v", selectorAndArg[:8], callErr)
	}
	return ret
}

func TestNileVRFFulfillReplay(t *testing.T) {
	sdb, tvm, diskdb := newVRFState(t)
	seedVRFCoordinatorState(t, sdb)

	// Cross-check the seeded slots against the values served by the stalled
	// node's triggerconstantcontract at head 16,745,721.
	wantCallbacks := "000000000000000000000000f4e0e122d0776779d50b12e94ba230db4932bb5f" +
		"000000000000000000000000000000000000000000000000000000000000000c" +
		"5bc26acd4c5d024f31842b85a285ce07f0b2dcbadbfe71b77076b433aee35f28"
	if got := hex.EncodeToString(callVRFGetter(t, tvm, "21f36509"+requestIDHex)); got != wantCallbacks {
		t.Fatalf("callbacks(requestId) mismatch:\n got %s\nwant %s", got, wantCallbacks)
	}
	wantSA := "00000000000000000000000097e3c84e3f402c963264369a098bb2f0922cb125" +
		"000000000000000000000000000000000000000000000000000000000000000a" +
		"3034643737333839306263333437663838353434646338356265613234393835"
	if got := hex.EncodeToString(callVRFGetter(t, tvm, "75d35070"+keyHashHex)); got != wantSA {
		t.Fatalf("serviceAgreements(keyHash) mismatch:\n got %s\nwant %s", got, wantSA)
	}

	// Provide the real block 16,745,511 for BLOCKHASH.
	blk := nileBlock16745511(t)
	if err := rawdb.WriteBlock(diskdb, blk); err != nil {
		t.Fatal(err)
	}
	tvm.SetDB(diskdb)

	calldata := mustHexFile(t, "testdata/nile_vrf16745722/fulfill_calldata.hex")
	const limit = 142_857 // fee_limit 20 TRX at 140 sun/energy
	ret, left, err := tvm.Call(hexAddr(t, vrfOracleAddr), hexAddr(t, vrfCoordAddr), calldata, limit, 0)
	used := uint64(limit) - left
	t.Logf("fulfill: err=%v used=%d ret=%x logs=%d internal=%d", err, used, ret, len(tvm.Logs), len(tvm.InternalTransactions))
	if err != nil {
		t.Errorf("java canonical result is SUCCESS, got err=%v (revert data %x)", err, ret)
	}
	if used != 52909 {
		t.Errorf("energy: got %d want 52909 (java receipt)", used)
	}
}

// TestNileVRFFulfillNoBlockhash mirrors the failing node: without block
// 16,745,511 resolvable, BLOCKHASH pushes 0 and the coordinator reverts —
// fingerprint should match the stalled node's 2,687-energy revert.
func TestNileVRFFulfillNoBlockhash(t *testing.T) {
	sdb, tvm, _ := newVRFState(t)
	seedVRFCoordinatorState(t, sdb)
	// NOTE: no SetDB — BLOCKHASH resolves to zero.

	calldata := mustHexFile(t, "testdata/nile_vrf16745722/fulfill_calldata.hex")
	const limit = 142_857
	ret, left, err := tvm.Call(hexAddr(t, vrfOracleAddr), hexAddr(t, vrfCoordAddr), calldata, limit, 0)
	used := uint64(limit) - left
	t.Logf("fulfill(no blockhash): err=%v used=%d ret=%x", err, used, ret)
	if err != ErrExecutionReverted {
		t.Errorf("expected the coordinator to revert on zero blockhash, got %v", err)
	}
	if len(ret) != 0 {
		t.Errorf("revert data: got %x want empty (matches the stalled node)", ret)
	}
}
