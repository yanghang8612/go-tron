package vm

// Faithful replay of the Nile block 14,151,095 stall (2021-03-11):
// tx 62420abd9d20a7fefde42c064105e43bfab64caa6a7f517a8694c58802aecee8
// (TrueUSD TokenController.ratifyMint(2, 0x3c7f..., 400M*1e18) — the 3rd
// ratifier signature, which crosses the MULTISIG_MINT_SIGS=3 threshold and
// finalizes the mint through TokenController -> Registry (delegatecall
// proxies) -> TrueUSD.mint). java-tron canonical result: SUCCESS,
// energy_usage_total=81,370, 4 logs (MintRatified, Transfer, Mint,
// FinalizeMint). gtron rejects the block with "expected SUCCESS actual
// REVERT".
//
// The TUSD system (3 OwnedUpgradeabilityProxy + 3 implementations) was
// deployed 2021-03-09 and has only ~36 transactions before the failure, so
// the complete pre-state is reconstructed here by replaying every canonical
// transaction in order, asserting each one's contractRet / energy /
// log-count against the java-tron receipts as we go.
//
// Era flags (Nile 2021-03-11): TransferTrc10/Constantinople/Solidity059/
// ShieldedTRC20/Istanbul active; Freeze/Vote/London and later inactive
// (same era set as the GoldenTron 11,359,658 replay, cross-checked against
// docs/dev/fork-gates.md).

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type tusdReplayTx struct {
	TxID            string `json:"txid"`
	Block           uint64 `json:"block"`
	BlockTs         int64  `json:"block_ts"`
	Type            string `json:"type"`
	Caller          string `json:"caller"`
	To              string `json:"to"`
	Data            string `json:"data"`
	CallValue       int64  `json:"call_value"`
	FeeLimit        int64  `json:"fee_limit"`
	JavaRet         string `json:"java_ret"`
	JavaEnergyTotal uint64 `json:"java_energy_total"`
	JavaLogs        int    `json:"java_logs"`
	JavaInternal    int    `json:"java_internal"`
	CreateName      string `json:"create_name"`
	CreateBytecode  string `json:"create_bytecode"`
	CreateOrigin    string `json:"create_origin"`
	CreatePercent   int64  `json:"create_percent"`
	CreateEnergyLim int64  `json:"create_origin_energy_limit"`
	CreatedAddr     string `json:"created_addr"`
}

func tusdEraConfig() TVMConfig {
	return TVMConfig{
		TransferTrc10:  true,
		Constantinople: true,
		Solidity059:    true,
		ShieldedToken:  true,
		Istanbul:       true,
	}
}

// deployTUSDImpl deploys an implementation contract from its real creation
// bytecode at its canonical address so that proxies can delegatecall it.
func deployTUSDImpl(t *testing.T, sdb *state.StateDB, hexFile, addr, origin string, block uint64, ts int64) {
	t.Helper()
	creation := mustHexFile(t, hexFile)
	originAddr := hexAddr(t, origin)
	contractAddr := hexAddr(t, addr)
	tvm := NewTVM(sdb, nil, originAddr, block, ts,
		hexAddr(t, "41bc20ba9368b49c3d9d68731bb74ed2e88f4ae4c9"), 1, tusdEraConfig())
	meta := &contractpb.SmartContract{
		OriginAddress:              originAddr.Bytes(),
		ContractAddress:            contractAddr.Bytes(),
		ConsumeUserResourcePercent: 100,
		OriginEnergyLimit:          100,
	}
	_, _, _, err := tvm.CreateAtWithTokenAndContract(originAddr, contractAddr, creation, 10_000_000, 0, 0, 0, meta)
	if err != nil {
		t.Fatalf("deploy impl %s failed: %v", addr, err)
	}
}

func TestNileTUSDRatifyMintReplay(t *testing.T) {
	raw, err := os.ReadFile("testdata/nile_tusd14151095/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var txs []tusdReplayTx
	if err := json.Unmarshal(raw, &txs); err != nil {
		t.Fatal(err)
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	// Implementations referenced by the proxies' upgradeTo calls. Deployed
	// from their real creation bytecode just before the proxies were.
	deployTUSDImpl(t, sdb, "testdata/nile_tusd14151095/token_impl_creation.hex",
		"41fd677ac5b551f0eb9d9134a76057d07f3f2127af", "414ab3ded54b146eac9230c63c3f99160f72664c09", 14093700, 1615277400000)
	deployTUSDImpl(t, sdb, "testdata/nile_tusd14151095/ctrl_impl_creation.hex",
		"41d3ae8459d990198adc4aae652de8264e821574fa", "414ab3ded54b146eac9230c63c3f99160f72664c09", 14093710, 1615277430000)
	deployTUSDImpl(t, sdb, "testdata/nile_tusd14151095/reg_impl_creation.hex",
		"41b20a8ef46d2b17de922f3bebba2f68b671b55b11", "414ab3ded54b146eac9230c63c3f99160f72664c09", 14093720, 1615277460000)

	cfg := tusdEraConfig()
	witness := hexAddr(t, "41bc20ba9368b49c3d9d68731bb74ed2e88f4ae4c9")

	// Each canonical transaction sits in its own Nile block, so the real
	// node runs statedb.Commit() between them. That commit prunes
	// storage rows whose value is zero (mirroring java-tron
	// Storage.commit()'s isZero->delete), which is what lets a later
	// SSTORE into a struct field that a prior tx left at zero be billed as
	// a fresh SET (20000) rather than a RESET (5000). Committing and
	// reopening here keeps the replay faithful to that per-block boundary.
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit after impl deploy: %v", err)
	}
	sdb, err = state.New(root, db)
	if err != nil {
		t.Fatalf("reopen after impl deploy: %v", err)
	}

	for i, tx := range txs {
		caller := hexAddr(t, tx.Caller)
		tvm := NewTVM(sdb, nil, caller, tx.Block, tx.BlockTs, witness, 1, cfg)
		tvm.SetRootTransactionID(tcommon.HexToHash(tx.TxID))

		var (
			used    uint64
			ret     []byte
			callErr error
		)
		switch tx.Type {
		case "CreateSmartContract":
			creation, derr := hex.DecodeString(tx.CreateBytecode)
			if derr != nil {
				t.Fatalf("tx %d %s: bad creation bytecode: %v", i, tx.TxID[:12], derr)
			}
			contractAddr := hexAddr(t, tx.CreatedAddr)
			meta := &contractpb.SmartContract{
				OriginAddress:              caller.Bytes(),
				ContractAddress:            contractAddr.Bytes(),
				ConsumeUserResourcePercent: tx.CreatePercent,
				OriginEnergyLimit:          tx.CreateEnergyLim,
				Name:                       tx.CreateName,
			}
			const deployLimit = 10_000_000
			var left uint64
			_, _, left, callErr = tvm.CreateAtWithTokenAndContract(caller, contractAddr, creation, deployLimit, 0, 0, 0, meta)
			used = deployLimit - left
		case "TriggerSmartContract":
			calldata, derr := hex.DecodeString(tx.Data)
			if derr != nil {
				t.Fatalf("tx %d %s: bad calldata: %v", i, tx.TxID[:12], derr)
			}
			// All callers in this history are generously staked; none of the
			// canonical receipts is near any limit, so a flat 10M cap keeps
			// the replay faithful while still letting a runaway divergence
			// surface as OUT_OF_ENERGY.
			const callLimit = 10_000_000
			var left uint64
			ret, left, callErr = tvm.Call(caller, hexAddr(t, tx.To), calldata, callLimit, tx.CallValue)
			used = callLimit - left
		default:
			t.Fatalf("tx %d %s: unexpected type %s", i, tx.TxID[:12], tx.Type)
		}

		gotRet := "SUCCESS"
		switch {
		case callErr == nil:
		case callErr == ErrExecutionReverted:
			gotRet = "REVERT"
		default:
			gotRet = "FAIL(" + callErr.Error() + ")"
		}

		if gotRet != tx.JavaRet {
			t.Errorf("tx %d %s blk %d: result mismatch: got %s want %s (revert data %x)",
				i, tx.TxID[:12], tx.Block, gotRet, tx.JavaRet, ret)
		}
		if used != tx.JavaEnergyTotal {
			t.Errorf("tx %d %s blk %d: energy mismatch: got %d want %d (java receipt)",
				i, tx.TxID[:12], tx.Block, used, tx.JavaEnergyTotal)
		}
		if got := len(tvm.Logs); got != tx.JavaLogs {
			t.Errorf("tx %d %s blk %d: log count mismatch: got %d want %d",
				i, tx.TxID[:12], tx.Block, got, tx.JavaLogs)
		}

		// Commit at the per-block boundary and reopen, as the real node does.
		root, err := sdb.Commit()
		if err != nil {
			t.Fatalf("tx %d %s: commit: %v", i, tx.TxID[:12], err)
		}
		sdb, err = state.New(root, db)
		if err != nil {
			t.Fatalf("tx %d %s: reopen: %v", i, tx.TxID[:12], err)
		}
	}
}
