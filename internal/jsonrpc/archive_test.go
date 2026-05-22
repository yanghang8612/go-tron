package jsonrpc_test

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// Slice 7 of the State History Index: JSON-RPC archive-query handler tests,
// now exercising the reflection-framework EthAPI (the migration target) rather
// than the removed legacy dispatch.
//
// These exercise the block-tag plumbing on eth_getBalance / eth_getCode /
// eth_getStorageAt — namely that a non-"latest" block argument routes to the
// backend's *At (history-reader-backed) methods, "latest"/absent routes to the
// live read, and a backend gate error (history disabled) surfaces as a JSON-RPC
// error rather than a wrong value. The reconstruction itself is covered at the
// reader / TronBackend layers; here we only validate EthAPI's routing + error
// mapping. The freeze corpus does not cover archive blocks, so this is the
// authoritative coverage for that path.

const archiveTestAddr = "0x4101020304050607080900010203040506070809"

// sunToWeiHex mirrors EthAPI.GetBalance's SUN→wei scaling (× 1e12) for building
// expected eth_getBalance results.
func sunToWeiHex(sun int64) string {
	wei := new(big.Int).Mul(big.NewInt(sun), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei)
}

// jsonStr quotes s as a JSON string literal for postParity's wantResult.
func jsonStr(s string) string { return `"` + s + `"` }

// TestEthGetBalance_ArchiveBlock asserts that a numeric block argument routes
// to GetBalanceAt (the archive path), not GetBalance (live). The stub returns
// distinct values for the two paths so EthAPI's choice is observable.
func TestEthGetBalance_ArchiveBlock(t *testing.T) {
	// live = 1_000_000 SUN, archive = 2_000_000 SUN.
	ts := newEthServer(t, &stubBackend{balance: 1_000_000, balanceAt: 2_000_000, blockNumber: 100})

	// "latest" → live path.
	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["%s","latest"]}`, archiveTestAddr),
		jsonStr(sunToWeiHex(1_000_000)))

	// A historical block number (hex and decimal forms) → archive path.
	for _, blockArg := range []string{"0x5", "5"} {
		postParity(t, ts.URL,
			fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["%s","%s"]}`, archiveTestAddr, blockArg),
			jsonStr(sunToWeiHex(2_000_000)))
	}
}

// TestEthGetCode_ArchiveBlock asserts eth_getCode routes a numeric block to
// GetCodeAt.
func TestEthGetCode_ArchiveBlock(t *testing.T) {
	ts := newEthServer(t, &stubBackend{
		code:        []byte{0x60, 0x80},
		codeAt:      []byte{0xde, 0xad},
		blockNumber: 100,
	})

	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["%s","latest"]}`, archiveTestAddr),
		jsonStr("0x6080"))
	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["%s","0x2"]}`, archiveTestAddr),
		jsonStr("0xdead"))
}

// TestEthGetStorageAt_ArchiveBlock asserts eth_getStorageAt routes a numeric
// block (the 3rd positional arg) to GetStorageAtBlock.
func TestEthGetStorageAt_ArchiveBlock(t *testing.T) {
	liveSlot := common.Hash{0x11}
	histSlot := common.Hash{0x22}
	ts := newEthServer(t, &stubBackend{storage: liveSlot, storageAt: histSlot, blockNumber: 100})

	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getStorageAt","params":["%s","0x0","latest"]}`, archiveTestAddr),
		jsonStr(fmt.Sprintf("0x%x", liveSlot[:])))
	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getStorageAt","params":["%s","0x0","0x3"]}`, archiveTestAddr),
		jsonStr(fmt.Sprintf("0x%x", histSlot[:])))
}

// TestEthArchive_GateError asserts that when the backend's archive query
// returns an error (e.g. history disabled), EthAPI surfaces it as a JSON-RPC
// error for a historical block — but a "latest" query still succeeds via the
// live path that never calls the gated method.
func TestEthArchive_GateError(t *testing.T) {
	gate := errors.New("archive history not available: node not running with --history.enabled")
	ts := newEthServer(t, &stubBackend{balance: 1_000_000, atErr: gate, blockNumber: 100})

	// Historical block → archive path → error surfaced for all three methods.
	for _, body := range []string{
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["%s","0x5"]}`, archiveTestAddr),
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["%s","0x5"]}`, archiveTestAddr),
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getStorageAt","params":["%s","0x0","0x5"]}`, archiveTestAddr),
	} {
		if _, errObj := postRPC(t, ts.URL, body); errObj == nil {
			t.Errorf("with history disabled: expected JSON-RPC error, got none for body=%s", body)
		}
	}

	// "latest" → live path → no error even though atErr is set.
	postParity(t, ts.URL,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["%s","latest"]}`, archiveTestAddr),
		jsonStr(sunToWeiHex(1_000_000)))
}
