package core

// Replay of the Nile block 18,278,266 envelope-validation stall (2021-07-29):
// tx 6ca2cf5c23523f6f5eb8283164c93bc039bc263ef4afb26cdfc18aea2cc33f27, a
// TransferContract whose owner address is exactly keccak256("")[12:] — the
// "ghost" address with no private key. Its single signature is constructed so
// that recId=0 ECDSA recovery is the point at infinity. java-tron's
// ECKey.recoverPubBytesFromSignature does not reject that: it encodes infinity
// as one 0x00 byte and computeAddress hashes the empty slice back to
// keccak256("")[12:], matching the owner — so the default Owner permission
// clears and every java node accepted the block. gtron's recover (go-ethereum)
// rejected the infinity point, failing envelope validation. The
// SigToAddressJavaCompat shim makes gtron mirror java.

import (
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"google.golang.org/protobuf/proto"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestNileGhostAddressEnvelopeValidates(t *testing.T) {
	rawData, err := hex.DecodeString(
		"0a02e7762208f4377c4b5da1842a4090b19ecfb02f5a67080112630a2d747970652e676f6f676c65" +
			"617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412320a1541" +
			"dcc703c0e500b653ca82273b7bfad8045d85a47012154175f09e51f8ecb695a0be1701581ec949" +
			"3b16449518c0843d70d2ec9acfb02f")
	if err != nil {
		t.Fatal(err)
	}
	rawPB := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, rawPB); err != nil {
		t.Fatal(err)
	}
	sig, _ := hex.DecodeString(
		"bdd60d53b9876fd4c2d3d142d43b60350e70dbc0554d4ec34c977518bda2db24" +
			"4cf392d2900d507cec8bcc9e7aae71f9267069c8a454aca0f637c6470e6cc2be00")

	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData:   rawPB,
		Signature: [][]byte{sig},
	})

	// Sanity: the rebuilt tx hashes to the canonical Nile txID.
	if got := hex.EncodeToString(tx.Hash().Bytes()); got != "6ca2cf5c23523f6f5eb8283164c93bc039bc263ef4afb26cdfc18aea2cc33f27" {
		t.Fatalf("tx hash mismatch: got %s (fixture decode is wrong)", got)
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	// The owner account is never materialized on-chain (it's a ghost), so
	// ValidateTxEnvelope falls back to the default Owner permission keyed on
	// the owner address — which is exactly keccak256("")[12:], the address the
	// java-compat recovery yields.
	if err := ValidateTxEnvelope(tx, statedb, false); err != nil {
		t.Fatalf("envelope must validate (java accepted block 18,278,266): %v", err)
	}
}
