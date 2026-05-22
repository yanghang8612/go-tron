package state

import (
	"bytes"
	"strconv"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// mapTokens is large enough that a non-deterministic protobuf map encoding
// (Go map iteration order) will, with overwhelming probability, differ between
// two independent marshals on at least one of the loop iterations below.
const mapTokens = 32

// assetAccount builds an Account whose AssetV2 protobuf map has many entries.
func assetAccount(addr tcommon.Address) *types.Account {
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	pb := acc.Proto()
	pb.AssetV2 = make(map[string]int64, mapTokens)
	for i := 0; i < mapTokens; i++ {
		// keys are token-ID strings, mirroring SetTRC10Balance
		pb.AssetV2[strconv.Itoa(1000+i)] = int64(i + 1)
	}
	return acc
}

// TestAccountMarshalDeterministicWithMaps isolates the determinism guarantee to
// Account.Marshal() itself, independent of the trie/RLP machinery. Two marshals
// of the same account with a populated protobuf map must produce byte-identical
// output. With the default proto.Marshal (no Deterministic option) the map is
// emitted in randomized Go iteration order, so this fails today.
func TestAccountMarshalDeterministicWithMaps(t *testing.T) {
	acc := assetAccount(testAddr(0x42))
	want, err := acc.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for i := 0; i < 64; i++ {
		got, err := acc.Marshal()
		if err != nil {
			t.Fatalf("marshal iter %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Account.Marshal() not deterministic at iter %d: map fields reordered", i)
		}
	}
}

// TestCommitInternalRootDeterministicWithMaps is the end-to-end guarantee: the
// internal full-state root must be reproducible for an account carrying a
// multi-entry protobuf map. Each iteration commits the same logical account on a
// fresh in-memory DB; all roots must be byte-identical. This is the root-state
// reproducibility property the rooted-state initiative depends on.
func TestCommitInternalRootDeterministicWithMaps(t *testing.T) {
	addr := testAddr(0x42)

	commitOnce := func() tcommon.Hash {
		sdb := newTestStateDB(t)
		sdb.CreateAccount(addr, corepb.AccountType_Normal)
		for i := 0; i < mapTokens; i++ {
			sdb.SetTRC10Balance(addr, int64(1000+i), int64(i+1))
		}
		root, err := sdb.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return root
	}

	want := commitOnce()
	for i := 0; i < 24; i++ {
		if got := commitOnce(); got != want {
			t.Fatalf("internal state root not deterministic at iter %d: got %s want %s", i, got.Hex(), want.Hex())
		}
	}
}
