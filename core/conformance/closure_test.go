package conformance

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestComputeClosure_Smoke_WitnessOnly(t *testing.T) {
	// Smoke corpus: 5 empty blocks with the same witness → closure = {witness}.
	rdr, err := openBlocksReader("../../test/fixtures/mainnet-blocks/smoke/blocks.bin")
	if err != nil {
		t.Skipf("smoke corpus not present: %v", err)
	}
	defer rdr.Close()

	var blocks []*types.Block
	for {
		b, err := rdr.Next()
		if err != nil {
			break
		}
		blocks = append(blocks, b)
	}
	if len(blocks) == 0 {
		t.Skip("empty smoke corpus")
	}

	addrs, unhandled, err := ComputeClosure(blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(unhandled) != 0 {
		t.Fatalf("unexpected unhandled types: %+v", unhandled)
	}
	if len(addrs) != 1 {
		t.Fatalf("want 1 addr, got %d: %v", len(addrs), addrs)
	}
	hex := ""
	for _, b := range addrs[0] {
		hex += string(b)
		_ = hex
	}
	wantHex := strings.Repeat("a", 40)
	got := addrs[0]
	for _, b := range got[1:] {
		if b != 0xaa {
			t.Fatalf("expected witness 41aaaa…, got %x (want …%s)", got[:], wantHex)
		}
	}
	if got[0] != 0x41 {
		t.Fatalf("bad prefix: %x", got[0])
	}
}

func TestComputeClosure_TransferContract(t *testing.T) {
	witnessHex := "41" + strings.Repeat("a", 40)
	ownerHex := "41" + strings.Repeat("b", 40)
	toHex := "41" + strings.Repeat("c", 40)

	witness, _ := ParseAddress(witnessHex)
	owner, _ := ParseAddress(ownerHex)
	to, _ := ParseAddress(toHex)

	tc := &contractpb.TransferContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       100,
	}
	anyParam, err := anypb.New(tc)
	if err != nil {
		t.Fatal(err)
	}

	rawTx := &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: anyParam,
		}},
	}
	txPB := &corepb.Transaction{RawData: rawTx}
	blk := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				WitnessAddress: witness[:],
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})

	addrs, unhandled, err := ComputeClosure([]*types.Block{blk})
	if err != nil {
		t.Fatal(err)
	}
	if len(unhandled) != 0 {
		t.Fatalf("unhandled: %+v", unhandled)
	}
	if len(addrs) != 3 {
		t.Fatalf("want 3 addrs (witness,owner,to), got %d: %v", len(addrs), addrs)
	}
	seen := map[string]bool{}
	for _, a := range addrs {
		seen[string(a[:])] = true
	}
	if !seen[string(witness[:])] || !seen[string(owner[:])] || !seen[string(to[:])] {
		t.Fatalf("missing expected addr: %v", addrs)
	}
}

func TestComputeClosure_UnknownTypeGoesToUnhandled(t *testing.T) {
	// VoteAssetContract is a real type but not in our switch — should
	// surface as unhandled, not panic.
	anyParam, _ := anypb.New(&contractpb.VoteAssetContract{})
	tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_VoteAssetContract,
			Parameter: anyParam,
		}},
	}}
	blk := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1}},
		Transactions: []*corepb.Transaction{tx},
	})
	_, unhandled, err := ComputeClosure([]*types.Block{blk})
	if err != nil {
		t.Fatal(err)
	}
	if unhandled[corepb.Transaction_Contract_VoteAssetContract] != 1 {
		t.Fatalf("want 1 VoteAssetContract in unhandled, got %+v", unhandled)
	}
}
