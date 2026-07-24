package types

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var transactionMerkleBenchmarkSink common.Hash

func BenchmarkTransactionMerkleRoot64(b *testing.B) {
	tx := benchmarkTransactionPB(b)
	transactions := make([]*corepb.Transaction, 64)
	for i := range transactions {
		transactions[i] = tx
	}
	b.ReportAllocs()
	for b.Loop() {
		root, err := TransactionMerkleRoot(transactions)
		if err != nil {
			b.Fatal(err)
		}
		transactionMerkleBenchmarkSink = root
	}
}

func leafHash(b byte) common.Hash {
	var h common.Hash
	h[31] = b
	return h
}

func TestTransactionMerkleRootHashesFullProto(t *testing.T) {
	tx := &corepb.Transaction{
		RawData:   &corepb.TransactionRaw{Timestamp: 1234},
		Signature: [][]byte{{1, 2, 3}},
		Ret:       []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}},
	}
	encoded, err := proto.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	want := common.Hash(sha256.Sum256(encoded))
	got, err := TransactionMerkleRoot([]*corepb.Transaction{tx})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("transaction merkle leaf: got %x, want full-proto hash %x", got, want)
	}

	raw, err := proto.Marshal(tx.RawData)
	if err != nil {
		t.Fatal(err)
	}
	if got == common.Hash(sha256.Sum256(raw)) {
		t.Fatal("transaction merkle leaf must not use the raw_data-only txid")
	}
}

func TestBlockValidateTransactionMerkleRoot(t *testing.T) {
	tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7}}
	root, err := TransactionMerkleRoot([]*corepb.Transaction{tx})
	if err != nil {
		t.Fatal(err)
	}
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 9, TxTrieRoot: root.Bytes()}},
		Transactions: []*corepb.Transaction{tx},
	})
	if err := block.ValidateTransactionMerkleRoot(); err != nil {
		t.Fatalf("valid root rejected: %v", err)
	}

	block.Proto().BlockHeader.RawData.TxTrieRoot[0] ^= 0xff
	if err := block.ValidateTransactionMerkleRoot(); err == nil {
		t.Fatal("mismatching root accepted")
	}
	block.Proto().BlockHeader.RawData.TxTrieRoot = nil
	if err := block.ValidateTransactionMerkleRoot(); err == nil {
		t.Fatal("missing 32-byte root accepted")
	}
}

// TestMerkleRoot_Empty: java-tron stores 32 bytes of zero in `tx_trie_root`
// for blocks with no transactions (verified live: block #1 of the local
// java-tron private chain has txTrieRoot == 32×0).
func TestMerkleRoot_Empty(t *testing.T) {
	if got := MerkleRoot(nil); got != (common.Hash{}) {
		t.Fatalf("empty: want zero hash, got %x", got)
	}
}

// TestMerkleRoot_Single: single leaf carries up unchanged
// (java-tron MerkleTree returns the leaf as the root when count == 1).
func TestMerkleRoot_Single(t *testing.T) {
	leaf := leafHash(0x42)
	if got := MerkleRoot([]common.Hash{leaf}); got != leaf {
		t.Fatalf("single: want %x, got %x", leaf, got)
	}
}

// TestMerkleRoot_TwoLeaves: parent = SHA256(left || right).
func TestMerkleRoot_TwoLeaves(t *testing.T) {
	a, b := leafHash(0xAA), leafHash(0xBB)
	h := sha256.New()
	h.Write(a[:])
	h.Write(b[:])
	var want common.Hash
	copy(want[:], h.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b}); got != want {
		t.Fatalf("two: want %x, got %x", want, got)
	}
}

// TestMerkleRoot_Three_OddCarriesUnchanged: with odd count at any level,
// the trailing leaf carries up unchanged (no doubling). This is the
// detail that diverges from naive Bitcoin-style merkle and is critical
// for matching java-tron.
func TestMerkleRoot_Three_OddCarriesUnchanged(t *testing.T) {
	a, b, c := leafHash(1), leafHash(2), leafHash(3)
	// Level 1: [SHA(a||b), c]
	h1 := sha256.New()
	h1.Write(a[:])
	h1.Write(b[:])
	var ab common.Hash
	copy(ab[:], h1.Sum(nil))
	// Level 2: SHA(ab || c)
	h2 := sha256.New()
	h2.Write(ab[:])
	h2.Write(c[:])
	var want common.Hash
	copy(want[:], h2.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b, c}); got != want {
		t.Fatalf("three: want %x, got %x", want, got)
	}
}

// TestMerkleRoot_Four_FullPaired: paired all the way up.
func TestMerkleRoot_Four_FullPaired(t *testing.T) {
	a, b, c, d := leafHash(1), leafHash(2), leafHash(3), leafHash(4)
	// Level 1: [SHA(a||b), SHA(c||d)]
	h := sha256.New()
	h.Write(a[:])
	h.Write(b[:])
	var ab common.Hash
	copy(ab[:], h.Sum(nil))
	h = sha256.New()
	h.Write(c[:])
	h.Write(d[:])
	var cd common.Hash
	copy(cd[:], h.Sum(nil))
	// Level 2: SHA(ab || cd)
	h = sha256.New()
	h.Write(ab[:])
	h.Write(cd[:])
	var want common.Hash
	copy(want[:], h.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b, c, d}); got != want {
		t.Fatalf("four: want %x, got %x", want, got)
	}
}

func TestMerkleRootDoesNotMutateInput(t *testing.T) {
	leaves := []common.Hash{leafHash(1), leafHash(2), leafHash(3), leafHash(4), leafHash(5)}
	want := append([]common.Hash(nil), leaves...)
	_ = MerkleRoot(leaves)
	for i := range leaves {
		if leaves[i] != want[i] {
			t.Fatalf("leaf %d mutated: got %x, want %x", i, leaves[i], want[i])
		}
	}
}

func TestMerkleRootMatchesReferenceAcrossWidths(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for count := 0; count <= 257; count++ {
		leaves := make([]common.Hash, count)
		for i := range leaves {
			if _, err := rng.Read(leaves[i][:]); err != nil {
				t.Fatal(err)
			}
		}
		original := append([]common.Hash(nil), leaves...)
		want := referenceMerkleRoot(leaves)
		if got := MerkleRoot(leaves); got != want {
			t.Fatalf("count %d: got %x, want %x", count, got, want)
		}
		for i := range leaves {
			if leaves[i] != original[i] {
				t.Fatalf("count %d leaf %d mutated", count, i)
			}
		}
	}
}

func referenceMerkleRoot(leaves []common.Hash) common.Hash {
	if len(leaves) == 0 {
		return common.Hash{}
	}
	level := append([]common.Hash(nil), leaves...)
	for len(level) > 1 {
		next := make([]common.Hash, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			var pair [2 * sha256.Size]byte
			copy(pair[:sha256.Size], level[i][:])
			copy(pair[sha256.Size:], level[i+1][:])
			next = append(next, sha256.Sum256(pair[:]))
		}
		level = next
	}
	return level[0]
}
