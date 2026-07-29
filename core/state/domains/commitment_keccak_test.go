package domains

import (
	"bytes"
	"fmt"
	"hash"
	"math/rand"
	"testing"

	gethkeccak "github.com/ethereum/go-ethereum/crypto/keccak"
	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/sha3"
)

type readableHash interface {
	hash.Hash
	Read([]byte) (int, error)
}

func digestWith(h readableHash, chunks ...[]byte) [32]byte {
	h.Reset()
	for _, chunk := range chunks {
		_, _ = h.Write(chunk)
	}
	var out [32]byte
	_, _ = h.Read(out[:])
	return out
}

func TestCommitmentKeccakMatchesLegacyReference(t *testing.T) {
	reference := sha3.NewLegacyKeccak256().(readableHash)
	candidate := gethkeccak.NewLegacyKeccak256().(readableHash)
	rng := rand.New(rand.NewSource(1))

	for _, size := range []int{0, 1, 31, 32, 135, 136, 137, 255, 1024} {
		input := make([]byte, size)
		_, _ = rng.Read(input)
		want := digestWith(reference, input)
		got := digestWith(candidate, input)
		if got != want {
			t.Fatalf("size %d: got %x, want %x", size, got, want)
		}
	}

	// nodeHash writes the domain byte, then one nibble and one hash per child.
	var domain = []byte{1}
	chunks := make([][]byte, 1, 1+16*2)
	chunks[0] = domain
	for nibble := byte(0); nibble < 16; nibble++ {
		chunks = append(chunks, []byte{nibble})
		child := make([]byte, 32)
		_, _ = rng.Read(child)
		chunks = append(chunks, child)
	}
	if got, want := digestWith(candidate, chunks...), digestWith(reference, chunks...); got != want {
		t.Fatalf("branch-shaped input: got %x, want %x", got, want)
	}
}

func TestCommitmentHasherReuseMatchesOneShot(t *testing.T) {
	h := borrowKeccak()
	defer returnKeccak(h)

	for _, tc := range []struct {
		key   []byte
		value []byte
	}{
		{key: nil, value: nil},
		{key: []byte("account"), value: []byte("value")},
		{key: bytes.Repeat([]byte{0xab}, 137), value: bytes.Repeat([]byte{0xcd}, 255)},
	} {
		if got, want := keyPathWithHasher(h, tc.key), keyPath(tc.key); got != want {
			t.Fatalf("keyPathWithHasher(%x) = %x, want %x", tc.key, got, want)
		}
		if got, want := leafValueHashWithHasher(h, tc.key, tc.value), leafValueHash(tc.key, tc.value); got != want {
			t.Fatalf("leafValueHashWithHasher(%x) = %x, want %x", tc.key, got, want)
		}
	}

	var branch BranchData
	branch.SetHashChild(1, common.Hash{1, 2, 3})
	branch.SetLeafChild(9, []byte("leaf"), common.Hash{4, 5, 6})
	if got, want := branch.nodeHashWith(h), branch.nodeHash(); got != want {
		t.Fatalf("nodeHashWith(reused) = %x, want %x", got, want)
	}
}

func benchmarkCommitmentKeccak(b *testing.B, newHash func() readableHash) {
	h := newHash()
	domain := []byte{1}
	nibbles := make([]byte, 16)
	children := make([]byte, 16*32)
	for i := range nibbles {
		nibbles[i] = byte(i)
	}
	if _, err := rand.New(rand.NewSource(2)).Read(children); err != nil {
		b.Fatal(err)
	}
	var out [32]byte
	b.ReportAllocs()
	b.SetBytes(int64(1 + len(nibbles) + len(children)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		_, _ = h.Write(domain)
		for nibble := range nibbles {
			_, _ = h.Write(nibbles[nibble : nibble+1])
			_, _ = h.Write(children[nibble*32 : (nibble+1)*32])
		}
		_, _ = h.Read(out[:])
	}
	if bytes.Equal(out[:], make([]byte, len(out))) {
		b.Fatal("unexpected zero digest")
	}
}

func BenchmarkCommitmentKeccakXCrypto(b *testing.B) {
	benchmarkCommitmentKeccak(b, func() readableHash {
		return sha3.NewLegacyKeccak256().(readableHash)
	})
}

func BenchmarkCommitmentKeccakGeth(b *testing.B) {
	benchmarkCommitmentKeccak(b, func() readableHash {
		return gethkeccak.NewLegacyKeccak256().(readableHash)
	})
}

func BenchmarkCommitmentKeccakGethContiguous(b *testing.B) {
	h := gethkeccak.NewLegacyKeccak256().(readableHash)
	input := make([]byte, 1+16*(1+32))
	input[0] = 1
	for nibble := 0; nibble < 16; nibble++ {
		off := 1 + nibble*(1+32)
		input[off] = byte(nibble)
		_, _ = rand.New(rand.NewSource(int64(nibble + 3))).Read(input[off+1 : off+1+32])
	}
	var out [32]byte
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		_, _ = h.Write(input)
		_, _ = h.Read(out[:])
	}
	if bytes.Equal(out[:], make([]byte, len(out))) {
		b.Fatal("unexpected zero digest")
	}
}

var branchNodeHashSink common.Hash

func BenchmarkBranchDataNodeHash(b *testing.B) {
	benchmarkBranchDataNodeHash(b, false)
}

func BenchmarkBranchDataNodeHashReusedHasher(b *testing.B) {
	benchmarkBranchDataNodeHash(b, true)
}

func benchmarkBranchDataNodeHash(b *testing.B, reuseHasher bool) {
	for _, children := range []int{1, 4, 16} {
		var branch BranchData
		for nibble := 0; nibble < children; nibble++ {
			h := common.Hash{byte(nibble), byte(nibble + 1)}
			if nibble&1 == 0 {
				branch.SetHashChild(uint8(nibble), h)
			} else {
				branch.SetLeafChild(uint8(nibble), []byte{byte(nibble)}, h)
			}
		}
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			var hasher *pooledKeccak
			if reuseHasher {
				hasher = borrowKeccak()
				b.Cleanup(func() { returnKeccak(hasher) })
			}
			b.ReportAllocs()
			for b.Loop() {
				if hasher == nil {
					branchNodeHashSink = branch.nodeHash()
				} else {
					branchNodeHashSink = branch.nodeHashWith(hasher)
				}
			}
		})
	}
}
