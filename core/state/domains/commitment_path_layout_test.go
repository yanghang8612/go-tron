package domains

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// Build expected wire bytes directly from the fixture, without consulting the
// branch's masks or encoder. Include empty and multi-byte-length legacy keys
// alongside path leaves, then reuse the same decoder destination.
func TestCommitmentMixedLeafLayoutBytesAndOwnership(t *testing.T) {
	rng := rand.New(rand.NewSource(20260911))
	var decoded BranchData
	var arena []byte
	for iteration := 0; iteration < 512; iteration++ {
		var branch BranchData
		want := []byte{0, 0}
		var mask uint16
		for nibble := uint8(0); nibble < 16; nibble++ {
			kind := rng.Intn(4)
			if kind == 3 {
				continue
			}
			mask |= 1 << nibble
			var value, path common.Hash
			_, _ = rng.Read(value[:])
			_, _ = rng.Read(path[:])
			want = append(want, byte(kind))
			switch kind {
			case int(kindHash):
				branch.SetHashChild(nibble, value)
			case int(kindLeaf):
				key := make([]byte, []int{0, 1, 32, 127, 128, 256}[rng.Intn(6)])
				_, _ = rng.Read(key)
				branch.SetLeafChild(nibble, key, value)
				want = binary.AppendUvarint(want, uint64(len(key)))
				want = append(want, key...)
			case int(kindLeafPath):
				branch.setLeafChildPath(nibble, path[:], value)
				want = append(want, path[:]...)
			}
			want = append(want, value[:]...)
		}
		binary.BigEndian.PutUint16(want, mask)
		if got := branch.Encode(); !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: encoded bytes differ", iteration)
		}
		input := bytes.Clone(want)
		arena = arena[:0]
		if err := decodeBranchDataIntoArena(input, &decoded, &arena); err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		clear(input)
		if got := decoded.Encode(); !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: decoded branch borrowed scoped input", iteration)
		}
		if branch.nodeHash() != decoded.nodeHash() {
			t.Fatalf("iteration %d: node hash changed", iteration)
		}
	}
}

func BenchmarkCommitmentPathLeafLayout(b *testing.B) {
	for _, children := range []int{2, 4, 16} {
		for _, legacy := range []bool{false, true} {
			var branch BranchData
			for i := 0; i < children; i++ {
				h := common.Hash{byte(i + 1)}
				if legacy && i%2 == 0 {
					branch.SetLeafChild(uint8(i), bytes.Repeat([]byte{byte(i)}, 32+i), h)
				} else {
					branch.setLeafChildPath(uint8(i), h[:], h)
				}
			}
			name := fmt.Sprintf("children=%d/mixed_legacy=%t", children, legacy)
			b.Run(name+"/layout", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchmarkBranchBits, benchmarkBranchSize = branch.encodingLayout()
				}
			})
			b.Run(name+"/encode", func(b *testing.B) {
				buf := make([]byte, 0, len(branch.Encode()))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					benchmarkEncodedBranch = branch.EncodeTo(buf[:0])
				}
			})
			b.Run(name+"/decode_arena", func(b *testing.B) {
				encoded := branch.Encode()
				var decoded BranchData
				var arena []byte
				if err := decodeBranchDataIntoArena(encoded, &decoded, &arena); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					arena = arena[:0]
					if err := decodeBranchDataIntoArena(encoded, &decoded, &arena); err != nil {
						b.Fatal(err)
					}
				}
				benchmarkDecodedBranch = decoded
			})
		}
	}
}
