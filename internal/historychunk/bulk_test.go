package historychunk

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestBulkCutsMatchScalarWithPendingAndPartitions(t *testing.T) {
	rng := rand.New(rand.NewSource(20260908))
	random := make([]byte, 5<<20)
	rng.Read(random)
	inputs := [][]byte{random, bytes.Repeat([]byte{0}, 3<<20), bytes.Repeat([]byte("bulk-history-pattern"), 120000)}
	for kind, p := range inputs {
		for _, workers := range []int{0, 1, 2, 4, 8, 100} {
			for _, prefixSize := range []int{0, 1, 63, MinSize - 1, MinSize + 7, MaxSize - 1} {
				t.Run(fmt.Sprintf("%d/%d/%d", kind, workers, prefixSize), func(t *testing.T) {
					var scalar, bulk Splitter
					prefix := random[:prefixSize]
					scalar.scalarCuts(prefix)
					bulk.scalarCuts(prefix)
					for offset := 0; offset < len(p); {
						// Alternate scalar-size and parallel-size input writes.
						size := min(len(p)-offset, (1<<20)+offset%70001)
						chunk := p[offset : offset+size]
						want, got := scalar.scalarCuts(chunk), bulk.Cuts(chunk, workers)
						if !reflect.DeepEqual(want, got) || scalar != bulk {
							t.Fatalf("offset %d: cuts or pending/hash changed; scalar=%+v bulk=%+v", offset, scalar, bulk)
						}
						offset += size
					}
				})
			}
		}
	}
}

func TestBulkCandidatesFinalShortTailAndDenseBoundaryReduction(t *testing.T) {
	random := make([]byte, (1<<20)+64)
	rand.New(rand.NewSource(1)).Read(random)
	// Force cuts at each possible final tail length, including a hash reset
	// within the last 63 bytes. Candidate reduction must respect min spacing.
	for tail := 0; tail < 64; tail++ {
		p := random[:len(random)-tail]
		var s Splitter
		groups := []bulkCandidates{{positions: []int{1, 2, MinSize, MinSize + 1, 2 * MinSize, len(p) - tail}, endHash: 123}}
		cuts := s.consumeCandidates(p, groups)
		if len(cuts) == 0 {
			t.Fatal("no cuts")
		}
		previous := 0
		for _, end := range cuts {
			if end-previous < MinSize || end-previous > MaxSize {
				t.Fatal("min/max boundary violated")
			}
			previous = end
		}
		if len(p)-previous < 64 {
			var h uint64
			for _, b := range p[previous:] {
				h = h<<1 + gear[b]
			}
			if s.hash != h {
				t.Fatal("short tail did not reset hash")
			}
		}
	}
}

func TestBulkCutsDoNotRetainOrModifyInput(t *testing.T) {
	p := make([]byte, 2<<20)
	rand.New(rand.NewSource(2)).Read(p)
	before := bytes.Clone(p)
	var s Splitter
	s.Cuts(p, 4)
	if !bytes.Equal(p, before) {
		t.Fatal("input mutated")
	}
	want := s
	clear(p)
	if s != want {
		t.Fatal("splitter retained mutable input")
	}
	if got := s.Cuts(nil, 8); got != nil || s != want {
		t.Fatal("empty write changed state")
	}
}

func TestBulkCutsDenseCandidatesFallBackWithoutChangingState(t *testing.T) {
	// A repeated triple has a steady-state zero candidate when
	// 4*gear[a]+2*gear[b]+gear[c] is zero modulo TargetSize. This triggers the
	// memory cap with real input, without mutating the shared hash table.
	var pattern []byte
	byHash := make(map[uint64]byte)
	for c := 0; c < 256; c++ {
		byHash[gear[c]&(TargetSize-1)] = byte(c)
	}
	for a := 0; a < 256 && pattern == nil; a++ {
		for b := 0; b < 256; b++ {
			if c, ok := byHash[(0-4*gear[a]-2*gear[b])&(TargetSize-1)]; ok {
				pattern = []byte{byte(a), byte(b), c}
				break
			}
		}
	}
	if pattern == nil {
		t.Fatal("no dense-candidate fixture for this Gear table")
	}
	p := bytes.Repeat(pattern, 1<<20)
	for _, workers := range []int{2, 4, 8} {
		var scalar, bulk Splitter
		prefix := bytes.Repeat([]byte{17}, MinSize+7)
		scalar.scalarCuts(prefix)
		bulk.scalarCuts(prefix)
		want, got := scalar.scalarCuts(p), bulk.Cuts(p, workers)
		if !reflect.DeepEqual(want, got) || scalar != bulk {
			t.Fatalf("workers %d: dense fallback changed cuts or state", workers)
		}
	}
}

func BenchmarkBulkCuts(b *testing.B) {
	p := make([]byte, 8<<20)
	rand.New(rand.NewSource(1)).Read(p)
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.SetBytes(int64(len(p)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var s Splitter
				s.Cuts(p, workers)
			}
		})
	}
}
