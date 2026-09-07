package historychunk

import (
	"bytes"
	"crypto/sha256"
	"math/rand"
	"reflect"
	"testing"
)

func boundaries(data []byte, partition int) []int {
	var s Splitter
	var out []int
	for off := 0; off < len(data); {
		end := min(len(data), off+partition)
		n, cut := s.Next(data[off:end])
		if n == 0 {
			panic("no progress")
		}
		off += n
		if cut {
			out = append(out, off)
		}
	}
	if len(out) == 0 || out[len(out)-1] != len(data) {
		out = append(out, len(data))
	}
	return out
}

func TestPartitionInvariantAndBounds(t *testing.T) {
	data := make([]byte, 2<<20)
	rand.New(rand.NewSource(20260907)).Read(data)
	want := boundaries(data, len(data))
	for _, width := range []int{1, 7, 4093, MaxSize, len(data)} {
		if got := boundaries(data, width); !reflect.DeepEqual(got, want) {
			t.Fatalf("partition %d changed boundaries", width)
		}
	}
	previous := 0
	for i, end := range want {
		n := end - previous
		if n > MaxSize || i < len(want)-1 && n < MinSize {
			t.Fatalf("chunk %d has invalid size %d", i, n)
		}
		previous = end
	}
}

func TestResetEmptyAndLowEntropy(t *testing.T) {
	var s Splitter
	if n, cut := s.Next(nil); n != 0 || cut {
		t.Fatal("empty input")
	}
	s.Next([]byte("pending"))
	if s.Pending() != 7 {
		t.Fatal("pending count")
	}
	s.Reset()
	if s.Pending() != 0 {
		t.Fatal("reset count")
	}
	data := bytes.Repeat([]byte{0}, 3*MaxSize+11)
	for _, end := range boundaries(data, 123) {
		if end <= 0 || end > len(data) {
			t.Fatal("invalid low entropy boundary")
		}
	}
}

func TestInsertionRealignsUnchangedChunks(t *testing.T) {
	data := make([]byte, 4<<20)
	rand.New(rand.NewSource(41)).Read(data)
	insert := 191927
	changed := append(append(append([]byte{}, data[:insert]...), []byte("new arbitrary bytes!")...), data[insert:]...)
	known := map[[32]byte]bool{}
	start := 0
	for _, end := range boundaries(data, 997) {
		known[sha256.Sum256(data[start:end])] = true
		start = end
	}
	start, reused := 0, 0
	for _, end := range boundaries(changed, 4091) {
		if known[sha256.Sum256(changed[start:end])] {
			reused += end - start
		}
		start = end
	}
	if reused < len(data)*9/10 {
		t.Fatalf("only %d/%d bytes realigned", reused, len(data))
	}
}

func BenchmarkSplitter(b *testing.B) {
	data := make([]byte, 4<<20)
	rand.New(rand.NewSource(41)).Read(data)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s Splitter
		for off := 0; off < len(data); {
			n, _ := s.Next(data[off:])
			off += n
		}
	}
}
