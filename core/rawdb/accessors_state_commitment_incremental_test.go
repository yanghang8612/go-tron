package rawdb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

var benchmarkStateCommitmentUpdates []StateCommitmentUpdate
var benchmarkStateCommitmentUpdate StateCommitmentUpdate

func TestAppendStateAccountLatestCommitmentKeyArena(t *testing.T) {
	owners := []common.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}}
	arena := make([]byte, 0, len(owners)*StateAccountLatestCommitmentKeySize())
	keys := make([][]byte, 0, len(owners))
	for _, owner := range owners {
		start := len(arena)
		arena = AppendStateAccountLatestCommitmentKey(arena, owner)
		keys = append(keys, arena[start:len(arena):len(arena)])
	}
	if len(arena) != cap(arena) {
		t.Fatalf("arena len/cap = %d/%d", len(arena), cap(arena))
	}
	for i, owner := range owners {
		want := StateAccountLatestCommitmentKey(owner)
		if !bytes.Equal(keys[i], want) {
			t.Fatalf("key %d = %x, want %x", i, keys[i], want)
		}
		if i > 0 && &keys[i][0] != &arena[i*StateAccountLatestCommitmentKeySize()] {
			t.Fatalf("key %d does not point into its arena segment", i)
		}
	}
}

func BenchmarkStateAccountLatestCommitmentKeys(b *testing.B) {
	const count = 1024
	owners := make([]common.Address, count)
	for i := range owners {
		binary.BigEndian.PutUint64(owners[i][common.AddressLength-8:], uint64(i))
	}
	b.Run("individual", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			updates := make([]StateCommitmentUpdate, count)
			for i, owner := range owners {
				updates[i].Key = StateAccountLatestCommitmentKey(owner)
			}
			benchmarkStateCommitmentUpdates = updates
		}
	})
	b.Run("arena", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			updates := make([]StateCommitmentUpdate, count)
			arena := make([]byte, 0, count*StateAccountLatestCommitmentKeySize())
			for i, owner := range owners {
				start := len(arena)
				arena = AppendStateAccountLatestCommitmentKey(arena, owner)
				updates[i].Key = arena[start:len(arena):len(arena)]
			}
			benchmarkStateCommitmentUpdates = updates
		}
	})
}

func makeSortedCommitmentUpdates(n int) []StateCommitmentUpdate {
	updates := make([]StateCommitmentUpdate, n)
	for i := range updates {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		updates[i] = StateCommitmentUpdate{Key: key, Value: []byte{byte(i)}}
	}
	return updates
}

func BenchmarkCoalesceStateCommitmentUpdatesSortedUnique(b *testing.B) {
	updates := makeSortedCommitmentUpdates(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkStateCommitmentUpdates = CoalesceStateCommitmentUpdates(updates)
	}
}

func BenchmarkStateCommitmentUpdateConstructors(b *testing.B) {
	key := bytes.Repeat([]byte{0x11}, 64)
	value := bytes.Repeat([]byte{0x22}, 64)
	b.Run("defensive-copy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkStateCommitmentUpdate = NewStateCommitmentPut(key, value)
		}
	})
	b.Run("owned", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkStateCommitmentUpdate = NewStateCommitmentPutOwned(key, value)
		}
	})
}

func TestCoalesceStateCommitmentUpdatesSortedUniqueReturnsInput(t *testing.T) {
	updates := makeSortedCommitmentUpdates(16)
	got := CoalesceStateCommitmentUpdates(updates)
	if len(got) != len(updates) || &got[0] != &updates[0] {
		t.Fatal("sorted unique updates should pass through without rebuilding")
	}
}

func TestCoalesceStateCommitmentUpdatesGeneralSemantics(t *testing.T) {
	updates := []StateCommitmentUpdate{
		{Key: []byte("b"), Value: []byte("old")},
		{Key: nil, Value: []byte("ignored")},
		{Key: []byte("a"), Value: []byte("a")},
		{Key: []byte("b"), Value: []byte("new")},
	}
	got := CoalesceStateCommitmentUpdates(updates)
	if len(got) != 2 || !bytes.Equal(got[0].Key, []byte("a")) || !bytes.Equal(got[1].Key, []byte("b")) || !bytes.Equal(got[1].Value, []byte("new")) {
		t.Fatalf("coalesce = %#v, want sorted unique last-writer-wins updates", got)
	}
}
