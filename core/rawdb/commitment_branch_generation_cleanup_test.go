package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type branchGenerationCountingDB struct {
	ethdb.KeyValueStore
	nextCalls    int
	iterators    int
	failIterator int
}

func (db *branchGenerationCountingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	db.iterators++
	return &branchGenerationCountingIterator{Iterator: db.KeyValueStore.NewIterator(prefix, start), db: db, fail: db.iterators == db.failIterator}
}

type branchGenerationCountingIterator struct {
	ethdb.Iterator
	db   *branchGenerationCountingDB
	fail bool
}

func (it *branchGenerationCountingIterator) Next() bool {
	it.db.nextCalls++
	if it.fail {
		return false
	}
	return it.Iterator.Next()
}
func (it *branchGenerationCountingIterator) Error() error {
	if it.fail {
		return errors.New("generation seek failure")
	}
	return it.Iterator.Error()
}

func TestCommitmentBranchGenerationCleanupSeeksPastRetainedRows(t *testing.T) {
	for _, keep := range []uint64{1, 255, math.MaxUint64} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			db := &branchGenerationCountingDB{KeyValueStore: NewMemoryDatabase()}
			defer db.Close()
			for _, g := range []uint64{1, 255, math.MaxUint64} {
				space, _ := NewCommitmentBranchDeltaKeyspace(g)
				rows := 1
				if g == keep {
					rows = 4096
				}
				for i := 0; i < rows; i++ {
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], uint64(i))
					if err := space.Write(db, key[:], []byte{1}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := WriteCommitmentBranch(db, nil, []byte("legacy")); err != nil {
				t.Fatal(err)
			}
			if err := DeleteCommitmentBranchDeltaGenerationsExcept(db, keep); err != nil {
				t.Fatal(err)
			}
			if db.nextCalls > 20 {
				t.Fatalf("cleanup walked retained rows: %d Next calls", db.nextCalls)
			}
			for _, g := range []uint64{1, 255, math.MaxUint64} {
				space, _ := NewCommitmentBranchDeltaKeyspace(g)
				present, err := space.HasRows(db)
				if err != nil || present != (g == keep) {
					t.Fatalf("generation %d present=%v err=%v", g, present, err)
				}
			}
			if _, present, err := ReadCommitmentBranch(db, nil); err != nil || !present {
				t.Fatalf("legacy changed: %v %v", present, err)
			}
		})
	}
}

func TestCommitmentBranchGenerationCleanupDiscoversErrorsBeforeDeleting(t *testing.T) {
	for _, kind := range []string{"short-boundary", "zero-generation", "iterator"} {
		t.Run(kind, func(t *testing.T) {
			db := &branchGenerationCountingDB{KeyValueStore: NewMemoryDatabase()}
			defer db.Close()
			space, _ := NewCommitmentBranchDeltaKeyspace(255)
			if err := space.Write(db, nil, []byte{1}); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "short-boundary":
				upper := prefixUpperBound(space.prefix())
				if err := db.Put(upper, []byte{1}); err != nil {
					t.Fatal(err)
				}
			case "zero-generation":
				key := append(append([]byte(nil), stateCommitmentBranchDeltaPrefix...), make([]byte, 8)...)
				if err := db.Put(key, []byte{1}); err != nil {
					t.Fatal(err)
				}
			case "iterator":
				db.failIterator = 2
			}
			if err := DeleteCommitmentBranchDeltaGenerationsExcept(db, math.MaxUint64); err == nil {
				t.Fatal("corrupt discovery succeeded")
			}
			db.failIterator = 0
			if present, err := space.HasRows(db); err != nil || !present {
				t.Fatalf("discovery error deleted rows: %v %v", present, err)
			}
		})
	}
}

func TestCommitmentBranchGenerationCleanupPebbleReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := NewPebbleDB(dir, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range []uint64{1, 255, math.MaxUint64} {
		space, _ := NewCommitmentBranchDeltaKeyspace(generation)
		for i := 0; i < 20; i++ {
			if err := space.Write(db, []byte{byte(i)}, []byte{byte(i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := WriteCommitmentBranch(db, nil, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if err := DeleteCommitmentBranchDeltaGenerationsExcept(db, 255); err != nil {
		t.Fatal(err)
	}
	if err := db.(interface{ SyncKeyValue() error }).SyncKeyValue(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = NewPebbleDB(dir, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, generation := range []uint64{1, 255, math.MaxUint64} {
		space, _ := NewCommitmentBranchDeltaKeyspace(generation)
		present, err := space.HasRows(db)
		if err != nil || present != (generation == 255) {
			t.Fatalf("generation %d present=%v err=%v", generation, present, err)
		}
		if generation == 255 {
			for i := 0; i < 20; i++ {
				got, ok, err := space.ReadNoCopy(db, []byte{byte(i)})
				if err != nil || !ok || len(got) != 1 || got[0] != byte(i+1) {
					t.Fatalf("retained row %d=%x ok=%v err=%v", i, got, ok, err)
				}
			}
		}
	}
	if _, ok, err := ReadCommitmentBranch(db, nil); err != nil || !ok {
		t.Fatal("legacy changed", ok, err)
	}
}
