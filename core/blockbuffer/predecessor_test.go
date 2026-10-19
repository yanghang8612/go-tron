package blockbuffer

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func checkPredecessorOracle(t *testing.T, source ethdb.Iteratee) {
	t.Helper()
	for _, prefix := range [][]byte{nil, []byte("p/"), []byte("absent"), {0xff}} {
		for _, upper := range [][]byte{nil, {}, []byte("p/"), []byte("p/05"), []byte("p/15"), []byte("q")} {
			var wk, wv []byte
			want := false
			it := source.NewIterator(prefix, nil)
			for it.Next() {
				if upper != nil && bytes.Compare(it.Key(), upper) >= 0 {
					break
				}
				wk = bytes.Clone(it.Key())
				wv = bytes.Clone(it.Value())
				want = true
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
			it.Release()
			k, v, ok, err := pointread.SeekBefore(source, prefix, upper)
			if err != nil || ok != want || !bytes.Equal(k, wk) || !bytes.Equal(v, wv) {
				t.Fatalf("prefix=%x upper=%x: %x/%x/%v/%v want %x/%x/%v", prefix, upper, k, v, ok, err, wk, wv, want)
			}
			if ok {
				k[0] ^= 1
				if len(v) > 0 {
					v[0] ^= 1
				}
				again, _, _, _ := pointread.SeekBefore(source, prefix, upper)
				if !bytes.Equal(again, wk) {
					t.Fatal("borrowed predecessor key")
				}
			}
		}
	}
}

func TestPredecessorLayerTombstonesRangesAndPinnedSnapshot(t *testing.T) {
	base, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	for _, k := range []string{"p/01", "p/04", "p/07", "p/09", "p/19", "q/1"} {
		if err := base.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	b := New(base)
	b.SetMaxInflight(3)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("p/05"), []byte("first"))
	b.Delete([]byte("p/19"))
	b.CommitBlock()
	pinned, err := b.NewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	checkPredecessorOracle(t, pinned)
	b.BeginBlock(bufHash(2), 2)
	handle, _ := b.NewestInflight()
	view := b.ViewLayer(handle)
	b.DeleteRange([]byte("p/04"), []byte("p/10"))
	b.Put([]byte("p/07"), []byte("revived"))
	b.Delete([]byte("p/01"))
	checkPredecessorOracle(t, b)
	checkPredecessorOracle(t, view)
	b.BeginBlock(bufHash(3), 3)
	b.Put([]byte("p/18"), []byte("future"))
	checkPredecessorOracle(t, b)
	checkPredecessorOracle(t, view)
	k, _, _, _ := view.SeekPrefixBefore([]byte("p/"), nil)
	if string(k) != "p/07" {
		t.Fatal("layer view leaked future", string(k))
	}
	b.CommitBlock()
	b.CommitBlock()
	if err := b.FlushUpTo(3, base); err != nil {
		t.Fatal(err)
	}
	checkPredecessorOracle(t, b)
	checkPredecessorOracle(t, pinned)
}

func TestPredecessorRandomOverlayMatchesIterator(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer base.Close()
	b := New(base)
	rng := rand.New(rand.NewSource(938))
	for block := 1; block <= 20; block++ {
		b.BeginBlock(bufHash(byte(block)), uint64(block))
		for j := 0; j < 12; j++ {
			key := []byte{'p', '/', byte('0' + rng.Intn(4)), byte('0' + rng.Intn(10))}
			if rng.Intn(3) == 0 {
				b.Delete(key)
			} else {
				b.Put(key, []byte{byte(block), byte(j)})
			}
		}
		b.CommitBlock()
		checkPredecessorOracle(t, b)
	}
}
