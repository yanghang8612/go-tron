package blockbuffer

import (
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestLayerBloomHashByteStringParity(t *testing.T) {
	for _, key := range []string{"", "a", "state-commitment-branch-v1-0123456789abcdef"} {
		if got, want := layerBloomHashBytes([]byte(key)), layerBloomHashString(key); got != want {
			t.Fatalf("hash parity for %q: bytes=%x string=%x", key, got, want)
		}
	}
}

func TestCommittedLayerBloomPreservesWritesDeletesAndLateBatch(t *testing.T) {
	b := New(nil)
	b.BeginBlock(common.Hash{0x01}, 1)
	if err := b.Put([]byte("present"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	late := b.NewBatch()
	if err := late.Put([]byte("late"), []byte("batch")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	view := b.loadReadView()
	if len(view.layers) != 1 || view.layers[0].bloom.Load() == nil {
		t.Fatalf("committed layer bloom missing: layers=%d", len(view.layers))
	}
	if got, err := b.GetNoCopy([]byte("present")); err != nil || string(got) != "value" {
		t.Fatalf("committed write = %q err=%v", got, err)
	}
	if ok, err := b.Has([]byte("deleted")); err != nil || ok {
		t.Fatalf("committed tombstone: ok=%v err=%v", ok, err)
	}

	// A batch may publish into its captured layer after promotion. Its key must
	// be added before the map mutation so the already-published filter cannot
	// hide the new value.
	if err := late.Write(); err != nil {
		t.Fatal(err)
	}
	if got, err := b.GetNoCopy([]byte("late")); err != nil || string(got) != "batch" {
		t.Fatalf("late committed-layer write = %q err=%v", got, err)
	}
	if bloom := view.layers[0].bloom.Load(); !bloom.mayContainHash(layerBloomHashString("late")) {
		t.Fatal("late committed-layer key absent from bloom")
	}
}

func BenchmarkCommittedLayerStackLookup(b *testing.B) {
	const layers = 2048
	makeLayers := func(withBloom bool) []*layer {
		out := make([]*layer, layers)
		for i := range out {
			l := newLayer(common.Hash{}, uint64(i+1))
			key := fmt.Sprintf("layer-key-%04d", i)
			s := l.shardForString(key)
			s.writes = map[string][]byte{key: []byte("value")}
			if withBloom {
				l.buildBloom()
			}
			out[i] = l
		}
		return out
	}
	key := []byte("layer-key-0000") // oldest layer: worst-case full stack walk
	keyHash := layerBloomHashBytes(key)
	for _, test := range []struct {
		name      string
		withBloom bool
	}{
		{name: "without-bloom", withBloom: false},
		{name: "with-bloom", withBloom: true},
	} {
		stack := makeLayers(test.withBloom)
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				value, found, tomb := lookupLayersNewest(stack, key, keyHash)
				if !found || tomb || string(value) != "value" {
					b.Fatal("lookup failed")
				}
			}
		})
	}
}
