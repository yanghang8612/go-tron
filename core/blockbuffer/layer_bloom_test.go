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

func TestCommittedLayerBloomSegmentPreservesLateBatch(t *testing.T) {
	b := New(nil)
	var late *bufferBatch
	for number := uint64(1); number <= layerBloomSegmentSize; number++ {
		b.BeginBlock(common.Hash{byte(number)}, number)
		if number == 1 {
			late = b.NewBatch().(*bufferBatch)
			if err := late.Put([]byte("late-segment-key"), []byte("late")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Put([]byte(fmt.Sprintf("layer-%d", number)), []byte("value")); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}
	view := b.loadReadView()
	segment := view.layers[len(view.layers)-1].segment.Load()
	if segment == nil || segment.first != view.layers[0] || segment.size != layerBloomSegmentSize {
		t.Fatal("complete committed-layer segment was not built")
	}
	if err := late.Write(); err != nil {
		t.Fatal(err)
	}
	if !segment.bloom.mayContainHash(layerBloomHashString("late-segment-key")) {
		t.Fatal("late batch key absent from committed-layer segment")
	}
	if got, err := b.GetNoCopy([]byte("late-segment-key")); err != nil || string(got) != "late" {
		t.Fatalf("late segment value = %q err=%v", got, err)
	}
}

func TestCommittedLayerBloomSegmentRejectsBrokenGroup(t *testing.T) {
	b := New(nil)
	var beforeHash common.Hash
	for number := uint64(1); number <= 2*layerBloomSegmentSize; number++ {
		hash := common.Hash{byte(number)}
		b.BeginBlock(hash, number)
		if number == layerBloomSegmentSize {
			beforeHash = hash
			if err := b.Put([]byte("before-broken-segment"), []byte("kept")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Put([]byte(fmt.Sprintf("layer-%d", number)), []byte("value")); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}
	// Remove an interior member of the second segment. Its last-layer marker
	// remains, but the first pointer no longer sits exactly segment.size entries
	// behind it, so lookup must not skip the preceding layer.
	b.DiscardBlock(common.Hash{byte(layerBloomSegmentSize + layerBloomSegmentSize/2)})
	if got, err := b.GetNoCopy([]byte("before-broken-segment")); err != nil || string(got) != "kept" {
		t.Fatalf("value before broken segment = %q err=%v", got, err)
	}
	if got := b.PendingBlocks(); len(got) != 2*layerBloomSegmentSize-1 || got[layerBloomSegmentSize-1] != beforeHash {
		t.Fatalf("unexpected committed topology after discard: len=%d", len(got))
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
		name        string
		withBloom   bool
		withSegment bool
	}{
		{name: "without-bloom", withBloom: false},
		{name: "with-bloom", withBloom: true},
		{name: "with-segment", withBloom: true, withSegment: true},
	} {
		stack := makeLayers(test.withBloom)
		if test.withSegment {
			for start := 0; start+layerBloomSegmentSize <= len(stack); start += layerBloomSegmentSize {
				buildLayerBloomSegment(stack[start : start+layerBloomSegmentSize])
			}
		}
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

func BenchmarkBuildLayerBloomSegment(b *testing.B) {
	const keysPerLayer = 128
	layers := make([]*layer, layerBloomSegmentSize)
	for layerIndex := range layers {
		l := newLayer(common.Hash{}, uint64(layerIndex+1))
		for keyIndex := 0; keyIndex < keysPerLayer; keyIndex++ {
			key := fmt.Sprintf("layer-%04d-key-%04d", layerIndex, keyIndex)
			s := l.shardForString(key)
			if s.writes == nil {
				s.writes = make(map[string][]byte)
			}
			s.writes[key] = []byte("value")
		}
		l.buildBloom()
		layers[layerIndex] = l
	}
	b.ReportMetric(layerBloomSegmentSize*keysPerLayer, "keys/op")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buildLayerBloomSegment(layers)
	}
}
