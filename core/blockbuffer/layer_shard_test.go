package blockbuffer

import (
	"math/rand"
	"testing"
)

func TestLayerShardIndexBytesAndStringAgree(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for n := 0; n <= 256; n++ {
		key := make([]byte, n)
		if _, err := rng.Read(key); err != nil {
			t.Fatal(err)
		}
		gotBytes := layerShardIndexBytes(key)
		gotString := layerShardIndexString(string(key))
		if gotBytes != gotString {
			t.Fatalf("len=%d: byte shard %d != string shard %d", n, gotBytes, gotString)
		}
		if gotBytes >= layerShardCount {
			t.Fatalf("len=%d: shard %d outside [0,%d)", n, gotBytes, layerShardCount)
		}
	}
}

func TestLayerShardCommitmentRootNibblesDoNotCollide(t *testing.T) {
	prefix := []byte("state-commitment-branch-v1-")
	seen := make(map[uint32]byte, 16)
	for nibble := byte(0); nibble < 16; nibble++ {
		key := append(append([]byte(nil), prefix...), nibble)
		shard := layerShardIndexBytes(key)
		if previous, ok := seen[shard]; ok {
			t.Fatalf("root nibbles %x and %x collide on shard %d", previous, nibble, shard)
		}
		seen[shard] = nibble
	}
}

func TestLayerShardedLookupPreservesPutDeleteSemantics(t *testing.T) {
	l := newLayer([32]byte{}, 1)
	b := &Buffer{}
	key := []byte("state-commitment-branch-v1-\x01\x02\x03")

	b.putInto(l, key, []byte("v1"))
	if got, found, tomb := l.lookup(key); !found || tomb || string(got) != "v1" {
		t.Fatalf("after put: got=(%q,%v,%v), want (v1,true,false)", got, found, tomb)
	}
	b.deleteInto(l, key)
	if got, found, tomb := l.lookup(key); found || !tomb || got != nil {
		t.Fatalf("after delete: got=(%q,%v,%v), want (nil,false,true)", got, found, tomb)
	}
	b.putInto(l, key, []byte("v2"))
	if got, found, tomb := l.lookup(key); !found || tomb || string(got) != "v2" {
		t.Fatalf("after replacement: got=(%q,%v,%v), want (v2,true,false)", got, found, tomb)
	}
}
