package blockbuffer

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// TestGetNoCopy_MatchesGet is the correctness gate for the commitment-fold
// read-path optimization: GetNoCopy must return byte-identical content to Get
// (so the decoded branch — and thus the state root — is unchanged), the only
// difference being that GetNoCopy aliases internal storage instead of copying.
// Tombstones and the layered stack must behave identically.
func TestGetNoCopy_MatchesGet(t *testing.T) {
	b := New(nil)
	b.BeginBlock(common.Hash{}, 1)

	key := []byte("state-commitment-branch-prefix")
	val := bytes.Repeat([]byte{0xab}, 1500) // ~1.5 KB, a realistic branch row
	if err := b.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := b.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	ncp, err := b.GetNoCopy(key)
	if err != nil {
		t.Fatalf("GetNoCopy: %v", err)
	}
	if !bytes.Equal(got, val) || !bytes.Equal(ncp, val) {
		t.Fatalf("content mismatch: Get=%x... GetNoCopy=%x...", got[:8], ncp[:8])
	}
	// Get must defensively copy; GetNoCopy must alias buffer storage.
	if len(got) > 0 && len(ncp) > 0 && &got[0] == &ncp[0] {
		t.Fatal("Get returned an alias, expected a copy")
	}

	// Sealed-layer read (after CommitBlock the write lives in b.layers).
	b.CommitBlock()
	if ncp, err = b.GetNoCopy(key); err != nil || !bytes.Equal(ncp, val) {
		t.Fatalf("GetNoCopy from sealed layer: %v / %x", err, ncp)
	}

	// Tombstone short-circuits identically to Get.
	b.BeginBlock(common.Hash{}, 2)
	if err := b.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := b.GetNoCopy(key); err != ErrNotFound {
		t.Fatalf("GetNoCopy after tombstone = %v, want ErrNotFound", err)
	}
}

func benchBuffer() (*Buffer, []byte) {
	b := New(nil)
	b.BeginBlock(common.Hash{}, 1)
	key := []byte("state-commitment-branch-prefix-hot")
	_ = b.Put(key, bytes.Repeat([]byte{0xcd}, 1500))
	return b, key
}

// BenchmarkBufferGet / BenchmarkBufferGetNoCopy isolate the per-read copy: Get
// allocates the ~1.5 KB value every call (the fold's dominant read-side alloc),
// GetNoCopy is allocation-free. This is the win the commitment branch reads get.
func BenchmarkBufferGet(b *testing.B) {
	buf, key := benchBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := buf.Get(key)
		_ = v
	}
}

func BenchmarkBufferGetNoCopy(b *testing.B) {
	buf, key := benchBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := buf.GetNoCopy(key)
		_ = v
	}
}
