package discover

import (
	"crypto/rand"
	"testing"
)

func randID() []byte {
	id := make([]byte, 64)
	rand.Read(id) //nolint:errcheck
	return id
}

func TestTableAddAndClosest(t *testing.T) {
	localID := randID()
	table := NewTable(localID)

	// Add 20 nodes
	for i := 0; i < 20; i++ {
		table.Add(&Node{ID: randID(), Port: 18888})
	}

	// Closest to local should return up to 16
	closest := table.Closest(localID, 16)
	if len(closest) == 0 {
		t.Fatal("expected some nodes in routing table")
	}
	if len(closest) > 16 {
		t.Fatalf("expected at most 16 nodes, got %d", len(closest))
	}
}

func TestTableEviction(t *testing.T) {
	localID := make([]byte, 64) // all-zero local ID
	table := NewTable(localID)

	// Fill one bucket: all IDs differ only in the last byte — they land in the same bucket
	baseID := make([]byte, 64)
	baseID[0] = 0xFF // far from local (max distance bucket)
	for i := 0; i < 20; i++ {
		id := make([]byte, 64)
		copy(id, baseID)
		id[63] = byte(i)
		table.Add(&Node{ID: id, Port: 18888})
	}

	// Bucket must cap at BucketSize (16)
	closest := table.Closest(baseID, 20)
	if len(closest) > BucketSize {
		t.Fatalf("bucket overflow: %d > %d", len(closest), BucketSize)
	}
}

func TestTableIgnoresLocalID(t *testing.T) {
	localID := randID()
	table := NewTable(localID)
	// Adding local ID should be silently ignored
	table.Add(&Node{ID: localID, Port: 18888})
	if table.Len() != 0 {
		t.Fatalf("expected 0 nodes, got %d", table.Len())
	}
}

func TestTableDeduplication(t *testing.T) {
	localID := randID()
	table := NewTable(localID)
	id := randID()

	// Add same node twice
	table.Add(&Node{ID: id, Port: 18888})
	table.Add(&Node{ID: id, Port: 18888})
	if table.Len() != 1 {
		t.Fatalf("expected 1 node after dedup, got %d", table.Len())
	}
}

func TestTableLen(t *testing.T) {
	localID := randID()
	table := NewTable(localID)
	for i := 0; i < 10; i++ {
		table.Add(&Node{ID: randID(), Port: 18888})
	}
	if table.Len() != 10 {
		t.Fatalf("expected 10 nodes, got %d", table.Len())
	}
}
