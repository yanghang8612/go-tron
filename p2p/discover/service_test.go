package discover

import (
	"bytes"
	"testing"
)

// TestNextLookupTargetRotation verifies that every kademliaMaxLoop-th cycle uses
// the local node ID as the lookup target (self-health) and all other cycles use
// a random target. Over 20 iterations exactly 4 self-hits are expected
// (cycles 5, 10, 15, 20).
func TestNextLookupTargetRotation(t *testing.T) {
	svc := &Service{table: &Table{localID: bytes.Repeat([]byte{0xAA}, 64)}}
	selfHits := 0
	for i := 0; i < 20; i++ {
		target := svc.nextLookupTarget()
		if bytes.Equal(target, svc.table.localID) {
			selfHits++
		}
	}
	// Over 20 iterations, self should be hit every 5th time = 4 times.
	if selfHits != 4 {
		t.Fatalf("self-lookup count = %d, want 4", selfHits)
	}
}
