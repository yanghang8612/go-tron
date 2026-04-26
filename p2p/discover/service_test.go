package discover

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// TestPingTimeoutEjectsFromTable verifies that when evictTimedOutPings runs,
// nodes whose pings have exceeded pingTimeout are removed from the routing table.
func TestPingTimeoutEjectsFromTable(t *testing.T) {
	localID := bytes.Repeat([]byte{0x00}, 64)
	table := NewTable(localID)

	ip := net.IPv4(10, 0, 0, 1)
	port := 18888
	nodeID := bytes.Repeat([]byte{0xFF}, 64)
	table.Add(&Node{ID: nodeID, IP: ip, Port: port})
	if table.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", table.Len())
	}

	svc := &Service{
		table:        table,
		pendingPings: make(map[string]*pendingPing),
	}

	// Record a pending ping that is already past the timeout
	key := "10.0.0.1:18888"
	svc.pendingPings[key] = &pendingPing{
		sentAt: time.Now().Add(-(pingTimeout + time.Second)),
		ip:     ip,
		port:   port,
	}

	svc.evictTimedOutPings()

	// Node must have been removed from the table
	if table.Len() != 0 {
		t.Fatalf("expected table empty after ping timeout eviction, got %d nodes", table.Len())
	}
	// Pending ping must have been cleared
	if len(svc.pendingPings) != 0 {
		t.Fatal("expected pendingPings empty after eviction")
	}
}

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
