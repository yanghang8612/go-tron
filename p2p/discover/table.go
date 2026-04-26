package discover

import (
	"net"
	"sync"
)

const (
	BucketSize = 16  // k in Kademlia
	NumBuckets = 512 // one per bit of 64-byte key space
)

// bucket holds up to BucketSize nodes at a given log-distance from local.
type bucket struct {
	entries []*Node
}

func (b *bucket) add(n *Node) {
	// Deduplicate by ID — move recently-seen node to front
	for i, e := range b.entries {
		if string(e.ID) == string(n.ID) {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.entries = append([]*Node{n}, b.entries...)
			return
		}
	}
	if len(b.entries) < BucketSize {
		b.entries = append([]*Node{n}, b.entries...)
	} else {
		// Evict oldest (last entry), prepend new node to front
		b.entries = append([]*Node{n}, b.entries[:len(b.entries)-1]...)
	}
}

// Table is a Kademlia routing table.
type Table struct {
	localID []byte
	buckets [NumBuckets]*bucket
	mu      sync.Mutex
}

// NewTable creates an empty routing table for the given local node ID.
func NewTable(localID []byte) *Table {
	t := &Table{localID: localID}
	for i := range t.buckets {
		t.buckets[i] = &bucket{}
	}
	return t
}

// Add inserts a node into the appropriate bucket.
// Nodes with the same ID as localID are ignored.
func (t *Table) Add(n *Node) {
	if string(n.ID) == string(t.localID) {
		return
	}
	d := LogDist(t.localID, n.ID)
	if d >= NumBuckets {
		d = NumBuckets - 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buckets[d].add(n)
}

// Closest returns the n nodes closest to target by XOR log-distance.
func (t *Table) Closest(target []byte, n int) []*Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	var result []*Node
	for _, b := range t.buckets {
		result = append(result, b.entries...)
	}
	sortByDist(result, target)
	if len(result) > n {
		result = result[:n]
	}
	return result
}

// RemoveByAddr removes all table entries matching the given IP and port.
// Returns the number of entries removed.
func (t *Table) RemoveByAddr(ip net.IP, port int) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	removed := 0
	for _, b := range t.buckets {
		j := 0
		for _, n := range b.entries {
			if n.IP.Equal(ip) && n.Port == port {
				removed++
				continue
			}
			b.entries[j] = n
			j++
		}
		b.entries = b.entries[:j]
	}
	return removed
}

// Len returns the total number of nodes in the table.
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, b := range t.buckets {
		total += len(b.entries)
	}
	return total
}

// xorCmp returns -1 if XOR(target, a) < XOR(target, b), 0 if equal, 1 if greater,
// comparing the raw byte-by-byte XOR distance in the standard big-endian sense.
func xorCmp(target, a, b []byte) int {
	n := len(target)
	if len(a) < n {
		n = len(a)
	}
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		xa := target[i] ^ a[i]
		xb := target[i] ^ b[i]
		if xa < xb {
			return -1
		}
		if xa > xb {
			return 1
		}
	}
	return 0
}

// sortByDist sorts nodes by raw XOR distance to target (ascending) using insertion sort.
// Raw XOR is strictly finer than LogDist: two nodes with the same log-distance can still
// differ by up to 2^N in actual distance, so LogDist-based ordering yields suboptimal Closest() results.
func sortByDist(nodes []*Node, target []byte) {
	for i := 1; i < len(nodes); i++ {
		for j := i; j > 0; j-- {
			if xorCmp(target, nodes[j].ID, nodes[j-1].ID) < 0 {
				nodes[j], nodes[j-1] = nodes[j-1], nodes[j]
			} else {
				break
			}
		}
	}
}
