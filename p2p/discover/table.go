package discover

import "sync"

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

// sortByDist sorts nodes by XOR log-distance to target (ascending) using insertion sort.
func sortByDist(nodes []*Node, target []byte) {
	for i := 1; i < len(nodes); i++ {
		for j := i; j > 0; j-- {
			di := LogDist(target, nodes[j].ID)
			dj := LogDist(target, nodes[j-1].ID)
			if di < dj {
				nodes[j], nodes[j-1] = nodes[j-1], nodes[j]
			} else {
				break
			}
		}
	}
}
