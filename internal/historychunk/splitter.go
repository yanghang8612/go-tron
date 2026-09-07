// Package historychunk supplies a deterministic, allocation-free boundary
// detector shared by history containers. It does not encode or retain values.
package historychunk

const (
	MinSize = 8 << 10
	// TargetSize is the inverse boundary-mask probability after MinSize, not
	// a promise that every chunk or the measured mean has this size.
	TargetSize = 32 << 10
	MaxSize    = 128 << 10
)

// A fixed SplitMix64 seed expands to the Gear table. The table and constants
// are part of the boundary algorithm; changing them changes encoded output.
// Readers never need this algorithm, because containers record actual ranges.
var gear = func() [256]uint64 {
	var table [256]uint64
	x := uint64(0x686973746f727933)
	for i := range table {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}()

// Splitter is a zero-value-ready streaming Gear boundary detector. It retains
// only counters, never input bytes. A caller must keep the unfinished chunk.
// Splitters are independent and must not be shared concurrently.
type Splitter struct {
	size int
	hash uint64
}

// Next consumes the shortest prefix ending at a boundary, or all of p if no
// boundary occurs. On cut, internal state resets for the next chunk. For
// nonempty p the returned count is positive. A final unfinished chunk belongs
// to the caller and may be shorter than MinSize. Input partitioning does not
// affect boundaries; the caller passes each byte exactly once.
func (s *Splitter) Next(p []byte) (consumed int, cut bool) {
	h, size := s.hash, s.size
	for i, b := range p {
		h = (h << 1) + gear[b]
		size++
		if size >= MaxSize || size >= MinSize && h&(TargetSize-1) == 0 {
			s.Reset()
			return i + 1, true
		}
	}
	s.hash, s.size = h, size
	return len(p), false
}

// Pending returns the number of consumed bytes after the last boundary.
func (s *Splitter) Pending() int { return s.size }

// Reset starts a new independent stream, discarding any pending boundary state.
func (s *Splitter) Reset() { s.size, s.hash = 0, 0 }
