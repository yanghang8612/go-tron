package etl

import "sync"

const (
	collectorArenaChunkSize       = 64 << 10
	collectorArenaPoolMaxCapacity = defaultBufferLimit + collectorArenaChunkSize
)

var collectorArenaPool = sync.Pool{New: func() any { return new(collectorByteArena) }}

// collectorByteArena owns stable chunks rather than one growing byte slice so
// entry key/value views never move while append expands the collector. Reset
// reuses every chunk after a successful spill, matching Erigon's compact
// sortable-buffer lifecycle without changing Collector's existing entry type.
type collectorByteArena struct {
	chunks   [][]byte
	chunk    int
	offset   int
	capacity int
}

func (a *collectorByteArena) alloc(size int) []byte {
	if size == 0 {
		return nil
	}
	for a.chunk < len(a.chunks) {
		current := a.chunks[a.chunk]
		if len(current)-a.offset >= size {
			start := a.offset
			a.offset += size
			return current[start:a.offset]
		}
		a.chunk++
		a.offset = 0
	}
	chunkSize := collectorArenaChunkSize
	if size > chunkSize {
		chunkSize = size
	}
	current := make([]byte, chunkSize)
	a.chunks = append(a.chunks, current)
	a.capacity += chunkSize
	a.chunk = len(a.chunks) - 1
	a.offset = size
	return current[:size]
}

func (a *collectorByteArena) reset() {
	a.chunk = 0
	a.offset = 0
}
