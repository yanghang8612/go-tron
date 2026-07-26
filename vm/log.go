package vm

import tcommon "github.com/tronprotocol/go-tron/common"

// compactLogTopics stores the contiguous topic bytes emitted by LOG1-LOG4.
// Keeping one slice header here replaces the [][]byte backing array whose size
// otherwise grows by 24 bytes per topic. Log itself only gains a pointer and
// remains in the same allocator size class.
type compactLogTopics struct {
	bytes []byte
}

// Log represents a contract log event emitted by LOG0-LOG4.
type Log struct {
	Address tcommon.Address
	Topics  [][]byte
	Data    []byte

	compactTopics *compactLogTopics
}

// TopicCount returns the number of event topics. Logs constructed by callers
// may use Topics directly; opcode-created logs use the compact representation.
func (l *Log) TopicCount() int {
	if l.compactTopics != nil {
		return len(l.compactTopics.bytes) / 32
	}
	return len(l.Topics)
}

// Topic returns topic i. Opcode-created topics are immutable,
// capacity-limited 32-byte views into their shared byte arena.
func (l *Log) Topic(i int) []byte {
	if l.compactTopics == nil {
		return l.Topics[i]
	}
	start := i * 32
	return l.compactTopics.bytes[start : start+32 : start+32]
}
