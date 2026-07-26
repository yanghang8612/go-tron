package vm

import tcommon "github.com/tronprotocol/go-tron/common"

const (
	// A mainnet transaction ordinarily emits only a handful of logs. Reusing
	// these modest backing arrays removes the first append allocation without
	// letting an exceptional contract pin an unbounded slice in the pool.
	maxPooledExecutionLogs = 64
	executionLogPoolDepth  = 64
)

var executionLogPool = make(chan []Log, executionLogPoolDepth)

// Log represents a contract log event emitted by LOG0-LOG4.
type Log struct {
	Address tcommon.Address
	Topics  [][]byte
	Data    []byte

	// Opcode-created topics share one immutable payload allocation with Data.
	// Keeping the compact slice header directly in Log avoids allocating a
	// separate wrapper object for every LOG1-LOG4. Caller-created logs continue
	// to use Topics.
	compactTopics []byte
}

// TopicCount returns the number of event topics. Logs constructed by callers
// may use Topics directly; opcode-created logs use the compact representation.
func (l *Log) TopicCount() int {
	if l.compactTopics != nil {
		return len(l.compactTopics) / 32
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
	return l.compactTopics[start : start+32 : start+32]
}

// appendExecutionLog lazily borrows the receipt-log slice backing for a TVM
// execution. The log payload bytes themselves are never pooled: TransactionInfo
// borrows them after execution and may outlive both TVM and actuator.Result.
func (tvm *TVM) appendExecutionLog(log Log) {
	if tvm.Logs == nil {
		select {
		case tvm.Logs = <-executionLogPool:
		default:
		}
	}
	tvm.Logs = append(tvm.Logs, log)
}

// ReleaseExecutionLogs returns only the []Log backing array after callers have
// copied each log's slice headers into their durable receipt representation.
// Every slot (including reverted entries beyond len) is cleared before reuse;
// payload allocations remain owned by the copied Data/topic slices.
func ReleaseExecutionLogs(logs []Log) {
	if cap(logs) == 0 {
		return
	}
	storage := logs[:cap(logs)]
	clear(storage)
	if cap(storage) > maxPooledExecutionLogs {
		return
	}
	select {
	case executionLogPool <- storage[:0]:
	default:
	}
}
