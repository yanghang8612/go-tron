package core

import "github.com/ethereum/go-ethereum/metrics"

type flushCollectionEnd uint8

const (
	flushCollectionQueued flushCollectionEnd = iota
	flushCollectionTimeout
	flushCollectionClosed
)

var flushCollectionCounters = func() [3]*metrics.Counter {
	var counters [3]*metrics.Counter
	for i, name := range [...]string{"queued_snapshot", "window_timeout", "queue_closed"} {
		counters[i] = metrics.NewRegisteredCounter("chain/flush/collect/"+name, nil)
	}
	return counters
}()
var (
	flushInlineUnavailable = metrics.NewRegisteredCounter("chain/flush/inline/not_started_or_closed", nil)
	flushInlineFull        = metrics.NewRegisteredCounter("chain/flush/inline/queue_full", nil)
)
