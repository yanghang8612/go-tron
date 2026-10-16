package core

import (
	"github.com/ethereum/go-ethereum/metrics"
	"testing"
	"time"
)

func TestFlushCollectionObservation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		reason          flushCollectionEnd
		timeout, closed bool
	}{
		{"queued", flushCollectionQueued, false, false}, {"timeout", flushCollectionTimeout, true, false}, {"closed", flushCollectionClosed, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue := make(chan uint64, 2)
			queue <- 11
			var wait <-chan time.Time
			if tc.closed {
				close(queue)
				wait = make(chan time.Time)
			} else if tc.timeout {
				clock := make(chan time.Time)
				close(clock)
				wait = clock
			}
			before := [3]int64{}
			for i, c := range flushCollectionCounters {
				before[i] = c.Snapshot().Count()
			}
			cutoff, pending, closed := collectFlushCutoffs(queue, 10, wait)
			if cutoff != 11 || pending != 2 || closed != tc.closed {
				t.Fatalf("result%d/%d/%v", cutoff, pending, closed)
			}
			for i, c := range flushCollectionCounters {
				want := int64(0)
				if i == int(tc.reason) {
					want = 1
				}
				if c.Snapshot().Count()-before[i] != want {
					t.Fatalf("reason%d delta%d want%d", i, c.Snapshot().Count()-before[i], want)
				}
			}
		})
	}
	for _, name := range []string{"chain/flush/collect/window_timeout", "chain/flush/collect/queue_closed", "chain/flush/inline/queue_full"} {
		if metrics.DefaultRegistry.Get(name) == nil {
			t.Fatalf("metric not registered:%s", name)
		}
	}
}
