package snapshots

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const eventLogReplayMemory = 8 << 20

var eventReplayMetrics = newEventReplayMetrics(defaultColdSnapshotMetrics + "history/event_replay/")

type eventReplayMetricSet struct {
	attempts, replayed, overflows, sourcePasses, unavailable *metrics.Counter
	lastPeakBytes, lastRows, lastSourcePasses                *metrics.Gauge
}

func newEventReplayMetrics(namespace string) eventReplayMetricSet {
	return eventReplayMetricSet{
		attempts:         metrics.GetOrRegisterCounter(namespace+"attempts", nil),
		replayed:         metrics.GetOrRegisterCounter(namespace+"replayed", nil),
		overflows:        metrics.GetOrRegisterCounter(namespace+"overflows", nil),
		sourcePasses:     metrics.GetOrRegisterCounter(namespace+"source_passes", nil),
		unavailable:      metrics.GetOrRegisterCounter(namespace+"snapshot_unavailable", nil),
		lastPeakBytes:    metrics.GetOrRegisterGauge(namespace+"last/peak_charge_bytes", nil),
		lastRows:         metrics.GetOrRegisterGauge(namespace+"last/source_rows", nil),
		lastSourcePasses: metrics.GetOrRegisterGauge(namespace+"last/source_passes", nil),
	}
}

// One observation per chain build, including failed attempts. No registry or
// atomic metric operation runs per row. Peaks are conservative replay charges,
// not process RSS; source_rows counts first-pass input, not retained rows.
func (m eventReplayMetricSet) record(r *eventLogBuildReplay) {
	m.attempts.Inc(1)
	if r.replayed {
		m.replayed.Inc(1)
	}
	if r.overflow {
		m.overflows.Inc(1)
	}
	m.sourcePasses.Inc(int64(r.sourcePasses))
	m.lastPeakBytes.Update(coldSnapshotUintGauge(r.peakCharge))
	m.lastRows.Update(coldSnapshotUintGauge(r.sourceRows))
	m.lastSourcePasses.Update(int64(r.sourcePasses))
}

// Only point and iterator reads reach the pinned snapshot. Keep the original
// store solely to preserve optional ChainDB capabilities; never close it here.
type eventLogSnapshotStore struct {
	ethdb.KeyValueStore
	snapshot pointread.KeyValueSnapshot
}

func (s eventLogSnapshotStore) Get(key []byte) ([]byte, error) { return s.snapshot.Get(key) }
func (s eventLogSnapshotStore) Has(key []byte) (bool, error)   { return s.snapshot.Has(key) }
func (s eventLogSnapshotStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	return s.snapshot.NewIterator(prefix, start)
}
func (s eventLogSnapshotStore) IsPinnedKeyValueView() bool { return true }
func (s eventLogSnapshotStore) Close() error {
	return errors.New("snapshots: event-log snapshot facade does not own live store")
}

func buildEventLogV4ChainReplay(chain *rawdb.ChainDB, dir, relPath string, from, to uint64) (ref SegmentRef, err error) {
	snapshot, err := chain.NewKeyValueSnapshot()
	if errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
		eventReplayMetrics.unavailable.Inc(1)
		return BuildEventLogV4SegmentFromReader(eventLogV3ChainReader{chain: chain, identities: newEventLogChainIdentityCache()}, dir, relPath, from, to)
	}
	if err != nil {
		return ref, err
	}
	defer func() {
		if closeErr := snapshot.Close(); closeErr != nil {
			ref = SegmentRef{}
			err = errors.Join(err, closeErr)
		}
	}()
	// Preserve private cold sidecar routing, including external receipt logs.
	view := *chain
	view.KeyValueStore = eventLogSnapshotStore{KeyValueStore: chain.KeyValueStore, snapshot: snapshot}
	replay := &eventLogBuildReplay{EventLogReader: eventLogV3ChainReader{chain: &view, identities: newEventLogChainIdentityCache()}, limit: eventLogReplayMemory}
	defer eventReplayMetrics.record(replay)
	return BuildEventLogV4SegmentFromReader(replay, dir, relPath, from, to)
}

// A build-local cache of owned verified rows. Exceeding the charged budget
// releases the entire cache and preserves the original two source passes over
// the same pinned chain view, including identity-cache authentication. Generic
// FromReader deliberately keeps its original two-pass contract.
type eventLogBuildReplay struct {
	rawdb.EventLogReader
	limit                  uint64
	rows                   []EventLog
	charge                 uint64
	passes                 int
	overflow               bool
	peakCharge, sourceRows uint64
	sourcePasses           int
	replayed               bool
}

func (r *eventLogBuildReplay) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.passes++
	if r.passes == 1 {
		r.sourcePasses++
		return r.EventLogReader.IterateEventLogs(from, to, filter, func(row EventLog) (bool, error) {
			r.sourceRows++
			cont, err := fn(row)
			if err != nil || !cont {
				return cont, err
			}
			if r.overflow {
				return true, nil
			}
			// Empty rows still consume fixed space, bounding cardinality. The charge
			// includes owned row/protobuf/backing arrays plus allocation allowance.
			charge := uint64(proto.Size(row.Log)) + 512 + uint64(len(row.Log.Topics))*48
			if charge > r.limit || r.charge > r.limit-charge {
				r.rows = nil
				r.charge = 0
				r.overflow = true
			} else {
				row.Log = proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
				r.rows = append(r.rows, row)
				r.charge += charge
				r.peakCharge = max(r.peakCharge, r.charge)
			}
			return true, nil
		})
	}
	if r.passes != 2 {
		return errors.New("snapshots: event-log replay used outside two-pass build")
	}
	if r.overflow {
		r.sourcePasses++
		return r.EventLogReader.IterateEventLogs(from, to, filter, fn)
	}
	r.replayed = true
	for _, row := range r.rows {
		if cont, err := fn(row); err != nil || !cont {
			return err
		}
	}
	return nil
}
