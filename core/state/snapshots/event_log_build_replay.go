package snapshots

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const eventLogReplayMemory = 8 << 20

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
func (s eventLogSnapshotStore) Close() error {
	return errors.New("snapshots: event-log snapshot facade does not own live store")
}

func buildEventLogV4ChainReplay(chain *rawdb.ChainDB, dir, relPath string, from, to uint64) (ref SegmentRef, err error) {
	snapshot, err := chain.NewKeyValueSnapshot()
	if errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
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
	return BuildEventLogV4SegmentFromReader(replay, dir, relPath, from, to)
}

// A build-local cache of owned verified rows. Exceeding the charged budget
// releases the entire cache and preserves the original two source passes over
// the same pinned chain view, including identity-cache authentication. Generic
// FromReader deliberately keeps its original two-pass contract.
type eventLogBuildReplay struct {
	rawdb.EventLogReader
	limit    uint64
	rows     []EventLog
	charge   uint64
	passes   int
	overflow bool
}

func (r *eventLogBuildReplay) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.passes++
	if r.passes == 1 {
		return r.EventLogReader.IterateEventLogs(from, to, filter, func(row EventLog) (bool, error) {
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
			}
			return true, nil
		})
	}
	if r.passes != 2 {
		return errors.New("snapshots: event-log replay used outside two-pass build")
	}
	if r.overflow {
		return r.EventLogReader.IterateEventLogs(from, to, filter, fn)
	}
	for _, row := range r.rows {
		if cont, err := fn(row); err != nil || !cont {
			return err
		}
	}
	return nil
}
