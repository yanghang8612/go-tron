package snapshots

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// EventLogSourceBlockHash is a rewrite capability, separate from row iteration.
// A log-free terminal block has no row from which its hash could be inferred.
// Publishers therefore require an authenticated source cut at the exact end.
type eventLogSourceAnchorReader interface {
	EventLogSourceBlockHash(fromBlock, toBlock uint64) (common.Hash, bool, error)
}

func requireEventLogReaderAnchor(reader rawdb.EventLogReader, from, to uint64) (common.Hash, error) {
	source, ok := reader.(eventLogSourceAnchorReader)
	if !ok {
		return common.Hash{}, errors.New("snapshots: event-log rewrite reader lacks authenticated terminal source identity")
	}
	hash, present, err := source.EventLogSourceBlockHash(from, to)
	if err != nil {
		return common.Hash{}, err
	}
	if !present || hash == (common.Hash{}) {
		return common.Hash{}, fmt.Errorf("snapshots: event-log rewrite range [%d,%d] lacks an authenticated exact-end source anchor", from, to)
	}
	return hash, nil
}

func pinEventLogRewriteReader(reader rawdb.EventLogReader) (rawdb.EventLogReader, func(), error) {
	if manager, ok := reader.(*Manager); ok {
		pinned, release, err := manager.PinHistoryReadView()
		if err != nil {
			return nil, nil, err
		}
		if pinned == nil {
			release()
			return nil, nil, errors.New("snapshots: event rewrite source manifest is missing")
		}
		return pinned, release, nil
	}
	return reader, func() {}, nil
}

// EventLogSourceBlockHash authenticates complete source-file coverage and
// inherits only an exact terminal ref's anchor. A partial end cannot use the
// final log's block hash, even if that row happens to be present.
func (m *Manager) EventLogSourceBlockHash(from, to uint64) (common.Hash, bool, error) {
	if m == nil || to < from {
		return common.Hash{}, false, nil
	}
	if !m.pinned {
		reader, release, err := pinEventLogRewriteReader(m)
		if err != nil {
			return common.Hash{}, false, err
		}
		defer release()
		return reader.(*Manager).EventLogSourceBlockHash(from, to)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return common.Hash{}, false, err
	}
	covered, err := m.EventLogRangeCovered(from, to)
	if err != nil || !covered {
		return common.Hash{}, false, err
	}
	var terminal common.Hash
	verified, err := visitEventSourceRefs(manifest, from, to, func(ref SegmentRef) (bool, error) {
		if ref.SourceBlockHash == "" {
			return false, nil
		}
		hash, err := parseEventSourceBlockHash(ref.SourceBlockHash)
		if err != nil {
			return false, err
		}
		if ref.ToTxNum == to {
			terminal = hash
		}
		return true, nil
	})
	return terminal, verified && terminal != (common.Hash{}), err
}

func buildAnchoredEventLogV4FromReader(reader rawdb.EventLogReader, dir, path string, from, to uint64) (SegmentRef, error) {
	anchor, err := requireEventLogReaderAnchor(reader, from, to)
	if err != nil {
		return SegmentRef{}, err
	}
	ref, err := BuildEventLogV4SegmentFromReader(reader, dir, path, from, to)
	if err != nil {
		return SegmentRef{}, err
	}
	current, err := requireEventLogReaderAnchor(reader, from, to)
	if err != nil {
		return SegmentRef{}, err
	}
	if current != anchor {
		return SegmentRef{}, fmt.Errorf("snapshots: event-log rewrite terminal source changed at block %d", to)
	}
	ref.SourceBlockHash = anchor.Hex()
	return ref, nil
}

func buildAnchoredEventLogFromChain(chain *rawdb.ChainDB, to uint64, build func() (SegmentRef, error)) (SegmentRef, error) {
	if chain == nil {
		return SegmentRef{}, errors.New("snapshots: nil event source chain")
	}
	anchor, err := requireSnapshotStageBoundaryHash(chain, rawdb.StageSnapshotEventLogBuild, to)
	if err != nil {
		return SegmentRef{}, err
	}
	ref, err := build()
	if err != nil {
		return SegmentRef{}, err
	}
	current, err := requireSnapshotStageBoundaryHash(chain, rawdb.StageSnapshotEventLogBuild, to)
	if err != nil {
		return SegmentRef{}, err
	}
	if current != anchor {
		return SegmentRef{}, fmt.Errorf("snapshots: event source terminal changed at block %d", to)
	}
	ref.SourceBlockHash = anchor.Hex()
	return ref, nil
}
