package snapshots

import (
	"encoding/hex"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// The freezer's target is demand, not authority. An independent event builder
// may only consume completely executed canonical solid blocks. In particular,
// an absent Finish is not equivalent to an uncapped source.
func (r *Runner) independentEventSourceCap(db AggregatorDB) (uint64, error) {
	finish, ok, err := r.verifiedFinishStageBlock(db)
	if err != nil || !ok {
		return 0, err
	}
	solid := r.chain.LatestSolidifiedBlockNum()
	if solid <= 0 {
		return 0, nil
	}
	return min(uint64(solid), finish), nil
}

func (r *Runner) verifyIndependentEventSource(db AggregatorDB, span coldSidecarBlockRange, anchor common.Hash) error {
	cap, err := r.independentEventSourceCap(db)
	if err != nil {
		return err
	}
	if anchor == (common.Hash{}) || span.to > cap {
		return fmt.Errorf("snapshots: event source range [%d,%d] no longer below solid/Finish %d", span.from, span.to, cap)
	}
	current, err := r.snapshotBuildStageBoundaryHash(db, rawdb.StageSnapshotEventLogBuild, span.to)
	if err != nil {
		return err
	}
	if current != anchor {
		return fmt.Errorf("snapshots: event source block %d changed from %x to %x before publication", span.to, anchor, current)
	}
	return nil
}

// Retain the immutable files in the catalog, but derive install progress only
// from complete companion ranges whose source cut is already locally known.
// Splitting a ref would claim an unauthenticated partial-file boundary.
func eventManifestAtBlock(manifest *Manifest, block uint64) *Manifest {
	if manifest == nil {
		return nil
	}
	out := *manifest
	out.Segments = make([]SegmentRef, 0, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		if (ref.Kind == SegmentEventLog || ref.Kind == SegmentEventLogIndex) && ref.ToTxNum > block {
			continue
		}
		out.Segments = append(out.Segments, ref)
	}
	return &out
}

// SetEventLogCanonicalVerifier enables canonical authority checks in the live
// manager. Set it before exposing the manager to ChainDB. Offline managers that
// intentionally inspect files alone leave it unset. A false result means the
// complete segment cut has not been executed/solidified locally yet.
func (m *Manager) SetEventLogCanonicalVerifier(verify func(uint64, common.Hash) (bool, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventCanonicalVerifier = verify
}

func parseEventSourceBlockHash(raw string) (common.Hash, error) {
	if len(raw) != common.HashLength*2 {
		return common.Hash{}, fmt.Errorf("snapshots: malformed event source block hash %q", raw)
	}
	b, err := hex.DecodeString(raw)
	if err != nil || len(b) != common.HashLength || common.BytesToHash(b) == (common.Hash{}) {
		return common.Hash{}, fmt.Errorf("snapshots: invalid event source block hash %q", raw)
	}
	return common.BytesToHash(b), nil
}

func verifyEventSourceRefs(refs []SegmentRef, from, to uint64, verify func(uint64, common.Hash) (bool, error), requireAnchors bool) (bool, error) {
	for _, ref := range refs {
		if ref.ToTxNum < from || ref.FromTxNum > to {
			continue
		}
		if ref.SourceBlockHash == "" {
			if requireAnchors {
				return false, nil
			}
			continue
		}
		hash, err := parseEventSourceBlockHash(ref.SourceBlockHash)
		if err != nil {
			return false, err
		}
		ok, err := verify(ref.ToTxNum, hash)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func (m *Manager) eventSourcesCanonical(manifest *Manifest, from, to uint64) (bool, error) {
	m.mu.RLock()
	verify := m.eventCanonicalVerifier
	m.mu.RUnlock()
	if verify == nil {
		return true, nil
	}
	return verifyEventSourceRefs(eventLogRefs(manifest), from, to, verify, true)
}

// NewCanonicalEventLogVerifier binds cold coverage to a fully executed solid
// canonical segment end. It is separate from file/companion authentication:
// authenticated remote files alone do not prove which fork this node executed.
func NewCanonicalEventLogVerifier(chain ChainSource) func(uint64, common.Hash) (bool, error) {
	r := &Runner{chain: chain}
	return func(block uint64, expected common.Hash) (bool, error) {
		if chain == nil || chain.DB() == nil {
			return false, nil
		}
		cap, err := r.independentEventSourceCap(chain.DB())
		if err != nil || block > cap {
			return false, err
		}
		hash, present, err := r.canonicalHashLookup(chain.DB())(block)
		if err != nil || !present {
			return false, err
		}
		if hash != expected {
			return false, fmt.Errorf("snapshots: event source block %d hash %x does not match canonical hash %x", block, expected, hash)
		}
		return true, nil
	}
}
