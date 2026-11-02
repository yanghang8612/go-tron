package snapshots

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// ErrEventLogCompanionRepairRequired identifies a catalog shape that cannot
// be repaired inside the next freezer demand without retiring a wider range.
var ErrEventLogCompanionRepairRequired = errors.New("snapshots: event-log companion requires offline rebuild")

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
	m.eventCanonicalScope = nil
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

// SetEventLogCanonicalSource installs an operation-scoped production verifier.
// Each request observes Finish/solid once, then checks every intersecting exact
// terminal hash without retaining a positive proof across requests or reorgs.
func (m *Manager) SetEventLogCanonicalSource(chain ChainSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventCanonicalVerifier = nil
	m.eventCanonicalScope = func() (func(uint64, common.Hash) (bool, error), error) {
		return newCanonicalEventLogVerificationScope(chain)
	}
}

func (m *Manager) eventSourcesCanonical(manifest *Manifest, from, to uint64) (bool, error) {
	m.mu.RLock()
	verify, begin := m.eventCanonicalVerifier, m.eventCanonicalScope
	m.mu.RUnlock()
	if begin != nil {
		var err error
		verify, err = begin()
		if err != nil {
			return false, err
		}
	}
	if verify == nil {
		return true, nil
	}
	return visitEventSourceRefs(manifest, from, to, func(ref SegmentRef) (bool, error) {
		if ref.SourceBlockHash == "" {
			return false, nil
		}
		hash, err := parseEventSourceBlockHash(ref.SourceBlockHash)
		if err != nil {
			return false, err
		}
		return verify(ref.ToTxNum, hash)
	})
}

func canonicalEventLogTerminalVerifier(chain ChainSource, cap uint64) func(uint64, common.Hash) (bool, error) {
	return func(block uint64, expected common.Hash) (bool, error) {
		if chain == nil || chain.DB() == nil || block > cap {
			return false, nil
		}
		hash, present, err := canonicalEventSourceHash(chain, block)
		if err != nil || !present {
			return false, err
		}
		if hash != expected {
			return false, fmt.Errorf("snapshots: event source block %d hash %x does not match canonical hash %x", block, expected, hash)
		}
		return true, nil
	}
}

// Keep the short-lived read scope independent of the multi-kilobyte Runner;
// capturing a Runner in each terminal closure would allocate it on every RPC.
func canonicalEventSourceHash(chain ChainSource, n uint64) (common.Hash, bool, error) {
	if source, ok := chain.(canonicalHashLookupSource); ok {
		return source.CanonicalBlockHashStrict(n)
	}
	if source, ok := chain.(canonicalHashSource); ok {
		h, present := source.CanonicalBlockHash(n)
		return h, present, nil
	}
	return rawdb.ReadBlockHashByNumberStrict(chain.DB(), n)
}

func newCanonicalEventLogVerificationScope(chain ChainSource) (func(uint64, common.Hash) (bool, error), error) {
	if chain == nil || chain.DB() == nil {
		return canonicalEventLogTerminalVerifier(nil, 0), nil
	}
	finish, present, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(chain.DB(), rawdb.StageFinish, func(n uint64) (common.Hash, bool, error) { return canonicalEventSourceHash(chain, n) })
	if err != nil {
		return nil, fmt.Errorf("snapshots: %w", err)
	}
	cap := uint64(0)
	if present {
		if solid := chain.LatestSolidifiedBlockNum(); solid > 0 {
			cap = min(finish, uint64(solid))
		}
	}
	return canonicalEventLogTerminalVerifier(chain, cap), nil
}

// NewCanonicalEventLogVerifier is the single-terminal adapter used by callers
// without an operation scope. Live managers use SetEventLogCanonicalSource so
// multi-segment requests do not reread Finish for every terminal.
func NewCanonicalEventLogVerifier(chain ChainSource) func(uint64, common.Hash) (bool, error) {
	return func(block uint64, expected common.Hash) (bool, error) {
		verify, err := newCanonicalEventLogVerificationScope(chain)
		if err != nil {
			return false, err
		}
		return verify(block, expected)
	}
}
