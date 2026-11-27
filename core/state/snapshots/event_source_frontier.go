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

type eventCanonicalScopePolicy struct {
	verify              func(uint64, common.Hash) (bool, error)
	legacyStateBoundary func() (uint64, error) // nil => unanchored refs always rejected
}

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
	policy := eventCanonicalScopePolicy{verify: verify}
	if !requireAnchors {
		policy.legacyStateBoundary = func() (uint64, error) { return ^uint64(0), nil }
	}
	return verifyEventSourceRefsWithPolicy(refs, from, to, policy)
}

func verifyEventSourceRefsWithPolicy(refs []SegmentRef, from, to uint64, policy eventCanonicalScopePolicy) (bool, error) {
	for _, ref := range refs {
		if ref.ToTxNum < from || ref.FromTxNum > to {
			continue
		}
		ok, err := verifyEventSourceRefWithPolicy(ref, policy)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func verifyEventSourceRefWithPolicy(ref SegmentRef, policy eventCanonicalScopePolicy) (bool, error) {
	if ref.SourceBlockHash == "" {
		if policy.legacyStateBoundary == nil {
			return false, nil
		}
		legacyCap, err := policy.legacyStateBoundary()
		if err != nil {
			return false, err
		}
		if legacyCap == 0 || ref.ToTxNum > legacyCap {
			return false, nil
		}
		return true, nil
	}
	hash, err := parseEventSourceBlockHash(ref.SourceBlockHash)
	if err != nil {
		return false, err
	}
	return policy.verify(ref.ToTxNum, hash)
}

// SetEventLogCanonicalSource installs an operation-scoped production verifier.
// Each request observes Finish/solid once, then checks every intersecting exact
// terminal hash without retaining a positive proof across requests or reorgs.
// Legacy V4 segments with empty SourceBlockHash are accepted only when strictly
// within verified StageSnapshotBuild (lazily read if such a segment is hit).
func (m *Manager) SetEventLogCanonicalSource(chain ChainSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventCanonicalVerifier = nil
	m.eventCanonicalScope = func() (eventCanonicalScopePolicy, error) {
		return newCanonicalEventLogVerificationScope(chain)
	}
}

func (m *Manager) eventSourcesCanonical(manifest *Manifest, from, to uint64) (bool, error) {
	m.mu.RLock()
	verify, begin := m.eventCanonicalVerifier, m.eventCanonicalScope
	m.mu.RUnlock()
	var policy eventCanonicalScopePolicy
	if begin != nil {
		var err error
		policy, err = begin()
		if err != nil {
			return false, err
		}
	} else {
		policy = eventCanonicalScopePolicy{verify: verify}
	}
	if policy.verify == nil {
		return true, nil
	}
	return visitEventSourceRefs(manifest, from, to, func(ref SegmentRef) (bool, error) {
		return verifyEventSourceRefWithPolicy(ref, policy)
	})
}

func newLazyLegacyEventStateBoundary(chain ChainSource, cap uint64) func() (uint64, error) {
	var loaded bool
	var boundary uint64
	var loadErr error
	return func() (uint64, error) {
		if loaded {
			return boundary, loadErr
		}
		loaded = true
		if chain == nil || chain.DB() == nil || cap == 0 {
			return 0, nil
		}
		snapBlock, present, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(chain.DB(), rawdb.StageSnapshotBuild, func(n uint64) (common.Hash, bool, error) {
			return canonicalEventSourceHash(chain, n)
		})
		if err != nil {
			loadErr = fmt.Errorf("snapshots: %w", err)
			return 0, loadErr
		}
		if present && snapBlock > 0 {
			boundary = min(cap, snapBlock)
		}
		return boundary, nil
	}
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
	return readSnapshotCanonicalBlockHashStrict(chain.DB(), n)
}

func newCanonicalEventLogVerificationScope(chain ChainSource) (eventCanonicalScopePolicy, error) {
	if chain == nil || chain.DB() == nil {
		return eventCanonicalScopePolicy{verify: canonicalEventLogTerminalVerifier(nil, 0)}, nil
	}
	finish, present, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(chain.DB(), rawdb.StageFinish, func(n uint64) (common.Hash, bool, error) {
		return canonicalEventSourceHash(chain, n)
	})
	if err != nil {
		return eventCanonicalScopePolicy{}, fmt.Errorf("snapshots: %w", err)
	}
	cap := uint64(0)
	if present {
		if solid := chain.LatestSolidifiedBlockNum(); solid > 0 {
			cap = min(finish, uint64(solid))
		}
	}
	return eventCanonicalScopePolicy{
		verify:              canonicalEventLogTerminalVerifier(chain, cap),
		legacyStateBoundary: newLazyLegacyEventStateBoundary(chain, cap),
	}, nil
}

// NewCanonicalEventLogVerifier is the single-terminal adapter used by callers
// without an operation scope. Live managers use SetEventLogCanonicalSource so
// multi-segment requests do not reread Finish for every terminal.
func NewCanonicalEventLogVerifier(chain ChainSource) func(uint64, common.Hash) (bool, error) {
	return func(block uint64, expected common.Hash) (bool, error) {
		policy, err := newCanonicalEventLogVerificationScope(chain)
		if err != nil {
			return false, err
		}
		return policy.verify(block, expected)
	}
}
