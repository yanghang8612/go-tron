package snapshots

import (
	"context"
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// RecheckColdBindingPublication checks the continued applicability of an
// already completed semantic proof. The caller retains the proof's cold lease
// and holds publication read admission until durable publication finishes.
// Unrelated manifest additions need not invalidate an unchanged authenticated
// trio. Replaced/retired trios still require a new semantic proof.
// This metadata-only check never yields a maintenance lease under writer locks.
func (c *HistoryStagingPhysicalFactCollector) RecheckColdBindingPublication(ctx context.Context, proved, current *Manifest, binding rawdb.HistoryStagingColdBinding) (rawdb.HistoryStagingColdBinding, error) {
	var zero rawdb.HistoryStagingColdBinding
	if c == nil || ctx == nil || proved == nil || current == nil || proved.Chain == nil || current.Chain == nil || proved.Generation == 0 ||
		binding.ManifestEpoch != proved.Generation || len(binding.Spans) == 0 ||
		proved.HistoryStagingResetEpoch != current.HistoryStagingResetEpoch ||
		current.Generation < proved.Generation || current.VisibleTxStart > proved.VisibleTxStart || current.VisibleTxEnd < proved.VisibleTxEnd {
		return zero, rawdb.ErrHistoryStagingConflict
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	for _, manifest := range []*Manifest{proved, current} {
		if err := manifest.Validate(); err != nil {
			return zero, errors.Join(rawdb.ErrHistoryStagingConflict, err)
		}
		if err := manifest.ValidateProduction(); err != nil {
			return zero, errors.Join(rawdb.ErrHistoryStagingConflict, err)
		}
	}
	if err := current.ValidateChainIdentity(*proved.Chain); err != nil {
		return zero, errors.Join(rawdb.ErrHistoryStagingConflict, err)
	}
	active := make(map[string]SegmentRef, len(current.Segments))
	for _, ref := range current.Segments {
		active[ref.Path] = ref
	}
	original := make(map[string]SegmentRef, len(proved.Segments))
	for _, ref := range proved.Segments {
		original[ref.Path] = ref
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, span := range binding.Spans {
		fact, ok := c.entries[span.ContentID]
		if !ok {
			return zero, rawdb.ErrHistoryStagingConflict
		}
		if _, ok := c.states[span.ContentID]; !ok {
			return zero, rawdb.ErrHistoryStagingConflict
		}
		if id, err := historyStagingTrioID(fact.Refs); err != nil || id != span.ContentID {
			return zero, rawdb.ErrHistoryStagingConflict
		}
		for _, ref := range fact.Refs {
			if actual, ok := original[ref.Path]; !ok || actual != ref {
				return zero, rawdb.ErrHistoryStagingConflict
			}
			if actual, ok := active[ref.Path]; !ok || actual != ref {
				return zero, rawdb.ErrHistoryStagingConflict
			}
		}
	}
	if err := c.recheckAllLocked(ctx); err != nil {
		return zero, err
	}
	binding.ManifestEpoch = current.Generation
	return binding, nil
}
