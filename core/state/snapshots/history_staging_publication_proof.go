package snapshots

import (
	"context"
	"errors"
	"fmt"

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
	if c == nil || ctx == nil || proved == nil || current == nil || proved.Generation == 0 || len(binding.Spans) == 0 {
		return zero, fmt.Errorf("%w: invalid_proof_context bucket=%d", rawdb.ErrHistoryStagingConflict, binding.Bucket)
	}
	if (proved.Chain == nil) != (current.Chain == nil) {
		return zero, fmt.Errorf("%w: chain_binding_changed bucket=%d", rawdb.ErrHistoryStagingConflict, binding.Bucket)
	}
	if binding.ManifestEpoch != proved.Generation ||
		proved.HistoryStagingResetEpoch != current.HistoryStagingResetEpoch ||
		current.Generation < proved.Generation || current.VisibleTxStart > proved.VisibleTxStart || current.VisibleTxEnd < proved.VisibleTxEnd {
		return zero, fmt.Errorf("%w: manifest_guard bucket=%d binding_generation=%d proved_generation=%d current_generation=%d proved_reset=%d current_reset=%d proved_range=[%d,%d] current_range=[%d,%d]", rawdb.ErrHistoryStagingConflict, binding.Bucket, binding.ManifestEpoch, proved.Generation, current.Generation, proved.HistoryStagingResetEpoch, current.HistoryStagingResetEpoch, proved.VisibleTxStart, proved.VisibleTxEnd, current.VisibleTxStart, current.VisibleTxEnd)
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
	// Production manifests may be unbound; startup already authenticates these
	// through durable staging receipts and ContentIDs. This only preserves an
	// existing proof. The mover still checks canonical bucket/tx and route
	// identity under its chain barrier before publishing any durable binding.
	if proved.Chain != nil {
		if err := current.ValidateChainIdentity(*proved.Chain); err != nil {
			return zero, errors.Join(rawdb.ErrHistoryStagingConflict, err)
		}
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
			return zero, fmt.Errorf("%w: missing_fact bucket=%d content=%x", rawdb.ErrHistoryStagingConflict, binding.Bucket, span.ContentID)
		}
		if _, ok := c.states[span.ContentID]; !ok {
			return zero, fmt.Errorf("%w: missing_file_state bucket=%d content=%x", rawdb.ErrHistoryStagingConflict, binding.Bucket, span.ContentID)
		}
		if id, err := historyStagingTrioID(fact.Refs); err != nil || id != span.ContentID {
			return zero, fmt.Errorf("%w: fact_content_id bucket=%d content=%x", rawdb.ErrHistoryStagingConflict, binding.Bucket, span.ContentID)
		}
		for _, ref := range fact.Refs {
			if actual, ok := original[ref.Path]; !ok || actual != ref {
				return zero, fmt.Errorf("%w: proved_ref_changed bucket=%d path=%s proved_generation=%d current_generation=%d present=%t", rawdb.ErrHistoryStagingConflict, binding.Bucket, ref.Path, proved.Generation, current.Generation, ok)
			}
			if actual, ok := active[ref.Path]; !ok || actual != ref {
				return zero, fmt.Errorf("%w: active_ref_changed bucket=%d path=%s proved_generation=%d current_generation=%d present=%t", rawdb.ErrHistoryStagingConflict, binding.Bucket, ref.Path, proved.Generation, current.Generation, ok)
			}
		}
	}
	if err := c.recheckAllLocked(ctx); err != nil {
		return zero, err
	}
	binding.ManifestEpoch = current.Generation
	return binding, nil
}
