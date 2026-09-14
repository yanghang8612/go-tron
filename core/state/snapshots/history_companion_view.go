package snapshots

// HistoryCompanionView is a detached, read-only companion lookup for a bulk
// operation. It exposes SegmentRef values, never its indexed Manifest or slice.
// It does not authenticate metadata or files; callers must keep their existing
// manifest and file verification checks.
type HistoryCompanionView struct {
	manifest *Manifest
}

// NewHistoryCompanionView copies only the active SegmentRef container and builds
// a bounded lookup index. Strings remain shared immutable values; retired refs,
// chain identity and progress are not retained. The copy plus index is O(N)
// temporary memory, in addition to the caller's manifest. Keep the view local to
// one bulk operation so its active copy and index are collectible on return.
// Nil or oversized inputs return nil without copying; callers must then use the
// original DomainCfg companion methods and their scan semantics.
func NewHistoryCompanionView(manifest *Manifest) *HistoryCompanionView {
	return newHistoryCompanionView(manifest, manifestLookupMaxReferences)
}

func newHistoryCompanionView(manifest *Manifest, limit int) *HistoryCompanionView {
	if manifest == nil || len(manifest.Segments) > limit {
		return nil
	}
	segments := make([]SegmentRef, len(manifest.Segments))
	copy(segments, manifest.Segments)
	return &HistoryCompanionView{manifest: manifestLookupView(&Manifest{Segments: segments})}
}

// HistoryIndexRef preserves DomainCfg's complete companion identity predicate.
func (v *HistoryCompanionView) HistoryIndexRef(cfg DomainCfg, history SegmentRef) (SegmentRef, bool) {
	if v == nil {
		return SegmentRef{}, false
	}
	return cfg.HistoryIndexRef(v.manifest, history)
}

// HistoryAccessorRef preserves DomainCfg's complete companion identity predicate.
func (v *HistoryCompanionView) HistoryAccessorRef(cfg DomainCfg, history SegmentRef) (SegmentRef, bool) {
	if v == nil {
		return SegmentRef{}, false
	}
	return cfg.HistoryAccessorRef(v.manifest, history)
}
