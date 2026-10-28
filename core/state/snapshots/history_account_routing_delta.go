package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sort"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const (
	accountDeltaMaxRows          = uint64(8_000_000)
	accountDeltaMaxFileBytes     = uint64(256 << 20)
	accountDeltaRequestMetadata  = uint64(2 << 20)
	accountDeltaRequestResident  = uint64(8 << 20)
	accountDeltaRequestObjects   = 128
	accountDeltaRequestKeys      = 256
	accountDeltaScheduleMetadata = uint64(32 << 20)
)

func isAccountRoutingKind(kind SegmentKind) bool {
	return kind == SegmentAccountHistoryRouting || kind == SegmentAccountHistoryRoutingDelta
}

type accountDeltaKeyProof struct {
	bound uint64
	found bool
}
type accountDeltaObjectProof struct {
	attempted bool
	covered   map[[32]byte]struct{}
	keys      []accountDeltaKeyProof
}

// All proofs are request-local. An omitted, unreadable or budget-excluded
// object is unknown; the caller still reads the authoritative history source.
// Each object's bound is usable only for that same object's exact covered IDs.
type accountDeltaHistoryReader struct {
	refs                                 []SegmentRef
	objects                              []accountDeltaObjectProof
	keys                                 [][]byte
	byKey                                map[string]int
	metadataRemaining, residentRemaining uint64
	opened                               int
}

func newAccountDeltaHistoryReader(m *Manifest, from, to uint64, keys [][]byte) *accountDeltaHistoryReader {
	r := &accountDeltaHistoryReader{metadataRemaining: accountDeltaRequestMetadata, residentRemaining: accountDeltaRequestResident}
	if m == nil || len(keys) > accountDeltaRequestKeys {
		return r
	}
	r.byKey = make(map[string]int)
	for _, key := range keys {
		if !accountRoutingKey(key) {
			continue
		}
		if _, ok := r.byKey[string(key)]; ok {
			continue
		}
		r.byKey[string(key)] = len(r.keys)
		r.keys = append(r.keys, append([]byte(nil), key...))
	}
	if len(r.keys) == 0 {
		return r
	}
	for _, ref := range m.Segments {
		if ref.Kind == SegmentAccountHistoryRoutingDelta && ref.ToTxNum >= from && ref.FromTxNum <= to {
			if len(r.refs) == accountDeltaRequestObjects {
				break
			}
			r.refs = append(r.refs, ref)
		}
	}
	r.objects = make([]accountDeltaObjectProof, len(r.refs))
	return r
}

func (r *accountDeltaHistoryReader) maySkip(ctx context.Context, dir string, history SegmentRef, id [32]byte, key []byte, from uint64) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if r == nil || len(r.refs) == 0 {
		return false, nil
	}
	index, ok := r.byKey[string(key)]
	if !ok {
		return false, nil
	}
	for i, ref := range r.refs {
		if history.FromTxNum < ref.FromTxNum || history.ToTxNum > ref.ToTxNum {
			continue
		}
		proof := &r.objects[i]
		if !proof.attempted {
			proof.attempted = true
			keyCost := uint64(len(r.keys))*64 + 128
			if r.residentRemaining < keyCost || r.metadataRemaining < 128 {
				continue
			}
			r.opened++
			var charged uint64
			reader, err := openAccountHistoryRoutingBudget(ctx, dir, ref, r.metadataRemaining, r.residentRemaining-keyCost, &charged)
			r.metadataRemaining -= min(charged, r.metadataRemaining)
			if err != nil {
				if e := contextError(ctx); e != nil {
					return false, e
				}
				continue
			}
			// Charge authenticated reads even if a used child page is unreadable.
			residentCost := reader.metadata.header.metadataBytes() + uint64(reader.metadata.header.sources)*128 + keyCost
			r.residentRemaining -= residentCost
			values := make([]accountDeltaKeyProof, len(r.keys))
			for j, k := range r.keys {
				values[j].bound, values[j].found, err = reader.lookup(ctx, k)
				if err != nil {
					break
				}
			}
			covered := reader.metadata.covered
			closeErr := reader.Close()
			if err != nil || closeErr != nil {
				if e := contextError(ctx); e != nil {
					return false, e
				}
				continue // lookup failure is never a proved missing key.
			}
			proof.covered, proof.keys = covered, values
		}
		if _, ok := proof.covered[id]; ok {
			value := proof.keys[index]
			if !value.found || from > value.bound || history.FromTxNum > value.bound {
				return true, nil
			}
		}
	}
	return false, nil
}

type accountDeltaBuildStats struct {
	attempted, sourceReads, sources, unsupported int
	inputBytes, dictionaryKeys                   uint64
	nextTx                                       uint64
	eof                                          bool
}

// Coverage union is a scheduling hint only. Query proofs must never borrow a
// bound from this union. Authenticate current metadata even on repeated builds.
func accountRoutingCoveredForBuild(ctx context.Context, dir string, m *Manifest) (map[[32]byte]struct{}, error) {
	covered := make(map[[32]byte]struct{})
	remaining := accountDeltaScheduleMetadata
	for _, ref := range m.Segments {
		if !isAccountRoutingKind(ref.Kind) {
			continue
		}
		r, err := openAccountHistoryRoutingBudget(ctx, dir, ref, remaining, 64<<20, nil)
		if err != nil {
			return nil, err
		}
		cost := r.metadata.header.metadataBytes() + 64
		for id := range r.metadata.covered {
			covered[id] = struct{}{}
			if len(covered) > historyCandidateMaxSources {
				r.Close()
				return nil, errSystemHistoryRoutingBudget
			}
		}
		remaining -= cost
		if err := r.Close(); err != nil {
			return nil, err
		}
	}
	return covered, nil
}

func BuildAccountHistoryRoutingDeltaContext(ctx context.Context, dir string, m *Manifest, maxNew int) (SegmentRef, bool, error) {
	ref, built, _, err := buildAccountRoutingDelta(ctx, dir, m, maxNew, 0, smallSystemRoutingWorkLimit{bytes: 128 << 20, duration: 2 * time.Second})
	return ref, built, err
}

func buildAccountRoutingDelta(ctx context.Context, dir string, m *Manifest, maxNew int, start uint64, work smallSystemRoutingWorkLimit) (ref SegmentRef, built bool, stats accountDeltaBuildStats, err error) {
	stats.nextTx = start
	if err = contextError(ctx); err != nil {
		return
	}
	if m == nil || maxNew < 1 {
		return
	}
	m = manifestLookupView(m)
	covered, e := accountRoutingCoveredForBuild(ctx, dir, m)
	if e != nil {
		err = e
		return
	}
	collector, e := etl.NewCollector(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 32 << 20})
	if e != nil {
		err = e
		return
	}
	defer collector.Close()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var ids [][32]byte
	from, to := ^uint64(0), uint64(0)
	began := time.Now()
	stats.eof = true
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange || h.ToTxNum < start {
			continue
		}
		if err = contextError(ctx); err != nil {
			return
		}
		a, ok := cfg.HistoryAccessorRef(m, h)
		if !ok {
			err = errAccountRoutingCorrupt
			return
		}
		id := systemHistoryRoutingPairID(h, a)
		if _, ok := covered[id]; ok {
			stats.nextTx = h.ToTxNum + 1
			continue
		}
		if stats.attempted >= min(maxNew, systemHistoryRoutingMaxNewSources) || stats.sourceReads > 0 && work.duration > 0 && time.Since(began) >= work.duration {
			stats.eof = false
			break
		}
		stats.attempted++
		if work.bytes > 0 && (a.Size > work.bytes || a.Size > work.bytes-stats.inputBytes) {
			if stats.inputBytes > 0 && a.Size <= work.bytes {
				stats.eof = false
				break
			}
			stats.unsupported++
			stats.nextTx = h.ToTxNum + 1
			continue
		}
		keys, e := accountRoutingDictionaryCost(dir, a)
		if errors.Is(e, errSystemHistoryRoutingBudget) {
			stats.unsupported++
			stats.nextTx = h.ToTxNum + 1
			continue
		}
		if e != nil {
			err = e
			return
		}
		if keys > accountDeltaMaxRows-stats.dictionaryKeys {
			if stats.dictionaryKeys > 0 && keys <= accountDeltaMaxRows {
				stats.eof = false
				break
			}
			stats.unsupported++
			stats.nextTx = h.ToTxNum + 1
			continue
		}
		stats.sourceReads++
		stats.inputBytes += a.Size
		e = iterateHistoryRoutingSourceKeys(ctx, dir, h, a, func(key []byte) error {
			if !accountRoutingKey(key) {
				return nil
			}
			return collector.PutEncoded(29, 0, func(out, _ []byte) { copy(out, key); binary.BigEndian.PutUint64(out[21:], h.ToTxNum) })
		})
		if errors.Is(e, errSystemHistoryRoutingBudget) {
			stats.unsupported++
			stats.nextTx = h.ToTxNum + 1
			continue
		}
		if e != nil {
			err = e
			return
		}
		stats.dictionaryKeys += keys
		stats.sources++
		ids = append(ids, id)
		from = min(from, h.FromTxNum)
		to = max(to, h.ToTxNum)
		stats.nextTx = h.ToTxNum + 1
	}
	if stats.eof {
		stats.nextTx = 0
	}
	if len(ids) == 0 {
		return
	}
	ref, built, err = writeAccountRoutingTable(ctx, dir, SegmentAccountHistoryRoutingDelta, ids, from, to, collector, nil)
	return
}

var errAccountDeltaPublicationConflict = errors.New("snapshots: account delta publication conflict")

// Consolidation never opens the base, and selects only a bounded pair of delta
// objects at the same level. Combining upper bounds is safe but can weaken
// per-object absence selectivity; the HTTP comparison must measure that tradeoff.
func compactAccountRoutingDeltas(ctx context.Context, dir string, m *Manifest, maxBytes uint64) (ref SegmentRef, replace []SegmentRef, built bool, err error) {
	if err = contextError(ctx); err != nil {
		return
	}
	if m == nil || maxBytes == 0 {
		return
	}
	var refs []SegmentRef
	for _, r := range m.Segments {
		if r.Kind == SegmentAccountHistoryRoutingDelta {
			refs = append(refs, r)
		}
	}
	view := manifestLookupView(m)
	current := manifestHistorySources(view)
	if current == nil {
		err = errSystemHistoryRoutingBudget
		return
	}
	// Authenticate candidate source sets before reading data rows. Fully stale
	// prefixes must neither incur a table scan nor pin all future selection.
	checked := make(map[SegmentRef]bool)
	hasCurrent := func(input SegmentRef) (bool, error) {
		if result, ok := checked[input]; ok {
			return result, nil
		}
		reader, e := openAccountHistoryRouting(ctx, dir, input)
		if e != nil {
			return false, e
		}
		result := false
		for id := range reader.metadata.covered {
			if _, ok := current.byID[id]; ok {
				result = true
				break
			}
		}
		if e := reader.Close(); e != nil {
			return false, e
		}
		checked[input] = result
		return result, nil
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].FromTxNum < refs[j].FromTxNum })
	for i := 1; i < len(refs); i++ {
		a, b := refs[i-1], refs[i]
		if a.effectiveAggregationSteps() != b.effectiveAggregationSteps() || a.Size > maxBytes || b.Size > maxBytes-a.Size || a.Rows > accountDeltaMaxRows || b.Rows > accountDeltaMaxRows-a.Rows {
			continue
		}
		aCurrent, e := hasCurrent(a)
		if e != nil {
			err = e
			return
		}
		bCurrent, e := hasCurrent(b)
		if e != nil {
			err = e
			return
		}
		if !aCurrent && !bCurrent {
			continue
		}
		replace = []SegmentRef{a, b}
		break
	}
	if len(replace) == 0 {
		return
	}
	collector, e := etl.NewCollector(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 32 << 20})
	if e != nil {
		err = e
		return
	}
	defer collector.Close()
	ids := make(map[[32]byte]struct{})
	from, to := ^uint64(0), uint64(0)
	for _, input := range replace {
		reader, e := openAccountHistoryRouting(ctx, dir, input)
		if e != nil {
			err = e
			return
		}
		useful := false
		for id := range reader.metadata.covered {
			index, ok := current.byID[id]
			if !ok {
				continue
			}
			h := view.Segments[current.sources[index].row]
			ids[id] = struct{}{}
			useful = true
			from = min(from, h.FromTxNum)
			to = max(to, h.ToTxNum)
		}
		if !useful {
			if e := reader.Close(); e != nil {
				err = e
				return
			}
			continue
		}
		it := accountRoutingIterator{reader: reader}
		for {
			key, bound, ok, e := it.next(ctx)
			if e != nil {
				reader.Close()
				err = e
				return
			}
			if !ok {
				break
			}
			e = collector.PutEncoded(29, 0, func(out, _ []byte) { copy(out, key[:]); binary.BigEndian.PutUint64(out[21:], bound) })
			if e != nil {
				reader.Close()
				err = e
				return
			}
		}
		if e := reader.Close(); e != nil {
			err = e
			return
		}
	}
	if len(ids) == 0 {
		return
	} // Stale-only retirement is a separate maintenance decision.
	ordered := make([][32]byte, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return string(ordered[i][:]) < string(ordered[j][:]) })
	ref, built, err = writeAccountRoutingTable(ctx, dir, SegmentAccountHistoryRoutingDelta, ordered, from, to, collector, nil)
	if built {
		ref.AggregationSteps = replace[0].effectiveAggregationSteps() + replace[1].effectiveAggregationSteps()
	}
	return
}

// Caller holds the history read lease through build and this publication.
// Envelopes are not ownership: only the explicitly named input refs retire.
func publishAccountHistoryDelta(ctx context.Context, dir string, parent *Manifest, ref SegmentRef, replace []SegmentRef) (*Manifest, error) {
	if parent == nil || ref.Kind != SegmentAccountHistoryRoutingDelta {
		return nil, errAccountRoutingCorrupt
	}
	guardFrom, guardTo := ref.FromTxNum, ref.ToTxNum
	for _, input := range replace {
		if input.Kind != SegmentAccountHistoryRoutingDelta {
			return nil, errAccountRoutingCorrupt
		}
		guardFrom = min(guardFrom, input.FromTxNum)
		guardTo = max(guardTo, input.ToTxNum)
	}
	expected := pagedHistoryRoutingInputs(parent, guardFrom, guardTo, SegmentAccountHistoryRoutingDelta)
	manifestPublicationMu.Lock()
	defer manifestPublicationMu.Unlock()
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	latest, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		return nil, err
	}
	current := pagedHistoryRoutingInputs(manifestLookupView(latest), guardFrom, guardTo, SegmentAccountHistoryRoutingDelta)
	if len(expected) != len(current) {
		return nil, errAccountDeltaPublicationConflict
	}
	for input := range expected {
		if _, ok := current[input]; !ok {
			return nil, errAccountDeltaPublicationConflict
		}
	}
	output, err := openAccountHistoryRouting(ctx, dir, ref)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	known := manifestHistorySources(manifestLookupView(latest))
	if known == nil {
		return nil, errSystemHistoryRoutingBudget
	}
	for id := range output.metadata.covered {
		if _, ok := known.byID[id]; !ok {
			return nil, errAccountDeltaPublicationConflict
		}
	}
	remove := make(map[SegmentRef]struct{}, len(replace))
	for _, input := range replace {
		if input.Kind != SegmentAccountHistoryRoutingDelta {
			return nil, errAccountRoutingCorrupt
		}
		if _, ok := current[input]; !ok {
			return nil, errAccountDeltaPublicationConflict
		}
		old, e := openAccountHistoryRouting(ctx, dir, input)
		if e != nil {
			return nil, e
		}
		for id := range old.metadata.covered {
			if _, current := known.byID[id]; !current {
				continue
			}
			if _, ok := output.metadata.covered[id]; !ok {
				old.Close()
				return nil, errAccountRoutingCorrupt
			}
		}
		if e := old.Close(); e != nil {
			return nil, e
		}
		remove[input] = struct{}{}
	}
	next := cloneManifest(latest)
	next.Generation++
	next.PublishedUnix = time.Now().Unix()
	next.Segments = next.Segments[:0]
	haveOutput, removed := false, false
	for _, input := range latest.Segments {
		if input == ref {
			haveOutput = true
			next.Segments = append(next.Segments, input)
			continue // Content-addressed consolidation can reuse an active output.
		}
		if _, ok := remove[input]; ok {
			next.Retired = append(next.Retired, input)
			removed = true
			continue
		}
		if input.Path == ref.Path {
			return nil, errAccountDeltaPublicationConflict
		}
		next.Segments = append(next.Segments, input)
	}
	if haveOutput && !removed {
		return latest, nil
	}
	if !haveOutput {
		next.Segments = append(next.Segments, ref)
	}
	if err := publishManifestLocked(dir, next); err != nil {
		return nil, err
	}
	return next, nil
}
