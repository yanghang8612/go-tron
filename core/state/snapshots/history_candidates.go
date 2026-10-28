package snapshots

import (
	"context"
	"sort"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// The immutable manifest owns these identities. No decoded file bytes or
// absence proofs survive a request. Bound the additional table independently
// of the general path index (roughly 50 MiB at the source limit).
const historyCandidateMaxSources = 1 << 18

type historySource struct {
	row    uint32
	id     [32]byte
	paired bool
}

type historySourceTable struct {
	sources []historySource // original manifest order, including legacy sources
	byID    map[[32]byte]uint32
}

func manifestHistorySources(m *Manifest) *historySourceTable {
	if m == nil || m.lookup == nil {
		return nil
	}
	m.lookup.historyOnce.Do(func() {
		n := 0
		for _, h := range m.Segments {
			if h.Kind == SegmentHistory && h.normalizedDataset() == SegmentDatasetStateDomainChange {
				n++
			}
		}
		if n > historyCandidateMaxSources {
			return
		}
		t := &historySourceTable{sources: make([]historySource, 0, n), byID: make(map[[32]byte]uint32, n)}
		cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
		for row, h := range m.Segments {
			if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
				continue
			}
			s := historySource{row: uint32(row)}
			if a, ok := cfg.HistoryAccessorRef(m, h); ok {
				s.id, s.paired = systemHistoryRoutingPairID(h, a), true
				if _, duplicate := t.byID[s.id]; duplicate {
					return
				}
				t.byID[s.id] = uint32(len(t.sources))
			}
			t.sources = append(t.sources, s)
		}
		m.lookup.history = t
	})
	return m.lookup.history
}

type historyCandidateQuery struct {
	query        StateHistoryQuery
	lookup       []byte
	accountBound uint64
	accountFound bool
}

const historyCandidatePlanBytes = 16 << 20

type historyCandidateCursor struct {
	lookup []byte
	rows   []uint32
	pos    int
}

type historyCandidatePlan struct {
	table    *historySourceTable
	cursors  []historyCandidateCursor
	fallback []historySource
	pos      int
}

// Coverage and the route-column -> manifest-row translation do not depend on
// the requested key. Compile them once for the whole batch, after authenticating
// this request's route. A new plan never borrows another request's proof.
type historyCandidateRoute struct {
	unknown []uint32
	rows    []uint32 // table index + 1; zero is outside/unknown
}

type historyCandidateRoutes map[*systemHistoryRouting]*historyCandidateRoute

func newHistoryCandidatePlan(ctx context.Context, dir string, m *Manifest, from, to uint64, queries []historyCandidateQuery, broad, small *systemHistoryRouting, account *accountHistoryRoutingReader, metadata *metadataHistoryRoutingReader) (*historyCandidatePlan, error) {
	p := &historyCandidatePlan{table: manifestHistorySources(m)}
	routes := make(historyCandidateRoutes, 2)
	used := 0
	if p.table != nil && len(queries) <= 256 {
		for _, q := range queries {
			candidates, err := historyCandidatesWithRoutes(ctx, dir, m, from, to, []historyCandidateQuery{q}, broad, small, account, metadata, routes)
			if err != nil {
				return nil, err
			}
			used += len(candidates)*4 + len(q.lookup) + 64
			if used > historyCandidatePlanBytes {
				p.cursors = nil
				break
			}
			cursor := historyCandidateCursor{lookup: q.lookup, rows: make([]uint32, len(candidates))}
			for i, candidate := range candidates {
				cursor.rows[i] = candidate.row
			}
			p.cursors = append(p.cursors, cursor)
		}
		if len(p.cursors) == len(queries) {
			return p, nil
		}
	}
	var err error
	p.fallback, err = historyCandidatesWithRoutes(ctx, dir, m, from, to, queries, broad, small, account, metadata, routes)
	return p, err
}

// Merge only still-unresolved key cursors. A key changed in every source must
// stop contributing candidates immediately after its first authoritative hit;
// a static union would keep walking the entire catalog for a different absent key.
func (p *historyCandidatePlan) next(active func([]byte) bool) (historySource, bool) {
	if p.fallback != nil {
		if p.pos == len(p.fallback) {
			return historySource{}, false
		}
		s := p.fallback[p.pos]
		p.pos++
		return s, true
	}
	row, found := uint32(0), false
	for i := range p.cursors {
		c := &p.cursors[i]
		if c.pos == len(c.rows) || !active(c.lookup) {
			continue
		}
		if !found || c.rows[c.pos] < row {
			row, found = c.rows[c.pos], true
		}
	}
	if !found {
		return historySource{}, false
	}
	for i := range p.cursors {
		c := &p.cursors[i]
		if c.pos < len(c.rows) && c.rows[c.pos] == row {
			c.pos++
		}
	}
	i := sort.Search(len(p.table.sources), func(i int) bool { return p.table.sources[i].row >= row })
	return p.table.sources[i], true
}

// Compile inverted memberships into a union of possible sources. Uncovered,
// replaced, unsupported and unreadable auxiliary proofs always remain possible.
// The authoritative loop below still authenticates files and resolves rows in
// manifest order; a candidate is never an answer.
func historyCandidates(ctx context.Context, dir string, m *Manifest, from, to uint64, queries []historyCandidateQuery, broad, small *systemHistoryRouting, account *accountHistoryRoutingReader, metadata *metadataHistoryRoutingReader) ([]historySource, error) {
	return historyCandidatesWithRoutes(ctx, dir, m, from, to, queries, broad, small, account, metadata, make(historyCandidateRoutes, 2))
}

func historyCandidatesWithRoutes(ctx context.Context, dir string, m *Manifest, from, to uint64, queries []historyCandidateQuery, broad, small *systemHistoryRouting, account *accountHistoryRoutingReader, metadata *metadataHistoryRoutingReader, routes historyCandidateRoutes) ([]historySource, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	t := manifestHistorySources(m)
	if t == nil {
		var out []historySource
		for i, h := range m.Segments {
			if h.Kind == SegmentHistory && h.normalizedDataset() == SegmentDatasetStateDomainChange && h.ToTxNum >= from && h.FromTxNum <= to {
				out = append(out, historySource{row: uint32(i)})
			}
		}
		return out, nil
	}
	possible := make([]bool, len(t.sources))
	inside := func(i int) bool { h := m.Segments[t.sources[i].row]; return h.ToTxNum >= from && h.FromTxNum <= to }
	// Build unknown columns once per authenticated route, rather than doing a
	// source membership lookup for every key at every manifest row.
	addRoute := func(r *systemHistoryRouting, key []byte) bool {
		if r == nil || contractStorageHistoryRoutingKey(key) && !r.storagePrefixes {
			return false
		}
		key = normalizedHistoryRoutingKey(key)
		if r.selection != nil && !r.selection[string(key)] {
			return false
		}
		compiled, ok := routes[r]
		if !ok {
			compiled = &historyCandidateRoute{rows: make([]uint32, len(r.sources))}
			for i, s := range t.sources {
				if !inside(i) {
					continue
				}
				column, covered := r.columns[s.id]
				if !s.paired || !covered || column < 0 || column >= len(r.sources) || r.sources[column] != s.id {
					compiled.unknown = append(compiled.unknown, uint32(i))
				} else {
					compiled.rows[column] = uint32(i + 1)
				}
			}
			routes[r] = compiled
		}
		for _, i := range compiled.unknown {
			possible[i] = true
		}
		members := r.keys[string(key)]
		if len(members) > 0 {
			systemHistoryMembersIterate(members, func(column int) {
				if i := compiled.rows[column]; i != 0 {
					possible[i-1] = true
				}
			})
		}
		return true
	}
	for _, q := range queries {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if smallSystemHistoryRoutingQuery(q.query) && addRoute(small, q.lookup) {
			continue
		}
		if systemHistoryRoutingQuery(q.query) && q.query.Domain != kvdomains.ContractMetadata && addRoute(broad, q.lookup) {
			continue
		}
		for i, s := range t.sources {
			if possible[i] || !inside(i) {
				continue
			}
			if i%256 == 0 {
				if err := contextError(ctx); err != nil {
					return nil, err
				}
			}
			h := m.Segments[s.row]
			if account != nil && accountRoutingQuery(q.query) && s.paired {
				if _, covered := account.metadata.covered[s.id]; covered && (!q.accountFound || from > q.accountBound || h.FromTxNum > q.accountBound) {
					continue
				}
			}
			if q.query.Domain == kvdomains.ContractMetadata && metadataRoutingKey(q.lookup) && s.paired {
				f, covered, err := metadata.source(ctx, dir, h, s.id)
				if err == nil && covered && !f.mayContain(q.lookup) {
					continue
				}
				// Defer an unreadable page to demand order. Speculation must not
				// expose an error belonging only to a later unneeded source.
				if err == nil && !covered && broad != nil {
					if col, ok := broad.columns[s.id]; ok && !broad.mayContain(col, q.lookup) {
						continue
					}
				}
			}
			possible[i] = true
		}
	}
	out := make([]historySource, 0)
	for i, s := range t.sources {
		if possible[i] {
			out = append(out, s)
		}
	}
	return out, contextError(ctx)
}
