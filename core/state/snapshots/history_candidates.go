package snapshots

import (
	"context"

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

// Compile inverted memberships into a union of possible sources. Uncovered,
// replaced, unsupported and unreadable auxiliary proofs always remain possible.
// The authoritative loop below still authenticates files and resolves rows in
// manifest order; a candidate is never an answer.
func historyCandidates(ctx context.Context, dir string, m *Manifest, from, to uint64, queries []historyCandidateQuery, broad, small *systemHistoryRouting, account *accountHistoryRoutingReader, metadata *metadataHistoryRoutingReader) ([]historySource, error) {
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
	unknown := make(map[*systemHistoryRouting][]uint32, 2)
	addRoute := func(r *systemHistoryRouting, key []byte) bool {
		if r == nil || contractStorageHistoryRoutingKey(key) && !r.storagePrefixes {
			return false
		}
		key = normalizedHistoryRoutingKey(key)
		if r.selection != nil && !r.selection[string(key)] {
			return false
		}
		rows, ok := unknown[r]
		if !ok {
			for i, s := range t.sources {
				if !inside(i) {
					continue
				}
				if _, covered := r.columns[s.id]; !s.paired || !covered {
					rows = append(rows, uint32(i))
				}
			}
			unknown[r] = rows
		}
		for _, i := range rows {
			possible[i] = true
		}
		members := r.keys[string(key)]
		if len(members) > 0 {
			systemHistoryMembersIterate(members, func(column int) {
				if i, ok := t.byID[r.sources[column]]; ok && inside(int(i)) {
					possible[i] = true
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
