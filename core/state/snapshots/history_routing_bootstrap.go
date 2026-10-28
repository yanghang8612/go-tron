package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
)

// A projection of a fully authenticated broad route. It is not a general
// routing object: only small-key memberships and empty metadata domains are
// retained. All authority remains bound to an exact history/accessor pairID.
type historyRoutingBootstrap struct {
	sources          [][32]byte
	columns          map[[32]byte]int
	small            map[string][]byte
	metadataNonempty []bool
}

var sharedHistoryRoutingBootstrap struct {
	sync.Mutex
	digest         [32]byte
	from, to, rows uint64
	value          *historyRoutingBootstrap
}

func loadHistoryRoutingBootstrap(ctx context.Context, dir string, m *Manifest) (*historyRoutingBootstrap, error) {
	var ref *SegmentRef
	for i := range m.Segments {
		if m.Segments[i].Kind == SegmentSystemHistoryRouting {
			if ref != nil {
				return nil, errors.New("snapshots: multiple system history routing segments")
			}
			ref = &m.Segments[i]
		}
	}
	if ref == nil {
		return nil, nil
	}
	data, digest, err := readSystemHistoryRoutingBytes(ctx, dir, *ref)
	if err != nil {
		return nil, err
	}
	sharedHistoryRoutingBootstrap.Lock()
	cached := sharedHistoryRoutingBootstrap.value
	if sharedHistoryRoutingBootstrap.digest != digest || sharedHistoryRoutingBootstrap.from != ref.FromTxNum || sharedHistoryRoutingBootstrap.to != ref.ToTxNum || sharedHistoryRoutingBootstrap.rows != ref.Rows {
		cached = nil
	}
	sharedHistoryRoutingBootstrap.Unlock()
	if cached != nil {
		return cached, nil
	}
	if len(data) < systemHistoryRoutingHeaderSize {
		return nil, errors.New("snapshots: truncated bootstrap route")
	}
	n := int(binary.BigEndian.Uint32(data[12:16]))
	if n > systemHistoryRoutingMaxSources {
		return nil, errors.New("snapshots: bootstrap source budget exceeded")
	}
	b := &historyRoutingBootstrap{small: make(map[string][]byte), metadataNonempty: make([]bool, n)}
	charge := n * 128
	route, err := walkHistoryRoutingFamily(ctx, data, *ref, false, map[string]bool{}, func(key []byte, mode byte, payload []byte) error {
		if smallSystemHistoryRoutingKey(key) {
			charge += len(key) + len(payload) + 257
			if len(b.small) >= smallSystemHistoryRoutingMaxKeys || charge > smallSystemHistoryRoutingBuildBytes {
				return errSystemHistoryRoutingBudget
			}
			members := make([]byte, len(payload)+1)
			members[0] = mode
			copy(members[1:], payload)
			b.small[string(key)] = members
		}
		if metadataRoutingKey(key) {
			if mode == 0 {
				for i, v := range payload {
					for bit := 0; bit < 8; bit++ {
						if v&(1<<bit) != 0 {
							b.metadataNonempty[i*8+bit] = true
						}
					}
				}
			} else {
				for i := 0; i < len(payload); i += 4 {
					b.metadataNonempty[binary.BigEndian.Uint32(payload[i:])] = true
				}
			}
		}
		return contextError(ctx)
	})
	if errors.Is(err, errSystemHistoryRoutingBudget) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	b.sources, b.columns = route.sources, route.columns
	sharedHistoryRoutingBootstrap.Lock()
	sharedHistoryRoutingBootstrap.digest = digest
	sharedHistoryRoutingBootstrap.from = ref.FromTxNum
	sharedHistoryRoutingBootstrap.to = ref.ToTxNum
	sharedHistoryRoutingBootstrap.rows = ref.Rows
	sharedHistoryRoutingBootstrap.value = b
	sharedHistoryRoutingBootstrap.Unlock()
	return b, nil
}

// Avoid opening an unrelated or redundant broad object on a complete small
// route. Overlap is an admission hint only; reuse still requires exact pairID.
func historyRoutingBootstrapNeeded(m *Manifest, old *systemHistoryRouting) bool {
	var broad *SegmentRef
	for i := range m.Segments {
		if m.Segments[i].Kind == SegmentSystemHistoryRouting {
			broad = &m.Segments[i]
			break
		}
	}
	if broad == nil {
		return false
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange || h.ToTxNum < broad.FromTxNum || h.FromTxNum > broad.ToTxNum {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(m, h)
		if !ok {
			continue
		}
		if old == nil {
			return true
		}
		if _, ok := old.columns[systemHistoryRoutingPairID(h, a)]; !ok {
			return true
		}
	}
	return false
}
