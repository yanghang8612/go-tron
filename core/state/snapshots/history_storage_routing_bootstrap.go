package snapshots

import (
	"context"
	"encoding/binary"
)

// Project every validated V2 storage membership into bounded source filters.
// Two walks avoid keeping a hash per membership. Complete exact pair identities
// are preserved; oversized sources remain unknown. No canonical source is read.
const storageRoutingBootstrapBytes = 64 << 20

type storageRoutingBootstrap struct {
	route   *systemHistoryRouting
	filters []storageSourceFilter
}

func projectStorageHistoryRouting(ctx context.Context, data []byte, ref SegmentRef) (*storageRoutingBootstrap, error) {
	var counts []uint32
	if len(data) >= systemHistoryRoutingHeaderSize && binary.BigEndian.Uint32(data[12:16]) <= systemHistoryRoutingMaxSources {
		counts = make([]uint32, binary.BigEndian.Uint32(data[12:16]))
	}
	visitColumns := func(mode byte, payload []byte, fn func(int)) {
		if mode == 1 {
			for off := 0; off < len(payload); off += 4 {
				fn(int(binary.BigEndian.Uint32(payload[off:])))
			}
			return
		}
		for i, b := range payload {
			for bit := 0; bit < 8; bit++ {
				if b&(1<<bit) != 0 {
					fn(i*8 + bit)
				}
			}
		}
	}
	route, err := walkHistoryRoutingFamily(ctx, data, ref, false, map[string]bool{}, func(key []byte, mode byte, payload []byte) error {
		if contractStorageHistoryRoutingKey(key) {
			visitColumns(mode, payload, func(col int) { counts[col]++ })
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !route.storagePrefixes {
		return nil, nil
	}
	b := &storageRoutingBootstrap{route: route, filters: make([]storageSourceFilter, len(counts))}
	charge := len(counts) * 256
	for col, n := range counts {
		if n <= storageRoutingSourceHashes {
			size := storageFilterBytes(n)
			charge += size
			if charge > storageRoutingBootstrapBytes {
				return nil, nil
			}
			b.filters[col] = storageSourceFilter{count: n, bits: make([]byte, size)}
		}
	}
	_, err = walkHistoryRoutingFamily(ctx, data, ref, false, map[string]bool{}, func(key []byte, mode byte, payload []byte) error {
		if !contractStorageHistoryRoutingKey(key) {
			return nil
		}
		h := hashStoragePrefix(key)
		visitColumns(mode, payload, func(col int) {
			if counts[col] > storageRoutingSourceHashes {
				return
			}
			f := b.filters[col]
			for i := uint64(0); i < storageRoutingProbes; i++ {
				bit := (h.first + i*h.step) & uint64(len(f.bits)*8-1)
				f.bits[bit/8] |= 1 << (bit % 8)
			}
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	// An oversized source is never represented by an empty filter.
	for col, n := range counts {
		if n > storageRoutingSourceHashes {
			delete(b.route.columns, b.route.sources[col])
		}
	}
	return b, nil
}
