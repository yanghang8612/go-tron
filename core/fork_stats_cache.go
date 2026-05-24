package core

import (
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

type cachedForkStatsStore struct {
	statedb *state.StateDB
	cache   map[int32][]byte
}

func (s *cachedForkStatsStore) ReadForkStats(version int32) []byte {
	if cached, ok := s.cache[version]; ok {
		return cloneForkStats(cached)
	}
	stats := s.statedb.ReadForkStats(version)
	s.cache[version] = cloneForkStats(stats)
	return cloneForkStats(stats)
}

func (s *cachedForkStatsStore) ReadForkStatsBatch(versions []int32) map[int32][]byte {
	out := make(map[int32][]byte, len(versions))
	missing := make([]int32, 0, len(versions))
	for _, version := range versions {
		if cached, ok := s.cache[version]; ok {
			if cached != nil {
				out[version] = cloneForkStats(cached)
			}
			continue
		}
		missing = append(missing, version)
	}
	if len(missing) > 0 {
		loaded := s.statedb.ReadForkStatsBatch(missing)
		for _, version := range missing {
			stats := loaded[version]
			s.cache[version] = cloneForkStats(stats)
			if stats != nil {
				out[version] = cloneForkStats(stats)
			}
		}
	}
	return out
}

func (s *cachedForkStatsStore) WriteForkStats(version int32, stats []byte) {
	s.cache[version] = cloneForkStats(stats)
	s.statedb.WriteForkStatsFinal(version, stats)
}

func cloneForkStats(stats []byte) []byte {
	if stats == nil {
		return nil
	}
	out := make([]byte, len(stats))
	copy(out, stats)
	return out
}

func (bc *BlockChain) forkControllerForState(statedb *state.StateDB) *forks.ForkController {
	if bc.forkStatsCache == nil {
		bc.forkStatsCache = make(map[int32][]byte, len(forks.KnownVersions))
	}
	return forks.NewForkControllerFromStore(&cachedForkStatsStore{
		statedb: statedb,
		cache:   bc.forkStatsCache,
	})
}

func (bc *BlockChain) clearForkStatsCache() {
	for version := range bc.forkStatsCache {
		delete(bc.forkStatsCache, version)
	}
}
