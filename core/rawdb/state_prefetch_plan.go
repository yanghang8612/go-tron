package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// StatePrefetchPlan is a bounded, deduplicated set of flat-state point reads.
// Physical key construction stays in rawdb. IDs are stable even though the
// storage engine may visit rows in physical key order. A rejected Add returns
// -1. Values passed to Execute's callback are read-only and callback-scoped.
type StatePrefetchPlan struct {
	keys  [][]byte
	kv    []bool
	index map[string]int
	limit int
}

func NewStatePrefetchPlan(limit int) *StatePrefetchPlan {
	if limit < 0 {
		limit = 0
	}
	return &StatePrefetchPlan{limit: limit, index: make(map[string]int)}
}

func (p *StatePrefetchPlan) add(key []byte, kv bool) int {
	if id, ok := p.index[string(key)]; ok {
		return id
	}
	if len(p.keys) >= p.limit {
		return -1
	}
	id := len(p.keys)
	p.index[string(key)] = id
	p.keys = append(p.keys, key)
	p.kv = append(p.kv, kv)
	return id
}

func (p *StatePrefetchPlan) AddAccount(owner common.Address) int {
	return p.add(stateAccountLatestKey(owner), false)
}

func (p *StatePrefetchPlan) AddKV(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) int {
	return p.add(stateKVLatestKey(owner, generation, domain, key), true)
}

func (p *StatePrefetchPlan) AddCode(hash common.Hash) int {
	if hash == (common.Hash{}) {
		return -1
	}
	return p.add(stateCodeKey(hash), false)
}

func (p *StatePrefetchPlan) Execute(db ethdb.KeyValueReader, visit func(int, []byte, bool, error) error) error {
	decode := func(id int, value []byte, present bool, err error) error {
		if err == nil && present && p.kv[id] {
			value, err = decodeStateKVLatestValueNoCopy(value)
			if err != nil {
				present = false
				err = fmt.Errorf("state prefetch row %d: %w", id, err)
			}
		}
		return visit(id, value, present, err)
	}
	if batch, ok := db.(pointread.BatchPrefetcher); ok {
		return batch.PrefetchBatch(p.keys, decode)
	}
	for id, key := range p.keys {
		value, present, err := prefetchStatePresentNoCopy(db, key, "state prefetch")
		if err := decode(id, value, present, err); err != nil {
			return err
		}
	}
	return nil
}
