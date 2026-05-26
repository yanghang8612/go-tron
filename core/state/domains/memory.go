package domains

import (
	"bytes"
	"errors"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type MemoryStore struct {
	values map[string]memoryEntry
}

type memoryEntry struct {
	owner  common.Address
	domain kvdomains.KVDomain
	key    []byte
	value  []byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{values: make(map[string]memoryEntry)}
}

func (m *MemoryStore) GetLatest(owner common.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if err := validateDomain(domain); err != nil {
		return nil, false, err
	}
	if m == nil {
		return nil, false, nil
	}
	entry, ok := m.values[logicalKey(owner, domain, key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), entry.value...), true, nil
}

func (m *MemoryStore) DomainPut(owner common.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if m == nil {
		return errors.New("domains: nil memory store")
	}
	m.ensure()
	m.values[logicalKey(owner, domain, key)] = memoryEntry{
		owner:  owner,
		domain: domain,
		key:    append([]byte(nil), key...),
		value:  append([]byte(nil), value...),
	}
	return nil
}

func (m *MemoryStore) DomainDel(owner common.Address, domain kvdomains.KVDomain, key []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	delete(m.values, logicalKey(owner, domain, key))
	return nil
}

func (m *MemoryStore) DomainDelPrefix(owner common.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	for k, entry := range m.values {
		if entry.owner == owner && entry.domain == domain && bytes.HasPrefix(entry.key, prefix) {
			delete(m.values, k)
		}
	}
	return nil
}

func (m *MemoryStore) DomainIterate(owner common.Address, domain kvdomains.KVDomain, prefix []byte, fn IterateFunc) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if fn == nil || m == nil {
		return nil
	}
	entries := make([]memoryEntry, 0)
	for _, entry := range m.values {
		if entry.owner == owner && entry.domain == domain && bytes.HasPrefix(entry.key, prefix) {
			entries = append(entries, memoryEntry{
				owner:  entry.owner,
				domain: entry.domain,
				key:    append([]byte(nil), entry.key...),
				value:  append([]byte(nil), entry.value...),
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	for _, entry := range entries {
		cont, err := fn(append([]byte(nil), entry.key...), append([]byte(nil), entry.value...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func (m *MemoryStore) ensure() {
	if m.values == nil {
		m.values = make(map[string]memoryEntry)
	}
}
