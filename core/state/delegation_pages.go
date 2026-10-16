package state

import (
	"bytes"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type legacyDelegationPages struct{ from, to [][][]byte }

// ReadPagedDelegationIndex reconstructs an aggregate index from a consistent
// generic KV view. The same helper serves current and historical state.
func ReadPagedDelegationIndex(account []byte, get func([]byte) ([]byte, bool, error)) (*corepb.DelegatedResourceAccountIndex, bool, error) {
	record, _, exists, err := readPagedDelegationIndex(account, get)
	return record, exists, err
}

func readPagedDelegationIndex(account []byte, get func([]byte) ([]byte, bool, error)) (*corepb.DelegatedResourceAccountIndex, legacyDelegationPages, bool, error) {
	var pages legacyDelegationPages
	data, exists, err := get(rawdb.DrAccountIndexLegacyStateKey(account))
	if err != nil || !exists {
		return nil, pages, exists, err
	}
	record, fromCount, toCount, err := statecodec.UnmarshalDelegationDescriptor(data)
	if err != nil {
		return nil, pages, true, fmt.Errorf("decode dr account index legacy: %w", err)
	}
	for _, dir := range []struct {
		incoming bool
		count    uint64
		pages    *[][][]byte
		list     *[][]byte
	}{{true, fromCount, &pages.from, &record.FromAccounts}, {false, toCount, &pages.to, &record.ToAccounts}} {
		entries := 0
		for i := uint64(0); i < dir.count; i++ {
			data, ok, err := get(rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, i))
			if err != nil {
				return nil, pages, true, err
			}
			if !ok {
				return nil, pages, true, fmt.Errorf("delegation page missing (account=%x incoming=%v page=%d)", account, dir.incoming, i)
			}
			list, err := statecodec.UnmarshalDelegationPage(data)
			if err != nil {
				return nil, pages, true, fmt.Errorf("decode delegation page %d: %w", i, err)
			}
			// Include one spare header and reject arithmetic overflow before
			// accumulating counts or computing the aggregate allocation size.
			maxEntries := int(^uint(0)>>1)/24 - 1
			if len(list) > maxEntries-entries {
				return nil, pages, true, fmt.Errorf("delegation index entry count overflows addressable memory")
			}
			*dir.pages = append(*dir.pages, list)
			entries += len(list)
		}
		if entries != 0 {
			// Count actual decoded entries, including interior empty pages. Do
			// not reserve from the untrusted descriptor's page count. Growing
			// the aggregate once per page repeatedly copies large header arrays.
			// Page and aggregate headers must stay separate: page removal and
			// aggregate removal compact their respective arrays independently.
			// Edge edits commonly append immediately after loading. A single
			// spare header avoids copying the full aggregate on that append;
			// cache admission already accounts for the complete capacity.
			*dir.list = make([][]byte, entries, entries+1)
			offset := 0
			for _, page := range *dir.pages {
				offset += copy((*dir.list)[offset:], page)
			}
		}
	}
	return record, pages, true, nil
}

func splitLegacyDelegationPages(record *corepb.DelegatedResourceAccountIndex) legacyDelegationPages {
	split := func(list [][]byte) [][][]byte {
		var out [][][]byte
		for len(list) != 0 {
			count := min(len(list), statecodec.DelegationPageEntries)
			// Own page headers: removals must never mutate the materialized list headers.
			out = append(out, append([][]byte(nil), list[:count]...))
			list = list[count:]
		}
		return out
	}
	return legacyDelegationPages{from: split(record.FromAccounts), to: split(record.ToAccounts)}
}

func (s *StateDB) writeLegacyDelegationDescriptor(e *legacyDelegationEntry) error {
	data, err := statecodec.MarshalDelegationDescriptor(e.record, uint64(len(e.pages.from)), uint64(len(e.pages.to)))
	if err != nil {
		return err
	}
	return s.writeSystemDelegation([]byte(e.key), data)
}

func (s *StateDB) deleteDrAccountIndexLegacy(account []byte) error {
	entry, exists, err := s.loadLegacyDelegation(account)
	if err != nil || !exists {
		return err
	}
	for _, dir := range []struct {
		incoming bool
		pages    [][][]byte
	}{{true, entry.pages.from}, {false, entry.pages.to}} {
		for i := range dir.pages {
			if err := s.deleteSystemDelegation(rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, uint64(i))); err != nil {
				return err
			}
		}
	}
	return s.deleteSystemDelegation(rawdb.DrAccountIndexLegacyStateKey(account))
}

func (s *StateDB) writeLegacyDelegationPage(e *legacyDelegationEntry, incoming bool, page int, list [][]byte) error {
	encoded, err := statecodec.MarshalDelegationPage(list)
	if err != nil {
		return err
	}
	account := []byte(e.key)[len(rawdb.DrAccountIndexLegacyStateKey(nil)):]
	return s.writeSystemDelegation(rawdb.DrAccountIndexLegacyPageStateKey(account, incoming, uint64(page)), encoded)
}

// Page reads participate in optimistic transaction dependencies even on cache
// hits; generic page mutation invalidates the complete decoded anchor.
func (s *StateDB) recordLegacyDelegationPageReads(e *legacyDelegationEntry) {
	if s.transactionAccess == nil {
		return
	}
	account := []byte(e.key)[len(rawdb.DrAccountIndexLegacyStateKey(nil)):]
	for _, dir := range []struct {
		incoming bool
		pages    [][][]byte
	}{{true, e.pages.from}, {false, e.pages.to}} {
		for i := range dir.pages {
			s.recordAccountKVRead(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, uint64(i)))
		}
	}
}

// Keep append/remove semantics local to the touched pages. Removing duplicates
// rewrites each page that actually contained the peer and preserves all others.
func (s *StateDB) changeLegacyDelegationPages(e *legacyDelegationEntry, peer []byte, incoming, remove bool) error {
	pages := &e.pages.to
	if incoming {
		pages = &e.pages.from
	}
	if remove {
		changed := make([]int, 0, 1)
		for i, page := range *pages {
			found := false
			for _, address := range page {
				if bytes.Equal(address, peer) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			next := removeDelegationAccount(page, peer)
			clear(page[len(next):])
			(*pages)[i] = next
			changed = append(changed, i)
		}
		// Trailing empties have no order significance. Reclaim them immediately;
		// interior empty pages keep their IDs so unrelated pages never move.
		for len(*pages) > 0 && len((*pages)[len(*pages)-1]) == 0 {
			i := len(*pages) - 1
			account := []byte(e.key)[len(rawdb.DrAccountIndexLegacyStateKey(nil)):]
			if err := s.deleteSystemDelegation(rawdb.DrAccountIndexLegacyPageStateKey(account, incoming, uint64(i))); err != nil {
				return err
			}
			(*pages)[i] = nil
			*pages = (*pages)[:i]
		}
		for _, i := range changed {
			if i < len(*pages) {
				if err := s.writeLegacyDelegationPage(e, incoming, i, (*pages)[i]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if len(*pages) == 0 || len((*pages)[len(*pages)-1]) == statecodec.DelegationPageEntries {
		oldCap := cap(*pages)
		*pages = append(*pages, nil)
		e.charge += (cap(*pages)-oldCap)*24 + 15
	}
	i := len(*pages) - 1
	oldCap := cap((*pages)[i])
	(*pages)[i] = append((*pages)[i], bytes.Clone(peer))
	e.charge += (cap((*pages)[i]) - oldCap) * 24
	return s.writeLegacyDelegationPage(e, incoming, i, (*pages)[i])
}
