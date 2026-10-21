package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func storageRoutingFixture(t *testing.T) (string, *Manifest, []StateHistoryQuery) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 9}
	var refs []SegmentRef
	for tx := uint64(1); tx <= 3; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		for slot := 0; slot < 3; slot++ {
			who, gen := owner, uint64(0)
			if tx == 1 {
				who = common.Address{0x41, 8}
			}
			if tx == 3 {
				gen = 1
			}
			key := make([]byte, 32)
			key[31] = byte(slot)
			row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: uint64(slot), FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: who, Generation: gen, Domain: kvdomains.ContractStorage, Key: key, PrevExists: true, Prev: []byte{byte(tx), byte(slot)}}
			if err := rawdb.WriteStateDomainChange(db, row); err != nil {
				t.Fatal(err)
			}
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-storage-route-%d.seg", tx))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	m := NewManifest(1, 3, refs)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	var queries []StateHistoryQuery
	for gen := uint64(0); gen < 3; gen++ {
		for slot := 0; slot < 4; slot++ {
			key := make([]byte, 32)
			key[31] = byte(slot)
			queries = append(queries, StateHistoryQuery{rawdb.StateFlatDomainKVLatest, owner, gen, kvdomains.ContractStorage, key})
		}
	}
	alias := queries[0]
	alias.Owner[0] = 0
	queries = append(queries, alias)
	return dir, m, queries
}

func TestStorageHistoryRoutingMembershipPartialLegacyAndUpgrade(t *testing.T) {
	dir, original, queries := storageRoutingFixture(t)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	check := func(m *Manifest) {
		t.Helper()
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 4; from++ {
			got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries[:len(queries)-1])
			if err != nil {
				t.Fatal(err)
			}
			// Aliased full owners deliberately use separate demand reads; a mixed
			// batch rejects their identical encoded lookup identities.
			alias, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries[len(queries)-1:])
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, alias...)
			for i, q := range queries {
				var want *rawdb.StateDomainChange
				err := oracle.IterateStateDomainChangesByKey(from, 4, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[i], want) {
					t.Fatalf("from %d query %d mismatch: %v", from, i, err)
				}
			}
		}
		if err := checkSystemHistoryRoutingContext(context.Background(), dir, m, mustRoutingRef(t, m)); err != nil {
			t.Fatal(err)
		}
	}
	m := original
	for pass := 0; pass < 3; pass++ {
		m = addRoutingFixture(t, dir, m, 1)
		check(m)
	}
	route, err := loadSystemHistoryRouting(context.Background(), dir, m)
	if err != nil || !route.storagePrefixes || len(route.keys) != 3 {
		t.Fatal("slots were not deduplicated by owner/generation/domain", err)
	}
	lookup := stateDomainChangeBinaryAccessorLookupKey(queries[0].FlatDomain, queries[0].Owner, queries[0].Generation, queries[0].Domain, queries[0].Key)
	if !bytes.Equal(normalizedHistoryRoutingKey(lookup), lookup[:31]) {
		t.Fatal("wrong storage prefix")
	}
	var yes, no int
	for i := range route.sources {
		if route.mayContain(i, lookup) {
			yes++
		} else {
			no++
		}
	}
	if yes != 1 || no != 2 {
		t.Fatal("membership mixed owners or generations", yes, no)
	}
	// Create the exact older encoding: same authenticated source identities,
	// but no storage family claim. An absent V1 entry cannot exclude storage.
	ref := mustRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	legacy := append([]byte(nil), data[:systemHistoryRoutingHeaderSize+len(route.sources)*32]...)
	binary.BigEndian.PutUint32(legacy[8:12], 1)
	binary.BigEndian.PutUint32(legacy[16:20], 0)
	ref.Rows, ref.Size = 0, uint64(len(legacy))
	sum := sha256.Sum256(legacy)
	ref.Checksum = fmt.Sprintf("sha256:%x", sum)
	ref.Path = fmt.Sprintf("history/system-routing-%d-%d-%x.route", ref.FromTxNum, ref.ToTxNum, sum)
	if err := os.WriteFile(filepath.Join(dir, ref.Path), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(1, 3, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	check(m)
	old, err := loadSystemHistoryRouting(context.Background(), dir, m)
	if err != nil || old.storagePrefixes || !old.mayContain(0, lookup) {
		t.Fatal("V1 falsely proved storage absent", err)
	}
	// The first upgrade batch can attest only one freshly checked dictionary;
	// it must not silently reuse all three V1 source columns as V2 coverage.
	for pass := 0; pass < 3; pass++ {
		m = addRoutingFixture(t, dir, m, 1)
		r, err := loadSystemHistoryRouting(context.Background(), dir, m)
		if err != nil || len(r.sources) != pass+1 || !r.storagePrefixes {
			t.Fatal("unverified V1 coverage reused", err)
		}
		check(m)
	}
	if !reflect.DeepEqual(original.Progress, m.Progress) {
		t.Fatal("auxiliary route changed history frontiers")
	}
}
