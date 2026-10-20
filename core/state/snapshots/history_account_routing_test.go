package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func accountRoutingFixture(t *testing.T, n int) (string, *Manifest, []StateHistoryQuery) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	var queries []StateHistoryQuery
	for i := 0; i < n; i++ {
		var id common.AccountID
		binary.BigEndian.PutUint64(id[12:], uint64(i+1))
		queries = append(queries, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: id.Address(common.AddressPrefixMainnet)})
	}
	var refs []SegmentRef
	for tx := uint64(1); tx <= 3; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		for i, q := range queries {
			if tx != 1 && i%3 != int(tx-1) {
				continue
			}
			row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: uint64(i), FlatDomain: q.FlatDomain, Owner: q.Owner, PrevExists: tx != 1, Prev: []byte{byte(i), byte(tx)}}
			if !row.PrevExists {
				row.Prev = nil
			}
			if err := rawdb.WriteStateDomainChange(db, row); err != nil {
				t.Fatal(err)
			}
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-account-route-%d.seg", tx))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	m := NewManifest(1, 3, refs)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, m, queries
}

func addAccountRoutingFixture(t *testing.T, dir string, m *Manifest, maxNew int) *Manifest {
	t.Helper()
	ref, built, err := BuildAccountHistoryRoutingContext(context.Background(), dir, m, maxNew)
	if err != nil || !built {
		t.Fatalf("account build: %v %v", built, err)
	}
	next, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	return next
}
func mustAccountRoutingRef(t *testing.T, m *Manifest) SegmentRef {
	t.Helper()
	ref, ok := accountRoutingRef(m, 0, ^uint64(0))
	if !ok {
		t.Fatal("missing account route")
	}
	return ref
}

func TestAccountHistoryRoutingPartialCoverageAndCuts(t *testing.T) {
	dir, original, queries := accountRoutingFixture(t, 9)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	queries = append(queries, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 9}}, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVGeneration, Owner: queries[0].Owner})
	m := original
	for pass := 0; pass < 3; pass++ {
		m = addAccountRoutingFixture(t, dir, m, 1)
		r, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		ref := mustAccountRoutingRef(t, m)
		index, err := openAccountHistoryRouting(context.Background(), dir, ref)
		if err != nil {
			t.Fatal(err)
		}
		if len(index.metadata.covered) != pass+1 {
			t.Fatalf("covered %d", len(index.metadata.covered))
		}
		index.Close()
		if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 4; from++ {
			got, err := r.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
			if err != nil {
				t.Fatal(err)
			}
			for i, q := range queries {
				var want *rawdb.StateDomainChange
				err := oracle.IterateStateDomainChangesByKey(from, 4, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[i], want) {
					t.Fatalf("pass=%d from=%d query=%d got=%+v want=%+v err=%v", pass, from, i, got[i], want, err)
				}
			}
		}
	}
	if !reflect.DeepEqual(original.Progress, m.Progress) {
		t.Fatal("account index advanced a frontier")
	}
	if _, built, err := BuildAccountHistoryRoutingContext(context.Background(), dir, m, 64); err != nil || built {
		t.Fatalf("complete coverage rebuilt: %v %v", built, err)
	}
	if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireChecksums: true, RequireRegistered: true}); err != nil {
		t.Fatal(err)
	}
}

func TestAccountHistoryRoutingTwoLevelPagesAndCurrentCorruption(t *testing.T) {
	const count = accountRoutingPageRecords*accountRoutingDirectoryEntries + 17
	dir, m, queries := accountRoutingFixture(t, count)
	m = addAccountRoutingFixture(t, dir, m, 64)
	ref := mustAccountRoutingRef(t, m)
	r, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	if r.metadata.header.directories < 2 {
		t.Fatal("fixture does not cross a directory boundary")
	}
	for i, q := range queries {
		key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, 0, 0, nil)
		bound, ok, err := r.lookup(context.Background(), key)
		want := uint64(1)
		if i%3 == 1 {
			want = 2
		}
		if i%3 == 2 {
			want = 3
		}
		if err != nil || !ok || bound != want {
			t.Fatalf("key %d bound=%d want=%d found=%v err=%v", i, bound, want, ok, err)
		}
	}
	if len(r.pages) > 8 || len(r.directories) > 8 {
		t.Fatal("unbounded request cache")
	}
	h := r.metadata.header
	r.Close()
	path := filepath.Join(dir, ref.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	key := stateDomainChangeBinaryAccessorLookupKey(queries[len(queries)-1].FlatDomain, queries[len(queries)-1].Owner, 0, 0, nil)
	positions := []uint64{accountRoutingHeaderBytes, h.metadataBytes() + accountRoutingPageBytes, h.metadataBytes() + h.directories*accountRoutingPageBytes + (h.pages-1)*accountRoutingPageBytes + 21}
	for _, offset := range positions {
		bad := append([]byte(nil), data...)
		bad[offset] ^= 1
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, stat.ModTime(), stat.ModTime()); err != nil {
			t.Fatal(err)
		}
		current, e := openAccountHistoryRouting(context.Background(), dir, ref)
		if e == nil {
			_, _, e = current.lookup(context.Background(), key)
			current.Close()
		}
		if e == nil {
			t.Fatalf("same-inode/size/mtime corruption at %d accepted", offset)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
}

func TestAccountHistoryRoutingSkipsOnlyBoundCoveredPairs(t *testing.T) {
	dir, m, queries := accountRoutingFixture(t, 6)
	m = addAccountRoutingFixture(t, dir, m, 64)
	// Query zero changed only at tx=1. No file from the later covered pairs is
	// required. A forged pair identity must disable that negative proof.
	for _, ref := range m.Segments {
		if ref.Kind == SegmentHistory || ref.Kind == SegmentAccessor {
			if err := os.Rename(filepath.Join(dir, ref.Path), filepath.Join(dir, ref.Path+".retained")); err != nil {
				t.Fatal(err)
			}
		}
	}
	r, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.FirstStateDomainChangesContext(context.Background(), 2, 3, queries[:1])
	if err != nil || got[0] != nil {
		t.Fatalf("covered absence: %+v %v", got, err)
	}
	stale := cloneManifest(m)
	for i := range stale.Segments {
		if stale.Segments[i].Kind == SegmentAccessor && stale.Segments[i].FromTxNum == 2 {
			stale.Segments[i].Checksum = "sha256:" + fmt.Sprintf("%064x", 1)
		}
	}
	r, err = OpenPinnedManager(dir, stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.FirstStateDomainChangesContext(context.Background(), 2, 3, queries[:1]); err == nil {
		t.Fatal("stale coverage suppressed required fallback")
	}
}

func TestAccountHistoryRoutingReplacementKeepsConservativeMaximum(t *testing.T) {
	dir, m, queries := accountRoutingFixture(t, 6)
	m = addAccountRoutingFixture(t, dir, m, 64)
	q := queries[2] // Existing upper bound is 3; replacement source ends at 2.
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{2}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 2, BlockHash: common.Hash{2}, TxNum: 2, FlatDomain: q.FlatDomain, Owner: q.Owner, PrevExists: true, Prev: []byte("new-middle")}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 2, 2, "history/state-domain-change-account-route-replacement.seg")
	if err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(2, 2, refs)
	if err != nil {
		t.Fatal(err)
	}
	m = addAccountRoutingFixture(t, dir, m, 64)
	ref := mustAccountRoutingRef(t, m)
	index, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, 0, 0, nil)
	bound, found, err := index.lookup(context.Background(), key)
	if err != nil || !found || bound != 3 {
		t.Fatalf("maximum lowered: %d %v %v", bound, found, err)
	}
	r, _ := OpenPinnedManager(dir, m)
	got, err := r.FirstStateDomainChangesContext(context.Background(), 3, 3, []StateHistoryQuery{q})
	if err != nil || got[0] == nil || got[0].TxNum != 3 {
		t.Fatalf("later row hidden: %+v %v", got, err)
	}
	if err := CheckAccountHistoryRouting(dir, ref); err != nil {
		t.Fatal(err)
	}
}

func TestAccountHistoryRoutingFDRangeBoundsAndCancel(t *testing.T) {
	dir, m, queries := accountRoutingFixture(t, 3)
	m = addAccountRoutingFixture(t, dir, m, 64)
	ref := mustAccountRoutingRef(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := BuildAccountHistoryRoutingContext(ctx, dir, m, 64); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	key := stateDomainChangeBinaryAccessorLookupKey(queries[0].FlatDomain, queries[0].Owner, 0, 0, nil)
	if _, _, err := r.lookup(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ref.Path)
	moved := path + ".moved"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, ref.Size), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.lookup(context.Background(), key); err != nil || !ok {
		t.Fatalf("reader reopened replacement path: %v %v", ok, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.file != nil {
		t.Fatal("reader retained FD")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openAccountHistoryRouting(context.Background(), dir, ref); err == nil {
		t.Fatal("bad replacement accepted")
	}
	reader, _ := OpenPinnedManager(dir, m)
	if _, err := reader.FirstStateDomainChangesContext(context.Background(), 4, 5, queries); err != nil {
		t.Fatalf("unused range opened index: %v", err)
	}
	data, err := os.ReadFile(moved)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{12, 16, 24, 32} {
		bad := append([]byte(nil), data[:accountRoutingHeaderBytes]...)
		for i := offset; i < offset+4; i++ {
			bad[i] = 255
		}
		if _, err := decodeAccountRoutingHeader(bad, ref); err == nil {
			t.Fatalf("bad count at %d accepted", offset)
		}
	}
}

func TestAccountHistoryRoutingStrictCheckRejectsUnderestimatedBound(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 3)
	m = addAccountRoutingFixture(t, dir, m, 64)
	ref := mustAccountRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeAccountRoutingHeader(data[:accountRoutingHeaderBytes], ref)
	if err != nil {
		t.Fatal(err)
	}
	dataOffset := h.metadataBytes() + h.directories*accountRoutingPageBytes
	// Lower the last account's bound from 3 to 1, then recompute every physical
	// authentication layer. Only the authoritative dictionary check can reject it.
	binary.BigEndian.PutUint64(data[dataOffset+2*accountRoutingRecordBytes+21:], 1)
	sum := sha256.Sum256(data[dataOffset : dataOffset+accountRoutingPageBytes])
	copy(data[h.metadataBytes()+21:h.metadataBytes()+53], sum[:])
	dirSum := sha256.Sum256(data[h.metadataBytes() : h.metadataBytes()+accountRoutingPageBytes])
	rootEntry := accountRoutingHeaderBytes + uint64(h.sources)*32
	copy(data[rootEntry+21:rootEntry+53], dirSum[:])
	root := sha256.Sum256(data[:h.metadataBytes()])
	ref.LookupRoot = fmt.Sprintf("sha256:%x", root)
	ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
		t.Fatal("physically valid false absence accepted")
	}
}

func TestAccountHistoryRoutingGenerationAndOwnerAliases(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 7}
	alias := owner
	alias[0] = 0xa0
	var refs []SegmentRef
	for tx, address := range []common.Address{owner, alias} {
		num := uint64(tx + 1)
		if err := rawdb.WriteStateTxRange(db, num, common.Hash{byte(num)}, num, num); err != nil {
			t.Fatal(err)
		}
		for seq, flat := range []rawdb.StateFlatDomain{rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration} {
			row := &rawdb.StateDomainChange{BlockNum: num, BlockHash: common.Hash{byte(num)}, TxNum: num, Seq: uint64(seq), FlatDomain: flat, Owner: address, PrevExists: true, Prev: []byte{byte(num)}}
			if flat == rawdb.StateFlatDomainKVGeneration {
				row.Prev = binary.BigEndian.AppendUint64(nil, num)
			}
			if err := rawdb.WriteStateDomainChange(db, row); err != nil {
				t.Fatal(err)
			}
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, num, num, fmt.Sprintf("history/state-domain-change-account-alias-%d.seg", num))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	original := NewManifest(1, 2, refs)
	if err := PublishManifest(dir, original); err != nil {
		t.Fatal(err)
	}
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	m := addAccountRoutingFixture(t, dir, original, 64)
	r, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []common.Address{owner, alias} {
		for _, flat := range []rawdb.StateFlatDomain{rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration} {
			for from := uint64(1); from <= 3; from++ {
				q := StateHistoryQuery{FlatDomain: flat, Owner: address}
				got, err := r.FirstStateDomainChangesContext(context.Background(), from, 3, []StateHistoryQuery{q})
				if err != nil {
					t.Fatal(err)
				}
				var want *rawdb.StateDomainChange
				err = oracle.IterateStateDomainChangesByKey(from, 3, flat, address, 0, 0, nil, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[0], want) {
					t.Fatalf("flat=%d owner=%x from=%d got=%+v want=%+v", flat, address, from, got[0], want)
				}
			}
		}
	}
}

func TestAccountHistoryRoutingMergeOrder(t *testing.T) {
	var key [21]byte
	key[0] = byte(rawdb.StateFlatDomainAccountLatest)
	var out []byte
	var bound uint64
	w := &accountRoutingMerge{ctx: context.Background(), emit: func(k []byte, v uint64) error { out = append([]byte(nil), k...); bound = v; return nil }}
	for _, v := range []uint64{2, 4, 7} {
		raw := append([]byte(nil), key[:]...)
		raw = binary.BigEndian.AppendUint64(raw, v)
		if err := w.Put(raw, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.finish(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, key[:]) || bound != 7 {
		t.Fatalf("group max %x/%d", out, bound)
	}
}

func TestAccountHistoryRoutingRejectsNonSHA256MetadataRoot(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 3)
	m = addAccountRoutingFixture(t, dir, m, 64)
	ref := mustAccountRoutingRef(t, m)
	for _, invalid := range []string{"", "sha256:0011223344556677", "00112233445566778899001122334455667788990011223344556677889900112233", "sha256:" + fmt.Sprintf("%066x", 1)} {
		ref.LookupRoot = invalid
		if err := validateSegmentRef(ref); err == nil {
			t.Fatalf("invalid metadata root accepted: %q", invalid)
		}
	}
}
