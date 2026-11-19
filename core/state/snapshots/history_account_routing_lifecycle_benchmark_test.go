package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func benchmarkRouteKey(n uint64) (key [21]byte, owner common.Address) {
	key[0] = byte(rawdb.StateFlatDomainAccountLatest)
	binary.BigEndian.PutUint64(key[13:], n)
	owner = common.Address(key)
	owner[0] = common.AddressPrefixMainnet
	return key, owner
}

// Build an authenticated old routing object with conservative extra rows.
// Small canonical sources keep fixture preparation bounded. Every covered
// canonical key still has its real upper bound and strict verification checks
// the complete authenticated object, including the extra old rows.
func benchmarkOldRoute(b *testing.B, dir string, refs []SegmentRef, count uint64) SegmentRef {
	b.Helper()
	var pages, directories bytes.Buffer
	w := &accountRoutingPageWriter{
		data: bufio.NewWriter(&pages), directories: bufio.NewWriter(&directories), maxTx: count,
	}
	for n := uint64(1); n <= 1_000_000; n++ {
		key, _ := benchmarkRouteKey(n)
		bound := min(n, count)
		if err := w.put(key[:], bound); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.finish(); err != nil {
		b.Fatal(err)
	}
	header := make([]byte, accountRoutingHeaderBytes)
	copy(header, accountRoutingMagic[:])
	binary.BigEndian.PutUint32(header[8:], 1)
	binary.BigEndian.PutUint32(header[12:], uint32(count))
	binary.BigEndian.PutUint64(header[16:], w.rows)
	binary.BigEndian.PutUint64(header[24:], w.pages)
	binary.BigEndian.PutUint64(header[32:], w.dirPages)
	binary.BigEndian.PutUint64(header[40:], 1)
	binary.BigEndian.PutUint64(header[48:], count)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	m := manifestLookupView(NewManifest(1, count, refs))
	for _, history := range m.Segments {
		if history.Kind != SegmentHistory {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(m, history)
		if !ok {
			b.Fatal("missing lifecycle fixture accessor")
		}
		id := systemHistoryRoutingPairID(history, accessor)
		header = append(header, id[:]...)
	}
	header = append(header, w.roots...)
	root := sha256.Sum256(header)
	data := append(header, directories.Bytes()...)
	data = append(data, pages.Bytes()...)
	hash := sha256.Sum256(data)
	ref := SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRouting,
		Rows: w.rows, FromTxNum: 1, ToTxNum: count, Size: uint64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", hash), LookupRoot: fmt.Sprintf("sha256:%x", root),
		Path: fmt.Sprintf("history/account-routing-1-%d-%x.route", count, hash),
	}
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0644); err != nil {
		b.Fatal(err)
	}
	return ref
}

func benchmarkAccountRouteSourceFixture(b *testing.B) (dir string, refs [][]SegmentRef) {
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	b.Setenv(manifestCacheBudgetEnv, "805306368")
	b.Cleanup(loadedManifestCache.clear)
	dir = b.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	const first, last uint64 = 512, 640
	refs = make([][]SegmentRef, last+1)
	for tx := uint64(1); tx <= last; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			b.Fatal(err)
		}
		ownerID := tx
		if tx > first && tx%2 == 0 {
			ownerID = tx - first // Repeat keys across the old and new coverage.
		}
		_, owner := benchmarkRouteKey(ownerID)
		row := &rawdb.StateDomainChange{
			BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx,
			FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner,
		}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			b.Fatal(err)
		}
		var err error
		refs[tx], err = BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-account-coalesce-%d.seg", tx))
		if err != nil {
			b.Fatal(err)
		}
	}
	return dir, refs
}

func BenchmarkAccountRoutingCoalescedLifecycle(b *testing.B) {
	ctx := context.Background()
	dir, refs := benchmarkAccountRouteSourceFixture(b)
	const first, middle, last uint64 = 512, 576, 640
	var oldRefs []SegmentRef
	for _, group := range refs[1 : first+1] {
		oldRefs = append(oldRefs, group...)
	}
	oldRoute := benchmarkOldRoute(b, dir, oldRefs, first)
	oldRefs = append(oldRefs, oldRoute)
	aggregate := NewAggregator(dir)
	var rewritten uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := PublishManifest(dir, NewManifest(1, first, oldRefs)); err != nil {
			b.Fatal(err)
		}
		start := first + 1
		for _, end := range []uint64{middle, last} {
			var next []SegmentRef
			for _, group := range refs[start : end+1] {
				next = append(next, group...)
			}
			if _, err := aggregate.Integrate(1, end, next); err != nil {
				b.Fatal(err)
			}
			built, err := refreshAccountHistoryRoutingContext(ctx, dir, true)
			if err != nil {
				b.Fatal(err)
			}
			if built {
				rewritten++
			}
			loadedManifestCache.clear()
			manager, err := OpenManager(dir)
			if err != nil {
				b.Fatal(err)
			}
			_, repeated := benchmarkRouteKey(end - first)
			_, uncovered := benchmarkRouteKey(end - 1)
			_, absent := benchmarkRouteKey(2_000_000)
			queries := []StateHistoryQuery{
				{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: repeated},
				{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: uncovered},
				{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: absent},
			}
			rows, err := manager.FirstStateDomainChangesContext(ctx, start, end, queries)
			if err != nil || rows[0] == nil || rows[0].TxNum != end || rows[1] == nil || rows[1].TxNum != end-1 || rows[2] != nil {
				b.Fatalf("round %d query mismatch: %v %v", end, rows, err)
			}
			m := manager.Manifest()
			ref, ok := accountRoutingRef(m, 0, ^uint64(0))
			if !ok {
				b.Fatal("route absent after reopen")
			}
			if err := CheckAccountHistoryRouting(dir, ref); err != nil {
				b.Fatal(err)
			}
			start = end + 1
		}
	}
	b.ReportMetric(float64(rewritten)/float64(b.N), "route-rewrites/op")
}

func BenchmarkAccountRoutingBacklogLifecycle(b *testing.B) {
	ctx := context.Background()
	dir, refs := benchmarkAccountRouteSourceFixture(b)
	const first, last uint64 = 256, 640
	var oldRefs, next []SegmentRef
	for _, group := range refs[1 : first+1] {
		oldRefs = append(oldRefs, group...)
	}
	for _, group := range refs[first+1 : last+1] {
		next = append(next, group...)
	}
	oldRefs = append(oldRefs, benchmarkOldRoute(b, dir, oldRefs, first))
	aggregate := NewAggregator(dir)
	var rewrites, routeBytes uint64
	var routeMax, queryElapsed time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := PublishManifest(dir, NewManifest(1, first, oldRefs)); err != nil {
			b.Fatal(err)
		}
		if _, err := aggregate.Integrate(1, last, next); err != nil {
			b.Fatal(err)
		}
		var ref SegmentRef
		for step := 0; ; step++ {
			if step == 2 {
				b.Fatal("insufficient routing coverage after two builds")
			}
			started := time.Now()
			built, err := refreshAccountHistoryRoutingContext(ctx, dir, true)
			routeMax = max(routeMax, time.Since(started))
			if err != nil || !built {
				b.Fatalf("backlogged route refresh: %t %v", built, err)
			}
			rewrites++
			m, err := loadProductionManifestReadOnly(dir)
			if err != nil {
				b.Fatal(err)
			}
			ref, _ = accountRoutingRef(m, 0, ^uint64(0))
			info, err := os.Stat(filepath.Join(dir, ref.Path))
			if err != nil {
				b.Fatal(err)
			}
			routeBytes += uint64(info.Size())
			r, err := openAccountHistoryRouting(ctx, dir, ref)
			if err != nil {
				b.Fatal(err)
			}
			covered := len(r.metadata.covered)
			r.Close()
			if covered == int(first+128) {
				break
			}
		}
		if err := CheckAccountHistoryRouting(dir, ref); err != nil {
			b.Fatal(err)
		}
		loadedManifestCache.clear()
		manager, err := OpenManager(dir)
		if err != nil {
			b.Fatal(err)
		}
		_, covered := benchmarkRouteKey(first + 128)
		_, uncovered := benchmarkRouteKey(last - 1)
		_, absent := benchmarkRouteKey(2_000_000)
		queries := []StateHistoryQuery{
			{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: covered},
			{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: uncovered},
			{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: absent},
		}
		started := time.Now()
		rows, err := manager.FirstStateDomainChangesContext(ctx, first+1, last, queries)
		queryElapsed += time.Since(started)
		if err != nil || rows[0] == nil || rows[0].TxNum != first+128 || rows[1] == nil || rows[1].TxNum != last-1 || rows[2] != nil {
			b.Fatalf("backlogged route query mismatch: %v %v", rows, err)
		}
	}
	b.ReportMetric(float64(rewrites)/float64(b.N), "route-rewrites/op")
	b.ReportMetric(128, "new-covered-sources/op")
	b.ReportMetric(128, "new-dictionary-keys/op")
	b.ReportMetric(float64(routeBytes)/float64(b.N), "route-output-B/op")
	b.ReportMetric(float64(routeMax.Nanoseconds()), "max-refresh-ns")
	b.ReportMetric(float64(queryElapsed.Nanoseconds())/float64(b.N), "query-ns/op")
}
