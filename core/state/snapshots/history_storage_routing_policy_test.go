package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// A real legacy V2 trio remains queryable but has no authenticated V6 key
// dictionary. Runtime routing must rotate past it without asserting absence.
func storageRuntimeLegacySource(t *testing.T, dir string, tx uint64) []SegmentRef {
	t.Helper()
	changes := []*rawdb.StateDomainChange{binaryStateDomainChange(tx, tx, 1, "legacy")}
	data, index, entries := encodeStateDomainChangeBinarySegmentV2IndexesForTest(t, tx, tx, changes, nil)
	idx, err := encodeStateDomainChangeBinaryIndex(tx, tx, index)
	if err != nil {
		t.Fatal(err)
	}
	accessor, err := encodeStateDomainChangeBinaryAccessorV2ForTest(tx, tx, entries)
	if err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("history/state-domain-change-runtime-legacy-%d.seg", tx)
	var refs []SegmentRef
	for _, file := range []struct {
		kind SegmentKind
		path string
		data []byte
	}{{SegmentHistory, path, data}, {SegmentAccessor, stateDomainChangeBinaryAccessorPath(path), accessor}, {SegmentInverted, stateDomainChangeBinaryIndexPath(path), idx}} {
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: file.kind, FromTxNum: tx, ToTxNum: tx, Path: file.path}
		setStateDomainChangeBinaryRefMetadata(&ref, file.data)
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, ref.Path), file.data); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, refs[0], refs[2], refs[1]); err != nil {
		t.Fatal(err)
	}
	return refs
}

func storageRuntimeModernSource(t *testing.T, dir string, tx uint64, key string) []SegmentRef {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
		t.Fatal(err)
	}
	row := binaryStateDomainChange(tx, tx, 1, key)
	if err := rawdb.WriteStateDomainChange(db, row); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-runtime-modern-%d-%s.seg", tx, key))
	if err != nil {
		t.Fatal(err)
	}
	return refs
}

func TestStorageRoutingRuntimeReusesBroadAndRepairsInteriorHole(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	m = addStorageFilterFixture(t, dir, m, 1)
	m = replaceMetadataRepairFixtureSources(t, dir, 2, 2)
	// The old broad table still spans 1..3, but its tx2 pair is obsolete.
	// Unchanged sources must not be opened, and tx2 must not be skipped by
	// admitting only heights above the old broad frontier.
	for _, r := range m.Segments {
		if r.Kind == SegmentAccessor && r.FromTxNum != 2 {
			p := filepath.Join(dir, r.Path)
			if err := os.Rename(p, p+".held"); err != nil {
				t.Fatal(err)
			}
			defer os.Rename(p+".held", p)
		}
	}
	s := storageRoutingRefreshState{}
	now := time.Unix(5000, 0)
	if built, err := refreshStorageHistoryRoutingContext(context.Background(), dir, &s, now); err != nil || !built || s.last.sourceReads != 1 || s.last.broadReads != 1 {
		t.Fatal("missed interior hole or reread covered sources", built, s.last, err)
	}
	next, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(m.Progress, next.Progress) {
		t.Fatal("auxiliary publication changed frontiers", err)
	}
	reader := newStorageHistoryRoutingReader(next)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range next.Segments {
		if h.Kind == SegmentHistory && h.FromTxNum == 2 {
			a, _ := cfg.HistoryAccessorRef(next, h)
			if _, covered, err := reader.source(context.Background(), dir, h, systemHistoryRoutingPairID(h, a)); err != nil || !covered {
				t.Fatal("new exact pair not published", covered, err)
			}
		}
	}
	// Maintenance ID hints may be reused without a second broad decode, but
	// they are never handed to query readers. Current queries still authenticate.
	broad := mustRoutingRef(t, next)
	path := filepath.Join(dir, broad.Path)
	if err := os.Rename(path, path+".held"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(path+".held", path)
	if built, err := refreshStorageHistoryRoutingContext(context.Background(), dir, &s, now.Add(time.Second)); err != nil || built || s.last.sourceReads != 0 || s.last.broadReads != 0 || s.notBefore.IsZero() {
		t.Fatal("completed cursor reopened immutable sources", built, s.last, err)
	}
	otherDir, other, _ := routingFixture(t)
	other = addStorageFilterFixture(t, otherDir, other, 1)
	_ = addRoutingFixture(t, otherDir, other, 1)
	if built, err := refreshStorageHistoryRoutingContext(context.Background(), otherDir, &s, now.Add(2*time.Second)); err != nil || !built || s.last.broadReads != 1 || s.last.sourceReads != 2 {
		t.Fatal("another directory inherited broad hints or idle backoff", built, s.last, err)
	}
}

func TestStorageRoutingRuntimeUnsupportedTailAndEOFBackoff(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addStorageFilterFixture(t, dir, m, 1)
	refs := storageRuntimeLegacySource(t, dir, 1)
	m, err := NewAggregator(dir).Integrate(1, 3, refs)
	if err != nil {
		t.Fatal(err)
	}
	s := storageRoutingRefreshState{}
	now := time.Unix(6000, 0)
	refresh := func(at time.Time) (bool, error) {
		return refreshStorageHistoryRoutingWithBudget(context.Background(), dir, &s, at, 1, smallSystemRoutingWorkLimit{bytes: 1})
	}
	if built, err := refresh(now); err != nil || built || s.nextTx != 2 || s.last.unsupported != 1 || !s.holes || s.notBefore.Sub(now) < time.Minute {
		t.Fatal("unsupported head did not advance a bounded scan", built, s, err)
	}
	for _, wantNext := range []uint64{3, 4} {
		if built, err := refresh(s.notBefore); err != nil || !built || s.nextTx != wantNext || s.last.sourceReads != 1 {
			t.Fatal("unsupported head starved valid tail", built, wantNext, s, err)
		}
	}
	at := s.notBefore
	if at.IsZero() {
		at = now.Add(5 * time.Minute)
	}
	if built, err := refresh(at); err != nil || built || s.nextTx != 0 || !s.holes || s.notBefore.Sub(at) < time.Minute {
		t.Fatal("EOF incorrectly converted a remaining hole to 30s idle", built, s, err)
	}
	// Appending a real trio cannot bypass failed-prefix recovery. This call
	// must not read even the now-absent original canonical accessors.
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var moved []string
	for _, r := range current.Segments {
		if r.Kind == SegmentAccessor {
			p := filepath.Join(dir, r.Path)
			if err := os.Rename(p, p+".held"); err != nil {
				t.Fatal(err)
			}
			moved = append(moved, p)
		}
	}
	tail := writeCompactionStateDomainChangeSegment(t, dir, 4, 4, binaryStateDomainChange(4, 4, 1, "tail"))
	if _, err := NewAggregator(dir).Integrate(1, 4, tail); err != nil {
		t.Fatal(err)
	}
	if built, err := refresh(at.Add(time.Second)); err != nil || built || s.last.sourceReads != 0 || s.last.broadReads != 0 {
		t.Fatal("append defeated backoff", built, s.last, err)
	}
	for _, p := range moved {
		if err := os.Rename(p+".held", p); err != nil {
			t.Fatal(err)
		}
	}
	_ = replaceMetadataRepairFixtureSources(t, dir, 1, 1)
	if built, err := refresh(at.Add(2 * time.Second)); err != nil || !built || s.last.sourceReads != 1 || s.nextTx != 2 {
		t.Fatal("prefix replacement did not cancel stale backoff", built, s, err)
	}
}

func TestStorageRoutingRuntimeConflictAndErrorKeepCursor(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addStorageFilterFixture(t, dir, m, 1)
	replacement := storageRuntimeModernSource(t, dir, 2, "replacement")
	var active []SegmentRef
	for _, r := range m.Segments {
		if r.Kind == SegmentHistory || r.Kind == SegmentAccessor || r.Kind == SegmentInverted {
			if r.FromTxNum == 2 {
				continue
			}
		}
		active = append(active, r)
	}
	changed := NewManifest(1, 3, append(active, replacement...))
	before, _ := filepath.Glob(filepath.Join(dir, "history/storage-routing-*.route"))
	manifestPublicationMu.Lock()
	locked := true
	defer func() {
		if locked {
			manifestPublicationMu.Unlock()
		}
	}()
	s := storageRoutingRefreshState{}
	type result struct {
		built bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		built, err := refreshStorageHistoryRoutingWithBudget(context.Background(), dir, &s, time.Unix(7000, 0), 1, smallSystemRoutingWorkLimit{})
		done <- result{built, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(dir, "history/storage-routing-*.route"))
		if len(files) > len(before) {
			break // Candidate complete; guarded publisher is blocked on our lock.
		}
		if time.Now().After(deadline) {
			t.Fatal("routing candidate did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if err := publishManifestLocked(dir, changed); err != nil {
		t.Fatal(err)
	}
	manifestPublicationMu.Unlock()
	locked = false
	r := <-done
	if r.err != nil || r.built || s.nextTx != 0 {
		t.Fatal("conflicting publication advanced cursor", r, s.nextTx)
	}
	if built, err := refreshStorageHistoryRoutingWithBudget(context.Background(), dir, &s, time.Unix(7001, 0), 1, smallSystemRoutingWorkLimit{}); err != nil || !built || s.nextTx != 3 {
		t.Fatal("conflict did not retry the same source", built, s.nextTx, err)
	}
	s.nextTx = 3
	latest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range latest.Segments {
		if ref.Kind == SegmentAccessor && ref.FromTxNum == 3 {
			p := filepath.Join(dir, ref.Path)
			if err := os.Rename(p, p+".held"); err != nil {
				t.Fatal(err)
			}
			built, readErr := refreshStorageHistoryRoutingContext(context.Background(), dir, &s, time.Unix(7002, 0))
			if err := os.Rename(p+".held", p); err != nil {
				t.Fatal(err)
			}
			if built || readErr == nil || s.nextTx != 3 {
				t.Fatal("source failure advanced cursor", built, s.nextTx, readErr)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if built, err := refreshStorageHistoryRoutingContext(ctx, dir, &s, time.Unix(7002, 0)); built || !errors.Is(err, context.Canceled) || s.nextTx != 3 {
		t.Fatal("cancellation advanced cursor", built, s.nextTx, err)
	}
}

func TestHistoryRoutingMaintenanceReservesCanonicalAndGateKeepsTurn(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	m.Progress = &Progress{HistoryBuildTxNum: 70, AccessorBuildTxNum: 70}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: gate})
	for i := 0; i < 3; i++ {
		result, err := r.compactHistory(context.Background(), false)
		if err != nil || !result.RoutingBuilt || result.Merged {
			t.Fatal("lightweight family lost its opportunity", i, result, err)
		}
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []SegmentKind{SegmentSmallSystemHistoryRouting, SegmentMetadataHistoryRouting, SegmentStorageHistoryRouting} {
		found := false
		for _, ref := range current.Segments {
			found = found || ref.Kind == kind
		}
		if !found {
			t.Fatal("family starved", kind)
		}
	}
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("could not hold gate")
	}
	result, err := r.compactHistory(context.Background(), false)
	release()
	if err != nil || !result.Deferred || r.compactionBudget.routingWorkSinceMerge != 3 {
		t.Fatal("gate rejection consumed canonical reservation", result, err)
	}
	result, err = r.compactHistory(context.Background(), false)
	if err != nil || !result.Merged || result.RoutingBuilt || result.MergePasses != 1 || result.InputSources > busyHistoryCompactionSources || result.InputBytes > busyHistoryCompactionInputBytes || result.InputLogicalBytes > busyHistoryCompactionLogicalBytes || result.InputRecords > busyHistoryCompactionInputRecords || r.compactionBudget.routingWorkSinceMerge != 0 {
		t.Fatal("canonical merge starved or escaped reserved bounds", result, err)
	}
	current, err = LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(m.Progress, current.Progress) {
		t.Fatal("maintenance changed cold frontiers", m.Progress, current.Progress, err)
	}
	manager, err := OpenPinnedManager(dir, current)
	if err != nil {
		t.Fatal(err)
	}
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Domain: kvdomains.SystemDynamicProperty, Key: []byte("future-1")}
	got, err := manager.FirstStateDomainChangesContext(context.Background(), 1, 70, []StateHistoryQuery{q})
	if err != nil || got[0] == nil || got[0].TxNum != 1 || !reflect.DeepEqual(got[0].Prev, []byte{1}) {
		t.Fatal("reserved merge changed history result", got, err)
	}
}

func TestStorageRoutingRuntimeNoOutputEndsHeavyOpportunity(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addAccountRoutingFixture(t, dir, m, 3)
	m = addStorageFilterFixture(t, dir, m, 1)
	for tx := uint64(1); tx <= 3; tx++ {
		refs := storageRuntimeLegacySource(t, dir, tx)
		var err error
		m, err = NewAggregator(dir).Integrate(1, 3, refs)
		if err != nil {
			t.Fatal(err)
		}
	}
	// A stacked account task would fail on this missing active object.
	account := mustAccountRoutingRef(t, m)
	p := filepath.Join(dir, account.Path)
	if err := os.Rename(p, p+".held"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(p+".held", p)
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, MetricsNamespace: "test/" + t.Name(), HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	r.compactionBudget.nextLightweightRoute = 2
	result, err := r.compactHistory(context.Background(), false)
	if err != nil || !result.Deferred || result.DeferReason != "storage-routing-work" || r.compactionBudget.storageRouting.last.sourceReads != 3 || r.compactionBudget.systemRouting.metric != nil || r.compactionBudget.routingWorkSinceMerge != 1 {
		t.Fatal("no-output budget stacked additional heavy work", result, err)
	}
}
