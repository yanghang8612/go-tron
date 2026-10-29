package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestAccountRoutingRuntimeAppendsWithoutRewritingBase(t *testing.T) {
	dir, m, qs := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	base := mustAccountRoutingRef(t, m)
	before, err := os.ReadFile(filepath.Join(dir, base.Path))
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	s := accountRoutingRefreshState{}
	now := time.Unix(12000, 0)
	for pass := 0; pass < 2; pass++ {
		built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, now.Add(time.Duration(pass)*time.Second), 1, smallSystemRoutingWorkLimit{})
		if err != nil || !built || s.last.sourceReads != 1 || s.last.sources != 1 || !s.last.didWork() {
			t.Fatal("bounded delta did not advance", built, s, err)
		}
	}
	current, err := LoadProductionManifest(dir)
	if err != nil || mustAccountRoutingRef(t, current) != base || !reflect.DeepEqual(m.Progress, current.Progress) {
		t.Fatal("changed base or canonical frontiers", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, base.Path))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rewrote old base bytes", err)
	}
	manager, err := OpenPinnedManager(dir, current)
	if err != nil {
		t.Fatal(err)
	}
	for from := uint64(1); from <= 3; from++ {
		got, err := manager.FirstStateDomainChangesContext(context.Background(), from, 3, qs)
		want, wantErr := oracle.FirstStateDomainChangesContext(context.Background(), from, 3, qs)
		if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("runtime publication changed historical result", from, got, want, err, wantErr)
		}
	}
	if s.nextTx != 0 || s.holes || s.notBefore.Sub(now) < accountRoutingIdleRetry {
		t.Fatal("finished coverage did not enter idle backoff", s)
	}
	// Backoff must be decided before authenticating old metadata again.
	if err := os.Rename(filepath.Join(dir, base.Path), filepath.Join(dir, base.Path)+".held"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(filepath.Join(dir, base.Path)+".held", filepath.Join(dir, base.Path))
	if built, err := refreshAccountHistoryDeltaContext(context.Background(), dir, &s, now.Add(2*time.Second)); err != nil || built || s.last.didWork() {
		t.Fatal("idle maintenance reread immutable metadata", built, s.last, err)
	}
}

func TestAccountRoutingRuntimeUnsupportedPrefixTailAndBackoff(t *testing.T) {
	dir, m, qs := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	if _, err := NewAggregator(dir).Integrate(1, 3, storageRuntimeLegacySource(t, dir, 1)); err != nil {
		t.Fatal(err)
	}
	s := accountRoutingRefreshState{}
	now := time.Unix(13000, 0)
	refresh := func(at time.Time) (bool, error) {
		return refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, at, 1, smallSystemRoutingWorkLimit{})
	}
	if built, err := refresh(now); err != nil || built || s.nextTx != 2 || s.last.unsupported != 1 || !s.last.didWork() || !s.holes || s.notBefore.Sub(now) < time.Minute {
		t.Fatal("unsupported head pinned cursor or omitted work", built, s, err)
	}
	at := s.notBefore
	if built, err := refresh(at); err != nil || !built || s.nextTx != 3 || s.last.sources != 1 || !s.holes {
		t.Fatal("unsupported prefix starved valid tail", built, s, err)
	}
	if built, err := refresh(at.Add(time.Second)); err != nil || !built || s.nextTx != 0 || !s.last.eof || !s.holes || s.notBefore.Sub(at) < time.Minute {
		t.Fatal("EOF lost the remaining unsupported hole", built, s, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = accountRouteDeltaAddSource(t, dir, current, 4, qs[:1])
	if built, err := refresh(at.Add(2 * time.Second)); err != nil || built || s.last.didWork() {
		t.Fatal("appended tail defeated failed-prefix backoff", built, s, err)
	}
	// Replacing the unsupported identity inside the old horizon must wake the
	// scan, even though the unrelated appended tail did not.
	current, err = LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneManifest(current)
	next.Segments = next.Segments[:0]
	for _, ref := range current.Segments {
		if ref.FromTxNum == 1 && (ref.Kind == SegmentHistory || ref.Kind == SegmentAccessor || ref.Kind == SegmentInverted) {
			continue
		}
		next.Segments = append(next.Segments, ref)
	}
	_ = accountRouteDeltaAddSource(t, dir, next, 1, qs[:1])
	if built, err := refresh(at.Add(3 * time.Second)); err != nil || !built || s.last.sources != 1 || s.last.unsupported != 0 || s.holes {
		t.Fatal("interior replacement did not invalidate deferral", built, s, err)
	}
}

func TestAccountRoutingRuntimeConflictFailureAndCancellationKeepCursor(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	active := make([]SegmentRef, 0, len(m.Segments))
	for _, r := range m.Segments {
		if r.FromTxNum == 2 && (r.Kind == SegmentHistory || r.Kind == SegmentAccessor || r.Kind == SegmentInverted) {
			continue
		}
		active = append(active, r)
	}
	changed := NewManifest(1, 3, append(active, storageRuntimeModernSource(t, dir, 2, "account-conflict")...))
	manifestPublicationMu.Lock()
	locked := true
	defer func() {
		if locked {
			manifestPublicationMu.Unlock()
		}
	}()
	s := accountRoutingRefreshState{}
	type result struct {
		built bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, time.Unix(14000, 0), 1, smallSystemRoutingWorkLimit{})
		done <- result{built, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(dir, "history/account-routing-delta-*.route"))
		if len(files) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("account candidate did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if err := publishManifestLocked(dir, changed); err != nil {
		t.Fatal(err)
	}
	manifestPublicationMu.Unlock()
	locked = false
	got := <-done
	if got.err != nil || got.built || s.nextTx != 0 || !s.last.didWork() || s.notBefore.IsZero() {
		t.Fatal("conflict advanced cursor or lost work", got, s)
	}
	if built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, time.Unix(14001, 0), 1, smallSystemRoutingWorkLimit{}); err != nil || !built || s.nextTx != 3 {
		t.Fatal("changed input did not retry the original cursor", built, s, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range current.Segments {
		if ref.Kind != SegmentAccessor || ref.FromTxNum != 3 {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		if err := os.Rename(path, path+".held"); err != nil {
			t.Fatal(err)
		}
		built, readErr := refreshAccountHistoryDeltaContext(context.Background(), dir, &s, time.Unix(14002, 0))
		if err := os.Rename(path+".held", path); err != nil {
			t.Fatal(err)
		}
		if built || readErr == nil || s.nextTx != 3 {
			t.Fatal("hard read failure advanced cursor", built, s, readErr)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if built, err := refreshAccountHistoryDeltaContext(ctx, dir, &s, time.Unix(14003, 0)); built || !errors.Is(err, context.Canceled) || s.nextTx != 3 {
		t.Fatal("cancelled work advanced cursor", built, s, err)
	}
}

func TestAccountRoutingRuntimeGetsTurnAfterReservedMerge(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	m.Progress = &Progress{HistoryBuildTxNum: 70, AccessorBuildTxNum: 70}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	for pass := 0; pass < 3; pass++ {
		result, err := r.compactHistory(context.Background(), false)
		if err != nil || !result.RoutingBuilt {
			t.Fatal("earlier family did not publish", pass, result, err)
		}
	}
	if result, err := r.compactHistory(context.Background(), false); err != nil || !result.Merged || result.RoutingBuilt {
		t.Fatal("account displaced the canonical reservation", result, err)
	}
	if result, err := r.compactHistory(context.Background(), false); err != nil || !result.RoutingBuilt || r.compactionBudget.accountRouting.last.sources == 0 {
		t.Fatal("account starved after the canonical reservation", result, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(m.Progress, current.Progress) {
		t.Fatal("account publication changed cold progress", err)
	}
	hasDelta := false
	for _, ref := range current.Segments {
		if ref.Kind == SegmentAccountHistoryRouting {
			t.Fatal("normal Runner built a global account base")
		}
		hasDelta = hasDelta || ref.Kind == SegmentAccountHistoryRoutingDelta
	}
	if !hasDelta {
		t.Fatal("Runner did not publish its account delta")
	}
}

func TestAccountRoutingRuntimeSuccessfulPublishCannotAbsorbEarlierReplacement(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	active := make([]SegmentRef, 0, len(m.Segments))
	for _, ref := range m.Segments {
		if ref.FromTxNum == 1 && (ref.Kind == SegmentHistory || ref.Kind == SegmentAccessor || ref.Kind == SegmentInverted) {
			continue
		}
		active = append(active, ref)
	}
	changed := NewManifest(1, 3, append(active, storageRuntimeModernSource(t, dir, 1, "earlier-account-replacement")...))
	manifestPublicationMu.Lock()
	locked := true
	defer func() {
		if locked {
			manifestPublicationMu.Unlock()
		}
	}()
	s := accountRoutingRefreshState{}
	type result struct {
		built bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, time.Unix(15000, 0), 1, smallSystemRoutingWorkLimit{})
		done <- result{built, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(dir, "history/account-routing-delta-*.route"))
		if len(files) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("account candidate did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if err := publishManifestLocked(dir, changed); err != nil {
		t.Fatal(err)
	}
	manifestPublicationMu.Unlock()
	locked = false
	got := <-done
	if !got.built || got.err != nil || s.nextTx != 0 || !s.notBefore.IsZero() {
		t.Fatal("successful tx2 publication absorbed concurrent tx1 replacement", got, s)
	}
	if built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, time.Unix(15001, 0), 1, smallSystemRoutingWorkLimit{}); err != nil || !built || s.nextTx != 3 || s.last.sources != 1 {
		t.Fatal("replacement did not get the next bounded pass", built, s, err)
	}
}

func TestAccountRoutingRuntimeBadOldMetadataCannotPinHealthyTail(t *testing.T) {
	for _, kind := range []SegmentKind{SegmentAccountHistoryRouting, SegmentAccountHistoryRoutingDelta} {
		t.Run(string(kind), func(t *testing.T) {
			dir, m, _ := accountRoutingFixture(t, 9)
			var ref SegmentRef
			if kind == SegmentAccountHistoryRouting {
				m = addAccountRoutingFixture(t, dir, m, 1)
				ref = mustAccountRoutingRef(t, m)
			} else {
				ref = accountRouteDeltaBuild(t, dir, m, 1)
				var err error
				m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(dir, ref.Path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[0] ^= 1
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			s := accountRoutingRefreshState{}
			for pass := 0; pass < 3; pass++ {
				if built, err := refreshAccountHistoryDeltaWithBudget(context.Background(), dir, &s, time.Unix(16000+int64(pass), 0), 1, smallSystemRoutingWorkLimit{}); err != nil || !built || s.last.sources != 1 || pass == 0 && s.last.metadataUnknown != 1 {
					t.Fatal("damaged optional object pinned new source", pass, built, s, err)
				}
			}
			current, err := LoadProductionManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			covered := make(map[[32]byte]struct{})
			for _, r := range current.Segments {
				if r.Kind != SegmentAccountHistoryRoutingDelta {
					continue
				}
				reader, err := openAccountHistoryRouting(context.Background(), dir, r)
				if err != nil {
					t.Fatal(err)
				}
				for id := range reader.metadata.covered {
					covered[id] = struct{}{}
				}
				reader.Close()
			}
			if len(covered) != 3 {
				t.Fatal("did not recover all current pairs", len(covered))
			}
		})
	}
}

func TestAccountRoutingRuntimeNoOutputEndsHeavyOpportunity(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	for tx := uint64(1); tx <= 3; tx++ {
		var err error
		m, err = NewAggregator(dir).Integrate(1, 3, storageRuntimeLegacySource(t, dir, tx))
		if err != nil {
			t.Fatal(err)
		}
	}
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	r.compactionBudget.nextLightweightRoute = 3
	result, err := r.compactHistory(context.Background(), false)
	if err != nil || !result.Deferred || result.DeferReason != "account-routing-work" || r.compactionBudget.accountRouting.last.unsupported != 3 || r.compactionBudget.routingWorkSinceMerge != 1 || r.compactionBudget.nextLightweightRoute != 0 || r.compactionBudget.systemRouting.metric != nil {
		t.Fatal("account no-output work stacked another family", result, r.compactionBudget, err)
	}
}
