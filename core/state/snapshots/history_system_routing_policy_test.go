package snapshots

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func routingOneNewBudget(t *testing.T, dir string, m *Manifest) systemRoutingBuildLimits {
	t.Helper()
	ref, built, err := BuildSystemHistoryRoutingContext(context.Background(), dir, m, 1)
	if err != nil || !built {
		t.Fatalf("one-source reference: %v %v", built, err)
	}
	limits := defaultSystemHistoryRoutingLimits
	limits.bytes = int(ref.Size)
	return limits
}

func TestSystemRoutingAdaptiveReusesAuthenticatedDictionaries(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	old, err := loadSystemHistoryRouting(context.Background(), dir, m)
	if err != nil {
		t.Fatal(err)
	}
	limits := routingOneNewBudget(t, dir, m)
	stats, ref, built, err := buildAdaptiveSystemHistoryRouting(context.Background(), dir, m, old, 64, 0, limits)
	if err != nil || !built || stats.limit != 1 || stats.selected != 1 || stats.attempts != 2 || stats.budgetRejects != 1 {
		t.Fatalf("adaptive: %+v built=%v err=%v", stats, built, err)
	}
	if stats.dictionaryReads != 2 || stats.cacheHits == 0 {
		t.Fatalf("dictionaries reread: %+v", stats)
	}
	before := cloneManifest(m)
	m, err = NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSystemHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
	oracle, err := OpenPinnedManager(dir, before)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	for from := uint64(1); from <= 4; from++ {
		got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
		if err != nil {
			t.Fatal(err)
		}
		want, err := oracle.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("from=%d mismatch %v", from, err)
		}
	}
	if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
}

func TestSystemRoutingAdaptiveLearnsAndBackoffSkipsInputReads(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	limits := routingOneNewBudget(t, dir, m)
	state := systemRoutingRefreshState{}
	state.initMetrics("test/" + t.Name())
	now := time.Unix(1000, 0)
	if built, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, now, limits); err != nil || !built || state.batch != 1 {
		t.Fatalf("first: %v %v batch=%d", built, err, state.batch)
	}
	if built, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, now, limits); err != nil || built {
		t.Fatalf("full route: %v %v", built, err)
	}
	if state.failures != 1 || state.notBefore.Sub(now) < systemRoutingBudgetRetryMin {
		t.Fatalf("no backoff: %+v", state)
	}
	attempts := state.metric.attempts.Snapshot().Count()
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var pending SegmentRef
	for _, ref := range m.Segments {
		if ref.Kind == SegmentAccessor && ref.FromTxNum == 3 {
			pending = ref
		}
	}
	path := filepath.Join(dir, pending.Path)
	if err := os.Rename(path, path+".missing"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(path+".missing", path)
	if built, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, now.Add(time.Second), limits); err != nil || built {
		t.Fatalf("backoff touched input: %v %v", built, err)
	}
	if state.metric.attempts.Snapshot().Count() != attempts {
		t.Fatal("backoff reassembled route")
	}
	// Scheduling hints cannot suppress authoritative query errors for an
	// uncovered source. The missing file must still fail a real query.
	reader, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.FirstStateDomainChangesContext(context.Background(), 3, 3, queries); err == nil {
		t.Fatal("query trusted scheduling cache as coverage")
	}
	if err := os.Rename(path+".missing", path); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		at := state.notBefore
		if built, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, at, limits); err != nil || built {
			t.Fatalf("retry %d: %v %v", i, built, err)
		}
		if delay := state.notBefore.Sub(at); delay < systemRoutingBudgetRetryMin || delay > systemRoutingBudgetRetryMax+time.Second {
			t.Fatalf("unbounded backoff %s", delay)
		}
	}
}

func TestSystemRoutingBackoffIdentityIgnoresTailButBindsCoveredReplacements(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	route, err := loadSystemHistoryRouting(context.Background(), dir, m)
	if err != nil {
		t.Fatal(err)
	}
	covered := systemRoutingCovered(route.sources)
	identity := systemRoutingRefIdentity(func() *SegmentRef { r := mustRoutingRef(t, m); return &r }())
	key, err := systemRoutingInputIdentity(context.Background(), manifestLookupView(m), identity, covered, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Existing pending tail changes lie beyond the failed prefix and do not
	// restart an expensive build every time another leaf arrives.
	tail := cloneManifest(m)
	for i, r := range tail.Segments {
		if r.Kind == SegmentHistory && r.FromTxNum == 3 {
			tail.Segments[i].Checksum = "sha256:" + fmt.Sprintf("%064x", 1)
		}
	}
	got, err := systemRoutingInputIdentity(context.Background(), manifestLookupView(tail), identity, covered, 2)
	if err != nil || got != key {
		t.Fatal("unrelated tail invalidated budget backoff", err)
	}
	// A reusable covered source beyond the prefix must also be included.
	for _, h := range m.Segments {
		if h.Kind == SegmentHistory && h.FromTxNum == 3 {
			a, _ := DefaultDomainRegistry().byDataset[SegmentDatasetStateDomainChange].HistoryAccessorRef(manifestLookupView(m), h)
			covered[systemHistoryRoutingPairID(h, a)] = struct{}{}
		}
	}
	key, err = systemRoutingInputIdentity(context.Background(), manifestLookupView(m), identity, covered, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err = systemRoutingInputIdentity(context.Background(), manifestLookupView(tail), identity, covered, 2)
	if err != nil || got == key {
		t.Fatal("covered replacement retained budget failure", err)
	}
	changed := cloneManifest(m)
	for i, r := range changed.Segments {
		if r.Kind == SegmentAccessor && r.FromTxNum == 2 {
			changed.Segments[i].Checksum = "sha256:" + fmt.Sprintf("%064x", 2)
		}
	}
	got, err = systemRoutingInputIdentity(context.Background(), manifestLookupView(changed), identity, covered, 2)
	if err != nil || got == key {
		t.Fatal("attempted accessor replacement retained failure", err)
	}
}

func TestSystemRoutingPolicyRotatesUnsupportedSources(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	n := 0
	for i := range m.Segments {
		ref := &m.Segments[i]
		if ref.Kind != SegmentAccessor {
			continue
		}
		if n < 64 {
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(data[8:12], 0)
			if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
				t.Fatal(err)
			}
			ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
		}
		n++
	}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	state := systemRoutingRefreshState{}
	now := time.Unix(2000, 0)
	if built, err := refreshSystemHistoryRoutingWithStateContext(context.Background(), dir, false, &state, now); err != nil || built {
		t.Fatalf("unsupported: %v %v", built, err)
	}
	if state.notBefore.Sub(now) < systemRoutingUnsupportedRetry {
		t.Fatal("missing unsupported backoff")
	}
	if built, err := refreshSystemHistoryRoutingWithStateContext(context.Background(), dir, false, &state, state.notBefore); err != nil || !built {
		t.Fatalf("valid tail starved: %v %v", built, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadSystemHistoryRouting(context.Background(), dir, current)
	if err != nil || len(route.sources) != 6 {
		t.Fatalf("sources=%v err=%v", route, err)
	}
	if err := CheckSystemHistoryRouting(dir, mustRoutingRef(t, current)); err != nil {
		t.Fatal(err)
	}
}

func TestSystemRoutingPolicyDoesNotMaskCorruptionOrCancellation(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	state := systemRoutingRefreshState{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := refreshSystemHistoryRoutingWithStateContext(ctx, dir, false, &state, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if state.dir != "" {
		t.Fatal("cancelled call changed scheduling state")
	}
	ref := mustRoutingRef(t, m)
	path := filepath.Join(dir, ref.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(data)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := refreshSystemHistoryRoutingWithStateContext(context.Background(), dir, false, &state, time.Now()); err == nil {
		t.Fatal("corruption treated as budget miss")
	}
	if !state.notBefore.IsZero() {
		t.Fatal("corruption installed backoff")
	}
}

func TestSystemRoutingNoCapacityStillHonorsSourceBytesBudget(t *testing.T) {
	dir, m, _ := storageRoutingFixture(t)
	// Header and one source alone fill the budget; no broad key can fit.
	limits := defaultSystemHistoryRoutingLimits
	limits.bytes = systemHistoryRoutingHeaderSize + 32
	stats, _, built, err := buildAdaptiveSystemHistoryRouting(context.Background(), dir, m, nil, 64, 0, limits)
	if err != nil || built || stats.budgetRejects == 0 || stats.dictionaryReads != 3 {
		t.Fatalf("budget: %+v %v %v", stats, built, err)
	}
}

// Err-call cancellation is deterministic and exercises work after input reads,
// without timing a goroutine against a sub-millisecond fixture.
type routingCancelContext struct {
	context.Context
	calls, at int
}

func (c *routingCancelContext) Err() error {
	c.calls++
	if c.at > 0 && c.calls >= c.at {
		return context.Canceled
	}
	return nil
}

func TestSystemRoutingCandidateCancellationDoesNotPublish(t *testing.T) {
	dir, m, _ := routingFixture(t)
	counting := &routingCancelContext{Context: context.Background()}
	stats, ref, built, err := buildAdaptiveSystemHistoryRouting(counting, dir, m, nil, 64, 0, defaultSystemHistoryRoutingLimits)
	if err != nil || !built || stats.selected != 3 {
		t.Fatalf("reference %v %v %+v", built, err, stats)
	}
	if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{counting.calls / 2, counting.calls - 4} {
		ctx := &routingCancelContext{Context: context.Background(), at: at}
		_, _, built, err := buildAdaptiveSystemHistoryRouting(ctx, dir, m, nil, 64, 0, defaultSystemHistoryRoutingLimits)
		if !errors.Is(err, context.Canceled) || built {
			t.Fatalf("at=%d calls=%d built=%v err=%v", at, ctx.calls, built, err)
		}
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); !os.IsNotExist(err) {
			t.Fatal("cancelled candidate left output", err)
		}
	}
	after, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("cancel changed manifest", err)
	}
}

func TestSystemRoutingKeySortCancellationAndOrder(t *testing.T) {
	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("%06d", len(keys)-i)
	}
	ctx := &routingCancelContext{Context: context.Background(), at: 3}
	if err := sortSystemRoutingKeysContext(ctx, keys); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := sortSystemRoutingKeysContext(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatal("unsorted keys")
		}
	}
}

func TestSystemRoutingBackoffSurvivesRealTailAppendAndInvalidatesRetirement(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	limits := routingOneNewBudget(t, dir, m)
	s := systemRoutingRefreshState{}
	s.initMetrics("test/" + t.Name())
	now := time.Unix(3000, 0)
	for i := 0; i < 2; i++ {
		if _, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &s, now, limits); err != nil {
			t.Fatal(err)
		}
	}
	if s.notBefore.IsZero() {
		t.Fatal("no initial backoff")
	}
	attempts := s.metric.attempts.Snapshot().Count()
	// Append an actual source trio and publish a new manifest, beyond the
	// exhausted prefix. This cannot turn each cold leaf into a full rebuild.
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Domain: kvdomains.SystemDynamicProperty, Key: []byte("tail")}
	if err := rawdb.WriteStateTxRange(db, 4, common.Hash{4}, 4, 4); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 4, BlockHash: common.Hash{4}, TxNum: 4, FlatDomain: q.FlatDomain, Owner: q.Owner, Domain: q.Domain, Key: q.Key}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 4, 4, "history/state-domain-change-route-tail.seg")
	if err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(1, 4, refs)
	if err != nil {
		t.Fatal(err)
	}
	if built, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &s, now.Add(time.Second), limits); err != nil || built {
		t.Fatal(built, err)
	}
	if s.metric.attempts.Snapshot().Count() != attempts {
		t.Fatal("append defeated backoff")
	}
	// Retire an old covered trio. Its absence frees budget immediately, even
	// though the prior scheduling deadline has not elapsed.
	var active []SegmentRef
	for _, r := range m.Segments {
		if r.FromTxNum == 1 && r.ToTxNum == 1 && r.Kind != SegmentSystemHistoryRouting {
			continue
		}
		active = append(active, r)
	}
	m.Segments = active
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if _, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &s, now.Add(2*time.Second), limits); err != nil {
		t.Fatal(err)
	}
	if s.metric.attempts.Snapshot().Count() == attempts {
		t.Fatal("retirement did not invalidate backoff")
	}
}

func TestSystemRoutingAdaptiveSnapshotAndSignedRestore(t *testing.T) {
	dir, identity, owner := writeVerifiableHistoryManifest(t)
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	stats, ref, built, err := buildAdaptiveSystemHistoryRouting(context.Background(), dir, m, nil, 64, 0, defaultSystemHistoryRoutingLimits)
	if err != nil || !built || stats.selected == 0 {
		t.Fatal(built, err)
	}
	if _, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref}); err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatal(err)
	}
	for _, signed := range []bool{false, true} {
		db := rawdb.NewMemoryDatabase()
		if signed {
			r, e := RestoreSnapshotFromVerifiedCatalog(db, dir, identity, []ed25519.PublicKey{pub})
			if e != nil || r.ChangesRestored != 2 {
				db.Close()
				t.Fatal(r, e)
			}
		} else {
			r, e := RestoreSnapshotFromVerifiedManifestWithOptions(db, dir, identity, strictBoundaryOptions(t, db))
			if e != nil || r.ChangesRestored != 2 {
				db.Close()
				t.Fatal(r, e)
			}
		}
		row, ok, e := rawdb.ReadStateDomainChange(db, 1, 1)
		if e != nil || !ok || row.Owner != owner || string(row.Prev) != "old-a" {
			db.Close()
			t.Fatal(row, ok, e)
		}
		census, e := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
		db.Close()
		if e != nil || !census.Complete || !census.MuHashVerified {
			t.Fatal(census, e)
		}
	}
}

func TestSystemRoutingBackoffSourceReplacementAndDirectorySeparation(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 1)
	limits := routingOneNewBudget(t, dir, m)
	state := systemRoutingRefreshState{}
	now := time.Unix(4000, 0)
	for i := 0; i < 2; i++ {
		if _, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, now, limits); err != nil {
			t.Fatal(err)
		}
	}
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A real missing accessor after relevant manifest replacement remains an I/O
	// error, even while the old schedule would have deferred the build.
	for i, r := range m.Segments {
		if r.Kind == SegmentAccessor && r.FromTxNum == 3 {
			if err := os.Rename(filepath.Join(dir, r.Path), filepath.Join(dir, r.Path)+".offline"); err != nil {
				t.Fatal(err)
			}
			m.Segments[i].Checksum = fmt.Sprintf("sha256:%064x", 123)
		}
	}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if _, err := refreshSystemHistoryRoutingWithLimitsContext(context.Background(), dir, false, &state, now.Add(time.Second), limits); err == nil {
		t.Fatal("source I/O hidden by old backoff")
	}
	if !state.notBefore.IsZero() || state.failures != 0 {
		t.Fatal("source I/O installed budget backoff")
	}
	other, otherM, _ := routingFixture(t)
	addRoutingFixture(t, other, otherM, 1)
	if built, err := refreshSystemHistoryRoutingWithStateContext(context.Background(), other, false, &state, now.Add(2*time.Second)); err != nil || !built {
		t.Fatal("directory inherited scheduling hints", built, err)
	}
	if state.dir != other || state.batch != 64 || state.failures != 0 {
		t.Fatal("directory did not reset policy")
	}
}
