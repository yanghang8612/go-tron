package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func accountRouteDeltaAddSource(t *testing.T, dir string, m *Manifest, tx uint64, qs []StateHistoryQuery) *Manifest {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
		t.Fatal(err)
	}
	for i, q := range qs {
		row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: uint64(i), FlatDomain: q.FlatDomain, Owner: q.Owner, PrevExists: true, Prev: []byte{byte(tx), byte(i)}}
		if q.FlatDomain == rawdb.StateFlatDomainKVGeneration {
			row.Prev = binary.BigEndian.AppendUint64(nil, tx)
		}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-account-delta-%d.seg", tx))
	if err != nil {
		t.Fatal(err)
	}
	next := cloneManifest(m)
	next.VisibleTxEnd = max(tx, next.VisibleTxEnd)
	next.Segments = append(next.Segments, refs...)
	if err := PublishManifest(dir, next); err != nil {
		t.Fatal(err)
	}
	return next
}

func accountRouteDeltaFixture(t *testing.T) (string, *Manifest, []StateHistoryQuery) {
	t.Helper()
	dir, m, qs := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	var id common.AccountID
	id[19] = 0xff
	qs = append(qs, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: id.Address(common.AddressPrefixMainnet)})
	gen := qs[len(qs)-1]
	gen.FlatDomain = rawdb.StateFlatDomainKVGeneration
	qs = append(qs, gen)
	m = accountRouteDeltaAddSource(t, dir, m, 4, []StateHistoryQuery{qs[0], qs[len(qs)-2], gen})
	return dir, m, qs
}

func accountRouteDeltaBuild(t *testing.T, dir string, m *Manifest, n int) SegmentRef {
	t.Helper()
	ref, built, err := BuildAccountHistoryRoutingDeltaContext(context.Background(), dir, m, n)
	if err != nil || !built {
		t.Fatalf("build %v %v", built, err)
	}
	return ref
}

func TestAccountRoutingDeltaIndependentProofsAndLatePositive(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	oracle, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	baseRef := mustAccountRoutingRef(t, m)
	baseBytes, err := os.ReadFile(filepath.Join(dir, baseRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	original := cloneManifest(m)
	a := accountRouteDeltaBuild(t, dir, m, 1)
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Build an overlapping second table from the original canonical view. Its
	// positive at Tx4 must not revoke A's independent absence proof for Tx2.
	b := accountRouteDeltaBuild(t, dir, original, 64)
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatal("delta overlap rejected", err)
	}
	if !reflect.DeepEqual(original.Progress, m.Progress) {
		t.Fatal("delta advanced canonical progress")
	}
	after, _ := os.ReadFile(filepath.Join(dir, baseRef.Path))
	if !reflect.DeepEqual(after, baseBytes) || mustAccountRoutingRef(t, m) != baseRef {
		t.Fatal("delta rewrote the base")
	}
	index := newAccountDeltaHistoryReader(manifestLookupView(m), 2, 4, [][]byte{stateDomainChangeBinaryAccessorLookupKey(qs[9].FlatDomain, qs[9].Owner, 0, 0, nil)})
	if index.opened != 0 {
		t.Fatal("eager delta reads")
	}
	table := manifestHistorySources(manifestLookupView(m))
	for _, src := range table.sources {
		h := manifestLookupView(m).Segments[src.row]
		if h.FromTxNum != 2 && h.FromTxNum != 4 {
			continue
		}
		skip, e := index.maySkip(context.Background(), dir, h, src.id, index.keys[0], 2)
		if e != nil || skip != (h.FromTxNum == 2) {
			t.Fatalf("independent proof tx%d skip=%v err=%v", h.FromTxNum, skip, e)
		}
	}
	manager, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	for from := uint64(1); from <= 5; from++ {
		got, err := manager.FirstStateDomainChangesContext(context.Background(), from, 5, qs)
		if err != nil {
			t.Fatal(err)
		}
		for i, q := range qs {
			var want *rawdb.StateDomainChange
			err := oracle.IterateStateDomainChangesByKey(from, 5, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
			if err != nil || !reflect.DeepEqual(got[i], want) {
				t.Fatalf("from%d key%d got%+v want%+v %v", from, i, got[i], want, err)
			}
		}
	}
	for _, ref := range []SegmentRef{a, b} {
		if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
			t.Fatal(err)
		}
	}
	// Explicitly replace A by the already active B. Range integration must not
	// have retired A earlier; exact replacement must not retire active B.
	hadA := false
	for _, ref := range m.Segments {
		hadA = hadA || ref == a
	}
	if !hadA {
		t.Fatal("overlap retired an independent proof")
	}
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, b, []SegmentRef{a, b})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range m.Segments {
		if ref == a {
			t.Fatal("explicit input not retired")
		}
	}
	for _, ref := range m.Retired {
		if ref.Path == b.Path {
			t.Fatal("active reused output retired")
		}
	}
	if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireChecksums: true, RequireRegistered: true}); err != nil {
		t.Fatal(err)
	}
}

func TestAccountRoutingDeltaUnknownBudgetsAndCurrentCorruption(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	var err error
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := stateDomainChangeBinaryAccessorLookupKey(qs[9].FlatDomain, qs[9].Owner, 0, 0, nil)
	view := manifestLookupView(m)
	sources := manifestHistorySources(view)
	var h SegmentRef
	var id [32]byte
	for _, src := range sources.sources {
		if view.Segments[src.row].FromTxNum == 3 {
			h = view.Segments[src.row]
			id = src.id
		}
	}
	makeReader := func() *accountDeltaHistoryReader { return newAccountDeltaHistoryReader(view, 2, 4, [][]byte{key}) }
	r := makeReader()
	r.metadataRemaining = 127
	if skip, e := r.maySkip(context.Background(), dir, h, id, key, 2); e != nil || skip || r.opened != 0 {
		t.Fatal("budget exclusion became absence", skip, e, r.opened)
	}
	r = makeReader()
	r.residentRemaining = 1
	if skip, e := r.maySkip(context.Background(), dir, h, id, key, 2); e != nil || skip {
		t.Fatal("resident exclusion became absence", skip, e)
	}
	r = makeReader()
	changed := id
	changed[0] ^= 1
	if skip, e := r.maySkip(context.Background(), dir, h, changed, key, 5); e != nil || skip {
		t.Fatal("stale identity inherited coverage", skip, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := makeReader().maySkip(ctx, dir, h, id, key, 2); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel swallowed", err)
	}
	before, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	root, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	header := root.metadata.header
	root.Close()
	for _, off := range []uint64{0, header.metadataBytes(), header.metadataBytes() + header.directories*accountRoutingPageBytes} {
		damaged := append([]byte(nil), before...)
		damaged[off] ^= 1
		if err := os.WriteFile(filepath.Join(dir, ref.Path), damaged, 0644); err != nil {
			t.Fatal(err)
		}
		r = makeReader()
		if skip, e := r.maySkip(context.Background(), dir, h, id, key, 5); e != nil || skip {
			t.Fatalf("corrupt offset%d became absence %v %v", off, skip, e)
		}
		manager, e := OpenPinnedManager(dir, m)
		if e != nil {
			t.Fatal(e)
		}
		got, e := manager.FirstStateDomainChangesContext(context.Background(), 2, 4, []StateHistoryQuery{qs[9]})
		if e != nil || len(got) != 1 || got[0] == nil || got[0].TxNum != 4 {
			t.Fatal("corrupt optional route broke canonical result", e, got)
		}
		if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
			t.Fatal("strict import accepted damaged route")
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ref.Path), before, 0644); err != nil {
		t.Fatal(err)
	}
	// A later request rereads metadata, even if another request populated proofs.
	r = makeReader()
	if _, e := r.maySkip(context.Background(), dir, h, id, key, 5); e != nil {
		t.Fatal(e)
	}
	if err := os.Rename(filepath.Join(dir, ref.Path), filepath.Join(dir, ref.Path)+".hold"); err != nil {
		t.Fatal(err)
	}
	if skip, e := makeReader().maySkip(context.Background(), dir, h, id, key, 5); e != nil || skip {
		t.Fatal("missing object became absence", skip, e)
	}
}

func TestAccountRoutingDeltaPublicationConflictAndNoGlobalMerge(t *testing.T) {
	dir, m, _ := accountRouteDeltaFixture(t)
	ref := accountRouteDeltaBuild(t, dir, m, 1)
	// A concurrent auxiliary publication within the proof envelope invalidates
	// this build view. No stale complete manifest may overwrite it.
	other := accountRouteDeltaBuild(t, dir, m, 64)
	current, err := publishAccountHistoryDelta(context.Background(), dir, m, other, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if _, err := publishAccountHistoryDelta(context.Background(), dir, m, ref, nil); !errors.Is(err, errAccountDeltaPublicationConflict) {
		t.Fatal("stale publication accepted", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("conflict changed manifest")
	}
	if _, err := publishAccountHistoryDelta(context.Background(), dir, current, other, []SegmentRef{mustAccountRoutingRef(t, current)}); err == nil {
		t.Fatal("base accepted as delta replacement input")
	}
}

func TestAccountRoutingDeltaBuildContinuesAfterOversizeSource(t *testing.T) {
	dir, m, qs := accountRoutingFixture(t, 3)
	m = addAccountRoutingFixture(t, dir, m, 64)
	var many []StateHistoryQuery
	for i := 0; i < 400; i++ {
		var id common.AccountID
		binary.BigEndian.PutUint64(id[12:], uint64(i+100))
		many = append(many, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: id.Address(common.AddressPrefixMainnet)})
	}
	m = accountRouteDeltaAddSource(t, dir, m, 4, many)
	m = accountRouteDeltaAddSource(t, dir, m, 5, qs[:1])
	m = manifestLookupView(m)
	var smallBytes uint64
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind == SegmentHistory && h.FromTxNum == 5 {
			a, _ := cfg.HistoryAccessorRef(m, h)
			smallBytes = a.Size
		}
	}
	ref, built, stats, err := buildAccountRoutingDelta(context.Background(), dir, m, 64, 0, smallSystemRoutingWorkLimit{bytes: smallBytes, duration: 0})
	if err != nil || !built || stats.sources != 1 || stats.unsupported != 1 || !stats.eof {
		t.Fatalf("build %v %+v %v", built, stats, err)
	}
	r, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.metadata.header.from != 5 || r.metadata.header.to != 5 {
		t.Fatal("oversize source gained coverage", r.metadata.header)
	}
	if _, err := publishAccountHistoryDelta(context.Background(), dir, m, ref, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAccountRoutingDeltaReplacementDoesNotInheritAbsence(t *testing.T) {
	dir, m, _ := accountRouteDeltaFixture(t)
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	var err error
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneManifest(m)
	next.Segments = next.Segments[:0]
	for _, r := range m.Segments {
		if r.FromTxNum == 3 && (r.Kind == SegmentHistory || r.Kind == SegmentAccessor || r.Kind == SegmentInverted) {
			continue
		}
		next.Segments = append(next.Segments, r)
	}
	var id common.AccountID
	id[19] = 0xfe
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: id.Address(common.AddressPrefixMainnet)}
	next = accountRouteDeltaAddSource(t, dir, next, 3, []StateHistoryQuery{q})
	manager, err := OpenPinnedManager(dir, next)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.FirstStateDomainChangesContext(context.Background(), 2, 4, []StateHistoryQuery{q})
	if err != nil || len(got) != 1 || got[0] == nil || got[0].TxNum != 3 {
		t.Fatal("replacement inherited old absence", got, err)
	}
}

func TestAccountRoutingDeltaConsolidationPreservesCurrentProofs(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	var inputs []SegmentRef
	for i := 0; i < 2; i++ {
		ref := accountRouteDeltaBuild(t, dir, m, 1)
		inputs = append(inputs, ref)
		var err error
		m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, built, err := compactAccountRoutingDeltas(context.Background(), dir, m, 1); err != nil || built {
		t.Fatal("ignored consolidation input budget", built, err)
	}
	ref, replace, built, err := compactAccountRoutingDeltas(context.Background(), dir, m, 128<<20)
	if err != nil || !built || len(replace) != 2 {
		t.Fatal("consolidation", built, replace, err)
	}
	if ref.AggregationSteps != 2 {
		t.Fatal("consolidation level", ref.AggregationSteps)
	}
	next, err := publishAccountHistoryDelta(context.Background(), dir, m, ref, replace)
	if err != nil {
		t.Fatal(err)
	}
	if mustAccountRoutingRef(t, next) != mustAccountRoutingRef(t, m) {
		t.Fatal("consolidation changed base")
	}
	r, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(r.metadata.covered) != 2 {
		t.Fatal("consolidation lost coverage", len(r.metadata.covered))
	}
	if err := checkAccountHistoryRoutingContext(context.Background(), dir, next, ref); err != nil {
		t.Fatal(err)
	}
	if !historyRoutingRetiredObject(inputs[0]) {
		t.Fatal("delta is not recognized as auxiliary retirement")
	}
	manager, err := OpenPinnedManager(dir, next)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.FirstStateDomainChangesContext(context.Background(), 2, 4, []StateHistoryQuery{qs[2]})
	if err != nil || got[0] == nil || got[0].TxNum != 3 {
		t.Fatal("consolidation lost positive", got, err)
	}
}

func TestAccountRoutingDeltaSnapshotSignedRestoreAndMuHash(t *testing.T) {
	dir, identity, owner := writeVerifiableHistoryManifest(t)
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	if _, err := publishAccountHistoryDelta(context.Background(), dir, m, ref, nil); err != nil {
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
			result, err := RestoreSnapshotFromVerifiedCatalog(db, dir, identity, []ed25519.PublicKey{pub})
			if err != nil || result.ChangesRestored != 2 {
				db.Close()
				t.Fatal(result, err)
			}
		} else {
			result, err := RestoreSnapshotFromVerifiedManifestWithOptions(db, dir, identity, strictBoundaryOptions(t, db))
			if err != nil || result.ChangesRestored != 2 {
				db.Close()
				t.Fatal(result, err)
			}
		}
		row, ok, err := rawdb.ReadStateDomainChange(db, 1, 1)
		if err != nil || !ok || row.Owner != owner || string(row.Prev) != "old-a" {
			db.Close()
			t.Fatal(row, ok, err)
		}
		census, err := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
		db.Close()
		if err != nil || !census.Complete || !census.MuHashVerified {
			t.Fatal(census, err)
		}
	}
}

func TestAccountRoutingDeltaFailedAuthenticationConsumesReadBudget(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	good, e := openAccountHistoryRouting(context.Background(), dir, ref)
	if e != nil {
		t.Fatal(e)
	}
	cost := good.metadata.header.metadataBytes() + 64
	good.Close()
	ref.LookupRoot = "sha256:" + strings.Repeat("0", 64)
	fake := cloneManifest(m)
	for i := 0; i < 8; i++ {
		fake.Segments = append(fake.Segments, ref)
	}
	key := stateDomainChangeBinaryAccessorLookupKey(qs[9].FlatDomain, qs[9].Owner, 0, 0, nil)
	view := manifestLookupView(m)
	src := manifestHistorySources(view).sources[1]
	h := view.Segments[src.row]
	r := newAccountDeltaHistoryReader(fake, 2, 4, [][]byte{key})
	r.metadataRemaining = 2*cost + 64
	if skip, err := r.maySkip(context.Background(), dir, h, src.id, key, 5); err != nil || skip {
		t.Fatal(skip, err)
	}
	if r.opened != 2 || r.metadataRemaining != 64 {
		t.Fatal("failed authentication escaped cumulative read budget", r.opened, r.metadataRemaining, cost)
	}
}

func TestAccountRoutingDeltaConsolidationCanRetireOutsideOutputEnvelope(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	for i := 0; i < 2; i++ {
		ref := accountRouteDeltaBuild(t, dir, m, 1)
		var err error
		m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	next := cloneManifest(m)
	next.Segments = next.Segments[:0]
	for _, r := range m.Segments {
		if r.FromTxNum == 2 && (r.Kind == SegmentHistory || r.Kind == SegmentAccessor || r.Kind == SegmentInverted) {
			continue
		}
		next.Segments = append(next.Segments, r)
	}
	next = accountRouteDeltaAddSource(t, dir, next, 2, qs[:1])
	ref, inputs, built, err := compactAccountRoutingDeltas(context.Background(), dir, next, 128<<20)
	if err != nil || !built || len(inputs) != 2 || ref.FromTxNum != 3 {
		t.Fatal("expected envelope shrink after stale ID drop", ref, built, err)
	}
	result, err := publishAccountHistoryDelta(context.Background(), dir, next, ref, inputs)
	if err != nil {
		t.Fatal("stale-only input outside output envelope cannot retire", err)
	}
	for _, r := range result.Segments {
		for _, old := range inputs {
			if r == old {
				t.Fatal("explicit consolidation input remains", old)
			}
		}
	}
	manager, err := OpenPinnedManager(dir, result)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := manager.FirstStateDomainChangesContext(context.Background(), 2, 4, qs[:1])
	if err != nil || rows[0] == nil || rows[0].TxNum != 2 {
		t.Fatal("replacement positive lost after consolidation", rows, err)
	}
}

func TestAccountRoutingDeltaStaleOnlyPrefixDoesNotBlockConsolidation(t *testing.T) {
	dir, m, qs := accountRouteDeltaFixture(t)
	m = accountRouteDeltaAddSource(t, dir, m, 5, qs[:1])
	var inputs []SegmentRef
	for i := 0; i < 4; i++ {
		ref := accountRouteDeltaBuild(t, dir, m, 1)
		inputs = append(inputs, ref)
		var err error
		m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	next := cloneManifest(m)
	next.Segments = next.Segments[:0]
	for _, r := range m.Segments {
		if (r.FromTxNum == 2 || r.FromTxNum == 3) && (r.Kind == SegmentHistory || r.Kind == SegmentAccessor || r.Kind == SegmentInverted) {
			continue
		}
		next.Segments = append(next.Segments, r)
	}
	next = accountRouteDeltaAddSource(t, dir, next, 2, qs[:1])
	next = accountRouteDeltaAddSource(t, dir, next, 3, qs[:1])
	// Metadata is intact but stale data pages are unusable. Selection and a
	// mixed stale/current merge must not scan those obsolete data rows at all.
	for _, old := range inputs[:2] {
		reader, err := openAccountHistoryRouting(context.Background(), dir, old)
		if err != nil {
			t.Fatal(err)
		}
		off := reader.metadata.header.metadataBytes() + reader.metadata.header.directories*accountRoutingPageBytes
		reader.Close()
		raw, err := os.ReadFile(filepath.Join(dir, old.Path))
		if err != nil {
			t.Fatal(err)
		}
		raw[off] ^= 1
		if err := os.WriteFile(filepath.Join(dir, old.Path), raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	ref, replace, built, err := compactAccountRoutingDeltas(context.Background(), dir, next, 128<<20)
	if err != nil || !built || len(replace) != 2 {
		t.Fatal("stale prefix blocked useful consolidation", built, replace, err)
	}
	if ref.FromTxNum < 4 {
		t.Fatal("stale IDs inherited new coverage", ref)
	}
	if _, err := publishAccountHistoryDelta(context.Background(), dir, next, ref, replace); err != nil {
		t.Fatal(err)
	}
}
