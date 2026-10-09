package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func receiptAuditWithCount(t *testing.T, dir string, manifest *Manifest, calls *int) *HistoryStagingReceiptAudit {
	t.Helper()
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	audit.auth.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
		*calls++
		return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
	}
	return audit
}

func clearReceiptProcessEntry(t *testing.T, dir string, refs [3]SegmentRef) {
	t.Helper()
	id, err := historyStagingTrioID(refs)
	if err != nil {
		t.Fatal(err)
	}
	key := sha256.Sum256(append(append([]byte("gtron-history-staging-receipt-checksum-v1\x00"), dir...), id[:]...))
	historyStagingReceiptChecksumCache.Lock()
	delete(historyStagingReceiptChecksumCache.trios, key)
	historyStagingReceiptChecksumCache.Unlock()
}

func TestHistoryStagingReceiptSidecarRestartReuseAndFallback(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	refs := stagingReceiptRefs(t, dir, manifest)
	ctx := context.Background()
	calls := 0
	first := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := first.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if got := first.Stats(); got.PhysicalSHATrios != 1 || got.PersistentHits != 0 {
		t.Fatalf("first audit counters: %+v", got)
	}
	if calls != 1 {
		t.Fatalf("first physical SHA count = %d", calls)
	}
	if _, err := os.Stat(filepath.Join(dir, historyStagingReceiptSidecar)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("certificate written before completed audit: %v", err)
	}
	if err := first.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, refs)
	calls = 0
	second := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := second.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if got := second.Stats(); got.PhysicalSHATrios != 0 || got.PersistentHits != 1 {
		t.Fatalf("reused audit counters: %+v", got)
	}
	wrongBinding := binding
	wrongBinding.Spans = append([]rawdb.HistoryStagingColdSpan(nil), binding.Spans...)
	wrongBinding.Spans[0].ContentID[0] ^= 1
	if err := second.VerifyBinding(ctx, wrongBinding); err == nil {
		t.Fatal("physical certificate authorized a different durable ContentID")
	}
	if calls != 0 {
		t.Fatalf("restart sidecar repeated SHA %d times", calls)
	}
	if err := second.RecheckAll(ctx); err != nil {
		t.Fatal(err)
	}

	// An unknown but otherwise internally consistent version is still a miss.
	certificatePath := filepath.Join(dir, historyStagingReceiptSidecar)
	original, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatal(err)
	}
	var unknown historyStagingReceiptSidecarDocument
	if err := json.Unmarshal(original, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown.Version = 99
	body, err := json.Marshal(unknown.historyStagingReceiptSidecarBody)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	unknown.Digest = hex.EncodeToString(digest[:])
	versionBytes, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certificatePath, versionBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, refs)
	calls = 0
	versionAudit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := versionAudit.VerifyBinding(ctx, binding); err != nil || calls != 1 {
		t.Fatalf("unknown sidecar version was reused: err=%v SHA=%d", err, calls)
	}
	// A broken advisory document must also cause a real checksum pass.
	for _, malformed := range []string{"{bad"} {
		if err := os.WriteFile(filepath.Join(dir, historyStagingReceiptSidecar), []byte(malformed), 0o600); err != nil {
			t.Fatal(err)
		}
		clearReceiptProcessEntry(t, dir, refs)
		calls = 0
		audit := receiptAuditWithCount(t, dir, manifest, &calls)
		if err := audit.VerifyBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("malformed sidecar %q reused authentication: %d", malformed, calls)
		}
	}
}

func TestHistoryStagingReceiptSidecarChangedFileRejectsRestoredMtime(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	first := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := first.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	path := filepath.Join(dir, refs[0].Path)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0x01
	tmp := path + ".replacement"
	if err := os.WriteFile(tmp, data, before.Mode()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, refs)
	calls := 0
	audit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := audit.VerifyBinding(ctx, binding); err == nil || calls != 1 {
		t.Fatalf("changed inode/content accepted or SHA skipped: err=%v calls=%d", err, calls)
	}
}

func TestHistoryStagingReceiptSidecarInPlaceEditWithRestoredMtimeFallsBack(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	first := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := first.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	path := filepath.Join(dir, refs[0].Path)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Give coarse-timestamp filesystems a new ctime tick. The content size,
	// inode and mtime remain unchanged, so ctime must reject this cache hit.
	time.Sleep(1100 * time.Millisecond)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, before.Size()-1); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, refs)
	calls := 0
	audit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := audit.VerifyBinding(ctx, binding); err == nil || calls != 1 {
		t.Fatalf("same-inode corruption with restored mtime accepted: err=%v SHA=%d", err, calls)
	}
}

func TestHistoryStagingReceiptSidecarFinalRecheckIsNotAdvisory(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	audit := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := audit.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := audit.RecheckAll(ctx); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	path := filepath.Join(dir, refs[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".new", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
	err = audit.CommitPhysicalCertificates(ctx)
	if err == nil || errors.Is(err, ErrHistoryStagingReceiptSidecarWrite) {
		t.Fatalf("late physical change downgraded to advisory write error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, historyStagingReceiptSidecar)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late change still committed a certificate: %v", err)
	}
}

func TestHistoryStagingColdProverPhysicalCertificateDoesNotAuthorizeSemanticProof(t *testing.T) {
	dir, manifest, blocks, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer prover.Close()
	needed := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := 4; i <= 6; i++ {
		needed[i] = true
	}
	if _, err := prover.Build(ctx, blocks, needed); err != nil {
		t.Fatal(err)
	}
	if err := prover.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, stagingReceiptRefs(t, dir, manifest))
	calls := 0
	audit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := audit.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("new trio was physically rehashed: %d", calls)
	}
	// The full prover cache for a fresh worker remains empty. A sidecar cannot
	// replace its semantic/range proof, even though receipt SHA can be reused.
	fresh, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if len(fresh.verified) != 0 {
		t.Fatal("physical sidecar populated full prover")
	}
	if err := fresh.VerifyBinding(ctx, binding, blocks); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingWriterPhysicalFactsCommitOnlyAfterExplicitPublication(t *testing.T) {
	dir, manifest, blocks, binding := stagingReceiptFixture(t)
	ctx, facts, err := WithHistoryStagingPhysicalFacts(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer prover.Close()
	needed := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := 4; i <= 6; i++ {
		needed[i] = true
	}
	if _, err := prover.Build(ctx, blocks, needed); err != nil {
		t.Fatal(err)
	}
	if err := facts.RecheckAll(ctx); err != nil {
		t.Fatalf("pre-publication physical recheck: %v", err)
	}
	path := filepath.Join(dir, historyStagingReceiptSidecar)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("proof or read-only recheck wrote before publication: %v", err)
	}
	if err := facts.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, stagingReceiptRefs(t, dir, manifest))
	calls := 0
	audit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := audit.VerifyBinding(context.Background(), binding); err != nil || calls != 0 {
		t.Fatalf("writer fact was not reused after durable publication: err=%v SHA=%d", err, calls)
	}
}

func TestHistoryStagingReceiptSidecarCancelledAuditDoesNotCommit(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	audit := receiptAuditWithCount(t, dir, manifest, new(int))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := audit.VerifyBinding(ctx, binding); !errors.Is(err, context.Canceled) {
		t.Fatalf("verify: %v", err)
	}
	if err := audit.CommitPhysicalCertificates(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, historyStagingReceiptSidecar)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled audit left certificate: %v", err)
	}
}

func TestHistoryStagingReceiptSidecarForceFullAndWrongRefs(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	initial := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := initial.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := initial.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	clearReceiptProcessEntry(t, dir, refs)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	forced, err := NewHistoryStagingReceiptAuditForceFull(pinned)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	forced.auth.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
		calls++
		return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
	}
	if err := forced.VerifyBinding(ctx, binding); err != nil || calls != 1 {
		t.Fatalf("forced cold SHA: err=%v calls=%d", err, calls)
	}
	if got := forced.Stats(); got.PhysicalSHATrios != 1 || got.PersistentHits != 0 {
		t.Fatalf("forced counters: %+v", got)
	}

	// Even a document with a recomputed envelope digest cannot pass when an
	// entry's exact segment refs differ from the pinned manifest.
	path := filepath.Join(dir, historyStagingReceiptSidecar)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc historyStagingReceiptSidecarDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	doc.Entries[0].Refs[0].Path += ".wrong"
	body, err := json.Marshal(doc.historyStagingReceiptSidecarBody)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	doc.Digest = hex.EncodeToString(digest[:])
	data, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	clearReceiptProcessEntry(t, dir, refs)
	calls = 0
	audit := receiptAuditWithCount(t, dir, manifest, &calls)
	if err := audit.VerifyBinding(ctx, binding); err != nil || calls != 1 {
		t.Fatalf("wrong refs reused receipt: err=%v calls=%d", err, calls)
	}
}

func TestHistoryStagingReceiptSidecarWriteFailureIsAdvisory(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	audit := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := audit.VerifyBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, historyStagingReceiptSidecar), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := audit.CommitPhysicalCertificates(context.Background()); !errors.Is(err, ErrHistoryStagingReceiptSidecarWrite) {
		t.Fatalf("sidecar persistence error was not advisory: %v", err)
	}
	if err := audit.RecheckAll(context.Background()); err != nil {
		t.Fatalf("advisory write failure invalidated strong audit: %v", err)
	}
}

func TestHistoryStagingWriterPhysicalFactsRejectChangeBeforeCommit(t *testing.T) {
	dir, manifest, blocks, _ := stagingReceiptFixture(t)
	ctx, facts, err := WithHistoryStagingPhysicalFacts(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer prover.Close()
	needed := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := 4; i <= 6; i++ {
		needed[i] = true
	}
	if _, err := prover.Build(ctx, blocks, needed); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	path := filepath.Join(dir, refs[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".new", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
	if err := facts.RecheckAll(ctx); err == nil || errors.Is(err, ErrHistoryStagingReceiptSidecarWrite) {
		t.Fatalf("changed physical proof passed read-only recheck: %v", err)
	}
	err = facts.CommitPhysicalCertificates(ctx)
	if err == nil || errors.Is(err, ErrHistoryStagingReceiptSidecarWrite) {
		t.Fatalf("changed physical proof downgraded to advisory error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, historyStagingReceiptSidecar)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("changed proof persisted certificate: %v", err)
	}
}

func TestHistoryStagingReceiptSidecarMergeAndPruneRetired(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	ctx := context.Background()
	first := receiptAuditWithCount(t, dir, manifest, new(int))
	if err := first.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	states := first.auth.taskVerified[binding.Spans[0].ContentID]
	makeOther := func(suffix string, delta uint64) historyStagingReceiptCertificate {
		otherRefs := refs
		otherRefs[0].Path += suffix
		otherRefs[0].FromTxNum += delta
		id, err := historyStagingTrioID(otherRefs)
		if err != nil {
			t.Fatal(err)
		}
		cert, ok := historyStagingMakeCertificate(id, otherRefs, states)
		if !ok {
			t.Fatal("fixture lacks strong stat")
		}
		return cert
	}
	retired, newA, newB := makeOther(".retired", 1), makeOther(".new-a", 2), makeOther(".new-b", 3)
	if err := historyStagingWriteCertificates(ctx, dir, map[[32]byte]historyStagingReceiptCertificate{retired.ID: retired}, nil, nil); err != nil {
		t.Fatal(err)
	}
	// This pinned audit sees retired but its manifest does not contain it.
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if err := audit.VerifyBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	// Two concurrent newly verified publications merge through the sidecar
	// mutex. Neither was in the older audit's initial sidecar view.
	var wg sync.WaitGroup
	for _, entry := range []historyStagingReceiptCertificate{newA, newB} {
		wg.Add(1)
		go func(entry historyStagingReceiptCertificate) {
			defer wg.Done()
			if err := historyStagingWriteCertificates(ctx, dir, map[[32]byte]historyStagingReceiptCertificate{entry.ID: entry}, nil, nil); err != nil {
				t.Error(err)
			}
		}(entry)
	}
	wg.Wait()
	if err := audit.CommitPhysicalCertificates(ctx); err != nil {
		t.Fatal(err)
	}
	entries := historyStagingReadCertificates(dir)
	if len(entries) != 3 || entries[retired.ID] == retired || entries[newA.ID] != newA || entries[newB.ID] != newB {
		t.Fatalf("retired pruning lost concurrent writer facts: count=%d retired=%v newA=%v newB=%v",
			len(entries), entries[retired.ID] == retired, entries[newA.ID] == newA, entries[newB.ID] == newB)
	}
}

func TestHistoryStagingReceiptSidecarLargeIntegerFingerprintRoundTrip(t *testing.T) {
	value := historyStagingDiskFingerprint{Device: (1 << 54) + 17, Inode: (1 << 54) + 31,
		Size: (1 << 53) + 7, Mode: 0o640, Mtime: (1 << 60) + 3, Ctime: [2]int64{(1 << 32) + 11, 987654321}}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded historyStagingDiskFingerprint
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != value {
		t.Fatalf("typed inode/time lost precision: got=%+v want=%+v", decoded, value)
	}
}
