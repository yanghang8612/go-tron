package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// This fixture writes the existing persisted formats through their public
// writers. It deliberately combines repeated transaction/key versions,
// missing versus present-empty values, a repair row, and an empty final block.
// Expected output is the independently existing complete builder, not a second
// encoder for the new container.
func referenceBuildSource(t *testing.T, mode string) (*pebbledb.Database, string, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebbledb.New(dir, 16, 16, "test/reference-build/", false, pebbledb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	closeDB := func() {
		if !closed {
			closed = true
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(closeDB)
	shared := mode == "shared" || mode == "repair"
	rawdb.SetStateHistoryCrossBlockDedup(shared)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	large := make([]byte, 384<<10)
	_, _ = rand.New(rand.NewSource(20260915)).Read(large)
	for block := uint64(1); block <= 4; block++ {
		begin := (block-1)*3 + 1
		if err := rawdb.WriteStateTxRange(db, block, common.Hash{byte(block)}, begin, begin+2); err != nil {
			t.Fatal(err)
		}
		if block == 4 {
			continue // Legal range with no seq=0 pack or standalone rows.
		}
		makeRow := func(tx, seq uint64, key string, present bool, prev []byte) *rawdb.StateDomainChange {
			return &rawdb.StateDomainChange{BlockNum: block, BlockHash: common.Hash{byte(block)}, TxNum: tx, Seq: seq,
				FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: coldBuilderOwner(0x42), Generation: 7,
				Domain: kvdomains.SystemDelegation, Key: []byte(key), PrevExists: present, Prev: bytes.Clone(prev),
				NextExists: true, Next: []byte("transient-not-archived")}
		}
		rows := []*rawdb.StateDomainChange{
			makeRow(begin, 1, "account/missing", false, nil),
			makeRow(begin, 2, "account/empty", true, nil),
			makeRow(begin+1, 3, "delegation/shared", true, large),
			makeRow(begin+1, 4, "delegation/shared", true, large),
			makeRow(begin+2, 5, "delegation/shared\x00edge", true, []byte{byte(block), 0, 255}),
		}
		// Same tx/key with distinguishable versions makes a reversal visible
		// to the full ordered digest, while preserving substantial chunk reuse.
		rows[3].Prev[len(rows[3].Prev)-1] ^= byte(block)
		if mode == "legacy" {
			rows[0].Seq = 0 // Historical standalone sequence-zero compatibility.
			for _, row := range rows {
				if err := rawdb.WriteStateDomainChange(db, row); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			batch := db.NewBatch()
			if err := rawdb.WriteStateDomainChangeBlockRows(historySharedSeedWriter{db, batch}, rows); err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			if mode == "repair" {
				if err := rawdb.WriteStateDomainChange(db, makeRow(begin+1, 3, "delegation/shared", true, []byte{byte(block), 3, 0})); err != nil {
					t.Fatal(err)
				}
				// A late physical Seq can repair an earlier transaction. The
				// cold oracle sorts by logical tx first, not physical Seq alone.
				if err := rawdb.WriteStateDomainChange(db, makeRow(begin, 99, "repair/value", true, []byte{byte(block), 9})); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return db, dir, closeDB
}

func referenceBuildKVDigest(t *testing.T, db ethdb.Iteratee) [32]byte {
	t.Helper()
	h := sha256.New()
	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		for _, b := range [][]byte{it.Key(), it.Value()} {
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(b)))
			h.Write(size[:])
			h.Write(b)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func referenceBuildHistoryRef(t *testing.T, refs []SegmentRef) SegmentRef {
	t.Helper()
	for _, ref := range refs {
		if ref.Kind == SegmentHistory {
			return ref
		}
	}
	t.Fatal("history ref absent")
	return SegmentRef{}
}

func referenceBuildLogicalBytes(t *testing.T, dir string, ref SegmentRef) []byte {
	t.Helper()
	r, size, _, err := openHistorySegmentForReadWithCacheLimit(dir, ref, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.NewSectionReader(r, 0, int64(size)))
	if err := errors.Join(readErr, r.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func referenceBuildAssertQueries(t *testing.T, dir string, refs []SegmentRef, want []*rawdb.StateDomainChange) {
	t.Helper()
	ref := referenceBuildHistoryRef(t, refs)
	manifest := NewManifest(1, 12, refs)
	for _, interval := range [][2]uint64{{1, 12}, {1, 1}, {2, 2}, {3, 4}, {8, 9}, {10, 12}, {0, 0}, {13, 14}} {
		var expected []*rawdb.StateDomainChange
		for _, row := range want {
			if row.TxNum >= interval[0] && row.TxNum <= interval[1] {
				expected = append(expected, row)
			}
		}
		got, err := readStateDomainChangeHistoryRange(dir, manifest, ref, interval[0], interval[1])
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("tx interval %v differs: got=%d want=%d error=%v", interval, len(got), len(expected), err)
		}
	}
	for _, key := range []string{"account/missing", "account/empty", "delegation/shared", "delegation/shared\x00edge", "absent"} {
		lookup := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, coldBuilderOwner(0x42), 7, kvdomains.SystemDelegation, []byte(key))
		var expected []*rawdb.StateDomainChange
		for _, row := range want {
			if bytes.Equal(row.Key, []byte(key)) {
				expected = append(expected, row)
			}
		}
		got, err := readStateDomainChangeHistoryByKey(dir, manifest, ref, lookup, 1, 12)
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("key %q differs: got=%d want=%d error=%v", key, len(got), len(expected), err)
		}
	}
	prefix := stateDomainChangeBinaryAccessorLookupPrefix(coldBuilderOwner(0x42), 7, kvdomains.SystemDelegation, []byte("delegation/"))
	var got []*rawdb.StateDomainChange
	if err := iterateStateDomainChangeHistoryByPrefix(dir, manifest, ref, prefix, 1, 12, func(row *rawdb.StateDomainChange) (bool, error) {
		got = append(got, cloneStateDomainChangeForSegment(row))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	// Prefix APIs may group by key. Compare every full archival row, including
	// duplicate multiplicity, independently of this API's key traversal order.
	digestMultiset := func(rows []*rawdb.StateDomainChange) map[[32]byte]int {
		out := make(map[[32]byte]int)
		for _, row := range rows {
			out[sha256.Sum256(historyDiagnosticRow(row))]++
		}
		return out
	}
	var expected []*rawdb.StateDomainChange
	for _, row := range want {
		if bytes.HasPrefix(row.Key, []byte("delegation/")) {
			expected = append(expected, row)
		}
	}
	if !reflect.DeepEqual(digestMultiset(got), digestMultiset(expected)) {
		t.Fatal("prefix full-row multiset differs")
	}
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := mgr.FirstStateDomainChangesByKeysContext(context.Background(), 1, 12, rawdb.StateFlatDomainKVLatest,
		coldBuilderOwner(0x42), 7, kvdomains.SystemDelegation, [][]byte{[]byte("account/missing"), []byte("account/empty"), []byte("delegation/shared"), []byte("absent")})
	if err != nil || len(first) != 3 || first["absent"] != nil {
		t.Fatal("point lookup coverage differs", err, len(first))
	}
	var firstDelegation *rawdb.StateDomainChange
	for _, row := range want {
		if string(row.Key) == "delegation/shared" {
			firstDelegation = row
			break
		}
	}
	if firstDelegation == nil || first["account/missing"].PrevExists || !first["account/empty"].PrevExists || len(first["account/empty"].Prev) != 0 || !bytes.Equal(first["delegation/shared"].Prev, firstDelegation.Prev) {
		t.Fatal("point lookup lost missing/empty/large value semantics")
	}
}

func TestHistoryReferenceBuildIntegrationOracle(t *testing.T) {
	for _, tc := range []struct{ source, format string }{{"legacy", "2"}, {"pack", "3"}, {"shared", "auto"}, {"repair", "auto"}} {
		t.Run(tc.source+"-"+tc.format, func(t *testing.T) {
			db, sourceDir, closeDB := referenceBuildSource(t, tc.source)
			before := referenceBuildKVDigest(t, db)
			view, release, err := rawdb.AcquireStateHistoryReadView(db)
			if err != nil || !view.IsPinnedKeyValueView() {
				t.Fatal("pinned source unavailable", err)
			}
			released := false
			defer func() {
				if !released {
					if err := release(); err != nil {
						t.Error(err)
					}
				}
			}()
			ctx := context.Background()
			oldDir, newDir := t.TempDir(), t.TempDir()
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			rel := cfg.HistoryPath(1, 12)
			digestSource := DigestHotStateHistoryContext
			if tc.source == "legacy" || tc.source == "repair" {
				digestSource = DigestHotStateHistoryCompatibilityContext
			}
			hot, err := digestSource(ctx, view, t.TempDir(), 1, 12, 1, 4, 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			var old []SegmentRef
			if tc.source == "legacy" || tc.source == "repair" {
				// The public diagnostic deliberately keeps the production
				// borrowed-only contract. Its established owning compatibility
				// builder is the independent oracle for these historical rows.
				owning := cfg
				owning.IterateHotHistoryBlockTxBorrowed = nil
				built, buildErr := buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextFormat(ctx, view, oldDir,
					SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 12, Path: rel},
					owning, etl.Options{BufferLimit: 128}, &stateDomainChangeHistoryBlockRange{from: 1, to: 4}, tc.format)
				old, err = built.refs, buildErr
			} else {
				old, err = BuildDiagnosticStateHistoryTrioWithETLContext(ctx, view, oldDir, 1, 12, 1, 4, rel, tc.format, etl.Options{BufferLimit: 128})
			}
			if err != nil {
				t.Fatal(err)
			}
			refs, stats, err := BuildDiagnosticStateHistoryReferenceTrioContext(ctx, view, newDir, 1, 12, 1, 4, rel, etl.Options{BufferLimit: 128})
			if err != nil {
				t.Fatal(err)
			}
			if len(refs) != 3 || stats.SourceBlocks != 4 || stats.Rows != hot.Rows || stats.PrevBytes != hot.PrevBytes {
				t.Fatal("incomplete source accounting", stats, hot)
			}
			if (tc.source == "shared" || tc.source == "repair") && stats.SharedBlocks == 0 {
				t.Fatal("fixture did not exercise actual shared packs")
			}
			if (tc.source == "legacy" || tc.source == "pack" || tc.source == "repair") && stats.FallbackBlocks == 0 {
				t.Fatal("compatibility path not exercised")
			}
			for _, built := range []struct {
				dir  string
				refs []SegmentRef
			}{{oldDir, old}, {newDir, refs}} {
				digest, err := DigestColdStateHistoryContext(ctx, built.dir, built.refs)
				if err != nil || digest != hot {
					t.Fatal("complete source/cold digest differs", err, digest, hot)
				}
				for _, ref := range built.refs {
					var err error
					switch ref.Kind {
					case SegmentHistory:
						err = CheckStateDomainChangeSegment(built.dir, ref)
					case SegmentInverted:
						err = CheckStateDomainChangeIndexSegment(built.dir, ref)
					case SegmentAccessor:
						err = CheckStateDomainChangeAccessorSegment(built.dir, ref)
					}
					if err != nil {
						t.Fatal("companion check failed", ref.Kind, err)
					}
				}
			}
			oldRef, newRef := referenceBuildHistoryRef(t, old), referenceBuildHistoryRef(t, refs)
			oldLogical := referenceBuildLogicalBytes(t, oldDir, oldRef)
			if !bytes.Equal(oldLogical, referenceBuildLogicalBytes(t, newDir, newRef)) || uint64(len(oldLogical)) != stats.LogicalBytes {
				t.Fatal("virtual V6 bytes differ from original complete builder")
			}
			want, err := OpenStateDomainChangeSegment(oldDir, oldRef)
			if err != nil || len(want.TxRanges) != 4 {
				t.Fatal("oracle empty-block tx boundary lost", err)
			}
			var versions []*rawdb.StateDomainChange
			for _, row := range want.Changes {
				if row.TxNum == 2 && string(row.Key) == "delegation/shared" {
					versions = append(versions, row)
				}
			}
			if len(versions) != 2 || bytes.Equal(versions[0].Prev, versions[1].Prev) {
				t.Fatal("duplicate tx/key fixture lost")
			}
			if before != referenceBuildKVDigest(t, db) {
				t.Fatal("builder modified source")
			}
			if _, err := os.Stat(filepath.Join(newDir, ManifestFile)); !os.IsNotExist(err) {
				t.Fatal("diagnostic published a manifest", err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			released = true
			closeDB()
			if err := os.RemoveAll(sourceDir); err != nil {
				t.Fatal(err)
			}
			// Reopen every output after destroying the source; external hot chunk
			// dependencies cannot accidentally satisfy any subsequent read.
			got, err := DigestColdStateHistoryContext(ctx, newDir, refs)
			if err != nil || got != hot {
				t.Fatal("new trio depends on closed/deleted source", err)
			}
			referenceBuildAssertQueries(t, newDir, refs, want.Changes)
		})
	}
}

func TestHistoryReferenceBuildActiveCancelAndSourceError(t *testing.T) {
	db, _, _ := referenceBuildSource(t, "shared")
	before := referenceBuildKVDigest(t, db)
	source := &historyExecutionSource{KeyValueStore: db, factory: db, block: true, entered: make(chan struct{}), release: make(chan struct{})}
	view, release, err := rawdb.AcquireStateHistoryReadView(source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer source.unblock()
	dir := t.TempDir()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	type outcome struct {
		refs []SegmentRef
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		refs, _, err := BuildDiagnosticStateHistoryReferenceTrioContext(ctx, view, dir, 1, 12, 1, 4, cfg.HistoryPath(1, 12), etl.Options{})
		done <- outcome{refs, err}
	}()
	joined := false
	defer func() {
		cancel()
		source.unblock()
		if !joined {
			<-done
		}
	}()
	waitParallelRead(t, source.entered)
	cancel()
	select {
	case got := <-done:
		joined = true
		t.Fatal("returned while source read is active", got)
	case <-time.After(20 * time.Millisecond):
	}
	source.unblock()
	got := <-done
	joined = true
	if !errors.Is(got.err, context.Canceled) || len(got.refs) != 0 || source.active.Load() != 0 || source.closed.Load() != 0 || source.late.Load() != 0 {
		t.Fatal("cancel escaped read lifetime or returned refs", got, source.active.Load(), source.closed.Load())
	}
	if before != referenceBuildKVDigest(t, db) {
		t.Fatal("cancellation changed input")
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); !os.IsNotExist(err) {
		t.Fatal("cancel published manifest", err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
		t.Fatal("cancel advanced snapshot stage", ok, err)
	}
	sentinel := errors.New("reference source read failure")
	fault := referenceBuildFaultView{StateHistoryReadView: view, err: sentinel}
	refs, _, err := BuildDiagnosticStateHistoryReferenceTrioContext(context.Background(), fault, t.TempDir(), 1, 12, 1, 4, cfg.HistoryPath(1, 12), etl.Options{})
	if !errors.Is(err, sentinel) || len(refs) != 0 || before != referenceBuildKVDigest(t, db) {
		t.Fatal("source failure not preserved", err)
	}
}

type referenceBuildFaultView struct {
	rawdb.StateHistoryReadView
	err error
}

func (v referenceBuildFaultView) Get([]byte) ([]byte, error) { return nil, v.err }

func TestHistoryReferenceBuildRejectsMissingBlockRange(t *testing.T) {
	db, _, _ := referenceBuildSource(t, "pack")
	if err := rawdb.DeleteStateTxRange(db, 2); err != nil {
		t.Fatal(err)
	}
	before := referenceBuildKVDigest(t, db)
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	}()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	refs, _, err := BuildDiagnosticStateHistoryReferenceTrioContext(context.Background(), view, t.TempDir(), 1, 12, 1, 4, cfg.HistoryPath(1, 12), etl.Options{})
	if err == nil || len(refs) != 0 || before != referenceBuildKVDigest(t, db) {
		t.Fatal("missing interior range was accepted or source changed", err)
	}
}

func TestHistoryReferenceSpoolRejectsBoundsAndCorruption(t *testing.T) {
	row := &rawdb.StateDomainChange{BlockNum: 1, TxNum: 1, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: coldBuilderOwner(0x42), Domain: kvdomains.SystemDelegation, Key: []byte("key"), PrevExists: true}
	var buf bytes.Buffer
	n, err := writeHistoryReferenceSpoolRow(&buf, row, 3, []historyReferenceValueSpan{{1, 2, 3}}, historyReferenceSpoolLimit)
	if err != nil || n != uint64(buf.Len()) {
		t.Fatal(err, n)
	}
	valid := bytes.Clone(buf.Bytes())
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"truncated-header", func(b []byte) []byte { return b[:15] }},
		{"truncated-span", func(b []byte) []byte { return b[:len(b)-1] }},
		{"oversized-metadata", func(b []byte) []byte { binary.BigEndian.PutUint32(b[:4], math.MaxUint32); return b }},
		{"oversized-count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[12:16], math.MaxUint32); return b }},
		{"oversized-prev", func(b []byte) []byte { binary.BigEndian.PutUint64(b[4:12], math.MaxUint64); return b }},
		{"mismatched-prev", func(b []byte) []byte { binary.BigEndian.PutUint64(b[4:12], 4); return b }},
		{"zero-span", func(b []byte) []byte { binary.BigEndian.PutUint32(b[len(b)-4:], 0); return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := readHistoryReferenceSpoolRow(bytes.NewReader(tc.mutate(bytes.Clone(valid)))); err == nil {
				t.Fatal("corrupt spool accepted")
			}
		})
	}
	buf.Reset()
	if _, err := writeHistoryReferenceSpoolRow(&buf, row, 3, []historyReferenceValueSpan{{1, 2, 3}}, n-1); err == nil || buf.Len() != 0 {
		t.Fatal("spool budget wrote partial metadata", err)
	}
}
