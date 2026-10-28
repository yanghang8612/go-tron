package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func accessorAdoptionFixture(t *testing.T, forged bool) (string, HistoryAccessorAdoptionOptions) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, candidates := filepath.Join(root, "snapshots"), filepath.Join(root, "candidates")
	for _, p := range []string{dir, candidates} {
		if e := os.MkdirAll(filepath.Join(p, "history"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	const count = 1024
	changes := make([]*rawdb.StateDomainChange, count)
	for i := range changes {
		changes[i] = &rawdb.StateDomainChange{BlockNum: uint64(i/8 + 1), BlockHash: common.Hash{30: byte(i / 8), 31: 1}, TxNum: 1 + uint64(i/8)*1_000_000, Seq: uint64(i + 1), FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0: common.AddressPrefixMainnet, 20: 1}, Generation: 1, Domain: kvdomains.ContractStorage, Key: []byte{byte(i % 16)}, PrevExists: true, Prev: bytes.Repeat([]byte{byte(i)}, 32)}
	}
	from, to := uint64(1), changes[len(changes)-1].TxNum+10
	body, index, _, err := encodeStateDomainChangeBinarySegmentV6(from, to, changes)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := encodeStateDomainChangeBinaryIndex(from, to, index)
	if err != nil {
		t.Fatal(err)
	}
	h := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: from, ToTxNum: to, AggregationSteps: 1, Path: stateDomainChangeHistorySegmentPath(from, to)}
	setStateDomainChangeBinaryRefMetadata(&h, body)
	h.Path = contentAddressedSnapshotPath(h.Path, h.Checksum)
	a := h
	a.Kind = SegmentAccessor
	a.Path = stateDomainChangeBinaryAccessorPath(h.Path)
	inverted := h
	inverted.Kind = SegmentInverted
	inverted.Path = stateDomainChangeBinaryIndexPath(h.Path)
	setStateDomainChangeBinaryRefMetadata(&inverted, idx)
	build := func(version uint32) []byte {
		out := t.TempDir()
		b, e := newStateDomainChangeV6Build(etl.Options{TempDir: t.TempDir()}, out, "build")
		if e != nil {
			t.Fatal(e)
		}
		defer b.Close()
		for _, row := range changes {
			if e = b.CollectKey(row); e != nil {
				t.Fatal(e)
			}
		}
		if e = b.FinishDictionary(); e != nil {
			t.Fatal(e)
		}
		offset := index[0].offset
		for i, row := range changes {
			id, e := b.KeyID(stateDomainChangeBinaryAccessorKey(row))
			if e != nil {
				t.Fatal(e)
			}
			tx := row.TxNum
			if forged && version == 11 && i == len(changes)-1 {
				tx++
			}
			if e = b.CollectPosting(id, tx, offset, uint64(i)); e != nil {
				t.Fatal(e)
			}
			offset += 4 + uint64(binary.BigEndian.Uint32(body[offset:offset+4]))
		}
		ref, _, e := b.buildAccessorContextVersion(context.Background(), out, a, uint64(count), version)
		if e != nil {
			t.Fatal(e)
		}
		data, e := os.ReadFile(filepath.Join(out, ref.Path))
		if e != nil {
			t.Fatal(e)
		}
		return data
	}
	old, newData := build(7), build(11)
	if len(newData) >= len(old) {
		t.Fatalf("fixture does not save space: %d -> %d", len(old), len(newData))
	}
	setStateDomainChangeBinaryRefMetadata(&a, old)
	candidate := a
	setStateDomainChangeBinaryRefMetadata(&candidate, newData)
	for _, f := range []struct {
		path string
		data []byte
	}{{filepath.Join(dir, h.Path), body}, {filepath.Join(dir, a.Path), old}, {filepath.Join(dir, inverted.Path), idx}, {filepath.Join(candidates, candidate.Path), newData}} {
		if e := os.WriteFile(f.path, f.data, 0600); e != nil {
			t.Fatal(e)
		}
	}
	m := NewManifestForChain(from, to, []SegmentRef{h, a, inverted}, ChainIdentity{ChainID: 1, NetworkID: 2, GenesisHash: strings.Repeat("a1", 32)})
	m.Generation = 81
	m.Progress = &Progress{HistoryBuildTxNum: 1, AccessorBuildTxNum: 1}
	if err = PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, HistoryAccessorAdoptionOptions{Entries: []HistoryAccessorAdoption{{History: h, Accessor: a, Inverted: inverted, Candidate: candidate}}, CandidateDir: candidates, ReceiptDir: filepath.Join(root, "receipt")}
}

func TestHistoryAccessorAdoptionRetainsSourcesAndReversesOnlyRefs(t *testing.T) {
	dir, opts := accessorAdoptionFixture(t, false)
	ctx := context.Background()
	before := referenceMigrationFiles(t, dir)
	r, err := AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || !r.DryRun || r.Trios != 1 || r.TuplesVerified != 1024 {
		t.Fatal(r, err)
	}
	if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
		t.Fatal("read-only plan wrote files")
	}
	if _, err = os.Stat(opts.ReceiptDir); !os.IsNotExist(err) {
		t.Fatal("plan created receipt directory", err)
	}
	entry := opts.Entries[0]
	query := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0: common.AddressPrefixMainnet, 20: 1}, Generation: 1, Domain: kvdomains.ContractStorage, Key: []byte{5}}
	manager, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	want, err := manager.FirstStateDomainChangesContext(ctx, entry.History.FromTxNum, entry.History.ToTxNum, []StateHistoryQuery{query})
	if err != nil {
		t.Fatal(err)
	}
	opts.Apply = true
	r, err = AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || r.DryRun || r.AfterGeneration != 82 {
		t.Fatal(r, err)
	}
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Segments) != 3 || len(m.Retired) != 0 {
		t.Fatal("old refs enrolled in reclamation", m)
	}
	for _, ref := range []SegmentRef{entry.History, entry.Accessor, entry.Inverted} {
		if _, e := adoptionRead(ctx, dir, ref); e != nil {
			t.Fatal("old source lost", e)
		}
		oldInfo, e := os.Stat(filepath.Join(dir, ref.Path))
		if e != nil {
			t.Fatal(e)
		}
		backup, e := os.Stat(filepath.Join(opts.ReceiptDir, "old", ref.Path))
		if e != nil || !os.SameFile(oldInfo, backup) {
			t.Fatal("backup not retained", e)
		}
	}
	if _, err = PruneRetiredSegmentFiles(dir); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []SegmentRef{entry.History, entry.Accessor, entry.Inverted} {
		if _, e := adoptionRead(ctx, dir, ref); e != nil {
			t.Fatal(e)
		}
	}
	manager, err = OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.FirstStateDomainChangesContext(ctx, entry.History.FromTxNum, entry.History.ToTxNum, []StateHistoryQuery{query})
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("adoption changed row", err)
	}
	// A running node can advance progress or publish unrelated retired metadata.
	// Rollback must retain that progress rather than restore the whole backup.
	m.Generation++
	m.Progress.HistoryBuildTxNum = 2
	extra := entry.History
	extra.Path = "unrelated-retired.seg"
	m.Retired = append(m.Retired, extra)
	if err = PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	advanced := cloneManifest(m)
	r, err = AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || !r.AlreadyApplied {
		t.Fatal("resume after progress", r, err)
	}
	opts.Rollback = true
	r, err = AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || !r.RolledBack {
		t.Fatal(r, err)
	}
	after, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != advanced.Generation+1 || !reflect.DeepEqual(after.Progress, advanced.Progress) || !reflect.DeepEqual(after.Retired, advanced.Retired) {
		t.Fatal("rollback lost unrelated progress")
	}
	for path, sha := range before {
		if path == ManifestFile {
			continue
		}
		b, e := os.ReadFile(filepath.Join(dir, path))
		if e != nil || checksumBytes(b) != sha {
			t.Fatal("source changed", path, e)
		}
	}
	r, err = AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || !r.AlreadyApplied || !r.RolledBack {
		t.Fatal("rollback replay", r, err)
	}
}

func TestHistoryAccessorAdoptionResumePreparedAndCommitted(t *testing.T) {
	dir, opts := accessorAdoptionFixture(t, false)
	ctx := context.Background()
	pair := adoptionPair(opts.Entries[0])
	// Simulate an output collision after the journal becomes durable. This
	// must leave the old manifest active and permit a later bounded resume.
	obstruction := filepath.Join(dir, pair.New[1].Path)
	if e := os.Mkdir(obstruction, 0700); e != nil {
		t.Fatal(e)
	}
	opts.Apply = true
	if _, err := AdoptHistoryAccessorsV11Context(ctx, dir, opts); err == nil {
		t.Fatal("accepted directory as accessor")
	}
	m, err := LoadProductionManifest(dir)
	if err != nil || m.Generation != 81 {
		t.Fatal("failed preparation published", err)
	}
	if err = os.Remove(obstruction); err != nil {
		t.Fatal(err)
	} // Test-owned empty obstruction only.
	opts.Entries = nil
	r, err := AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || r.AfterGeneration != 82 {
		t.Fatal("prepared resume", r, err)
	}
	var j accessorAdoptionJournal
	b, err := os.ReadFile(filepath.Join(opts.ReceiptDir, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	j.Status = "prepared"
	if err = adoptionWriteJournal(opts.ReceiptDir, &j); err != nil {
		t.Fatal(err)
	}
	r, err = AdoptHistoryAccessorsV11Context(ctx, dir, opts)
	if err != nil || !r.AlreadyApplied {
		t.Fatal("uncertain publication resume", r, err)
	}
}

func TestHistoryAccessorAdoptionRejectsForgedTuplesAndPaths(t *testing.T) {
	for _, mode := range []string{"tuple", "symlink", "inside-receipt", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			dir, opts := accessorAdoptionFixture(t, mode == "tuple")
			ctx := context.Background()
			if mode == "symlink" {
				p := filepath.Join(opts.CandidateDir, opts.Entries[0].Candidate.Path)
				if e := os.Rename(p, p+".real"); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(p+".real", p); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "inside-receipt" {
				opts.ReceiptDir = filepath.Join(dir, "held")
			}
			if mode == "cancel" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			before := referenceMigrationFiles(t, dir)
			if _, err := AdoptHistoryAccessorsV11Context(ctx, dir, opts); err == nil {
				t.Fatal("accepted invalid adoption", mode)
			}
			if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
				t.Fatal("rejected plan changed files")
			}
		})
	}
}

func TestHistoryAccessorAdoptionPreparedJournalRejectsManifestConflict(t *testing.T) {
	dir, opts := accessorAdoptionFixture(t, false)
	pair := adoptionPair(opts.Entries[0])
	obstruction := filepath.Join(dir, pair.New[1].Path)
	if e := os.Mkdir(obstruction, 0700); e != nil {
		t.Fatal(e)
	}
	opts.Apply = true
	if _, e := AdoptHistoryAccessorsV11Context(context.Background(), dir, opts); e == nil {
		t.Fatal("expected preparation failure")
	}
	m, e := LoadProductionManifest(dir)
	if e != nil {
		t.Fatal(e)
	}
	m.Generation++
	m.Progress.HistoryBuildTxNum = 2
	if e = PublishManifest(dir, m); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, ManifestFile))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = AdoptHistoryAccessorsV11Context(context.Background(), dir, opts); e == nil {
		t.Fatal("overwrote conflicting manifest")
	}
	after, e := os.ReadFile(filepath.Join(dir, ManifestFile))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("manifest conflict was not retained")
	}
}
