package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Cancel synchronously at an observable phase boundary, without timers or a
// production-only test hook. WithAttrs preserves the hook for module loggers.
type historyCancelLogHandler struct {
	slog.Handler
	onRecord func(slog.Record)
}

func (h historyCancelLogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.onRecord(record)
	return h.Handler.Handle(ctx, record)
}

func (h historyCancelLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h historyCancelLogHandler) WithGroup(name string) slog.Handler {
	h.Handler = h.Handler.WithGroup(name)
	return h
}

func TestCompactHistoryCancellationPreservesManifestAndInputs(t *testing.T) {
	for _, phase := range []string{"validate-sources", "collect-keys", "build-dictionary", "write-tx-ranges", "copy-records", "finalize-history", "verify-history", "build-accessor", "before-manifest"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
			refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
			if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			files := historyCancellationFiles(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			previous := gtronlog.Root()
			defer gtronlog.SetDefault(previous)
			var reached atomic.Bool
			gtronlog.SetDefault(gtronlog.NewLogger(historyCancelLogHandler{
				Handler: slog.NewTextHandler(io.Discard, nil),
				onRecord: func(record slog.Record) {
					match := phase == "before-manifest" && record.Message == "History cold snapshot compaction completed"
					if record.Message == "History cold snapshot compaction phase started" {
						record.Attrs(func(attr slog.Attr) bool {
							match = match || attr.Key == "phase" && attr.Value.String() == phase
							return true
						})
					}
					if match {
						reached.Store(true)
						cancel()
					}
				},
			}))
			result, err := CompactHistoryDomainContext(ctx, dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true})
			if !reached.Load() || !errors.Is(err, context.Canceled) || result.Merged {
				t.Fatalf("reached=%v result=%+v err=%v, want canceled unpublished merge", reached.Load(), result, err)
			}
			after, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("cancellation changed manifest: %v", err)
			}
			if got := historyCancellationFiles(t, dir); !reflect.DeepEqual(files, got) {
				t.Fatalf("cancellation leaked or removed files:\nbefore %v\nafter  %v", files, got)
			}
			gtronlog.SetDefault(previous)
			result, err = CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true})
			if err != nil || !result.Merged {
				t.Fatalf("retry after cancellation = %+v, %v", result, err)
			}
			manifest, err := LoadProductionManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyHistorySegmentWithCompanions(dir, manifest, compactionRefByKind(t, result, SegmentHistory)); err != nil {
				t.Fatalf("retry produced invalid history: %v", err)
			}
		})
	}
}

type historyCancelCheckContext struct {
	context.Context
	cancel context.CancelFunc
	check  func() bool
}

func (c *historyCancelCheckContext) Err() error {
	if c.check() {
		c.cancel()
	}
	return c.Context.Err()
}

func historyCancelAfterChecks(t *testing.T, checks int64) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var remaining atomic.Int64
	remaining.Store(checks)
	return &historyCancelCheckContext{Context: ctx, cancel: cancel, check: func() bool { return remaining.Add(-1) <= 0 }}
}

func TestHistoryCompactionCancelsDuringRecordCopy(t *testing.T) {
	const records = 8192
	dir := t.TempDir()
	changes := make([]*rawdb.StateDomainChange, records)
	for i := range changes {
		changes[i] = binaryStateDomainChange(uint64(i+1), uint64(i+1), 1, "shared-key")
	}
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, records, changes...)
	files := historyCancellationFiles(t, dir)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	selection := historyCompactionSelection{fromTxNum: 1, toTxNum: records, aggregationSteps: 2,
		candidates: []historyCompactionCandidate{{history: refs[0], companions: refs[1:]}}}
	progress := newHistoryCompactionProgress(cfg.Dataset, 1, records, 1)
	defer progress.finish(context.Canceled)
	sources, err := collectStateDomainChangeBinaryCompactionSources(context.Background(), dir, selection, progress)
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &historyCancelCheckContext{Context: parent, cancel: cancel, check: func() bool {
		return progress.phase.Load() == historyCompactionPhaseCopyRecords && progress.recordsProcessed.Load() >= 4096
	}}
	_, _, _, err = writeCompactedStateDomainChangeBinaryFiles(ctx, dir, cfg, selection, sources, progress)
	if !errors.Is(err, context.Canceled) || progress.recordsProcessed.Load() != 4096 {
		t.Fatalf("copy error=%v records=%d, want canceled at 4096", err, progress.recordsProcessed.Load())
	}
	if got := historyCancellationFiles(t, dir); !reflect.DeepEqual(files, got) {
		t.Fatalf("copy cancellation changed files: before=%v after=%v", files, got)
	}
}

func TestHistoryV6BuildCancelsDuringETL(t *testing.T) {
	const records = 8192
	for _, spill := range []bool{false, true} {
		for _, phase := range []string{"dictionary", "accessor"} {
			t.Run(fmt.Sprintf("%s/spill=%v", phase, spill), func(t *testing.T) {
				dir := t.TempDir()
				limit := 8 << 20
				if spill {
					limit = 16 << 10
				}
				build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: limit}, dir, "history/test.seg")
				if err != nil {
					t.Fatal(err)
				}
				defer build.Close()
				for i := 0; i < records; i++ {
					key := "shared-key"
					if phase == "dictionary" {
						key = fmt.Sprintf("key-%08d", i)
					}
					if err := build.CollectKey(binaryStateDomainChange(uint64(i+1), uint64(i+1), 1, key)); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "dictionary" {
					if got := build.keys.Stats().SpilledRuns > 0; got != spill {
						t.Fatalf("dictionary spilled=%v, want %v", got, spill)
					}
					err = build.FinishDictionaryContext(historyCancelAfterChecks(t, 6))
					if build.keyCount == 0 || build.keys == nil {
						t.Fatal("did not cancel in the middle of dictionary ETL")
					}
				} else {
					if err := build.FinishDictionary(); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < records; i++ {
						if err := build.CollectPosting(0, uint64(i+1), uint64(i+100), uint64(i)); err != nil {
							t.Fatal(err)
						}
					}
					if got := build.postings.Stats().SpilledRuns > 0; got != spill {
						t.Fatalf("postings spilled=%v, want %v", got, spill)
					}
					_, _, err = build.BuildAccessorContext(historyCancelAfterChecks(t, 6), dir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: 1, ToTxNum: records, Path: "history/test.kv"}, records)
					if build.postings == nil {
						t.Fatal("did not cancel before posting ETL completed")
					}
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("build error = %v, want context.Canceled", err)
				}
				build.Close()
				if files := historyCancellationFiles(t, dir); len(files) != 0 {
					t.Fatalf("canceled ETL left files: %v", files)
				}
			})
		}
	}
}

func TestHistoryIndexRewriteCancellationKeepsInputOpen(t *testing.T) {
	dir := t.TempDir()
	file, name, err := createStateDomainChangeBinaryTempFile(dir, "test.idx")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := writeStateDomainChangeBinaryHeaderTo(file, stateDomainChangeBinaryIndexMagic, 1, 1024, 1024); err != nil {
		t.Fatal(err)
	}
	// Cancellation occurs before decoding the second frame; valid entries are
	// emitted using the same staging writer as history builds.
	for i := uint64(0); i < 1024; i++ {
		var raw [stateDomainChangeBinaryIndexEntrySize]byte
		binary.BigEndian.PutUint64(raw[0:8], i+1)
		binary.BigEndian.PutUint64(raw[8:16], i+100)
		binary.BigEndian.PutUint64(raw[16:24], i)
		binary.BigEndian.PutUint64(raw[24:32], 1)
		if _, err := file.Write(raw[:]); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = rewriteStateDomainChangeBinaryIndexV7Context(historyCancelAfterChecks(t, 4), file, name)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("rewrite error = %v", err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("rewrite lost original file ownership: %v", err)
	}
	if files := historyCancellationFiles(t, dir); len(files) != 1 || files[0] != filepath.Base(name) {
		t.Fatalf("rewrite leaked files: %v", files)
	}
}

func TestHistoryCompressedFinalizationCancelsDuringAssembly(t *testing.T) {
	dir := t.TempDir()
	tmp, err := createStateDomainChangeHistoryTemp(dir, "history/test.seg", true)
	if err != nil {
		t.Fatal(err)
	}
	defer tmp.Close()
	payload := make([]byte, 4<<20)
	_, _ = rand.New(rand.NewSource(42)).Read(payload)
	if _, err := tmp.Write(payload); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var copied atomic.Int64
	ctx := &historyCancelCheckContext{Context: parent, cancel: cancel, check: func() bool {
		stat, err := os.Stat(tmp.tmpName)
		if err == nil && stat.Size() > 1<<20 {
			copied.Store(stat.Size())
			return true
		}
		return false
	}}
	_, err = tmp.FinalizeContext(ctx, SegmentRef{Path: "history/test.seg"}, true)
	if !errors.Is(err, context.Canceled) || copied.Load() == 0 {
		t.Fatalf("assembly error=%v copied=%d, want cancellation after partial output", err, copied.Load())
	}
	tmp.Close()
	if files := historyCancellationFiles(t, dir); len(files) != 0 {
		t.Fatalf("canceled compressed assembly left files: %v", files)
	}
}

func historyCancellationFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			files = append(files, rel)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}
