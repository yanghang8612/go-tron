package core

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type coldSourceSnapshotStore struct {
	*pebbledb.Database
	opens, active atomic.Int32
	gate          func()
	closeError    error
}

func (d *coldSourceSnapshotStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	s, e := d.Database.NewKeyValueSnapshot()
	if e != nil {
		return nil, e
	}
	d.opens.Add(1)
	d.active.Add(1)
	return &coldSourceSnapshot{KeyValueSnapshot: s, owner: d}, nil
}

type coldSourceSnapshot struct {
	pointread.KeyValueSnapshot
	owner    *coldSourceSnapshotStore
	once     sync.Once
	closeErr error
}

func (*coldSourceSnapshot) IsPinnedKeyValueView() bool { return true }
func (s *coldSourceSnapshot) NewIterator(prefix, start []byte) ethdb.Iterator {
	if s.owner.gate != nil {
		s.owner.gate()
	}
	return s.KeyValueSnapshot.NewIterator(prefix, start)
}
func (s *coldSourceSnapshot) Close() error {
	s.once.Do(func() {
		s.closeErr = errors.Join(s.KeyValueSnapshot.Close(), s.owner.closeError)
		s.owner.active.Add(-1)
	})
	return s.closeErr
}

func testHistorySourceSnapshotBuildAppendPruneAndLifecycle(t *testing.T, withManager bool, expectedCloseErr error) {
	dir := t.TempDir()
	opts := pebbledb.DefaultOptions()
	opts.MemTableSizeBytes = 1 << 20
	opts.MaxConcurrentCompactions = 1
	open := func(name string) *coldSourceSnapshotStore {
		d, e := pebbledb.New(filepath.Join(dir, name), 16, 32, "test/cold-source/"+name+"/", false, opts)
		if e != nil {
			t.Fatal(e)
		}
		return &coldSourceSnapshotStore{Database: d}
	}
	hot, hist := open("hot"), open("history")
	hist.closeError = expectedCloseErr
	store := rawdb.NewDirectHistoryStore(hot, hist)
	defer store.Close()
	bc := &BlockChain{db: rawdb.WrapKeyValueStore(store)}

	published := filepath.Join(dir, "published")
	if withManager {
		manifest := snapshots.NewManifestForChain(0, 0, nil, snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: (common.Hash{1}).Hex()})
		if err := snapshots.PublishManifest(published, manifest); err != nil {
			t.Fatal(err)
		}
		manager, err := snapshots.OpenManager(published)
		if err != nil {
			t.Fatal(err)
		}
		bc.stateCodeColdHistory = manager
	}
	owner := common.Address{0x41, 1}
	seed := func(block uint64) error {
		batch := store.NewBatch()
		defer batch.Close()
		if err := rawdb.WriteStateTxRange(batch, block, common.Hash{byte(block)}, block, block); err != nil {
			return err
		}
		row := &rawdb.StateDomainChange{BlockNum: block, BlockHash: common.Hash{byte(block)}, Seq: 1, TxNum: block, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemDelegation, Key: []byte("cold-source-snapshot"), PrevExists: true, Prev: []byte{byte(block), 0x41, 0x27}}
		if err := rawdb.WriteStateDomainChangeBlockRows(batch, []*rawdb.StateDomainChange{row}); err != nil {
			return err
		}
		return batch.Write()
	}
	for block := uint64(1); block <= 16; block++ {
		if err := seed(block); err != nil {
			t.Fatal(err)
		}
	}
	view, release, err := bc.AcquireStateHistorySourceView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if release != nil {
			_ = release()
		}
	}()
	if hot.opens.Load() != 0 || hot.active.Load() != 0 || hist.opens.Load() != 1 || hist.active.Load() != 1 {
		t.Fatal("cold source pinned unused hot or lost history snapshot")
	}
	if withManager {
		pinned, ok := view.(interface {
			PinnedColdManifestView() snapshots.PinnedManifestView
		})
		if !ok || pinned.PinnedColdManifestView() == (snapshots.PinnedManifestView{}) {
			t.Fatal("normal manager source did not pin its manifest")
		}
		if _, ok := view.(pointread.ConcurrentOwnedKeyValueView); ok {
			t.Fatal("test unexpectedly changed baseline wrapper parallel capability")
		}
		retired, e := snapshots.PruneRetiredSegmentFilesContextWithVerifier(context.Background(), published, func(context.Context, string, *snapshots.Manifest) error { return nil })
		if e != nil || !retired.DeferredForReaders {
			t.Fatal("pinned source lost cold-file lease", e)
		}

	}

	if _, e := view.(ethdb.KeyValueReader).Get([]byte("cold-source-foreground")); !errors.Is(e, rawdb.ErrStateHistorySnapshotScope) {
		t.Fatal("source leaked hot reads")
	}
	writeStart, appendDone, gateDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	startWriter := sync.OnceFunc(func() { close(writeStart) })
	go func() {
		<-writeStart
		defer close(gateDone)
		appendErr := seed(17)
		close(appendDone)
		if appendErr != nil {
			t.Error(appendErr)
			return
		}
		start, limit, e := rawdb.StateHistoryBlockRangeBounds(1, 8)
		if e != nil {
			t.Error(e)
			return
		}
		if e = store.DeleteRange(start, limit); e != nil {
			t.Error(e)
			return
		}
		if e = store.Put([]byte("cold-source-foreground"), []byte("new-hot")); e != nil {
			t.Error(e)
			return
		}
		if err := hist.Compact(nil, nil); err != nil {
			t.Error(err)
		}
	}()
	defer func() { startWriter(); <-gateDone }()
	hist.gate = func() {
		startWriter()
		<-appendDone
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	coldDir := filepath.Join(dir, "cold")
	cached, cache, cacheErr := rawdb.AcquireSerialStateHistoryChunkCacheView(context.Background(), view)
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	defer func() {
		if cache != nil {
			_ = cache.Close()
		}
	}()
	refs, err := snapshots.BuildDiagnosticStateHistoryTrioWithETLContext(context.Background(), cached, coldDir, 1, 16, 1, 17, cfg.HistoryPath(1, 16), "3", etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	<-gateDone
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache = nil
	hist.gate = nil
	hotDigest, err := snapshots.DigestHotStateHistoryContext(context.Background(), view, filepath.Join(dir, "digest"), 1, 16, 1, 17, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	coldDigest, err := snapshots.DigestColdStateHistoryContext(context.Background(), coldDir, refs)
	if err != nil || hotDigest != coldDigest {
		t.Fatal("cold source changed during append/prune", err)
	}
	count := 0
	for block := uint64(1); block <= 16; block++ {
		if err := rawdb.IterateStateDomainChanges(view, block, func(r *rawdb.StateDomainChange) (bool, error) {
			count++
			if !bytes.Equal(r.Prev, []byte{byte(block), 0x41, 0x27}) {
				t.Fatal("independent old-history expected value differs")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if count != 16 {
		t.Fatal("old source omitted pruned rows")
	}
	if hot.opens.Load() != 0 || hist.opens.Load() != 1 {
		t.Fatal("nested cold builder took a newer source snapshot")
	}
	closeErr := release()
	release = nil
	if !errors.Is(closeErr, expectedCloseErr) {
		t.Fatal("source Close error changed", closeErr)
	}
	if withManager {
		retired, e := snapshots.PruneRetiredSegmentFilesContextWithVerifier(context.Background(), published, func(context.Context, string, *snapshots.Manifest) error { return nil })
		if e != nil || retired.DeferredForReaders {
			t.Fatal("source Close leaked manager read lease", e)
		}
	}
	if hist.active.Load() != 0 {
		t.Fatal("cold source leaked snapshot")
	}
	if _, ok, e := rawdb.ReadStateDomainChange(store, 1, 1); e != nil || ok {
		t.Fatal("current reader saw pruned row", e)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	// Preserve the existing lock acquisition contract: a canceled context
	// prevents waiting for the chain lock (an immediately available lock may win).
	bc.chainmu.Lock()
	_, cancelRelease, cancelErr := bc.AcquireStateHistorySourceView(canceled)
	bc.chainmu.Unlock()
	if cancelRelease != nil {
		_ = cancelRelease()
	}
	if !errors.Is(cancelErr, context.Canceled) {
		t.Fatal("canceled lock wait acquired source", cancelErr)
	}
	bc.closed.Store(true)
	_, closedRelease, closedErr := bc.AcquireStateHistorySourceView(context.Background())
	if closedRelease != nil {
		_ = closedRelease()
	}
	if !errors.Is(closedErr, ErrBlockChainClosed) {
		t.Fatal("closed chain acquired source", closedErr)
	}
}

func TestHistorySourceSnapshotBuildAppendPruneAndLifecycle(t *testing.T) {
	for _, withManager := range []bool{false, true} {
		name := "no-manager"
		if withManager {
			name = "pinned-manager"
		}
		t.Run(name, func(t *testing.T) { testHistorySourceSnapshotBuildAppendPruneAndLifecycle(t, withManager, nil) })
	}
}

func TestHistorySourceSnapshotManagerCloseError(t *testing.T) {
	testHistorySourceSnapshotBuildAppendPruneAndLifecycle(t, true, errors.New("cold source snapshot close failure"))
}
