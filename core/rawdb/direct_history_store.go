package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// DirectHistoryStore presents two databases as one fixed schema partition.
// Historical state rows live directly in history; all other rows live in hot.
// Both databases must be accessed for mutations through this wrapper. Existing
// single-database data must be moved offline before the partition is enabled.
//
// The mutex serializes mutations with acquisition of both engine snapshots.
// A mixed batch writes and syncs history before writing hot. This is a durability
// ordering across independent databases, not a transaction spanning them.
type DirectHistoryStore struct {
	hot, history ethdb.KeyValueStore
	mu           sync.Mutex
}

var _ ethdb.KeyValueStore = (*DirectHistoryStore)(nil)
var _ pointread.KeyValueSnapshotter = (*DirectHistoryStore)(nil)
var _ pointread.Viewer = (*DirectHistoryStore)(nil)
var _ pointread.CapacitySnapshotter = (*DirectHistoryStore)(nil)

func NewDirectHistoryStore(hot, history ethdb.KeyValueStore) *DirectHistoryStore {
	return &DirectHistoryStore{hot: hot, history: history}
}

func (d *DirectHistoryStore) store(key []byte) ethdb.KeyValueStore {
	if IsDirectHistoryKey(key) {
		return d.history
	}
	return d.hot
}
func (d *DirectHistoryStore) Get(key []byte) ([]byte, error) { return d.store(key).Get(key) }
func (d *DirectHistoryStore) Has(key []byte) (bool, error)   { return d.store(key).Has(key) }
func (d *DirectHistoryStore) GetWithPresence(key []byte) ([]byte, bool, error) {
	if reader, ok := d.store(key).(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		return reader.GetWithPresence(key)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return readPresentValue(d.store(key), key, "direct history store")
}
func (d *DirectHistoryStore) View(key []byte, fn func([]byte) error) error {
	source := d.store(key)
	if view, ok := source.(interface {
		View([]byte, func([]byte) error) error
	}); ok {
		return view.View(key, fn)
	}
	value, err := source.Get(key)
	if err != nil {
		return err
	}
	return fn(value)
}
func (d *DirectHistoryStore) IsKeyNotFound(err error) bool {
	for _, source := range []ethdb.KeyValueStore{d.hot, d.history} {
		if checker, ok := source.(interface{ IsKeyNotFound(error) bool }); ok && checker.IsKeyNotFound(err) {
			return true
		}
	}
	return false
}
func directHistoryOwned(source any) bool {
	owned, ok := source.(pointread.OwnedKeyValueReader)
	return ok && owned.GetReturnsOwnedBytes()
}
func (d *DirectHistoryStore) GetReturnsOwnedBytes() bool {
	return directHistoryOwned(d.hot) && directHistoryOwned(d.history)
}
func (d *DirectHistoryStore) Put(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.store(key).Put(key, value)
}
func (d *DirectHistoryStore) Delete(key []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.store(key).Delete(key)
}
func (d *DirectHistoryStore) DeleteRange(start, end []byte) error {
	batch := d.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(start, end); err != nil {
		return err
	}
	return batch.Write()
}
func (d *DirectHistoryStore) NewBatch() ethdb.Batch {
	return &directHistoryBatch{db: d, hot: d.hot.NewBatch(), history: d.history.NewBatch()}
}
func (d *DirectHistoryStore) NewBatchWithSize(size int) ethdb.Batch {
	// Reserving the whole capacity in both engines doubles a large flush's
	// allocation. Histories normally dominate; let hot grow when necessary.
	return &directHistoryBatch{db: d, hot: d.hot.NewBatch(), history: d.history.NewBatchWithSize(size)}
}
func (d *DirectHistoryStore) SyncKeyValue() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.history.SyncKeyValue(); err != nil {
		return err
	}
	return d.hot.SyncKeyValue()
}
func (d *DirectHistoryStore) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return errors.Join(d.history.Close(), d.hot.Close())
}
func (d *DirectHistoryStore) Stat() (string, error) {
	hot, he := d.hot.Stat()
	history, ie := d.history.Stat()
	return "hot:\n" + hot + "\nhistory:\n" + history, errors.Join(he, ie)
}

// partitionDirectHistoryRange emits disjoint portions of [start,end), with nil
// bounds retaining ethdb's unbounded meaning. It does not scan database keys.
func partitionDirectHistoryRange(start, end []byte, visit func(bool, []byte, []byte) error) error {
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	ranges := DirectHistoryRanges()
	sort.Slice(ranges, func(i, j int) bool { return bytes.Compare(ranges[i].Start, ranges[j].Start) < 0 })
	cursor := start
	for _, r := range ranges {
		if bytes.Compare(r.End, cursor) <= 0 {
			continue
		}
		if end != nil && bytes.Compare(r.Start, end) >= 0 {
			break
		}
		lo := r.Start
		if bytes.Compare(cursor, lo) > 0 {
			lo = cursor
		}
		hi := r.End
		if end != nil && bytes.Compare(end, hi) < 0 {
			hi = end
		}
		if bytes.Compare(cursor, lo) < 0 {
			if err := visit(false, cursor, lo); err != nil {
				return err
			}
		}
		if err := visit(true, lo, hi); err != nil {
			return err
		}
		cursor = hi
		if end != nil && bytes.Equal(cursor, end) {
			return nil
		}
	}
	if end == nil || bytes.Compare(cursor, end) < 0 {
		return visit(false, cursor, end)
	}
	return nil
}
func (d *DirectHistoryStore) Compact(start, end []byte) error {
	// Compaction changes physical placement only. Keeping it outside the
	// mutation/snapshot mutex lets foreground imports continue during a long
	// freezer compaction; engine lifecycle coordination handles Close.
	return partitionDirectHistoryRange(start, end, func(history bool, lo, hi []byte) error {
		if history {
			return d.history.Compact(lo, hi)
		}
		return d.hot.Compact(lo, hi)
	})
}
func (d *DirectHistoryStore) EstimateDiskUsage(start, end []byte) (uint64, error) {
	var total uint64
	err := partitionDirectHistoryRange(start, end, func(history bool, lo, hi []byte) error {
		source := d.hot
		if history {
			source = d.history
		}
		estimator, ok := source.(interface {
			EstimateDiskUsage([]byte, []byte) (uint64, error)
		})
		if !ok {
			return errors.New("rawdb: disk usage estimates unsupported")
		}
		n, err := estimator.EstimateDiskUsage(lo, hi)
		total += n
		return err
	})
	return total, err
}
func (d *DirectHistoryStore) MaintenancePressure() maintenance.StoragePressure {
	var result maintenance.StoragePressure
	for _, source := range []ethdb.KeyValueStore{d.hot, d.history} {
		if sampler, ok := source.(interface {
			MaintenancePressure() maintenance.StoragePressure
		}); ok {
			p := sampler.MaintenancePressure()
			result = mergeDirectHistoryPressure(result, p)
		}
	}
	return result
}

// Thresholds are normalized to the larger observed count so the one public
// sample preserves the worst occupancy ratio of either engine. Rounding down
// a normalized threshold is conservative for maintenance admission.
func directHistoryPressureThreshold(count int64, threshold int, scale int64) int {
	if count <= 0 || threshold <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(threshold), uint64(scale))
	maxInt := uint64(^uint(0) >> 1)
	if hi >= uint64(count) {
		return int(maxInt)
	}
	quotient, _ := bits.Div64(hi, lo, uint64(count))
	if quotient > maxInt {
		return int(maxInt)
	}
	return int(quotient)
}
func directHistoryPressureMinThreshold(a, b int) int {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	return min(a, b)
}
func mergeDirectHistoryPressure(a, b maintenance.StoragePressure) maintenance.StoragePressure {
	if !b.Available {
		return a
	}
	if !a.Available {
		return b
	}
	result := a
	if b.SampledAt.Before(result.SampledAt) {
		result.SampledAt = b.SampledAt
	}
	result.WriteStalled = a.WriteStalled || b.WriteStalled
	result.StallCount = a.StallCount + b.StallCount
	result.StallDuration = a.StallDuration + b.StallDuration
	result.CompactionDebt = a.CompactionDebt + b.CompactionDebt
	result.L0Sublevels = max(a.L0Sublevels, b.L0Sublevels)
	result.L0CompactionThreshold = directHistoryPressureMinThreshold(directHistoryPressureThreshold(int64(a.L0Sublevels), a.L0CompactionThreshold, int64(result.L0Sublevels)), directHistoryPressureThreshold(int64(b.L0Sublevels), b.L0CompactionThreshold, int64(result.L0Sublevels)))
	result.L0StopWritesThreshold = directHistoryPressureMinThreshold(directHistoryPressureThreshold(int64(a.L0Sublevels), a.L0StopWritesThreshold, int64(result.L0Sublevels)), directHistoryPressureThreshold(int64(b.L0Sublevels), b.L0StopWritesThreshold, int64(result.L0Sublevels)))
	result.MemTableCount = max(a.MemTableCount, b.MemTableCount)
	result.MemTableStopWritesThreshold = directHistoryPressureMinThreshold(directHistoryPressureThreshold(a.MemTableCount, a.MemTableStopWritesThreshold, result.MemTableCount), directHistoryPressureThreshold(b.MemTableCount, b.MemTableStopWritesThreshold, result.MemTableCount))
	return result
}

type directHistoryBatch struct {
	db                     *DirectHistoryStore
	hot, history           ethdb.Batch
	hotDirty, historyDirty bool
}

func (b *directHistoryBatch) target(history bool) ethdb.Batch {
	if history {
		b.historyDirty = true
		return b.history
	}
	b.hotDirty = true
	return b.hot
}
func (b *directHistoryBatch) Put(key, value []byte) error {
	return b.target(IsDirectHistoryKey(key)).Put(key, value)
}
func (b *directHistoryBatch) PutString(key string, value []byte) error {
	target := b.target(isDirectHistoryKeyString(key))
	if writer, ok := target.(interface{ PutString(string, []byte) error }); ok {
		return writer.PutString(key, value)
	}
	return target.Put([]byte(key), value)
}
func (b *directHistoryBatch) PutOwnedValue(key, value []byte) error { return b.Put(key, value) }
func (b *directHistoryBatch) PutStringOwnedValue(key string, value []byte) error {
	return b.PutString(key, value)
}
func (b *directHistoryBatch) PutOwnedKeyValue(key, value []byte) error { return b.Put(key, value) }
func (b *directHistoryBatch) PutValueFunc(key []byte, size int, fill func([]byte) error) error {
	target := b.target(IsDirectHistoryKey(key))
	if writer, ok := target.(interface {
		PutValueFunc([]byte, int, func([]byte) error) error
	}); ok {
		return writer.PutValueFunc(key, size, fill)
	}
	value := make([]byte, size)
	if err := fill(value); err != nil {
		return err
	}
	return target.Put(key, value)
}
func (b *directHistoryBatch) Delete(key []byte) error {
	return b.target(IsDirectHistoryKey(key)).Delete(key)
}
func (b *directHistoryBatch) DeleteString(key string) error {
	target := b.target(isDirectHistoryKeyString(key))
	if writer, ok := target.(interface{ DeleteString(string) error }); ok {
		return writer.DeleteString(key)
	}
	return target.Delete([]byte(key))
}
func (b *directHistoryBatch) DeleteRange(start, end []byte) error {
	return partitionDirectHistoryRange(start, end, func(history bool, lo, hi []byte) error { return b.target(history).DeleteRange(lo, hi) })
}
func (b *directHistoryBatch) ValueSize() int { return b.hot.ValueSize() + b.history.ValueSize() }
func (b *directHistoryBatch) Write() error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	if b.historyDirty {
		if err := b.history.Write(); err != nil {
			return fmt.Errorf("write direct history: %w", err)
		}
		if b.hotDirty {
			if err := b.db.history.SyncKeyValue(); err != nil {
				return fmt.Errorf("sync direct history before hot write: %w", err)
			}
		}
	}
	if b.hotDirty {
		return b.hot.Write()
	}
	return nil
}
func (b *directHistoryBatch) Reset() {
	b.hot.Reset()
	b.history.Reset()
	b.hotDirty = false
	b.historyDirty = false
}
func (b *directHistoryBatch) Close() { b.hot.Close(); b.history.Close() }
func (b *directHistoryBatch) Replay(w ethdb.KeyValueWriter) error {
	if err := b.history.Replay(w); err != nil {
		return err
	}
	return b.hot.Replay(w)
}

// Engine iterators each pin their own sequence at creation. Hold the shared
// mutation lock until both have been created, then merge their visible keys.
func (d *DirectHistoryStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	d.mu.Lock()
	defer d.mu.Unlock()
	return newDirectHistoryIterator(d.hot, d.history, prefix, start)
}
func (d *DirectHistoryStore) SeekPrefix(prefix, start []byte) ([]byte, []byte, bool, error) {
	return directHistorySeek(d, prefix, start)
}
func directHistorySeek(source ethdb.Iteratee, prefix, start []byte) ([]byte, []byte, bool, error) {
	it := source.NewIterator(prefix, start)
	defer it.Release()
	if !it.Next() {
		return nil, nil, false, it.Error()
	}
	return bytes.Clone(it.Key()), bytes.Clone(it.Value()), true, it.Error()
}

type directHistoryIterator struct {
	hot, history                              ethdb.Iterator
	hotValid, historyValid, started, released bool
	current                                   ethdb.Iterator
}

func newDirectHistoryIterator(hot, history ethdb.Iteratee, prefix, start []byte) ethdb.Iterator {
	// The usual exact-family scans only touch their owning engine. A broader
	// prefix (including stage-progress) needs the ordered merge below.
	lower := append(bytes.Clone(prefix), start...)
	upper := prefixUpperBound(prefix)
	intersects := false
	for _, r := range DirectHistoryRanges() {
		if bytes.Compare(lower, r.End) >= 0 || (upper != nil && bytes.Compare(upper, r.Start) <= 0) {
			continue
		}
		intersects = true
		if bytes.Compare(lower, r.Start) >= 0 && upper != nil && bytes.Compare(upper, r.End) <= 0 {
			return history.NewIterator(prefix, start)
		}
	}
	if !intersects {
		return hot.NewIterator(prefix, start)
	}
	return &directHistoryIterator{hot: hot.NewIterator(prefix, start), history: history.NewIterator(prefix, start)}
}
func directHistoryIteratorNext(it ethdb.Iterator, history bool) bool {
	for it.Next() {
		if IsDirectHistoryKey(it.Key()) == history {
			return true
		}
	}
	return false
}
func (it *directHistoryIterator) Next() bool {
	if it.released {
		return false
	}
	if !it.started {
		it.hotValid = directHistoryIteratorNext(it.hot, false)
		it.historyValid = directHistoryIteratorNext(it.history, true)
		it.started = true
	} else if it.current == it.hot {
		it.hotValid = directHistoryIteratorNext(it.hot, false)
	} else if it.current == it.history {
		it.historyValid = directHistoryIteratorNext(it.history, true)
	}
	it.current = nil
	if it.hotValid && (!it.historyValid || bytes.Compare(it.hot.Key(), it.history.Key()) < 0) {
		it.current = it.hot
	} else if it.historyValid {
		it.current = it.history
	}
	return it.current != nil
}
func (it *directHistoryIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.Key()
}
func (it *directHistoryIterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.Value()
}
func (it *directHistoryIterator) Error() error {
	return errors.Join(it.hot.Error(), it.history.Error())
}
func (it *directHistoryIterator) Release() {
	if !it.released {
		it.hot.Release()
		it.history.Release()
		it.released = true
		it.current = nil
	}
}

type directHistorySnapshot struct{ hot, history pointread.KeyValueSnapshot }

func (d *DirectHistoryStore) SupportsKeyValueSnapshots() bool {
	return pointread.SupportsKeyValueSnapshots(d.hot) && pointread.SupportsKeyValueSnapshots(d.history)
}
func (d *DirectHistoryStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	hotFactory, ho := d.hot.(pointread.KeyValueSnapshotter)
	historyFactory, io := d.history.(pointread.KeyValueSnapshotter)
	if !ho || !io {
		return nil, pointread.ErrKeyValueSnapshotUnsupported
	}
	hot, err := hotFactory.NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	history, err := historyFactory.NewKeyValueSnapshot()
	if err != nil {
		return nil, errors.Join(err, hot.Close())
	}
	return &directHistorySnapshot{hot: hot, history: history}, nil
}
func (s *directHistorySnapshot) reader(key []byte) pointread.KeyValueSnapshot {
	if IsDirectHistoryKey(key) {
		return s.history
	}
	return s.hot
}
func (s *directHistorySnapshot) Get(key []byte) ([]byte, error) { return s.reader(key).Get(key) }
func (s *directHistorySnapshot) Has(key []byte) (bool, error)   { return s.reader(key).Has(key) }
func (s *directHistorySnapshot) NewIterator(prefix, start []byte) ethdb.Iterator {
	return newDirectHistoryIterator(s.hot, s.history, prefix, start)
}
func (s *directHistorySnapshot) SeekPrefix(prefix, start []byte) ([]byte, []byte, bool, error) {
	return directHistorySeek(s, prefix, start)
}
func (s *directHistorySnapshot) Close() error             { return errors.Join(s.history.Close(), s.hot.Close()) }
func (*directHistorySnapshot) IsPinnedKeyValueView() bool { return true }
func (s *directHistorySnapshot) GetReturnsOwnedBytes() bool {
	return directHistoryOwned(s.hot) && directHistoryOwned(s.history)
}
func (s *directHistorySnapshot) ConcurrentOwnedHistoryReads() bool {
	hot, ho := s.hot.(pointread.ConcurrentOwnedKeyValueView)
	history, io := s.history.(pointread.ConcurrentOwnedKeyValueView)
	return ho && io && hot.ConcurrentOwnedHistoryReads() && history.ConcurrentOwnedHistoryReads()
}

type directHistoryPointView struct{ hot, history pointread.View }

func (d *DirectHistoryStore) NewPointReadView() (pointread.View, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	hf, ho := d.hot.(pointread.Viewer)
	inf, io := d.history.(pointread.Viewer)
	if !ho || !io {
		return nil, errors.New("rawdb: point read views unsupported")
	}
	hot, err := hf.NewPointReadView()
	if err != nil {
		return nil, err
	}
	history, err := inf.NewPointReadView()
	if err != nil {
		return nil, errors.Join(err, hot.Close())
	}
	return &directHistoryPointView{hot: hot, history: history}, nil
}
func (v *directHistoryPointView) Get(key []byte, fn func([]byte) error) error {
	if IsDirectHistoryKey(key) {
		return v.history.Get(key, fn)
	}
	return v.hot.Get(key, fn)
}
func (v *directHistoryPointView) Close() error { return errors.Join(v.history.Close(), v.hot.Close()) }

type directHistoryPointSnapshot struct{ hot, history pointread.Snapshot }

func (d *DirectHistoryStore) NewPointReadSnapshot() (pointread.Snapshot, error) {
	return d.NewPointReadSnapshotWithCapacity(0)
}
func (d *DirectHistoryStore) NewPointReadSnapshotWithCapacity(capacity int) (pointread.Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	open := func(source any) (pointread.Snapshot, error) {
		if factory, ok := source.(pointread.CapacitySnapshotter); ok {
			return factory.NewPointReadSnapshotWithCapacity(capacity)
		}
		if factory, ok := source.(pointread.Snapshotter); ok {
			return factory.NewPointReadSnapshot()
		}
		return nil, errors.New("rawdb: point read snapshots unsupported")
	}
	hot, err := open(d.hot)
	if err != nil {
		return nil, err
	}
	history, err := open(d.history)
	if err != nil {
		return nil, errors.Join(err, hot.Close())
	}
	return &directHistoryPointSnapshot{hot: hot, history: history}, nil
}
func (s *directHistoryPointSnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	// Both engine snapshots were already pinned under the mutation lock. Omit
	// only the cursor whose prefix range cannot contain a routed key.
	if history, exclusive := directHistoryPrefixPartition(prefix); exclusive {
		if history {
			return s.history.NewCursor(prefix)
		}
		return s.hot.NewCursor(prefix)
	}

	hot, err := s.hot.NewCursor(prefix)
	if err != nil {
		return nil, err
	}
	history, err := s.history.NewCursor(prefix)
	if err != nil {
		return nil, errors.Join(err, hot.Close())
	}
	return &directHistoryCursor{hot: hot, history: history}, nil
}
func (s *directHistoryPointSnapshot) Close() error {
	return errors.Join(s.history.Close(), s.hot.Close())
}

type directHistoryCursor struct{ hot, history pointread.Cursor }

func (c *directHistoryCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if IsDirectHistoryKey(key) {
		return c.history.View(key, fn)
	}
	return c.hot.View(key, fn)
}
func (c *directHistoryCursor) Close() error { return errors.Join(c.history.Close(), c.hot.Close()) }
