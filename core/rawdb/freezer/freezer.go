// Copyright 2019 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Vendored from go-ethereum/core/rawdb/freezer.go and adapted for gtron's
// package layout. Material changes from upstream:
//   - package rename (rawdb -> freezer)
//   - logger swap (geth log -> gtron's common/log facade)
//   - simplified write-op signature: `AncientWriteOp` is gtron-local and
//     only exposes `AppendRaw` since slice 1 of the chain-freezer stores
//     pre-encoded protobuf / raw-byte blobs
//   - public `HasAncient`, `AncientCount(kind)` helpers added so the gtron-side
//     `AncientReader` interface can be implemented without callers reaching
//     into private fields. `AncientCount` reads the per-table `items` atomic
//     (the same field `Retrieve` consults) instead of `f.head`, so the count
//     stays consistent with what `Retrieve` will serve if a `TruncateHead`
//     loop is interrupted partway through.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

package freezer

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/gofrs/flock"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

// FreezerTableSize defines the maximum size of a freezer data file (one
// shard). Above this threshold a new file is opened.
//
// 2 * 1024 * 1024 * 1024 = 2 GiB per the chain-freezer design doc. Upstream
// go-ethereum uses 2 * 1000 * 1000 * 1000 (2 GB); we intentionally pick the
// binary-power value because the spec spells it out as 2 GiB.
const FreezerTableSize uint32 = 2 * 1024 * 1024 * 1024

const repairStatsFilename = "repair.json"

var (
	// errReadOnly is returned if the freezer is opened in read only mode. All the
	// mutations are disallowed.
	errReadOnly = errors.New("read only")

	// errSymlinkDatadir is returned if the ancient directory specified by user
	// is a symbolic link.
	errSymlinkDatadir = errors.New("symbolic link datadir is not supported")

	// ErrV2SourcePruned reports that immutable V2 coverage can no longer be
	// extended from the local V1 tables because their virtual tail has already
	// advanced past the published V2 prefix. Cold snapshots may still serve the
	// missing history, but an online V1-to-V2 migration cannot reconstruct it.
	ErrV2SourcePruned = errors.New("ancient V2 source has been pruned")
)

// AncientWriteOp is implemented by the batch handed to `ModifyAncients`.
//
// Slice 1 only needs raw-byte appends — RLP encoding is geth-specific and
// gtron tables hold pre-marshalled proto bytes anyway. The interface lives
// here next to its only producer; the parent rawdb package re-exports it.
type AncientWriteOp interface {
	AppendRaw(kind string, number uint64, item []byte) error
}

// TableConfig is the public alias of freezerTableConfig — callers configure
// tables when opening a freezer.
type TableConfig struct {
	NoSnappy bool // disables item compression
	Prunable bool // true for tables that can be pruned by TruncateTail
}

func (c TableConfig) toInternal() freezerTableConfig {
	return freezerTableConfig{noSnappy: c.NoSnappy, prunable: c.Prunable}
}

// Freezer is an append-only database to store immutable ordered data into
// flat files:
//
//   - The append-only nature ensures that disk writes are minimized.
//   - The in-order data ensures that disk reads are always optimized.
type Freezer struct {
	datadir string
	head    atomic.Uint64 // Number of items stored (including items removed from tail)
	tail    atomic.Uint64 // Number of the first stored item in the freezer

	// This lock synchronizes writers and the truncate operation, as well as
	// the "atomic" (batched) read operations.
	writeLock  sync.RWMutex
	writeBatch *freezerBatch
	v2Mu       sync.RWMutex
	v2Migrate  sync.Mutex
	// tailMutation prevents an external snapshot tail-prune from advancing the
	// V1 source while MigrateV2 is writing and verifying a segment. MigrateV2
	// uses truncateTailLocked for its own post-publication reclamation.
	tailMutation sync.Mutex

	readonly      bool
	tables        map[string]*freezerTable // Data tables for storing everything
	v2            *v2Store                 // Immutable Zstd segment prefix, if present
	txIndex       *TransactionIndexStore   // Immutable transaction hash -> location runs
	repairStats   RepairStats
	repairMetrics freezerRepairMetrics
	instanceLock  *flock.Flock // File-system lock to prevent double opens
	closeOnce     sync.Once
}

// Stats is a stable, read-only snapshot of freezer-wide and per-table bounds.
// Head is the append head (exclusive), Tail is the freezer-wide virtual tail
// enforced by TruncateTail, and Tables records each table's physical and
// virtual tail state.
type Stats struct {
	Datadir    string
	ReadOnly   bool
	Head       uint64
	Tail       uint64
	V2Coverage uint64
	Tables     []TableStats
	Repair     RepairStats
}

// RepairStats describes the most recent writable-open repair pass. Applied is
// true when at least one table was truncated to the common freezer bounds.
type RepairStats struct {
	Applied    bool               `json:"applied"`
	TargetHead uint64             `json:"targetHead"`
	TargetTail uint64             `json:"targetTail"`
	RecordedAt string             `json:"recordedAt,omitempty"`
	Tables     []TableRepairStats `json:"tables,omitempty"`
}

// TableRepairStats records one table bound change made by freezer repair.
type TableRepairStats struct {
	Name             string `json:"name"`
	HeadBefore       uint64 `json:"headBefore"`
	HeadAfter        uint64 `json:"headAfter"`
	HiddenTailBefore uint64 `json:"hiddenTailBefore"`
	HiddenTailAfter  uint64 `json:"hiddenTailAfter"`
}

func (s RepairStats) clone() RepairStats {
	out := s
	out.Tables = append([]TableRepairStats(nil), s.Tables...)
	return out
}

type freezerRepairMetrics struct {
	applied    *metrics.Gauge
	tables     *metrics.Gauge
	targetHead *metrics.Gauge
	targetTail *metrics.Gauge
	recordedAt *metrics.Gauge
	events     *metrics.Counter
}

func newFreezerRepairMetrics(namespace string) freezerRepairMetrics {
	prefix := namespace + "ancient/repair/"
	return freezerRepairMetrics{
		applied:    metrics.GetOrRegisterGauge(prefix+"applied", nil),
		tables:     metrics.GetOrRegisterGauge(prefix+"tables", nil),
		targetHead: metrics.GetOrRegisterGauge(prefix+"target/head", nil),
		targetTail: metrics.GetOrRegisterGauge(prefix+"target/tail", nil),
		recordedAt: metrics.GetOrRegisterGauge(prefix+"recorded", nil),
		events:     metrics.GetOrRegisterCounter(prefix+"events", nil),
	}
}

func (m freezerRepairMetrics) update(repair RepairStats, countEvent bool) {
	applied := int64(0)
	if repair.Applied {
		applied = 1
	}
	m.applied.Update(applied)
	m.tables.Update(int64(len(repair.Tables)))
	m.targetHead.Update(uint64MetricValue(repair.TargetHead))
	m.targetTail.Update(uint64MetricValue(repair.TargetTail))
	m.recordedAt.Update(repairRecordedUnix(repair.RecordedAt))
	if countEvent && repair.Applied {
		m.events.Inc(1)
	}
}

func repairRecordedUnix(recordedAt string) int64 {
	if recordedAt == "" {
		return 0
	}
	ts, err := time.Parse(time.RFC3339Nano, recordedAt)
	if err != nil {
		return 0
	}
	return ts.Unix()
}

func uint64MetricValue(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// TableStats describes one freezer table's storage bounds. PhysicalTail is the
// first row still backed by local data files, while HiddenTail is the virtual
// tail used to hide rows after TruncateTail and before/after physical shard
// reclamation. VisibleSize excludes HiddenSize.
type TableStats struct {
	Name         string
	Head         uint64
	PhysicalTail uint64
	HiddenTail   uint64
	Prunable     bool
	NoSnappy     bool
	TailFile     uint32
	HeadFile     uint32
	HeadBytes    int64
	VisibleSize  uint64
	HiddenSize   uint64
	V2Size       uint64
}

// NewFreezer creates a freezer instance for maintaining immutable ordered
// data according to the given parameters.
//
// The 'tables' argument defines the freezer tables and their configuration.
// Each value is a TableConfig specifying whether snappy compression is
// disabled (NoSnappy) and whether the table is prunable from the virtual tail.
func NewFreezer(datadir string, namespace string, readonly bool, maxTableSize uint32, tables map[string]TableConfig) (*Freezer, error) {
	// Create the initial freezer object
	var (
		readMeter  = metrics.NewRegisteredMeter(namespace+"ancient/read", nil)
		writeMeter = metrics.NewRegisteredMeter(namespace+"ancient/write", nil)
		sizeGauge  = metrics.NewRegisteredGauge(namespace+"ancient/size", nil)
	)
	// Ensure the datadir is not a symbolic link if it exists.
	if info, err := os.Lstat(datadir); !os.IsNotExist(err) {
		if info == nil {
			gtronlog.Warn("Could not Lstat the database", "path", datadir)
			return nil, errors.New("lstat failed")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			gtronlog.Warn("Symbolic link ancient database is not supported", "path", datadir)
			return nil, errSymlinkDatadir
		}
	}
	// Leveldb/Pebble uses LOCK as the filelock filename. To prevent the
	// name collision, we use FLOCK as the lock name.
	flockFile := filepath.Join(datadir, "FLOCK")
	if err := os.MkdirAll(filepath.Dir(flockFile), 0755); err != nil {
		return nil, err
	}
	lock := flock.New(flockFile)
	tryLock := lock.TryLock
	if readonly {
		tryLock = lock.TryRLock
	}
	if locked, err := tryLock(); err != nil {
		return nil, err
	} else if !locked {
		return nil, errors.New("locking failed")
	}
	// Open all the supported data tables
	freezer := &Freezer{
		datadir:       datadir,
		readonly:      readonly,
		tables:        make(map[string]*freezerTable),
		repairMetrics: newFreezerRepairMetrics(namespace),
		instanceLock:  lock,
	}

	// Create the tables.
	for name, config := range tables {
		table, err := newTable(datadir, name, readMeter, writeMeter, sizeGauge, maxTableSize, config.toInternal(), readonly)
		if err != nil {
			for _, table := range freezer.tables {
				table.Close()
			}
			lock.Unlock()
			return nil, err
		}
		freezer.tables[name] = table
	}
	var err error
	if freezer.readonly {
		// In readonly mode only validate, don't truncate.
		// validate also sets `freezer.frozen`.
		err = freezer.validate()
	} else {
		// Truncate all tables to common length.
		err = freezer.repair()
	}
	if err != nil {
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Unlock()
		return nil, err
	}
	if !freezer.repairStats.Applied {
		freezer.loadPersistedRepairStats()
	}
	freezer.repairMetrics.update(freezer.repairStats, false)
	freezer.v2, err = openV2Store(datadir)
	if err != nil {
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Unlock()
		return nil, fmt.Errorf("open ancient V2: %w", err)
	}
	if coverage := freezer.v2.coverage; coverage > freezer.head.Load() {
		// Fresh-sync direct V2 leaves the legacy V1 tables fully reclaimed at
		// their last head. The immutable manifest prefix is then the durable
		// logical head and survives a crash without separate metadata.
		for kind, table := range freezer.tables {
			if table.itemHidden.Load() != table.items.Load() {
				freezer.v2.Close()
				for _, opened := range freezer.tables {
					opened.Close()
				}
				lock.Unlock()
				return nil, fmt.Errorf("ancient V2 coverage %d exceeds live V1 table %s head %d tail %d", coverage, kind, table.items.Load(), table.itemHidden.Load())
			}
		}
		freezer.head.Store(coverage)
	} else if coverage > 0 && freezer.tail.Load() > coverage {
		freezer.v2.Close()
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Unlock()
		return nil, fmt.Errorf("ancient V2 coverage %d leaves a gap before V1 tail %d", coverage, freezer.tail.Load())
	}
	freezer.txIndex, err = OpenTransactionIndexStore(datadir)
	if err != nil {
		freezer.v2.Close()
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Unlock()
		return nil, fmt.Errorf("open ancient transaction index: %w", err)
	}
	if coverage := freezer.txIndex.Coverage(); coverage > freezer.V2Coverage() {
		freezer.txIndex.Close()
		freezer.v2.Close()
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Unlock()
		return nil, fmt.Errorf("transaction index coverage %d exceeds ancient V2 coverage %d", coverage, freezer.V2Coverage())
	}
	if !readonly {
		cleanup, cleanupErr := cleanupUnreferencedTransactionIndexRuns(datadir, freezer.txIndex)
		if cleanupErr != nil {
			// Orphan cleanup is a space-reclamation repair. A transient unlink or
			// directory-sync error must not make an otherwise valid index unusable;
			// regular index maintenance retries the same cleanup.
			gtronlog.Warn("Could not clean unreferenced transaction index runs", "database", datadir, "err", cleanupErr)
		} else if cleanup.Files > 0 {
			gtronlog.Info("Cleaned unreferenced transaction index runs", "database", datadir, "files", cleanup.Files, "bytes", cleanup.Bytes)
		}
	}

	// Create the write batch.
	freezer.writeBatch = newFreezerBatch(freezer)

	gtronlog.Info("Opened ancient database", "database", datadir, "readonly", readonly)
	return freezer, nil
}

// Close terminates the chain freezer, closing all the data files.
func (f *Freezer) Close() error {
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	var errs []error
	f.closeOnce.Do(func() {
		f.v2Mu.Lock()
		if f.v2 != nil {
			if err := f.v2.Close(); err != nil {
				errs = append(errs, err)
			}
			f.v2 = nil
		}
		if f.txIndex != nil {
			if err := f.txIndex.Close(); err != nil {
				errs = append(errs, err)
			}
			f.txIndex = nil
		}
		f.v2Mu.Unlock()
		for _, table := range f.tables {
			if err := table.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if err := f.instanceLock.Unlock(); err != nil {
			errs = append(errs, err)
		}
	})
	return errors.Join(errs...)
}

// TransactionIndexCandidates returns truncated-fingerprint matches. Callers
// must verify the full hash against the canonical block body.
func (f *Freezer) TransactionIndexCandidates(hash [32]byte) ([]uint64, error) {
	if f == nil {
		return nil, nil
	}
	f.v2Mu.RLock()
	defer f.v2Mu.RUnlock()
	if f.txIndex == nil {
		return nil, nil
	}
	return f.txIndex.Candidates(hash)
}

func (f *Freezer) TransactionIndexCoverage() uint64 {
	if f == nil {
		return 0
	}
	f.v2Mu.RLock()
	defer f.v2Mu.RUnlock()
	if f.txIndex == nil {
		return 0
	}
	return f.txIndex.Coverage()
}

// PublishTransactionIndexRun publishes a verified contiguous run and refreshes
// the live reader before covered hot rows may be removed.
func (f *Freezer) PublishTransactionIndexRun(result TransactionIndexBuildResult) error {
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	if f.readonly {
		return errReadOnly
	}
	if result.EndBlock > f.V2Coverage() {
		return fmt.Errorf("transaction index end %d exceeds V2 coverage %d", result.EndBlock, f.V2Coverage())
	}
	current, err := OpenTransactionIndexStore(f.datadir)
	if err != nil {
		return err
	}
	if current.Coverage() == result.EndBlock {
		if err := verifyTransactionIndexBuildResult(f.datadir, result); err != nil {
			_ = current.Close()
			return err
		}
		f.replaceTransactionIndexStore(current)
		return nil
	}
	if current.Coverage() != result.StartBlock {
		_ = current.Close()
		return fmt.Errorf("transaction index publish: live coverage %d does not match run start %d", current.Coverage(), result.StartBlock)
	}
	if err := current.Close(); err != nil {
		return err
	}
	if err := PublishTransactionIndexRun(f.datadir, result); err != nil {
		return err
	}
	store, err := OpenTransactionIndexStore(f.datadir)
	if err != nil {
		return err
	}
	f.replaceTransactionIndexStore(store)
	return nil
}

func (f *Freezer) CompactTransactionIndexTail() (bool, error) {
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	if f.readonly {
		return false, errReadOnly
	}
	// Reconcile the in-memory view only when necessary. A previous pass may
	// have published a new manifest and then failed before reloading it; using
	// that stale view as a deletion authority could remove the newly selected
	// merged run. Avoid reopening the multi-megabyte run directories on every
	// healthy maintenance pass.
	current := f.txIndex
	if err := validateSelectedTransactionIndexStore(f.datadir, current); err != nil {
		current, err = OpenTransactionIndexStore(f.datadir)
		if err != nil {
			return false, err
		}
		if current.Coverage() > f.V2Coverage() {
			_ = current.Close()
			return false, fmt.Errorf("transaction index coverage %d exceeds ancient V2 coverage %d", current.Coverage(), f.V2Coverage())
		}
		f.replaceTransactionIndexStore(current)
	}
	cleanup, err := cleanupUnreferencedTransactionIndexRuns(f.datadir, current)
	if err != nil {
		return false, err
	}
	_, _, merged, err := CompactTransactionIndexTail(f.datadir)
	if err != nil || !merged {
		return cleanup.Files > 0, err
	}
	store, err := OpenTransactionIndexStore(f.datadir)
	if err != nil {
		return false, err
	}
	f.replaceTransactionIndexStore(store)
	if _, err := cleanupUnreferencedTransactionIndexRuns(f.datadir, store); err != nil {
		return true, err
	}
	return true, nil
}

func (f *Freezer) replaceTransactionIndexStore(store *TransactionIndexStore) {
	f.v2Mu.Lock()
	old := f.txIndex
	f.txIndex = store
	f.v2Mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// AncientDatadir returns the path of the ancient store.
func (f *Freezer) AncientDatadir() (string, error) {
	return f.datadir, nil
}

// Ancient retrieves an ancient binary blob from the append-only immutable files.
func (f *Freezer) Ancient(kind string, number uint64) ([]byte, error) {
	if table := f.tables[kind]; table != nil {
		f.v2Mu.RLock()
		store := f.v2
		if store != nil && store.has(kind, number) {
			data, err := store.read(kind, number)
			f.v2Mu.RUnlock()
			if err == nil {
				return data, nil
			}
			// A published segment can overlap V1 until reclamation. Keep the
			// verified source as a fallback during that rollback window.
			if !table.has(number) {
				return nil, err
			}
		} else {
			f.v2Mu.RUnlock()
		}
		return table.Retrieve(number)
	}
	return nil, errUnknownTable
}

// AncientNoCopy returns an immutable view into a decoded V2 frame. Callers must
// not mutate the returned bytes. V1 data follows the same immutable contract.
func (f *Freezer) AncientNoCopy(kind string, number uint64) ([]byte, error) {
	if table := f.tables[kind]; table != nil {
		f.v2Mu.RLock()
		store := f.v2
		if store != nil && store.has(kind, number) {
			data, err := store.readNoCopy(kind, number)
			f.v2Mu.RUnlock()
			if err == nil {
				return data, nil
			}
			if !table.has(number) {
				return nil, err
			}
		} else {
			f.v2Mu.RUnlock()
		}
		return table.Retrieve(number)
	}
	return nil, errUnknownTable
}

// AncientRange retrieves multiple items in sequence, starting from the index 'start'.
// It will return
//   - at most 'count' items,
//   - if maxBytes is specified: at least 1 item (even if exceeding the maxByteSize),
//     but will otherwise return as many items as fit into maxByteSize.
//   - if maxBytes is not specified, 'count' items will be returned if they are present.
func (f *Freezer) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	table := f.tables[kind]
	if table == nil {
		return nil, errUnknownTable
	}
	f.v2Mu.RLock()
	store := f.v2
	if store == nil || len(store.segments[kind]) == 0 || start >= store.coverage {
		f.v2Mu.RUnlock()
		return table.RetrieveItems(start, count, maxBytes)
	}
	output, err := store.readRange(kind, start, count, maxBytes)
	coverage := store.coverage
	f.v2Mu.RUnlock()
	if err != nil {
		if table.has(start) {
			return table.RetrieveItems(start, count, maxBytes)
		}
		return nil, err
	}
	if uint64(len(output)) == count || start+uint64(len(output)) < coverage {
		return output, nil
	}
	var total uint64
	for _, data := range output {
		total += uint64(len(data))
	}
	if maxBytes != 0 && total >= maxBytes {
		return output, nil
	}
	var remainingBytes uint64
	if maxBytes != 0 {
		remainingBytes = maxBytes - total
	}
	suffix, err := table.RetrieveItems(coverage, count-uint64(len(output)), remainingBytes)
	if err != nil {
		if len(output) > 0 && errors.Is(err, errOutOfBounds) {
			return output, nil
		}
		return nil, err
	}
	for _, data := range suffix {
		if len(output) > 0 && maxBytes != 0 && total+uint64(len(data)) > maxBytes {
			break
		}
		output = append(output, data)
		total += uint64(len(data))
	}
	return output, nil
}

// Ancients returns the length of the frozen items.
func (f *Freezer) Ancients() (uint64, error) {
	return f.head.Load(), nil
}

// AncientCount returns the number of items stored in the named table.
//
// Slice-1 callers want a kind-keyed count. We read the per-table `items`
// atomic directly (the same field `Retrieve` uses as its authority) rather
// than the global `f.head` because `TruncateHead` updates each table in a
// loop before re-storing `f.head`: a partial failure mid-loop would leave
// `f.head` ahead of one of the tables, and a kind-keyed query that returned
// the global value would then disagree with what `Retrieve(kind, ...)`
// actually serves. Reading the per-table atomic closes that window.
func (f *Freezer) AncientCount(kind string) (uint64, error) {
	table, ok := f.tables[kind]
	if !ok {
		return 0, errUnknownTable
	}
	count := table.items.Load()
	if coverage := f.V2Coverage(); coverage > count {
		count = coverage
	}
	return count, nil
}

// HasAncient returns true if the named table has an entry at the given number.
//
// Mirrors `freezerTable.has`; consolidates the "tail <= number < head" check
// behind the public surface so callers don't have to compose
// `Ancients()` + `Tail()` themselves.
func (f *Freezer) HasAncient(kind string, number uint64) (bool, error) {
	table := f.tables[kind]
	if table == nil {
		return false, errUnknownTable
	}
	f.v2Mu.RLock()
	hasV2 := f.v2 != nil && f.v2.has(kind, number)
	f.v2Mu.RUnlock()
	return table.has(number) || hasV2, nil
}

// Tail returns the number of first stored item in the freezer.
func (f *Freezer) Tail() (uint64, error) {
	if f.V2Coverage() > 0 {
		return 0, nil
	}
	return f.tail.Load(), nil
}

// V1Tail returns the virtual tail of the mutable V1 tables even when an
// immutable V2 prefix makes Tail report the composite store's logical tail.
// Callers deciding whether V1 can still source an online V2 migration must use
// this value instead of Tail.
func (f *Freezer) V1Tail() uint64 {
	if f == nil {
		return 0
	}
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()
	logicalHead := f.head.Load()
	direct := false
	for _, table := range f.tables {
		if logicalHead > table.items.Load() && table.itemHidden.Load() == table.items.Load() {
			direct = true
			continue
		}
		direct = false
		break
	}
	if direct {
		// There is no mutable V1 suffix to prune. Reporting the logical head
		// prevents the cold-snapshot tail pruner from repeatedly targeting
		// nonexistent legacy rows.
		return logicalHead
	}
	return f.tail.Load()
}

// V2Coverage is the first block not covered by the contiguous immutable V2
// segment prefix.
func (f *Freezer) V2Coverage() uint64 {
	if f == nil {
		return 0
	}
	f.v2Mu.RLock()
	defer f.v2Mu.RUnlock()
	if f.v2 == nil {
		return 0
	}
	return f.v2.coverage
}

// CanAppendV2Direct reports whether start is the complete logical ancient
// head and every legacy V1 table has no visible suffix. In that state a fresh
// sync may publish the next immutable segment directly from canonical hot
// rows without first materializing another V1 copy.
func (f *Freezer) CanAppendV2Direct(start uint64) bool {
	if f == nil || f.readonly {
		return false
	}
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()
	return f.canAppendV2DirectLocked(start)
}

func (f *Freezer) canAppendV2DirectLocked(start uint64) bool {
	if f.head.Load() != start || f.V2Coverage() != start {
		return false
	}
	for _, table := range f.tables {
		if table.items.Load() > start || table.itemHidden.Load() != table.items.Load() {
			return false
		}
	}
	return true
}

// AncientSize returns the ancient size of the specified category.
func (f *Freezer) AncientSize(kind string) (uint64, error) {
	// This needs the write lock to avoid data races on table fields.
	// Speed doesn't matter here, AncientSize is for debugging.
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()

	if table := f.tables[kind]; table != nil {
		size, err := table.size()
		if err != nil {
			return 0, err
		}
		f.v2Mu.RLock()
		if f.v2 != nil {
			size += f.v2.size(kind)
		}
		f.v2Mu.RUnlock()
		return size, nil
	}
	return 0, errUnknownTable
}

// Stats returns a point-in-time freezer status snapshot for operators and
// tests. It is intentionally diagnostic and takes the freezer read lock.
func (f *Freezer) Stats() (Stats, error) {
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()

	stats := Stats{
		Datadir:    f.datadir,
		ReadOnly:   f.readonly,
		Head:       f.head.Load(),
		Tail:       f.tail.Load(),
		V2Coverage: f.V2Coverage(),
		Tables:     make([]TableStats, 0, len(f.tables)),
		Repair:     f.repairStats.clone(),
	}
	names := make([]string, 0, len(f.tables))
	for name := range f.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		table := f.tables[name]
		tableStats, err := table.stats()
		if err != nil {
			return Stats{}, err
		}
		tableStats.Name = name
		f.v2Mu.RLock()
		if f.v2 != nil {
			tableStats.V2Size = f.v2.size(name)
		}
		f.v2Mu.RUnlock()
		stats.Tables = append(stats.Tables, tableStats)
	}
	return stats, nil
}

func (f *Freezer) repairStatsPath() string {
	return filepath.Join(f.datadir, repairStatsFilename)
}

func (f *Freezer) loadPersistedRepairStats() {
	data, err := os.ReadFile(f.repairStatsPath())
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		gtronlog.Warn("Could not read ancient database repair diagnostics", "database", f.datadir, "err", err)
		return
	}
	var repair RepairStats
	if err := json.Unmarshal(data, &repair); err != nil {
		gtronlog.Warn("Could not decode ancient database repair diagnostics", "database", f.datadir, "err", err)
		return
	}
	if repair.Applied {
		f.repairStats = repair.clone()
	}
}

func (f *Freezer) persistRepairStats(repair RepairStats) error {
	if !repair.Applied {
		return nil
	}
	data, err := json.MarshalIndent(repair, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(f.datadir, "."+repairStatsFilename+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, f.repairStatsPath())
}

// ModifyAncients runs the given write operation.
func (f *Freezer) ModifyAncients(fn func(AncientWriteOp) error) (writeSize int64, err error) {
	if f.readonly {
		return 0, errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()
	for _, table := range f.tables {
		if f.head.Load() > table.items.Load() {
			return 0, errors.New("ancient freezer is in direct V2 mode; V1 append is disabled")
		}
	}

	// Roll back all tables to the starting position in case of error.
	prevItem := f.head.Load()
	defer func() {
		if err != nil {
			// The write operation has failed. Go back to the previous item position.
			for name, table := range f.tables {
				err := table.truncateHead(prevItem)
				if err != nil {
					gtronlog.Error("Freezer table roll-back failed", "table", name, "index", prevItem, "err", err)
				}
			}
		}
	}()

	f.writeBatch.reset()
	if err := fn(f.writeBatch); err != nil {
		return 0, err
	}
	item, writeSize, err := f.writeBatch.commit()
	if err != nil {
		return 0, err
	}
	f.head.Store(item)
	return writeSize, nil
}

// TruncateHead discards any recent data above the provided threshold number.
// It returns the previous head number.
func (f *Freezer) TruncateHead(items uint64) (uint64, error) {
	if f.readonly {
		return 0, errReadOnly
	}
	if coverage := f.V2Coverage(); items < coverage {
		return 0, fmt.Errorf("head truncation %d is below immutable V2 coverage %d", items, coverage)
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	oitems := f.head.Load()
	if oitems <= items {
		return oitems, nil
	}
	for _, table := range f.tables {
		if err := table.truncateHead(items); err != nil {
			return 0, err
		}
	}
	f.head.Store(items)
	return oitems, nil
}

// TruncateTail marks all ancient items below items as pruned from the virtual
// tail. It only works when every freezer table was opened as prunable. The
// operation hides old rows from Ancient/HasAncient immediately and persists the
// new tail in table metadata. Call PruneTailFiles to reclaim fully-hidden data
// shards from disk.
func (f *Freezer) TruncateTail(items uint64) (uint64, error) {
	f.tailMutation.Lock()
	defer f.tailMutation.Unlock()
	return f.truncateTailLocked(items)
}

// truncateTailLocked advances the V1 virtual tail while the caller owns
// tailMutation. Keeping this separate lets MigrateV2 serialize the complete
// write/verify/publish/reclaim transaction against external tail pruning.
func (f *Freezer) truncateTailLocked(items uint64) (uint64, error) {
	if f.readonly {
		return 0, errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	head := f.head.Load()
	if items > head {
		return 0, errors.New("tail truncation above head")
	}
	otail := f.tail.Load()
	if items <= otail {
		return otail, nil
	}
	for kind, table := range f.tables {
		if !table.config.prunable {
			return 0, fmt.Errorf("freezer table %s is not prunable", kind)
		}
	}
	for _, table := range f.tables {
		if err := table.truncateTail(items); err != nil {
			return 0, err
		}
	}
	f.tail.Store(items)
	return otail, nil
}

// PruneTailFiles physically deletes data shards that are completely below the
// virtual tail. Tables with different compression ratios may advance their
// physical offsets to different item numbers; the freezer-wide Tail remains the
// virtual tail enforced by TruncateTail.
func (f *Freezer) PruneTailFiles() (uint64, error) {
	if f.readonly {
		return 0, errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	for kind, table := range f.tables {
		if !table.config.prunable {
			return 0, fmt.Errorf("freezer table %s is not prunable", kind)
		}
	}
	var removed uint64
	for _, table := range f.tables {
		n, err := table.pruneTailFiles()
		removed += n
		if err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// Sync flushes all data tables to disk.
func (f *Freezer) Sync() error {
	var errs []error
	for _, table := range f.tables {
		if err := table.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// validate checks that every table has the same boundary.
// Used instead of `repair` in readonly mode.
func (f *Freezer) validate() error {
	if len(f.tables) == 0 {
		return nil
	}
	var (
		head       uint64
		prunedTail *uint64
	)
	// get any head value
	for _, table := range f.tables {
		head = table.items.Load()
		break
	}
	for kind, table := range f.tables {
		// all tables have to have the same head
		if head != table.items.Load() {
			return fmt.Errorf("freezer table %s has a differing head: %d != %d", kind, table.items.Load(), head)
		}
		if !table.config.prunable {
			// non-prunable tables have to start at 0
			if table.itemHidden.Load() != 0 {
				return fmt.Errorf("non-prunable freezer table '%s' has a non-zero tail: %d", kind, table.itemHidden.Load())
			}
		} else {
			// prunable tables have to have the same length
			if prunedTail == nil {
				tmp := table.itemHidden.Load()
				prunedTail = &tmp
			}
			if *prunedTail != table.itemHidden.Load() {
				return fmt.Errorf("freezer table %s has differing tail: %d != %d", kind, table.itemHidden.Load(), *prunedTail)
			}
		}
	}

	if prunedTail == nil {
		tmp := uint64(0)
		prunedTail = &tmp
	}

	f.head.Store(head)
	f.tail.Store(*prunedTail)
	return nil
}

// repair truncates all data tables to the same length.
func (f *Freezer) repair() error {
	var (
		head       = uint64(math.MaxUint64)
		prunedTail = uint64(0)
	)
	// get the minimal head and the maximum tail
	for _, table := range f.tables {
		head = min(head, table.items.Load())
		prunedTail = max(prunedTail, table.itemHidden.Load())
	}
	repair := RepairStats{
		TargetHead: head,
		TargetTail: prunedTail,
	}
	names := make([]string, 0, len(f.tables))
	for name := range f.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	// apply the pruning
	for _, kind := range names {
		table := f.tables[kind]
		headBefore := table.items.Load()
		hiddenTailBefore := table.itemHidden.Load()
		// all tables need to have the same head
		if err := table.truncateHead(head); err != nil {
			return err
		}
		if !table.config.prunable {
			// non-prunable tables have to start at 0
			if table.itemHidden.Load() != 0 {
				panic(fmt.Sprintf("non-prunable freezer table %s has non-zero tail: %v", kind, table.itemHidden.Load()))
			}
		}
		if table.config.prunable && table.itemHidden.Load() < prunedTail {
			if err := table.truncateTail(prunedTail); err != nil {
				return err
			}
		}
		headAfter := table.items.Load()
		hiddenTailAfter := table.itemHidden.Load()
		if headBefore != headAfter || hiddenTailBefore != hiddenTailAfter {
			repair.Applied = true
			repair.Tables = append(repair.Tables, TableRepairStats{
				Name:             kind,
				HeadBefore:       headBefore,
				HeadAfter:        headAfter,
				HiddenTailBefore: hiddenTailBefore,
				HiddenTailAfter:  hiddenTailAfter,
			})
		}
	}

	f.head.Store(head)
	f.tail.Store(prunedTail)
	if repair.Applied {
		repair.RecordedAt = time.Now().UTC().Format(time.RFC3339Nano)
		f.repairStats = repair
		f.repairMetrics.update(repair, true)
		if err := f.persistRepairStats(repair); err != nil {
			gtronlog.Warn("Could not persist ancient database repair diagnostics", "database", f.datadir, "err", err)
		}
		gtronlog.Warn("Repaired ancient database table bounds",
			"database", f.datadir,
			"targetHead", repair.TargetHead,
			"targetTail", repair.TargetTail,
			"recordedAt", repair.RecordedAt,
			"tables", len(repair.Tables),
		)
	}
	return nil
}
