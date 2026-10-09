package pebbledb

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// MaintenanceControlReserveBytes is kept above MinFreeBytes for WAL, MANIFEST,
// OPTIONS and directory metadata. Artificially failing those writes can make
// Pebble's commit outcome uncertain, so the allocation guard only rejects SSTs.
const MaintenanceControlReserveBytes uint64 = 1 << 30

var (
	ErrMaintenanceLiveRange = errors.New("maintenance range contains live keys")
	ErrMaintenanceWALReplay = errors.New("maintenance requires explicit WAL replay permission")
)

// MaintenanceOptions bounds one handle's total SST allocation attempts,
// including WAL recovery and all levels of manual compaction. A table-size
// target and EstimateDiskUsage are NOT output/peak-space limits. Other processes
// can consume disk concurrently; stop competing writers or leave ample reserve.
type MaintenanceOptions struct {
	MinFreeBytes        uint64
	MaxSSTWriteBytes    uint64
	TargetFileSizeBytes int64 // zero: 32 MiB, same target at every level
	AllowWALReplay      bool
}

// MaintenanceInspection describes [start,end). Estimates include tombstones and
// old versions. Whole overlapping table bytes are informational, not a bound:
// compaction may expand to further adjacent tables at several levels.
type MaintenanceInspection struct {
	EstimatedBytes         uint64
	OverlappingTableBytes  uint64
	LevelOverlapBytes      [7]uint64
	Empty                  bool
	FirstLiveKeyHex        string `json:",omitempty"`
	OverlappingStartKeyHex string `json:",omitempty"`
	OverlappingEndKeyHex   string `json:",omitempty"`
	MemTableCount          int64
	MemTableBytes          uint64
}

type MaintenanceResult struct {
	Before           MaintenanceInspection
	After            MaintenanceInspection
	SSTBytesAdmitted uint64
}

// MaintenanceSpaceStats reports Pebble's current bookkeeping. These values
// include versions retained in SSTs and are not reclaimable-byte promises.
type MaintenanceSpaceStats struct {
	DiskSpaceBytes   uint64   `json:"disk_space_bytes"`
	SSTFiles         [7]int64 `json:"sst_files"`
	SSTBytes         [7]int64 `json:"sst_bytes"`
	ObsoleteSSTBytes uint64   `json:"obsolete_sst_bytes"`
	ZombieSSTBytes   uint64   `json:"zombie_sst_bytes"`
	WALPhysicalBytes uint64   `json:"wal_physical_bytes"`
	MemTableBytes    uint64   `json:"memtable_bytes"`
}

// MaintenanceSample is a bounded read-only canary. The key is owned by the
// caller; only a digest of its value is retained across the compaction.
type MaintenanceSample struct {
	KeyHex      string `json:"key_hex"`
	ValueSHA256 string `json:"value_sha256"`
}

// MaintenanceDB starts read-only and holds the directory lock until Close,
// including across the transition to read/write. It deliberately exposes no
// logical mutations: CompactEmptyRange only rewrites existing Pebble records.
// It must not be used as a general writable KeyValueStore: a space error during
// background memtable flush may otherwise cause unbounded flush retries.
type MaintenanceDB struct {
	mu       sync.Mutex
	path     string
	opts     MaintenanceOptions
	db       *pebble.DB
	lock     *pebble.Lock
	fs       *maintenanceFS
	readOnly bool
	closed   bool
}

// OpenMaintenance neither creates a database nor rewrites its files. The first
// CompactEmptyRange explicitly transitions to read/write after an empty-range
// proof and space checks. AllowWALReplay authorizes bounded synchronous recovery
// during that transition; replay errors leave the WAL available for recovery.
func OpenMaintenance(path string, opts MaintenanceOptions) (*MaintenanceDB, error) {
	return openMaintenance(path, opts, vfs.Default)
}

func openMaintenance(path string, opts MaintenanceOptions, fs vfs.FS) (*MaintenanceDB, error) {
	if opts.TargetFileSizeBytes == 0 {
		opts.TargetFileSizeBytes = 32 << 20
	}
	if opts.MinFreeBytes == 0 || opts.MaxSSTWriteBytes == 0 || opts.TargetFileSizeBytes <= 0 || opts.TargetFileSizeBytes > 128<<20 {
		return nil, errors.New("maintenance requires positive free-space/write limits and a table target <= 128 MiB")
	}
	if opts.MinFreeBytes > ^uint64(0)-MaintenanceControlReserveBytes || opts.MaxSSTWriteBytes > ^uint64(0)-opts.MinFreeBytes-MaintenanceControlReserveBytes {
		return nil, errors.New("maintenance space limits overflow")
	}
	lock, err := pebble.LockDirectory(path, fs)
	if err != nil {
		return nil, err
	}
	m := &MaintenanceDB{path: path, opts: opts, lock: lock, readOnly: true}
	m.fs = &maintenanceFS{FS: fs, path: path, floor: opts.MinFreeBytes + MaintenanceControlReserveBytes, budget: opts.MaxSSTWriteBytes}
	m.db, err = m.open(true)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return m, nil
}

func (m *MaintenanceDB) open(readOnly bool) (*pebble.DB, error) {
	levels := levelOptions(m.opts.TargetFileSizeBytes)
	for i := range levels {
		levels[i].TargetFileSize = m.opts.TargetFileSizeBytes
	}
	cache := pebble.NewCache(64 << 20)
	defer cache.Unref()
	opts := &pebble.Options{
		FS: m.fs, Lock: m.lock, Comparer: exactPointComparer,
		ReadOnly: readOnly, ErrorIfNotExists: true,
		DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 1 },
		Cache: cache, MaxOpenFiles: 128, MemTableSize: 64 << 20,
		Levels: levels, Logger: panicLogger{},
	}
	opts.Experimental.ReadSamplingMultiplier = -1
	return pebble.Open(m.path, opts)
}

func (m *MaintenanceDB) usable() error {
	if m.closed || m.db == nil {
		return errors.New("maintenance handle closed or failed to reopen; close and inspect again")
	}
	return nil
}

// Get returns an owned copy and does not expose a snapshot or iterator that
// could pin obsolete SST files across compaction.
func (m *MaintenanceDB) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.usable(); err != nil {
		return nil, err
	}
	value, closer, err := m.db.Get(key)
	if err != nil {
		return nil, err
	}
	value = bytes.Clone(value)
	return value, closer.Close()
}

// Has implements ethdb.KeyValueReader without exposing a mutation API.
func (m *MaintenanceDB) Has(key []byte) (bool, error) {
	_, err := m.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (m *MaintenanceDB) Inspect(start, end []byte) (MaintenanceInspection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inspect(start, end)
}

// InspectAll observes the exact current logical key bounds without changing
// the store. A nil upper bound means no physical or live keys exist.
func (m *MaintenanceDB) InspectAll() (MaintenanceInspection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start, end, err := m.allBounds()
	if err != nil || end == nil {
		return MaintenanceInspection{Empty: true}, err
	}
	return m.inspectPreserving(start, end, true)
}

func (m *MaintenanceDB) SpaceStats() (MaintenanceSpaceStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.usable(); err != nil {
		return MaintenanceSpaceStats{}, err
	}
	stats := m.db.Metrics()
	var out MaintenanceSpaceStats
	out.DiskSpaceBytes = stats.DiskSpaceUsage()
	for i, level := range stats.Levels {
		out.SSTFiles[i], out.SSTBytes[i] = level.NumFiles, level.Size
	}
	out.ObsoleteSSTBytes = stats.Table.ObsoleteSize
	out.ZombieSSTBytes = stats.Table.ZombieSize
	out.WALPhysicalBytes = stats.WAL.PhysicalSize
	out.MemTableBytes = uint64(stats.MemTable.Size)
	return out, nil
}

// SamplePrefixHashes records the first few live values within a schema-owned
// range without retaining their values or a snapshot across compaction.
func (m *MaintenanceDB) SamplePrefixHashes(start, end []byte, limit int) ([]MaintenanceSample, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.usable(); err != nil {
		return nil, err
	}
	if len(start) == 0 || len(end) == 0 || bytes.Compare(start, end) >= 0 || limit < 1 || limit > 16 {
		return nil, errors.New("maintenance canary requires bounded increasing schema range and 1..16 samples")
	}
	it, err := m.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end, KeyTypes: pebble.IterKeyTypePointsOnly})
	if err != nil {
		return nil, err
	}
	out := make([]MaintenanceSample, 0, limit)
	for ok := it.First(); ok && len(out) < limit; ok = it.Next() {
		sum := sha256.Sum256(it.Value())
		out = append(out, MaintenanceSample{KeyHex: hex.EncodeToString(it.Key()), ValueSHA256: hex.EncodeToString(sum[:])})
	}
	return out, errors.Join(it.Error(), it.Close())
}

func (m *MaintenanceDB) inspect(start, end []byte) (out MaintenanceInspection, err error) {
	return m.inspectPreserving(start, end, false)
}

func (m *MaintenanceDB) inspectPreserving(start, end []byte, allowEmptyStart bool) (out MaintenanceInspection, err error) {
	if err = m.usable(); err != nil {
		return out, err
	}
	if (!allowEmptyStart && len(start) == 0) || len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return out, errors.New("maintenance requires an explicit nonempty increasing key range")
	}
	iterOpts := &pebble.IterOptions{LowerBound: start, UpperBound: end}
	if m.db.FormatMajorVersion() >= pebble.FormatRangeKeys {
		iterOpts.KeyTypes = pebble.IterKeyTypePointsAndRanges
	}
	iter, err := m.db.NewIter(iterOpts)
	if err != nil {
		return out, err
	}
	out.Empty = !iter.First()
	if !out.Empty {
		out.FirstLiveKeyHex = hex.EncodeToString(iter.Key())
	}
	if err = errors.Join(iter.Error(), iter.Close()); err != nil {
		return out, err
	}
	out.EstimatedBytes, err = m.db.EstimateDiskUsage(start, end)
	if err != nil {
		return out, err
	}
	tables, err := m.db.SSTables()
	if err != nil {
		return out, err
	}
	var smallest, largest []byte
	for level, files := range tables {
		for _, f := range files {
			if bytes.Compare(f.Smallest.UserKey, end) <= 0 && bytes.Compare(f.Largest.UserKey, start) >= 0 {
				out.LevelOverlapBytes[level] += f.Size
				out.OverlappingTableBytes += f.Size
				if smallest == nil || bytes.Compare(f.Smallest.UserKey, smallest) < 0 {
					smallest = f.Smallest.UserKey
				}
				if largest == nil || bytes.Compare(f.Largest.UserKey, largest) > 0 {
					largest = f.Largest.UserKey
				}
			}
		}
	}
	out.OverlappingStartKeyHex, out.OverlappingEndKeyHex = hex.EncodeToString(smallest), hex.EncodeToString(largest)
	metrics := m.db.Metrics()
	out.MemTableCount, out.MemTableBytes = metrics.MemTable.Count, metrics.MemTable.Size
	return out, nil
}

// CompactEmptyRange never deletes live records. It first proves the range is
// logically empty, including WAL records, and then asks Pebble to reclaim old
// physical versions. The hard budget applies across all jobs/levels/retries
// for this handle; create a new handle for another explicitly approved batch.
// A failure may follow successful earlier level rewrites, all preserving KV
// semantics. Close the handle and reopen read-only to verify after any error.
func (m *MaintenanceDB) CompactEmptyRange(start, end []byte) (out MaintenanceResult, err error) {
	return m.compactRange(start, end, true, false)
}

// CompactPreservingRange rewrites physical SST versions without removing any
// live logical key. Unlike CompactEmptyRange, live keys are explicitly allowed.
// A budget error can follow a completed lower-level rewrite; callers must
// close and reopen read-only to check the protected state before continuing.
func (m *MaintenanceDB) CompactPreservingRange(start, end []byte) (MaintenanceResult, error) {
	return m.compactRange(start, end, false, false)
}

// CompactPreservingAll derives bounds from all current SST metadata and live
// keys, including an empty or all-0xff key. The successor upper key ensures
// the maximum key is included rather than assuming a fixed-width sentinel.
func (m *MaintenanceDB) CompactPreservingAll() (MaintenanceResult, error) {
	m.mu.Lock()
	start, end, err := m.allBounds()
	m.mu.Unlock()
	if err != nil || end == nil {
		return MaintenanceResult{}, err
	}
	return m.compactRange(start, end, false, true)
}

// ReopenReadOnly closes Pebble and reopens it under the same directory lock.
// Call it after either success or failure to verify durable protected state.
func (m *MaintenanceDB) ReopenReadOnly() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("maintenance handle closed")
	}
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			m.db = nil
			return err
		}
		m.db = nil
	}
	db, err := m.open(true)
	if err != nil {
		return err
	}
	m.db, m.readOnly = db, true
	return nil
}

func (m *MaintenanceDB) allBounds() ([]byte, []byte, error) {
	if err := m.usable(); err != nil {
		return nil, nil, err
	}
	var maximum []byte
	tables, err := m.db.SSTables()
	if err != nil {
		return nil, nil, err
	}
	for _, level := range tables {
		for _, table := range level {
			if maximum == nil || bytes.Compare(table.Largest.UserKey, maximum) > 0 {
				maximum = bytes.Clone(table.Largest.UserKey)
			}
		}
	}
	iterOpts := &pebble.IterOptions{}
	if m.db.FormatMajorVersion() >= pebble.FormatRangeKeys {
		iterOpts.KeyTypes = pebble.IterKeyTypePointsAndRanges
	}
	it, err := m.db.NewIter(iterOpts)
	if err != nil {
		return nil, nil, err
	}
	if it.Last() {
		candidate := it.Key()
		if _, rangeEnd := it.RangeBounds(); bytes.Compare(rangeEnd, candidate) > 0 {
			candidate = rangeEnd
		}
		if maximum == nil || bytes.Compare(candidate, maximum) > 0 {
			maximum = make([]byte, len(candidate))
			copy(maximum, candidate)
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return nil, nil, err
	}
	if maximum == nil {
		return nil, nil, nil
	}
	return nil, append(maximum, 0), nil
}

func (m *MaintenanceDB) compactRange(start, end []byte, requireEmpty, allowEmptyStart bool) (out MaintenanceResult, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() { out.SSTBytesAdmitted = m.fs.admittedBytes() }()
	if len(end) == 0 || bytes.Compare(start, end) >= 0 || (!allowEmptyStart && len(start) == 0) {
		return out, errors.New("maintenance requires an explicit increasing key range")
	}
	out.Before, err = m.inspectPreserving(start, end, allowEmptyStart)
	if err != nil {
		return out, err
	}
	if requireEmpty && !out.Before.Empty {
		return out, fmt.Errorf("%w: first key %s", ErrMaintenanceLiveRange, out.Before.FirstLiveKeyHex)
	}
	if m.readOnly && out.Before.MemTableCount != 0 && !m.opts.AllowWALReplay {
		return out, ErrMaintenanceWALReplay
	}
	if err = m.fs.preflight(); err != nil {
		return out, err
	}
	if m.readOnly {
		if err = m.db.Close(); err != nil {
			m.db = nil
			return out, err
		}
		m.db = nil
		// In Pebble v1.1.5 RW Open synchronously flushes replayed WALs and
		// returns an SST error directly. Once it succeeds, only a fresh empty
		// memtable exists. No write API is exposed here, so Compact cannot
		// encounter the failing background-flush retry/wait path.
		m.db, err = m.open(false)
		if err != nil {
			return out, err
		}
		m.readOnly = false
	}
	err = m.db.Compact(start, end, false)
	var inspectErr error
	out.After, inspectErr = m.inspectPreserving(start, end, allowEmptyStart)
	if requireEmpty && inspectErr == nil && !out.After.Empty {
		inspectErr = errors.New("maintenance invariant failed: compacted empty range became live")
	}
	return out, errors.Join(err, inspectErr)
}

func (m *MaintenanceDB) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var err error
	if m.db != nil {
		err = m.db.Close()
		m.db = nil
	}
	return errors.Join(err, m.lock.Close())
}
