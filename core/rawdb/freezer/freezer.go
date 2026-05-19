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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

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

var (
	// errReadOnly is returned if the freezer is opened in read only mode. All the
	// mutations are disallowed.
	errReadOnly = errors.New("read only")

	// errSymlinkDatadir is returned if the ancient directory specified by user
	// is a symbolic link.
	errSymlinkDatadir = errors.New("symbolic link datadir is not supported")
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
	Prunable bool // true for tables that can be pruned by TruncateTail (unused in slice 1)
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

	readonly     bool
	tables       map[string]*freezerTable // Data tables for storing everything
	instanceLock *flock.Flock             // File-system lock to prevent double opens
	closeOnce    sync.Once
}

// NewFreezer creates a freezer instance for maintaining immutable ordered
// data according to the given parameters.
//
// The 'tables' argument defines the freezer tables and their configuration.
// Each value is a TableConfig specifying whether snappy compression is
// disabled (NoSnappy) and whether the table is prunable (Prunable; unused
// in slice 1 but kept on the type to stay structurally aligned with geth).
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
		datadir:      datadir,
		readonly:     readonly,
		tables:       make(map[string]*freezerTable),
		instanceLock: lock,
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

	// Create the write batch.
	freezer.writeBatch = newFreezerBatch(freezer)

	gtronlog.Info("Opened ancient database", "database", datadir, "readonly", readonly)
	return freezer, nil
}

// Close terminates the chain freezer, closing all the data files.
func (f *Freezer) Close() error {
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	var errs []error
	f.closeOnce.Do(func() {
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

// AncientDatadir returns the path of the ancient store.
func (f *Freezer) AncientDatadir() (string, error) {
	return f.datadir, nil
}

// Ancient retrieves an ancient binary blob from the append-only immutable files.
func (f *Freezer) Ancient(kind string, number uint64) ([]byte, error) {
	if table := f.tables[kind]; table != nil {
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
	if table := f.tables[kind]; table != nil {
		return table.RetrieveItems(start, count, maxBytes)
	}
	return nil, errUnknownTable
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
	return table.items.Load(), nil
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
	return table.has(number), nil
}

// Tail returns the number of first stored item in the freezer.
func (f *Freezer) Tail() (uint64, error) {
	return f.tail.Load(), nil
}

// AncientSize returns the ancient size of the specified category.
func (f *Freezer) AncientSize(kind string) (uint64, error) {
	// This needs the write lock to avoid data races on table fields.
	// Speed doesn't matter here, AncientSize is for debugging.
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()

	if table := f.tables[kind]; table != nil {
		return table.size()
	}
	return 0, errUnknownTable
}

// ModifyAncients runs the given write operation.
func (f *Freezer) ModifyAncients(fn func(AncientWriteOp) error) (writeSize int64, err error) {
	if f.readonly {
		return 0, errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

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
	// apply the pruning
	for kind, table := range f.tables {
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
		// Slice 1 has no prunable tables, so we deliberately do NOT
		// implement the truncateTail branch here. Re-enable when a
		// pruning use-case lands (see go-ethereum's freezer.go::repair).
	}

	f.head.Store(head)
	f.tail.Store(prunedTail)
	return nil
}
