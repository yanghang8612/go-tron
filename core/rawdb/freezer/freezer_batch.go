// Copyright 2021 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Vendored from go-ethereum/core/rawdb/freezer_batch.go and adapted for
// gtron's package layout. The RLP `Append` path is dropped because slice 1
// of the chain-freezer only stores pre-encoded protobuf / raw-byte blobs;
// `AppendRaw` is the sole entry point.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

package freezer

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/golang/snappy"
)

// ErrUnknownTable is returned if the user attempts to read from or write to
// a table that is not tracked by the freezer. Exported so the parent rawdb
// package can translate it into a public sentinel.
var ErrUnknownTable = errors.New("unknown table")

// errUnknownTable is the package-private alias kept so vendored code below
// reads identically to upstream go-ethereum.
var errUnknownTable = ErrUnknownTable

// errOutOrderInsertion is returned if the user attempts to inject out-of-order
// binary blobs into the freezer.
var errOutOrderInsertion = errors.New("the append operation is out-order")

const (
	// freezerBatchBufferLimit is the maximum amount of data that will be
	// buffered in memory for a single freezer table batch.
	freezerBatchBufferLimit = 2 * 1024 * 1024

	// freezerTableFlushThreshold defines the threshold for triggering a freezer
	// table sync operation. If the number of accumulated uncommitted items exceeds
	// this value, a sync will be scheduled.
	freezerTableFlushThreshold = 512
)

// freezerBatch is a write operation of multiple items on a freezer.
type freezerBatch struct {
	tables map[string]*freezerTableBatch
}

func newFreezerBatch(f *Freezer) *freezerBatch {
	batch := &freezerBatch{tables: make(map[string]*freezerTableBatch, len(f.tables))}
	for kind, table := range f.tables {
		batch.tables[kind] = table.newBatch()
	}
	return batch
}

// AppendRaw adds an item of the given kind.
func (batch *freezerBatch) AppendRaw(kind string, num uint64, item []byte) error {
	if table := batch.tables[kind]; table != nil {
		return table.AppendRaw(num, item)
	}
	return errUnknownTable
}

// reset initializes the batch.
func (batch *freezerBatch) reset() {
	for _, tb := range batch.tables {
		tb.reset()
	}
}

// commit is called at the end of a write operation and
// writes all remaining data to tables.
func (batch *freezerBatch) commit() (item uint64, writeSize int64, err error) {
	// Check that count agrees on all batches.
	item = uint64(math.MaxUint64)
	for name, tb := range batch.tables {
		if item < math.MaxUint64 && tb.curItem != item {
			return 0, 0, fmt.Errorf("table %s is at item %d, want %d", name, tb.curItem, item)
		}
		item = tb.curItem
	}

	// Commit all table batches.
	for _, tb := range batch.tables {
		if err := tb.commit(); err != nil {
			return 0, 0, err
		}
		writeSize += tb.totalBytes
	}
	return item, writeSize, nil
}

// freezerTableBatch is a batch for a freezer table.
type freezerTableBatch struct {
	t *freezerTable

	sb          *snappyBuffer
	dataBuffer  []byte
	indexBuffer []byte
	curItem     uint64 // expected index of next append
	totalBytes  int64  // counts written bytes since reset
}

// newBatch creates a new batch for the freezer table.
func (t *freezerTable) newBatch() *freezerTableBatch {
	batch := &freezerTableBatch{t: t}
	if !t.config.noSnappy {
		batch.sb = new(snappyBuffer)
	}
	batch.reset()
	return batch
}

// reset clears the batch for reuse.
func (batch *freezerTableBatch) reset() {
	batch.dataBuffer = batch.dataBuffer[:0]
	batch.indexBuffer = batch.indexBuffer[:0]
	batch.curItem = batch.t.items.Load()
	batch.totalBytes = 0
}

// AppendRaw injects a binary blob at the end of the freezer table. The item number is a
// precautionary parameter to ensure data correctness, but the table will reject already
// existing data.
func (batch *freezerTableBatch) AppendRaw(item uint64, blob []byte) error {
	if item != batch.curItem {
		return fmt.Errorf("%w: have %d want %d", errOutOrderInsertion, item, batch.curItem)
	}

	encItem := blob
	if batch.sb != nil {
		encItem = batch.sb.compress(blob)
	}
	return batch.appendItem(encItem)
}

func (batch *freezerTableBatch) appendItem(data []byte) error {
	// Check if item fits into current data file.
	itemSize := int64(len(data))
	itemOffset := batch.t.headBytes + int64(len(batch.dataBuffer))
	if itemOffset+itemSize > int64(batch.t.maxFileSize) {
		// It doesn't fit, go to next file first.
		if err := batch.commit(); err != nil {
			return err
		}
		if err := batch.t.advanceHead(); err != nil {
			return err
		}
		itemOffset = 0
	}

	// Put data to buffer.
	batch.dataBuffer = append(batch.dataBuffer, data...)
	batch.totalBytes += itemSize

	// Put index entry to buffer.
	entry := indexEntry{filenum: batch.t.headId, offset: uint32(itemOffset + itemSize)}
	batch.indexBuffer = entry.append(batch.indexBuffer)
	batch.curItem++

	return batch.maybeCommit()
}

// maybeCommit writes the buffered data if the buffer is full enough.
func (batch *freezerTableBatch) maybeCommit() error {
	if len(batch.dataBuffer) > freezerBatchBufferLimit {
		return batch.commit()
	}
	return nil
}

// commit writes the batched items to the backing freezerTable. Note the index
// file isn't fsync'd after the file write, the recent write can be lost
// after the power failure.
func (batch *freezerTableBatch) commit() error {
	_, err := batch.t.head.Write(batch.dataBuffer)
	if err != nil {
		return err
	}
	dataSize := int64(len(batch.dataBuffer))
	batch.dataBuffer = batch.dataBuffer[:0]

	_, err = batch.t.index.Write(batch.indexBuffer)
	if err != nil {
		return err
	}
	indexSize := int64(len(batch.indexBuffer))
	batch.indexBuffer = batch.indexBuffer[:0]

	// Update headBytes of table.
	batch.t.headBytes += dataSize
	items := batch.curItem - batch.t.items.Load()
	batch.t.items.Store(batch.curItem)

	// Update metrics.
	batch.t.sizeGauge.Inc(dataSize + indexSize)
	batch.t.writeMeter.Mark(dataSize + indexSize)

	// Periodically sync the table.
	batch.t.uncommitted += items
	if batch.t.uncommitted > freezerTableFlushThreshold && time.Since(batch.t.lastSync) > 30*time.Second {
		batch.t.uncommitted = 0
		batch.t.lastSync = time.Now()
		return batch.t.Sync()
	}
	return nil
}

// snappyBuffer writes snappy in block format, and can be reused. It is
// reset when WriteTo is called.
type snappyBuffer struct {
	dst []byte
}

// compress snappy-compresses the data.
func (s *snappyBuffer) compress(data []byte) []byte {
	// The snappy library does not care what the capacity of the buffer is,
	// but only checks the length. If the length is too small, it will
	// allocate a brand new buffer.
	// To avoid that, we check the required size here, and grow the size of the
	// buffer to utilize the full capacity.
	if n := snappy.MaxEncodedLen(len(data)); len(s.dst) < n {
		if cap(s.dst) < n {
			s.dst = make([]byte, n)
		}
		s.dst = s.dst[:n]
	}

	s.dst = snappy.Encode(s.dst, data)
	return s.dst
}
