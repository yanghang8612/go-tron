package pebbledb

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// EstimateDiskUsage uses SST metadata/block offsets, without scanning history
// values. It excludes WAL/memtables and can include obsolete versions. It is a
// pressure signal, not a count of live bytes or immediately reclaimable space.
func (d *Database) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if len(start) == 0 || len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return 0, fmt.Errorf("disk estimate requires an explicit increasing key range")
	}
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return 0, pebble.ErrClosed
	}
	return d.db.EstimateDiskUsage(start, end)
}
