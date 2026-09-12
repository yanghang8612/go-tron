package rawdb

import "fmt"

// StateDomainChangeDeleteOptions enables coarse range tombstones for consecutive
// indexed seq=0 changeset rows. Zero options preserve point deletion. When enabled,
// zero limits select 64 blocks minimum, 1024 blocks maximum, and 64 MiB maximum.
// Bytes include physical keys and values. A whole final row may exceed the byte
// limit; a run shorter than MinRangeBlocks still falls back to point deletion.
type StateDomainChangeDeleteOptions struct {
	EnableRangeDelete bool
	MinRangeBlocks    uint64
	MaxRangeBlocks    uint64
	MaxRangeBytes     uint64
}

// StateDomainChangeDeleteStats counts authoritative changeset rows accepted by
// the supplied writer, not durable commits or reclaimed physical disk space.
// The caller must publish these statistics only after its batch flush succeeds.
// Inverse posting deletes are excluded. Any error returns zero statistics, even
// when the writer accepted earlier operations; retrying those is idempotent.
type StateDomainChangeDeleteStats struct {
	RangeRuns  uint64
	RangeRows  uint64
	RangeBytes uint64
	PointRows  uint64
	PointBytes uint64
}

// DeleteStateDomainChangeBlocksWithOptions deletes selected history blocks with
// optional coarse range tombstones. Selection must be strictly increasing. Only
// indexed, consecutive blocks represented by adjacent, exact seq=0 physical keys
// are combined. Missing/unselected blocks, repair rows, and malformed keys split
// runs. Values are not copied or decoded on the indexed path, but their lengths
// are read for budgets and statistics. Unindexed posting cleanup is unchanged.
//
// Callers must serialize old-height repairs and other writers throughout the
// scan and commit: a range tombstone would also cover intervening keys inserted
// after the scan. The optional DeleteRange capability is used only on db itself,
// so a batching caller must explicitly forward it to the same batch writer.
// There is no underlying-database escape hatch. Unsupported writers use points.
func DeleteStateDomainChangeBlocksWithOptions(db StateKVLatestStore, blockNums []uint64, opts StateDomainChangeDeleteOptions) (StateDomainChangeDeleteStats, error) {
	var zero StateDomainChangeDeleteStats
	if len(blockNums) == 0 {
		return zero, nil
	}
	for i := 1; i < len(blockNums); i++ {
		if blockNums[i] <= blockNums[i-1] {
			return zero, fmt.Errorf("rawdb: state domain change delete blocks are not strictly increasing at %d after %d", blockNums[i], blockNums[i-1])
		}
	}
	if opts.EnableRangeDelete {
		if opts.MinRangeBlocks == 0 {
			opts.MinRangeBlocks = 64
		}
		if opts.MaxRangeBlocks == 0 {
			opts.MaxRangeBlocks = 1024
		}
		if opts.MaxRangeBytes == 0 {
			opts.MaxRangeBytes = 64 << 20
		}
		if opts.MinRangeBlocks > opts.MaxRangeBlocks {
			return zero, fmt.Errorf("rawdb: state domain change range minimum %d exceeds maximum %d", opts.MinRangeBlocks, opts.MaxRangeBlocks)
		}
	}
	indexedHead, staged, err := stateHistoryIndexedHead(db, blockNums[len(blockNums)-1])
	if err != nil {
		return zero, err
	}
	deletes := stateDomainChangeRangeDeleter{db: db, opts: opts}
	if opts.EnableRangeDelete {
		deletes.ranges, _ = db.(interface{ DeleteRange(start, end []byte) error })
	}
	// Match the original API's sparse-selection guard without extra watermark
	// reads or a broad scan through unselected history.
	if blockNums[len(blockNums)-1]-blockNums[0] > uint64(len(blockNums))*4 {
		for i := range blockNums {
			if err := deleteStateDomainChangeBlocksScan(db, blockNums[i:i+1], indexedHead, staged, &deletes); err != nil {
				return zero, err
			}
			if err := deletes.flushRun(); err != nil {
				return zero, err
			}
		}
	} else {
		if err := deleteStateDomainChangeBlocksScan(db, blockNums, indexedHead, staged, &deletes); err != nil {
			return zero, err
		}
		// The scan has released its iterator before the final run is submitted.
		if err := deletes.flushRun(); err != nil {
			return zero, err
		}
	}
	return deletes.stats, nil
}

type stateDomainChangeRangeDeleter struct {
	db     StateKVLatestStore
	opts   StateDomainChangeDeleteOptions
	ranges interface{ DeleteRange(start, end []byte) error }
	stats  StateDomainChangeDeleteStats
	first  uint64
	last   uint64
	rows   uint64
	bytes  uint64
}

func (d *stateDomainChangeRangeDeleter) indexedRow(key []byte, blockNum, seq uint64, valueSize int) error {
	if d.ranges == nil || seq != 0 {
		if err := d.flushRun(); err != nil {
			return err
		}
		return d.pointRow(key, valueSize)
	}
	if d.rows != 0 && (d.last == ^uint64(0) || blockNum != d.last+1) {
		if err := d.flushRun(); err != nil {
			return err
		}
	}
	if d.rows == 0 {
		d.first = blockNum
	}
	d.last = blockNum
	d.rows++
	d.bytes += uint64(len(key)) + uint64(valueSize)
	if d.rows >= d.opts.MaxRangeBlocks || d.bytes >= d.opts.MaxRangeBytes {
		return d.flushRun()
	}
	return nil
}

func (d *stateDomainChangeRangeDeleter) pointRow(key []byte, valueSize int) error {
	if err := d.db.Delete(key); err != nil {
		return err
	}
	d.stats.PointRows++
	d.stats.PointBytes += uint64(len(key)) + uint64(valueSize)
	return nil
}

func (d *stateDomainChangeRangeDeleter) flushRun() error {
	if d.rows == 0 {
		return nil
	}
	if d.rows >= d.opts.MinRangeBlocks {
		start, end := stateChangeSetPackedDeleteRange(d.first, d.last)
		if err := d.ranges.DeleteRange(start, end); err != nil {
			return err
		}
		d.stats.RangeRuns++
		d.stats.RangeRows += d.rows
		d.stats.RangeBytes += d.bytes
	} else {
		for offset := uint64(0); offset < d.rows; offset++ {
			if err := d.db.Delete(stateChangeSetKey(d.first+offset, 0)); err != nil {
				return err
			}
		}
		d.stats.PointRows += d.rows
		d.stats.PointBytes += d.bytes
	}
	d.rows, d.bytes = 0, 0
	return nil
}
