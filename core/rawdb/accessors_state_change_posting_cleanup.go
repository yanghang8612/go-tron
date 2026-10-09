package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/ethdb"
)

// StateChangePostingFrameCursor exposes only the schema-owned information
// needed by the offline whole-frame cleanup. Key is borrowed from the
// iterator and remains valid only until the callback returns.
type StateChangePostingFrameCursor struct {
	Key        []byte
	Hash       [sha256.Size]byte
	FirstBlock uint64
	LastBlock  uint64
	Rows       uint64
	ValueBytes uint64
}

// ScanStateChangePostingFrames walks every live frame, validates its entire
// ordered encoding without allocating a []uint64 per frame, and propagates
// iterator errors. The caller may retain a hash but not the borrowed key.
func ScanStateChangePostingFrames(ctx context.Context, db ethdb.Iteratee, visit func(StateChangePostingFrameCursor) error) error {
	if ctx == nil || db == nil || visit == nil {
		return errors.New("rawdb: invalid posting cleanup scan")
	}
	it := db.NewIterator(stateChangePostingPrefix, nil)
	defer it.Release()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		if len(key) != len(stateChangePostingPrefix)+sha256.Size+8 || !bytes.HasPrefix(key, stateChangePostingPrefix) {
			return errors.New("rawdb: malformed posting frame key")
		}
		var frame StateChangePostingFrameCursor
		frame.Key = key
		copy(frame.Hash[:], key[len(stateChangePostingPrefix):])
		frame.FirstBlock = binary.BigEndian.Uint64(key[len(key)-8:])
		last, rows, err := stateChangePostingLastBlock(frame.FirstBlock, it.Value())
		if err != nil {
			return err
		}
		frame.LastBlock, frame.Rows = last, rows
		frame.ValueBytes = uint64(len(it.Value()))
		if err := visit(frame); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("rawdb: posting cleanup iterator: %w", err)
	}
	return ctx.Err()
}

func stateChangePostingLastBlock(first uint64, value []byte) (uint64, uint64, error) {
	if len(value) < 2 || value[0] != stateChangePostingValueVersion {
		return 0, 0, errors.New("rawdb: malformed posting frame version")
	}
	count, consumed := binary.Uvarint(value[1:])
	if consumed <= 0 || count == 0 || count > uint64(StateChangePostingFrameRows) {
		return 0, 0, errors.New("rawdb: malformed posting frame count")
	}
	last, off := first, 1+consumed
	for i := uint64(1); i < count; i++ {
		if off >= len(value) {
			return 0, 0, errors.New("rawdb: truncated posting frame")
		}
		delta, n := binary.Uvarint(value[off:])
		if n <= 0 || delta == 0 || last > math.MaxUint64-delta {
			return 0, 0, errors.New("rawdb: malformed posting frame delta")
		}
		last += delta
		off += n
	}
	if off != len(value) {
		return 0, 0, errors.New("rawdb: trailing posting frame bytes")
	}
	return last, count, nil
}

// StateChangeDirectoryCursor supplies the exact hash used by the posting
// frame key. Key is borrowed and only valid inside the callback.
type StateChangeDirectoryCursor struct {
	Key  []byte
	Hash [sha256.Size]byte
}

func ScanStateChangeKeyDirectory(ctx context.Context, db ethdb.Iteratee, visit func(StateChangeDirectoryCursor) error) error {
	if ctx == nil || db == nil || visit == nil {
		return errors.New("rawdb: invalid directory cleanup scan")
	}
	it := db.NewIterator(stateChangeKeyDirectoryPrefix, nil)
	defer it.Release()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		if !bytes.HasPrefix(key, stateChangeKeyDirectoryPrefix) || len(key) == len(stateChangeKeyDirectoryPrefix) || len(it.Value()) != 0 {
			return errors.New("rawdb: malformed state change directory row")
		}
		if err := visit(StateChangeDirectoryCursor{Key: key, Hash: stateChangePostingHash(key[len(stateChangeKeyDirectoryPrefix):])}); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("rawdb: directory cleanup iterator: %w", err)
	}
	return ctx.Err()
}

// ScanStateChangePostingFramePage releases its iterator at each page boundary
// so an offline deleter never pins the entire hot LSM for a billions-row pass.
// after is an opaque full physical key returned by the preceding page.
func ScanStateChangePostingFramePage(ctx context.Context, db ethdb.Iteratee, after []byte, maxRows uint64, visit func(StateChangePostingFrameCursor) error) ([]byte, bool, error) {
	if ctx == nil || db == nil || visit == nil || maxRows == 0 || maxRows > 100000 ||
		(len(after) != 0 && (len(after) != len(stateChangePostingPrefix)+sha256.Size+8 || !bytes.HasPrefix(after, stateChangePostingPrefix))) {
		return nil, false, errors.New("rawdb: invalid posting page cursor")
	}
	var start []byte
	if len(after) > 0 {
		start = after[len(stateChangePostingPrefix):]
	}
	it := db.NewIterator(stateChangePostingPrefix, start)
	defer it.Release()
	var rows uint64
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		key := it.Key()
		if len(after) > 0 && bytes.Compare(key, after) <= 0 {
			continue
		}
		if len(key) != len(stateChangePostingPrefix)+sha256.Size+8 || !bytes.HasPrefix(key, stateChangePostingPrefix) {
			return nil, false, errors.New("rawdb: malformed posting frame key")
		}
		var frame StateChangePostingFrameCursor
		frame.Key = key
		copy(frame.Hash[:], key[len(stateChangePostingPrefix):])
		frame.FirstBlock = binary.BigEndian.Uint64(key[len(key)-8:])
		last, count, err := stateChangePostingLastBlock(frame.FirstBlock, it.Value())
		if err != nil {
			return nil, false, err
		}
		frame.LastBlock, frame.Rows, frame.ValueBytes = last, count, uint64(len(it.Value()))
		if err := visit(frame); err != nil {
			return nil, false, err
		}
		rows++
		if rows == maxRows {
			if err := it.Error(); err != nil {
				return nil, false, err
			}
			return bytes.Clone(key), false, nil
		}
	}
	if err := it.Error(); err != nil {
		return nil, false, fmt.Errorf("rawdb: posting page iterator: %w", err)
	}
	return nil, true, ctx.Err()
}

func ScanStateChangeKeyDirectoryPage(ctx context.Context, db ethdb.Iteratee, after []byte, maxRows uint64, visit func(StateChangeDirectoryCursor) error) ([]byte, bool, error) {
	if ctx == nil || db == nil || visit == nil || maxRows == 0 || maxRows > 100000 ||
		(len(after) != 0 && (len(after) <= len(stateChangeKeyDirectoryPrefix) || !bytes.HasPrefix(after, stateChangeKeyDirectoryPrefix))) {
		return nil, false, errors.New("rawdb: invalid directory page cursor")
	}
	var start []byte
	if len(after) > 0 {
		start = after[len(stateChangeKeyDirectoryPrefix):]
	}
	it := db.NewIterator(stateChangeKeyDirectoryPrefix, start)
	defer it.Release()
	var rows uint64
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		key := it.Key()
		if len(after) > 0 && bytes.Compare(key, after) <= 0 {
			continue
		}
		if !bytes.HasPrefix(key, stateChangeKeyDirectoryPrefix) || len(key) == len(stateChangeKeyDirectoryPrefix) || len(it.Value()) != 0 {
			return nil, false, errors.New("rawdb: malformed state change directory row")
		}
		if err := visit(StateChangeDirectoryCursor{Key: key, Hash: stateChangePostingHash(key[len(stateChangeKeyDirectoryPrefix):])}); err != nil {
			return nil, false, err
		}
		rows++
		if rows == maxRows {
			if err := it.Error(); err != nil {
				return nil, false, err
			}
			return bytes.Clone(key), false, nil
		}
	}
	if err := it.Error(); err != nil {
		return nil, false, fmt.Errorf("rawdb: directory page iterator: %w", err)
	}
	return nil, true, ctx.Err()
}
