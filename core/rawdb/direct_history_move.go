package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// DirectHistoryMoveProgress describes physical work, not a semantic certificate.
type DirectHistoryMoveProgress struct {
	Family        string `json:"family"`
	CopiedRows    uint64 `json:"copied_rows"`
	CopiedBytes   uint64 `json:"copied_bytes"`
	DiscardedRows uint64 `json:"discarded_rows"`
}

// MoveDirectHistory converts the old physical layout once, while all stores are
// exclusively opened offline. It copies unchanged bytes and immediately deletes
// each durable page from its old store. Source deletion itself is the resume
// cursor: no plans, hashes, receipts, or semantic comparison are generated.
// Old routes are read only to select the existing authoritative copy; they are
// removed after conversion and never used by the new runtime.
func MoveDirectHistory(ctx context.Context, hot, legacy, history ethdb.KeyValueStore, pageBytes int, progress func(DirectHistoryMoveProgress) error) error {
	if ctx == nil || hot == nil || history == nil || pageBytes <= 0 {
		return errors.New("rawdb: invalid direct history move")
	}
	// A small store identity prevents a resumed move from switching to an
	// empty destination after earlier pages have already been deleted.
	_, hotPaired, err := ReadDirectHistoryPair(hot)
	if err != nil {
		return err
	}
	_, targetPaired, err := ReadDirectHistoryPair(history)
	if err != nil {
		return err
	}
	if !hotPaired && targetPaired {
		if err := AdoptUnmarkedDirectHistoryPair(hot, history); err != nil {
			return err
		}
	}
	if err := EnsureDirectHistoryPair(hot, history, true); err != nil {
		return err
	}
	marked, err := DirectHistoryLayoutInitialized(hot)
	if err != nil {
		return err
	}
	if marked {
		ok, err := DirectHistoryLayoutInitialized(history)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("rawdb: direct history target is missing")
		}
		return nil
	}
	identity, staged, err := ReadHistoryStagingIdentity(hot)
	if err != nil {
		return err
	}
	var manager *HistoryStagingManager
	var epoch uint64
	if staged {
		if legacy == nil {
			return errors.New("rawdb: legacy staging database is required for conversion")
		}
		manager, err = NewHistoryStagingManager(hot, legacy, identity)
		if err != nil {
			return err
		}
		if err := manager.VerifyIdentity(); err != nil {
			return err
		}
		epoch, err = manager.CurrentEpoch()
		if err != nil {
			return err
		}
	}
	routes := make(map[uint64]HistoryStagingOwner)
	owner := func(key []byte) (HistoryStagingOwner, error) {
		if manager == nil {
			return HistoryStagingOwnerSource, nil
		}
		bucket, payload, err := historyStagingPayloadBucket(key)
		if err != nil {
			return 0, err
		}
		if !payload {
			return HistoryStagingOwnerSource, nil
		}
		if v, ok := routes[bucket]; ok {
			return v, nil
		}
		route, present, err := manager.ReadRoute(bucket)
		if err != nil {
			return 0, err
		}
		v := HistoryStagingOwnerSource
		if present {
			if route.Epoch != epoch {
				return 0, fmt.Errorf("rawdb: legacy route epoch differs at bucket %d", bucket)
			}
			v = route.Owner
		}
		routes[bucket] = v
		return v, nil
	}
	moveRange := func(source ethdb.KeyValueStore, family string, prefix, end []byte, decode func([]byte) ([]byte, error), keep func([]byte) (bool, error)) error {
		var total DirectHistoryMoveProgress
		total.Family = family
		var after []byte
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			out, remove := history.NewBatch(), source.NewBatch()
			it := source.NewIterator(prefix, after)
			var last []byte
			count, used := 0, 0
			for it.Next() {
				if len(end) != 0 && bytes.Compare(it.Key(), end) >= 0 {
					break
				}
				if err := ctx.Err(); err != nil {
					it.Release()
					out.Close()
					remove.Close()
					return err
				}
				key, err := decode(it.Key())
				if err != nil {
					it.Release()
					out.Close()
					remove.Close()
					return err
				}
				selected, err := keep(key)
				if err != nil {
					it.Release()
					out.Close()
					remove.Close()
					return err
				}
				if selected {
					if err := out.Put(key, it.Value()); err != nil {
						it.Release()
						out.Close()
						remove.Close()
						return err
					}
					total.CopiedRows++
					total.CopiedBytes += uint64(len(key) + len(it.Value()))
				} else {
					total.DiscardedRows++
				}
				if err := remove.Delete(it.Key()); err != nil {
					it.Release()
					out.Close()
					remove.Close()
					return err
				}
				last = append(last[:0], it.Key()...)
				count++
				used += len(it.Key()) + len(it.Value())
				if used >= pageBytes {
					break
				}
			}
			err := it.Error()
			it.Release()
			if err == nil && count > 0 {
				err = out.Write()
				if err == nil {
					err = history.SyncKeyValue()
				}
				if err == nil {
					err = remove.Write()
				}
				if err == nil {
					err = source.SyncKeyValue()
				}
			}
			out.Close()
			remove.Close()
			if err != nil {
				return fmt.Errorf("move %s: %w", family, err)
			}
			if count == 0 {
				return nil
			}
			if progress != nil {
				if err := progress(total); err != nil {
					return err
				}
			}
			// A zero byte appended to the last exact key seeks strictly after it.
			after = append(append([]byte(nil), last[len(prefix):]...), 0)
		}
	}
	identityKey := func(key []byte) ([]byte, error) { return key, nil }
	for _, r := range DirectHistoryRanges() {
		if err := moveRange(hot, r.Name, r.Start, r.End, identityKey, func(key []byte) (bool, error) {
			v, err := owner(key)
			return v == HistoryStagingOwnerSource, err
		}); err != nil {
			return err
		}
	}
	if staged {
		prefix := LegacyHistoryPayloadPrefix(epoch)
		if err := moveRange(legacy, "legacy_target", prefix, prefixUpperBound(prefix), func(key []byte) ([]byte, error) {
			return DecodeLegacyHistoryPayloadKey(epoch, key)
		}, func(key []byte) (bool, error) {
			// Canonical tx ranges remain in hot in the old layout. Do not replace
			// them with the old staging copies.
			if bytes.HasPrefix(key, stateTxRangePrefix) {
				return false, nil
			}
			v, err := owner(key)
			return v == HistoryStagingOwnerTarget, err
		}); err != nil {
			return err
		}
	}
	if err := MarkDirectHistoryLayout(history); err != nil {
		return err
	}
	if err := history.SyncKeyValue(); err != nil {
		return err
	}
	// Leave old metadata until the hot format bit is durable. A crash before
	// this point can always resume by interpreting the same old ownership.
	if err := MarkDirectHistoryLayout(hot); err != nil {
		return err
	}
	return hot.SyncKeyValue()
}
