package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

func validateHistoryStagingColdBinding(binding HistoryStagingColdBinding) error {
	first, last, err := StateHistoryChunkBucketBounds(binding.Bucket)
	if err != nil || binding.Version != HistoryStagingFormatVersion || binding.Epoch == 0 || binding.BindingEpoch == 0 || binding.ManifestEpoch == 0 || len(binding.Spans) == 0 {
		return ErrHistoryStagingConflict
	}
	var previous uint64
	for i, span := range binding.Spans {
		if span.From < first || span.To > last || span.To < span.From || (i > 0 && span.From <= previous) || span.ContentID == ([32]byte{}) || span.SemanticHash == ([32]byte{}) || span.TxRangeDigest == ([32]byte{}) {
			return ErrHistoryStagingConflict
		}
		previous = span.To
	}
	return nil
}

func (m *HistoryStagingManager) ReadColdBindingAt(epoch, bucket uint64) (HistoryStagingColdBinding, bool, error) {
	var binding HistoryStagingColdBinding
	if m == nil {
		return binding, false, errors.New("rawdb: missing staging manager")
	}
	present, err := readHistoryStagingValue(m.hot, historyStagingColdBindingKey(epoch, bucket), &binding)
	if err != nil || !present {
		return binding, present, err
	}
	if binding.Epoch != epoch || binding.Bucket != bucket || validateHistoryStagingColdBinding(binding) != nil {
		return HistoryStagingColdBinding{}, false, ErrHistoryStagingConflict
	}
	return binding, true, nil
}

// CertifyColdRange publishes a complete replacement binding for one bucket.
// verify authenticates the cold trio and canonical/tx-range equivalence while
// the caller holds its outer writer/manifest guard. It runs before routeMu.
// Route+binding+reverse refs commit atomically, then SyncKeyValue makes them
// durable before any source/target payload can be deleted.
func (m *HistoryStagingManager) CertifyColdRange(ctx context.Context, binding HistoryStagingColdBinding, verify func() error) error {
	if m == nil || ctx == nil || verify == nil {
		return errors.New("rawdb: missing cold binding proof")
	}
	if err := validateHistoryStagingColdBinding(binding); err != nil {
		return err
	}
	if err := verify(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	m.routeMu.Lock()
	if epoch, err := m.CurrentEpoch(); err != nil || epoch != binding.Epoch {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		m.routeMu.Unlock()
		return ErrHistoryStagingResetting
	}
	route, present, err := m.ReadRoute(binding.Bucket)
	if err != nil || !present || route.Epoch != binding.Epoch {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	old, hasOld, err := m.ReadColdBindingAt(binding.Epoch, binding.Bucket)
	if err != nil {
		m.routeMu.Unlock()
		return err
	}
	if hasOld {
		if reflect.DeepEqual(binding, old) {
			m.routeMu.Unlock()
			return m.syncHot()
		}
		if binding.BindingEpoch != old.BindingEpoch+1 {
			m.routeMu.Unlock()
			return ErrHistoryStagingConflict
		}
	} else if binding.BindingEpoch != 1 {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	if err := writeHistoryStagingValue(batch, historyStagingColdBindingKey(binding.Epoch, binding.Bucket), binding); err != nil {
		m.routeMu.Unlock()
		return err
	}
	oldIDs, newIDs := make(map[[32]byte]struct{}), make(map[[32]byte]struct{})
	for _, span := range old.Spans {
		oldIDs[span.ContentID] = struct{}{}
	}
	for _, span := range binding.Spans {
		newIDs[span.ContentID] = struct{}{}
	}
	for id := range oldIDs {
		if _, kept := newIDs[id]; !kept {
			if err := batch.Delete(historyStagingColdRefKey(id, binding.Epoch, binding.Bucket)); err != nil {
				m.routeMu.Unlock()
				return err
			}
		}
	}
	for id := range newIDs {
		if err := batch.Put(historyStagingColdRefKey(id, binding.Epoch, binding.Bucket), []byte{1}); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	route.ColdBindingEpoch = binding.BindingEpoch
	if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, binding.Bucket), route); err != nil {
		m.routeMu.Unlock()
		return err
	}
	// Once a publication is attempted, no physical cold GC may rely on
	// in-memory reverse refs until the source WAL is known durable.
	m.coldGCUncertain = true
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	m.coldGCUncertain = false
	return nil
}

// RebindCold walks only reverse references to the replaced file content ID.
// The caller's verify callback must certify the replacement manifest and
// semantic equivalence for EVERY affected subrange. Each route batch is
// bounded at 128 buckets and independently synced; old refs remain for any
// unprocessed bucket after an interruption.
func (m *HistoryStagingManager) RebindCold(ctx context.Context, oldContentID [32]byte, manifestEpoch uint64, build func(HistoryStagingColdBinding) (HistoryStagingColdBinding, error), verify func(HistoryStagingColdBinding, HistoryStagingColdBinding) error) error {
	if m == nil || ctx == nil || build == nil || verify == nil || oldContentID == ([32]byte{}) || manifestEpoch == 0 {
		return errors.New("rawdb: invalid cold rebind")
	}
	prefix := append(bytes.Clone(historyStagingColdRefPrefix), oldContentID[:]...)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentEpoch, err := m.CurrentEpoch()
		if err != nil {
			return err
		}
		var epochStart [8]byte
		binary.BigEndian.PutUint64(epochStart[:], currentEpoch)
		// Older reset epochs remain pinned by their refs until explicit
		// reconciliation. They must not block rebinding the current epoch;
		// CanGCCold will still deny their physical file deletion.
		it := m.hot.NewIterator(prefix, epochStart[:])
		refs := make([][2]uint64, 0, 128)
		for it.Next() && len(refs) < cap(refs) {
			key := it.Key()
			if len(key) != len(prefix)+16 || !bytes.HasPrefix(key, prefix) || len(it.Value()) != 1 || it.Value()[0] != 1 {
				it.Release()
				return ErrHistoryStagingConflict
			}
			refEpoch := binary.BigEndian.Uint64(key[len(prefix):])
			if refEpoch != currentEpoch {
				it.Release()
				return ErrHistoryStagingConflict
			}
			refs = append(refs, [2]uint64{refEpoch, binary.BigEndian.Uint64(key[len(prefix)+8:])})
		}
		err = it.Error()
		it.Release()
		if err != nil {
			return err
		}
		if len(refs) == 0 {
			return nil
		}
		// Authenticate each replacement outside routeMu and the cold-GC
		// publication fence. Only this bounded page is kept in memory.
		type preparedBinding struct {
			old HistoryStagingColdBinding
			new HistoryStagingColdBinding
		}
		prepared := make([]preparedBinding, 0, len(refs))
		for _, ref := range refs {
			old, present, err := m.ReadColdBindingAt(ref[0], ref[1])
			if err != nil || !present {
				return ErrHistoryStagingConflict
			}
			foundOld := false
			for _, span := range old.Spans {
				foundOld = foundOld || span.ContentID == oldContentID
			}
			if !foundOld {
				return ErrHistoryStagingConflict
			}
			newBinding, err := build(old)
			if err != nil {
				return err
			}
			if newBinding.Bucket != old.Bucket || newBinding.Epoch != old.Epoch || newBinding.BindingEpoch != old.BindingEpoch+1 || newBinding.ManifestEpoch != manifestEpoch || validateHistoryStagingColdBinding(newBinding) != nil || !historyStagingSameCoverage(old.Spans, newBinding.Spans) {
				return fmt.Errorf("rawdb: replacement cold binding for bucket %d is incomplete", ref[1])
			}
			for _, span := range newBinding.Spans {
				if span.ContentID == oldContentID {
					return ErrHistoryStagingConflict
				}
			}
			if err := verify(old, newBinding); err != nil {
				return err
			}
			prepared = append(prepared, preparedBinding{old: old, new: newBinding})
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// A reset cannot start while this page is published. The route lock
		// covers only O(128) version checks and one atomic Pebble batch; the
		// durability Sync occurs after it is released. The GC fence spans
		// both operations so an old file is never unlinked from a merely
		// visible, unsynced reverse-index update.
		m.jobMu.RLock()
		m.coldGCMu.Lock()
		writeErr := func() error {
			m.routeMu.Lock()
			defer m.routeMu.Unlock()
			if epoch, err := m.CurrentEpoch(); err != nil || epoch != currentEpoch {
				return ErrHistoryStagingConflict
			}
			if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
				return ErrHistoryStagingResetting
			}
			batch := m.hot.NewBatch()
			defer batch.Reset()
			for _, item := range prepared {
				old, present, err := m.ReadColdBindingAt(item.old.Epoch, item.old.Bucket)
				if err != nil || !present || !reflect.DeepEqual(old, item.old) {
					return ErrHistoryStagingConflict
				}
				route, present, err := m.ReadRoute(item.old.Bucket)
				if err != nil || !present || route.Epoch != item.old.Epoch || route.ColdBindingEpoch != item.old.BindingEpoch {
					return ErrHistoryStagingConflict
				}
				if err := writeHistoryStagingValue(batch, historyStagingColdBindingKey(item.new.Epoch, item.new.Bucket), item.new); err != nil {
					return err
				}
				oldIDs, newIDs := make(map[[32]byte]struct{}), make(map[[32]byte]struct{})
				for _, span := range item.old.Spans {
					oldIDs[span.ContentID] = struct{}{}
				}
				for _, span := range item.new.Spans {
					newIDs[span.ContentID] = struct{}{}
				}
				for id := range oldIDs {
					if _, kept := newIDs[id]; !kept {
						if err := batch.Delete(historyStagingColdRefKey(id, item.new.Epoch, item.new.Bucket)); err != nil {
							return err
						}
					}
				}
				for id := range newIDs {
					if err := batch.Put(historyStagingColdRefKey(id, item.new.Epoch, item.new.Bucket), []byte{1}); err != nil {
						return err
					}
				}
				route.ColdBindingEpoch = item.new.BindingEpoch
				if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, item.new.Bucket), route); err != nil {
					return err
				}
			}
			m.coldGCUncertain = true
			if err := batch.Write(); err != nil {
				return err
			}
			m.generation++
			return nil
		}()
		if writeErr == nil {
			writeErr = m.syncHot()
			if writeErr == nil {
				m.coldGCUncertain = false
			}
		}
		m.coldGCMu.Unlock()
		m.jobMu.RUnlock()
		if writeErr != nil {
			return writeErr
		}
	}
}

func historyStagingSameCoverage(a, b []HistoryStagingColdSpan) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	first, last := a[0].From, a[len(a)-1].To
	if b[0].From != first || b[len(b)-1].To != last || last-first >= StateHistoryChunkBucketBlocks {
		return false
	}
	for block := first; block <= last; block++ {
		coveredA, coveredB := false, false
		for _, span := range a {
			coveredA = coveredA || (block >= span.From && block <= span.To)
		}
		for _, span := range b {
			coveredB = coveredB || (block >= span.From && block <= span.To)
		}
		if coveredA != coveredB {
			return false
		}
	}
	return true
}

// CanGCCold denies physical retired-file deletion while ANY durable binding
// or in-process route view can still reference it. The cold manager must also
// enforce its own manifest/retired leases and publication guard.
func (m *HistoryStagingManager) CanGCCold(contentID [32]byte) (bool, error) {
	if m == nil || contentID == ([32]byte{}) {
		return false, errors.New("rawdb: invalid cold GC identity")
	}
	m.coldGCMu.RLock()
	defer m.coldGCMu.RUnlock()
	return m.canGCColdLocked(contentID)
}

// WithColdGCLease keeps the durable-reference publication fence held through
// the caller's physical unlink. A standalone CanGCCold result is only a
// snapshot and must not authorize a later unlink: a new binding could be
// published in between. The callback must not publish cold bindings.
func (m *HistoryStagingManager) WithColdGCLease(contentID [32]byte, unlink func() error) (bool, error) {
	if m == nil || contentID == ([32]byte{}) || unlink == nil {
		return false, errors.New("rawdb: invalid cold GC lease")
	}
	m.coldGCMu.RLock()
	defer m.coldGCMu.RUnlock()
	allowed, err := m.canGCColdLocked(contentID)
	if err != nil || !allowed {
		return false, err
	}
	if err := unlink(); err != nil {
		return false, err
	}
	return true, nil
}

func (m *HistoryStagingManager) canGCColdLocked(contentID [32]byte) (bool, error) {
	if m.coldGCUncertain {
		return false, nil
	}
	prefix := append(bytes.Clone(historyStagingColdRefPrefix), contentID[:]...)
	it := m.hot.NewIterator(prefix, nil)
	hasRef := it.Next()
	err := it.Error()
	it.Release()
	if err != nil || hasRef {
		return false, err
	}
	claimPrefix := append(bytes.Clone(historyStagingClaimColdRefPrefix), contentID[:]...)
	it = m.hot.NewIterator(claimPrefix, nil)
	hasClaimRef := it.Next()
	err = it.Error()
	it.Release()
	if err != nil || hasClaimRef {
		return false, err
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	for _, count := range m.leases {
		if count != 0 {
			return false, nil
		}
	}
	return true, nil
}

// SyncColdDependencyPublications is the final same-process recovery fence
// after rechecking a current manifest's affected bindings. A prior batch may
// be visible in Pebble while its explicit Sync failed; an empty reverse-index
// rebind loop must not make that publication serveable without a fresh Sync.
func (m *HistoryStagingManager) SyncColdDependencyPublications() error {
	if m == nil {
		return errors.New("rawdb: missing staging manager")
	}
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	if !m.coldGCUncertain {
		return nil
	}
	if err := m.syncHot(); err != nil {
		return err
	}
	m.coldGCUncertain = false
	return nil
}
