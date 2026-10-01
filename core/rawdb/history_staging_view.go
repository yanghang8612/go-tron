package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type historyStagingRouteCache struct {
	bucket uint64
	route  HistoryStagingRoute
}

// HistoryStagingView pins one source sequence, one target sequence and one
// route generation. It only routes hot-history payload families. Canonical
// state/head/latest and stage-progress keys always come from its source view.
type HistoryStagingView struct {
	manager     *HistoryStagingManager
	hot         StateHistoryReadView
	stage       pointread.KeyValueSnapshot
	releaseHot  func() error
	releaseCold func() error
	generation  uint64
	epoch       uint64
	cache       atomic.Pointer[historyStagingRouteCache]
	closed      atomic.Bool
}

// AcquireView must be called while the caller holds its outer chain guard.
// captureHot runs inside routeMu, so the source snapshot and route generation
// cannot be mixed across adoption. captureCold may be nil when the caller has
// separately pinned its cold manifest/retired-file lease.
func (m *HistoryStagingManager) AcquireView(captureHot func() (StateHistoryReadView, func() error, error), captureCold func() (func() error, error)) (*HistoryStagingView, error) {
	if m == nil || captureHot == nil {
		return nil, errors.New("rawdb: missing staging view capture")
	}
	m.routeMu.RLock()
	defer m.routeMu.RUnlock()
	if err := m.VerifyIdentity(); err != nil {
		return nil, err
	}
	if intent, present, err := m.ReadResetIntent(); err != nil {
		return nil, err
	} else if present && !intent.Complete {
		return nil, ErrHistoryStagingResetting
	}
	hot, releaseHot, err := captureHot()
	if err != nil {
		return nil, err
	}
	if hot == nil || !hot.IsPinnedKeyValueView() || releaseHot == nil {
		if releaseHot != nil {
			_ = releaseHot()
		}
		return nil, ErrStateHistoryReadViewUnpinned
	}
	epochValue, present, err := readPresentValue(hot, historyStagingEpochKey, "history staging epoch")
	if err != nil || !present || len(epochValue) != 8 || binary.BigEndian.Uint64(epochValue) == 0 {
		_ = releaseHot()
		return nil, ErrHistoryStagingIncomplete
	}
	epoch := binary.BigEndian.Uint64(epochValue)
	stage, err := m.stage.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil || stage == nil {
		_ = releaseHot()
		if err == nil {
			err = errors.New("rawdb: nil staging snapshot")
		}
		return nil, err
	}
	var releaseCold func() error
	if captureCold != nil {
		releaseCold, err = captureCold()
		if err != nil {
			_ = stage.Close()
			_ = releaseHot()
			return nil, err
		}
	}
	m.leaseMu.Lock()
	m.leases[m.generation]++
	m.leaseMu.Unlock()
	return &HistoryStagingView{manager: m, hot: hot, stage: stage, releaseHot: releaseHot, releaseCold: releaseCold, generation: m.generation, epoch: epoch}, nil
}

func (v *HistoryStagingView) IsPinnedKeyValueView() bool { return v != nil && !v.closed.Load() }

func (v *HistoryStagingView) Close() error {
	if v == nil || !v.closed.CompareAndSwap(false, true) {
		return nil
	}
	var errs []error
	if v.releaseCold != nil {
		errs = append(errs, v.releaseCold())
	}
	errs = append(errs, v.stage.Close(), v.releaseHot())
	v.manager.leaseMu.Lock()
	if count := v.manager.leases[v.generation]; count <= 1 {
		delete(v.manager.leases, v.generation)
	} else {
		v.manager.leases[v.generation] = count - 1
	}
	v.manager.leaseMu.Unlock()
	return errors.Join(errs...)
}

func (v *HistoryStagingView) Route(bucket uint64) (HistoryStagingRoute, error) {
	if v == nil || v.closed.Load() {
		return HistoryStagingRoute{}, errors.New("rawdb: closed staging view")
	}
	if cache := v.cache.Load(); cache != nil && cache.bucket == bucket {
		return cache.route, nil
	}
	var route HistoryStagingRoute
	present, err := readHistoryStagingValue(v.hot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), &route)
	if err != nil {
		return route, err
	}
	if !present || route.Version != HistoryStagingFormatVersion || route.Bucket != bucket || route.Epoch != v.epoch || route.Owner < HistoryStagingOwnerSource || route.Owner > HistoryStagingOwnerCold {
		return route, ErrHistoryStagingIncomplete
	}
	if route.Owner == HistoryStagingOwnerTarget {
		var receipt HistoryStagingReceipt
		ok, err := readHistoryStagingValue(v.stage, historyStagingReceiptKey(route.Epoch, bucket), &receipt)
		if err != nil {
			return route, err
		}
		digest, err := historyStagingReceiptDigest(receipt)
		if err != nil || !ok || receipt.Version != HistoryStagingFormatVersion || receipt.Epoch != route.Epoch || digest != route.ReceiptDigest {
			return route, ErrHistoryStagingIncomplete
		}
	}
	if route.Owner == HistoryStagingOwnerCold || route.ColdBindingEpoch != 0 {
		binding, present, err := v.PinnedColdBindingForRoute(route)
		if err != nil || !present || (route.Owner == HistoryStagingOwnerCold && !historyStagingBindingCovers(binding, bucket*StateHistoryChunkBucketBlocks, bucket*StateHistoryChunkBucketBlocks+StateHistoryChunkBucketBlocks-1)) {
			return route, ErrHistoryStagingIncomplete
		}
	}
	v.cache.Store(&historyStagingRouteCache{bucket: bucket, route: route})
	return route, nil
}

func (v *HistoryStagingView) PinnedColdBindingForRoute(route HistoryStagingRoute) (HistoryStagingColdBinding, bool, error) {
	var binding HistoryStagingColdBinding
	if v == nil || v.closed.Load() {
		return binding, false, errors.New("rawdb: closed staging view")
	}
	present, err := readHistoryStagingValue(v.hot, historyStagingColdBindingKey(route.Epoch, route.Bucket), &binding)
	if err != nil || !present {
		return binding, present, err
	}
	if binding.Epoch != route.Epoch || binding.Bucket != route.Bucket || binding.BindingEpoch != route.ColdBindingEpoch || validateHistoryStagingColdBinding(binding) != nil {
		return HistoryStagingColdBinding{}, false, ErrHistoryStagingConflict
	}
	return binding, true, nil
}

func (v *HistoryStagingView) PinnedColdBinding(bucket uint64) (HistoryStagingColdBinding, bool, error) {
	route, err := v.Route(bucket)
	if err != nil {
		return HistoryStagingColdBinding{}, false, err
	}
	if route.ColdBindingEpoch == 0 {
		return HistoryStagingColdBinding{}, false, nil
	}
	return v.PinnedColdBindingForRoute(route)
}

func historyStagingBindingCovers(binding HistoryStagingColdBinding, first, last uint64) bool {
	next := first
	for _, span := range binding.Spans {
		if span.To < next {
			continue
		}
		if span.From > next {
			return false
		}
		if span.To >= last {
			return true
		}
		next = span.To + 1
	}
	return false
}

// RequireRoutedRange checks every bucket, including cold-only buckets with no
// physical hot key. Core must separately match each pinned cold binding to its
// pinned manifest before merging cold and hot rows.
func (v *HistoryStagingView) RequireRoutedRange(first, last uint64) error {
	if v == nil || last < first {
		return errors.New("rawdb: invalid staged history range")
	}
	for bucket := first / StateHistoryChunkBucketBlocks; bucket <= last/StateHistoryChunkBucketBlocks; bucket++ {
		route, err := v.Route(bucket)
		if err != nil {
			return err
		}
		if route.Owner == HistoryStagingOwnerCold || route.ColdBindingEpoch != 0 {
			binding, present, err := v.PinnedColdBinding(bucket)
			if err != nil || !present {
				return ErrHistoryStagingIncomplete
			}
			if route.Owner == HistoryStagingOwnerCold {
				bucketFirst, bucketLast, err := StateHistoryChunkBucketBounds(bucket)
				if err != nil {
					return err
				}
				if bucketFirst < first {
					bucketFirst = first
				}
				if bucketLast > last {
					bucketLast = last
				}
				if !historyStagingBindingCovers(binding, bucketFirst, bucketLast) {
					return ErrHistoryStagingIncomplete
				}
			}
		}
	}
	return nil
}

func (v *HistoryStagingView) RequireHotRange(first, last uint64) error {
	if err := v.RequireRoutedRange(first, last); err != nil {
		return err
	}
	for bucket := first / StateHistoryChunkBucketBlocks; bucket <= last/StateHistoryChunkBucketBlocks; bucket++ {
		route, _ := v.Route(bucket)
		if route.Owner == HistoryStagingOwnerCold {
			return ErrHistoryStagingColdOwned
		}
		binding, present, err := v.PinnedColdBinding(bucket)
		if err != nil {
			return err
		}
		if present {
			for _, span := range binding.Spans {
				if span.From <= last && span.To >= first {
					return ErrHistoryStagingColdOwned
				}
			}
		}
	}
	return nil
}

func (v *HistoryStagingView) isColdBlock(route HistoryStagingRoute, block uint64) (bool, error) {
	if route.Owner == HistoryStagingOwnerCold {
		return true, nil
	}
	if route.ColdBindingEpoch == 0 {
		return false, nil
	}
	binding, present, err := v.PinnedColdBindingForRoute(route)
	if err != nil || !present {
		return false, ErrHistoryStagingIncomplete
	}
	for _, span := range binding.Spans {
		if block >= span.From && block <= span.To {
			return true, nil
		}
	}
	return false, nil
}

func historyStagingPayloadBucket(key []byte) (uint64, bool, error) {
	switch {
	case bytes.HasPrefix(key, stateChangeSetPrefix):
		if len(key) != len(stateChangeSetPrefix)+16 {
			return 0, true, ErrHistoryStagingConflict
		}
		return binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) / StateHistoryChunkBucketBlocks, true, nil
	case bytes.HasPrefix(key, stateHistorySharedChunkPrefix):
		if len(key) != len(stateHistorySharedChunkPrefix)+8+32 {
			return 0, true, ErrHistoryStagingConflict
		}
		return binary.BigEndian.Uint64(key[len(stateHistorySharedChunkPrefix):]), true, nil
	case bytes.HasPrefix(key, stateHistorySharedBucketPrefix):
		if len(key) != len(stateHistorySharedBucketPrefix)+8 {
			return 0, true, ErrHistoryStagingConflict
		}
		return binary.BigEndian.Uint64(key[len(stateHistorySharedBucketPrefix):]), true, nil
	default:
		return 0, false, nil
	}
}

func (v *HistoryStagingView) Get(key []byte) ([]byte, error) {
	if v == nil || v.closed.Load() {
		return nil, errors.New("rawdb: closed staging view")
	}
	bucket, payload, err := historyStagingPayloadBucket(key)
	if err != nil {
		return nil, err
	}
	if !payload {
		return v.hot.Get(key)
	}
	route, err := v.Route(bucket)
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(key, stateChangeSetPrefix) {
		cold, err := v.isColdBlock(route, binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]))
		if err != nil {
			return nil, err
		}
		if cold {
			return nil, ErrHistoryStagingColdOwned
		}
	}
	switch route.Owner {
	case HistoryStagingOwnerSource:
		return v.hot.Get(key)
	case HistoryStagingOwnerTarget:
		return v.stage.Get(historyStagingPayloadKey(route.Epoch, key))
	case HistoryStagingOwnerCold:
		return nil, ErrHistoryStagingColdOwned
	default:
		return nil, ErrHistoryStagingIncomplete
	}
}

func (v *HistoryStagingView) Has(key []byte) (bool, error) {
	if v == nil || v.closed.Load() {
		return false, errors.New("rawdb: closed staging view")
	}
	bucket, payload, err := historyStagingPayloadBucket(key)
	if err != nil {
		return false, err
	}
	if !payload {
		return v.hot.Has(key)
	}
	route, err := v.Route(bucket)
	if err != nil {
		return false, err
	}
	if bytes.HasPrefix(key, stateChangeSetPrefix) {
		cold, err := v.isColdBlock(route, binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]))
		if err != nil {
			return false, err
		}
		if cold {
			return false, ErrHistoryStagingColdOwned
		}
	}
	switch route.Owner {
	case HistoryStagingOwnerSource:
		return v.hot.Has(key)
	case HistoryStagingOwnerTarget:
		return v.stage.Has(historyStagingPayloadKey(route.Epoch, key))
	case HistoryStagingOwnerCold:
		return false, ErrHistoryStagingColdOwned
	default:
		return false, ErrHistoryStagingIncomplete
	}
}

func (v *HistoryStagingView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if v == nil || v.closed.Load() {
		return &stateHistoryErrorIterator{err: errors.New("rawdb: closed staging view")}
	}
	if bytes.HasPrefix(prefix, stateChangeSetPrefix) || bytes.HasPrefix(prefix, stateHistorySharedChunkPrefix) || bytes.HasPrefix(prefix, stateHistorySharedBucketPrefix) {
		return &historyStagingMergedIterator{view: v, source: v.hot.NewIterator(prefix, start), target: v.stage.NewIterator(historyStagingPayloadKey(v.epoch, prefix), start)}
	}
	if len(prefix) == 0 || bytes.HasPrefix(stateChangeSetPrefix, prefix) || bytes.HasPrefix(stateHistorySharedChunkPrefix, prefix) || bytes.HasPrefix(stateHistorySharedBucketPrefix, prefix) {
		return &stateHistoryErrorIterator{err: errors.New("rawdb: broad history iteration requires an explicit prefix")}
	}
	return v.hot.NewIterator(prefix, start)
}

type historyStagingMergedIterator struct {
	view           *HistoryStagingView
	source, target ethdb.Iterator
	sKey, sValue   []byte
	tKey, tValue   []byte
	key, value     []byte
	sDone, tDone   bool
	err            error
}

func (it *historyStagingMergedIterator) fillSource() {
	if it.sKey != nil || it.sDone {
		return
	}
	if !it.source.Next() {
		it.err = errors.Join(it.err, it.source.Error())
		it.sDone = true
		return
	}
	it.sKey, it.sValue = bytes.Clone(it.source.Key()), bytes.Clone(it.source.Value())
}

func (it *historyStagingMergedIterator) fillTarget() {
	if it.tKey != nil || it.tDone {
		return
	}
	if !it.target.Next() {
		it.err = errors.Join(it.err, it.target.Error())
		it.tDone = true
		return
	}
	key := it.target.Key()
	epochPrefix := historyStagingPayloadEpochPrefix(it.view.epoch)
	if !bytes.HasPrefix(key, epochPrefix) {
		it.err = ErrHistoryStagingConflict
		return
	}
	it.tKey, it.tValue = bytes.Clone(key[len(epochPrefix):]), bytes.Clone(it.target.Value())
}

func (it *historyStagingMergedIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		it.fillSource()
		it.fillTarget()
		if it.err != nil || (it.sDone && it.tDone) {
			return false
		}
		compare := 0
		if it.sDone {
			compare = 1
		} else if it.tDone {
			compare = -1
		} else {
			compare = bytes.Compare(it.sKey, it.tKey)
		}
		var key []byte
		if compare <= 0 {
			key = it.sKey
		} else {
			key = it.tKey
		}
		bucket, payload, err := historyStagingPayloadBucket(key)
		if err != nil || !payload {
			it.err = fmt.Errorf("rawdb: malformed staged history iterator key %x: %w", key, err)
			return false
		}
		route, err := it.view.Route(bucket)
		if err != nil {
			it.err = err
			return false
		}
		if bytes.HasPrefix(key, stateChangeSetPrefix) {
			cold, err := it.view.isColdBlock(route, binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]))
			if err != nil {
				it.err = err
				return false
			}
			if cold {
				if compare <= 0 {
					it.sKey, it.sValue = nil, nil
				}
				if compare >= 0 {
					it.tKey, it.tValue = nil, nil
				}
				continue
			}
		}
		var value []byte
		visible := false
		if route.Owner == HistoryStagingOwnerSource && compare <= 0 {
			value, visible = it.sValue, true
		} else if route.Owner == HistoryStagingOwnerTarget && compare >= 0 {
			value, visible = it.tValue, true
		}
		if compare <= 0 {
			it.sKey, it.sValue = nil, nil
		}
		if compare >= 0 {
			it.tKey, it.tValue = nil, nil
		}
		if visible {
			it.key, it.value = key, value
			return true
		}
	}
}

func (it *historyStagingMergedIterator) Error() error  { return it.err }
func (it *historyStagingMergedIterator) Key() []byte   { return it.key }
func (it *historyStagingMergedIterator) Value() []byte { return it.value }
func (it *historyStagingMergedIterator) Release() {
	it.source.Release()
	it.target.Release()
}
