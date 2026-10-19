package blockbuffer

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func seekBeforeWithBase(base ethdb.KeyValueReader, o *overlayState, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	var best string
	for k, op := range o.m {
		if op.deleted || upper != nil && bytes.Compare([]byte(k), upper) >= 0 {
			continue
		}
		if !ok || k > best {
			best = k
			value = op.value
			ok = true
		}
	}
	if ok {
		key = []byte(best)
		value = bytes.Clone(value)
	}
	iter, yes := base.(ethdb.Iteratee)
	if !yes {
		return
	}
	limit := bytes.Clone(upper)
	for {
		k, v, found, e := pointread.SeekBefore(iter, prefix, limit)
		if e != nil {
			return nil, nil, false, e
		}
		if !found {
			return
		}
		if ok && bytes.Compare(key, k) >= 0 {
			return
		}
		if op, masked := o.m[string(k)]; masked {
			if !op.deleted {
				return bytes.Clone(k), bytes.Clone(op.value), true, nil
			}
			limit = k
			continue
		}
		if o.rangeDeletesString(string(k)) {
			limit = k
			continue
		}
		return k, v, true, nil
	}
}

func (b *Buffer) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	v := b.loadReadView()
	o := newOverlayState()
	for i := len(v.inflight) - 1; i >= 0; i-- {
		o.walk(v.inflight[i], prefix, nil)
	}
	for i := len(v.layers) - 1; i >= 0; i-- {
		o.walk(v.layers[i], prefix, nil)
	}
	return seekBeforeWithBase(b.base, o, prefix, upper)
}
func (v *LayerView) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	view := v.b.loadReadView()
	o := newOverlayState()
	o.walk(v.l, prefix, nil)
	for i := len(view.layers) - 1; i >= 0; i-- {
		o.walk(view.layers[i], prefix, nil)
	}
	return seekBeforeWithBase(v.b.base, o, prefix, upper)
}
func (s *ReadSnapshot) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	if s != nil && s.historyOnly && !rawdb.IsDirectHistoryPrefix(prefix) {
		return nil, nil, false, rawdb.ErrStateHistorySnapshotScope
	}
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return nil, nil, false, errors.New("blockbuffer: closed predecessor snapshot")
	}
	o := newOverlayState()
	for i := len(s.view.inflight) - 1; i >= 0; i-- {
		o.walk(s.view.inflight[i], prefix, nil)
	}
	for i := len(s.view.layers) - 1; i >= 0; i-- {
		o.walk(s.view.layers[i], prefix, nil)
	}
	return seekBeforeWithBase(s.base, o, prefix, upper)
}
