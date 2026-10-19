package pebbledb

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
)

type reverseIterSource interface {
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}

func seekBefore(source reverseIterSource, prefix, upper []byte) (key, value []byte, ok bool, retErr error) {
	bound := upperBound(prefix)
	if upper != nil && (bound == nil || bytes.Compare(upper, bound) < 0) {
		bound = bytes.Clone(upper)
	}
	if bound != nil && bytes.Compare(bound, prefix) <= 0 {
		return nil, nil, false, nil
	}
	it, err := source.NewIter(&pebble.IterOptions{LowerBound: bytes.Clone(prefix), UpperBound: bound})
	if err != nil {
		return nil, nil, false, err
	}
	if bound == nil {
		ok = it.Last()
	} else {
		ok = it.SeekLT(bound)
	}
	if ok {
		key = bytes.Clone(it.Key())
		value = bytes.Clone(it.Value())
	}
	retErr = errors.Join(it.Error(), it.Close())
	return
}
func (d *Database) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	return seekBefore(d.db, prefix, upper)
}
func (s *keyValueSnapshot) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	if s == nil || s.snapshot == nil {
		return nil, nil, false, pebble.ErrClosed
	}
	return seekBefore(s.snapshot, prefix, upper)
}
