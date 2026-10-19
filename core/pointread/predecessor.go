package pointread

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
)

// PrefixPredecessor returns the largest key with prefix strictly below the
// absolute upper bound. Returned bytes are owned; nil upper means no bound.
type PrefixPredecessor interface {
	SeekPrefixBefore(prefix, upper []byte) (key, value []byte, ok bool, err error)
}

// SeekBefore uses a native reverse seek where available. The forward fallback
// keeps memory backends and deliberately simple test adapters correct.
func SeekBefore(source ethdb.Iteratee, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	if seeker, yes := source.(PrefixPredecessor); yes {
		return seeker.SeekPrefixBefore(prefix, upper)
	}
	it := source.NewIterator(prefix, nil)
	for it.Next() {
		if upper != nil && bytes.Compare(it.Key(), upper) >= 0 {
			break
		}
		key = bytes.Clone(it.Key())
		value = bytes.Clone(it.Value())
		ok = true
	}
	err = it.Error()
	it.Release()
	err = errors.Join(err, it.Error())
	return
}
