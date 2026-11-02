package main

import (
	"bytes"
	"fmt"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var bundleMagic = []byte{'G', 'T', 'P', 'G', 1}

// Read-only copy of the frozen experiment's complete bundle materialization.
// It validates all members before returning a requested row; no producer code
// or encoding path is added here.
func bundleValue(first, payload, target []byte) ([]byte, bool, int, error) {
	if len(payload) > 16<<10 || !bytes.HasPrefix(payload, bundleMagic) {
		return nil, false, 0, fmt.Errorf("not bounded bundle")
	}
	d := decoder{data: payload, off: len(bundleMagic)}
	count, e := d.word()
	if e != nil || count < 2 || count > 16 {
		return nil, false, 0, fmt.Errorf("invalid count")
	}
	owner, generation, domain, logical, ok := rawdb.DecodeStateKVLatestKey(first)
	if !ok || domain < kvdomains.AccountAsset || domain > kvdomains.AccountAssetOperationTimeV2 {
		return nil, false, 0, fmt.Errorf("bad bundle key")
	}
	prev := logical
	prevPhysical := first
	var found []byte
	for i := uint64(0); i < count; i++ {
		key := first
		if i > 0 {
			shared, e := d.word()
			if e != nil || shared > uint64(len(prev)) {
				return nil, false, 0, fmt.Errorf("bad shared prefix")
			}
			suffix, e := d.blob()
			if e != nil {
				return nil, false, 0, e
			}
			logical = append(append([]byte(nil), prev[:shared]...), suffix...)
			key = rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logical)
			if bytes.Compare(prevPhysical, key) >= 0 {
				return nil, false, 0, fmt.Errorf("unordered member")
			}
		}
		v, e := d.blob()
		if e != nil {
			return nil, false, 0, e
		}
		if e = rawdb.ValidateStateKVLatestValue(v); e != nil {
			return nil, false, 0, e
		}
		if bytes.Equal(key, target) {
			found = v
		}
		prev, prevPhysical = logical, key
	}
	if e = d.done(); e != nil {
		return nil, false, 0, e
	}
	return found, found != nil, int(count), nil
}
