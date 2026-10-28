package snapshots

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSmallSystemSelectiveSourceMatchesFullDictionary(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	seq := uint64(0)
	put := func(flat rawdb.StateFlatDomain, owner common.Address, gen uint64, domain kvdomains.KVDomain, key []byte) {
		t.Helper()
		seq++
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1, Seq: seq, FlatDomain: flat, Owner: owner, Generation: gen, Domain: domain, Key: key}); err != nil {
			t.Fatal(err)
		}
	}
	// More than one sparse directory group, owner-compressed keys, and a large
	// unrelated namespace under the same system owner must not become a scan.
	for i := uint64(0); i < 18000; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], i)
		put(rawdb.StateFlatDomainKVLatest, common.SystemAccountAddress, i%3, kvdomains.SystemShielded, key[:])
	}
	for _, gen := range []uint64{0, 1, 2, math.MaxUint64} {
		for _, domain := range []kvdomains.KVDomain{kvdomains.SystemDynamicProperty, kvdomains.SystemForkVote} {
			for _, key := range [][]byte{nil, []byte("a"), []byte("future-new-key")} {
				put(rawdb.StateFlatDomainKVLatest, common.SystemAccountAddress, gen, domain, key)
			}
		}
	}
	for _, id := range []common.AccountID{common.SystemAccountID, {}} {
		for _, flat := range []rawdb.StateFlatDomain{rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration} {
			put(flat, id.Address(common.AddressPrefixMainnet), 0, 0, nil)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-selective.seg")
	if err != nil {
		t.Fatal(err)
	}
	var h, a SegmentRef
	for _, r := range refs {
		if r.Kind == SegmentHistory {
			h = r
		}
		if r.Kind == SegmentAccessor {
			a = r
		}
	}
	full, err := smallSystemHistorySourceKeys(context.Background(), dir, h, a)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := collectSmallSystemHistorySourceKeys(context.Background(), dir, h, a, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fast, full) || len(full) != 28 {
		t.Fatal("selective membership differs from full dictionary", len(fast), len(full), err)
	}
	path := filepath.Join(dir, a.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = collectSmallSystemHistorySourceKeys(context.Background(), dir, h, a, true); err == nil {
		t.Fatal("unrelated physical corruption bypassed source checksum")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = collectSmallSystemHistorySourceKeys(ctx, dir, h, a, true); err != context.Canceled {
		t.Fatal("cancellation", err)
	}
}
