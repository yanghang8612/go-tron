package snapshots

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type historyBuildReadViewSource struct {
	ethdb.KeyValueStore
	factory        pointread.KeyValueSnapshotter
	afterCapture   func()
	opened, closed int
}

func (s *historyBuildReadViewSource) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	view, err := s.factory.NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	s.opened++
	if s.afterCapture != nil {
		s.afterCapture()
	}
	return &historyBuildReadViewSnapshot{KeyValueSnapshot: view, source: s}, nil
}

type historyBuildReadViewSnapshot struct {
	pointread.KeyValueSnapshot
	source *historyBuildReadViewSource
}

func (s *historyBuildReadViewSnapshot) Close() error {
	s.source.closed++
	return s.KeyValueSnapshot.Close()
}

func TestStateDomainHistoryBuildPinsAllInputPasses(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		name := "tx-range"
		if bounded {
			name = "block-range"
		}
		t.Run(name, func(t *testing.T) {
			db, err := pebbledb.New(t.TempDir(), 16, 16, "test/history-build-read-view/", false, pebbledb.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			for block := uint64(1); block <= 2; block++ {
				row := &rawdb.StateDomainChange{BlockNum: block, TxNum: block, Seq: 1,
					FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{common.AddressPrefixMainnet, 0x75},
					Domain: kvdomains.SystemDelegation, Key: []byte("relation"), PrevExists: true, Prev: []byte{byte(block)}}
				if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{row}); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateTxRange(db, block, common.Hash{byte(block)}, block, block); err != nil {
					t.Fatal(err)
				}
			}
			source := &historyBuildReadViewSource{KeyValueStore: db, factory: db}
			source.afterCapture = func() {
				if err := rawdb.DeleteStateDomainChangeBlocks(db, []uint64{1, 2}); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			var refs []SegmentRef
			if bounded {
				refs, err = BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(source, dir, 1, 2, 1, 2, "history/state-domain-change-pinned.seg")
			} else {
				refs, err = BuildStateDomainChangeHistorySegmentsFromDB(source, dir, 1, 2, "history/state-domain-change-pinned.seg")
			}
			if err != nil || source.opened != 1 || source.closed != 1 {
				t.Fatalf("build err=%v opened=%d closed=%d", err, source.opened, source.closed)
			}
			rows := 0
			for _, ref := range refs {
				if ref.Kind != SegmentHistory {
					continue
				}
				changes, err := openStateDomainChangeHistoryChanges(dir, ref)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range changes {
					if !bytes.Equal(row.Prev, []byte{byte(row.BlockNum)}) {
						t.Fatalf("wrong cold previous value: %+v", row)
					}
					rows++
				}
			}
			if rows != 2 {
				t.Fatalf("cold rows=%d want=2", rows)
			}
		})
	}
}
