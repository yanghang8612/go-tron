package blockbuffer

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSharedHistoryPackAncestorLifecycle(t *testing.T) {
	rawdb.SetStateHistoryCrossBlockDedup(true)
	t.Cleanup(func() { rawdb.SetStateHistoryCrossBlockDedup(false) })
	for _, outcome := range []string{"separate_flush", "failed_group_retry", "ancestor_failure", "reorg_suffix"} {
		t.Run(outcome, func(t *testing.T) {
			base := newHistoryChunkBaseTest(t)
			b := New(base)
			b.SetMaxInflight(2)
			first := sharedHistoryTestRow(42, 20260913)
			second := sharedHistoryTestRow(43, 20260913)
			b.BeginBlock(bufHash(42), 42)
			writeSharedHistoryTestRow(t, b, first)
			ancestor, _ := b.NewestInflight()
			b.BeginBlock(bufHash(43), 43)
			reusedBefore := sharedHistoryTestCounter(t, "reused_raw_bytes")
			writeSharedHistoryTestRow(t, b, second)
			if reused := sharedHistoryTestCounter(t, "reused_raw_bytes") - reusedBefore; reused < 128<<10 {
				t.Fatalf("child reused only %d bytes from its inflight ancestor", reused)
			}
			descendant, _ := b.NewestInflight()
			assertSharedHistoryTestRow(t, b, second)
			if got, ok, err := rawdb.ReadStateDomainChange(base, second.BlockNum, second.Seq); err != nil || ok || got != nil {
				t.Fatalf("uncommitted history reached base: row = %v, exists = %v, err = %v", got, ok, err)
			}

			switch outcome {
			case "separate_flush":
				if err := b.CommitInflight(descendant); err == nil {
					t.Fatal("dependent history promoted before its ancestor")
				}
				if err := b.CommitInflight(ancestor); err != nil {
					t.Fatal(err)
				}
				if err := b.FlushUpTo(first.BlockNum, base); err != nil {
					t.Fatal(err)
				}
				assertSharedHistoryTestRow(t, b, second)
				if err := b.CommitInflight(descendant); err != nil {
					t.Fatal(err)
				}
				if err := b.FlushUpTo(second.BlockNum, base); err != nil {
					t.Fatal(err)
				}
				assertSharedHistoryTestRow(t, base, first)
				assertSharedHistoryTestRow(t, base, second)
			case "failed_group_retry":
				if err := b.CommitInflight(ancestor); err != nil {
					t.Fatal(err)
				}
				if err := b.CommitInflight(descendant); err != nil {
					t.Fatal(err)
				}
				if err := b.FlushUpTo(second.BlockNum, &failingBatcher{KeyValueStore: base}); !errors.Is(err, errForcedBatchWrite) {
					t.Fatalf("failed group error = %v", err)
				}
				assertSharedHistoryTestRow(t, b, first)
				assertSharedHistoryTestRow(t, b, second)
				assertHistoryBaseEmptyTest(t, base)
				if err := b.FlushUpTo(second.BlockNum, base); err != nil {
					t.Fatal(err)
				}
				assertSharedHistoryTestRow(t, base, second)
			case "ancestor_failure", "reorg_suffix":
				if outcome == "ancestor_failure" {
					// core's ordered failure handler owns dependent-job cleanup.
					b.DiscardInflight(ancestor)
					b.DiscardInflight(descendant)
				} else {
					if err := b.CommitInflight(ancestor); err != nil {
						t.Fatal(err)
					}
					if err := b.CommitInflight(descendant); err != nil {
						t.Fatal(err)
					}
					b.DiscardBlock(bufHash(43))
					b.DiscardBlock(bufHash(42))
				}
				if err := b.FlushUpTo(second.BlockNum, base); err != nil {
					t.Fatal(err)
				}
				assertHistoryBaseEmptyTest(t, base)
				if _, ok, err := rawdb.ReadStateDomainChange(b, second.BlockNum, second.Seq); err != nil || ok {
					t.Fatalf("discarded reference remains visible: exists = %v, err = %v", ok, err)
				}
				// Reusing identical bytes on the replacement branch must stage
				// their chunks again; stale existence caches cannot satisfy this.
				b.BeginBlock(bufHash(44), first.BlockNum)
				writeSharedHistoryTestRow(t, b, first)
				b.CommitBlock()
				if err := b.FlushUpTo(first.BlockNum, base); err != nil {
					t.Fatal(err)
				}
				assertSharedHistoryTestRow(t, base, first)
			}
		})
	}
}

func TestSharedHistoryPackSnapshotSurvivesReorgAndReplacement(t *testing.T) {
	rawdb.SetStateHistoryCrossBlockDedup(true)
	t.Cleanup(func() { rawdb.SetStateHistoryCrossBlockDedup(false) })
	base := newHistoryChunkBaseTest(t)
	b := New(base)
	b.SetMaxInflight(2)
	first := sharedHistoryTestRow(42, 20260913)
	second := sharedHistoryTestRow(43, 20260913)
	b.BeginBlock(bufHash(42), 42)
	writeSharedHistoryTestRow(t, b, first)
	ancestor, _ := b.NewestInflight()
	b.BeginBlock(bufHash(43), 43)
	writeSharedHistoryTestRow(t, b, second)
	descendant, _ := b.NewestInflight()
	snapshot, err := b.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	b.DiscardInflight(ancestor)
	b.DiscardInflight(descendant)
	replacement := sharedHistoryTestRow(43, 20260914)
	b.BeginBlock(bufHash(44), replacement.BlockNum)
	writeSharedHistoryTestRow(t, b, replacement)
	b.CommitBlock()
	if err := b.FlushUpTo(replacement.BlockNum, base); err != nil {
		t.Fatal(err)
	}
	assertSharedHistoryTestRow(t, base, replacement)
	assertSharedHistoryTestRow(t, snapshot, first)
	assertSharedHistoryTestRow(t, snapshot, second)
}

func TestSharedHistoryPackMemoryWrappersKeepReadableInlinePacks(t *testing.T) {
	rawdb.SetStateHistoryCrossBlockDedup(true)
	t.Cleanup(func() { rawdb.SetStateHistoryCrossBlockDedup(false) })
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	for name, store := range map[string]ethdb.KeyValueStore{
		"memory":              base,
		"memory chain":        rawdb.NewChainDB(base, rawdb.NoopAncient{}),
		"memory geth wrapper": rawdb.WrapKeyValueStore(base),
	} {
		t.Run(name, func(t *testing.T) {
			b := New(store)
			row := sharedHistoryTestRow(42, 20260913)
			b.BeginBlock(bufHash(42), 42)
			selected := sharedHistoryTestCounter(t, "selected")
			if err := rawdb.WriteStateDomainChangeBlockRows(b, []*rawdb.StateDomainChange{row}); err != nil {
				t.Fatal(err)
			}
			if delta := sharedHistoryTestCounter(t, "selected") - selected; delta != 0 {
				t.Fatalf("non-snapshot base wrote %d shared packs", delta)
			}
			assertSharedHistoryTestRow(t, b, row)
		})
	}
}

func sharedHistoryTestRow(blockNum uint64, seed int64) *rawdb.StateDomainChange {
	prev := make([]byte, 768<<10)
	_, _ = rand.New(rand.NewSource(seed)).Read(prev)
	return &rawdb.StateDomainChange{
		BlockNum: blockNum, Seq: 1, TxNum: blockNum * 10,
		FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3}, Generation: 7,
		Domain: kvdomains.SystemDelegation, Key: []byte("history-chunk-fixture"), PrevExists: true, Prev: prev,
	}
}

func writeSharedHistoryTestRow(t *testing.T, b *Buffer, row *rawdb.StateDomainChange) {
	t.Helper()
	before := sharedHistoryTestCounter(t, "selected")
	if err := rawdb.WriteStateDomainChangeBlockRows(b, []*rawdb.StateDomainChange{row}); err != nil {
		t.Fatal(err)
	}
	if delta := sharedHistoryTestCounter(t, "selected") - before; delta != 1 {
		t.Fatalf("fixture did not select a real shared history pack: selected delta = %d", delta)
	}
}

func assertSharedHistoryTestRow(t *testing.T, reader ethdb.KeyValueReader, want *rawdb.StateDomainChange) {
	t.Helper()
	got, ok, err := rawdb.ReadStateDomainChange(reader, want.BlockNum, want.Seq)
	if err != nil || !ok || got == nil {
		t.Fatalf("read shared history row: present = %v, err = %v", ok, err)
	}
	if got.BlockNum != want.BlockNum || got.Seq != want.Seq || got.TxNum != want.TxNum ||
		got.FlatDomain != want.FlatDomain || got.Owner != want.Owner || got.Generation != want.Generation ||
		got.Domain != want.Domain || !bytes.Equal(got.Key, want.Key) || got.PrevExists != want.PrevExists || !bytes.Equal(got.Prev, want.Prev) {
		t.Fatal("shared history round trip changed row identity, transaction, or Prev")
	}
}

func sharedHistoryTestCounter(t *testing.T, field string) int64 {
	t.Helper()
	counter, ok := metrics.Get("state/history/shared/" + field).(*metrics.Counter)
	if !ok {
		t.Fatalf("shared history counter %q is not registered", field)
	}
	return counter.Snapshot().Count()
}

func assertHistoryBaseEmptyTest(t *testing.T, base ethdb.Iteratee) {
	t.Helper()
	it := base.NewIterator(nil, nil)
	defer it.Release()
	if it.Next() {
		t.Fatalf("uncommitted history key %x reached the durable base", it.Key())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
}
