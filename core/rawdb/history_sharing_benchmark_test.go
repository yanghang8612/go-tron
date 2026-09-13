package rawdb

import (
	"context"
	"testing"
)

func TestHistorySharingBenchmarkUsesProductionBlockDedupWithoutGlobalMutation(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(false)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(512<<10, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	baseline, _ := encodeStateDomainChangeBlockStorageWithDedup(raw, rows, true)
	snappy, _ := encodeStateDomainChangeBlockStorage(raw)
	if len(baseline)*8 >= len(snappy)*7 {
		t.Fatal("fixture does not distinguish real block dedup baseline")
	}
	b, err := NewHistorySharingBenchmark()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	sample, err := b.AddPack(context.Background(), rows[0].BlockNum, raw)
	if err != nil {
		t.Fatal(err)
	}
	if sample.BaselinePackBytes != uint64(len(baseline)) || stateChangeBlockChunkEncoding.Load() {
		t.Fatal("benchmark used or mutated CLI-default global gate")
	}
	if sample.NewKVBytes > (sample.BaselineKVBytes*9)/8 {
		t.Fatal("candidate admission used a different baseline policy")
	}
}

func TestHistorySharingBenchmarkSameHeightsReopenAndBoundary(t *testing.T) {
	b, err := NewHistorySharingBenchmark()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	rows := chunkHistoryRows(512<<10, 4)
	for i, row := range rows {
		row.BlockNum, row.Seq, row.TxNum = 1022+uint64(i), 1, 9000+uint64(i)
		raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
		sample, err := b.AddPack(context.Background(), row.BlockNum, raw)
		if err != nil || !sample.ByteExact || !sample.Shared {
			t.Fatalf("sample=%+v err=%v", sample, err)
		}
		if sample.Block != row.BlockNum || sample.Bucket != row.BlockNum/1024 {
			t.Fatal("height was rewritten")
		}
	}
	samples, total, err := b.ReopenAndVerify(context.Background())
	if err != nil || len(samples) != 4 || total == 0 {
		t.Fatalf("reopen %v %d", err, total)
	}
	if samples[2].NewChunkValueBytes < 400<<10 {
		t.Fatal("second bucket silently shared a prior bucket seed")
	}
	if samples[1].NewKVBytes >= samples[0].NewKVBytes/2 || samples[3].NewKVBytes >= samples[2].NewKVBytes/2 {
		t.Fatal("within bucket sharing did not save")
	}
	for _, sample := range samples {
		if !sample.ReopenExact {
			t.Fatal("unverified restart")
		}
	}
	if _, err := b.AddPack(context.Background(), 2000, []byte{1}); err == nil {
		t.Fatal("sealed replay accepted writes")
	}
}

func TestHistorySharingBenchmarkRejectsCanceledAndNonascending(t *testing.T) {
	b, err := NewHistorySharingBenchmark()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	row := chunkHistoryRows(128<<10, 1)[0]
	row.BlockNum, row.Seq = 4, 1
	raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
	if _, err := b.AddPack(context.Background(), 4, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AddPack(context.Background(), 4, raw); err == nil {
		t.Fatal("duplicate height accepted")
	}
	if _, _, err := b.ReopenAndVerify(context.Background()); err == nil {
		t.Fatal("failed replay passed verification")
	}
	c, err := NewHistorySharingBenchmark()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.AddPack(ctx, 4, raw); err == nil || c.encoded != 0 {
		t.Fatal("canceled replay performed work")
	}
}
