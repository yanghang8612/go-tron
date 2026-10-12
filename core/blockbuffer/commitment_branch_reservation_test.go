package blockbuffer

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Physical-key skew deliberately violates the writer's uniform reservation
// estimate. Concurrent map growth must preserve values, keys, and borrowed
// references across durable retirement and subsequent layer writes.
func TestCommitmentBranchReservationSkewedShardGrowth(t *testing.T) {
	const batches, rows = 64, 8
	disk := rawdb.NewMemoryDatabase()
	buffer := New(disk)
	buffer.BeginBlock(bufHash(1), 1)
	handle, _ := buffer.NewestInflight()
	view := buffer.ViewLayer(handle)
	first := []byte(rawdb.CommitmentBranchKeyPrefix)
	seconds := make([][]string, batches)
	values := make([][][]byte, batches)
	candidate := 0
	for batch := range batches {
		for len(seconds[batch]) < rows {
			key := fmt.Sprintf("skew-%d", candidate)
			candidate++
			if layerShardIndexBytes(append(append([]byte(nil), first...), key...)) != 0 {
				continue
			}
			seconds[batch] = append(seconds[batch], key)
			values[batch] = append(values[batch], []byte(fmt.Sprintf("value-%d-%d", batch, len(seconds[batch]))))
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, batches)
	for batch := range batches {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			errs[batch] = view.PutKeyPartsStringsOwnedValuesWithBatchCount(first, seconds[batch], values[batch], batches)
		}(batch)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	borrowed := make([][]byte, 0, batches*rows)
	for batch := range batches {
		for i, key := range seconds[batch] {
			got, err := view.GetNoCopy(append(append([]byte(nil), first...), key...))
			if err != nil || !bytes.Equal(got, values[batch][i]) {
				t.Fatalf("branch %d/%d: value=%q err=%v", batch, i, got, err)
			}
			borrowed = append(borrowed, got)
		}
	}
	if err := buffer.CommitInflight(handle); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(disk); err != nil {
		t.Fatal(err)
	}
	buffer.BeginBlock(bufHash(2), 2)
	next, _ := buffer.NewestInflight()
	nextView := buffer.ViewLayer(next)
	for batch := range batches {
		newer := make([][]byte, rows)
		for i := range newer {
			newer[i] = []byte("replacement")
		}
		if err := nextView.PutKeyPartsStringsOwnedValuesWithBatchCount(first, seconds[batch], newer, batches); err != nil {
			t.Fatal(err)
		}
	}
	buffer.DiscardActive()
	for batch := range batches {
		for i, key := range seconds[batch] {
			want := values[batch][i]
			if !bytes.Equal(borrowed[batch*rows+i], want) {
				t.Fatal("borrowed branch changed after retirement")
			}
			got, err := disk.Get(append(append([]byte(nil), first...), key...))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("durable branch %d/%d: value=%q err=%v", batch, i, got, err)
			}
		}
	}
}
