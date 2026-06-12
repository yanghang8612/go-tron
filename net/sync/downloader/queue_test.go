package downloader

import (
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func queueID(num uint64) types.BlockID {
	return types.BlockID{Hash: tcommon.Hash{byte(num)}, Num: num}
}

func TestPopFetchBatchFiltersPreservesOrderAndKeepsOverflow(t *testing.T) {
	candidates := []types.BlockID{
		queueID(1),
		queueID(2),
		queueID(3),
		queueID(4),
		queueID(5),
	}
	var seen []uint64
	batch, remaining := PopFetchBatch(candidates, 2, func(bid types.BlockID) bool {
		seen = append(seen, bid.Num)
		return bid.Num != 2 && bid.Num != 4
	})

	if want := []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("filter saw nums %v, want %v", seen, want)
	}
	if got, want := blockNums(batch), []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch nums = %v, want %v", got, want)
	}
	if got, want := blockNums(remaining), []uint64{5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("remaining nums = %v, want %v", got, want)
	}
}

func TestPopFetchBatchDropsIneligibleWithoutReturningEmptyTail(t *testing.T) {
	candidates := []types.BlockID{queueID(1), queueID(2), queueID(3)}
	batch, remaining := PopFetchBatch(candidates, 10, func(bid types.BlockID) bool {
		return bid.Num == 2
	})

	if got, want := blockNums(batch), []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch nums = %v, want %v", got, want)
	}
	if remaining != nil {
		t.Fatalf("remaining = %v, want nil", blockNums(remaining))
	}
}

func TestPopFetchBatchKeepsCandidatesWhenMaxInvalid(t *testing.T) {
	candidates := []types.BlockID{queueID(1)}
	batch, remaining := PopFetchBatch(candidates, 0, func(types.BlockID) bool {
		t.Fatal("filter should not be called when max <= 0")
		return true
	})

	if batch != nil {
		t.Fatalf("batch = %v, want nil", blockNums(batch))
	}
	if got, want := blockNums(remaining), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("remaining nums = %v, want %v", got, want)
	}
}

func blockNums(ids []types.BlockID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = id.Num
	}
	return out
}
