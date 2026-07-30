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

func TestPlanNextFetchBatchClassifiesDropsAndKeepsAcceptedOverflow(t *testing.T) {
	candidates := []types.BlockID{
		queueID(1),
		queueID(2),
		queueID(3),
		queueID(4),
		queueID(5),
	}
	got := PlanNextFetchBatch(candidates, 2, func(bid types.BlockID) FetchCandidateFacts {
		switch bid.Num {
		case 1, 3, 5:
			return FetchCandidateFacts{ReservedPath: true}
		case 2:
			return FetchCandidateFacts{KnownOrRequested: true}
		case 4:
			return FetchCandidateFacts{ReservedPath: true, PeerRequested: true}
		default:
			return FetchCandidateFacts{}
		}
	})

	if want := []uint64{1, 3}; !reflect.DeepEqual(blockNums(got.Batch), want) {
		t.Fatalf("batch nums = %v, want %v", blockNums(got.Batch), want)
	}
	if want := []uint64{5}; !reflect.DeepEqual(blockNums(got.Remaining), want) {
		t.Fatalf("remaining nums = %v, want %v", blockNums(got.Remaining), want)
	}
	if len(got.Steps) != 1 || got.Steps[0].Action != NextFetchBatchReplaceFetchList || !reflect.DeepEqual(blockNums(got.Steps[0].IDs), []uint64{5}) {
		t.Fatalf("steps = %+v, want replace fetch list with block 5", got.Steps)
	}
	wantDecisions := []FetchCandidateDecision{
		FetchCandidateAccepted,
		FetchCandidateKnownOrRequested,
		FetchCandidateAccepted,
		FetchCandidatePeerDuplicate,
		FetchCandidateAccepted,
	}
	if len(got.Decisions) != len(wantDecisions) {
		t.Fatalf("decisions = %+v, want %d", got.Decisions, len(wantDecisions))
	}
	for i, want := range wantDecisions {
		if got.Decisions[i].ID.Num != uint64(i+1) || got.Decisions[i].Decision != want {
			t.Fatalf("decision %d = %+v, want block %d decision %v", i, got.Decisions[i], i+1, want)
		}
	}
}

func TestPlanNextFetchBatchKeepsDeferredCandidates(t *testing.T) {
	candidates := []types.BlockID{queueID(1), queueID(2), queueID(3)}
	got := PlanNextFetchBatch(candidates, 2, func(bid types.BlockID) FetchCandidateFacts {
		if bid.Num == 2 {
			return FetchCandidateFacts{Deferred: true}
		}
		return FetchCandidateFacts{ReservedPath: true}
	})

	if want := []uint64{1, 3}; !reflect.DeepEqual(blockNums(got.Batch), want) {
		t.Fatalf("batch nums = %v, want %v", blockNums(got.Batch), want)
	}
	if want := []uint64{2}; !reflect.DeepEqual(blockNums(got.Remaining), want) {
		t.Fatalf("remaining nums = %v, want %v", blockNums(got.Remaining), want)
	}
}

func TestPlanNextFetchBatchKeepsCandidatesWhenMaxInvalid(t *testing.T) {
	candidates := []types.BlockID{queueID(1)}
	got := PlanNextFetchBatch(candidates, 0, func(types.BlockID) FetchCandidateFacts {
		t.Fatal("classifier should not be called when max <= 0")
		return FetchCandidateFacts{ReservedPath: true}
	})

	if got.Batch != nil || got.Decisions != nil {
		t.Fatalf("plan = %+v, want no batch or decisions", got)
	}
	if want := []uint64{1}; !reflect.DeepEqual(blockNums(got.Remaining), want) {
		t.Fatalf("remaining nums = %v, want %v", blockNums(got.Remaining), want)
	}
}

func TestAssignRetryCandidatesPartitionsByDecision(t *testing.T) {
	retries := []types.BlockID{
		queueID(1),
		queueID(2),
		queueID(3),
		queueID(4),
		queueID(5),
	}
	var seen []uint64
	assigned, keep := AssignRetryCandidates(retries, func(bid types.BlockID) RetryDecision {
		seen = append(seen, bid.Num)
		switch bid.Num {
		case 1, 4:
			return RetryAssign
		case 2, 5:
			return RetryKeep
		default:
			return RetryDrop
		}
	})

	if want := []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("classifier saw nums %v, want %v", seen, want)
	}
	if got, want := blockNums(assigned), []uint64{1, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("assigned nums = %v, want %v", got, want)
	}
	if got, want := blockNums(keep), []uint64{2, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keep nums = %v, want %v", got, want)
	}
}

func TestAssignRetryCandidatesDropsByDefault(t *testing.T) {
	assigned, keep := AssignRetryCandidates([]types.BlockID{queueID(1)}, nil)
	if assigned != nil {
		t.Fatalf("assigned = %v, want nil", blockNums(assigned))
	}
	if keep != nil {
		t.Fatalf("keep = %v, want nil", blockNums(keep))
	}
}

func TestPlanRetryAssignmentRecordsDecisions(t *testing.T) {
	retries := []types.BlockID{
		queueID(1),
		queueID(2),
		queueID(3),
		queueID(4),
		queueID(5),
	}
	got := PlanRetryAssignment(retries, func(bid types.BlockID) RetryCandidateFacts {
		switch bid.Num {
		case 1:
			return RetryCandidateFacts{KnownOrRequested: true}
		case 2:
			return RetryCandidateFacts{InWindow: false}
		case 3:
			return RetryCandidateFacts{InWindow: true, PeerRequested: true}
		case 4:
			return RetryCandidateFacts{InWindow: true, ReservedPath: true}
		default:
			return RetryCandidateFacts{InWindow: true}
		}
	})

	if want := []uint64{4}; !reflect.DeepEqual(blockNums(got.Assigned), want) {
		t.Fatalf("assigned nums = %v, want %v", blockNums(got.Assigned), want)
	}
	if want := []uint64{2, 3}; !reflect.DeepEqual(blockNums(got.Keep), want) {
		t.Fatalf("keep nums = %v, want %v", blockNums(got.Keep), want)
	}
	if len(got.Steps) != 2 ||
		got.Steps[0].Action != RetryAssignmentAppendAssigned ||
		!reflect.DeepEqual(blockNums(got.Steps[0].IDs), []uint64{4}) ||
		got.Steps[1].Action != RetryAssignmentReplaceRetryList ||
		!reflect.DeepEqual(blockNums(got.Steps[1].IDs), []uint64{2, 3}) {
		t.Fatalf("steps = %+v, want append assigned 4 and keep 2/3", got.Steps)
	}
	wantDecisions := []RetryDecision{
		RetryDrop,
		RetryKeep,
		RetryKeep,
		RetryAssign,
		RetryDrop,
	}
	if len(got.Decisions) != len(wantDecisions) {
		t.Fatalf("decisions = %+v, want %d", got.Decisions, len(wantDecisions))
	}
	for i, want := range wantDecisions {
		if got.Decisions[i].ID.Num != uint64(i+1) || got.Decisions[i].Decision != want {
			t.Fatalf("decision %d = %+v, want block %d decision %v", i, got.Decisions[i], i+1, want)
		}
	}
}

func TestPlanRetryAssignmentDropsByDefault(t *testing.T) {
	got := PlanRetryAssignment([]types.BlockID{queueID(1)}, nil)
	if got.Assigned != nil || got.Keep != nil {
		t.Fatalf("plan = %+v, want no assigned or kept entries", got)
	}
	if len(got.Decisions) != 1 || got.Decisions[0].Decision != RetryDrop {
		t.Fatalf("decisions = %+v, want one drop", got.Decisions)
	}
}

func TestApplyQueuePlans(t *testing.T) {
	retryApplier := new(recordingRetryAssignmentApplier)
	retryPlan := RetryAssignmentPlan{Steps: []RetryAssignmentStep{
		{Action: RetryAssignmentAppendAssigned, IDs: []types.BlockID{queueID(4)}},
		{Action: RetryAssignmentStepAction(255), IDs: []types.BlockID{queueID(99)}},
		{Action: RetryAssignmentReplaceRetryList, IDs: []types.BlockID{queueID(2), queueID(3)}},
	}}
	retryResult := ApplyRetryAssignmentPlan(retryPlan, retryApplier)
	if !reflect.DeepEqual(blockNums(retryApplier.assigned), []uint64{4}) || !reflect.DeepEqual(blockNums(retryApplier.keep), []uint64{2, 3}) {
		t.Fatalf("retry apply assigned/keep = %v/%v, want [4]/[2 3]", blockNums(retryApplier.assigned), blockNums(retryApplier.keep))
	}
	if !reflect.DeepEqual(retryResult.AppliedSteps, []RetryAssignmentStepAction{RetryAssignmentAppendAssigned, RetryAssignmentReplaceRetryList}) ||
		!reflect.DeepEqual(retryResult.UnknownSteps, []RetryAssignmentStepAction{RetryAssignmentStepAction(255)}) {
		t.Fatalf("retry apply result = %+v, want append/replace applied and unknown [255]", retryResult)
	}

	retryApplier.assigned = nil
	retryApplier.keep = nil
	retryResult = ApplyRetryAssignmentPlan(RetryAssignmentPlan{
		Assigned: []types.BlockID{queueID(7)},
		Keep:     []types.BlockID{queueID(8)},
	}, retryApplier)
	if !reflect.DeepEqual(blockNums(retryApplier.assigned), []uint64{7}) || !reflect.DeepEqual(blockNums(retryApplier.keep), []uint64{8}) {
		t.Fatalf("retry fallback assigned/keep = %v/%v, want [7]/[8]", blockNums(retryApplier.assigned), blockNums(retryApplier.keep))
	}
	if !reflect.DeepEqual(retryResult.AppliedSteps, []RetryAssignmentStepAction{RetryAssignmentAppendAssigned, RetryAssignmentReplaceRetryList}) ||
		len(retryResult.UnknownSteps) != 0 {
		t.Fatalf("retry fallback result = %+v, want append/replace applied", retryResult)
	}
	if nilResult := ApplyRetryAssignmentPlan(retryPlan, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil retry result = %+v, want empty", nilResult)
	}

	fetchApplier := new(recordingNextFetchBatchApplier)
	fetchPlan := NextFetchBatchPlan{Steps: []NextFetchBatchStep{
		{Action: NextFetchBatchStepAction(255), IDs: []types.BlockID{queueID(99)}},
		{Action: NextFetchBatchReplaceFetchList, IDs: []types.BlockID{queueID(5)}},
	}}
	fetchResult := ApplyNextFetchBatchPlan(fetchPlan, fetchApplier)
	if !reflect.DeepEqual(blockNums(fetchApplier.remaining), []uint64{5}) {
		t.Fatalf("fetch apply remaining = %v, want [5]", blockNums(fetchApplier.remaining))
	}
	if !reflect.DeepEqual(fetchResult.AppliedSteps, []NextFetchBatchStepAction{NextFetchBatchReplaceFetchList}) ||
		!reflect.DeepEqual(fetchResult.UnknownSteps, []NextFetchBatchStepAction{NextFetchBatchStepAction(255)}) {
		t.Fatalf("fetch apply result = %+v, want replace applied and unknown [255]", fetchResult)
	}

	fetchResult = ApplyNextFetchBatchPlan(NextFetchBatchPlan{Remaining: []types.BlockID{queueID(6)}}, fetchApplier)
	if !reflect.DeepEqual(blockNums(fetchApplier.remaining), []uint64{6}) {
		t.Fatalf("fetch fallback remaining = %v, want [6]", blockNums(fetchApplier.remaining))
	}
	if !reflect.DeepEqual(fetchResult.AppliedSteps, []NextFetchBatchStepAction{NextFetchBatchReplaceFetchList}) ||
		len(fetchResult.UnknownSteps) != 0 {
		t.Fatalf("fetch fallback result = %+v, want replace applied", fetchResult)
	}
	if nilResult := ApplyNextFetchBatchPlan(fetchPlan, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil fetch result = %+v, want empty", nilResult)
	}
}

func TestAppendDisconnectedPeerRetriesFiltersPendingBeforeFetchQueue(t *testing.T) {
	existing := []types.BlockID{queueID(9)}
	pending := map[tcommon.Hash]types.BlockID{
		queueID(2).Hash: queueID(2),
		queueID(1).Hash: queueID(1),
	}
	queued := []types.BlockID{queueID(3), queueID(4), queueID(5)}

	got := AppendDisconnectedPeerRetries(existing, pending, queued, func(bid types.BlockID) bool {
		return bid.Num != 2 && bid.Num != 4
	})

	if len(got) != 4 {
		t.Fatalf("retry nums = %v, want 4 entries", blockNums(got))
	}
	if got[0].Num != 9 {
		t.Fatalf("first retry = %d, want existing entry 9", got[0].Num)
	}
	if got[1].Num != 1 {
		t.Fatalf("pending retry = %d, want 1", got[1].Num)
	}
	if tail := blockNums(got[2:]); !reflect.DeepEqual(tail, []uint64{3, 5}) {
		t.Fatalf("fetch queue retry tail = %v, want [3 5]", tail)
	}
}

func TestAppendDisconnectedPeerRetriesSortsPendingByNumberAndHash(t *testing.T) {
	id1b := types.BlockID{Hash: tcommon.Hash{0x03}, Num: 1}
	id2 := queueID(2)
	id1a := types.BlockID{Hash: tcommon.Hash{0x01}, Num: 1}
	pending := map[tcommon.Hash]types.BlockID{
		id1b.Hash: id1b,
		id2.Hash:  id2,
		id1a.Hash: id1a,
	}

	got := AppendDisconnectedPeerRetries(nil, pending, []types.BlockID{queueID(3)}, nil)
	want := []types.BlockID{id1a, id1b, id2, queueID(3)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retry ids = %v, want %v", blockNums(got), blockNums(want))
	}
}

func blockNums(ids []types.BlockID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = id.Num
	}
	return out
}

type recordingRetryAssignmentApplier struct {
	assigned []types.BlockID
	keep     []types.BlockID
}

func (a *recordingRetryAssignmentApplier) AppendAssignedRetries(ids []types.BlockID) {
	a.assigned = append(a.assigned, ids...)
}

func (a *recordingRetryAssignmentApplier) ReplaceRetryList(ids []types.BlockID) {
	a.keep = append([]types.BlockID(nil), ids...)
}

type recordingNextFetchBatchApplier struct {
	remaining []types.BlockID
}

func (a *recordingNextFetchBatchApplier) ReplaceFetchList(ids []types.BlockID) {
	a.remaining = append([]types.BlockID(nil), ids...)
}
