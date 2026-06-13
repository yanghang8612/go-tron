package downloader

import (
	"bytes"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// BlockFilter reports whether a candidate block ID is still eligible for the
// local fetch queue. The caller owns side effects such as path reservation.
type BlockFilter func(types.BlockID) bool

// RetryDecision tells AssignRetryCandidates what to do with one retry entry.
type RetryDecision uint8

const (
	// RetryDrop removes the entry from the retry list without assigning it.
	RetryDrop RetryDecision = iota
	// RetryKeep leaves the entry in the retry list for a later peer/session.
	RetryKeep
	// RetryAssign appends the entry to the target peer's fetch queue.
	RetryAssign
)

// RetryClassifier classifies one retry entry. Callers own any side effects
// needed before assignment, such as reserving a block path.
type RetryClassifier func(types.BlockID) RetryDecision

// RetryCandidateClassifier gathers service-owned facts for one retry-list
// candidate. It may reserve the session block path before returning facts.
type RetryCandidateClassifier func(types.BlockID) RetryCandidateFacts

// RetryCandidatePlan records the downloader decision for one retry-list entry.
type RetryCandidatePlan struct {
	ID       types.BlockID
	Facts    RetryCandidateFacts
	Decision RetryDecision
}

// RetryAssignmentPlan is the downloader-owned result for assigning retry-list
// entries into one peer's fetch queue.
type RetryAssignmentPlan struct {
	Assigned  []types.BlockID
	Keep      []types.BlockID
	Decisions []RetryCandidatePlan
	Steps     []RetryAssignmentStep
}

// FetchCandidateClassifier gathers service-owned facts for one fetch-list
// candidate. It may reserve the session block path before returning facts.
type FetchCandidateClassifier func(types.BlockID) FetchCandidateFacts

// FetchCandidatePlan records the downloader decision for one fetch-list entry.
type FetchCandidatePlan struct {
	ID       types.BlockID
	Facts    FetchCandidateFacts
	Decision FetchCandidateDecision
}

// NextFetchBatchPlan is the downloader-owned result for draining one peer's
// fetch-list into the next FETCH_INV_DATA batch.
type NextFetchBatchPlan struct {
	Batch     []types.BlockID
	Remaining []types.BlockID
	Decisions []FetchCandidatePlan
	Steps     []NextFetchBatchStep
}

// RetryAssignmentStepAction names one queue mutation after retry assignment.
type RetryAssignmentStepAction uint8

const (
	RetryAssignmentAppendAssigned RetryAssignmentStepAction = iota
	RetryAssignmentReplaceRetryList
)

// RetryAssignmentStep is one downloader-owned retry queue mutation.
type RetryAssignmentStep struct {
	Action RetryAssignmentStepAction
	IDs    []types.BlockID
}

// RetryAssignmentPlanApplier performs queue mutations named by a retry
// assignment plan.
type RetryAssignmentPlanApplier interface {
	AppendAssignedRetries(ids []types.BlockID)
	ReplaceRetryList(ids []types.BlockID)
}

// NextFetchBatchStepAction names one queue mutation after draining a fetch
// queue into the next request batch.
type NextFetchBatchStepAction uint8

const (
	NextFetchBatchReplaceFetchList NextFetchBatchStepAction = iota
)

// NextFetchBatchStep is one downloader-owned fetch queue mutation.
type NextFetchBatchStep struct {
	Action NextFetchBatchStepAction
	IDs    []types.BlockID
}

// NextFetchBatchPlanApplier performs queue mutations named by a next-fetch
// batch plan.
type NextFetchBatchPlanApplier interface {
	ReplaceFetchList(ids []types.BlockID)
}

// PopFetchBatch filters candidates in order, returns up to max eligible block
// IDs for the next FETCH_INV_DATA request, and keeps later eligible IDs in
// remaining. Ineligible IDs are dropped.
//
// remaining may reuse candidates' backing array; callers should replace their
// queue with the returned slice.
func PopFetchBatch(candidates []types.BlockID, max int, accept BlockFilter) (batch []types.BlockID, remaining []types.BlockID) {
	if len(candidates) == 0 || max <= 0 {
		return nil, candidates
	}
	batch = make([]types.BlockID, 0, max)
	remaining = candidates[:0]
	for _, bid := range candidates {
		if accept != nil && !accept(bid) {
			continue
		}
		if len(batch) < max {
			batch = append(batch, bid)
			continue
		}
		remaining = append(remaining, bid)
	}
	if len(remaining) == 0 {
		remaining = nil
	}
	return batch, remaining
}

// PlanNextFetchBatch classifies one peer-local fetch queue, emits up to max
// accepted block IDs, keeps accepted overflow in Remaining, and drops rejected
// entries. Remaining may reuse fetchList's backing array.
func PlanNextFetchBatch(fetchList []types.BlockID, max int, classify FetchCandidateClassifier) NextFetchBatchPlan {
	plan := NextFetchBatchPlan{}
	if len(fetchList) == 0 || max <= 0 {
		plan.Remaining = fetchList
		return plan
	}
	plan.Batch = make([]types.BlockID, 0, max)
	plan.Remaining = fetchList[:0]
	for _, bid := range fetchList {
		var facts FetchCandidateFacts
		if classify != nil {
			facts = classify(bid)
		}
		decision := ClassifyFetchCandidate(facts)
		plan.Decisions = append(plan.Decisions, FetchCandidatePlan{
			ID:       bid,
			Facts:    facts,
			Decision: decision,
		})
		if decision != FetchCandidateAccepted {
			continue
		}
		if len(plan.Batch) < max {
			plan.Batch = append(plan.Batch, bid)
			continue
		}
		plan.Remaining = append(plan.Remaining, bid)
	}
	if len(plan.Remaining) == 0 {
		plan.Remaining = nil
	}
	return plan.withSteps()
}

// AssignRetryCandidates partitions a retry list into entries assigned to the
// current peer and entries that should remain retryable. Dropped entries are
// intentionally omitted from both results.
//
// keep may reuse retries' backing array; callers should replace their retry
// list with the returned slice.
func AssignRetryCandidates(retries []types.BlockID, classify RetryClassifier) (assigned []types.BlockID, keep []types.BlockID) {
	if len(retries) == 0 {
		return nil, nil
	}
	keep = retries[:0]
	for _, bid := range retries {
		decision := RetryDrop
		if classify != nil {
			decision = classify(bid)
		}
		switch decision {
		case RetryAssign:
			assigned = append(assigned, bid)
		case RetryKeep:
			keep = append(keep, bid)
		default:
			// RetryDrop or unknown decisions remove the stale entry.
		}
	}
	if len(keep) == 0 {
		keep = nil
	}
	return assigned, keep
}

// PlanRetryAssignment classifies one retry queue, assigns eligible entries to
// the target peer's fetch queue, keeps entries that are still retryable for a
// later peer/session, and drops stale/conflicting entries. Keep may reuse
// retries' backing array.
func PlanRetryAssignment(retries []types.BlockID, classify RetryCandidateClassifier) RetryAssignmentPlan {
	plan := RetryAssignmentPlan{}
	if len(retries) == 0 {
		return plan
	}
	plan.Keep = retries[:0]
	for _, bid := range retries {
		var facts RetryCandidateFacts
		decision := RetryDrop
		if classify != nil {
			facts = classify(bid)
			decision = ClassifyRetryCandidate(facts)
		}
		plan.Decisions = append(plan.Decisions, RetryCandidatePlan{
			ID:       bid,
			Facts:    facts,
			Decision: decision,
		})
		switch decision {
		case RetryAssign:
			plan.Assigned = append(plan.Assigned, bid)
		case RetryKeep:
			plan.Keep = append(plan.Keep, bid)
		default:
			// RetryDrop or unknown decisions remove the stale entry.
		}
	}
	if len(plan.Keep) == 0 {
		plan.Keep = nil
	}
	return plan.withSteps()
}

func (p RetryAssignmentPlan) withSteps() RetryAssignmentPlan {
	p.Steps = []RetryAssignmentStep{
		{Action: RetryAssignmentAppendAssigned, IDs: append([]types.BlockID(nil), p.Assigned...)},
		{Action: RetryAssignmentReplaceRetryList, IDs: append([]types.BlockID(nil), p.Keep...)},
	}
	return p
}

func (p NextFetchBatchPlan) withSteps() NextFetchBatchPlan {
	p.Steps = []NextFetchBatchStep{
		{Action: NextFetchBatchReplaceFetchList, IDs: append([]types.BlockID(nil), p.Remaining...)},
	}
	return p
}

// ApplyRetryAssignmentPlan executes downloader-owned retry assignment queue
// mutations.
func ApplyRetryAssignmentPlan(plan RetryAssignmentPlan, applier RetryAssignmentPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case RetryAssignmentAppendAssigned:
			applier.AppendAssignedRetries(step.IDs)
		case RetryAssignmentReplaceRetryList:
			applier.ReplaceRetryList(step.IDs)
		}
	}
}

// ApplyNextFetchBatchPlan executes downloader-owned fetch queue mutations.
func ApplyNextFetchBatchPlan(plan NextFetchBatchPlan, applier NextFetchBatchPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case NextFetchBatchReplaceFetchList:
			applier.ReplaceFetchList(step.IDs)
		}
	}
}

// AppendDisconnectedPeerRetries appends block IDs left behind by a disconnected
// peer to the global retry list. Pending in-flight IDs are considered before
// the peer's local fetch queue, matching java-tron's retry preference for
// requests that were already sent.
func AppendDisconnectedPeerRetries(retries []types.BlockID, pending map[tcommon.Hash]types.BlockID, queued []types.BlockID, shouldRetry BlockFilter) []types.BlockID {
	pendingIDs := make([]types.BlockID, 0, len(pending))
	for _, bid := range pending {
		pendingIDs = append(pendingIDs, bid)
	}
	sort.Slice(pendingIDs, func(i, j int) bool {
		if pendingIDs[i].Num != pendingIDs[j].Num {
			return pendingIDs[i].Num < pendingIDs[j].Num
		}
		return bytes.Compare(pendingIDs[i].Hash[:], pendingIDs[j].Hash[:]) < 0
	})
	for _, bid := range pendingIDs {
		if shouldRetry == nil || shouldRetry(bid) {
			retries = append(retries, bid)
		}
	}
	for _, bid := range queued {
		if shouldRetry == nil || shouldRetry(bid) {
			retries = append(retries, bid)
		}
	}
	return retries
}
