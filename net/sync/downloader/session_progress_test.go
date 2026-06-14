package downloader

import (
	"reflect"
	"testing"
)

func TestSessionProgressEstimatedRemainingUsesTargetHeadWhenAhead(t *testing.T) {
	progress := SessionProgress{
		CurrentHead:    10,
		TargetHead:     15,
		RetryListLen:   3,
		BlockBufferLen: 4,
		Peers: []PeerProgress{{
			FetchListLen: 2,
			Inflight:     1,
			RemainNum:    20,
		}},
	}

	if got := progress.EstimatedRemaining(); got != 5 {
		t.Fatalf("EstimatedRemaining = %d, want target-head diff 5", got)
	}
}

func TestSessionProgressEstimatedRemainingFallsBackToQueues(t *testing.T) {
	progress := SessionProgress{
		CurrentHead:    15,
		TargetHead:     10,
		RetryListLen:   3,
		BlockBufferLen: 4,
		Peers: []PeerProgress{
			{FetchListLen: 2, Inflight: 1, RemainNum: 20},
			{FetchListLen: 5, Inflight: 6, RemainNum: -9},
		},
	}

	if got := progress.EstimatedRemaining(); got != 41 {
		t.Fatalf("EstimatedRemaining = %d, want 41", got)
	}
}

func TestPlanIdleDrainAfterRefill(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	finish := IdleDrainPlan{
		Finish: true,
		Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{Progress: complete, JoinAvailablePeersAllowed: true}); !reflect.DeepEqual(got, finish) {
		t.Fatalf("complete idle plan = %+v, want %+v", got, finish)
	}
	join := IdleDrainPlan{
		JoinAvailablePeers: true,
		Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{JoinAvailablePeersAllowed: true}); !reflect.DeepEqual(got, join) {
		t.Fatalf("joinable idle plan = %+v, want %+v", got, join)
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{}); !reflect.DeepEqual(got, IdleDrainPlan{}) {
		t.Fatalf("incomplete idle plan = %+v, want no action", got)
	}
}

func TestPlanFetchRefillDispatch(t *testing.T) {
	tests := map[string]struct {
		input FetchRefillDispatchInput
		want  FetchRefillDispatchPlan
	}{
		"active outbound": {
			input: FetchRefillDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true}},
			want: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
		"no outbound": {
			input: FetchRefillDispatchInput{Progress: SessionProgress{Syncing: true}},
			want:  FetchRefillDispatchPlan{},
		},
		"not syncing": {
			input: FetchRefillDispatchInput{OutboundRequests: 1},
			want:  FetchRefillDispatchPlan{},
		},
		"paused": {
			input: FetchRefillDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true, Paused: true}},
			want:  FetchRefillDispatchPlan{},
		},
	}
	for name, test := range tests {
		if got := PlanFetchRefillDispatch(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s plan = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestPlanFetchRefillRun(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	got := PlanFetchRefillRun(FetchRefillRunInput{
		OutboundRequests: 2,
		Progress:         active,
	})
	want := FetchRefillRunPlan{
		MirrorLegacy: true,
		LockedSteps:  []FetchRefillRunStep{{Action: FetchRefillRunMirrorLegacy}},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active refill run = %+v, want %+v", got, want)
	}

	got = PlanFetchRefillRun(FetchRefillRunInput{
		OutboundRequests: 2,
		Progress:         SessionProgress{Syncing: true, Paused: true},
	})
	want = FetchRefillRunPlan{
		MirrorLegacy: true,
		LockedSteps:  []FetchRefillRunStep{{Action: FetchRefillRunMirrorLegacy}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paused refill run = %+v, want mirror only", got)
	}
}

func TestPlanLocalDrainIteration(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	tests := map[string]struct {
		input LocalDrainIterationInput
		want  LocalDrainIterationPlan
	}{
		"not syncing": {
			input: LocalDrainIterationInput{},
			want: LocalDrainIterationPlan{
				StopLoop: true,
				Steps:    []LocalDrainIterationStep{{Action: LocalDrainIterationStop}},
			},
		},
		"paused": {
			input: LocalDrainIterationInput{Progress: SessionProgress{Syncing: true, Paused: true}},
			want: LocalDrainIterationPlan{
				StopLoop: true,
				Steps:    []LocalDrainIterationStep{{Action: LocalDrainIterationStop}},
			},
		},
		"empty drain": {
			input: LocalDrainIterationInput{Progress: active},
			want: LocalDrainIterationPlan{
				EmptyDrain: true,
				Steps:      []LocalDrainIterationStep{{Action: LocalDrainIterationEmpty}},
			},
		},
		"import batch": {
			input: LocalDrainIterationInput{Progress: active, BufferedBatchLen: 2},
			want: LocalDrainIterationPlan{
				ImportBatch: true,
				Steps:       []LocalDrainIterationStep{{Action: LocalDrainIterationImport}},
			},
		},
	}
	for name, test := range tests {
		if got := PlanLocalDrainIteration(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s local drain iteration = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestPlanLocalDrainRunUsesStagedBodyDrainResult(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	importBatch := BufferedBatch{Buffered: []BufferedBlock{{Num: 6}}}

	importRun := PlanLocalDrainRun(LocalDrainRunInput{
		Progress: active,
		Drain:    StagedBodyDrainRunResult{Batch: importBatch},
	})
	if !reflect.DeepEqual(importRun.Batch, importBatch) ||
		!reflect.DeepEqual(importRun.Drain.Batch, importBatch) ||
		!importRun.Iteration.ImportBatch ||
		len(importRun.Iteration.Steps) != 1 ||
		importRun.Iteration.Steps[0].Action != LocalDrainIterationImport {
		t.Fatalf("import drain run = %+v, want import branch with original staged drain batch", importRun)
	}

	emptyRun := PlanLocalDrainRun(LocalDrainRunInput{Progress: active})
	if !emptyRun.Iteration.EmptyDrain ||
		len(emptyRun.Iteration.Steps) != 1 ||
		emptyRun.Iteration.Steps[0].Action != LocalDrainIterationEmpty {
		t.Fatalf("empty drain run = %+v, want empty branch", emptyRun)
	}

	stoppedRun := PlanLocalDrainRun(LocalDrainRunInput{
		Progress: SessionProgress{Syncing: true, Paused: true},
		Drain:    StagedBodyDrainRunResult{Batch: importBatch},
	})
	if !stoppedRun.Iteration.StopLoop ||
		len(stoppedRun.Iteration.Steps) != 1 ||
		stoppedRun.Iteration.Steps[0].Action != LocalDrainIterationStop {
		t.Fatalf("stopped drain run = %+v, want stop branch even with buffered batch", stoppedRun)
	}
}

func TestPlanEmptyDrainJoinProbe(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: complete}); got.CheckJoinAvailablePeers {
		t.Fatalf("complete join probe = %+v, want no peer-join check", got)
	}
	incomplete := SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 2}
	if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: incomplete}); !got.CheckJoinAvailablePeers {
		t.Fatalf("incomplete join probe = %+v, want peer-join check", got)
	}
	for name, progress := range map[string]SessionProgress{
		"not syncing": {},
		"paused":      {Syncing: true, Paused: true, CurrentHead: 1, TargetHead: 2},
	} {
		if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: progress}); got.CheckJoinAvailablePeers {
			t.Fatalf("%s join probe = %+v, want no peer-join check", name, got)
		}
	}
}

func TestPlanEmptyDrainRefill(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	got := PlanEmptyDrainRefill(EmptyDrainRefillInput{
		OutboundRequests:          2,
		Progress:                  complete,
		JoinAvailablePeersAllowed: true,
	})
	want := EmptyDrainRefillPlan{
		Idle: IdleDrainPlan{
			Finish: true,
			Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete empty-drain refill plan = %+v, want %+v", got, want)
	}

	got = PlanEmptyDrainRefill(EmptyDrainRefillInput{
		Progress:                  SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 2},
		JoinAvailablePeersAllowed: true,
	})
	want = EmptyDrainRefillPlan{
		Idle: IdleDrainPlan{
			JoinAvailablePeers: true,
			Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joinable empty-drain refill plan = %+v, want %+v", got, want)
	}

	got = PlanEmptyDrainRefill(EmptyDrainRefillInput{
		OutboundRequests: 3,
		Progress:         SessionProgress{Syncing: true, Paused: true},
	})
	if !reflect.DeepEqual(got, EmptyDrainRefillPlan{}) {
		t.Fatalf("paused empty-drain refill plan = %+v, want no action", got)
	}
}

func TestPlanEmptyDrainRun(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	gate := &recordingEmptyDrainJoinGate{allowed: true}
	got := PlanEmptyDrainRun(EmptyDrainRunInput{
		OutboundRequests: 2,
		Progress:         complete,
	}, gate)
	want := EmptyDrainRunPlan{
		JoinProbe:    EmptyDrainJoinProbePlan{},
		MirrorLegacy: true,
		LockedSteps:  []EmptyDrainRunStep{{Action: EmptyDrainMirrorLegacy}},
		Refill: EmptyDrainRefillPlan{
			Idle: IdleDrainPlan{
				Finish: true,
				Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
			},
			Dispatch: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete empty-drain run = %+v, want %+v", got, want)
	}
	if gate.calls != 0 {
		t.Fatalf("complete empty-drain called join gate %d times, want 0", gate.calls)
	}

	incomplete := SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 3}
	gate = &recordingEmptyDrainJoinGate{allowed: true}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{Progress: incomplete}, gate)
	want = EmptyDrainRunPlan{
		JoinProbe:                 EmptyDrainJoinProbePlan{CheckJoinAvailablePeers: true},
		JoinAvailablePeersAllowed: true,
		MirrorLegacy:              true,
		LockedSteps:               []EmptyDrainRunStep{{Action: EmptyDrainMirrorLegacy}},
		Refill: EmptyDrainRefillPlan{
			Idle: IdleDrainPlan{
				JoinAvailablePeers: true,
				Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joinable empty-drain run = %+v, want %+v", got, want)
	}
	if gate.calls != 1 || !reflect.DeepEqual(gate.progress, incomplete) {
		t.Fatalf("join gate calls/progress = %d/%+v, want one call with incomplete progress", gate.calls, gate.progress)
	}

	gate = &recordingEmptyDrainJoinGate{allowed: false}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{
		OutboundRequests: 1,
		Progress:         incomplete,
	}, gate)
	want = EmptyDrainRunPlan{
		JoinProbe:    EmptyDrainJoinProbePlan{CheckJoinAvailablePeers: true},
		MirrorLegacy: true,
		LockedSteps:  []EmptyDrainRunStep{{Action: EmptyDrainMirrorLegacy}},
		Refill: EmptyDrainRefillPlan{
			Dispatch: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-joinable empty-drain run = %+v, want %+v", got, want)
	}

	gate = &recordingEmptyDrainJoinGate{allowed: true}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{Progress: SessionProgress{Syncing: true, Paused: true}}, gate)
	if !reflect.DeepEqual(got, EmptyDrainRunPlan{}) {
		t.Fatalf("paused empty-drain run = %+v, want no action", got)
	}
	if gate.calls != 0 {
		t.Fatalf("paused empty-drain called join gate %d times, want 0", gate.calls)
	}
}

func TestPlanEmptyDrainPreparation(t *testing.T) {
	got := PlanEmptyDrainPreparation(EmptyDrainPreparationInput{
		Progress: SessionProgress{Syncing: true, CurrentHead: 8, TargetHead: 12},
	})
	want := EmptyDrainPreparationPlan{
		BeginBufferWait:  true,
		BufferWaitNext:   9,
		RefillFetchSlots: true,
		Steps: []EmptyDrainPreparationStep{
			{Action: EmptyDrainPrepareBeginBufferWait, Next: 9},
			{Action: EmptyDrainPrepareRefillFetchSlots},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active empty-drain preparation = %+v, want %+v", got, want)
	}

	for name, progress := range map[string]SessionProgress{
		"not syncing": {},
		"paused":      {Syncing: true, Paused: true, CurrentHead: 8},
	} {
		if got := PlanEmptyDrainPreparation(EmptyDrainPreparationInput{Progress: progress}); !reflect.DeepEqual(got, EmptyDrainPreparationPlan{}) {
			t.Fatalf("%s empty-drain preparation = %+v, want no action", name, got)
		}
	}
}

func TestApplyEmptyDrainPreparationPlan(t *testing.T) {
	applier := &recordingEmptyDrainPreparationApplier{outbound: 2}
	result := ApplyEmptyDrainPreparationPlan(EmptyDrainPreparationPlan{Steps: []EmptyDrainPreparationStep{
		{Action: EmptyDrainPrepareBeginBufferWait, Next: 10},
		{Action: EmptyDrainPreparationStepAction(255)},
		{Action: EmptyDrainPrepareRefillFetchSlots},
	}}, applier)

	if !reflect.DeepEqual(applier.begins, []uint64{10}) {
		t.Fatalf("buffer wait begins = %+v, want [10]", applier.begins)
	}
	if applier.refills != 1 {
		t.Fatalf("refills = %d, want 1", applier.refills)
	}
	if result.OutboundRequests != 2 ||
		!reflect.DeepEqual(result.AppliedSteps, []EmptyDrainPreparationStepAction{EmptyDrainPrepareBeginBufferWait, EmptyDrainPrepareRefillFetchSlots}) ||
		!reflect.DeepEqual(result.UnknownSteps, []EmptyDrainPreparationStepAction{EmptyDrainPreparationStepAction(255)}) {
		t.Fatalf("empty-drain preparation result = %+v, want begin/refill applied with unknown [255]", result)
	}

	applier = &recordingEmptyDrainPreparationApplier{outbound: 3}
	result = ApplyEmptyDrainPreparationPlan(EmptyDrainPreparationPlan{
		BeginBufferWait:  true,
		BufferWaitNext:   11,
		RefillFetchSlots: true,
	}, applier)
	if !reflect.DeepEqual(applier.begins, []uint64{11}) ||
		applier.refills != 1 ||
		result.OutboundRequests != 3 ||
		!reflect.DeepEqual(result.AppliedSteps, []EmptyDrainPreparationStepAction{EmptyDrainPrepareBeginBufferWait, EmptyDrainPrepareRefillFetchSlots}) ||
		len(result.UnknownSteps) != 0 {
		t.Fatalf("fallback empty-drain preparation result = %+v begins=%+v refills=%d, want begin/refill", result, applier.begins, applier.refills)
	}

	nilResult := ApplyEmptyDrainPreparationPlan(EmptyDrainPreparationPlan{RefillFetchSlots: true}, nil)
	if nilResult.OutboundRequests != 0 || len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil empty-drain preparation result = %+v, want empty", nilResult)
	}
}

func TestApplyEmptyDrainRunLockedPlan(t *testing.T) {
	applier := new(recordingEmptyDrainRunApplier)
	result := ApplyEmptyDrainRunLockedPlan(EmptyDrainRunPlan{LockedSteps: []EmptyDrainRunStep{
		{Action: EmptyDrainMirrorLegacy},
		{Action: EmptyDrainRunStepAction(255)},
	}}, applier)
	if applier.mirrors != 1 {
		t.Fatalf("mirror calls = %d, want 1", applier.mirrors)
	}
	if !reflect.DeepEqual(result.Locked.AppliedSteps, []EmptyDrainRunStepAction{EmptyDrainMirrorLegacy}) ||
		!reflect.DeepEqual(result.Locked.UnknownSteps, []EmptyDrainRunStepAction{EmptyDrainRunStepAction(255)}) ||
		len(result.Idle.AppliedSteps) != 0 ||
		len(result.Dispatch.AppliedSteps) != 0 {
		t.Fatalf("empty drain locked apply result = %+v, want mirror applied and unknown [255]", result)
	}

	applier.mirrors = 0
	result = ApplyEmptyDrainRunLockedPlan(EmptyDrainRunPlan{MirrorLegacy: true}, applier)
	if applier.mirrors != 1 {
		t.Fatalf("fallback mirror calls = %d, want 1", applier.mirrors)
	}
	if !reflect.DeepEqual(result.Locked.AppliedSteps, []EmptyDrainRunStepAction{EmptyDrainMirrorLegacy}) || len(result.Locked.UnknownSteps) != 0 {
		t.Fatalf("fallback empty drain locked apply result = %+v, want mirror applied", result)
	}
	if nilResult := ApplyEmptyDrainRunLockedPlan(EmptyDrainRunPlan{MirrorLegacy: true}, nil); len(nilResult.Locked.AppliedSteps) != 0 || len(nilResult.Locked.UnknownSteps) != 0 {
		t.Fatalf("nil empty drain locked apply result = %+v, want empty", nilResult)
	}
}

func TestApplyEmptyDrainRunPlan(t *testing.T) {
	runApplier := new(recordingEmptyDrainRunApplier)
	idleApplier := new(recordingIdleDrainApplier)
	dispatchApplier := new(recordingFetchRefillDispatchApplier)
	plan := EmptyDrainRunPlan{
		LockedSteps: []EmptyDrainRunStep{
			{Action: EmptyDrainMirrorLegacy},
			{Action: EmptyDrainRunStepAction(255)},
		},
		Refill: EmptyDrainRefillPlan{
			Idle: IdleDrainPlan{Steps: []IdleDrainStep{
				{Action: IdleDrainJoinAvailablePeers},
				{Action: IdleDrainStepAction(254)},
			}},
			Dispatch: FetchRefillDispatchPlan{Steps: []FetchRefillDispatchStep{
				{Action: FetchRefillDispatchSendOutbound},
				{Action: FetchRefillDispatchStepAction(253)},
			}},
		},
	}

	locked := ApplyEmptyDrainRunLockedPlan(plan, runApplier)
	postLock := ApplyEmptyDrainRunPostLockPlan(plan, idleApplier)
	dispatch := ApplyEmptyDrainRunDispatchPlan(plan, dispatchApplier)
	if runApplier.mirrors != 1 {
		t.Fatalf("empty drain run mirrors = %d, want 1", runApplier.mirrors)
	}
	if !reflect.DeepEqual(idleApplier.calls, []IdleDrainStepAction{IdleDrainJoinAvailablePeers}) {
		t.Fatalf("empty drain run idle calls = %+v, want join", idleApplier.calls)
	}
	if dispatchApplier.sent != 1 {
		t.Fatalf("empty drain run dispatch sends = %d, want 1", dispatchApplier.sent)
	}
	if !reflect.DeepEqual(locked.Locked.AppliedSteps, []EmptyDrainRunStepAction{EmptyDrainMirrorLegacy}) ||
		!reflect.DeepEqual(locked.Locked.UnknownSteps, []EmptyDrainRunStepAction{EmptyDrainRunStepAction(255)}) ||
		len(locked.Idle.AppliedSteps) != 0 ||
		len(locked.Dispatch.AppliedSteps) != 0 {
		t.Fatalf("locked empty drain run result = %+v, want mirror applied and unknown [255]", locked)
	}
	if !reflect.DeepEqual(postLock.Idle.AppliedSteps, []IdleDrainStepAction{IdleDrainJoinAvailablePeers}) ||
		!reflect.DeepEqual(postLock.Idle.UnknownSteps, []IdleDrainStepAction{IdleDrainStepAction(254)}) ||
		len(postLock.Locked.AppliedSteps) != 0 ||
		len(postLock.Dispatch.AppliedSteps) != 0 {
		t.Fatalf("post-lock empty drain run result = %+v, want idle join and unknown [254]", postLock)
	}
	if !reflect.DeepEqual(dispatch.Dispatch.AppliedSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchSendOutbound}) ||
		!reflect.DeepEqual(dispatch.Dispatch.UnknownSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchStepAction(253)}) ||
		len(dispatch.Locked.AppliedSteps) != 0 ||
		len(dispatch.Idle.AppliedSteps) != 0 {
		t.Fatalf("dispatch empty drain run result = %+v, want send and unknown [253]", dispatch)
	}

	if nilLocked := ApplyEmptyDrainRunLockedPlan(plan, nil); len(nilLocked.Locked.AppliedSteps) != 0 || len(nilLocked.Locked.UnknownSteps) != 0 {
		t.Fatalf("nil locked empty drain run result = %+v, want empty", nilLocked)
	}
	if nilPostLock := ApplyEmptyDrainRunPostLockPlan(plan, nil); len(nilPostLock.Idle.AppliedSteps) != 0 || len(nilPostLock.Idle.UnknownSteps) != 0 {
		t.Fatalf("nil post-lock empty drain run result = %+v, want empty", nilPostLock)
	}
	if nilDispatch := ApplyEmptyDrainRunDispatchPlan(plan, nil); len(nilDispatch.Dispatch.AppliedSteps) != 0 || len(nilDispatch.Dispatch.UnknownSteps) != 0 {
		t.Fatalf("nil dispatch empty drain run result = %+v, want empty", nilDispatch)
	}
}

func TestApplyIdleDrainAfterRefillPlan(t *testing.T) {
	applier := new(recordingIdleDrainApplier)
	result := ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Steps: []IdleDrainStep{
		{Action: IdleDrainJoinAvailablePeers},
		{Action: IdleDrainStepAction(255)},
		{Action: IdleDrainFinish},
	}}, applier)

	want := []IdleDrainStepAction{IdleDrainJoinAvailablePeers, IdleDrainFinish}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("idle drain calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(result.AppliedSteps, want) ||
		!reflect.DeepEqual(result.UnknownSteps, []IdleDrainStepAction{IdleDrainStepAction(255)}) {
		t.Fatalf("idle drain result = %+v, want join/finish applied and unknown [255]", result)
	}
	applier.calls = nil
	result = ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Finish: true}, applier)
	if !reflect.DeepEqual(applier.calls, []IdleDrainStepAction{IdleDrainFinish}) {
		t.Fatalf("fallback idle drain calls = %+v, want finish", applier.calls)
	}
	if !reflect.DeepEqual(result.AppliedSteps, []IdleDrainStepAction{IdleDrainFinish}) || len(result.UnknownSteps) != 0 {
		t.Fatalf("fallback idle drain result = %+v, want finish applied", result)
	}
	if nilResult := ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Steps: []IdleDrainStep{{Action: IdleDrainFinish}}}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil idle drain result = %+v, want empty", nilResult)
	}
}

func TestApplyFetchRefillDispatchPlan(t *testing.T) {
	applier := new(recordingFetchRefillDispatchApplier)
	result := ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{Steps: []FetchRefillDispatchStep{
		{Action: FetchRefillDispatchSendOutbound},
		{Action: FetchRefillDispatchStepAction(255)},
	}}, applier)
	if applier.sent != 1 {
		t.Fatalf("refill dispatch sends = %d, want 1", applier.sent)
	}
	if !reflect.DeepEqual(result.AppliedSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchSendOutbound}) ||
		!reflect.DeepEqual(result.UnknownSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchStepAction(255)}) {
		t.Fatalf("refill dispatch result = %+v, want send applied and unknown [255]", result)
	}

	applier.sent = 0
	result = ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{SendOutboundRequests: true}, applier)
	if applier.sent != 1 {
		t.Fatalf("fallback refill dispatch sends = %d, want 1", applier.sent)
	}
	if !reflect.DeepEqual(result.AppliedSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchSendOutbound}) || len(result.UnknownSteps) != 0 {
		t.Fatalf("fallback refill dispatch result = %+v, want send applied", result)
	}
	if nilResult := ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{SendOutboundRequests: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil refill dispatch result = %+v, want empty", nilResult)
	}
}

func TestApplyFetchRefillRunPlan(t *testing.T) {
	runApplier := new(recordingFetchRefillRunApplier)
	dispatchApplier := new(recordingFetchRefillDispatchApplier)
	plan := FetchRefillRunPlan{
		LockedSteps: []FetchRefillRunStep{
			{Action: FetchRefillRunMirrorLegacy},
			{Action: FetchRefillRunStepAction(255)},
		},
		Dispatch: FetchRefillDispatchPlan{Steps: []FetchRefillDispatchStep{
			{Action: FetchRefillDispatchSendOutbound},
			{Action: FetchRefillDispatchStepAction(254)},
		}},
	}

	locked := ApplyFetchRefillRunLockedPlan(plan, runApplier)
	postLock := ApplyFetchRefillRunPostLockPlan(plan, dispatchApplier)
	if runApplier.mirrors != 1 {
		t.Fatalf("fetch refill run mirrors = %d, want 1", runApplier.mirrors)
	}
	if dispatchApplier.sent != 1 {
		t.Fatalf("fetch refill run dispatch sends = %d, want 1", dispatchApplier.sent)
	}
	if !reflect.DeepEqual(locked.Locked.AppliedSteps, []FetchRefillRunStepAction{FetchRefillRunMirrorLegacy}) ||
		!reflect.DeepEqual(locked.Locked.UnknownSteps, []FetchRefillRunStepAction{FetchRefillRunStepAction(255)}) ||
		len(locked.Dispatch.AppliedSteps) != 0 {
		t.Fatalf("locked fetch refill run result = %+v, want mirror applied and unknown [255]", locked)
	}
	if !reflect.DeepEqual(postLock.Dispatch.AppliedSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchSendOutbound}) ||
		!reflect.DeepEqual(postLock.Dispatch.UnknownSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchStepAction(254)}) ||
		len(postLock.Locked.AppliedSteps) != 0 {
		t.Fatalf("post-lock fetch refill run result = %+v, want send applied and unknown [254]", postLock)
	}

	runApplier.mirrors = 0
	locked = ApplyFetchRefillRunLockedPlan(FetchRefillRunPlan{MirrorLegacy: true}, runApplier)
	if runApplier.mirrors != 1 ||
		!reflect.DeepEqual(locked.Locked.AppliedSteps, []FetchRefillRunStepAction{FetchRefillRunMirrorLegacy}) ||
		len(locked.Locked.UnknownSteps) != 0 {
		t.Fatalf("fallback locked fetch refill run result = %+v mirrors=%d, want mirror applied", locked, runApplier.mirrors)
	}
	if nilLocked := ApplyFetchRefillRunLockedPlan(plan, nil); len(nilLocked.Locked.AppliedSteps) != 0 || len(nilLocked.Locked.UnknownSteps) != 0 {
		t.Fatalf("nil locked fetch refill run result = %+v, want empty", nilLocked)
	}
	if nilPostLock := ApplyFetchRefillRunPostLockPlan(plan, nil); len(nilPostLock.Dispatch.AppliedSteps) != 0 || len(nilPostLock.Dispatch.UnknownSteps) != 0 {
		t.Fatalf("nil post-lock fetch refill run result = %+v, want empty", nilPostLock)
	}
}

func TestPlanPostInventorySettlement(t *testing.T) {
	tests := map[string]struct {
		input PostInventorySettlementInput
		want  PostInventorySettlementPlan
	}{
		"stalled retries with no outbound resets": {
			input: PostInventorySettlementInput{Progress: SessionProgress{
				Syncing:      true,
				RetryListLen: 1,
				Peers:        []PeerProgress{{Done: true}},
			}},
			want: PostInventorySettlementPlan{
				Reset:              true,
				TryFindPeer:        true,
				LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryReset}},
				AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryTryFindPeer}},
			},
		},
		"outbound requests suppress stalled reset": {
			input: PostInventorySettlementInput{
				OutboundRequests: 1,
				Progress: SessionProgress{
					Syncing:      true,
					RetryListLen: 1,
					Peers:        []PeerProgress{{Done: true}},
				},
			},
			want: PostInventorySettlementPlan{
				Mirror:      true,
				LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			},
		},
		"complete session finishes": {
			input: PostInventorySettlementInput{Progress: SessionProgress{
				Syncing:     true,
				CurrentHead: 9,
				TargetHead:  9,
				Peers:       []PeerProgress{{Done: true}},
			}},
			want: PostInventorySettlementPlan{
				Mirror:             true,
				Finish:             true,
				LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryMirror}},
				AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryFinish}},
			},
		},
		"incomplete session mirrors legacy queues": {
			input: PostInventorySettlementInput{},
			want: PostInventorySettlementPlan{
				Mirror:      true,
				LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			},
		},
	}

	for name, test := range tests {
		if got := PlanPostInventorySettlement(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s plan = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestPlanPostInventoryRun(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	got := PlanPostInventoryRun(PostInventoryRunInput{
		OutboundRequests: 2,
		Progress:         active,
	})
	want := PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Mirror:      true,
			LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active outbound run = %+v, want %+v", got, want)
	}

	stalled := SessionProgress{Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{Done: true}}}
	got = PlanPostInventoryRun(PostInventoryRunInput{Progress: stalled})
	want = PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Reset:              true,
			TryFindPeer:        true,
			LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryReset}},
			AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryTryFindPeer}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stalled retry run = %+v, want %+v", got, want)
	}

	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	got = PlanPostInventoryRun(PostInventoryRunInput{
		OutboundRequests: 1,
		Progress:         complete,
	})
	want = PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Mirror:             true,
			Finish:             true,
			LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryFinish}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete outbound run = %+v, want %+v", got, want)
	}
}

func TestApplyPostInventoryRunPlan(t *testing.T) {
	settlementApplier := new(recordingPostInventorySettlementApplier)
	dispatchApplier := new(recordingFetchRefillDispatchApplier)
	plan := PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			LockedSteps: []PostInventorySettlementStep{
				{Action: PostInventoryMirror},
				{Action: PostInventorySettlementStepAction(255)},
			},
			AfterDispatchSteps: []PostInventorySettlementStep{
				{Action: PostInventoryFinish},
				{Action: PostInventorySettlementStepAction(254)},
			},
		},
		Dispatch: FetchRefillDispatchPlan{
			Steps: []FetchRefillDispatchStep{
				{Action: FetchRefillDispatchSendOutbound},
				{Action: FetchRefillDispatchStepAction(253)},
			},
		},
	}

	locked := ApplyPostInventoryRunLockedPlan(plan, settlementApplier)
	postLock := ApplyPostInventoryRunPostLockPlan(plan, dispatchApplier, settlementApplier)

	if !reflect.DeepEqual(settlementApplier.calls, []PostInventorySettlementStepAction{PostInventoryMirror, PostInventoryFinish}) {
		t.Fatalf("post-inventory run settlement calls = %+v, want mirror/finish", settlementApplier.calls)
	}
	if dispatchApplier.sent != 1 {
		t.Fatalf("post-inventory run dispatch sends = %d, want 1", dispatchApplier.sent)
	}
	if !reflect.DeepEqual(locked.LockedSettlement.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryMirror}) ||
		!reflect.DeepEqual(locked.LockedSettlement.UnknownSteps, []PostInventorySettlementStepAction{PostInventorySettlementStepAction(255)}) ||
		len(locked.Dispatch.AppliedSteps) != 0 ||
		len(locked.AfterDispatchSettlement.AppliedSteps) != 0 {
		t.Fatalf("locked post-inventory run result = %+v, want locked mirror result only", locked)
	}
	if !reflect.DeepEqual(postLock.Dispatch.AppliedSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchSendOutbound}) ||
		!reflect.DeepEqual(postLock.Dispatch.UnknownSteps, []FetchRefillDispatchStepAction{FetchRefillDispatchStepAction(253)}) ||
		!reflect.DeepEqual(postLock.AfterDispatchSettlement.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryFinish}) ||
		!reflect.DeepEqual(postLock.AfterDispatchSettlement.UnknownSteps, []PostInventorySettlementStepAction{PostInventorySettlementStepAction(254)}) ||
		len(postLock.LockedSettlement.AppliedSteps) != 0 {
		t.Fatalf("post-lock post-inventory run result = %+v, want dispatch and after-dispatch settlement", postLock)
	}

	if nilLocked := ApplyPostInventoryRunLockedPlan(plan, nil); len(nilLocked.LockedSettlement.AppliedSteps) != 0 || len(nilLocked.LockedSettlement.UnknownSteps) != 0 {
		t.Fatalf("nil locked post-inventory run result = %+v, want empty", nilLocked)
	}
	if nilPostLock := ApplyPostInventoryRunPostLockPlan(plan, nil, nil); len(nilPostLock.Dispatch.AppliedSteps) != 0 ||
		len(nilPostLock.Dispatch.UnknownSteps) != 0 ||
		len(nilPostLock.AfterDispatchSettlement.AppliedSteps) != 0 ||
		len(nilPostLock.AfterDispatchSettlement.UnknownSteps) != 0 {
		t.Fatalf("nil post-lock post-inventory run result = %+v, want empty", nilPostLock)
	}
}

func TestApplyPostInventorySettlementPlan(t *testing.T) {
	applier := new(recordingPostInventorySettlementApplier)
	plan := PostInventorySettlementPlan{
		LockedSteps: []PostInventorySettlementStep{
			{Action: PostInventoryReset},
			{Action: PostInventorySettlementStepAction(255)},
			{Action: PostInventoryMirror},
		},
		AfterDispatchSteps: []PostInventorySettlementStep{
			{Action: PostInventoryTryFindPeer},
			{Action: PostInventorySettlementStepAction(255)},
			{Action: PostInventoryFinish},
		},
	}
	lockedResult := ApplyPostInventorySettlementLockedPlan(plan, applier)
	afterResult := ApplyPostInventorySettlementAfterDispatchPlan(plan, applier)

	want := []PostInventorySettlementStepAction{
		PostInventoryReset,
		PostInventoryMirror,
		PostInventoryTryFindPeer,
		PostInventoryFinish,
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("post-inventory calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(lockedResult.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryReset, PostInventoryMirror}) ||
		!reflect.DeepEqual(lockedResult.UnknownSteps, []PostInventorySettlementStepAction{PostInventorySettlementStepAction(255)}) {
		t.Fatalf("locked post-inventory result = %+v, want reset/mirror applied and unknown [255]", lockedResult)
	}
	if !reflect.DeepEqual(afterResult.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryTryFindPeer, PostInventoryFinish}) ||
		!reflect.DeepEqual(afterResult.UnknownSteps, []PostInventorySettlementStepAction{PostInventorySettlementStepAction(255)}) {
		t.Fatalf("after-dispatch post-inventory result = %+v, want try-find/finish applied and unknown [255]", afterResult)
	}

	applier.calls = nil
	lockedResult = ApplyPostInventorySettlementLockedPlan(PostInventorySettlementPlan{Mirror: true}, applier)
	afterResult = ApplyPostInventorySettlementAfterDispatchPlan(PostInventorySettlementPlan{Finish: true}, applier)
	want = []PostInventorySettlementStepAction{PostInventoryMirror, PostInventoryFinish}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fallback post-inventory calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(lockedResult.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryMirror}) ||
		len(lockedResult.UnknownSteps) != 0 {
		t.Fatalf("fallback locked post-inventory result = %+v, want mirror applied", lockedResult)
	}
	if !reflect.DeepEqual(afterResult.AppliedSteps, []PostInventorySettlementStepAction{PostInventoryFinish}) ||
		len(afterResult.UnknownSteps) != 0 {
		t.Fatalf("fallback after-dispatch post-inventory result = %+v, want finish applied", afterResult)
	}
	if nilResult := ApplyPostInventorySettlementLockedPlan(PostInventorySettlementPlan{Reset: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil locked post-inventory result = %+v, want empty", nilResult)
	}
	if nilResult := ApplyPostInventorySettlementAfterDispatchPlan(PostInventorySettlementPlan{TryFindPeer: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil after-dispatch post-inventory result = %+v, want empty", nilResult)
	}
}

func TestSessionProgressShouldFinish(t *testing.T) {
	done := SessionProgress{
		Syncing:     true,
		CurrentHead: 9,
		TargetHead:  9,
		Peers: []PeerProgress{
			{Done: true},
			{Done: true},
		},
	}
	if !done.ShouldFinish() {
		t.Fatal("drained session at target should finish")
	}

	for name, progress := range map[string]SessionProgress{
		"paused":         {Syncing: true, Paused: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
		"not-syncing":    {Syncing: false, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
		"retry":          {Syncing: true, CurrentHead: 9, TargetHead: 9, RetryListLen: 1, Peers: []PeerProgress{{Done: true}}},
		"buffer":         {Syncing: true, CurrentHead: 9, TargetHead: 9, BlockBufferLen: 1, Peers: []PeerProgress{{Done: true}}},
		"peer-fetch":     {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, FetchListLen: 1}}},
		"peer-inflight":  {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, Inflight: 1}}},
		"peer-requested": {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, ChainRequested: true}}},
		"peer-not-done":  {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: false}}},
		"below-target":   {Syncing: true, CurrentHead: 8, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
	} {
		if progress.ShouldFinish() {
			t.Fatalf("%s progress unexpectedly finished", name)
		}
	}
}

func TestSessionProgressShouldRestartForStalledRetries(t *testing.T) {
	progress := SessionProgress{
		Syncing:      true,
		RetryListLen: 2,
		Peers:        []PeerProgress{{Done: false}, {Done: true}},
	}
	if !progress.ShouldRestartForStalledRetries() {
		t.Fatal("idle peers with retries should restart")
	}

	for name, progress := range map[string]SessionProgress{
		"paused":         {Syncing: true, Paused: true, RetryListLen: 1},
		"not-syncing":    {Syncing: false, RetryListLen: 1},
		"no-retry":       {Syncing: true},
		"buffer":         {Syncing: true, RetryListLen: 1, BlockBufferLen: 1},
		"peer-fetch":     {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{FetchListLen: 1}}},
		"peer-inflight":  {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{Inflight: 1}}},
		"peer-requested": {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{ChainRequested: true}}},
	} {
		if progress.ShouldRestartForStalledRetries() {
			t.Fatalf("%s progress unexpectedly restarted", name)
		}
	}
}

type recordingIdleDrainApplier struct {
	calls []IdleDrainStepAction
}

type recordingEmptyDrainJoinGate struct {
	allowed  bool
	calls    int
	progress SessionProgress
}

func (g *recordingEmptyDrainJoinGate) CheckJoinAvailablePeers(progress SessionProgress) bool {
	g.calls++
	g.progress = progress
	return g.allowed
}

func (a *recordingIdleDrainApplier) FinishSync() {
	a.calls = append(a.calls, IdleDrainFinish)
}

func (a *recordingIdleDrainApplier) JoinAvailablePeers() {
	a.calls = append(a.calls, IdleDrainJoinAvailablePeers)
}

type recordingFetchRefillDispatchApplier struct {
	sent int
}

func (a *recordingFetchRefillDispatchApplier) SendOutboundRequests() {
	a.sent++
}

type recordingFetchRefillRunApplier struct {
	mirrors int
}

func (a *recordingFetchRefillRunApplier) MirrorLegacyUnderLock() {
	a.mirrors++
}

type recordingEmptyDrainRunApplier struct {
	mirrors int
}

func (a *recordingEmptyDrainRunApplier) MirrorLegacyUnderLock() {
	a.mirrors++
}

type recordingEmptyDrainPreparationApplier struct {
	begins   []uint64
	outbound int
	refills  int
}

func (a *recordingEmptyDrainPreparationApplier) BeginBufferWait(next uint64) {
	a.begins = append(a.begins, next)
}

func (a *recordingEmptyDrainPreparationApplier) RefillFetchSlots() int {
	a.refills++
	return a.outbound
}

type recordingPostInventorySettlementApplier struct {
	calls []PostInventorySettlementStepAction
}

func (a *recordingPostInventorySettlementApplier) ResetSyncUnderLock() {
	a.calls = append(a.calls, PostInventoryReset)
}

func (a *recordingPostInventorySettlementApplier) MirrorLegacyUnderLock() {
	a.calls = append(a.calls, PostInventoryMirror)
}

func (a *recordingPostInventorySettlementApplier) TryFindSyncPeer() {
	a.calls = append(a.calls, PostInventoryTryFindPeer)
}

func (a *recordingPostInventorySettlementApplier) FinishSync() {
	a.calls = append(a.calls, PostInventoryFinish)
}
