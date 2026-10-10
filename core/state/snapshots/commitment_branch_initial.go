package snapshots

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type CommitmentBaseMode string

const (
	CommitmentBaseOff         CommitmentBaseMode = "off"
	CommitmentBaseInitialOnly CommitmentBaseMode = "initial-only"
)

func ParseCommitmentBaseMode(value string) (CommitmentBaseMode, error) {
	switch CommitmentBaseMode(strings.ToLower(strings.TrimSpace(value))) {
	case "", CommitmentBaseOff:
		return CommitmentBaseOff, nil
	case CommitmentBaseInitialOnly:
		return CommitmentBaseInitialOnly, nil
	default:
		return "", fmt.Errorf("snapshots: invalid commitment base mode %q (want off or initial-only)", value)
	}
}

type initialCommitmentBranchRotator interface {
	BeginInitialCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error)
	CompleteInitialCommitmentBranchRotation(context.Context, rawdb.CommitmentBranchRotation, *VerifiedCommitmentBranchBase) error
	CleanupAcceptedInitialCommitmentBranchBase(context.Context, *VerifiedCommitmentBranchBase) (bool, error)
}

var (
	initialCommitmentAttempts  = metrics.NewRegisteredCounter("state/snapshot/cold/commitment_initial/attempts", nil)
	initialCommitmentDeferred  = metrics.NewRegisteredCounter("state/snapshot/cold/commitment_initial/deferred", nil)
	initialCommitmentCompleted = metrics.NewRegisteredCounter("state/snapshot/cold/commitment_initial/completed", nil)
	initialCommitmentErrors    = metrics.NewRegisteredCounter("state/snapshot/cold/commitment_initial/errors", nil)
	initialCommitmentCancelled = metrics.NewRegisteredCounter("state/snapshot/cold/commitment_initial/resource_cancelled", nil)
	initialCommitmentRunning   = metrics.NewRegisteredGauge("state/snapshot/cold/commitment_initial/running", nil)
)

func (r *Runner) initialCommitmentMode() bool {
	return r != nil && r.cfg.CommitmentBaseMode == CommitmentBaseInitialOnly
}

// Probe directly rather than updating the passMu-owned history controller from
// the monitor. Running work needs the free-space floor, not the initial output
// reserve again after it has already started consuming that reserve.
func (r *Runner) initialCommitmentResources(ctx context.Context, admission bool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	now := time.Now()
	load := r.cfg.HistoryLoadProbe()
	if !load.Available || !freshHistoryLoad(load.SampledAt, now) || load.HardLimitReached(now) {
		return errors.New("snapshots: initial commitment base requires fresh engine headroom")
	}
	resources := r.cfg.HistoryReadResourceProbe()
	if !resources.Available || !freshHistoryLoad(resources.SampledAt, now) {
		return errors.New("snapshots: initial commitment base requires fresh resource samples")
	}
	memory := uint64(512 << 20)
	if admission {
		memory = historyReadMinimumHeadroom
	}
	if resources.MemoryAvailableBytes < memory {
		return errors.New("snapshots: initial commitment base memory headroom insufficient")
	}
	if admission && (!load.DeviceAvailable || !freshHistoryLoad(load.DeviceSampledAt, now) || load.DeviceAwait < 0 || load.DeviceAwait > 2*time.Millisecond || resources.IdleCoresMilli < 1000) {
		return errors.New("snapshots: initial commitment base CPU/device admission deferred")
	}
	pressure, err := r.cfg.HistoryPressureProbe(ctx)
	if err != nil {
		return err
	}
	required := r.cfg.MinHistoryBuildFreeBytes
	if admission {
		reserve := r.cfg.InitialCommitmentBaseReserveBytes
		if reserve == 0 {
			reserve = 128 << 30
		}
		if reserve > math.MaxUint64-required {
			return errors.New("snapshots: initial commitment base free-space budget overflow")
		}
		required += reserve
	}
	if !pressure.FreeBytesAvailable || pressure.FreeBytes < required {
		return errors.New("snapshots: initial commitment base free-space budget unavailable")
	}
	return contextError(ctx)
}

func initialCommitmentRetry(work time.Duration) time.Duration {
	// Independent from the shared gate and history deadlines. Even a failed
	// multi-hour build must leave normal archive maintenance free to run.
	if work > time.Duration(math.MaxInt64/4) {
		return time.Duration(math.MaxInt64)
	}
	return max(time.Minute, 4*work)
}

func (r *Runner) initialCommitmentMonitor(ctx context.Context, cancel context.CancelCauseFunc, period time.Duration) func() {
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				if err := r.initialCommitmentResources(monitorCtx, false); err != nil {
					if monitorCtx.Err() != nil {
						return
					}
					initialCommitmentCancelled.Inc(1)
					cancel(err)
					return
				}
			}
		}
	}()
	return func() { stopMonitor(); <-done }
}

// Called under passMu before ordinary history work so continuous history debt
// cannot starve the explicitly requested one-time migration. A refused lease
// or resource check leaves ordinary archive work eligible in this same pass.
func (r *Runner) initialCommitmentPass(ctx context.Context) (attempted, built bool, retErr error) {
	if !r.initialCommitmentMode() || !r.cfg.Enabled || r.initialCommitmentDone || time.Now().Before(r.initialCommitmentNotBefore) {
		return false, false, nil
	}
	db := r.chain.DB()
	base, based, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil {
		return false, false, err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(db)
	if err != nil {
		return false, false, err
	}
	if based && (base.Generation != 1 || rotating) || rotating && rotation.Generation != 1 {
		return false, false, errors.New("snapshots: initial commitment mode rejects successor layouts")
	}
	if based {
		rows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(db)
		if err != nil {
			return false, false, err
		}
		if !rows {
			r.initialCommitmentDone = true
			return false, false, nil
		}
	}
	rotator, ok := r.chain.(initialCommitmentBranchRotator)
	if !ok {
		return false, false, errors.New("snapshots: initial commitment mode requires chain rotation support")
	}
	if err := r.initialCommitmentResources(ctx, true); err != nil {
		initialCommitmentDeferred.Inc(1)
		return false, false, contextError(ctx)
	}
	release, ok := r.cfg.HeavyWorkGate.TryAcquireWithCooldown(r.cfg.CatchupHeavyWorkCooldown)
	if !ok {
		initialCommitmentDeferred.Inc(1)
		return false, false, nil
	}
	defer release()
	if err := r.initialCommitmentResources(ctx, true); err != nil {
		initialCommitmentDeferred.Inc(1)
		return false, false, contextError(ctx)
	}
	started := time.Now()
	initialCommitmentAttempts.Inc(1)
	initialCommitmentRunning.Update(1)
	defer func() {
		initialCommitmentRunning.Update(0)
		r.initialCommitmentNotBefore = time.Now().Add(initialCommitmentRetry(time.Since(started)))
		if retErr != nil {
			initialCommitmentErrors.Inc(1)
		}
	}()
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	join := r.initialCommitmentMonitor(jobCtx, cancel, 2*time.Second)
	defer join()
	var proof *VerifiedCommitmentBranchBase
	if based {
		rotation = rawdb.CommitmentBranchRotation{Generation: base.Generation, SnapshotTxNum: base.SnapshotTxNum, Root: base.Root, BlockNum: base.BlockNum, BlockHash: base.BlockHash}
		proof, err = VerifyCommitmentBranchBase(jobCtx, r.cfg.Dir, rotation)
	} else {
		rotation, rotating, err = rotator.BeginInitialCommitmentBranchRotation()
		if err == nil && !rotating {
			return true, false, nil
		}
		if err == nil {
			err = contextError(jobCtx)
		}
		if err == nil {
			var result *AggregatorBuildResult
			result, err = NewAggregator(r.cfg.Dir).BuildCommitmentBranchBaseContext(jobCtx, db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: rotation.SnapshotTxNum})
			if result != nil {
				built = len(result.Segments) > 0
				proof = result.commitmentBaseProof
			}
			if err == nil && proof == nil {
				proof, err = VerifyCommitmentBranchBase(jobCtx, r.cfg.Dir, rotation)
			}
		}
	}
	// Core verifies the independent branch root outside chain locks and checks
	// job cancellation once more before its indivisible durable marker/delete
	// section. The monitor remains active through that final admission.
	if err == nil {
		err = contextError(jobCtx)
	}
	if err != nil {
		return true, built, errors.Join(err, context.Cause(jobCtx))
	}
	if based {
		_, err = rotator.CleanupAcceptedInitialCommitmentBranchBase(jobCtx, proof)
	} else {
		err = rotator.CompleteInitialCommitmentBranchRotation(jobCtx, rotation, proof)
	}
	if errors.Is(err, ErrCommitmentBranchRotationNotSolidified) {
		return true, built, nil
	}
	if err != nil {
		return true, built, err
	}
	r.initialCommitmentDone = true
	initialCommitmentCompleted.Inc(1)
	coldSnapshotLog.Info("Initial commitment branch base accepted and legacy range cleared", "generation", rotation.Generation, "block", rotation.BlockNum, "tx", rotation.SnapshotTxNum, "elapsed", time.Since(started))
	return true, built, nil
}
