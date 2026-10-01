package core

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var (
	historyStagingMoverAttempts         = metrics.NewRegisteredCounter("core/history_staging/mover/attempts", nil)
	historyStagingMoverErrors           = metrics.NewRegisteredCounter("core/history_staging/mover/errors", nil)
	historyStagingMoverMoved            = metrics.NewRegisteredCounter("core/history_staging/mover/moved_buckets", nil)
	historyStagingMoverCopiedBytes      = metrics.NewRegisteredCounter("core/history_staging/mover/copied_bytes", nil)
	historyStagingMoverResumedClaims    = metrics.NewRegisteredCounter("core/history_staging/mover/resumed_claims", nil)
	historyStagingMoverResumedClears    = metrics.NewRegisteredCounter("core/history_staging/mover/resumed_clears", nil)
	historyStagingMoverSkippedHot       = metrics.NewRegisteredCounter("core/history_staging/mover/skipped_hot_pressure", nil)
	historyStagingMoverSkippedStage     = metrics.NewRegisteredCounter("core/history_staging/mover/skipped_stage_pressure", nil)
	historyStagingMoverSkippedGate      = metrics.NewRegisteredCounter("core/history_staging/mover/skipped_heavy_gate", nil)
	historyStagingMoverProofNanos       = metrics.NewRegisteredCounter("core/history_staging/mover/proof_nanos", nil)
	historyStagingMoverCopyNanos        = metrics.NewRegisteredCounter("core/history_staging/mover/copy_nanos", nil)
	historyStagingMoverAdoptNanos       = metrics.NewRegisteredCounter("core/history_staging/mover/adopt_nanos", nil)
	historyStagingMoverClearNanos       = metrics.NewRegisteredCounter("core/history_staging/mover/clear_nanos", nil)
	historyStagingMoverLastSuccess      = metrics.NewRegisteredGauge("core/history_staging/mover/last_success_unix", nil)
	historyStagingMoverLastError        = metrics.NewRegisteredGauge("core/history_staging/mover/last_error_unix", nil)
	historyStagingMoverNextBucket       = metrics.NewRegisteredGauge("core/history_staging/mover/next_bucket", nil)
	historyStagingMoverEligibleBucket   = metrics.NewRegisteredGauge("core/history_staging/mover/eligible_bucket", nil)
	historyStagingMoverSourceBuckets    = metrics.NewRegisteredGauge("core/history_staging/mover/census_source_buckets", nil)
	historyStagingMoverUnclearedBuckets = metrics.NewRegisteredGauge("core/history_staging/mover/census_target_uncleared_buckets", nil)
	historyStagingMoverCensusAt         = metrics.NewRegisteredGauge("core/history_staging/mover/census_unix", nil)
)

// HistoryStagingMoverConfig bounds one optional transfer. Hot and Stage
// pressure are independent engine measurements; the shared device must also
// have a fresh observation. No sample is interpreted as spare capacity.
type HistoryStagingMoverConfig struct {
	HistoryWindow uint64
	Cadence       time.Duration
	Limits        rawdb.HistoryStagingLimits
	HeavyWorkGate *maintenance.HeavyWorkGate
	HotPressure   func() maintenance.StoragePressure
	StagePressure func() maintenance.StoragePressure
}

type HistoryStagingMover struct {
	bc                   *BlockChain
	cfg                  HistoryStagingMoverConfig
	passMu               sync.Mutex // separate from snapshots.Runner.passMu
	mu                   sync.Mutex
	cancel               context.CancelFunc
	done                 chan struct{}
	nextBucket           uint64
	auditCursor          []byte
	retireCursor         []byte
	quarantineEpoch      uint64
	quarantineResetEpoch uint64
	quarantineRefsDone   bool
	quarantineHotDone    bool
	auditSource          uint64
	auditUncleared       uint64
	lastLog              time.Time
}

func NewHistoryStagingMover(bc *BlockChain, cfg HistoryStagingMoverConfig) (*HistoryStagingMover, error) {
	if bc == nil || bc.HistoryStagingManager() == nil || cfg.HistoryWindow == 0 || cfg.Cadence <= 0 || cfg.HotPressure == nil || cfg.StagePressure == nil || cfg.HeavyWorkGate == nil {
		return nil, errors.New("history staging mover: incomplete configuration")
	}
	m := &HistoryStagingMover{bc: bc, cfg: cfg, nextBucket: 1, quarantineEpoch: 1}
	if !bc.historyStagingMover.CompareAndSwap(nil, m) {
		return nil, errors.New("history staging mover: already installed")
	}
	return m, nil
}

func (m *HistoryStagingMover) Start() error {
	if m == nil {
		return errors.New("history staging mover: nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done != nil {
		return errors.New("history staging mover: already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel, m.done = cancel, make(chan struct{})
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(m.cfg.Cadence)
		defer ticker.Stop()
		for {
			// A failed optional pass is retried after the cadence; it never
			// blocks canonical import or turns an unknown pressure into idle.
			if err := m.RunOnce(ctx); err != nil && ctx.Err() == nil {
				m.mu.Lock()
				if time.Since(m.lastLog) >= time.Minute {
					log.Warn("History staging mover pass failed", "err", err)
					m.lastLog = time.Now()
				}
				m.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

// Stop cancels and drains copy/proof before reset takes the chain writer
// guard. Calling it under chainmu would deadlock an adoption waiting there.
func (m *HistoryStagingMover) Stop() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

func (m *HistoryStagingMover) hasStarted() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done != nil
}

func historyStagingPressureReady(p maintenance.StoragePressure, now time.Time) bool {
	if !p.Available || p.SampledAt.IsZero() || now.Sub(p.SampledAt) < -time.Second || now.Sub(p.SampledAt) > 15*time.Second || p.HardLimitReached(now) {
		return false
	}
	if !p.DeviceAvailable || p.DeviceSampledAt.IsZero() || now.Sub(p.DeviceSampledAt) < -time.Second || now.Sub(p.DeviceSampledAt) > 15*time.Second {
		return false
	}
	return p.DeviceBusyPPM < 900_000 && p.DeviceQueueMilli < 1_000 && p.DeviceAwait < 20*time.Millisecond
}

// RunOnce migrates at most one full bucket, with the configured copy limits.
// It is callable by tests and deliberately does not own the cold Runner lock.
func (m *HistoryStagingMover) RunOnce(ctx context.Context) (err error) {
	if m == nil || ctx == nil {
		return errors.New("history staging mover: missing context")
	}
	m.passMu.Lock()
	defer m.passMu.Unlock()
	historyStagingMoverAttempts.Inc(1)
	defer func() {
		if err != nil && ctx.Err() == nil {
			historyStagingMoverErrors.Inc(1)
			historyStagingMoverLastError.Update(time.Now().Unix())
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.bc != nil && m.bc.HistoryStagingManager() != nil {
		if err := m.updateCensus(ctx); err != nil {
			return err
		}
	}
	now := time.Now()
	if !historyStagingPressureReady(m.cfg.HotPressure(), now) {
		historyStagingMoverSkippedHot.Inc(1)
		return nil
	}
	if !historyStagingPressureReady(m.cfg.StagePressure(), now) {
		historyStagingMoverSkippedStage.Inc(1)
		return nil
	}
	releaseHeavy, ok := m.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		historyStagingMoverSkippedGate.Inc(1)
		return nil
	}
	defer releaseHeavy()
	moveErr := m.runAdmitted(ctx)
	retireErr := m.runRetirement(ctx)
	quarantineErr := m.runQuarantine(ctx)
	return errors.Join(moveErr, retireErr, quarantineErr)
}

// runQuarantine is a bounded post-reset cleanup page. The cold publication
// lease authenticates the durable reset-tail isolation marker before any old
// ContentID references or epoch-private target bytes are retired. Rawdb also
// refuses cleanup while an older routed view is leased.
func (m *HistoryStagingMover) runQuarantine(ctx context.Context) error {
	manager := m.bc.HistoryStagingManager()
	if manager == nil {
		return rawdb.ErrHistoryStagingUninitialized
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil || !present || !intent.Complete {
		return err
	}
	if m.quarantineResetEpoch != intent.NewEpoch {
		m.quarantineResetEpoch = intent.NewEpoch
		m.quarantineEpoch = 1
		m.quarantineRefsDone = false
		m.quarantineHotDone = false
	}
	if m.quarantineEpoch == 0 {
		m.quarantineEpoch = 1
	}
	if m.quarantineEpoch >= intent.NewEpoch && m.quarantineRefsDone && m.quarantineHotDone {
		return nil
	}
	coldManager, ok := m.bc.stateCodeColdHistory.(*snapshots.Manager)
	if !ok || coldManager == nil {
		return rawdb.ErrHistoryStagingIncomplete
	}
	var certifiedManifest *snapshots.Manifest
	release, err := snapshots.AcquireHistoryStagingQuarantineRetirement(ctx, coldManager.HistoryStagingDir(),
		func(ctx context.Context, manifest *snapshots.Manifest) error {
			if err := snapshots.VerifyHistoryStagingQuarantineColdTail(ctx, manifest, manager); err != nil {
				return err
			}
			certifiedManifest = manifest
			return nil
		})
	if err != nil {
		return err
	}
	defer release()
	verifyTail := func() error { return snapshots.VerifyHistoryStagingQuarantineColdTail(ctx, certifiedManifest, manager) }
	if m.quarantineEpoch < intent.NewEpoch {
		_, targetDone, targetErr := manager.ClearQuarantinedTargetEpoch(ctx, m.quarantineEpoch, 128, 32<<20, verifyTail)
		if targetErr != nil {
			return targetErr
		}
		if targetDone {
			m.quarantineEpoch++
		}
	}
	if !m.quarantineRefsDone {
		_, done, err := manager.RetireQuarantinedColdRefs(ctx, 128, verifyTail)
		if err != nil {
			return err
		}
		m.quarantineRefsDone = done
	}
	if m.quarantineEpoch >= intent.NewEpoch && m.quarantineRefsDone && !m.quarantineHotDone {
		_, done, err := manager.RetireQuarantinedHotMetadata(ctx, 128, 4<<20, verifyTail)
		if err != nil {
			return err
		}
		m.quarantineHotDone = done
	}
	return nil
}

// runRetirement uses its own wrapping metadata cursor. New cold publication
// can make an old TARGET bucket eligible long after the migration cursor has
// passed it. At most 256 metadata rows and one retirement job run per pass.
func (m *HistoryStagingMover) runRetirement(ctx context.Context) error {
	manager := m.bc.HistoryStagingManager()
	if manager == nil {
		return rawdb.ErrHistoryStagingUninitialized
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil {
		return err
	}
	states, next, complete, err := manager.ScanBuckets(ctx, m.retireCursor, 256)
	if err != nil {
		return err
	}
	for _, state := range states {
		if !state.HasRoute || state.Bucket == 0 || state.Route.Epoch != epoch {
			continue
		}
		route := state.Route
		var action func() error
		switch {
		case route.Owner == rawdb.HistoryStagingOwnerTarget && !route.SourceCleared && state.HasClaim && state.Claim.Epoch == epoch:
			claim := state.Claim
			action = func() error {
				historyStagingMoverResumedClears.Inc(1)
				started := time.Now()
				err := manager.ClearSource(ctx, claim, m.cfg.Limits)
				historyStagingMoverClearNanos.Inc(time.Since(started).Nanoseconds())
				if err != nil {
					return err
				}
				return m.finalizeColdBucket(ctx, state.Bucket)
			}
		case route.Owner == rawdb.HistoryStagingOwnerCold && !route.TargetCleared:
			action = func() error { return m.finalizeColdBucket(ctx, state.Bucket) }
		case route.Owner == rawdb.HistoryStagingOwnerTarget && route.SourceCleared && route.ColdBindingEpoch != 0:
			binding, present, err := manager.ReadColdBindingAt(route.Epoch, state.Bucket)
			if err != nil {
				return err
			}
			first, last, _ := rawdb.StateHistoryChunkBucketBounds(state.Bucket)
			if present && historyStagingBindingFull(binding, first, last) {
				action = func() error { return m.finalizeColdBucket(ctx, state.Bucket) }
			} else {
				action = func() error {
					certified, err := m.certifyNewColdTarget(ctx, state.Bucket)
					if err != nil || !certified {
						return err
					}
					return m.finalizeColdBucket(ctx, state.Bucket)
				}
			}
		case route.Owner == rawdb.HistoryStagingOwnerTarget && route.SourceCleared:
			action = func() error {
				certified, err := m.certifyNewColdTarget(ctx, state.Bucket)
				if err != nil || !certified {
					return err
				}
				return m.finalizeColdBucket(ctx, state.Bucket)
			}
		}
		if action == nil {
			continue
		}
		var selected [8]byte
		binary.BigEndian.PutUint64(selected[:], state.Bucket)
		if err := action(); err != nil {
			return err
		}
		m.retireCursor = append(m.retireCursor[:0], selected[:]...)
		return nil
	}
	if complete {
		m.retireCursor = nil
	} else {
		m.retireCursor = append(m.retireCursor[:0], next...)
	}
	return nil
}

// certifyNewColdTarget revisits a previously transferred pure-hot bucket when
// the cold Runner later publishes its history. It compares every logical
// target changeset (including zero-change blocks and repairs) with an
// authenticated cold trio before adding durable cold refs. A cold manifest's
// height or tx range alone never authorizes physical target deletion.
func (m *HistoryStagingMover) certifyNewColdTarget(ctx context.Context, bucket uint64) (bool, error) {
	manager := m.bc.HistoryStagingManager()
	coldManager, ok := m.bc.stateCodeColdHistory.(*snapshots.Manager)
	if !ok || coldManager == nil {
		return false, nil // no cold publication source is configured yet
	}
	releasePublication, err := snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
	if err != nil {
		return false, err
	}
	// Admission precedes canonical index/chain locks. Capture the source and
	// cold snapshots while it is held, then do expensive semantic work outside.
	blocks, err := m.canonicalBucketBlocks(ctx, bucket)
	if err != nil {
		releasePublication()
		return false, err
	}
	route, present, err := manager.ReadRoute(bucket)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerTarget || !route.SourceCleared {
		releasePublication()
		return false, rawdb.ErrHistoryStagingConflict
	}
	var oldBinding rawdb.HistoryStagingColdBinding
	if route.ColdBindingEpoch != 0 {
		var hasOld bool
		oldBinding, hasOld, err = manager.ReadColdBindingAt(route.Epoch, bucket)
		if err != nil || !hasOld || oldBinding.BindingEpoch != route.ColdBindingEpoch {
			releasePublication()
			return false, rawdb.ErrHistoryStagingConflict
		}
	}
	pinned, releaseCold, err := coldManager.PinHistoryReadView()
	if err != nil {
		releasePublication()
		return false, err
	}
	manifest := pinned.Manifest()
	if manifest == nil || manifest.VisibleTxEnd < blocks[len(blocks)-1].EndTxNum {
		releasePublication()
		releaseCold()
		return false, nil // the next cold publication may cover this bucket
	}
	target, err := manager.AcquireTargetBucketView(route.Epoch, bucket)
	releasePublication()
	if err != nil {
		releaseCold()
		return false, err
	}
	var spans []rawdb.HistoryStagingColdSpan
	var proofErr error
	if route.ColdBindingEpoch == 0 {
		spans, proofErr = snapshots.VerifyHistoryStagingTargetColdEquivalence(ctx, target, pinned.HistoryStagingDir(), manifest, blocks)
	} else {
		spans, proofErr = snapshots.VerifyHistoryStagingMixedTargetColdEquivalence(ctx, target, pinned.HistoryStagingDir(), manifest, blocks, oldBinding)
	}
	closeErr := target.Close()
	if proofErr != nil || closeErr != nil {
		releaseCold()
		return false, errors.Join(proofErr, closeErr)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: bucket,
		Epoch: route.Epoch, BindingEpoch: route.ColdBindingEpoch + 1, ManifestEpoch: manifest.Generation, Spans: spans}
	releasePublication, err = snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
	if err != nil {
		releaseCold()
		return false, err
	}
	defer releasePublication()
	defer releaseCold()
	live := coldManager.Manifest()
	if live == nil || live.Generation != binding.ManifestEpoch {
		return false, rawdb.ErrHistoryStagingConflict
	}
	if err := manager.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		return false, err
	}
	return true, nil
}

// The census is diagnostic only: 256 metadata rows per pass, and gauges are
// published only after one complete traversal. It never qualifies a move.
func (m *HistoryStagingMover) updateCensus(ctx context.Context) error {
	manager := m.bc.HistoryStagingManager()
	epoch, err := manager.CurrentEpoch()
	if err != nil {
		return err
	}
	states, next, complete, err := manager.ScanBuckets(ctx, m.auditCursor, 256)
	if err != nil {
		return err
	}
	for _, state := range states {
		if !state.HasRoute || state.Route.Epoch != epoch {
			continue
		}
		switch state.Route.Owner {
		case rawdb.HistoryStagingOwnerSource:
			m.auditSource++
		case rawdb.HistoryStagingOwnerTarget:
			if !state.Route.SourceCleared {
				m.auditUncleared++
			}
		}
	}
	if complete {
		historyStagingMoverSourceBuckets.Update(int64(m.auditSource))
		historyStagingMoverUnclearedBuckets.Update(int64(m.auditUncleared))
		historyStagingMoverCensusAt.Update(time.Now().Unix())
		m.auditSource, m.auditUncleared, m.auditCursor = 0, 0, nil
	} else {
		m.auditCursor = append(m.auditCursor[:0], next...)
	}
	return nil
}

func (m *HistoryStagingMover) runAdmitted(ctx context.Context) (retErr error) {
	proofStarted := time.Now()
	bc := m.bc
	manager := bc.HistoryStagingManager()
	if manager == nil {
		return errors.New("history staging mover: manager detached")
	}
	coldManager, ok := bc.stateCodeColdHistory.(*snapshots.Manager)
	// This admission precedes index/chain locks and pins a proof's cold view
	// before a concurrent manifest publication can replace its files.
	var releasePublication func()
	if ok && coldManager != nil {
		var err error
		releasePublication, err = snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
		if err != nil {
			return err
		}
	}
	defer func() {
		if releasePublication != nil {
			releasePublication()
		}
	}()
	bc.stateHistoryIndexMu.Lock()
	if err := lockMutexContext(ctx, &bc.chainmu); err != nil {
		bc.stateHistoryIndexMu.Unlock()
		return err
	}
	if bc.closed.Load() || bc.historyStagingReplayEpoch.Load() != 0 {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return nil
	}
	solid := bc.cachedDynProps().LatestSolidifiedBlockNum()
	head := bc.CurrentBlock()
	if head == nil {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return errors.New("history staging mover: current head unavailable")
	}
	finish, finishOK, finishErr := rawdb.ReadStageProgressRow(bc.db, rawdb.StageFinish)
	indexed, indexOK, indexErr := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if solid <= 0 || uint64(solid) <= m.cfg.HistoryWindow || finishErr != nil || indexErr != nil || !finishOK || !indexOK || !finish.HasBlockHash || !indexed.HasBlockHash {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return nil
	}
	eligible := min(head.Number(), uint64(solid)-m.cfg.HistoryWindow, finish.BlockNum, indexed.BlockNum)
	lastBucket := eligible / rawdb.StateHistoryChunkBucketBlocks
	historyStagingMoverEligibleBucket.Update(int64(lastBucket))
	if lastBucket == 0 {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return nil
	}
	if m.nextBucket > lastBucket {
		historyStagingMoverNextBucket.Update(int64(m.nextBucket))
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return nil
	}
	var bucket uint64
	var existingClaim rawdb.HistoryStagingClaim
	var hasClaim, resumeAbort bool
	for examined := uint64(0); examined < 256 && m.nextBucket <= lastBucket; examined++ {
		candidate := m.nextBucket
		m.nextBucket++
		historyStagingMoverNextBucket.Update(int64(m.nextBucket))
		_, last, _ := rawdb.StateHistoryChunkBucketBounds(candidate)
		if last > eligible {
			continue
		}
		route, present, err := manager.ReadRoute(candidate)
		if err != nil {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return err
		}
		if !present {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return rawdb.ErrHistoryStagingIncomplete
		}
		claim, claimPresent, err := manager.ReadClaim(candidate)
		if err != nil {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return err
		}
		if route.Owner == rawdb.HistoryStagingOwnerSource {
			bucket = candidate
			if claimPresent {
				existingClaim, hasClaim = claim, true
				resumeAbort = claim.Cancelled
			}
			break
		}
	}
	if bucket == 0 {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return nil
	}
	defer func() {
		if retErr != nil {
			m.nextBucket = bucket
			historyStagingMoverNextBucket.Update(int64(bucket))
		}
	}()
	if resumeAbort {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		if err := manager.AbortClaim(ctx, existingClaim, m.cfg.Limits); err != nil {
			return err
		}
		m.nextBucket = bucket // new proof and claim in a later bounded pass
		return nil
	}
	proofEligible := eligible
	if hasClaim {
		proofEligible = existingClaim.Proof.EligibleThrough
		if proofEligible > eligible {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return rawdb.ErrHistoryStagingConflict
		}
	}
	proof, needed, err := m.collectProofLocked(bucket, proofEligible, finish, indexed)
	if err != nil {
		bc.chainmu.Unlock()
		bc.stateHistoryIndexMu.Unlock()
		return err
	}
	if hasClaim {
		if proof.Epoch != existingClaim.Proof.Epoch || !reflect.DeepEqual(proof.Blocks, existingClaim.Proof.Blocks) {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return rawdb.ErrHistoryStagingConflict
		}
		proof = existingClaim.Proof // keep the durable stage/eligible anchors
	}
	var pinned *snapshots.Manager
	var releaseCold func()
	needCold := len(existingClaim.Proof.ColdSpans) > 0
	for _, neededBlock := range needed {
		needCold = needCold || neededBlock
	}
	if needCold && !hasClaim {
		if coldManager == nil {
			bc.chainmu.Unlock()
			bc.stateHistoryIndexMu.Unlock()
			return errors.New("history staging mover: cold manager unavailable")
		}
		pinned, releaseCold, err = coldManager.PinHistoryReadView()
	}
	bc.chainmu.Unlock()
	bc.stateHistoryIndexMu.Unlock()
	if releasePublication != nil {
		releasePublication()
		releasePublication = nil
	}
	if err != nil {
		return err
	}
	if releaseCold != nil {
		defer releaseCold()
	}
	if needCold && !hasClaim {
		if pinned == nil || pinned.Manifest() == nil {
			return errors.New("history staging mover: cold manifest unavailable")
		}
		prover, err := snapshots.NewHistoryStagingColdProver(pinned.HistoryStagingDir(), pinned.Manifest())
		if err != nil {
			return err
		}
		proof.ColdSpans, err = prover.Build(ctx, proof.Blocks, needed)
		if err != nil {
			return err
		}
	}
	if err := rawdb.VerifyHistoryStagingProof(proof); err != nil {
		return err
	}
	historyStagingMoverProofNanos.Inc(time.Since(proofStarted).Nanoseconds())
	if !historyStagingPressureReady(m.cfg.HotPressure(), time.Now()) || !historyStagingPressureReady(m.cfg.StagePressure(), time.Now()) {
		m.nextBucket = bucket
		return nil
	}
	// Qualification and adoption both recheck under index→chain. The expensive
	// checksum and bounded copy never hold either writer lock.
	var claim rawdb.HistoryStagingClaim
	if err := m.withValidatedProof(ctx, proof, func() error {
		var present bool
		var err error
		claim, present, err = manager.ReadClaim(bucket)
		if err != nil {
			return err
		}
		if !present {
			var id [32]byte
			if _, err := rand.Read(id[:]); err != nil {
				return err
			}
			claim, err = manager.BeginClaim(ctx, proof, id)
			return err
		}
		if !reflect.DeepEqual(claim.Proof, proof) {
			return errors.New("history staging mover: existing claim proof changed; requires recovery")
		}
		return nil
	}); err != nil {
		return err
	}
	if hasClaim {
		historyStagingMoverResumedClaims.Inc(1)
	}
	copyStarted := time.Now()
	receipt, err := manager.CopyClaim(ctx, claim, m.cfg.Limits)
	historyStagingMoverCopyNanos.Inc(time.Since(copyStarted).Nanoseconds())
	if err != nil {
		return err
	}
	var binding *rawdb.HistoryStagingColdBinding
	if len(proof.ColdSpans) > 0 {
		if coldManager == nil {
			return errors.New("history staging mover: cold manager unavailable at adoption")
		}
		releaseCurrentPublication, err := snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
		if err != nil {
			return err
		}
		currentPinned, release, err := coldManager.PinHistoryReadView()
		releaseCurrentPublication()
		if err != nil {
			return err
		}
		if release != nil {
			defer release()
		}
		if currentPinned == nil || currentPinned.Manifest() == nil {
			return errors.New("history staging mover: current cold manifest unavailable")
		}
		old, hasOld, err := manager.ReadColdBindingAt(proof.Epoch, proof.Bucket)
		if err != nil {
			return err
		}
		if !hasOld {
			old = rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: proof.Bucket, Epoch: proof.Epoch, BindingEpoch: 1, ManifestEpoch: currentPinned.Manifest().Generation, Spans: proof.ColdSpans}
		}
		candidate, err := snapshots.RebindHistoryStagingColdBinding(ctx, currentPinned.HistoryStagingDir(), currentPinned.Manifest(), old, proof.Blocks)
		if err != nil {
			return err
		}
		if hasOld {
			candidate.BindingEpoch = old.BindingEpoch + 1
		} else {
			candidate.BindingEpoch = 1
		}
		binding = &candidate
	}
	adoptStarted := time.Now()
	// A binding's manifest identity is checked and its durable references are
	// published under one short serving admission. The expensive verification
	// above runs outside this gate against its own pinned immutable files.
	if binding != nil {
		releasePublication, err = snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
		if err != nil {
			return err
		}
	}
	adoptErr := m.withValidatedProof(ctx, proof, func() error {
		if binding != nil {
			coldManager, ok := bc.stateCodeColdHistory.(*snapshots.Manager)
			if !ok || coldManager == nil {
				return rawdb.ErrHistoryStagingConflict
			}
			manifest := coldManager.Manifest()
			if manifest == nil || manifest.Generation != binding.ManifestEpoch {
				return rawdb.ErrHistoryStagingConflict
			}
			if err := manager.CertifyColdRange(ctx, *binding, func() error { return nil }); err != nil {
				return err
			}
		}
		_, err := manager.AdoptClaim(ctx, claim, proof)
		return err
	})
	if releasePublication != nil {
		releasePublication()
		releasePublication = nil
	}
	if adoptErr != nil {
		return adoptErr
	}
	historyStagingMoverAdoptNanos.Inc(time.Since(adoptStarted).Nanoseconds())
	clearStarted := time.Now()
	if err := manager.ClearSource(ctx, claim, m.cfg.Limits); err != nil {
		return err
	}
	historyStagingMoverClearNanos.Inc(time.Since(clearStarted).Nanoseconds())
	historyStagingMoverMoved.Inc(1)
	historyStagingMoverCopiedBytes.Inc(int64(receipt.PayloadBytes))
	historyStagingMoverLastSuccess.Update(time.Now().Unix())
	return m.finalizeColdBucket(ctx, bucket)
}

func historyStagingBindingFull(binding rawdb.HistoryStagingColdBinding, first, last uint64) bool {
	next := first
	for _, span := range binding.Spans {
		if span.To < next {
			continue
		}
		if span.From != next || span.To > last {
			return false
		}
		if span.To == last {
			return true
		}
		next = span.To + 1
	}
	return false
}

// finalizeColdBucket is a separate bounded retirement phase. A crash after
// source clearing or after the COLD route Sync is resumed by the next scan.
func (m *HistoryStagingMover) finalizeColdBucket(ctx context.Context, bucket uint64) error {
	manager := m.bc.HistoryStagingManager()
	route, present, err := manager.ReadRoute(bucket)
	if err != nil || !present || !route.SourceCleared {
		return rawdb.ErrHistoryStagingConflict
	}
	binding, present, err := manager.ReadColdBindingAt(route.Epoch, bucket)
	if err != nil {
		return err
	}
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		return err
	}
	if !present || !historyStagingBindingFull(binding, first, last) {
		return nil
	} // mixed hot/cold bucket stays TARGET
	if route.Owner == rawdb.HistoryStagingOwnerTarget {
		coldManager, ok := m.bc.stateCodeColdHistory.(*snapshots.Manager)
		if !ok || coldManager == nil {
			return rawdb.ErrHistoryStagingIncomplete
		}
		pinned, release, err := coldManager.PinHistoryReadView()
		if err != nil {
			return err
		}
		if release != nil {
			defer release()
		}
		if pinned == nil || pinned.Manifest() == nil {
			return rawdb.ErrHistoryStagingIncomplete
		}
		blocks, err := m.canonicalBucketBlocks(ctx, bucket)
		if err != nil {
			return err
		}
		current, err := snapshots.RebindHistoryStagingColdBinding(ctx, pinned.HistoryStagingDir(), pinned.Manifest(), binding, blocks)
		if err != nil {
			return err
		}
		releasePublication, err := snapshots.AcquireHistoryStagingPublicationRead(ctx, coldManager.HistoryStagingDir())
		if err != nil {
			return err
		}
		defer func() {
			if releasePublication != nil {
				releasePublication()
			}
		}()
		liveManifest := coldManager.Manifest()
		if liveManifest == nil || liveManifest.Generation != current.ManifestEpoch {
			return rawdb.ErrHistoryStagingConflict
		}
		if current.ManifestEpoch != binding.ManifestEpoch || !reflect.DeepEqual(current.Spans, binding.Spans) {
			if err := manager.CertifyColdRange(ctx, current, func() error { return nil }); err != nil {
				return err
			}
			binding = current
		}
		if err := manager.ReleaseTargetToCold(ctx, bucket, func(actual rawdb.HistoryStagingColdBinding) error {
			if actual.BindingEpoch != binding.BindingEpoch || !reflect.DeepEqual(actual.Spans, binding.Spans) {
				return rawdb.ErrHistoryStagingConflict
			}
			return nil // the pinned trio was fully authenticated above
		}); err != nil {
			return err
		}
		releasePublication()
		releasePublication = nil
	}
	if err := manager.ClearColdTarget(ctx, bucket, m.cfg.Limits); err != nil {
		return err
	}
	return nil
}

func (m *HistoryStagingMover) canonicalBucketBlocks(ctx context.Context, bucket uint64) ([]rawdb.HistoryStagingBlockProof, error) {
	bc := m.bc
	bc.stateHistoryIndexMu.Lock()
	defer bc.stateHistoryIndexMu.Unlock()
	if err := lockMutexContext(ctx, &bc.chainmu); err != nil {
		return nil, err
	}
	defer bc.chainmu.Unlock()
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		return nil, err
	}
	blocks := make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks)
	for number := first; number <= last; number++ {
		hash, present := rawdb.ReadBlockHash(bc.chaindb, number)
		if !present {
			return nil, rawdb.ErrHistoryStagingIncomplete
		}
		row, present, err := rawdb.ReadStateTxRange(bc.db, number)
		if err != nil || !present || row.BlockHash != hash {
			return nil, rawdb.ErrHistoryStagingIncomplete
		}
		blocks = append(blocks, rawdb.HistoryStagingBlockProof{Number: number, Hash: hash, BeginTxNum: row.BeginTxNum, EndTxNum: row.EndTxNum})
	}
	return blocks, nil
}

func (m *HistoryStagingMover) collectProofLocked(bucket, eligible uint64, finish, indexed rawdb.StageProgress) (rawdb.HistoryStagingProof, []bool, error) {
	bc := m.bc
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil || bucket == 0 || last > eligible || !bc.buffer.HistoryPrefixSettled(last) {
		return rawdb.HistoryStagingProof{}, nil, rawdb.ErrHistoryStagingIncomplete
	}
	for _, stage := range []rawdb.StageProgress{finish, indexed} {
		hash, present := rawdb.ReadBlockHash(bc.chaindb, stage.BlockNum)
		if !present || hash != stage.BlockHash {
			return rawdb.HistoryStagingProof{}, nil, rawdb.ErrHistoryStagingConflict
		}
	}
	epoch, err := bc.HistoryStagingManager().CurrentEpoch()
	if err != nil {
		return rawdb.HistoryStagingProof{}, nil, err
	}
	proof := rawdb.HistoryStagingProof{Bucket: bucket, Epoch: epoch, EligibleThrough: eligible, FinishBlock: finish.BlockNum, FinishHash: finish.BlockHash, IndexBlock: indexed.BlockNum, IndexHash: indexed.BlockHash, Blocks: make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks)}
	needed := make([]bool, 0, rawdb.StateHistoryChunkBucketBlocks)
	for number := first; number <= last; number++ {
		hash, present := rawdb.ReadBlockHash(bc.chaindb, number)
		if !present {
			return rawdb.HistoryStagingProof{}, nil, fmt.Errorf("history staging mover: canonical block %d missing", number)
		}
		row, present, err := rawdb.ReadStateTxRange(bc.db, number)
		if err != nil || !present || row.BlockHash != hash {
			return rawdb.HistoryStagingProof{}, nil, fmt.Errorf("history staging mover: tx range %d missing or mismatched: %w", number, err)
		}
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{Number: number, Hash: hash, BeginTxNum: row.BeginTxNum, EndTxNum: row.EndTxNum})
		receipt, hasReceipt, err := rawdb.ReadHistoryStagingBlockComplete(bc.db, epoch, number)
		if err != nil {
			return rawdb.HistoryStagingProof{}, nil, err
		}
		needed = append(needed, !hasReceipt || receipt.BlockHash != hash || receipt.BeginTxNum != row.BeginTxNum || receipt.EndTxNum != row.EndTxNum)
	}
	return proof, needed, nil
}

func (m *HistoryStagingMover) withValidatedProof(ctx context.Context, proof rawdb.HistoryStagingProof, action func() error) error {
	bc := m.bc
	bc.stateHistoryIndexMu.Lock()
	defer bc.stateHistoryIndexMu.Unlock()
	if err := lockMutexContext(ctx, &bc.chainmu); err != nil {
		return err
	}
	defer bc.chainmu.Unlock()
	if bc.historyStagingReplayEpoch.Load() != 0 {
		return rawdb.ErrHistoryStagingResetting
	}
	manager := bc.HistoryStagingManager()
	route, present, err := manager.ReadRoute(proof.Bucket)
	if err != nil || !present || route.Epoch != proof.Epoch || route.Owner != rawdb.HistoryStagingOwnerSource {
		return rawdb.ErrHistoryStagingConflict
	}
	for _, anchor := range []struct {
		number uint64
		hash   [32]byte
	}{{proof.FinishBlock, proof.FinishHash}, {proof.IndexBlock, proof.IndexHash}} {
		canonical, present := rawdb.ReadBlockHash(bc.chaindb, anchor.number)
		if !present || canonical != anchor.hash {
			return rawdb.ErrHistoryStagingConflict
		}
	}
	solid := bc.cachedDynProps().LatestSolidifiedBlockNum()
	head := bc.CurrentBlock()
	if head == nil {
		return rawdb.ErrHistoryStagingIncomplete
	}
	finish, finishOK, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageFinish)
	if err != nil || !finishOK {
		return rawdb.ErrHistoryStagingIncomplete
	}
	indexed, indexOK, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err != nil || !indexOK || solid <= 0 || uint64(solid) <= m.cfg.HistoryWindow {
		return rawdb.ErrHistoryStagingIncomplete
	}
	eligible := min(head.Number(), uint64(solid)-m.cfg.HistoryWindow, finish.BlockNum, indexed.BlockNum)
	if eligible < proof.EligibleThrough || finish.BlockNum < proof.FinishBlock || indexed.BlockNum < proof.IndexBlock {
		return rawdb.ErrHistoryStagingConflict
	}
	current, needed, err := m.collectProofLocked(proof.Bucket, proof.EligibleThrough, finish, indexed)
	if err != nil || !reflect.DeepEqual(current.Blocks, proof.Blocks) || current.Epoch != proof.Epoch {
		return rawdb.ErrHistoryStagingConflict
	}
	_ = needed
	return action()
}
