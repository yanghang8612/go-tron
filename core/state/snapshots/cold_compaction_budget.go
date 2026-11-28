package snapshots

import (
	"context"
	"math"
	"os"
	"sort"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

const (
	busyHistoryCompactionInputBytes          = uint64(128 << 20)
	busyHistoryCompactionLogicalBytes        = uint64(512 << 20)
	busyHistoryCompactionInputRecords        = uint64(400_000)
	busyHistoryCompactionSources             = uint64(8)
	busyHistoryCompactionNoCandidateRetry    = 30 * time.Second
	busyEventLogCompactionMaxSourceMainBytes = uint64(128 << 20)
	eventLogCompactionFairShareInterval      = 1
	heavyRoutingCompactionFairShareInterval  = 1
)

// Protected by Runner.passMu. A merge recovery belongs to optional compaction,
// independently of the deadline for publishing and pruning new cold history.
type historyCompactionBudgetState struct {
	notBefore                  time.Time
	pending                    bool
	pendingAt                  time.Time
	inputCosts                 *historyCompactionCostCache
	outerMetrics               *historyCompactionOuterMetrics
	routingRetiredCursor       retiredMetadataSweepCursor
	systemRouting              systemRoutingRefreshState
	preferMetadata             bool
	nonEventCompactionTurns    int
	heavyRoutingCompactionTurns int
	eventLogCompactHintBlk     uint64
	eventLogCompactIdleGen     uint64
	eventLogCompactLastBusy    bool
}

// Consume one priority opportunity, never a history admission deadline. Without
// this handoff a three-second history recovery could win the shared gate before
// the pending merge on every wake. The caller skips one build opportunity and
// calls compactHistory under the same pass lock; that attempt either makes
// progress or clears/re-arms its own bounded deferral.
func (r *Runner) shouldPrioritizePendingCompaction(now time.Time) bool {
	if r == nil {
		return false
	}
	s := &r.compactionBudget
	if !r.throughputCatchup() {
		s.pending = false
		return false
	}
	if !s.pending || now.Before(s.pendingAt) || now.Before(s.notBefore) || !r.historyLoadPermitsMerge() {
		return false
	}
	s.pending = false
	return true
}

// This readiness hint uses only the already published manifest. No checksum,
// accessor header or compressed body read is allowed before the heavy lease.
// Waiting for the source target can leave a small record/logical-limited run
// pending briefly, but avoids prioritizing an empty merge for every two leaves.
func (r *Runner) hasPotentialBusyCompaction() (bool, error) {
	manifest, err := loadProductionManifestReadOnly(r.cfg.Dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cfg, ok := DefaultDomainRegistry().Dataset(r.cfg.HistoryDataset)
	maxSources := busyHistoryCompactionSources
	if r.cfg.CompactMaxSteps > 0 {
		maxSources = min(maxSources, r.cfg.CompactMaxSteps)
	}
	if ok && maxSources >= 2 {
		var sources, bytes uint64
		var previous SegmentRef
		for _, candidate := range filterHistoryProofProtectedCandidates(r.cfg.Dir, historyCompactionCandidates(manifest, cfg)) {
			if candidate.history.effectiveAggregationSteps() != 1 {
				sources, bytes = 0, 0
				continue
			}
			if sources > 0 && !historySegmentsAreContiguous(previous, candidate.history) {
				sources, bytes = 0, 0
			}
			size := candidate.history.Size
			for _, companion := range candidate.companions {
				if companion.Size > math.MaxUint64-size {
					size = math.MaxUint64
					break
				}
				size += companion.Size
			}
			if size > busyHistoryCompactionInputBytes-bytes {
				if sources >= 2 {
					return true, nil
				}
				sources, bytes = 0, 0
			}
			if size > busyHistoryCompactionInputBytes {
				continue
			}
			sources++
			bytes += size
			previous = candidate.history
			if sources >= 2 && (sources >= maxSources || bytes == busyHistoryCompactionInputBytes) {
				return true, nil
			}
		}
	}
	return r.hasPotentialEventLogCompaction(manifest, true), nil
}

func (r *Runner) syncEventLogCompactionMode(busy bool) {
	if r.compactionBudget.eventLogCompactLastBusy != busy {
		r.compactionBudget.eventLogCompactIdleGen = 0
		if !busy {
			// Transitioning from busy (128 MiB cap) to idle (256 MiB cap) can
			// settle earlier partial merges into full 65k-block segments.
			r.compactionBudget.eventLogCompactHintBlk = 0
		}
		r.compactionBudget.eventLogCompactLastBusy = busy
	}
}

func (r *Runner) effectiveEventLogCompactionConfig(busy bool) EventLogCompactionConfig {
	evCfg := r.cfg.EventLogCompaction.withDefaults()
	evCfg.RetainObsoleteSegments = r.cfg.RetainObsoleteSegments
	if evCfg.VerificationCache == nil {
		evCfg.VerificationCache = r.cfg.ColdChainVerificationCache
	}
	if evCfg.MinFromBlockHint == 0 {
		evCfg.MinFromBlockHint = r.compactionBudget.eventLogCompactHintBlk
	}
	if busy && evCfg.MaxSourceMainBytes > busyEventLogCompactionMaxSourceMainBytes {
		evCfg.MaxSourceMainBytes = busyEventLogCompactionMaxSourceMainBytes
	}
	return evCfg
}

func (r *Runner) hasPotentialEventLogCompaction(manifest *Manifest, busy bool) bool {
	if !r.cfg.CompactEventLogs || !r.cfg.BuildEventLogs || manifest == nil {
		return false
	}
	sameMode := r.compactionBudget.eventLogCompactLastBusy == busy
	if sameMode && r.compactionBudget.eventLogCompactIdleGen == manifest.Generation {
		return false
	}
	evCfg := r.effectiveEventLogCompactionConfig(busy)
	if !sameMode && !busy {
		evCfg.MinFromBlockHint = 0
	}
	evRefs := eventLogRefs(manifest)
	startIdx := 0
	if evCfg.MinFromBlockHint > 0 {
		startIdx = sort.Search(len(evRefs), func(i int) bool {
			return evRefs[i].FromTxNum >= evCfg.MinFromBlockHint
		})
	}
	var runStart, prevEnd, runBytes uint64
	var runCount int
	for _, ref := range evRefs[startIdx:] {
		if ref.AggregationSteps == 0 {
			return true
		}
		span := ref.ToTxNum - ref.FromTxNum + 1
		if ref.ToTxNum-ref.FromTxNum == math.MaxUint64 {
			span = math.MaxUint64
		}
		if span >= evCfg.TargetBlocks || ref.Size >= evCfg.MaxSourceMainBytes/2 {
			runCount = 0
			continue
		}
		if runCount == 0 || prevEnd == math.MaxUint64 || ref.FromTxNum != prevEnd+1 ||
			ref.ToTxNum-runStart >= evCfg.TargetBlocks || ref.Size > evCfg.MaxSourceMainBytes-runBytes {
			runStart, prevEnd, runBytes, runCount = ref.FromTxNum, ref.ToTxNum, ref.Size, 1
			continue
		}
		prevEnd = ref.ToTxNum
		runBytes += ref.Size
		runCount++
		if runCount >= evCfg.MinV5MergeSegments || ref.ToTxNum-runStart+1 >= evCfg.TargetBlocks/2 || runBytes >= evCfg.MaxSourceMainBytes/2 {
			return true
		}
	}
	return false
}

func (r *Runner) tryCompactEventLogs(ctx context.Context, busy bool) (EventLogCompactionResult, bool, error) {
	if !r.cfg.CompactEventLogs || !r.cfg.BuildEventLogs {
		return EventLogCompactionResult{}, false, nil
	}
	r.syncEventLogCompactionMode(busy)
	if r.compactionBudget.eventLogCompactIdleGen != 0 {
		manifest, err := loadProductionManifestReadOnly(r.cfg.Dir)
		if err == nil && manifest.Generation == r.compactionBudget.eventLogCompactIdleGen {
			r.compactionBudget.nonEventCompactionTurns = 0
			return EventLogCompactionResult{Generation: manifest.Generation}, false, nil
		}
	}
	evCfg := r.effectiveEventLogCompactionConfig(busy)
	evRes, evErr := CompactEventLogsContext(ctx, r.cfg.Dir, evCfg)
	if evErr != nil {
		return EventLogCompactionResult{}, false, evErr
	}
	if evRes.Compacted {
		r.compactionBudget.eventLogCompactIdleGen = 0
		r.compactionBudget.nonEventCompactionTurns = 0
		resolved := evCfg.withDefaults()
		span := evRes.ToBlock - evRes.FromBlock + 1
		if evRes.ToBlock-evRes.FromBlock == math.MaxUint64 {
			span = math.MaxUint64
		}
		if span >= resolved.TargetBlocks || evRes.NewMainBytes >= resolved.MaxSourceMainBytes/2 {
			if evRes.ToBlock < math.MaxUint64 {
				r.compactionBudget.eventLogCompactHintBlk = evRes.ToBlock + 1
			} else {
				r.compactionBudget.eventLogCompactHintBlk = evRes.ToBlock
			}
		} else {
			r.compactionBudget.eventLogCompactHintBlk = evRes.FromBlock
		}
		coldSnapshotLog.Info("Compacted cold event-log segments",
			"fromBlock", evRes.FromBlock,
			"toBlock", evRes.ToBlock,
			"sources", evRes.SourceSegments,
			"beforeBytes", evRes.SourceMainBytes+evRes.SourceIndexBytes,
			"afterBytes", evRes.NewMainBytes+evRes.NewIndexBytes,
			"savedBytes", evRes.BytesSaved,
			"deletedFiles", evRes.DeletedFiles,
			"retiredTrimmed", evRes.RetiredTrimmed,
			"busy", busy)
		return evRes, true, nil
	}
	if evRes.Generation > 0 {
		r.compactionBudget.eventLogCompactIdleGen = evRes.Generation
	}
	r.compactionBudget.nonEventCompactionTurns = 0
	return evRes, false, nil
}

func historyCompactionRecovery(work time.Duration, failed bool) time.Duration {
	recovery := time.Duration(math.MaxInt64)
	if work <= time.Duration(math.MaxInt64)/4 {
		recovery = max(3*time.Second, work*4)
	}
	if failed {
		recovery = max(recovery, time.Minute)
	}
	return recovery
}

func (r *Runner) accountRoutingHealthy(now time.Time) bool {
	p := r.historyLoad.sample
	return r.historyLoad.level == 3 && r.historyLoad.reasons == 0 && r.historyLoad.cpuBurstReady(now) &&
		p.L0CompactionThreshold > 0 && p.L0Sublevels < p.L0CompactionThreshold
}

// This is an allowance for a busy, already admitted optional merge. The
// independent metadata probe and CPU/memory check run only when a dictionary
// would exceed the ordinary budget. They never open or extend the heavy lease.
func (r *Runner) accountRoutingExpansion(busy bool) func() bool {
	if !busy || r.cfg.HistoryRecoveryLoadProbe == nil || r.cfg.BusyHistoryBuildReady == nil ||
		!r.accountRoutingHealthy(time.Now()) {
		return nil
	}
	return func() bool {
		p := r.cfg.HistoryRecoveryLoadProbe()
		r.refreshHistoryLoadFromProbe(time.Now(), func() maintenance.StoragePressure { return p })
		return r.accountRoutingHealthy(time.Now()) && r.cfg.BusyHistoryBuildReady() &&
			r.accountRoutingHealthy(time.Now())
	}
}

func (r *Runner) refreshHeavyHistoryRouting(ctx context.Context, busy bool, routingLimits accountRoutingRefreshLimits) (built bool, expansionDenied bool, err error) {
	r.compactionBudget.systemRouting.initMetrics(r.cfg.MetricsNamespace)
	built, err = refreshSystemHistoryRoutingWithStateContext(ctx, r.cfg.Dir, busy, &r.compactionBudget.systemRouting, time.Now())
	if err != nil || built {
		return built, false, err
	}
	permitExtra := r.accountRoutingExpansion(busy)
	if permitExtra != nil {
		check := permitExtra
		permitExtra = func() bool {
			expansionDenied = !check()
			return !expansionDenied
		}
	}
	built, err = refreshAccountHistoryRoutingWithExpansionContext(ctx, r.cfg.Dir, busy, permitExtra, routingLimits)
	if err != nil {
		return false, expansionDenied, err
	}
	if built {
		return true, false, nil
	}
	return false, expansionDenied, nil
}

func (r *Runner) compactHistory(ctx context.Context, catchingUp bool) (HistoryCompactionResult, error) {
	return r.compactHistoryWithRoutingLimits(ctx, catchingUp, defaultAccountRoutingRefreshLimits())
}

func (r *Runner) compactHistoryWithRoutingLimits(ctx context.Context, catchingUp bool, routingLimits accountRoutingRefreshLimits) (total HistoryCompactionResult, err error) {
	if r == nil || !r.cfg.Enabled {
		return total, nil
	}
	if err := contextError(ctx); err != nil {
		return total, err
	}
	r.compactionBudget.pending = false
	// Network sync can briefly become idle while cold history is still far
	// behind. That transition must not admit an unbounded catch-up merge.
	busy := r.throughputCatchup() && (catchingUp || r.historySyncBudgetActive())
	if busy && !r.historyLoadPermitsMerge() {
		if !r.historyLoad.hard && !time.Now().Before(r.compactionBudget.notBefore) {
			potential, err := r.hasPotentialBusyCompaction()
			if err != nil {
				return total, err
			}
			if potential {
				r.compactionBudget.pending = true
				r.compactionBudget.pendingAt = r.compactionBudget.notBefore
			}
		}
		return HistoryCompactionResult{Deferred: true, DeferReason: "import-load"}, nil
	}
	now := time.Now()
	if now.Before(r.compactionBudget.notBefore) {
		return HistoryCompactionResult{Deferred: true, DeferReason: "merge-recovery", RetryAfter: r.compactionBudget.notBefore.Sub(now), RetryDeadline: r.compactionBudget.notBefore}, nil
	}
	// Compaction runs after the builder's lease has been released. It must take
	// its own lease instead of overlapping a freezer job that won that window.
	var release func()
	var admitted bool
	if busy {
		release, admitted = r.cfg.HeavyWorkGate.TryAcquireWithCooldown(3 * time.Second)
	} else {
		release, admitted = r.cfg.HeavyWorkGate.TryAcquire()
	}
	if !admitted {
		if busy {
			potential, err := r.hasPotentialBusyCompaction()
			if err != nil {
				return total, err
			}
			if !potential {
				return HistoryCompactionResult{Deferred: true, DeferReason: "leaf-target"}, nil
			}
		}
		retry := r.cfg.HeavyWorkGate.CooldownRemaining()
		if retry <= 0 {
			retry = 3 * time.Second
		}
		deadline := time.Now().Add(retry)
		if busy {
			r.compactionBudget.pending = true
			r.compactionBudget.pendingAt = deadline
		}
		return HistoryCompactionResult{Deferred: true, DeferReason: "heavy-work-gate", RetryAfter: retry, RetryDeadline: deadline}, nil
	}
	defer release()
	started := time.Now()
	defer func() {
		if busy && (total.Merged || total.RoutingBuilt || total.EventLogsCompacted || err != nil || total.Deferred) {
			total.Recovery = historyCompactionRecovery(time.Since(started), err != nil)
			if err == nil && !total.Merged && !total.RoutingBuilt && !total.EventLogsCompacted && total.DeferReason != "import-load" {
				// The cheap manifest hint cannot see oversized logical values or
				// record counts. Do not prioritize the same empty selection after
				// every newly published leaf when the detailed budget rejects it.
				total.Recovery = busyHistoryCompactionNoCandidateRetry
			}
			r.compactionBudget.notBefore = historyRecoveryDeadline(time.Now(), total.Recovery)
			total.RetryAfter = total.Recovery
			total.RetryDeadline = r.compactionBudget.notBefore
		}
	}()
	// Fair-share turn: give cold event-log compaction a periodic slot even while
	// state-history routing refreshes and leaf merges have pending backlog.
	if r.cfg.CompactEventLogs && r.cfg.BuildEventLogs && r.compactionBudget.nonEventCompactionTurns >= eventLogCompactionFairShareInterval {
		evRes, compacted, evErr := r.tryCompactEventLogs(ctx, busy)
		if evErr != nil {
			return total, evErr
		}
		if compacted {
			total.Dataset = r.cfg.HistoryDataset
			total.EventLogsCompacted = true
			total.EventLogCompaction = evRes
			return total, nil
		}
	}
	// The small singleton routing index removes cross-segment negative reads
	// without rewriting canonical rows or enlarging streaming merge budgets.
	deferHeavyRouting := r.compactionBudget.heavyRoutingCompactionTurns >= heavyRoutingCompactionFairShareInterval
	if r.cfg.HistoryDataset == SegmentDatasetStateDomainChange {
		if !r.cfg.RetainObsoleteSegments {
			pruned, pruneErr := pruneRetiredHistoryRoutingContext(ctx, r.cfg.Dir, r.compactionBudget.routingRetiredCursor)
			if pruneErr != nil {
				return total, pruneErr
			}
			r.compactionBudget.routingRetiredCursor = pruned.Next
		}
		// Alternate the two lightweight families. Returning after every
		// successful small batch used to starve metadata until all sources
		// were covered, leaving expensive query fallback for hours.
		small := func() (bool, error) { return refreshSmallSystemHistoryRoutingContext(ctx, r.cfg.Dir, busy) }
		metadata := func() (bool, error) { return refreshMetadataHistoryRoutingContext(ctx, r.cfg.Dir, busy) }
		families := []func() (bool, error){small, metadata}
		if r.compactionBudget.preferMetadata {
			families[0], families[1] = families[1], families[0]
		}
		for _, refresh := range families {
			built, routeErr := refresh()
			if routeErr != nil {
				return total, routeErr
			}
			if built {
				r.compactionBudget.preferMetadata = !r.compactionBudget.preferMetadata
				r.compactionBudget.nonEventCompactionTurns++
				return HistoryCompactionResult{Dataset: SegmentDatasetStateDomainChange, RoutingBuilt: true}, nil
			}
		}
		if !deferHeavyRouting {
			built, expansionDenied, routeErr := r.refreshHeavyHistoryRouting(ctx, busy, routingLimits)
			if routeErr != nil {
				return total, routeErr
			}
			if built {
				r.compactionBudget.heavyRoutingCompactionTurns++
				r.compactionBudget.nonEventCompactionTurns++
				return HistoryCompactionResult{Dataset: SegmentDatasetStateDomainChange, RoutingBuilt: true}, nil
			}
			if expansionDenied {
				// A fresh storage or CPU/memory rejection supersedes this pass's
				// initial admission. Do not start a different heavy job.
				return HistoryCompactionResult{Deferred: true, DeferReason: "import-load"}, nil
			}
		}
	}
	if r.compactionBudget.outerMetrics == nil {
		r.compactionBudget.outerMetrics = newHistoryCompactionOuterMetrics(r.cfg.MetricsNamespace)
	}
	cfg := CompactionConfig{outerMetrics: r.compactionBudget.outerMetrics, ReferenceContainer: r.cfg.HistoryReferenceContainer, MaxSteps: r.cfg.CompactMaxSteps, DeleteObsolete: !r.cfg.RetainObsoleteSegments}
	drain := !catchingUp
	if busy {
		cfg.BusyLeafOnly = true
		cfg.MaxInputBytes = busyHistoryCompactionInputBytes
		cfg.MaxInputLogicalBytes = busyHistoryCompactionLogicalBytes
		cfg.MaxInputRecords = busyHistoryCompactionInputRecords
		cfg.MaxSources = busyHistoryCompactionSources
		if !cfg.ReferenceContainer {
			if r.compactionBudget.inputCosts == nil {
				r.compactionBudget.inputCosts = newHistoryCompactionCostCache()
			}
			cfg.inputCostCache = r.compactionBudget.inputCosts
		}
		drain = false
	} else if catchingUp {
		cfg.MinSteps = r.cfg.CompactMaxSteps
	}
	for {
		if err := contextError(ctx); err != nil {
			return total, err
		}
		result, err := CompactHistoryDomainContext(ctx, r.cfg.Dir, r.cfg.HistoryDataset, cfg)
		if err != nil {
			return total, err
		}
		if !result.Merged {
			if !total.Merged && !total.RoutingBuilt {
				if deferHeavyRouting && r.cfg.HistoryDataset == SegmentDatasetStateDomainChange {
					r.compactionBudget.heavyRoutingCompactionTurns = 0
					built, expansionDenied, routeErr := r.refreshHeavyHistoryRouting(ctx, busy, routingLimits)
					if routeErr != nil {
						return total, routeErr
					}
					if built {
						r.compactionBudget.nonEventCompactionTurns++
						return HistoryCompactionResult{Dataset: SegmentDatasetStateDomainChange, RoutingBuilt: true}, nil
					}
					if expansionDenied {
						return HistoryCompactionResult{Deferred: true, DeferReason: "import-load"}, nil
					}
				}
				evRes, compacted, evErr := r.tryCompactEventLogs(ctx, busy)
				if evErr != nil {
					return total, evErr
				}
				if compacted {
					total.EventLogsCompacted = true
					total.EventLogCompaction = evRes
				}
			}
			if !total.Merged {
				if total.EventLogsCompacted {
					total.DeferReason = ""
					return total, nil
				}
				total = result
			}
			return total, nil
		}
		r.compactionBudget.heavyRoutingCompactionTurns = 0
		r.compactionBudget.nonEventCompactionTurns++
		mergeHistoryCompactionResult(&total, result)
		if !drain || r.cfg.MaxCompactionPasses > 0 && uint64(total.MergePasses) >= r.cfg.MaxCompactionPasses {
			return total, nil
		}
	}
}
