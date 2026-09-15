package snapshots

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// HistoryReadOptions is local to one complete history trio. Zero keeps the
// original serial reader and automatic codec topology. Enhanced reads retain
// one serial record/CDC consumer and never run beside the event builder.
type HistoryReadOptions struct {
	Workers    int
	ChunkCache bool
}

func (o HistoryReadOptions) Validate() error {
	if o.Workers != 0 && o.Workers != 2 && o.Workers != 4 && o.Workers != 8 {
		return errors.New("snapshots: history shared-read workers must be 0, 2, 4 or 8")
	}
	return nil
}
func (o HistoryReadOptions) enabled() bool { return o.Workers != 0 || o.ChunkCache }
func (o HistoryReadOptions) compressionWorkers() int {
	if o.enabled() {
		return 1
	}
	return 0
}

// HistoryReadResources is a fresh, immutable capacity observation. Available
// requires an audited CPU/quota/affinity sample; it does not grant a maintenance
// lease or prove storage readiness. Headroom is an admission target, not RSS.
type HistoryReadResources struct {
	Available            bool
	SampledAt            time.Time
	IdleCoresMilli       uint64
	MemoryAvailableBytes uint64
}

const historyReadMinimumHeadroom = uint64(2 << 30)
const historyReadResourceMaxAge = 15 * time.Second

type historyReadFallback uint8

const (
	historyReadEnabled historyReadFallback = iota
	historyReadDisabled
	historyReadReservedForcedBusy
	historyReadNoLease
	historyReadStoragePressure
	historyReadResourcesUnknown
	historyReadResourcesStale
	historyReadMemoryLow
	historyReadCPULow
)

func (r *Runner) selectHistoryReadOptions(forcedBusy bool, now time.Time) (HistoryReadOptions, historyReadFallback) {
	requested := HistoryReadOptions{Workers: r.cfg.HistorySharedReadWorkers, ChunkCache: r.cfg.HistorySharedChunkCache}
	none := HistoryReadOptions{}
	if !requested.enabled() {
		return none, historyReadDisabled
	}
	// Forced busy describes import scheduling, not resource pressure. Its
	// existing bounded batch/recovery stays authoritative; cap this optional
	// internal topology at four while still requiring all resource evidence.
	if forcedBusy && requested.Workers > 4 {
		requested.Workers = 4
	}
	if r.cfg.HeavyWorkGate == nil {
		return none, historyReadNoLease
	}
	if !r.historyLoad.cpuBurstReady(now) {
		return none, historyReadStoragePressure
	}
	if r.cfg.HistoryReadResourceProbe == nil {
		return none, historyReadResourcesUnknown
	}
	resources := r.cfg.HistoryReadResourceProbe()
	if !resources.Available {
		return none, historyReadResourcesUnknown
	}
	age := now.Sub(resources.SampledAt)
	if resources.SampledAt.IsZero() || age < -time.Second || age > historyReadResourceMaxAge {
		return none, historyReadResourcesStale
	}
	requiredMemory := historyReadMinimumHeadroom
	if requested.Workers != 0 {
		requiredMemory += rawdb.StateHistoryPipelineDecodedBudget
	}
	if requested.ChunkCache {
		requiredMemory += rawdb.StateHistoryChunkCachePayloadBudget
	}
	if resources.MemoryAvailableBytes < requiredMemory {
		return none, historyReadMemoryLow
	}
	procs := runtime.GOMAXPROCS(0)
	fits := func(workers int) bool {
		return procs >= workers+1 && resources.IdleCoresMilli >= uint64(workers+1)*1000
	}
	if requested.Workers == 0 {
		if fits(0) {
			return requested, historyReadEnabled
		}
	} else {
		for _, workers := range []int{8, 4, 2} {
			if workers <= requested.Workers && fits(workers) {
				requested.Workers = workers
				return requested, historyReadEnabled
			}
		}
	}
	return none, historyReadCPULow
}

// buildStateHistoryReadContext owns one pinned view (or one explicit cache
// session) until both passes, codec workers and finalization have joined. A
// snapshot close failure is a build failure and therefore prevents publication.
func buildStateHistoryReadContext(ctx context.Context, db ethdb.Iteratee, dir string, ref SegmentRef, blockRange *stateDomainChangeHistoryBlockRange, cfg DomainCfg, opts etl.Options, format string, reads HistoryReadOptions, compressionWorkers int) (refs []SegmentRef, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = reads.Validate(); err != nil {
		return nil, err
	}
	if compressionWorkers != 0 && compressionWorkers != 1 {
		return nil, errors.New("snapshots: history compression workers must be 0 or 1")
	}
	if db == nil || blockRange == nil || blockRange.to < blockRange.from || ref.ToTxNum < ref.FromTxNum || !isStateDomainChangeBinarySegmentPath(ref.Path) {
		return nil, errors.New("snapshots: invalid bounded history read source/range/path")
	}
	var view rawdb.StateHistoryReadView
	var release func() error
	if reads.ChunkCache {
		var cache *rawdb.StateHistoryChunkCache
		view, cache, err = rawdb.AcquireStateHistoryChunkCacheView(ctx, db)
		if err != nil {
			return nil, err
		}
		release = cache.Close
	} else {
		view, release, err = rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return nil, err
		}
	}
	defer func() { err = errors.Join(err, release()) }()
	if cfg.IterateHotHistoryBlockTxBorrowed == nil || cfg.IterateHotHistoryTxRangeBorrowed == nil {
		return nil, fmt.Errorf("snapshots: bounded history reader hooks missing for %s", cfg.Dataset)
	}
	changes := cfg.IterateHotHistoryBlockTxBorrowed
	if reads.Workers != 0 {
		changes = func(db ethdb.Iteratee, fromBlock, toBlock, fromTx, toTx uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
			return rawdb.IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx, db, fromBlock, toBlock, fromTx, toTx, reads.Workers, fn)
		}
	}
	cfg.IterateHotHistoryBlockTxBorrowed = func(db ethdb.Iteratee, fromBlock, toBlock, fromTx, toTx uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
		return changes(db, fromBlock, toBlock, fromTx, toTx, func(row *rawdb.StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return fn(row)
		})
	}
	ranges := cfg.IterateHotHistoryTxRangeBorrowed
	cfg.IterateHotHistoryTxRangeBorrowed = func(db ethdb.Iteratee, from, to uint64, fn func(*rawdb.StateTxRange) (bool, error)) error {
		return ranges(db, from, to, func(row *rawdb.StateTxRange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return fn(row)
		})
	}
	result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextExecution(ctx, view, dir, ref, cfg, opts, blockRange, format, compressionWorkers)
	return result.refs, errors.Join(err, ctx.Err())
}

// Current values identify the last admitted history attempt, including failures.
// Active distinguishes an in-flight attempt; published records the completed
// manifest/stage result. Deferred passes never erase the last actual topology.
type historyReadExecutionMetrics struct {
	values              map[string]*metrics.Gauge
	attempts, published *metrics.Counter
}

func newHistoryReadExecutionMetrics(namespace string) historyReadExecutionMetrics {
	prefix := normalizeColdSnapshotMetricNamespace(namespace) + "history/shared_read/"
	m := historyReadExecutionMetrics{values: make(map[string]*metrics.Gauge), attempts: metrics.GetOrRegisterCounter(prefix+"attempts", nil), published: metrics.GetOrRegisterCounter(prefix+"published", nil)}
	for _, name := range []string{"active", "workers", "chunk_cache", "codec_workers", "fallback_reason", "from_block", "to_block", "last_published"} {
		m.values[name] = metrics.GetOrRegisterGauge(prefix+"last/"+name, nil)
	}
	return m
}
func (m historyReadExecutionMetrics) begin(reads HistoryReadOptions, reason historyReadFallback, from, to uint64) {
	if m.values == nil {
		return
	}
	for k, v := range map[string]int64{"active": 1, "workers": int64(reads.Workers), "chunk_cache": boolGauge(reads.ChunkCache), "codec_workers": int64(reads.compressionWorkers()), "fallback_reason": int64(reason), "from_block": int64(from), "to_block": int64(to), "last_published": 0} {
		m.values[k].Update(v)
	}
	m.attempts.Inc(1)
}
func (m historyReadExecutionMetrics) finish(published bool) {
	if m.values == nil {
		return
	}
	m.values["active"].Update(0)
	m.values["last_published"].Update(boolGauge(published))
	if published {
		m.published.Inc(1)
	}
}
