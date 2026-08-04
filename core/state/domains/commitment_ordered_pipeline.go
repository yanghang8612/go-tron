package domains

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var ErrOrderedCommitmentPipelineClosed = errors.New("domains: ordered commitment pipeline closed")

// OrderedCommitmentResult is the completion of one block submitted to an
// OrderedCommitmentPipeline. Results are delivered in submission-independent
// channels; the caller retains responsibility for publishing them in block
// order.
type OrderedCommitmentResult struct {
	Root common.Hash
	Err  error
}

// OrderedCommitmentPipeline is the block-to-block counterpart of Erigon's
// StreamingCommitter split scheduler. Each first-nibble lane is permanently
// owned by one goroutine and processes blocks in order, while different lanes
// may advance into the next block as soon as their own parent lane is ready.
// This removes the per-block 16-way join/restart bubble without ever allowing
// one nibble to observe a future version of itself.
//
// The caller must submit blocks in canonical order and keep every supplied DB
// alive until its result arrives. DBs must support concurrent disjoint-prefix
// reads/writes (the production blockbuffer LayerView does). Close is valid only
// after all returned result channels have completed.
type OrderedCommitmentPipeline struct {
	lanes     [maxFoldNibbles]chan orderedCommitmentLaneTask
	laneWG    sync.WaitGroup
	failed    atomic.Pointer[error]
	closed    atomic.Bool
	inflight  atomic.Int64
	closeOnce sync.Once
}

type orderedCommitmentLaneTask struct {
	job *orderedCommitmentJob
	nb  uint8
	ops []op
}

type orderedCommitmentJob struct {
	store        *rawdbBranchStore
	root         BranchData
	ops          *[]op
	stats        *commitmentFoldStats
	siblingStats commitmentSiblingFoldStats
	activeSplits int
	changed      atomic.Bool
	err          atomic.Pointer[error]
	done         sync.WaitGroup
	result       chan OrderedCommitmentResult
}

// NewOrderedCommitmentPipeline verifies the currently visible persisted root
// once, then seeds all 16 lane owners from that root branch. The production
// commit worker constructs it after one ordinary fold has completed, retaining
// the existing rebuild/snapshot-repair path for startup and corruption checks.
func NewOrderedCommitmentPipeline(db CommitmentDB) (*OrderedCommitmentPipeline, error) {
	if db == nil {
		return nil, ErrNilCommitmentStore
	}
	storedRoot, rootOK, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return nil, err
	}
	store := newRawdbBranchStore(db)
	var root BranchData
	hasRoot, err := store.GetBranchInto(nil, &root)
	if err != nil {
		return nil, err
	}
	if rootOK {
		if storedRoot == (common.Hash{}) {
			if hasRoot {
				return nil, errors.New("domains: zero commitment root has a persisted root branch")
			}
		} else if !hasRoot {
			return nil, errors.New("domains: commitment root branch missing")
		} else if derived := rootHash(&root); derived != storedRoot {
			return nil, fmt.Errorf("domains: commitment root mismatch: stored %x derived %x", storedRoot, derived)
		}
	} else if hasRoot {
		return nil, errors.New("domains: commitment root row missing")
	}

	p := new(OrderedCommitmentPipeline)
	for nb := range p.lanes {
		p.lanes[nb] = make(chan orderedCommitmentLaneTask, 16)
		var laneRoot BranchData
		copyCommitmentLane(&laneRoot, &root, uint8(nb))
		p.laneWG.Add(1)
		go p.runLane(uint8(nb), laneRoot, p.lanes[nb])
	}
	commitmentPipelineEnabledGauge.Update(1)
	return p, nil
}

// Submit starts one ordered block fold. updates may share backing with the
// caller but must remain immutable until the returned channel yields.
func (p *OrderedCommitmentPipeline) Submit(db CommitmentDB, updates []rawdb.StateCommitmentUpdate) <-chan OrderedCommitmentResult {
	if p == nil || p.closed.Load() {
		return completedOrderedCommitmentResult(OrderedCommitmentResult{Err: ErrOrderedCommitmentPipelineClosed})
	}
	if failed := p.failed.Load(); failed != nil {
		return completedOrderedCommitmentResult(OrderedCommitmentResult{Err: *failed})
	}
	if db == nil {
		return completedOrderedCommitmentResult(OrderedCommitmentResult{Err: ErrNilCommitmentStore})
	}
	if _, ok := db.(interface{ ConcurrentReadWriteSafe() }); !ok {
		return completedOrderedCommitmentResult(OrderedCommitmentResult{
			Err: errors.New("domains: ordered commitment pipeline requires a concurrent layer store"),
		})
	}

	updates = rawdb.CoalesceStateCommitmentUpdates(updates)
	stats := beginCommitmentFoldStats(len(updates))
	hasher := borrowKeccak()
	ops, err := buildOpsWithHasher(updates, hasher)
	returnKeccak(hasher)
	if err != nil {
		finishCommitmentFoldStats(stats, true)
		return completedOrderedCommitmentResult(OrderedCommitmentResult{Err: err})
	}
	job := &orderedCommitmentJob{
		store:  newRawdbBranchStore(db),
		ops:    ops,
		stats:  stats,
		result: make(chan OrderedCommitmentResult, 1),
	}
	if ops != nil {
		stats.resolvedOps = uint64(len(*ops))
	}
	job.store.readParentBranches = true
	if err := job.store.beginParentRead(); err != nil {
		p.setFailed(err)
		if ops != nil {
			returnOpsBuf(ops)
		}
		finishCommitmentFoldStats(stats, true)
		job.result <- OrderedCommitmentResult{Err: err}
		close(job.result)
		return job.result
	}

	var starts, counts [maxFoldNibbles]int
	if ops != nil {
		for start := 0; start < len(*ops); {
			nb := pathNibble((*ops)[start].path, 0)
			end := start + 1
			for end < len(*ops) && pathNibble((*ops)[end].path, 0) == nb {
				end++
			}
			starts[nb] = start
			counts[nb] = end - start
			job.activeSplits++
			start = end
		}
	}
	if len(updates) > 0 {
		stats.parallelCalls = 1
		stats.parallelSplits = uint64(job.activeSplits)
		stats.parallelWorkers = uint64(job.activeSplits)
	}

	job.done.Add(maxFoldNibbles)
	observeCommitmentPipelineSubmit(p.inflight.Add(1))
	for nb := range p.lanes {
		var group []op
		if counts[nb] > 0 {
			group = (*ops)[starts[nb] : starts[nb]+counts[nb]]
		}
		p.lanes[nb] <- orderedCommitmentLaneTask{job: job, nb: uint8(nb), ops: group}
	}
	go p.finishJob(job)
	return job.result
}

func (p *OrderedCommitmentPipeline) runLane(nb uint8, root BranchData, tasks <-chan orderedCommitmentLaneTask) {
	defer p.laneWG.Done()
	hasher := borrowKeccak()
	defer returnKeccak(hasher)
	var path [pathLen]byte
	for task := range tasks {
		job := task.job
		if failed := p.failed.Load(); failed != nil {
			job.setError(*failed)
			job.done.Done()
			continue
		}
		changed := false
		if len(task.ops) > 0 {
			buf := borrowBufferedBranchStore(job.store)
			sub := commitmentTrie{store: buf, hasher: hasher, foldStats: &job.siblingStats[nb]}
			var err error
			changed, err = sub.applyNibble(path[:0], 0, &root, nb, task.ops)
			if err == nil && changed {
				err = buf.flush(job.store, job.activeSplits)
			}
			returnBufferedBranchStore(buf)
			if err != nil {
				job.setError(err)
				p.setFailed(err)
				job.done.Done()
				continue
			}
		}
		if changed {
			job.changed.Store(true)
		}
		copyCommitmentLane(&job.root, &root, nb)
		job.done.Done()
	}
}

func (p *OrderedCommitmentPipeline) finishJob(job *orderedCommitmentJob) {
	job.done.Wait()
	for nb := range job.siblingStats {
		job.stats.merge(&job.siblingStats[nb])
	}
	if err := job.store.closeParentRead(); err != nil {
		job.setError(err)
		p.setFailed(err)
	}
	if err := job.loadError(); err != nil {
		p.finishJobResult(job, common.Hash{}, err)
		return
	}

	changed := job.changed.Load()
	job.stats.changed = changed
	if changed {
		if job.root.childCount() == 0 {
			if err := job.store.DelBranch(nil); err != nil {
				p.setFailed(err)
				p.finishJobResult(job, common.Hash{}, err)
				return
			}
		} else if err := job.store.PutBranch(nil, job.root); err != nil {
			p.setFailed(err)
			p.finishJobResult(job, common.Hash{}, err)
			return
		}
	}
	var root common.Hash
	if job.root.childCount() > 0 {
		h := borrowKeccak()
		trie := commitmentTrie{hasher: h, foldStats: job.stats}
		root = trie.rootHash(&job.root)
		returnKeccak(h)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(job.store.db, root); err != nil {
		p.setFailed(err)
		p.finishJobResult(job, common.Hash{}, err)
		return
	}
	p.finishJobResult(job, root, nil)
}

func (p *OrderedCommitmentPipeline) finishJobResult(job *orderedCommitmentJob, root common.Hash, err error) {
	if job.ops != nil {
		returnOpsBuf(job.ops)
		job.ops = nil
	}
	finishCommitmentFoldStats(job.stats, err != nil)
	if err != nil {
		commitmentPipelineErrorsCounter.Inc(1)
	}
	commitmentPipelineInflightGauge.Update(p.inflight.Add(-1))
	job.result <- OrderedCommitmentResult{Root: root, Err: err}
	close(job.result)
}

func (job *orderedCommitmentJob) setError(err error) {
	if err == nil {
		return
	}
	job.err.CompareAndSwap(nil, &err)
}

func (job *orderedCommitmentJob) loadError() error {
	if err := job.err.Load(); err != nil {
		return *err
	}
	return nil
}

func (p *OrderedCommitmentPipeline) setFailed(err error) {
	if err != nil {
		p.failed.CompareAndSwap(nil, &err)
	}
}

func completedOrderedCommitmentResult(result OrderedCommitmentResult) <-chan OrderedCommitmentResult {
	done := make(chan OrderedCommitmentResult, 1)
	done <- result
	close(done)
	return done
}

func copyCommitmentLane(dst, src *BranchData, nb uint8) {
	dst.clearChild(nb)
	if src == nil || !src.childPresent(nb) {
		return
	}
	if src.childKindAt(nb) == kindHash {
		dst.SetHashChild(nb, src.hashChildAt(nb))
		return
	}
	identity, pathOnly, valueHash := src.leafChildIdentityAt(nb)
	if pathOnly {
		dst.setLeafChildPath(nb, identity, valueHash)
	} else {
		dst.SetLeafChild(nb, identity, valueHash)
	}
}

// Close stops the persistent lane owners. Callers must first receive every
// submitted result; closing with queued work is intentionally unsupported.
func (p *OrderedCommitmentPipeline) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		for _, lane := range p.lanes {
			close(lane)
		}
		p.laneWG.Wait()
		commitmentPipelineEnabledGauge.Update(0)
	})
}
