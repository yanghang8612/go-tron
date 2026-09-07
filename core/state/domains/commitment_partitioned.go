package domains

import (
	"fmt"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	commitmentPartitionsPerNibble  = 4
	commitmentChildrenPerPartition = maxFoldNibbles / commitmentPartitionsPerNibble
	commitmentPartitionCount       = maxFoldNibbles * commitmentPartitionsPerNibble
)

// Partition ownership extends below the first branch: four adjacent second
// nibbles share one ordered owner. Descendant writes are disjoint, even across
// blocks. Depth-one branches are composed only after all owners finish; owners
// retain their child slots and never read those asynchronously composed rows.
// Thus a fast partition may advance without observing a partial/future parent.
// Physical keys, branch encodings, collapse rules and FIFO publication stay
// identical to the ordinary fold.
type commitmentPartitionPipeline struct {
	lanes       [commitmentPartitionCount]chan orderedCommitmentLaneTask
	previous    *orderedCommitmentJob
	initialRoot common.Hash
}

// An empty update stream leaves every partition untouched. Depend on the
// previous result directly instead of scheduling 64 empty tasks and hashing
// the unchanged top branch again. The private completion is independent of
// the result channel consumed by the FIFO publisher.
func (p *OrderedCommitmentPipeline) submitEmptyPartitions(job *orderedCommitmentJob) {
	previous := p.partitioned.previous
	p.partitioned.previous = job
	observeCommitmentPipelineSubmit(p.inflight.Add(1))
	go func() {
		root := p.partitioned.initialRoot
		if previous != nil {
			<-previous.completed
			root = previous.finalRoot
		}
		if failed := p.failed.Load(); failed != nil {
			p.finishJobResult(job, common.Hash{}, *failed)
			return
		}
		if err := rawdb.WriteLatestDomainCommitmentRoot(job.store.db, root); err != nil {
			p.setFailed(err)
			p.finishJobResult(job, common.Hash{}, err)
			return
		}
		p.finishJobResult(job, root, nil)
	}()
}

type commitmentPartitionJob struct {
	roots   [maxFoldNibbles]BranchData
	stats   [commitmentPartitionCount]commitmentFoldStats
	changed [commitmentPartitionCount]bool
}

var commitmentPartitionJobPool = sync.Pool{New: func() any { return new(commitmentPartitionJob) }}

func returnCommitmentPartitionJob(job *commitmentPartitionJob) {
	for i := range job.roots {
		clearBranchForPool(&job.roots[i])
	}
	clear(job.stats[:])
	clear(job.changed[:])
	commitmentPartitionJobPool.Put(job)
}

func commitmentPartition(path common.Hash) int {
	return int(path[0]) / commitmentChildrenPerPartition
}

// Expand an absent/leaf first-level slot into a virtual branch, or load and
// authenticate its existing branch once. Virtual single-leaf branches are
// never persisted: composePartitions uses the ordinary linkChild collapse.
func commitmentPartitionRoots(store *rawdbBranchStore, root *BranchData) ([maxFoldNibbles]BranchData, error) {
	var roots [maxFoldNibbles]BranchData
	h := borrowKeccak()
	defer returnKeccak(h)
	trie := commitmentTrie{hasher: h}
	for nb := uint8(0); nb < maxFoldNibbles; nb++ {
		if !root.childPresent(nb) {
			continue
		}
		if root.childKindAt(nb) == kindHash {
			found, err := store.GetBranchInto([]byte{nb}, &roots[nb])
			if err != nil {
				return roots, err
			}
			if !found || trie.nodeHash(&roots[nb]) != root.hashChildAt(nb) {
				return roots, fmt.Errorf("domains: commitment partition branch mismatch at %x", nb)
			}
			continue
		}
		identity, pathOnly, valueHash := root.leafChildIdentityAt(nb)
		var path common.Hash
		if pathOnly {
			copy(path[:], identity)
		} else {
			path = trie.keyPath(identity)
		}
		child := pathNibble(path, 1)
		if pathOnly {
			roots[nb].setLeafChildPath(child, identity, valueHash)
		} else {
			roots[nb].SetLeafChild(child, identity, valueHash)
		}
	}
	return roots, nil
}

func (p *OrderedCommitmentPipeline) startPartitions(roots *[maxFoldNibbles]BranchData) {
	p.partitioned = new(commitmentPartitionPipeline)
	for partition := range p.partitioned.lanes {
		nb := partition / commitmentPartitionsPerNibble
		firstChild := (partition % commitmentPartitionsPerNibble) * commitmentChildrenPerPartition
		var root BranchData
		for child := firstChild; child < firstChild+commitmentChildrenPerPartition; child++ {
			copyCommitmentLane(&root, &roots[nb], uint8(child))
		}
		lane := make(chan orderedCommitmentLaneTask, 16)
		p.partitioned.lanes[partition] = lane
		p.laneWG.Add(1)
		go p.runPartition(partition, root, lane)
	}
}

func (p *OrderedCommitmentPipeline) submitPartitions(job *orderedCommitmentJob) {
	var starts, counts [commitmentPartitionCount]int
	job.activeSplits = 0
	if job.ops != nil {
		for start := 0; start < len(*job.ops); {
			partition := commitmentPartition((*job.ops)[start].path)
			end := start + 1
			for end < len(*job.ops) && commitmentPartition((*job.ops)[end].path) == partition {
				end++
			}
			starts[partition], counts[partition] = start, end-start
			job.activeSplits++
			start = end
		}
	}
	job.stats.parallelSplits = uint64(job.activeSplits)
	job.stats.parallelWorkers = uint64(job.activeSplits)
	job.done.Add(commitmentPartitionCount)
	observeCommitmentPipelineSubmit(p.inflight.Add(1))
	for partition, lane := range p.partitioned.lanes {
		var group []op
		if counts[partition] > 0 {
			group = (*job.ops)[starts[partition] : starts[partition]+counts[partition]]
		}
		lane <- orderedCommitmentLaneTask{job: job, ops: group}
	}
}

func (p *OrderedCommitmentPipeline) runPartition(partition int, root BranchData, tasks <-chan orderedCommitmentLaneTask) {
	defer p.laneWG.Done()
	h := borrowKeccak()
	defer returnKeccak(h)
	nb := partition / commitmentPartitionsPerNibble
	firstChild := (partition % commitmentPartitionsPerNibble) * commitmentChildrenPerPartition
	var path [pathLen]byte
	path[0] = byte(nb)
	for task := range tasks {
		job := task.job
		if prefetched := &job.prefetch[nb]; len(task.ops) > 0 && prefetched.active && !p.prefetchOverlap {
			started := time.Now()
			prefetched.critical.Wait()
			commitmentPipelinePrefetchCriticalWaitCallsCounter.Inc(1)
			commitmentPipelinePrefetchCriticalWaitNanosCounter.Inc(time.Since(started).Nanoseconds())
		}
		if failed := p.failed.Load(); failed != nil {
			job.setError(*failed)
			job.done.Done()
			continue
		}
		if len(task.ops) > 0 {
			buf := borrowBufferedBranchStore(job.store)
			trie := commitmentTrie{store: buf, hasher: h, foldStats: &job.partitions.stats[partition]}
			_, changed, err := trie.apply(path[:1], 1, &root, task.ops)
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
			job.partitions.changed[partition] = changed
			if changed {
				job.changed.Store(true)
			}
		}
		for child := firstChild; child < firstChild+commitmentChildrenPerPartition; child++ {
			copyCommitmentLane(&job.partitions.roots[nb], &root, uint8(child))
		}
		job.done.Done()
	}
}

// Called only after the partition join. Only changed first-level branches are
// written, and every collapse goes through the same helper as the serial fold.
// Unchanged legacy leaves keep their original encoding until actually changed.
func (p *OrderedCommitmentPipeline) composePartitions(job *orderedCommitmentJob) error {
	h := borrowKeccak()
	defer returnKeccak(h)
	trie := commitmentTrie{store: job.store, hasher: h, foldStats: job.stats}
	for partition := range job.partitions.stats {
		job.stats.merge(&job.partitions.stats[partition])
	}
	for nb := uint8(0); nb < maxFoldNibbles; nb++ {
		child := &job.partitions.roots[nb]
		changed := false
		for i := 0; i < commitmentPartitionsPerNibble; i++ {
			changed = changed || job.partitions.changed[int(nb)*commitmentPartitionsPerNibble+i]
		}
		if changed {
			if child.childCount() == 0 {
				child = nil
			}
			if err := trie.linkChild(&job.root, nb, []byte{nb}, child); err != nil {
				return err
			}
			continue
		}
		if child.childCount() == 0 {
			continue
		}
		if child.childCount() == 1 && child.childKindAt(child.onlyChildNibble()) == kindLeaf {
			identity, pathOnly, valueHash := child.leafChildIdentityAt(child.onlyChildNibble())
			if pathOnly {
				job.root.setLeafChildPath(nb, identity, valueHash)
			} else {
				job.root.SetLeafChild(nb, identity, valueHash)
			}
		} else {
			job.root.SetHashChild(nb, trie.nodeHash(child))
		}
	}
	return nil
}
