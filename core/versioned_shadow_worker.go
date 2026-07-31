package core

import (
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

const (
	discardShadowSampleInterval = uint64(64)
	discardShadowWorkerCount    = 4
)

var (
	discardShadowBlocksCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/blocks", nil)
	discardShadowCandidatesCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/candidates", nil)
	discardShadowExecutedCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/executed", nil)
	discardShadowMatchesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/matches", nil)
	discardShadowMismatchesCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatches", nil)
	discardShadowErrorsCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/errors", nil)
	discardShadowCopyNanosCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/copy_nanos", nil)
	discardShadowExecutionNanosCounter = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/execution_nanos", nil)
	discardShadowLastCandidatesGauge   = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/candidates", nil)
	discardShadowLastExecutedGauge     = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/executed", nil)
	discardShadowLastMatchesGauge      = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/matches", nil)
)

// discardKVOverlay isolates rawdb writes performed by actuators. Reads fall
// through to the canonical block view, while writes remain worker-local and
// are thrown away after one transaction.
type discardKVOverlay struct {
	parent  actuator.BufferedKVStore
	puts    map[string][]byte
	deletes map[string]struct{}
}

func (db *discardKVOverlay) reset() {
	clear(db.puts)
	clear(db.deletes)
}

func (db *discardKVOverlay) Has(key []byte) (bool, error) {
	keyString := string(key)
	if _, ok := db.puts[keyString]; ok {
		return true, nil
	}
	if _, ok := db.deletes[keyString]; ok {
		return false, nil
	}
	if db.parent == nil {
		return false, nil
	}
	return db.parent.Has(key)
}

func (db *discardKVOverlay) Get(key []byte) ([]byte, error) {
	keyString := string(key)
	if value, ok := db.puts[keyString]; ok {
		return append([]byte(nil), value...), nil
	}
	if _, ok := db.deletes[keyString]; ok {
		return nil, errors.New("not found")
	}
	if db.parent == nil {
		return nil, errors.New("not found")
	}
	return db.parent.Get(key)
}

func (db *discardKVOverlay) Put(key, value []byte) error {
	if db.puts == nil {
		db.puts = make(map[string][]byte)
	}
	keyString := string(key)
	db.puts[keyString] = append(db.puts[keyString][:0], value...)
	delete(db.deletes, keyString)
	return nil
}

func (db *discardKVOverlay) Delete(key []byte) error {
	if db.deletes == nil {
		db.deletes = make(map[string]struct{})
	}
	keyString := string(key)
	delete(db.puts, keyString)
	db.deletes[keyString] = struct{}{}
	return nil
}

type discardShadowBlock struct {
	base      *state.StateDB
	copyNanos int64
}

func prepareDiscardShadowBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, blockNum uint64) *discardShadowBlock {
	if statedb == nil || dynProps == nil || blockNum%discardShadowSampleInterval != 0 {
		return nil
	}
	started := time.Now()
	base, err := statedb.Copy()
	if err != nil {
		discardShadowErrorsCounter.Inc(1)
		return nil
	}
	base.SetDynamicProperties(dynProps.Copy())
	return &discardShadowBlock{base: base, copyNanos: time.Since(started).Nanoseconds()}
}

type discardShadowTaskResult struct {
	matched bool
	err     error
}

type discardShadowRunConfig struct {
	block                   *types.Block
	db                      actuator.BufferedKVStore
	validateEnvelope        bool
	activeWitnesses         []tcommon.Address
	genesisTimestamp        int64
	energyLimitForkBlockNum int64
	genesisHash             tcommon.Hash
	transactions            []*types.Transaction
	canonicalInfos          []*corepb.TransactionInfo
}

type discardShadowRunStats struct {
	candidates int64
	executed   int64
	matches    int64
	mismatches int64
	errors     int64
}

func (shadow *discardShadowBlock) run(versioned *versionedAccessShadow, cfg discardShadowRunConfig) discardShadowRunStats {
	if shadow == nil || shadow.base == nil || versioned == nil || cfg.block == nil || len(cfg.canonicalInfos) != len(cfg.transactions) {
		return discardShadowRunStats{}
	}
	candidates := make([]int, 0, discardShadowWorkerCount*2)
	for txIndex := range cfg.transactions {
		if txIndex < len(versioned.transactionSupported) && versioned.transactionSupported[txIndex] &&
			txIndex < len(versioned.dependencyHeads) && versioned.dependencyHeads[txIndex] < 0 {
			candidates = append(candidates, txIndex)
		}
	}
	if len(candidates) == 0 {
		return discardShadowRunStats{}
	}

	workerCount := min(discardShadowWorkerCount, len(candidates))
	workerStates := make([]*state.StateDB, 0, workerCount)
	workerStates = append(workerStates, shadow.base)
	copyStarted := time.Now()
	for len(workerStates) < workerCount {
		workerState, err := shadow.base.Copy()
		if err != nil {
			discardShadowErrorsCounter.Inc(1)
			break
		}
		workerState.SetDynamicProperties(shadow.base.DynamicProperties().Copy())
		workerStates = append(workerStates, workerState)
	}
	shadow.copyNanos += time.Since(copyStarted).Nanoseconds()
	workerCount = len(workerStates)
	if workerCount == 0 {
		return discardShadowRunStats{}
	}

	jobs := make(chan int)
	results := make(chan discardShadowTaskResult, len(candidates))
	var workers sync.WaitGroup
	executionStarted := time.Now()
	for _, workerState := range workerStates {
		workers.Add(1)
		go func(workerState *state.StateDB) {
			defer workers.Done()
			worker := discardShadowWorker{
				state:     workerState,
				dynProps:  workerState.DynamicProperties(),
				db:        discardKVOverlay{parent: cfg.db},
				forkCache: forks.NewVersionPassCache().BlockScope(),
			}
			for txIndex := range jobs {
				results <- worker.execute(txIndex, cfg)
			}
		}(workerState)
	}
	go func() {
		for _, txIndex := range candidates {
			jobs <- txIndex
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()

	var executed, matches, mismatches, executionErrors int64
	for result := range results {
		executed++
		switch {
		case result.err != nil:
			executionErrors++
		case result.matched:
			matches++
		default:
			mismatches++
		}
	}
	executionNanos := time.Since(executionStarted).Nanoseconds()
	discardShadowBlocksCounter.Inc(1)
	discardShadowCandidatesCounter.Inc(int64(len(candidates)))
	discardShadowExecutedCounter.Inc(executed)
	discardShadowMatchesCounter.Inc(matches)
	discardShadowMismatchesCounter.Inc(mismatches)
	discardShadowErrorsCounter.Inc(executionErrors)
	discardShadowCopyNanosCounter.Inc(shadow.copyNanos)
	discardShadowExecutionNanosCounter.Inc(executionNanos)
	discardShadowLastCandidatesGauge.Update(int64(len(candidates)))
	discardShadowLastExecutedGauge.Update(executed)
	discardShadowLastMatchesGauge.Update(matches)
	return discardShadowRunStats{
		candidates: int64(len(candidates)),
		executed:   executed,
		matches:    matches,
		mismatches: mismatches,
		errors:     executionErrors,
	}
}

type discardShadowWorker struct {
	state     *state.StateDB
	dynProps  *state.DynamicProperties
	db        discardKVOverlay
	forkCache *forks.VersionPassCache
	scratch   applyTransactionScratch
	infoSlot  transactionInfoSlot
}

func (worker *discardShadowWorker) execute(txIndex int, cfg discardShadowRunConfig) discardShadowTaskResult {
	if txIndex < 0 || txIndex >= len(cfg.transactions) || cfg.canonicalInfos[txIndex] == nil {
		return discardShadowTaskResult{err: errors.New("missing shadow transaction input")}
	}
	tx := cfg.transactions[txIndex]
	stateSnapshot := worker.state.Snapshot()
	dpSnapshot := worker.dynProps.Snapshot()
	worker.db.reset()
	worker.infoSlot.internalTxArena.Reset()
	worker.infoSlot.executionLogArena.Reset()
	worker.state.BeginBalanceTraceTransaction(tx.Hash().Bytes(), tx.ContractType().String())

	prevBlockTime := worker.dynProps.LatestBlockHeaderTimestamp()
	prevBlockHeadSlot := HeadSlot(prevBlockTime, cfg.genesisTimestamp)
	result, err := applyTransactionWithScratch(
		worker.state,
		worker.dynProps,
		tx,
		prevBlockTime,
		true,
		prevBlockHeadSlot,
		cfg.block.Timestamp(),
		cfg.block.Number(),
		&worker.db,
		cfg.activeWitnesses,
		cfg.energyLimitForkBlockNum,
		cfg.genesisHash,
		cfg.block.WitnessAddress(),
		true,
		cfg.validateEnvelope,
		true,
		worker.forkCache,
		nil,
		&worker.scratch,
		&worker.infoSlot.internalTxArena,
		&worker.infoSlot.executionLogArena,
	)
	if err == nil {
		err = ValidateTxVMContractRet(tx, corepb.Transaction_ResultContractResult(result.ContractRet))
	}
	if err != nil {
		worker.state.ClearBalanceTrace()
		worker.state.RevertToSnapshot(stateSnapshot)
		worker.dynProps.RevertToSnapshot(dpSnapshot)
		return discardShadowTaskResult{err: err}
	}

	shadowInfo := worker.infoSlot.build(tx, result, cfg.block.Number(), cfg.block.Timestamp(), worker.dynProps.AllowTransactionFeePool())
	matched := proto.Equal(shadowInfo, cfg.canonicalInfos[txIndex])
	vm.ReleaseExecutionLogs(result.Logs)
	result.Logs = nil
	worker.state.FinalizeTransaction()
	worker.state.EndBalanceTraceTransaction(balanceTraceTransactionStatus(result))
	worker.state.RevertToSnapshot(stateSnapshot)
	worker.dynProps.RevertToSnapshot(dpSnapshot)
	return discardShadowTaskResult{matched: matched}
}
