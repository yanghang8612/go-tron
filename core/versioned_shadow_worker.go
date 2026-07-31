package core

import (
	"bytes"
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
	discardShadowBlocksCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/blocks", nil)
	discardShadowCandidatesCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/candidates", nil)
	discardShadowExecutedCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/executed", nil)
	discardShadowMatchesCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/matches", nil)
	discardShadowMismatchesCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatches", nil)
	discardShadowCoreMatchesCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/core_matches", nil)
	discardShadowCoreMismatchesCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/core_mismatches", nil)
	discardShadowWriteSetMatchesCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_matches", nil)
	discardShadowWriteSetMismatchesCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_mismatches", nil)
	discardShadowWriteSetErrorsCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_errors", nil)
	discardShadowWriteSetApplyEligibleCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_eligible", nil)
	discardShadowWriteSetApplyUnsupportedCounter = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported", nil)
	discardShadowErrorsCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/errors", nil)
	discardShadowCopyNanosCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/copy_nanos", nil)
	discardShadowExecutionNanosCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/execution_nanos", nil)
	discardShadowLastCandidatesGauge             = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/candidates", nil)
	discardShadowLastExecutedGauge               = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/executed", nil)
	discardShadowLastMatchesGauge                = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/matches", nil)
	discardShadowMismatchVMCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/vm", nil)
	discardShadowMismatchTransferCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/transfer", nil)
	discardShadowMismatchOtherCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/other", nil)
	discardShadowErrorVMCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/vm", nil)
	discardShadowErrorTransferCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/transfer", nil)
	discardShadowErrorOtherCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/other", nil)
	discardShadowMismatchReceiptCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt", nil)
	discardShadowMismatchReceiptCoreCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core", nil)
	discardShadowMismatchReceiptEnergyCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_energy", nil)
	discardShadowMismatchEnergyUsageCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_usage", nil)
	discardShadowMismatchEnergyFeeCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_fee", nil)
	discardShadowMismatchOriginEnergyCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_origin_energy_usage", nil)
	discardShadowMismatchEnergyTotalCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_usage_total", nil)
	discardShadowMismatchReceiptBandwidthCounter = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_bandwidth", nil)
	discardShadowMismatchReceiptResultCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_result", nil)
	discardShadowMismatchOwnerDiagnosticCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_owner_diagnostic", nil)
	discardShadowMismatchEnergyDiagnosticCounter = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_diagnostic", nil)
	discardShadowMismatchFeeCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/fee", nil)
	discardShadowMismatchResultCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/contract_result", nil)
	discardShadowMismatchLogsCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/logs", nil)
	discardShadowMismatchInternalCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/internal_transactions", nil)
	discardShadowMismatchStatusCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/status", nil)
	discardShadowMismatchMessageCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/res_message", nil)
	discardShadowMismatchAddressCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/contract_address", nil)
	discardShadowMismatchIdentityCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/identity", nil)
	discardShadowMismatchOtherFieldCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/other", nil)
)

// discardKVOverlay isolates rawdb writes performed by actuators. Reads fall
// through to the canonical block view, while writes remain worker-local and
// are thrown away after one transaction.
type discardKVOverlay struct {
	parent   actuator.BufferedKVStore
	recorder *state.TransactionAccessRecorder
	puts     map[string][]byte
	deletes  map[string]struct{}
}

func (db *discardKVOverlay) reset() {
	clear(db.puts)
	clear(db.deletes)
}

func (db *discardKVOverlay) Has(key []byte) (bool, error) {
	db.recorder.RecordRawKVRead(key)
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
	db.recorder.RecordRawKVRead(key)
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
	db.recorder.RecordRawKVPut(key, value)
	return nil
}

func (db *discardKVOverlay) Delete(key []byte) error {
	if db.deletes == nil {
		db.deletes = make(map[string]struct{})
	}
	keyString := string(key)
	delete(db.puts, keyString)
	db.deletes[keyString] = struct{}{}
	db.recorder.RecordRawKVDelete(key)
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
	class         discardShadowTransactionClass
	mismatch      discardShadowMismatch
	coreMatch     bool
	matched       bool
	writeSetMatch bool
	writeSetErr   error
	applyEligible bool
	err           error
}

type discardShadowTransactionClass uint8

const (
	discardShadowOther discardShadowTransactionClass = iota
	discardShadowTransfer
	discardShadowVM
)

func classifyDiscardShadowTransaction(tx *types.Transaction) discardShadowTransactionClass {
	if tx == nil {
		return discardShadowOther
	}
	switch tx.ContractType() {
	case corepb.Transaction_Contract_TransferContract:
		return discardShadowTransfer
	case corepb.Transaction_Contract_TriggerSmartContract, corepb.Transaction_Contract_CreateSmartContract:
		return discardShadowVM
	default:
		return discardShadowOther
	}
}

type discardShadowMismatch uint32

const (
	discardShadowMismatchReceipt discardShadowMismatch = 1 << iota
	discardShadowMismatchFee
	discardShadowMismatchResult
	discardShadowMismatchLogs
	discardShadowMismatchInternal
	discardShadowMismatchStatus
	discardShadowMismatchMessage
	discardShadowMismatchAddress
	discardShadowMismatchIdentity
	discardShadowMismatchOtherField
	discardShadowMismatchReceiptCore
	discardShadowMismatchOwnerDiagnostic
	discardShadowMismatchEnergyDiagnostic
	discardShadowMismatchReceiptEnergy
	discardShadowMismatchReceiptBandwidth
	discardShadowMismatchReceiptResult
	discardShadowMismatchEnergyUsage
	discardShadowMismatchEnergyFee
	discardShadowMismatchOriginEnergy
	discardShadowMismatchEnergyTotal
)

func equalTransactionInfoMessages[A proto.Message](left, right []A) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func equalByteSlices(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func compareDiscardShadowInfo(shadow, canonical *corepb.TransactionInfo) discardShadowMismatch {
	if proto.Equal(shadow, canonical) {
		return 0
	}
	var mismatch discardShadowMismatch
	if !proto.Equal(shadow.GetReceipt(), canonical.GetReceipt()) {
		mismatch |= discardShadowMismatchReceipt
		shadowReceipt := proto.Clone(shadow.GetReceipt()).(*corepb.ResourceReceipt)
		canonicalReceipt := proto.Clone(canonical.GetReceipt()).(*corepb.ResourceReceipt)
		if shadowReceipt.GetOwnerBalance() != canonicalReceipt.GetOwnerBalance() ||
			shadowReceipt.GetOwnerFreeNetLeft() != canonicalReceipt.GetOwnerFreeNetLeft() ||
			shadowReceipt.GetOwnerFrozenNetLeft() != canonicalReceipt.GetOwnerFrozenNetLeft() ||
			shadowReceipt.GetOwnerNetLastConsumeTime() != canonicalReceipt.GetOwnerNetLastConsumeTime() ||
			shadowReceipt.GetOwnerFreeNetLastConsumeTime() != canonicalReceipt.GetOwnerFreeNetLastConsumeTime() ||
			shadowReceipt.GetOwnerFrozenForNet() != canonicalReceipt.GetOwnerFrozenForNet() ||
			shadowReceipt.GetOwnerFrozenForEnergy() != canonicalReceipt.GetOwnerFrozenForEnergy() {
			mismatch |= discardShadowMismatchOwnerDiagnostic
		}
		if shadowReceipt.GetOriginEnergyLeft() != canonicalReceipt.GetOriginEnergyLeft() ||
			shadowReceipt.GetCallerEnergyLeft() != canonicalReceipt.GetCallerEnergyLeft() ||
			shadowReceipt.GetOriginEnergyWindow() != canonicalReceipt.GetOriginEnergyWindow() ||
			shadowReceipt.GetCallerEnergyWindow() != canonicalReceipt.GetCallerEnergyWindow() ||
			shadowReceipt.GetCallerEnergyLimit() != canonicalReceipt.GetCallerEnergyLimit() ||
			shadowReceipt.GetOriginEnergyLimit() != canonicalReceipt.GetOriginEnergyLimit() ||
			shadowReceipt.GetOriginFrozenForEnergy() != canonicalReceipt.GetOriginFrozenForEnergy() ||
			shadowReceipt.GetCallerEnergyUsagePre() != canonicalReceipt.GetCallerEnergyUsagePre() ||
			shadowReceipt.GetOriginEnergyUsagePre() != canonicalReceipt.GetOriginEnergyUsagePre() ||
			shadowReceipt.GetCallerEnergyLastConsumeTime() != canonicalReceipt.GetCallerEnergyLastConsumeTime() ||
			shadowReceipt.GetOriginEnergyLastConsumeTime() != canonicalReceipt.GetOriginEnergyLastConsumeTime() ||
			shadowReceipt.GetTotalEnergyWeight() != canonicalReceipt.GetTotalEnergyWeight() ||
			shadowReceipt.GetTotalEnergyCurrentLimit() != canonicalReceipt.GetTotalEnergyCurrentLimit() {
			mismatch |= discardShadowMismatchEnergyDiagnostic
		}
		for _, receipt := range []*corepb.ResourceReceipt{shadowReceipt, canonicalReceipt} {
			receipt.OwnerBalance = 0
			receipt.OwnerFreeNetLeft = 0
			receipt.OwnerFrozenNetLeft = 0
			receipt.OwnerNetLastConsumeTime = 0
			receipt.OwnerFreeNetLastConsumeTime = 0
			receipt.OwnerFrozenForNet = 0
			receipt.OwnerFrozenForEnergy = 0
			receipt.OriginEnergyLeft = 0
			receipt.CallerEnergyLeft = 0
			receipt.OriginEnergyWindow = 0
			receipt.CallerEnergyWindow = 0
			receipt.CallerEnergyLimit = 0
			receipt.OriginEnergyLimit = 0
			receipt.OriginFrozenForEnergy = 0
			receipt.CallerEnergyUsagePre = 0
			receipt.OriginEnergyUsagePre = 0
			receipt.CallerEnergyLastConsumeTime = 0
			receipt.OriginEnergyLastConsumeTime = 0
			receipt.TotalEnergyWeight = 0
			receipt.TotalEnergyCurrentLimit = 0
		}
		if !proto.Equal(shadowReceipt, canonicalReceipt) {
			mismatch |= discardShadowMismatchReceiptCore
			if shadowReceipt.GetEnergyUsage() != canonicalReceipt.GetEnergyUsage() ||
				shadowReceipt.GetEnergyFee() != canonicalReceipt.GetEnergyFee() ||
				shadowReceipt.GetOriginEnergyUsage() != canonicalReceipt.GetOriginEnergyUsage() ||
				shadowReceipt.GetEnergyUsageTotal() != canonicalReceipt.GetEnergyUsageTotal() {
				mismatch |= discardShadowMismatchReceiptEnergy
			}
			if shadowReceipt.GetEnergyUsage() != canonicalReceipt.GetEnergyUsage() {
				mismatch |= discardShadowMismatchEnergyUsage
			}
			if shadowReceipt.GetEnergyFee() != canonicalReceipt.GetEnergyFee() {
				mismatch |= discardShadowMismatchEnergyFee
			}
			if shadowReceipt.GetOriginEnergyUsage() != canonicalReceipt.GetOriginEnergyUsage() {
				mismatch |= discardShadowMismatchOriginEnergy
			}
			if shadowReceipt.GetEnergyUsageTotal() != canonicalReceipt.GetEnergyUsageTotal() {
				mismatch |= discardShadowMismatchEnergyTotal
			}
			if shadowReceipt.GetNetUsage() != canonicalReceipt.GetNetUsage() || shadowReceipt.GetNetFee() != canonicalReceipt.GetNetFee() {
				mismatch |= discardShadowMismatchReceiptBandwidth
			}
			if shadowReceipt.GetResult() != canonicalReceipt.GetResult() || shadowReceipt.GetEnergyPenaltyTotal() != canonicalReceipt.GetEnergyPenaltyTotal() {
				mismatch |= discardShadowMismatchReceiptResult
			}
		}
	}
	if shadow.GetFee() != canonical.GetFee() || shadow.GetPackingFee() != canonical.GetPackingFee() {
		mismatch |= discardShadowMismatchFee
	}
	if !equalByteSlices(shadow.GetContractResult(), canonical.GetContractResult()) {
		mismatch |= discardShadowMismatchResult
	}
	if !equalTransactionInfoMessages(shadow.GetLog(), canonical.GetLog()) {
		mismatch |= discardShadowMismatchLogs
	}
	if !equalTransactionInfoMessages(shadow.GetInternalTransactions(), canonical.GetInternalTransactions()) {
		mismatch |= discardShadowMismatchInternal
	}
	if shadow.GetResult() != canonical.GetResult() {
		mismatch |= discardShadowMismatchStatus
	}
	if !bytes.Equal(shadow.GetResMessage(), canonical.GetResMessage()) {
		mismatch |= discardShadowMismatchMessage
	}
	if !bytes.Equal(shadow.GetContractAddress(), canonical.GetContractAddress()) {
		mismatch |= discardShadowMismatchAddress
	}
	if !bytes.Equal(shadow.GetId(), canonical.GetId()) || shadow.GetBlockNumber() != canonical.GetBlockNumber() || shadow.GetBlockTimeStamp() != canonical.GetBlockTimeStamp() {
		mismatch |= discardShadowMismatchIdentity
	}
	shadowRemainder := proto.Clone(shadow).(*corepb.TransactionInfo)
	canonicalRemainder := proto.Clone(canonical).(*corepb.TransactionInfo)
	for _, info := range []*corepb.TransactionInfo{shadowRemainder, canonicalRemainder} {
		info.Receipt = nil
		info.Fee = 0
		info.PackingFee = 0
		info.ContractResult = nil
		info.Log = nil
		info.InternalTransactions = nil
		info.Result = 0
		info.ResMessage = nil
		info.ContractAddress = nil
		info.Id = nil
		info.BlockNumber = 0
		info.BlockTimeStamp = 0
	}
	if !proto.Equal(shadowRemainder, canonicalRemainder) {
		mismatch |= discardShadowMismatchOtherField
	}
	return mismatch
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
	canonicalWriteSets      []state.TransactionWriteSet
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
	cfg.canonicalWriteSets = versioned.transactionWriteSets
	candidates := make([]int, 0, discardShadowWorkerCount*2)
	for txIndex := range cfg.transactions {
		writeSetReady := len(versioned.transactionWritesOK) == 0 ||
			(txIndex < len(versioned.transactionWritesOK) && versioned.transactionWritesOK[txIndex])
		if txIndex < len(versioned.transactionSupported) && versioned.transactionSupported[txIndex] &&
			txIndex < len(versioned.dependencyHeads) && versioned.dependencyHeads[txIndex] < 0 &&
			writeSetReady {
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
			worker.db.recorder = &worker.recorder
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

	var executed, matches, mismatches, coreMatches, coreMismatches, writeSetMatches, writeSetMismatches, writeSetErrors, applyEligible, applyUnsupported, executionErrors int64
	for result := range results {
		executed++
		switch {
		case result.err != nil:
			executionErrors++
			switch result.class {
			case discardShadowVM:
				discardShadowErrorVMCounter.Inc(1)
			case discardShadowTransfer:
				discardShadowErrorTransferCounter.Inc(1)
			default:
				discardShadowErrorOtherCounter.Inc(1)
			}
		case result.matched:
			matches++
		default:
			mismatches++
			switch result.class {
			case discardShadowVM:
				discardShadowMismatchVMCounter.Inc(1)
			case discardShadowTransfer:
				discardShadowMismatchTransferCounter.Inc(1)
			default:
				discardShadowMismatchOtherCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchReceipt != 0 {
				discardShadowMismatchReceiptCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchReceiptCore != 0 {
				discardShadowMismatchReceiptCoreCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchReceiptEnergy != 0 {
				discardShadowMismatchReceiptEnergyCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchEnergyUsage != 0 {
				discardShadowMismatchEnergyUsageCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchEnergyFee != 0 {
				discardShadowMismatchEnergyFeeCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchOriginEnergy != 0 {
				discardShadowMismatchOriginEnergyCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchEnergyTotal != 0 {
				discardShadowMismatchEnergyTotalCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchReceiptBandwidth != 0 {
				discardShadowMismatchReceiptBandwidthCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchReceiptResult != 0 {
				discardShadowMismatchReceiptResultCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchOwnerDiagnostic != 0 {
				discardShadowMismatchOwnerDiagnosticCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchEnergyDiagnostic != 0 {
				discardShadowMismatchEnergyDiagnosticCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchFee != 0 {
				discardShadowMismatchFeeCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchResult != 0 {
				discardShadowMismatchResultCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchLogs != 0 {
				discardShadowMismatchLogsCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchInternal != 0 {
				discardShadowMismatchInternalCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchStatus != 0 {
				discardShadowMismatchStatusCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchMessage != 0 {
				discardShadowMismatchMessageCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchAddress != 0 {
				discardShadowMismatchAddressCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchIdentity != 0 {
				discardShadowMismatchIdentityCounter.Inc(1)
			}
			if result.mismatch&discardShadowMismatchOtherField != 0 {
				discardShadowMismatchOtherFieldCounter.Inc(1)
			}
		}
		if result.err == nil {
			if result.coreMatch {
				coreMatches++
			} else {
				coreMismatches++
			}
			switch {
			case result.writeSetErr != nil:
				writeSetErrors++
			case result.writeSetMatch:
				writeSetMatches++
			default:
				writeSetMismatches++
			}
			if result.writeSetErr == nil {
				if result.applyEligible {
					applyEligible++
				} else {
					applyUnsupported++
				}
			}
		}
	}
	executionNanos := time.Since(executionStarted).Nanoseconds()
	discardShadowBlocksCounter.Inc(1)
	discardShadowCandidatesCounter.Inc(int64(len(candidates)))
	discardShadowExecutedCounter.Inc(executed)
	discardShadowMatchesCounter.Inc(matches)
	discardShadowMismatchesCounter.Inc(mismatches)
	discardShadowCoreMatchesCounter.Inc(coreMatches)
	discardShadowCoreMismatchesCounter.Inc(coreMismatches)
	discardShadowWriteSetMatchesCounter.Inc(writeSetMatches)
	discardShadowWriteSetMismatchesCounter.Inc(writeSetMismatches)
	discardShadowWriteSetErrorsCounter.Inc(writeSetErrors)
	discardShadowWriteSetApplyEligibleCounter.Inc(applyEligible)
	discardShadowWriteSetApplyUnsupportedCounter.Inc(applyUnsupported)
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
	recorder  state.TransactionAccessRecorder
}

func (worker *discardShadowWorker) execute(txIndex int, cfg discardShadowRunConfig) discardShadowTaskResult {
	if txIndex < 0 || txIndex >= len(cfg.transactions) || cfg.canonicalInfos[txIndex] == nil {
		return discardShadowTaskResult{err: errors.New("missing shadow transaction input")}
	}
	tx := cfg.transactions[txIndex]
	class := classifyDiscardShadowTransaction(tx)
	stateSnapshot := worker.state.Snapshot()
	dpSnapshot := worker.dynProps.Snapshot()
	journalMark := worker.state.DomainChangeJournalMark()
	worker.recorder.Reset(64)
	worker.state.SetTransactionAccessRecorder(&worker.recorder)
	worker.dynProps.SetTransactionAccessRecorder(&worker.recorder)
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
		worker.state.SetTransactionAccessRecorder(nil)
		worker.dynProps.SetTransactionAccessRecorder(nil)
		worker.state.ClearBalanceTrace()
		worker.state.RevertToSnapshot(stateSnapshot)
		worker.dynProps.RevertToSnapshot(dpSnapshot)
		return discardShadowTaskResult{class: class, err: err}
	}

	shadowInfo := worker.infoSlot.build(tx, result, cfg.block.Number(), cfg.block.Timestamp(), worker.dynProps.AllowTransactionFeePool())
	mismatch := compareDiscardShadowInfo(shadowInfo, cfg.canonicalInfos[txIndex])
	coreMismatch := mismatch &^ (discardShadowMismatchReceipt | discardShadowMismatchOwnerDiagnostic | discardShadowMismatchEnergyDiagnostic)
	vm.ReleaseExecutionLogs(result.Logs)
	result.Logs = nil
	worker.state.FinalizeTransaction()
	worker.state.EndBalanceTraceTransaction(balanceTraceTransactionStatus(result))
	worker.state.SetTransactionAccessRecorder(nil)
	worker.dynProps.SetTransactionAccessRecorder(nil)
	writes, known, writeSetErr := worker.state.CaptureTransactionWriteSet(journalMark, &worker.recorder, worker.dynProps)
	if writeSetErr == nil && !known {
		writeSetErr = errors.New("unknown worker state write")
	}
	writeSetMatch := writeSetErr == nil
	applyEligible := false
	if writeSetMatch && txIndex < len(cfg.canonicalWriteSets) {
		writeSetMatch = state.EqualTransactionWriteSets(writes, cfg.canonicalWriteSets[txIndex])
		applyEligible = state.ValidateTransactionWriteSetApply(writes, worker.dynProps, &worker.db) == nil
	}
	worker.state.RevertToSnapshot(stateSnapshot)
	worker.dynProps.RevertToSnapshot(dpSnapshot)
	return discardShadowTaskResult{
		class:         class,
		mismatch:      mismatch,
		coreMatch:     coreMismatch == 0,
		matched:       mismatch == 0,
		writeSetMatch: writeSetMatch,
		writeSetErr:   writeSetErr,
		applyEligible: applyEligible,
	}
}
