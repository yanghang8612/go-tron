package core

import (
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	versionedShadowBlocksCounter                              = metrics.NewRegisteredCounter("core/versioned_shadow/blocks", nil)
	versionedShadowTransactionsCounter                        = metrics.NewRegisteredCounter("core/versioned_shadow/transactions", nil)
	versionedShadowFirstPassValidCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/first_pass_valid", nil)
	versionedShadowNormalizedFirstPassValidCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/normalized_first_pass_valid", nil)
	versionedShadowConflictsCounter                           = metrics.NewRegisteredCounter("core/versioned_shadow/conflicts", nil)
	versionedShadowNormalizedConflictsCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/normalized_conflicts", nil)
	versionedShadowUnsupportedCounter                         = metrics.NewRegisteredCounter("core/versioned_shadow/unsupported", nil)
	versionedShadowReadCellsCounter                           = metrics.NewRegisteredCounter("core/versioned_shadow/read_cells", nil)
	versionedShadowWriteCellsCounter                          = metrics.NewRegisteredCounter("core/versioned_shadow/write_cells", nil)
	versionedShadowReadConflictsCounter                       = metrics.NewRegisteredCounter("core/versioned_shadow/read_conflicts", nil)
	versionedShadowWriteConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/write_conflicts", nil)
	versionedShadowAccountConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account", nil)
	versionedShadowStorageConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/storage", nil)
	versionedShadowAccountKVConflictsCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account_kv", nil)
	versionedShadowDynamicConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/dynamic", nil)
	versionedShadowOtherConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/other", nil)
	versionedShadowVMTransactionsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/vm/transactions", nil)
	versionedShadowVMFirstPassValidCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/vm/first_pass_valid", nil)
	versionedShadowVMNormalizedFirstPassCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/vm/normalized_first_pass_valid", nil)
	versionedShadowTransferTransactionsCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/transactions", nil)
	versionedShadowTransferFirstPassValidCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/first_pass_valid", nil)
	versionedShadowTransferNormalizedFirstPassCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/normalized_first_pass_valid", nil)
	versionedShadowOtherTransactionsCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/other/transactions", nil)
	versionedShadowOtherFirstPassValidCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/other/first_pass_valid", nil)
	versionedShadowOtherNormalizedFirstPassCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/other/normalized_first_pass_valid", nil)
	versionedShadowSettlementTaggedCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/tagged_transactions", nil)
	versionedShadowSettlementResolvedCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/resolved_first_pass", nil)
	versionedShadowSettlementBlackholeConflictCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/blackhole_balance", nil)
	versionedShadowSettlementBurnConflictCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/burn_trx", nil)
	versionedShadowSettlementFeePoolConflictCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/transaction_fee_pool", nil)
	versionedShadowSettlementTransactionCostConflictCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/total_transaction_cost", nil)
	versionedShadowSettlementCreateAccountCostConflictCounter = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/total_create_account_cost", nil)
	versionedShadowSettlementCreateWitnessCostConflictCounter = metrics.NewRegisteredCounter("core/versioned_shadow/settlement/conflict/total_create_witness_cost", nil)
	versionedShadowLastTransactionsGauge                      = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/transactions", nil)
	versionedShadowLastFirstPassValidGauge                    = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/first_pass_valid", nil)
	versionedShadowLastNormalizedFirstPassValidGauge          = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/normalized_first_pass_valid", nil)
	versionedShadowLastConflictsGauge                         = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/conflicts", nil)
	versionedShadowLastUnsupportedGauge                       = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/unsupported", nil)
	versionedShadowLastMaxDependencyDistanceGauge             = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/max_dependency_distance", nil)
)

// versionedAccessShadow is P4.2/P4.3's observe-only OCC validator. Canonical
// serial execution records the exact logical cells each transaction read and
// wrote. The block-local version map first asks whether that result would still
// be valid had the transaction read the block-start snapshot, matching Erigon's
// read-version validation rule. A second result models audited settlement
// accumulators as worker-returned deltas applied in original order.
//
// No execution result is copied or published. Unsupported range reads and
// unknown journal entries are counted conservatively and canonical state stays
// entirely on the existing serial path.
type versionedAccessShadow struct {
	recorder state.TransactionAccessRecorder
	versions map[state.TransactionAccessKey]int
	stats    versionedAccessShadowStats
}

type versionedAccessShadowStats struct {
	transactions                         int64
	firstPassValid                       int64
	normalizedFirstPassValid             int64
	conflicts                            int64
	normalizedConflicts                  int64
	unsupported                          int64
	readCells                            int64
	writeCells                           int64
	readConflicts                        int64
	writeConflicts                       int64
	accountConflicts                     int64
	storageConflicts                     int64
	accountKVConflicts                   int64
	dynamicConflicts                     int64
	otherConflicts                       int64
	vmTransactions                       int64
	vmFirstPassValid                     int64
	vmNormalizedFirstPass                int64
	transferTransactions                 int64
	transferFirstPass                    int64
	transferNormalizedFirstPass          int64
	otherTransactions                    int64
	otherFirstPassValid                  int64
	otherNormalizedFirstPass             int64
	settlementTaggedTransactions         int64
	settlementResolvedFirstPass          int64
	settlementBlackholeConflicts         int64
	settlementBurnConflicts              int64
	settlementFeePoolConflicts           int64
	settlementTransactionCostConflicts   int64
	settlementCreateAccountCostConflicts int64
	settlementCreateWitnessCostConflicts int64
	maxDependencyDistance                int64
}

func (s *versionedAccessShadow) Prepare(transactionCount int) {
	const maxVersionHint = 16 * 1024
	hint := transactionCount * 8
	if hint < 64 {
		hint = 64
	}
	if hint > maxVersionHint {
		hint = maxVersionHint
	}
	s.versions = make(map[state.TransactionAccessKey]int, hint)
}

func (s *versionedAccessShadow) BeginTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	s.recorder.Reset(64)
	statedb.SetTransactionAccessRecorder(&s.recorder)
	dynProps.SetTransactionAccessRecorder(&s.recorder)
}

func (s *versionedAccessShadow) detach(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	statedb.SetTransactionAccessRecorder(nil)
	dynProps.SetTransactionAccessRecorder(nil)
}

func (s *versionedAccessShadow) ObserveTransaction(txIndex int, tx *types.Transaction, statedb *state.StateDB, dynProps *state.DynamicProperties, journalMark int) {
	s.detach(statedb, dynProps)
	s.stats.transactions++

	var (
		readConflict                        bool
		normalizedReadConflict              bool
		writeConflict                       bool
		accountConflict                     bool
		storageConflict                     bool
		accountKVConflict                   bool
		dynamicConflict                     bool
		otherConflict                       bool
		settlementTagged                    bool
		settlementBlackholeConflict         bool
		settlementBurnConflict              bool
		settlementFeePoolConflict           bool
		settlementTransactionCostConflict   bool
		settlementCreateAccountCostConflict bool
		settlementCreateWitnessCostConflict bool
		maxDependencyIndex                  = -1
	)
	recordWriteConflict := func(key state.TransactionAccessKey) {
		previous, ok := s.versions[key]
		if !ok || previous >= txIndex {
			return
		}
		writeConflict = true
	}
	recordReadConflict := func(key state.TransactionAccessKey, mode state.TransactionAccessMode) {
		previous, ok := s.versions[key]
		if !ok || previous >= txIndex {
			return
		}
		readConflict = true
		if mode&state.TransactionAccessRead != 0 {
			normalizedReadConflict = true
		}
		if previous > maxDependencyIndex {
			maxDependencyIndex = previous
		}
		switch key.Kind {
		case state.TransactionAccessAccount, state.TransactionAccessWitness:
			accountConflict = true
		case state.TransactionAccessStorage, state.TransactionAccessTransientStorage:
			storageConflict = true
		case state.TransactionAccessAccountKV, state.TransactionAccessAccountKVGeneration:
			accountKVConflict = true
		case state.TransactionAccessDynamicInt, state.TransactionAccessDynamicString, state.TransactionAccessDynamicHash:
			dynamicConflict = true
		default:
			otherConflict = true
		}
		if mode&state.TransactionAccessCommutativeRead != 0 {
			switch {
			case key.Kind == state.TransactionAccessAccount:
				settlementBlackholeConflict = true
			case key.Kind == state.TransactionAccessDynamicInt:
				switch key.LogicalKey {
				case "burn_trx_amount":
					settlementBurnConflict = true
				case "transaction_fee_pool":
					settlementFeePoolConflict = true
				case "total_transaction_cost":
					settlementTransactionCostConflict = true
				case "total_create_account_cost":
					settlementCreateAccountCostConflict = true
				case "total_create_witness_cost":
					settlementCreateWitnessCostConflict = true
				}
			}
		}
	}

	// Reads and DynamicProperties writes were captured inline. Validate every
	// read before installing this transaction's writes into the version map.
	s.recorder.Visit(func(key state.TransactionAccessKey, mode state.TransactionAccessMode) bool {
		if mode&(state.TransactionAccessRead|state.TransactionAccessCommutativeRead) != 0 {
			s.stats.readCells++
			recordReadConflict(key, mode)
		}
		if mode&(state.TransactionAccessWrite|state.TransactionAccessCommutativeWrite) != 0 {
			s.stats.writeCells++
			recordWriteConflict(key)
		}
		if mode&(state.TransactionAccessCommutativeRead|state.TransactionAccessCommutativeWrite) != 0 {
			settlementTagged = true
		}
		return true
	})

	knownWrites := statedb.VisitTransactionAccessWritesSince(journalMark, func(key state.TransactionAccessKey) bool {
		s.stats.writeCells++
		recordWriteConflict(key)
		return true
	})

	unsupported := s.recorder.Unsupported() || !knownWrites
	// Erigon validates versions read by the worker. A blind write overlapping an
	// earlier write is still publishable in original order; track that overlap
	// separately, but do not turn it into a false read-version invalidation.
	conflict := readConflict
	normalizedConflict := normalizedReadConflict
	if unsupported {
		s.stats.unsupported++
	}
	if conflict {
		s.stats.conflicts++
		if readConflict {
			s.stats.readConflicts++
		}
		if accountConflict {
			s.stats.accountConflicts++
		}
		if storageConflict {
			s.stats.storageConflicts++
		}
		if accountKVConflict {
			s.stats.accountKVConflicts++
		}
		if dynamicConflict {
			s.stats.dynamicConflicts++
		}
		if otherConflict {
			s.stats.otherConflicts++
		}
		if distance := int64(txIndex - maxDependencyIndex); distance > s.stats.maxDependencyDistance {
			s.stats.maxDependencyDistance = distance
		}
	}
	if normalizedConflict {
		s.stats.normalizedConflicts++
	}
	if writeConflict {
		s.stats.writeConflicts++
	}
	if settlementTagged {
		s.stats.settlementTaggedTransactions++
	}
	if settlementBlackholeConflict {
		s.stats.settlementBlackholeConflicts++
	}
	if settlementBurnConflict {
		s.stats.settlementBurnConflicts++
	}
	if settlementFeePoolConflict {
		s.stats.settlementFeePoolConflicts++
	}
	if settlementTransactionCostConflict {
		s.stats.settlementTransactionCostConflicts++
	}
	if settlementCreateAccountCostConflict {
		s.stats.settlementCreateAccountCostConflicts++
	}
	if settlementCreateWitnessCostConflict {
		s.stats.settlementCreateWitnessCostConflicts++
	}

	firstPassValid := !unsupported && !conflict
	normalizedFirstPassValid := !unsupported && !normalizedConflict
	s.classify(tx, firstPassValid, normalizedFirstPassValid)
	if firstPassValid {
		s.stats.firstPassValid++
	}
	if normalizedFirstPassValid {
		s.stats.normalizedFirstPassValid++
	}
	if !unsupported && conflict && !normalizedConflict {
		s.stats.settlementResolvedFirstPass++
	}

	// Only after validation do this transaction's serially-authoritative writes
	// become the latest versions visible to later speculative attempts.
	s.recorder.Visit(func(key state.TransactionAccessKey, mode state.TransactionAccessMode) bool {
		if mode&(state.TransactionAccessWrite|state.TransactionAccessCommutativeWrite) != 0 {
			s.versions[key] = txIndex
		}
		return true
	})
	statedb.VisitTransactionAccessWritesSince(journalMark, func(key state.TransactionAccessKey) bool {
		s.versions[key] = txIndex
		return true
	})
}

func (s *versionedAccessShadow) classify(tx *types.Transaction, firstPassValid, normalizedFirstPassValid bool) {
	contractType := corepb.Transaction_Contract_AccountCreateContract
	if tx != nil {
		contractType = tx.ContractType()
	}
	switch contractType {
	case corepb.Transaction_Contract_TriggerSmartContract, corepb.Transaction_Contract_CreateSmartContract:
		s.stats.vmTransactions++
		if firstPassValid {
			s.stats.vmFirstPassValid++
		}
		if normalizedFirstPassValid {
			s.stats.vmNormalizedFirstPass++
		}
	case corepb.Transaction_Contract_TransferContract:
		s.stats.transferTransactions++
		if firstPassValid {
			s.stats.transferFirstPass++
		}
		if normalizedFirstPassValid {
			s.stats.transferNormalizedFirstPass++
		}
	default:
		s.stats.otherTransactions++
		if firstPassValid {
			s.stats.otherFirstPassValid++
		}
		if normalizedFirstPassValid {
			s.stats.otherNormalizedFirstPass++
		}
	}
}

func (s *versionedAccessShadow) Finish(statedb *state.StateDB, dynProps *state.DynamicProperties) versionedAccessShadowStats {
	s.detach(statedb, dynProps)
	return s.stats
}

func (s *versionedAccessShadow) Publish(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	stats := s.Finish(statedb, dynProps)
	versionedShadowBlocksCounter.Inc(1)
	versionedShadowTransactionsCounter.Inc(stats.transactions)
	versionedShadowFirstPassValidCounter.Inc(stats.firstPassValid)
	versionedShadowNormalizedFirstPassValidCounter.Inc(stats.normalizedFirstPassValid)
	versionedShadowConflictsCounter.Inc(stats.conflicts)
	versionedShadowNormalizedConflictsCounter.Inc(stats.normalizedConflicts)
	versionedShadowUnsupportedCounter.Inc(stats.unsupported)
	versionedShadowReadCellsCounter.Inc(stats.readCells)
	versionedShadowWriteCellsCounter.Inc(stats.writeCells)
	versionedShadowReadConflictsCounter.Inc(stats.readConflicts)
	versionedShadowWriteConflictsCounter.Inc(stats.writeConflicts)
	versionedShadowAccountConflictsCounter.Inc(stats.accountConflicts)
	versionedShadowStorageConflictsCounter.Inc(stats.storageConflicts)
	versionedShadowAccountKVConflictsCounter.Inc(stats.accountKVConflicts)
	versionedShadowDynamicConflictsCounter.Inc(stats.dynamicConflicts)
	versionedShadowOtherConflictsCounter.Inc(stats.otherConflicts)
	versionedShadowVMTransactionsCounter.Inc(stats.vmTransactions)
	versionedShadowVMFirstPassValidCounter.Inc(stats.vmFirstPassValid)
	versionedShadowVMNormalizedFirstPassCounter.Inc(stats.vmNormalizedFirstPass)
	versionedShadowTransferTransactionsCounter.Inc(stats.transferTransactions)
	versionedShadowTransferFirstPassValidCounter.Inc(stats.transferFirstPass)
	versionedShadowTransferNormalizedFirstPassCounter.Inc(stats.transferNormalizedFirstPass)
	versionedShadowOtherTransactionsCounter.Inc(stats.otherTransactions)
	versionedShadowOtherFirstPassValidCounter.Inc(stats.otherFirstPassValid)
	versionedShadowOtherNormalizedFirstPassCounter.Inc(stats.otherNormalizedFirstPass)
	versionedShadowSettlementTaggedCounter.Inc(stats.settlementTaggedTransactions)
	versionedShadowSettlementResolvedCounter.Inc(stats.settlementResolvedFirstPass)
	versionedShadowSettlementBlackholeConflictCounter.Inc(stats.settlementBlackholeConflicts)
	versionedShadowSettlementBurnConflictCounter.Inc(stats.settlementBurnConflicts)
	versionedShadowSettlementFeePoolConflictCounter.Inc(stats.settlementFeePoolConflicts)
	versionedShadowSettlementTransactionCostConflictCounter.Inc(stats.settlementTransactionCostConflicts)
	versionedShadowSettlementCreateAccountCostConflictCounter.Inc(stats.settlementCreateAccountCostConflicts)
	versionedShadowSettlementCreateWitnessCostConflictCounter.Inc(stats.settlementCreateWitnessCostConflicts)
	versionedShadowLastTransactionsGauge.Update(stats.transactions)
	versionedShadowLastFirstPassValidGauge.Update(stats.firstPassValid)
	versionedShadowLastNormalizedFirstPassValidGauge.Update(stats.normalizedFirstPassValid)
	versionedShadowLastConflictsGauge.Update(stats.conflicts)
	versionedShadowLastUnsupportedGauge.Update(stats.unsupported)
	versionedShadowLastMaxDependencyDistanceGauge.Update(stats.maxDependencyDistance)
}
