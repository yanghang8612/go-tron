package core

import (
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	versionedShadowBlocksCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/blocks", nil)
	versionedShadowTransactionsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/transactions", nil)
	versionedShadowFirstPassValidCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/first_pass_valid", nil)
	versionedShadowConflictsCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/conflicts", nil)
	versionedShadowUnsupportedCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/unsupported", nil)
	versionedShadowReadCellsCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/read_cells", nil)
	versionedShadowWriteCellsCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/write_cells", nil)
	versionedShadowReadConflictsCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/read_conflicts", nil)
	versionedShadowWriteConflictsCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/write_conflicts", nil)
	versionedShadowAccountConflictsCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account", nil)
	versionedShadowStorageConflictsCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/storage", nil)
	versionedShadowAccountKVConflictsCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account_kv", nil)
	versionedShadowDynamicConflictsCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/dynamic", nil)
	versionedShadowOtherConflictsCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/other", nil)
	versionedShadowVMTransactionsCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/vm/transactions", nil)
	versionedShadowVMFirstPassValidCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/vm/first_pass_valid", nil)
	versionedShadowTransferTransactionsCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/transactions", nil)
	versionedShadowTransferFirstPassValidCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/first_pass_valid", nil)
	versionedShadowOtherTransactionsCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/other/transactions", nil)
	versionedShadowOtherFirstPassValidCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/other/first_pass_valid", nil)
	versionedShadowLastTransactionsGauge          = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/transactions", nil)
	versionedShadowLastFirstPassValidGauge        = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/first_pass_valid", nil)
	versionedShadowLastConflictsGauge             = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/conflicts", nil)
	versionedShadowLastUnsupportedGauge           = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/unsupported", nil)
	versionedShadowLastMaxDependencyDistanceGauge = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/max_dependency_distance", nil)
)

// versionedAccessShadow is P4.2's observe-only OCC validator. Canonical serial
// execution records the exact logical cells each transaction read and wrote.
// The block-local version map then asks whether that result would still be
// valid had the transaction's first speculative attempt read the block-start
// snapshot, matching Erigon's read-version validation rule.
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
	transactions          int64
	firstPassValid        int64
	conflicts             int64
	unsupported           int64
	readCells             int64
	writeCells            int64
	readConflicts         int64
	writeConflicts        int64
	accountConflicts      int64
	storageConflicts      int64
	accountKVConflicts    int64
	dynamicConflicts      int64
	otherConflicts        int64
	vmTransactions        int64
	vmFirstPassValid      int64
	transferTransactions  int64
	transferFirstPass     int64
	otherTransactions     int64
	otherFirstPassValid   int64
	maxDependencyDistance int64
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
		readConflict       bool
		writeConflict      bool
		accountConflict    bool
		storageConflict    bool
		accountKVConflict  bool
		dynamicConflict    bool
		otherConflict      bool
		maxDependencyIndex = -1
	)
	recordConflict := func(key state.TransactionAccessKey, read bool) {
		previous, ok := s.versions[key]
		if !ok || previous >= txIndex {
			return
		}
		if read {
			readConflict = true
		} else {
			writeConflict = true
			return
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
	}

	// Reads and DynamicProperties writes were captured inline. Validate every
	// read before installing this transaction's writes into the version map.
	s.recorder.Visit(func(key state.TransactionAccessKey, mode state.TransactionAccessMode) bool {
		if mode&state.TransactionAccessRead != 0 {
			s.stats.readCells++
			recordConflict(key, true)
		}
		if mode&state.TransactionAccessWrite != 0 {
			s.stats.writeCells++
			recordConflict(key, false)
		}
		return true
	})

	knownWrites := statedb.VisitTransactionAccessWritesSince(journalMark, func(key state.TransactionAccessKey) bool {
		s.stats.writeCells++
		recordConflict(key, false)
		return true
	})

	unsupported := s.recorder.Unsupported() || !knownWrites
	// Erigon validates versions read by the worker. A blind write overlapping an
	// earlier write is still publishable in original order; track that overlap
	// separately, but do not turn it into a false read-version invalidation.
	conflict := readConflict
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
	if writeConflict {
		s.stats.writeConflicts++
	}

	firstPassValid := !unsupported && !conflict
	s.classify(tx, firstPassValid)
	if firstPassValid {
		s.stats.firstPassValid++
	}

	// Only after validation do this transaction's serially-authoritative writes
	// become the latest versions visible to later speculative attempts.
	s.recorder.Visit(func(key state.TransactionAccessKey, mode state.TransactionAccessMode) bool {
		if mode&state.TransactionAccessWrite != 0 {
			s.versions[key] = txIndex
		}
		return true
	})
	statedb.VisitTransactionAccessWritesSince(journalMark, func(key state.TransactionAccessKey) bool {
		s.versions[key] = txIndex
		return true
	})
}

func (s *versionedAccessShadow) classify(tx *types.Transaction, firstPassValid bool) {
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
	case corepb.Transaction_Contract_TransferContract:
		s.stats.transferTransactions++
		if firstPassValid {
			s.stats.transferFirstPass++
		}
	default:
		s.stats.otherTransactions++
		if firstPassValid {
			s.stats.otherFirstPassValid++
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
	versionedShadowConflictsCounter.Inc(stats.conflicts)
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
	versionedShadowTransferTransactionsCounter.Inc(stats.transferTransactions)
	versionedShadowTransferFirstPassValidCounter.Inc(stats.transferFirstPass)
	versionedShadowOtherTransactionsCounter.Inc(stats.otherTransactions)
	versionedShadowOtherFirstPassValidCounter.Inc(stats.otherFirstPassValid)
	versionedShadowLastTransactionsGauge.Update(stats.transactions)
	versionedShadowLastFirstPassValidGauge.Update(stats.firstPassValid)
	versionedShadowLastConflictsGauge.Update(stats.conflicts)
	versionedShadowLastUnsupportedGauge.Update(stats.unsupported)
	versionedShadowLastMaxDependencyDistanceGauge.Update(stats.maxDependencyDistance)
}
