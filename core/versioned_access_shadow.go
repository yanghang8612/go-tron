package core

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	versionedShadowBlocksCounter                              = metrics.NewRegisteredCounter("core/versioned_shadow/blocks", nil)
	versionedShadowTransactionsCounter                        = metrics.NewRegisteredCounter("core/versioned_shadow/transactions", nil)
	versionedShadowFirstPassValidCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/first_pass_valid", nil)
	versionedShadowNormalizedFirstPassValidCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/normalized_first_pass_valid", nil)
	versionedShadowTypedFirstPassValidCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/typed_first_pass_valid", nil)
	versionedShadowSenderFirstPassValidCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized_first_pass_valid", nil)
	versionedShadowConflictsCounter                           = metrics.NewRegisteredCounter("core/versioned_shadow/conflicts", nil)
	versionedShadowNormalizedConflictsCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/normalized_conflicts", nil)
	versionedShadowTypedConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/typed_conflicts", nil)
	versionedShadowSenderConflictsCounter                     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized_conflicts", nil)
	versionedShadowUnsupportedCounter                         = metrics.NewRegisteredCounter("core/versioned_shadow/unsupported", nil)
	versionedShadowReadCellsCounter                           = metrics.NewRegisteredCounter("core/versioned_shadow/read_cells", nil)
	versionedShadowWriteCellsCounter                          = metrics.NewRegisteredCounter("core/versioned_shadow/write_cells", nil)
	versionedShadowReadConflictsCounter                       = metrics.NewRegisteredCounter("core/versioned_shadow/read_conflicts", nil)
	versionedShadowWriteConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/write_conflicts", nil)
	versionedShadowAccountConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account", nil)
	versionedShadowStorageConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/storage", nil)
	versionedShadowAccountKVConflictsCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/account_kv", nil)
	versionedShadowDynamicConflictsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/dynamic", nil)
	versionedShadowRawKVConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/raw_kv", nil)
	versionedShadowOtherConflictsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/conflict/other", nil)
	versionedShadowVMTransactionsCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/vm/transactions", nil)
	versionedShadowVMFirstPassValidCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/vm/first_pass_valid", nil)
	versionedShadowVMNormalizedFirstPassCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/vm/normalized_first_pass_valid", nil)
	versionedShadowVMTypedFirstPassCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/vm/typed_first_pass_valid", nil)
	versionedShadowVMSenderFirstPassCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/vm/sender_serialized_first_pass_valid", nil)
	versionedShadowTransferTransactionsCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/transactions", nil)
	versionedShadowTransferFirstPassValidCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/first_pass_valid", nil)
	versionedShadowTransferNormalizedFirstPassCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/normalized_first_pass_valid", nil)
	versionedShadowTransferTypedFirstPassCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/typed_first_pass_valid", nil)
	versionedShadowTransferSenderFirstPassCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/transfer/sender_serialized_first_pass_valid", nil)
	versionedShadowOtherTransactionsCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/other/transactions", nil)
	versionedShadowOtherFirstPassValidCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/other/first_pass_valid", nil)
	versionedShadowOtherNormalizedFirstPassCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/other/normalized_first_pass_valid", nil)
	versionedShadowOtherTypedFirstPassCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/other/typed_first_pass_valid", nil)
	versionedShadowOtherSenderFirstPassCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/other/sender_serialized_first_pass_valid", nil)
	versionedShadowSenderDependencyTaggedCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_dependency/tagged_transactions", nil)
	versionedShadowSenderDependencyResolvedCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_dependency/resolved_first_pass", nil)
	versionedShadowSenderAccountConflictCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/conflict/account", nil)
	versionedShadowSenderStorageConflictCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/conflict/storage", nil)
	versionedShadowSenderAccountKVConflictCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/conflict/account_kv", nil)
	versionedShadowSenderDynamicConflictCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/conflict/dynamic", nil)
	versionedShadowSenderOtherConflictCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/conflict/other", nil)
	versionedShadowSenderCoarseConflictCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/coarse", nil)
	versionedShadowSenderExistenceConflictCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/existence", nil)
	versionedShadowSenderTypeConflictCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/type", nil)
	versionedShadowSenderBalanceConflictCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/balance", nil)
	versionedShadowSenderAllowanceConflictCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/allowance", nil)
	versionedShadowSenderBandwidthConflictCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/bandwidth", nil)
	versionedShadowSenderFrozenResourceConflictCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/sender_serialized/account_field/conflict/frozen_resource", nil)
	versionedShadowDependencyDAGWavesCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/waves", nil)
	versionedShadowDependencyDAGParallelTransactionsCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/parallel_transactions", nil)
	versionedShadowDAGSerialNanosCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/serial_execution_nanos", nil)
	versionedShadowDAGWaveNanosCounter                        = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/wave_execution_nanos", nil)
	versionedShadowDAGFourWorkerNanosCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/four_worker_execution_nanos", nil)
	versionedShadowReadyCriticalNanosCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/ready_queue/critical_path_execution_nanos", nil)
	versionedShadowReadyFourWorkerNanosCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/dependency_dag/ready_queue/four_worker_execution_nanos", nil)
	versionedShadowTypedResolvedCounter                       = metrics.NewRegisteredCounter("core/versioned_shadow/typed/resolved_first_pass", nil)
	versionedShadowTypedAccountConflictCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/typed/conflict/account", nil)
	versionedShadowTypedAccountCoarseConflictCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/coarse", nil)
	versionedShadowTypedAccountExistenceConflictCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/existence", nil)
	versionedShadowTypedAccountTypeConflictCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/type", nil)
	versionedShadowTypedAccountBalanceConflictCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/balance", nil)
	versionedShadowTypedAccountAllowanceConflictCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/allowance", nil)
	versionedShadowTypedAccountBandwidthConflictCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/bandwidth", nil)
	versionedShadowTypedAccountFrozenResourceConflictCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/typed/account_field/conflict/frozen_resource", nil)
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
	versionedShadowLastTypedFirstPassValidGauge               = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/typed_first_pass_valid", nil)
	versionedShadowLastSenderFirstPassValidGauge              = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/sender_serialized_first_pass_valid", nil)
	versionedShadowLastMaxSenderChainDepthGauge               = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/max_sender_chain_depth", nil)
	versionedShadowLastDependencyDAGWavesGauge                = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/waves", nil)
	versionedShadowLastDependencyDAGMaxWidthGauge             = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/max_width", nil)
	versionedShadowLastDependencyDAGParallelTransactionsGauge = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/parallel_transactions", nil)
	versionedShadowLastDAGSerialNanosGauge                    = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/serial_execution_nanos", nil)
	versionedShadowLastDAGWaveNanosGauge                      = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/wave_execution_nanos", nil)
	versionedShadowLastDAGFourWorkerNanosGauge                = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/four_worker_execution_nanos", nil)
	versionedShadowLastReadyCriticalNanosGauge                = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/ready_queue/critical_path_execution_nanos", nil)
	versionedShadowLastReadyFourWorkerNanosGauge              = metrics.NewRegisteredGauge("core/versioned_shadow/last_block/dependency_dag/ready_queue/four_worker_execution_nanos", nil)
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
	recorder             state.TransactionAccessRecorder
	versions             map[state.TransactionAccessKey]int
	rawAccountVersions   map[tcommon.Address]int
	accountFullVersions  map[tcommon.Address]int
	accountAnyVersions   map[tcommon.Address]int
	accountFieldVersions map[state.TransactionAccountFieldKey]int
	transactionOwners    []tcommon.Address
	transactionHasOwner  []bool
	senderChainDepths    []int
	lastSenderTx         map[tcommon.Address]int
	dependencyWaves      []int
	dependencyWaveWidths []int
	dependencyHeads      []int
	dependencyEdges      []transactionDependencyEdge
	transactionSupported []bool
	transactionDurations []int64
	transactionWriteSets []state.TransactionWriteSet
	transactionWritesOK  []bool
	transactionStarted   time.Time
	lastBarrierTx        int
	dependencyMinWave    int
	dependencyMaxWave    int
	stats                versionedAccessShadowStats
}

func (s *versionedAccessShadow) EnableWriteSetCapture(transactionCount int) {
	s.transactionWriteSets = make([]state.TransactionWriteSet, transactionCount)
	s.transactionWritesOK = make([]bool, transactionCount)
}

type versionedAccessShadowStats struct {
	transactions                         int64
	firstPassValid                       int64
	normalizedFirstPassValid             int64
	typedFirstPassValid                  int64
	senderFirstPassValid                 int64
	conflicts                            int64
	normalizedConflicts                  int64
	typedConflicts                       int64
	senderConflicts                      int64
	unsupported                          int64
	readCells                            int64
	writeCells                           int64
	readConflicts                        int64
	writeConflicts                       int64
	accountConflicts                     int64
	storageConflicts                     int64
	accountKVConflicts                   int64
	dynamicConflicts                     int64
	rawKVConflicts                       int64
	otherConflicts                       int64
	vmTransactions                       int64
	vmFirstPassValid                     int64
	vmNormalizedFirstPass                int64
	vmTypedFirstPass                     int64
	vmSenderFirstPass                    int64
	transferTransactions                 int64
	transferFirstPass                    int64
	transferNormalizedFirstPass          int64
	transferTypedFirstPass               int64
	transferSenderFirstPass              int64
	otherTransactions                    int64
	otherFirstPassValid                  int64
	otherNormalizedFirstPass             int64
	otherTypedFirstPass                  int64
	otherSenderFirstPass                 int64
	senderDependencyTaggedTransactions   int64
	senderDependencyResolvedFirstPass    int64
	maxSenderChainDepth                  int64
	senderAccountConflicts               int64
	senderStorageConflicts               int64
	senderAccountKVConflicts             int64
	senderDynamicConflicts               int64
	senderOtherConflicts                 int64
	senderCoarseConflicts                int64
	senderExistenceConflicts             int64
	senderTypeConflicts                  int64
	senderBalanceConflicts               int64
	senderAllowanceConflicts             int64
	senderBandwidthConflicts             int64
	senderFrozenResourceConflicts        int64
	dependencyDAGWaves                   int64
	dependencyDAGMaxWidth                int64
	dependencyDAGParallelTransactions    int64
	dependencyDAGSerialNanos             int64
	dependencyDAGWaveNanos               int64
	dependencyDAGFourWorkerNanos         int64
	dependencyDAGReadyCriticalNanos      int64
	dependencyDAGReadyFourWorkerNanos    int64
	typedResolvedFirstPass               int64
	typedAccountConflicts                int64
	typedAccountCoarseConflicts          int64
	typedAccountExistenceConflicts       int64
	typedAccountTypeConflicts            int64
	typedAccountBalanceConflicts         int64
	typedAccountAllowanceConflicts       int64
	typedAccountBandwidthConflicts       int64
	typedAccountFrozenResourceConflicts  int64
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

type transactionDependencyEdge struct {
	predecessor int
	dependent   int
	next        int
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
	s.rawAccountVersions = make(map[tcommon.Address]int, hint/4)
	s.accountFullVersions = make(map[tcommon.Address]int, hint/8)
	s.accountAnyVersions = make(map[tcommon.Address]int, hint/4)
	s.accountFieldVersions = make(map[state.TransactionAccountFieldKey]int, hint/2)
	s.transactionOwners = make([]tcommon.Address, transactionCount)
	s.transactionHasOwner = make([]bool, transactionCount)
	s.senderChainDepths = make([]int, transactionCount)
	s.lastSenderTx = make(map[tcommon.Address]int, transactionCount/4+1)
	s.dependencyWaves = make([]int, transactionCount)
	s.dependencyWaveWidths = make([]int, 0, transactionCount)
	s.dependencyHeads = make([]int, transactionCount)
	for txIndex := range s.dependencyHeads {
		s.dependencyHeads[txIndex] = -1
	}
	s.dependencyEdges = make([]transactionDependencyEdge, 0, transactionCount*2)
	s.transactionSupported = make([]bool, transactionCount)
	s.transactionDurations = make([]int64, transactionCount)
	s.dependencyMaxWave = -1
	s.lastBarrierTx = -1
}

func (s *versionedAccessShadow) addDependency(txIndex, predecessor int) {
	if txIndex < 0 || txIndex >= len(s.dependencyHeads) || predecessor < 0 || predecessor >= txIndex {
		return
	}
	for edgeIndex := s.dependencyHeads[txIndex]; edgeIndex >= 0; edgeIndex = s.dependencyEdges[edgeIndex].next {
		if s.dependencyEdges[edgeIndex].predecessor == predecessor {
			return
		}
	}
	s.dependencyEdges = append(s.dependencyEdges, transactionDependencyEdge{
		predecessor: predecessor,
		dependent:   txIndex,
		next:        s.dependencyHeads[txIndex],
	})
	s.dependencyHeads[txIndex] = len(s.dependencyEdges) - 1
}

// observeSenderDependency mirrors Erigon's pre-execution prevSenderTx edge:
// transactions from one sender are never dispatched concurrently. The model
// remains conservative for dependencies written by any different sender.
func (s *versionedAccessShadow) observeSenderDependency(txIndex int, tx *types.Transaction) (tcommon.Address, bool) {
	if tx == nil || tx.Contract() == nil || txIndex < 0 || txIndex >= len(s.transactionOwners) {
		return tcommon.Address{}, false
	}
	ownerBytes, shielded, err := tx.ContractOwnerAddress()
	if err != nil || shielded || len(ownerBytes) != tcommon.AddressLength {
		return tcommon.Address{}, false
	}
	owner := tcommon.BytesToAddress(ownerBytes)
	if !owner.ValidPrefix() {
		return tcommon.Address{}, false
	}
	s.transactionOwners[txIndex] = owner
	s.transactionHasOwner[txIndex] = true
	depth := 1
	if previous, ok := s.lastSenderTx[owner]; ok {
		s.stats.senderDependencyTaggedTransactions++
		depth = s.senderChainDepths[previous] + 1
		s.addDependency(txIndex, previous)
	}
	s.senderChainDepths[txIndex] = depth
	s.lastSenderTx[owner] = txIndex
	if int64(depth) > s.stats.maxSenderChainDepth {
		s.stats.maxSenderChainDepth = int64(depth)
	}
	return owner, true
}

func (s *versionedAccessShadow) writtenBySender(txIndex int, owner tcommon.Address, hasOwner bool) bool {
	return hasOwner && txIndex >= 0 && txIndex < len(s.transactionOwners) &&
		s.transactionHasOwner[txIndex] && s.transactionOwners[txIndex] == owner
}

func (s *versionedAccessShadow) BeginTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	s.recorder.Reset(64)
	statedb.SetTransactionAccessRecorder(&s.recorder)
	dynProps.SetTransactionAccessRecorder(&s.recorder)
	s.transactionStarted = time.Now()
}

func (s *versionedAccessShadow) detach(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	statedb.SetTransactionAccessRecorder(nil)
	dynProps.SetTransactionAccessRecorder(nil)
}

func shadowPreviousVersion[K comparable](versions map[K]int, key K, txIndex int) (int, bool) {
	previous, ok := versions[key]
	return previous, ok && previous < txIndex
}

func (s *versionedAccessShadow) rawPreviousVersion(key state.TransactionAccessKey, txIndex int) (int, bool) {
	switch key.Kind {
	case state.TransactionAccessAccount, state.TransactionAccessAccountField:
		return shadowPreviousVersion(s.rawAccountVersions, key.Address, txIndex)
	default:
		return shadowPreviousVersion(s.versions, key, txIndex)
	}
}

func (s *versionedAccessShadow) typedPreviousVersion(key state.TransactionAccessKey, txIndex int) (int, bool) {
	switch key.Kind {
	case state.TransactionAccessAccount:
		return shadowPreviousVersion(s.accountAnyVersions, key.Address, txIndex)
	case state.TransactionAccessAccountField:
		full, fullOK := shadowPreviousVersion(s.accountFullVersions, key.Address, txIndex)
		fieldKey := state.TransactionAccountFieldKey{Address: key.Address, Field: key.AccountField}
		field, fieldOK := shadowPreviousVersion(s.accountFieldVersions, fieldKey, txIndex)
		if !fullOK {
			return field, fieldOK
		}
		if !fieldOK || full >= field {
			return full, true
		}
		return field, true
	default:
		return shadowPreviousVersion(s.versions, key, txIndex)
	}
}

func (s *versionedAccessShadow) installRecordedWrite(key state.TransactionAccessKey, txIndex int) {
	switch key.Kind {
	case state.TransactionAccessAccount:
		s.rawAccountVersions[key.Address] = txIndex
		s.accountFullVersions[key.Address] = txIndex
		s.accountAnyVersions[key.Address] = txIndex
	case state.TransactionAccessAccountField:
		s.rawAccountVersions[key.Address] = txIndex
		fieldKey := state.TransactionAccountFieldKey{Address: key.Address, Field: key.AccountField}
		s.accountFieldVersions[fieldKey] = txIndex
		s.accountAnyVersions[key.Address] = txIndex
	default:
		s.versions[key] = txIndex
	}
}

func (s *versionedAccessShadow) installJournalWrite(key state.TransactionAccessKey, txIndex int) {
	if key.Kind != state.TransactionAccessAccount {
		s.versions[key] = txIndex
		return
	}
	s.rawAccountVersions[key.Address] = txIndex
	full, fields := s.recorder.AccountWriteCoverage(key.Address)
	if full || !fields {
		s.accountFullVersions[key.Address] = txIndex
		s.accountAnyVersions[key.Address] = txIndex
	}
}

func (s *versionedAccessShadow) ObserveTransaction(txIndex int, tx *types.Transaction, statedb *state.StateDB, dynProps *state.DynamicProperties, journalMark int) {
	if txIndex >= 0 && txIndex < len(s.transactionDurations) && !s.transactionStarted.IsZero() {
		s.transactionDurations[txIndex] = time.Since(s.transactionStarted).Nanoseconds()
	}
	s.detach(statedb, dynProps)
	if txIndex >= 0 && txIndex < len(s.transactionWriteSets) {
		writes, known, captureErr := statedb.CaptureTransactionWriteSet(journalMark, &s.recorder, dynProps)
		if captureErr == nil && known {
			s.transactionWriteSets[txIndex] = writes
			s.transactionWritesOK[txIndex] = true
		}
	}
	s.stats.transactions++
	if s.lastBarrierTx >= 0 {
		s.addDependency(txIndex, s.lastBarrierTx)
	}
	owner, hasOwner := s.observeSenderDependency(txIndex, tx)
	dependencyWave := s.dependencyMinWave

	var (
		readConflict                        bool
		normalizedReadConflict              bool
		typedReadConflict                   bool
		senderReadConflict                  bool
		writeConflict                       bool
		accountConflict                     bool
		storageConflict                     bool
		accountKVConflict                   bool
		dynamicConflict                     bool
		rawKVConflict                       bool
		otherConflict                       bool
		typedAccountConflict                bool
		typedAccountCoarseConflict          bool
		typedAccountExistenceConflict       bool
		typedAccountTypeConflict            bool
		typedAccountBalanceConflict         bool
		typedAccountAllowanceConflict       bool
		typedAccountBandwidthConflict       bool
		typedAccountFrozenResourceConflict  bool
		senderAccountConflict               bool
		senderStorageConflict               bool
		senderAccountKVConflict             bool
		senderDynamicConflict               bool
		senderOtherConflict                 bool
		senderCoarseConflict                bool
		senderExistenceConflict             bool
		senderTypeConflict                  bool
		senderBalanceConflict               bool
		senderAllowanceConflict             bool
		senderBandwidthConflict             bool
		senderFrozenResourceConflict        bool
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
		_, writeConflictForKey := s.rawPreviousVersion(key, txIndex)
		writeConflict = writeConflict || writeConflictForKey
	}
	recordReadConflict := func(key state.TransactionAccessKey, mode state.TransactionAccessMode) {
		previous, rawConflictForKey := s.rawPreviousVersion(key, txIndex)
		if !rawConflictForKey {
			return
		}
		readConflict = true
		ordinaryRead := mode&state.TransactionAccessRead != 0
		if ordinaryRead {
			normalizedReadConflict = true
			if typedPrevious, typedConflictForKey := s.typedPreviousVersion(key, txIndex); typedConflictForKey {
				typedReadConflict = true
				s.addDependency(txIndex, typedPrevious)
				if typedPrevious >= 0 && typedPrevious < len(s.dependencyWaves) {
					if wave := s.dependencyWaves[typedPrevious] + 1; wave > dependencyWave {
						dependencyWave = wave
					}
				}
				if !s.writtenBySender(typedPrevious, owner, hasOwner) {
					senderReadConflict = true
					switch key.Kind {
					case state.TransactionAccessAccount:
						senderAccountConflict = true
						senderCoarseConflict = true
					case state.TransactionAccessAccountField:
						senderAccountConflict = true
						switch key.AccountField {
						case state.TransactionAccountFieldExistence:
							senderExistenceConflict = true
						case state.TransactionAccountFieldAccountType:
							senderTypeConflict = true
						case state.TransactionAccountFieldBalance:
							senderBalanceConflict = true
						case state.TransactionAccountFieldAllowance:
							senderAllowanceConflict = true
						case state.TransactionAccountFieldFrozenResource:
							senderFrozenResourceConflict = true
						case state.TransactionAccountFieldNetUsage,
							state.TransactionAccountFieldLatestOperationTime,
							state.TransactionAccountFieldLatestConsumeTime,
							state.TransactionAccountFieldFreeNetUsage,
							state.TransactionAccountFieldLatestConsumeFreeTime,
							state.TransactionAccountFieldNetWindow:
							senderBandwidthConflict = true
						}
					case state.TransactionAccessStorage, state.TransactionAccessTransientStorage:
						senderStorageConflict = true
					case state.TransactionAccessAccountKV, state.TransactionAccessAccountKVGeneration:
						senderAccountKVConflict = true
					case state.TransactionAccessDynamicInt, state.TransactionAccessDynamicString, state.TransactionAccessDynamicHash:
						senderDynamicConflict = true
					default:
						senderOtherConflict = true
					}
				}
				switch key.Kind {
				case state.TransactionAccessAccount:
					typedAccountConflict = true
					typedAccountCoarseConflict = true
				case state.TransactionAccessAccountField:
					typedAccountConflict = true
					switch key.AccountField {
					case state.TransactionAccountFieldExistence:
						typedAccountExistenceConflict = true
					case state.TransactionAccountFieldAccountType:
						typedAccountTypeConflict = true
					case state.TransactionAccountFieldBalance:
						typedAccountBalanceConflict = true
					case state.TransactionAccountFieldAllowance:
						typedAccountAllowanceConflict = true
					case state.TransactionAccountFieldFrozenResource:
						typedAccountFrozenResourceConflict = true
					case state.TransactionAccountFieldNetUsage,
						state.TransactionAccountFieldLatestOperationTime,
						state.TransactionAccountFieldLatestConsumeTime,
						state.TransactionAccountFieldFreeNetUsage,
						state.TransactionAccountFieldLatestConsumeFreeTime,
						state.TransactionAccountFieldNetWindow:
						typedAccountBandwidthConflict = true
					}
				}
			}
		}
		if previous > maxDependencyIndex {
			maxDependencyIndex = previous
		}
		switch key.Kind {
		case state.TransactionAccessAccount, state.TransactionAccessAccountField, state.TransactionAccessWitness:
			accountConflict = true
		case state.TransactionAccessStorage, state.TransactionAccessTransientStorage:
			storageConflict = true
		case state.TransactionAccessAccountKV, state.TransactionAccessAccountKVGeneration:
			accountKVConflict = true
		case state.TransactionAccessDynamicInt, state.TransactionAccessDynamicString, state.TransactionAccessDynamicHash:
			dynamicConflict = true
		case state.TransactionAccessRawKV:
			rawKVConflict = true
		default:
			otherConflict = true
		}
		if mode&state.TransactionAccessCommutativeRead != 0 {
			switch {
			case key.Kind == state.TransactionAccessAccount || key.Kind == state.TransactionAccessAccountField:
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
	if txIndex >= 0 && txIndex < len(s.transactionSupported) {
		s.transactionSupported[txIndex] = !unsupported
	}
	// Unknown/range dependencies are a serial barrier. Later known tasks may
	// form parallel waves again, but only after this transaction's barrier wave.
	if unsupported {
		for predecessor := 0; predecessor < txIndex; predecessor++ {
			s.addDependency(txIndex, predecessor)
		}
		s.lastBarrierTx = txIndex
		dependencyWave = s.dependencyMaxWave + 1
		s.dependencyMinWave = dependencyWave + 1
	}
	if txIndex >= 0 && txIndex < len(s.dependencyWaves) {
		s.dependencyWaves[txIndex] = dependencyWave
	}
	for len(s.dependencyWaveWidths) <= dependencyWave {
		s.dependencyWaveWidths = append(s.dependencyWaveWidths, 0)
	}
	s.dependencyWaveWidths[dependencyWave]++
	if dependencyWave > s.dependencyMaxWave {
		s.dependencyMaxWave = dependencyWave
	}
	// Erigon validates versions read by the worker. A blind write overlapping an
	// earlier write is still publishable in original order; track that overlap
	// separately, but do not turn it into a false read-version invalidation.
	conflict := readConflict
	normalizedConflict := normalizedReadConflict
	typedConflict := typedReadConflict
	senderConflict := senderReadConflict
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
		if rawKVConflict {
			s.stats.rawKVConflicts++
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
	if typedConflict {
		s.stats.typedConflicts++
	}
	if senderConflict {
		s.stats.senderConflicts++
	}
	if senderAccountConflict {
		s.stats.senderAccountConflicts++
	}
	if senderStorageConflict {
		s.stats.senderStorageConflicts++
	}
	if senderAccountKVConflict {
		s.stats.senderAccountKVConflicts++
	}
	if senderDynamicConflict {
		s.stats.senderDynamicConflicts++
	}
	if senderOtherConflict {
		s.stats.senderOtherConflicts++
	}
	if senderCoarseConflict {
		s.stats.senderCoarseConflicts++
	}
	if senderExistenceConflict {
		s.stats.senderExistenceConflicts++
	}
	if senderTypeConflict {
		s.stats.senderTypeConflicts++
	}
	if senderBalanceConflict {
		s.stats.senderBalanceConflicts++
	}
	if senderAllowanceConflict {
		s.stats.senderAllowanceConflicts++
	}
	if senderBandwidthConflict {
		s.stats.senderBandwidthConflicts++
	}
	if senderFrozenResourceConflict {
		s.stats.senderFrozenResourceConflicts++
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
	if typedAccountConflict {
		s.stats.typedAccountConflicts++
	}
	if typedAccountCoarseConflict {
		s.stats.typedAccountCoarseConflicts++
	}
	if typedAccountExistenceConflict {
		s.stats.typedAccountExistenceConflicts++
	}
	if typedAccountTypeConflict {
		s.stats.typedAccountTypeConflicts++
	}
	if typedAccountBalanceConflict {
		s.stats.typedAccountBalanceConflicts++
	}
	if typedAccountAllowanceConflict {
		s.stats.typedAccountAllowanceConflicts++
	}
	if typedAccountBandwidthConflict {
		s.stats.typedAccountBandwidthConflicts++
	}
	if typedAccountFrozenResourceConflict {
		s.stats.typedAccountFrozenResourceConflicts++
	}

	firstPassValid := !unsupported && !conflict
	normalizedFirstPassValid := !unsupported && !normalizedConflict
	typedFirstPassValid := !unsupported && !typedConflict
	senderFirstPassValid := !unsupported && !senderConflict
	s.classify(tx, firstPassValid, normalizedFirstPassValid, typedFirstPassValid, senderFirstPassValid)
	if firstPassValid {
		s.stats.firstPassValid++
	}
	if normalizedFirstPassValid {
		s.stats.normalizedFirstPassValid++
	}
	if typedFirstPassValid {
		s.stats.typedFirstPassValid++
	}
	if senderFirstPassValid {
		s.stats.senderFirstPassValid++
	}
	if !unsupported && conflict && !normalizedConflict {
		s.stats.settlementResolvedFirstPass++
	}
	if !unsupported && normalizedConflict && !typedConflict {
		s.stats.typedResolvedFirstPass++
	}
	if !unsupported && typedConflict && !senderConflict {
		s.stats.senderDependencyResolvedFirstPass++
	}

	// Only after validation do this transaction's serially-authoritative writes
	// become the latest versions visible to later speculative attempts.
	s.recorder.Visit(func(key state.TransactionAccessKey, mode state.TransactionAccessMode) bool {
		if mode&(state.TransactionAccessWrite|state.TransactionAccessCommutativeWrite) != 0 {
			s.installRecordedWrite(key, txIndex)
		}
		return true
	})
	statedb.VisitTransactionAccessWritesSince(journalMark, func(key state.TransactionAccessKey) bool {
		s.installJournalWrite(key, txIndex)
		return true
	})
}

func (s *versionedAccessShadow) classify(tx *types.Transaction, firstPassValid, normalizedFirstPassValid, typedFirstPassValid, senderFirstPassValid bool) {
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
		if typedFirstPassValid {
			s.stats.vmTypedFirstPass++
		}
		if senderFirstPassValid {
			s.stats.vmSenderFirstPass++
		}
	case corepb.Transaction_Contract_TransferContract:
		s.stats.transferTransactions++
		if firstPassValid {
			s.stats.transferFirstPass++
		}
		if normalizedFirstPassValid {
			s.stats.transferNormalizedFirstPass++
		}
		if typedFirstPassValid {
			s.stats.transferTypedFirstPass++
		}
		if senderFirstPassValid {
			s.stats.transferSenderFirstPass++
		}
	default:
		s.stats.otherTransactions++
		if firstPassValid {
			s.stats.otherFirstPassValid++
		}
		if normalizedFirstPassValid {
			s.stats.otherNormalizedFirstPass++
		}
		if typedFirstPassValid {
			s.stats.otherTypedFirstPass++
		}
		if senderFirstPassValid {
			s.stats.otherSenderFirstPass++
		}
	}
}

type dependencyDAGTiming struct {
	serialNanos     int64
	waveNanos       int64
	fourWorkerNanos int64
}

type dependencyReadyQueueTiming struct {
	criticalPathNanos int64
	fourWorkerNanos   int64
}

// estimateDependencyDAGTiming applies a conservative wave barrier between DAG
// levels. waveNanos assumes unlimited workers inside a wave. fourWorkerNanos
// greedily assigns canonical-order transactions to four lanes inside each
// wave. A dependency-aware executor may overlap unrelated adjacent waves and
// beat these estimates, but cannot infer that opportunity from wave width
// alone.
func estimateDependencyDAGTiming(waves []int, durations []int64, waveCount int) dependencyDAGTiming {
	if waveCount <= 0 {
		return dependencyDAGTiming{}
	}
	waveMax := make([]int64, waveCount)
	waveWorkerLoads := make([][4]int64, waveCount)
	var timing dependencyDAGTiming
	for txIndex, duration := range durations {
		if duration < 0 {
			duration = 0
		}
		timing.serialNanos += duration
		if txIndex >= len(waves) || waves[txIndex] < 0 || waves[txIndex] >= waveCount {
			continue
		}
		wave := waves[txIndex]
		if duration > waveMax[wave] {
			waveMax[wave] = duration
		}
		lane := 0
		for candidate := 1; candidate < len(waveWorkerLoads[wave]); candidate++ {
			if waveWorkerLoads[wave][candidate] < waveWorkerLoads[wave][lane] {
				lane = candidate
			}
		}
		waveWorkerLoads[wave][lane] += duration
	}
	for wave := range waveMax {
		timing.waveNanos += waveMax[wave]
		var fourWorkerWaveNanos int64
		for _, load := range waveWorkerLoads[wave] {
			if load > fourWorkerWaveNanos {
				fourWorkerWaveNanos = load
			}
		}
		timing.fourWorkerNanos += fourWorkerWaveNanos
	}
	return timing
}

// estimateDependencyReadyQueueTiming removes the global barrier between DAG
// levels. It first computes the exact cost-weighted critical path, then runs a
// canonical-index ready queue on four workers. A dependent becomes runnable
// only after every directly observed predecessor has completed.
func estimateDependencyReadyQueueTiming(durations []int64, dependencyHeads []int, edges []transactionDependencyEdge) dependencyReadyQueueTiming {
	transactionCount := len(durations)
	if transactionCount == 0 {
		return dependencyReadyQueueTiming{}
	}
	criticalFinishes := make([]int64, transactionCount)
	indegrees := make([]int, transactionCount)
	successorHeads := make([]int, transactionCount)
	for txIndex := range successorHeads {
		successorHeads[txIndex] = -1
	}
	successorNext := make([]int, len(edges))
	for edgeIndex, edge := range edges {
		if edge.predecessor < 0 || edge.predecessor >= edge.dependent || edge.dependent >= transactionCount {
			continue
		}
		indegrees[edge.dependent]++
		successorNext[edgeIndex] = successorHeads[edge.predecessor]
		successorHeads[edge.predecessor] = edgeIndex
	}
	var timing dependencyReadyQueueTiming
	for txIndex, duration := range durations {
		if duration < 0 {
			duration = 0
		}
		var readyAt int64
		if txIndex < len(dependencyHeads) {
			for edgeIndex := dependencyHeads[txIndex]; edgeIndex >= 0; edgeIndex = edges[edgeIndex].next {
				predecessor := edges[edgeIndex].predecessor
				if predecessor >= 0 && predecessor < txIndex && criticalFinishes[predecessor] > readyAt {
					readyAt = criticalFinishes[predecessor]
				}
			}
		}
		criticalFinishes[txIndex] = readyAt + duration
		if criticalFinishes[txIndex] > timing.criticalPathNanos {
			timing.criticalPathNanos = criticalFinishes[txIndex]
		}
	}

	ready := make([]int, 0, transactionCount)
	pushReady := func(txIndex int) {
		ready = append(ready, txIndex)
		for child := len(ready) - 1; child > 0; {
			parent := (child - 1) / 2
			if ready[parent] <= ready[child] {
				break
			}
			ready[parent], ready[child] = ready[child], ready[parent]
			child = parent
		}
	}
	popReady := func() int {
		root := ready[0]
		last := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		if len(ready) == 0 {
			return root
		}
		ready[0] = last
		for parent := 0; ; {
			left := parent*2 + 1
			if left >= len(ready) {
				break
			}
			child := left
			right := left + 1
			if right < len(ready) && ready[right] < ready[left] {
				child = right
			}
			if ready[parent] <= ready[child] {
				break
			}
			ready[parent], ready[child] = ready[child], ready[parent]
			parent = child
		}
		return root
	}
	for txIndex, indegree := range indegrees {
		if indegree == 0 {
			pushReady(txIndex)
		}
	}
	runningTx := [4]int{-1, -1, -1, -1}
	workerFinishes := [4]int64{}
	completed := 0
	var now int64
	for completed < transactionCount {
		for worker := range runningTx {
			if runningTx[worker] >= 0 || len(ready) == 0 {
				continue
			}
			readyTx := popReady()
			duration := durations[readyTx]
			if duration < 0 {
				duration = 0
			}
			runningTx[worker] = readyTx
			workerFinishes[worker] = now + duration
		}
		nextCompletion := int64(-1)
		for worker, txIndex := range runningTx {
			if txIndex >= 0 && (nextCompletion < 0 || workerFinishes[worker] < nextCompletion) {
				nextCompletion = workerFinishes[worker]
			}
		}
		if nextCompletion < 0 {
			// A cycle is impossible because every captured edge points backward.
			// Keep the observer fail-safe if malformed test data violates that.
			return dependencyReadyQueueTiming{criticalPathNanos: timing.criticalPathNanos}
		}
		now = nextCompletion
		for worker, txIndex := range runningTx {
			if txIndex < 0 || workerFinishes[worker] != now {
				continue
			}
			runningTx[worker] = -1
			completed++
			for edgeIndex := successorHeads[txIndex]; edgeIndex >= 0; edgeIndex = successorNext[edgeIndex] {
				dependent := edges[edgeIndex].dependent
				indegrees[dependent]--
				if indegrees[dependent] == 0 {
					pushReady(dependent)
				}
			}
		}
	}
	timing.fourWorkerNanos = now
	return timing
}

func (s *versionedAccessShadow) Finish(statedb *state.StateDB, dynProps *state.DynamicProperties) versionedAccessShadowStats {
	s.detach(statedb, dynProps)
	stats := s.stats
	for _, width := range s.dependencyWaveWidths {
		if width == 0 {
			continue
		}
		stats.dependencyDAGWaves++
		if int64(width) > stats.dependencyDAGMaxWidth {
			stats.dependencyDAGMaxWidth = int64(width)
		}
		if width > 1 {
			stats.dependencyDAGParallelTransactions += int64(width)
		}
	}
	timing := estimateDependencyDAGTiming(s.dependencyWaves, s.transactionDurations, len(s.dependencyWaveWidths))
	stats.dependencyDAGSerialNanos = timing.serialNanos
	stats.dependencyDAGWaveNanos = timing.waveNanos
	stats.dependencyDAGFourWorkerNanos = timing.fourWorkerNanos
	readyTiming := estimateDependencyReadyQueueTiming(s.transactionDurations, s.dependencyHeads, s.dependencyEdges)
	stats.dependencyDAGReadyCriticalNanos = readyTiming.criticalPathNanos
	stats.dependencyDAGReadyFourWorkerNanos = readyTiming.fourWorkerNanos
	return stats
}

func (s *versionedAccessShadow) Publish(statedb *state.StateDB, dynProps *state.DynamicProperties) {
	stats := s.Finish(statedb, dynProps)
	versionedShadowBlocksCounter.Inc(1)
	versionedShadowTransactionsCounter.Inc(stats.transactions)
	versionedShadowFirstPassValidCounter.Inc(stats.firstPassValid)
	versionedShadowNormalizedFirstPassValidCounter.Inc(stats.normalizedFirstPassValid)
	versionedShadowTypedFirstPassValidCounter.Inc(stats.typedFirstPassValid)
	versionedShadowSenderFirstPassValidCounter.Inc(stats.senderFirstPassValid)
	versionedShadowConflictsCounter.Inc(stats.conflicts)
	versionedShadowNormalizedConflictsCounter.Inc(stats.normalizedConflicts)
	versionedShadowTypedConflictsCounter.Inc(stats.typedConflicts)
	versionedShadowSenderConflictsCounter.Inc(stats.senderConflicts)
	versionedShadowUnsupportedCounter.Inc(stats.unsupported)
	versionedShadowReadCellsCounter.Inc(stats.readCells)
	versionedShadowWriteCellsCounter.Inc(stats.writeCells)
	versionedShadowReadConflictsCounter.Inc(stats.readConflicts)
	versionedShadowWriteConflictsCounter.Inc(stats.writeConflicts)
	versionedShadowAccountConflictsCounter.Inc(stats.accountConflicts)
	versionedShadowStorageConflictsCounter.Inc(stats.storageConflicts)
	versionedShadowAccountKVConflictsCounter.Inc(stats.accountKVConflicts)
	versionedShadowDynamicConflictsCounter.Inc(stats.dynamicConflicts)
	versionedShadowRawKVConflictsCounter.Inc(stats.rawKVConflicts)
	versionedShadowOtherConflictsCounter.Inc(stats.otherConflicts)
	versionedShadowVMTransactionsCounter.Inc(stats.vmTransactions)
	versionedShadowVMFirstPassValidCounter.Inc(stats.vmFirstPassValid)
	versionedShadowVMNormalizedFirstPassCounter.Inc(stats.vmNormalizedFirstPass)
	versionedShadowVMTypedFirstPassCounter.Inc(stats.vmTypedFirstPass)
	versionedShadowVMSenderFirstPassCounter.Inc(stats.vmSenderFirstPass)
	versionedShadowTransferTransactionsCounter.Inc(stats.transferTransactions)
	versionedShadowTransferFirstPassValidCounter.Inc(stats.transferFirstPass)
	versionedShadowTransferNormalizedFirstPassCounter.Inc(stats.transferNormalizedFirstPass)
	versionedShadowTransferTypedFirstPassCounter.Inc(stats.transferTypedFirstPass)
	versionedShadowTransferSenderFirstPassCounter.Inc(stats.transferSenderFirstPass)
	versionedShadowOtherTransactionsCounter.Inc(stats.otherTransactions)
	versionedShadowOtherFirstPassValidCounter.Inc(stats.otherFirstPassValid)
	versionedShadowOtherNormalizedFirstPassCounter.Inc(stats.otherNormalizedFirstPass)
	versionedShadowOtherTypedFirstPassCounter.Inc(stats.otherTypedFirstPass)
	versionedShadowOtherSenderFirstPassCounter.Inc(stats.otherSenderFirstPass)
	versionedShadowSenderDependencyTaggedCounter.Inc(stats.senderDependencyTaggedTransactions)
	versionedShadowSenderDependencyResolvedCounter.Inc(stats.senderDependencyResolvedFirstPass)
	versionedShadowSenderAccountConflictCounter.Inc(stats.senderAccountConflicts)
	versionedShadowSenderStorageConflictCounter.Inc(stats.senderStorageConflicts)
	versionedShadowSenderAccountKVConflictCounter.Inc(stats.senderAccountKVConflicts)
	versionedShadowSenderDynamicConflictCounter.Inc(stats.senderDynamicConflicts)
	versionedShadowSenderOtherConflictCounter.Inc(stats.senderOtherConflicts)
	versionedShadowSenderCoarseConflictCounter.Inc(stats.senderCoarseConflicts)
	versionedShadowSenderExistenceConflictCounter.Inc(stats.senderExistenceConflicts)
	versionedShadowSenderTypeConflictCounter.Inc(stats.senderTypeConflicts)
	versionedShadowSenderBalanceConflictCounter.Inc(stats.senderBalanceConflicts)
	versionedShadowSenderAllowanceConflictCounter.Inc(stats.senderAllowanceConflicts)
	versionedShadowSenderBandwidthConflictCounter.Inc(stats.senderBandwidthConflicts)
	versionedShadowSenderFrozenResourceConflictCounter.Inc(stats.senderFrozenResourceConflicts)
	versionedShadowDependencyDAGWavesCounter.Inc(stats.dependencyDAGWaves)
	versionedShadowDependencyDAGParallelTransactionsCounter.Inc(stats.dependencyDAGParallelTransactions)
	versionedShadowDAGSerialNanosCounter.Inc(stats.dependencyDAGSerialNanos)
	versionedShadowDAGWaveNanosCounter.Inc(stats.dependencyDAGWaveNanos)
	versionedShadowDAGFourWorkerNanosCounter.Inc(stats.dependencyDAGFourWorkerNanos)
	versionedShadowReadyCriticalNanosCounter.Inc(stats.dependencyDAGReadyCriticalNanos)
	versionedShadowReadyFourWorkerNanosCounter.Inc(stats.dependencyDAGReadyFourWorkerNanos)
	versionedShadowTypedResolvedCounter.Inc(stats.typedResolvedFirstPass)
	versionedShadowTypedAccountConflictCounter.Inc(stats.typedAccountConflicts)
	versionedShadowTypedAccountCoarseConflictCounter.Inc(stats.typedAccountCoarseConflicts)
	versionedShadowTypedAccountExistenceConflictCounter.Inc(stats.typedAccountExistenceConflicts)
	versionedShadowTypedAccountTypeConflictCounter.Inc(stats.typedAccountTypeConflicts)
	versionedShadowTypedAccountBalanceConflictCounter.Inc(stats.typedAccountBalanceConflicts)
	versionedShadowTypedAccountAllowanceConflictCounter.Inc(stats.typedAccountAllowanceConflicts)
	versionedShadowTypedAccountBandwidthConflictCounter.Inc(stats.typedAccountBandwidthConflicts)
	versionedShadowTypedAccountFrozenResourceConflictCounter.Inc(stats.typedAccountFrozenResourceConflicts)
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
	versionedShadowLastTypedFirstPassValidGauge.Update(stats.typedFirstPassValid)
	versionedShadowLastSenderFirstPassValidGauge.Update(stats.senderFirstPassValid)
	versionedShadowLastMaxSenderChainDepthGauge.Update(stats.maxSenderChainDepth)
	versionedShadowLastDependencyDAGWavesGauge.Update(stats.dependencyDAGWaves)
	versionedShadowLastDependencyDAGMaxWidthGauge.Update(stats.dependencyDAGMaxWidth)
	versionedShadowLastDependencyDAGParallelTransactionsGauge.Update(stats.dependencyDAGParallelTransactions)
	versionedShadowLastDAGSerialNanosGauge.Update(stats.dependencyDAGSerialNanos)
	versionedShadowLastDAGWaveNanosGauge.Update(stats.dependencyDAGWaveNanos)
	versionedShadowLastDAGFourWorkerNanosGauge.Update(stats.dependencyDAGFourWorkerNanos)
	versionedShadowLastReadyCriticalNanosGauge.Update(stats.dependencyDAGReadyCriticalNanos)
	versionedShadowLastReadyFourWorkerNanosGauge.Update(stats.dependencyDAGReadyFourWorkerNanos)
	versionedShadowLastConflictsGauge.Update(stats.conflicts)
	versionedShadowLastUnsupportedGauge.Update(stats.unsupported)
	versionedShadowLastMaxDependencyDistanceGauge.Update(stats.maxDependencyDistance)
}
