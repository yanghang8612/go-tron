package net

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
)

// syncImportWindowObservation separates workload shape, downloader supply,
// canonical apply time, and sync-framework overhead for one progress window.
// Duration fields are normalized to a block/transaction so windows at
// different historical heights remain diagnosable without assuming that one
// transaction or one block represents a constant amount of work.
type syncImportWindowObservation struct {
	BlocksPerSec                        float64
	TxsPerSec                           float64
	TxsPerBlock                         float64
	EnergyPerSec                        float64
	EnergyPerBlock                      float64
	EnergyPerTx                         float64
	VMTransactionsPerSec                float64
	NativeTransactionsPerSec            float64
	VMTransactionsPerBlock              float64
	NativeTransactionsPerBlock          float64
	VMTransactionShare                  float64
	RawEnergyPerSec                     float64
	RawEnergyPerVMTransaction           float64
	BilledToRawEnergyRatio              float64
	VMExecutionMillisPerVMTransaction   float64
	VMExecutionNanosPerRawEnergy        float64
	ExecBusyRatio                       float64
	BufferWaitRatio                     float64
	ApplyCoverageRatio                  float64
	ImportMillisPerBlock                float64
	ApplyMillisPerBlock                 float64
	ImportOverheadMillisPerBlock        float64
	OutsideTxMillisPerBlock             float64
	ExecuteFixedMillisPerBlock          float64
	ValidateMillisPerBlock              float64
	ExecuteMillisPerBlock               float64
	TransactionMillisPerBlock           float64
	TransactionMillisPerTx              float64
	AccountRootMillisPerBlock           float64
	AdaptiveEnergyMillisPerBlock        float64
	RewardsMillisPerBlock               float64
	ShieldedMillisPerBlock              float64
	WitnessFlushMillisPerBlock          float64
	BlockStatsMillisPerBlock            float64
	MaintenanceMillisPerBlock           float64
	StateCommitMillisPerBlock           float64
	StateCommitAccountsPerBlock         float64
	StateCommitKVAccountsPerBlock       float64
	StateCommitKVItemsPerBlock          float64
	StateStorageWritesPerBlock          float64
	StateKVWritesPerBlock               float64
	CommitmentUpdatesPerBlock           float64
	StateCommitNanosPerCommitmentUpdate float64
	DPUpdateMillisPerBlock              float64
	PersistMillisPerBlock               float64
	PersistMetadataBytesPerBlock        float64
	PersistMetadataBytesPerTx           float64
	PersistMetadataRecordsPerBlock      float64
	PersistLookupRowsPerBlock           float64
	PersistTraceAccountsPerBlock        float64
	HooksMillisPerBlock                 float64
}

var syncImportWindowMetrics = struct {
	updatedUnix                         *metrics.Gauge
	elapsedSeconds                      *metrics.GaugeFloat64
	blocksPerSec                        *metrics.GaugeFloat64
	txsPerSec                           *metrics.GaugeFloat64
	txsPerBlock                         *metrics.GaugeFloat64
	energyPerSec                        *metrics.GaugeFloat64
	energyPerBlock                      *metrics.GaugeFloat64
	energyPerTx                         *metrics.GaugeFloat64
	vmTransactionsPerSec                *metrics.GaugeFloat64
	nativeTransactionsPerSec            *metrics.GaugeFloat64
	vmTransactionsPerBlock              *metrics.GaugeFloat64
	nativeTransactionsPerBlock          *metrics.GaugeFloat64
	vmTransactionShare                  *metrics.GaugeFloat64
	rawEnergyPerSec                     *metrics.GaugeFloat64
	rawEnergyPerVMTransaction           *metrics.GaugeFloat64
	billedToRawEnergyRatio              *metrics.GaugeFloat64
	vmExecutionMillisPerVMTransaction   *metrics.GaugeFloat64
	vmExecutionNanosPerRawEnergy        *metrics.GaugeFloat64
	execBusyRatio                       *metrics.GaugeFloat64
	bufferWaitRatio                     *metrics.GaugeFloat64
	bufferWaitSeconds                   *metrics.GaugeFloat64
	applyBlocks                         *metrics.Gauge
	applyTransactions                   *metrics.Gauge
	applyCoverageRatio                  *metrics.GaugeFloat64
	importMillisPerBlock                *metrics.GaugeFloat64
	applyMillisPerBlock                 *metrics.GaugeFloat64
	importOverheadMillisPerBlock        *metrics.GaugeFloat64
	outsideTxMillisPerBlock             *metrics.GaugeFloat64
	executeFixedMillisPerBlock          *metrics.GaugeFloat64
	validateMillisPerBlock              *metrics.GaugeFloat64
	executeMillisPerBlock               *metrics.GaugeFloat64
	transactionMillisPerBlock           *metrics.GaugeFloat64
	transactionMillisPerTx              *metrics.GaugeFloat64
	accountRootMillisPerBlock           *metrics.GaugeFloat64
	adaptiveEnergyMillisPerBlock        *metrics.GaugeFloat64
	rewardsMillisPerBlock               *metrics.GaugeFloat64
	shieldedMillisPerBlock              *metrics.GaugeFloat64
	witnessFlushMillisPerBlock          *metrics.GaugeFloat64
	blockStatsMillisPerBlock            *metrics.GaugeFloat64
	maintenanceMillisPerBlock           *metrics.GaugeFloat64
	stateCommitMillisPerBlock           *metrics.GaugeFloat64
	stateCommitAccountsPerBlock         *metrics.GaugeFloat64
	stateCommitKVAccountsPerBlock       *metrics.GaugeFloat64
	stateCommitKVItemsPerBlock          *metrics.GaugeFloat64
	stateStorageWritesPerBlock          *metrics.GaugeFloat64
	stateKVWritesPerBlock               *metrics.GaugeFloat64
	commitmentUpdatesPerBlock           *metrics.GaugeFloat64
	stateCommitNanosPerCommitmentUpdate *metrics.GaugeFloat64
	dpUpdateMillisPerBlock              *metrics.GaugeFloat64
	persistMillisPerBlock               *metrics.GaugeFloat64
	persistMetadataBytesPerBlock        *metrics.GaugeFloat64
	persistMetadataBytesPerTx           *metrics.GaugeFloat64
	persistMetadataRecordsPerBlock      *metrics.GaugeFloat64
	persistLookupRowsPerBlock           *metrics.GaugeFloat64
	persistTraceAccountsPerBlock        *metrics.GaugeFloat64
	hooksMillisPerBlock                 *metrics.GaugeFloat64
	activePeers                         *metrics.Gauge
	inflightBlocks                      *metrics.Gauge
	bufferedBlocks                      *metrics.Gauge
	requestedBlocks                     *metrics.Gauge
	retryBlocks                         *metrics.Gauge
}{
	updatedUnix:                         metrics.NewRegisteredGauge("sync/import/window/updated_unix", nil),
	elapsedSeconds:                      metrics.NewRegisteredGaugeFloat64("sync/import/window/elapsed_seconds", nil),
	blocksPerSec:                        metrics.NewRegisteredGaugeFloat64("sync/import/window/blocks_per_second", nil),
	txsPerSec:                           metrics.NewRegisteredGaugeFloat64("sync/import/window/transactions_per_second", nil),
	txsPerBlock:                         metrics.NewRegisteredGaugeFloat64("sync/import/window/transactions_per_block", nil),
	energyPerSec:                        metrics.NewRegisteredGaugeFloat64("sync/import/window/energy_per_second", nil),
	energyPerBlock:                      metrics.NewRegisteredGaugeFloat64("sync/import/window/energy_per_block", nil),
	energyPerTx:                         metrics.NewRegisteredGaugeFloat64("sync/import/window/energy_per_transaction", nil),
	vmTransactionsPerSec:                metrics.NewRegisteredGaugeFloat64("sync/import/window/vm_transactions_per_second", nil),
	nativeTransactionsPerSec:            metrics.NewRegisteredGaugeFloat64("sync/import/window/native_transactions_per_second", nil),
	vmTransactionsPerBlock:              metrics.NewRegisteredGaugeFloat64("sync/import/window/vm_transactions_per_block", nil),
	nativeTransactionsPerBlock:          metrics.NewRegisteredGaugeFloat64("sync/import/window/native_transactions_per_block", nil),
	vmTransactionShare:                  metrics.NewRegisteredGaugeFloat64("sync/import/window/vm_transaction_share", nil),
	rawEnergyPerSec:                     metrics.NewRegisteredGaugeFloat64("sync/import/window/raw_energy_per_second", nil),
	rawEnergyPerVMTransaction:           metrics.NewRegisteredGaugeFloat64("sync/import/window/raw_energy_per_vm_transaction", nil),
	billedToRawEnergyRatio:              metrics.NewRegisteredGaugeFloat64("sync/import/window/billed_to_raw_energy_ratio", nil),
	vmExecutionMillisPerVMTransaction:   metrics.NewRegisteredGaugeFloat64("sync/import/window/vm_execution_milliseconds_per_vm_transaction", nil),
	vmExecutionNanosPerRawEnergy:        metrics.NewRegisteredGaugeFloat64("sync/import/window/vm_execution_nanoseconds_per_raw_energy", nil),
	execBusyRatio:                       metrics.NewRegisteredGaugeFloat64("sync/import/window/exec_busy_ratio", nil),
	bufferWaitRatio:                     metrics.NewRegisteredGaugeFloat64("sync/import/window/buffer_wait_ratio", nil),
	bufferWaitSeconds:                   metrics.NewRegisteredGaugeFloat64("sync/import/window/buffer_wait_seconds", nil),
	applyBlocks:                         metrics.NewRegisteredGauge("sync/import/window/apply_sample_blocks", nil),
	applyTransactions:                   metrics.NewRegisteredGauge("sync/import/window/apply_sample_transactions", nil),
	applyCoverageRatio:                  metrics.NewRegisteredGaugeFloat64("sync/import/window/apply_sample_coverage_ratio", nil),
	importMillisPerBlock:                metrics.NewRegisteredGaugeFloat64("sync/import/window/import_milliseconds_per_block", nil),
	applyMillisPerBlock:                 metrics.NewRegisteredGaugeFloat64("sync/import/window/apply_milliseconds_per_block", nil),
	importOverheadMillisPerBlock:        metrics.NewRegisteredGaugeFloat64("sync/import/window/import_overhead_milliseconds_per_block", nil),
	outsideTxMillisPerBlock:             metrics.NewRegisteredGaugeFloat64("sync/import/window/outside_transaction_milliseconds_per_block", nil),
	executeFixedMillisPerBlock:          metrics.NewRegisteredGaugeFloat64("sync/import/window/execute_fixed_milliseconds_per_block", nil),
	validateMillisPerBlock:              metrics.NewRegisteredGaugeFloat64("sync/import/window/validate_milliseconds_per_block", nil),
	executeMillisPerBlock:               metrics.NewRegisteredGaugeFloat64("sync/import/window/execute_milliseconds_per_block", nil),
	transactionMillisPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/transaction_milliseconds_per_block", nil),
	transactionMillisPerTx:              metrics.NewRegisteredGaugeFloat64("sync/import/window/transaction_milliseconds_per_transaction", nil),
	accountRootMillisPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/account_state_root_milliseconds_per_block", nil),
	adaptiveEnergyMillisPerBlock:        metrics.NewRegisteredGaugeFloat64("sync/import/window/adaptive_energy_milliseconds_per_block", nil),
	rewardsMillisPerBlock:               metrics.NewRegisteredGaugeFloat64("sync/import/window/rewards_milliseconds_per_block", nil),
	shieldedMillisPerBlock:              metrics.NewRegisteredGaugeFloat64("sync/import/window/shielded_finalize_milliseconds_per_block", nil),
	witnessFlushMillisPerBlock:          metrics.NewRegisteredGaugeFloat64("sync/import/window/witness_flush_milliseconds_per_block", nil),
	blockStatsMillisPerBlock:            metrics.NewRegisteredGaugeFloat64("sync/import/window/block_statistics_milliseconds_per_block", nil),
	maintenanceMillisPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/maintenance_milliseconds_per_block", nil),
	stateCommitMillisPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_milliseconds_per_block", nil),
	stateCommitAccountsPerBlock:         metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_accounts_per_block", nil),
	stateCommitKVAccountsPerBlock:       metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_kv_accounts_per_block", nil),
	stateCommitKVItemsPerBlock:          metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_kv_items_per_block", nil),
	stateStorageWritesPerBlock:          metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_storage_writes_per_block", nil),
	stateKVWritesPerBlock:               metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_kv_writes_per_block", nil),
	commitmentUpdatesPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_commitment_updates_per_block", nil),
	stateCommitNanosPerCommitmentUpdate: metrics.NewRegisteredGaugeFloat64("sync/import/window/state_commit_nanoseconds_per_commitment_update", nil),
	dpUpdateMillisPerBlock:              metrics.NewRegisteredGaugeFloat64("sync/import/window/dp_update_milliseconds_per_block", nil),
	persistMillisPerBlock:               metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_milliseconds_per_block", nil),
	persistMetadataBytesPerBlock:        metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_metadata_bytes_per_block", nil),
	persistMetadataBytesPerTx:           metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_metadata_bytes_per_transaction", nil),
	persistMetadataRecordsPerBlock:      metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_metadata_records_per_block", nil),
	persistLookupRowsPerBlock:           metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_transaction_lookup_rows_per_block", nil),
	persistTraceAccountsPerBlock:        metrics.NewRegisteredGaugeFloat64("sync/import/window/persist_trace_accounts_per_block", nil),
	hooksMillisPerBlock:                 metrics.NewRegisteredGaugeFloat64("sync/import/window/hooks_milliseconds_per_block", nil),
	activePeers:                         metrics.NewRegisteredGauge("sync/import/window/active_peers", nil),
	inflightBlocks:                      metrics.NewRegisteredGauge("sync/import/window/inflight_blocks", nil),
	bufferedBlocks:                      metrics.NewRegisteredGauge("sync/import/window/buffered_blocks", nil),
	requestedBlocks:                     metrics.NewRegisteredGauge("sync/import/window/requested_blocks", nil),
	retryBlocks:                         metrics.NewRegisteredGauge("sync/import/window/retry_blocks", nil),
}

func newSyncImportWindowObservation(s tsync.Snapshot, elapsed time.Duration) syncImportWindowObservation {
	var out syncImportWindowObservation
	applyBlocks := s.ApplyBlocks
	applyTxs := s.ApplyTxs
	// Synthetic and legacy callers may populate ApplyStats directly. Production
	// always records explicit completion coverage through AddApplyBlockWithTxs.
	if applyBlocks == 0 && s.ApplyStats.Total() > 0 {
		applyBlocks = s.Blocks
	}
	if applyTxs == 0 && s.ApplyStats.TransactionExecute > 0 {
		applyTxs = s.Txs
	}
	if elapsed > 0 {
		seconds := elapsed.Seconds()
		out.BlocksPerSec = float64(s.Blocks) / seconds
		out.TxsPerSec = float64(s.Txs) / seconds
		out.EnergyPerSec = float64(s.ApplyStats.EnergyUsageTotal) / seconds
		out.VMTransactionsPerSec = float64(s.ApplyStats.VMTransactions) / seconds
		out.NativeTransactionsPerSec = float64(s.ApplyStats.NativeTransactions) / seconds
		out.RawEnergyPerSec = float64(s.ApplyStats.VMRawEnergyUsage) / seconds
		out.ExecBusyRatio = durationRatio(s.ExecElapsed, elapsed)
		out.BufferWaitRatio = durationRatio(s.BufferWaitElapsed, elapsed)
	}
	if s.Blocks > 0 {
		blocks := float64(s.Blocks)
		out.TxsPerBlock = float64(s.Txs) / blocks
		out.ApplyCoverageRatio = float64(applyBlocks) / blocks
		out.ImportMillisPerBlock = durationMillisPer(s.ExecElapsed, s.Blocks)
	}
	if applyBlocks > 0 {
		out.EnergyPerBlock = float64(s.ApplyStats.EnergyUsageTotal) / float64(applyBlocks)
		out.ApplyMillisPerBlock = durationMillisPer(s.ApplyStats.Total(), applyBlocks)
		if applyBlocks == s.Blocks {
			applyOverhead := s.ExecElapsed - s.ApplyStats.Total()
			if applyOverhead > 0 {
				out.ImportOverheadMillisPerBlock = durationMillisPer(applyOverhead, applyBlocks)
			}
		}
		outsideTx := s.ApplyStats.Total() - s.ApplyStats.TransactionExecute
		if outsideTx > 0 {
			out.OutsideTxMillisPerBlock = durationMillisPer(outsideTx, applyBlocks)
		}
		executeFixed := s.ApplyStats.Execute - s.ApplyStats.TransactionExecute
		if executeFixed > 0 {
			out.ExecuteFixedMillisPerBlock = durationMillisPer(executeFixed, applyBlocks)
		}
		out.ValidateMillisPerBlock = durationMillisPer(s.ApplyStats.Validate, applyBlocks)
		out.ExecuteMillisPerBlock = durationMillisPer(s.ApplyStats.Execute, applyBlocks)
		out.TransactionMillisPerBlock = durationMillisPer(s.ApplyStats.TransactionExecute, applyBlocks)
		out.AccountRootMillisPerBlock = durationMillisPer(s.ApplyStats.AccountStateRoot, applyBlocks)
		out.AdaptiveEnergyMillisPerBlock = durationMillisPer(s.ApplyStats.AdaptiveEnergy, applyBlocks)
		out.RewardsMillisPerBlock = durationMillisPer(s.ApplyStats.Rewards, applyBlocks)
		out.ShieldedMillisPerBlock = durationMillisPer(s.ApplyStats.ShieldedFinalize, applyBlocks)
		out.WitnessFlushMillisPerBlock = durationMillisPer(s.ApplyStats.WitnessFlush, applyBlocks)
		out.BlockStatsMillisPerBlock = durationMillisPer(s.ApplyStats.BlockStatistics, applyBlocks)
		out.MaintenanceMillisPerBlock = durationMillisPer(s.ApplyStats.Maintenance, applyBlocks)
		out.StateCommitMillisPerBlock = durationMillisPer(s.ApplyStats.StateCommit, applyBlocks)
		commitBlocks := float64(applyBlocks)
		commit := s.ApplyStats.StateCommitDetail
		out.StateCommitAccountsPerBlock = float64(commit.Accounts) / commitBlocks
		out.StateCommitKVAccountsPerBlock = float64(commit.KVAccounts) / commitBlocks
		out.StateCommitKVItemsPerBlock = float64(commit.KVItems) / commitBlocks
		out.StateStorageWritesPerBlock = float64(commit.Mutations.StoragePuts+commit.Mutations.StorageDeletes) / commitBlocks
		out.StateKVWritesPerBlock = float64(commit.Mutations.KVPutItems+commit.Mutations.KVDeleteItems) / commitBlocks
		out.CommitmentUpdatesPerBlock = float64(commit.CommitmentUpdates) / commitBlocks
		out.PersistMetadataBytesPerBlock = float64(s.ApplyStats.PersistDetail.MetadataBytes) / commitBlocks
		out.PersistMetadataRecordsPerBlock = float64(s.ApplyStats.PersistDetail.MetadataRecords) / commitBlocks
		out.PersistLookupRowsPerBlock = float64(s.ApplyStats.PersistDetail.TransactionLookupRows) / commitBlocks
		out.PersistTraceAccountsPerBlock = float64(s.ApplyStats.PersistDetail.TraceAccounts) / commitBlocks
		out.DPUpdateMillisPerBlock = durationMillisPer(s.ApplyStats.DPUpdate, applyBlocks)
		out.PersistMillisPerBlock = durationMillisPer(s.ApplyStats.Persist, applyBlocks)
		out.HooksMillisPerBlock = durationMillisPer(s.ApplyStats.Hooks, applyBlocks)
	}
	if applyTxs > 0 {
		out.EnergyPerTx = float64(s.ApplyStats.EnergyUsageTotal) / float64(applyTxs)
		out.TransactionMillisPerTx = durationMillisPer(s.ApplyStats.TransactionExecute, applyTxs)
		out.PersistMetadataBytesPerTx = float64(s.ApplyStats.PersistDetail.MetadataBytes) / float64(applyTxs)
	}
	vmTxs := s.ApplyStats.VMTransactions
	if vmTxs > 0 {
		if applyBlocks > 0 {
			out.VMTransactionsPerBlock = float64(vmTxs) / float64(applyBlocks)
		}
		out.RawEnergyPerVMTransaction = float64(s.ApplyStats.VMRawEnergyUsage) / float64(vmTxs)
		out.VMExecutionMillisPerVMTransaction = durationMillisPer(s.ApplyStats.VMExecution, vmTxs)
	}
	if s.ApplyStats.NativeTransactions > 0 && applyBlocks > 0 {
		out.NativeTransactionsPerBlock = float64(s.ApplyStats.NativeTransactions) / float64(applyBlocks)
	}
	classifiedTxs := vmTxs + s.ApplyStats.NativeTransactions
	if classifiedTxs > 0 {
		out.VMTransactionShare = float64(vmTxs) / float64(classifiedTxs)
	}
	if rawEnergy := s.ApplyStats.VMRawEnergyUsage; rawEnergy > 0 {
		out.BilledToRawEnergyRatio = float64(s.ApplyStats.EnergyUsageTotal) / float64(rawEnergy)
		out.VMExecutionNanosPerRawEnergy = float64(s.ApplyStats.VMExecution) / float64(rawEnergy)
	}
	if updates := s.ApplyStats.StateCommitDetail.CommitmentUpdates; updates > 0 {
		out.StateCommitNanosPerCommitmentUpdate = float64(s.ApplyStats.StateCommit) / float64(updates)
	}
	return out
}

func durationRatio(part, whole time.Duration) float64 {
	if part <= 0 || whole <= 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

func durationMillisPer(d time.Duration, count int) float64 {
	if d <= 0 || count <= 0 {
		return 0
	}
	return float64(d) / float64(time.Millisecond) / float64(count)
}

func updateSyncImportWindowMetrics(now time.Time, elapsed time.Duration, sample tsync.Snapshot, obs syncImportWindowObservation, diag syncdl.Diagnostics) {
	m := syncImportWindowMetrics
	m.updatedUnix.Update(now.Unix())
	m.elapsedSeconds.Update(elapsed.Seconds())
	m.blocksPerSec.Update(obs.BlocksPerSec)
	m.txsPerSec.Update(obs.TxsPerSec)
	m.txsPerBlock.Update(obs.TxsPerBlock)
	m.energyPerSec.Update(obs.EnergyPerSec)
	m.energyPerBlock.Update(obs.EnergyPerBlock)
	m.energyPerTx.Update(obs.EnergyPerTx)
	m.vmTransactionsPerSec.Update(obs.VMTransactionsPerSec)
	m.nativeTransactionsPerSec.Update(obs.NativeTransactionsPerSec)
	m.vmTransactionsPerBlock.Update(obs.VMTransactionsPerBlock)
	m.nativeTransactionsPerBlock.Update(obs.NativeTransactionsPerBlock)
	m.vmTransactionShare.Update(obs.VMTransactionShare)
	m.rawEnergyPerSec.Update(obs.RawEnergyPerSec)
	m.rawEnergyPerVMTransaction.Update(obs.RawEnergyPerVMTransaction)
	m.billedToRawEnergyRatio.Update(obs.BilledToRawEnergyRatio)
	m.vmExecutionMillisPerVMTransaction.Update(obs.VMExecutionMillisPerVMTransaction)
	m.vmExecutionNanosPerRawEnergy.Update(obs.VMExecutionNanosPerRawEnergy)
	m.execBusyRatio.Update(obs.ExecBusyRatio)
	m.bufferWaitRatio.Update(obs.BufferWaitRatio)
	m.bufferWaitSeconds.Update(elapsed.Seconds() * obs.BufferWaitRatio)
	m.applyBlocks.Update(int64(sample.ApplyBlocks))
	m.applyTransactions.Update(int64(sample.ApplyTxs))
	m.applyCoverageRatio.Update(obs.ApplyCoverageRatio)
	m.importMillisPerBlock.Update(obs.ImportMillisPerBlock)
	m.applyMillisPerBlock.Update(obs.ApplyMillisPerBlock)
	m.importOverheadMillisPerBlock.Update(obs.ImportOverheadMillisPerBlock)
	m.outsideTxMillisPerBlock.Update(obs.OutsideTxMillisPerBlock)
	m.executeFixedMillisPerBlock.Update(obs.ExecuteFixedMillisPerBlock)
	m.validateMillisPerBlock.Update(obs.ValidateMillisPerBlock)
	m.executeMillisPerBlock.Update(obs.ExecuteMillisPerBlock)
	m.transactionMillisPerBlock.Update(obs.TransactionMillisPerBlock)
	m.transactionMillisPerTx.Update(obs.TransactionMillisPerTx)
	m.accountRootMillisPerBlock.Update(obs.AccountRootMillisPerBlock)
	m.adaptiveEnergyMillisPerBlock.Update(obs.AdaptiveEnergyMillisPerBlock)
	m.rewardsMillisPerBlock.Update(obs.RewardsMillisPerBlock)
	m.shieldedMillisPerBlock.Update(obs.ShieldedMillisPerBlock)
	m.witnessFlushMillisPerBlock.Update(obs.WitnessFlushMillisPerBlock)
	m.blockStatsMillisPerBlock.Update(obs.BlockStatsMillisPerBlock)
	m.maintenanceMillisPerBlock.Update(obs.MaintenanceMillisPerBlock)
	m.stateCommitMillisPerBlock.Update(obs.StateCommitMillisPerBlock)
	m.stateCommitAccountsPerBlock.Update(obs.StateCommitAccountsPerBlock)
	m.stateCommitKVAccountsPerBlock.Update(obs.StateCommitKVAccountsPerBlock)
	m.stateCommitKVItemsPerBlock.Update(obs.StateCommitKVItemsPerBlock)
	m.stateStorageWritesPerBlock.Update(obs.StateStorageWritesPerBlock)
	m.stateKVWritesPerBlock.Update(obs.StateKVWritesPerBlock)
	m.commitmentUpdatesPerBlock.Update(obs.CommitmentUpdatesPerBlock)
	m.stateCommitNanosPerCommitmentUpdate.Update(obs.StateCommitNanosPerCommitmentUpdate)
	m.dpUpdateMillisPerBlock.Update(obs.DPUpdateMillisPerBlock)
	m.persistMillisPerBlock.Update(obs.PersistMillisPerBlock)
	m.persistMetadataBytesPerBlock.Update(obs.PersistMetadataBytesPerBlock)
	m.persistMetadataBytesPerTx.Update(obs.PersistMetadataBytesPerTx)
	m.persistMetadataRecordsPerBlock.Update(obs.PersistMetadataRecordsPerBlock)
	m.persistLookupRowsPerBlock.Update(obs.PersistLookupRowsPerBlock)
	m.persistTraceAccountsPerBlock.Update(obs.PersistTraceAccountsPerBlock)
	m.hooksMillisPerBlock.Update(obs.HooksMillisPerBlock)
	m.activePeers.Update(int64(diag.ActivePeerCount))
	m.inflightBlocks.Update(int64(diag.Inflight))
	m.bufferedBlocks.Update(int64(diag.BlockBufferLen))
	m.requestedBlocks.Update(int64(diag.RequestedLen))
	m.retryBlocks.Update(int64(diag.RetryListLen))
}
