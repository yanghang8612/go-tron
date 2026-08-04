package core

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

const (
	discardShadowSampleInterval     = uint64(64)
	discardShadowAsyncRetryInterval = uint64(256)
	// Keep one sampled cohort on the synchronous retry observer as a stable
	// reference. The other three sampled cohorts exercise the real async
	// scheduler, increasing long-chain coverage without changing canonical
	// publication.
	discardShadowAsyncRetryReferenceOffset = uint64(0)
	discardShadowAsyncRetryFirstOffset     = discardShadowSampleInterval
	discardShadowAsyncPublishOffset        = 3 * discardShadowSampleInterval
	discardShadowWorkerCount               = 4
	discardShadowRetryMaxAttempts          = int64(8)
	discardShadowRetryMaxExecutions        = int64(64)
	discardShadowRetryLookahead            = int64(4)
)

func useDiscardShadowAsyncRetry(blockNum uint64) bool {
	return blockNum%discardShadowSampleInterval == 0 &&
		blockNum%discardShadowAsyncRetryInterval != discardShadowAsyncRetryReferenceOffset
}

func useDiscardShadowAsyncRetryPublication(blockNum uint64) bool {
	return useDiscardShadowAsyncRetry(blockNum) && blockNum%discardShadowAsyncRetryInterval == discardShadowAsyncPublishOffset
}

var (
	discardShadowBlocksCounter                       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/blocks", nil)
	discardShadowCandidatesCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/candidates", nil)
	discardShadowExecutedCounter                     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/executed", nil)
	discardShadowMatchesCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/matches", nil)
	discardShadowMismatchesCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatches", nil)
	discardShadowCoreMatchesCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/core_matches", nil)
	discardShadowCoreMismatchesCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/core_mismatches", nil)
	discardShadowWriteSetMatchesCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_matches", nil)
	discardShadowWriteSetMismatchesCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_mismatches", nil)
	discardShadowWriteSetErrorsCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/state_write_set_errors", nil)
	discardShadowWriteSetApplyEligibleCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_eligible", nil)
	discardShadowWriteSetApplyUnsupportedCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported", nil)
	discardShadowWriteSetApplyMatchesCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_matches", nil)
	discardShadowWriteSetApplyMismatchesCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatches", nil)
	discardShadowWriteSetApplyErrorsCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_errors", nil)
	discardShadowOrderedApplyCandidatesCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/ordered_publisher/candidates", nil)
	discardShadowOrderedApplyMatchesCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/ordered_publisher/matches", nil)
	discardShadowOrderedApplyMismatchesCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/ordered_publisher/mismatches", nil)
	discardShadowOrderedApplyErrorsCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/ordered_publisher/errors", nil)
	discardShadowPreBlocksCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/blocks", nil)
	discardShadowPreTransfersCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/transfers", nil)
	discardShadowPreExecutedCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/executed", nil)
	discardShadowPreCandidatesCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/zero_indegree_candidates", nil)
	discardShadowPreInfoMatchesCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/info_matches", nil)
	discardShadowPreInfoMismatchesCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/info_mismatches", nil)
	discardShadowPreWriteMatchesCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/write_set_matches", nil)
	discardShadowPreWriteMismatchesCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/write_set_mismatches", nil)
	discardShadowPreApplyMatchesCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/apply_matches", nil)
	discardShadowPreApplyMismatchesCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/apply_mismatches", nil)
	discardShadowPreApplyUnsupportedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/apply_unsupported", nil)
	discardShadowPreValidatedCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/validated", nil)
	discardShadowPreOrderedCandidatesCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/ordered_candidates", nil)
	discardShadowPreOrderedMatchesCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/ordered_matches", nil)
	discardShadowPreOrderedMismatchesCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/ordered_mismatches", nil)
	discardShadowPreOrderedErrorsCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/ordered_errors", nil)
	discardShadowPreErrorsCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/errors", nil)
	discardShadowPreWallNanosCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/wall_nanos", nil)
	discardShadowPreBalanceTraceMatchesCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/balance_trace_matches", nil)
	discardShadowPreBalanceTraceMismatchesCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/balance_trace_mismatches", nil)
	discardShadowReadVersionCandidatesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/candidates", nil)
	discardShadowReadVersionPublishableCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/publishable", nil)
	discardShadowReadVersionConflictsCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/read_conflicts", nil)
	discardShadowReadVersionUnsupportedCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/unsupported", nil)
	discardShadowReadVersionDeltaInvalidCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/delta_invalid", nil)
	discardShadowReadVersionSenderCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/sender_conflicts", nil)
	discardShadowReadVersionBarrierCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/barrier_conflicts", nil)
	discardShadowReadVersionDAGMatchesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/dag_matches", nil)
	discardShadowReadVersionDAGMismatchesCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/preexecutor/read_version/dag_mismatches", nil)
	discardShadowSenderChainBlocksCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/blocks", nil)
	discardShadowSenderChainGroupsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/groups", nil)
	discardShadowSenderChainExecutedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/executed", nil)
	discardShadowSenderChainForwardedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/forwarded", nil)
	discardShadowSenderChainCandidatesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/candidates", nil)
	discardShadowSenderChainValidatedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/validated", nil)
	discardShadowSenderChainForwardedOKCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/forwarded_validated", nil)
	discardShadowSenderChainReadConflictsCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/read_conflicts", nil)
	discardShadowSenderChainSenderConflictsCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/sender_conflicts", nil)
	discardShadowSenderChainInfoMismatchesCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/info_mismatches", nil)
	discardShadowSenderChainWriteMismatchesCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/write_set_mismatches", nil)
	discardShadowSenderChainBalanceMismatchesCounter = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/balance_trace_mismatches", nil)
	discardShadowSenderChainErrorsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/errors", nil)
	discardShadowSenderChainWallNanosCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_chain/wall_nanos", nil)
	discardShadowRetryBlocksCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/blocks", nil)
	discardShadowRetryAttemptsCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/attempts", nil)
	discardShadowRetryExecutedCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/executed", nil)
	discardShadowRetryCandidatesCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/candidates", nil)
	discardShadowRetryRecoveredCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/recovered", nil)
	discardShadowRetryValidatedCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/validated", nil)
	discardShadowRetryInfoMismatchCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/info_mismatches", nil)
	discardShadowRetryWriteMismatchCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/write_set_mismatches", nil)
	discardShadowRetryBalanceMismatchCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/balance_trace_mismatches", nil)
	discardShadowRetryErrorsCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/errors", nil)
	discardShadowRetryBudgetSkippedCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/budget_skipped", nil)
	discardShadowRetryCopyNanosCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/copy_nanos", nil)
	discardShadowRetryPrefixRefreshCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/prefix/refreshes", nil)
	discardShadowRetryPrefixReuseCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/prefix/reuses", nil)
	discardShadowRetryPrefixAdvanceCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/prefix/advances", nil)
	discardShadowRetryPrefixAdvanceNanosCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/prefix/advance_nanos", nil)
	discardShadowRetryExecutionNanosCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/execution_nanos", nil)
	discardShadowRetryAsyncCandidatesCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/candidates", nil)
	discardShadowRetryAsyncReadyCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/ready", nil)
	discardShadowRetryAsyncLateCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/late", nil)
	discardShadowRetryAsyncUnknownCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/unknown", nil)
	discardShadowRetryAsyncValidatedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/validated", nil)
	discardShadowRetryAsyncRecoveredCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/recovered", nil)
	discardShadowRetryAsyncReadySlackNanosCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/ready_slack_nanos", nil)
	discardShadowRetryAsyncLateNanosCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_projection/late_nanos", nil)
	discardShadowRetryActualBlocksCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/blocks", nil)
	discardShadowRetryActualJobsCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/jobs", nil)
	discardShadowRetryActualBusySkippedCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/busy_skipped", nil)
	discardShadowRetryActualRunnerCapacityCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/runner_capacity", nil)
	discardShadowRetryActualMaxInflightCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/max_inflight", nil)
	discardShadowRetryActualDeferredCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/lookahead_deferred", nil)
	discardShadowRetryActualSupersededCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/superseded_before_execute", nil)
	discardShadowRetryActualQueueEnqueuedCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/enqueued", nil)
	discardShadowRetryActualQueueDequeuedCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/dequeued", nil)
	discardShadowRetryActualQueueBusyCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/enqueued_while_busy", nil)
	discardShadowRetryActualQueueDroppedCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/dropped", nil)
	discardShadowRetryActualQueueMaxDepthCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/max_depth", nil)
	discardShadowRetryActualQueueWaitNanosCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/queue/wait_nanos", nil)
	discardShadowRetryActualExecutedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/executed", nil)
	discardShadowRetryActualReadyCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/ready", nil)
	discardShadowRetryActualLateCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/late", nil)
	discardShadowRetryActualStaleCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/stale", nil)
	discardShadowRetryActualCandidatesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/candidates", nil)
	discardShadowRetryActualRejectedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected", nil)
	discardShadowRetryActualReadConflictCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected/read_conflict", nil)
	discardShadowRetryActualSenderConflictCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected/sender_conflict", nil)
	discardShadowRetryActualBarrierCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected/barrier", nil)
	discardShadowRetryActualUnsupportedCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected/unsupported", nil)
	discardShadowRetryActualDeltaInvalidCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/rejected/delta_invalid", nil)
	discardShadowRetryActualValidatedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/validated", nil)
	discardShadowRetryActualRecoveredCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/recovered", nil)
	discardShadowRetryActualErrorsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/errors", nil)
	discardShadowRetryActualRawKeysCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/frozen_raw_keys", nil)
	discardShadowRetryActualRawMissCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/frozen_raw_misses", nil)
	discardShadowRetryActualVersionCellsCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/frozen_version_cells", nil)
	discardShadowRetryActualDispatchNanosCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch_nanos", nil)
	discardShadowRetryActualPrefixNanosCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_nanos", nil)
	discardShadowRetryActualPrefixRefreshCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_refreshes", nil)
	discardShadowRetryActualPrefixReuseCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_reuses", nil)
	discardShadowRetryActualPrefixAdvanceCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_advances", nil)
	discardShadowRetryActualPrefixCopyNanosCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_copy_nanos", nil)
	discardShadowRetryActualPrefixAdvanceNsCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/prefix_advance_nanos", nil)
	discardShadowRetryActualRawFreezeNanosCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/raw_freeze_nanos", nil)
	discardShadowRetryActualVersionNanosCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/dispatch/version_snapshot_nanos", nil)
	discardShadowRetryActualPrewarmedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/prewarmed_runners", nil)
	discardShadowRetryActualExecutionNanosCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/execution_nanos", nil)
	discardShadowRetryActualFinishWaitNanosCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/finish_wait_nanos", nil)
	discardShadowRetrySharedStateJobsCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/shared_state/jobs", nil)
	discardShadowRetrySharedStateCopyNanosCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/shared_state/copy_nanos", nil)
	discardShadowRetrySharedStateErrorsCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/shared_state/errors", nil)
	parallelTransferEnabledGauge                     = metrics.NewRegisteredGauge("core/parallel_transfer/enabled", nil)
	parallelTransferBlocksCounter                    = metrics.NewRegisteredCounter("core/parallel_transfer/blocks", nil)
	parallelTransferPreexecutedCounter               = metrics.NewRegisteredCounter("core/parallel_transfer/preexecuted", nil)
	parallelTransferCandidatesCounter                = metrics.NewRegisteredCounter("core/parallel_transfer/candidates", nil)
	parallelTransferPublishedCounter                 = metrics.NewRegisteredCounter("core/parallel_transfer/published", nil)
	parallelTransferConflictFallbackCounter          = metrics.NewRegisteredCounter("core/parallel_transfer/fallback/conflict", nil)
	parallelTransferUnavailableFallbackCounter       = metrics.NewRegisteredCounter("core/parallel_transfer/fallback/unavailable", nil)
	parallelTransferPreflightFallbackCounter         = metrics.NewRegisteredCounter("core/parallel_transfer/fallback/preflight", nil)
	parallelTransferErrorsCounter                    = metrics.NewRegisteredCounter("core/parallel_transfer/errors", nil)
	parallelTransferPreexecutionNanosCounter         = metrics.NewRegisteredCounter("core/parallel_transfer/preexecution_nanos", nil)
	parallelTransferPublicationNanosCounter          = metrics.NewRegisteredCounter("core/parallel_transfer/publication_nanos", nil)
	parallelTransferPublicNetReservationsCounter     = metrics.NewRegisteredCounter("core/parallel_transfer/public_net/reservations", nil)
	parallelTransferPublicNetPublishedCounter        = metrics.NewRegisteredCounter("core/parallel_transfer/public_net/published", nil)
	parallelTransferPublicNetRebasedCounter          = metrics.NewRegisteredCounter("core/parallel_transfer/public_net/rebased", nil)
	parallelTransferPublicNetLimitFallbackCounter    = metrics.NewRegisteredCounter("core/parallel_transfer/public_net/fallback/limit", nil)
	parallelTransferChainPreexecutedCounter          = metrics.NewRegisteredCounter("core/parallel_transfer/sender_chain/preexecuted", nil)
	parallelTransferChainCandidatesCounter           = metrics.NewRegisteredCounter("core/parallel_transfer/sender_chain/candidates", nil)
	parallelTransferChainPublishedCounter            = metrics.NewRegisteredCounter("core/parallel_transfer/sender_chain/published", nil)
	parallelTransferChainPredFallbackCounter         = metrics.NewRegisteredCounter("core/parallel_transfer/sender_chain/fallback/predecessor", nil)
	discardShadowApplyMismatchMissingCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_reason/missing", nil)
	discardShadowApplyMismatchExtraCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_reason/extra", nil)
	discardShadowApplyMismatchPresenceCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_reason/presence", nil)
	discardShadowApplyMismatchCommutativeCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_reason/commutative", nil)
	discardShadowApplyMismatchValueCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_reason/value", nil)
	discardShadowApplyMismatchAccountCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/account", nil)
	discardShadowApplyMismatchAccountFieldCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/account_field", nil)
	discardShadowApplyMismatchWitnessCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/witness", nil)
	discardShadowApplyMismatchStorageCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/storage", nil)
	discardShadowApplyMismatchCodeCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/code", nil)
	discardShadowApplyMismatchMetadataCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/contract_metadata", nil)
	discardShadowApplyMismatchAccountKVCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/account_kv", nil)
	discardShadowApplyMismatchTransientCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/transient_storage", nil)
	discardShadowApplyMismatchDynamicCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/dynamic", nil)
	discardShadowApplyMismatchRawCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/raw_kv", nil)
	discardShadowApplyMismatchOtherCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_mismatch_kind/other", nil)
	discardShadowApplyUnsupportedAccountCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported/account", nil)
	discardShadowApplyUnsupportedGenerationCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported/account_kv_generation", nil)
	discardShadowApplyUnsupportedSelfDestructCounter = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported/self_destruct", nil)
	discardShadowApplyUnsupportedFieldCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported/account_field", nil)
	discardShadowApplyUnsupportedOtherCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/write_set_apply_unsupported/other", nil)
	discardShadowErrorsCounter                       = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/errors", nil)
	discardShadowCopyNanosCounter                    = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/copy_nanos", nil)
	discardShadowExecutionNanosCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/execution_nanos", nil)
	discardShadowLastCandidatesGauge                 = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/candidates", nil)
	discardShadowLastExecutedGauge                   = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/executed", nil)
	discardShadowLastMatchesGauge                    = metrics.NewRegisteredGauge("core/versioned_shadow/discard_worker/last_block/matches", nil)
	discardShadowMismatchVMCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/vm", nil)
	discardShadowMismatchTransferCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/transfer", nil)
	discardShadowMismatchOtherCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch/other", nil)
	discardShadowErrorVMCounter                      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/vm", nil)
	discardShadowErrorTransferCounter                = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/transfer", nil)
	discardShadowErrorOtherCounter                   = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/error/other", nil)
	discardShadowMismatchReceiptCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt", nil)
	discardShadowMismatchReceiptCoreCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core", nil)
	discardShadowMismatchReceiptEnergyCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_energy", nil)
	discardShadowMismatchEnergyUsageCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_usage", nil)
	discardShadowMismatchEnergyFeeCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_fee", nil)
	discardShadowMismatchOriginEnergyCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_origin_energy_usage", nil)
	discardShadowMismatchEnergyTotalCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_usage_total", nil)
	discardShadowMismatchReceiptBandwidthCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_bandwidth", nil)
	discardShadowMismatchReceiptResultCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_core_result", nil)
	discardShadowMismatchOwnerDiagnosticCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_owner_diagnostic", nil)
	discardShadowMismatchEnergyDiagnosticCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/receipt_energy_diagnostic", nil)
	discardShadowMismatchFeeCounter                  = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/fee", nil)
	discardShadowMismatchResultCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/contract_result", nil)
	discardShadowMismatchLogsCounter                 = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/logs", nil)
	discardShadowMismatchInternalCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/internal_transactions", nil)
	discardShadowMismatchStatusCounter               = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/status", nil)
	discardShadowMismatchMessageCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/res_message", nil)
	discardShadowMismatchAddressCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/contract_address", nil)
	discardShadowMismatchIdentityCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/identity", nil)
	discardShadowMismatchOtherFieldCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/discard_worker/mismatch_field/other", nil)
)

var (
	discardShadowVMSenderChainBlocksCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/blocks", nil)
	discardShadowVMSenderChainGroupsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/groups", nil)
	discardShadowVMSenderChainExecutedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/executed", nil)
	discardShadowVMSenderChainForwardedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/forwarded", nil)
	discardShadowVMSenderChainCandidatesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/candidates", nil)
	discardShadowVMSenderChainValidatedCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/validated", nil)
	discardShadowVMSenderChainForwardedOKCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/forwarded_validated", nil)
	discardShadowVMSenderChainReadConflictsCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/read_conflicts", nil)
	discardShadowVMSenderChainSenderConflictsCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/sender_conflicts", nil)
	discardShadowVMSenderChainInfoMismatchesCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/info_mismatches", nil)
	discardShadowVMSenderChainWriteMismatchesCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/write_set_mismatches", nil)
	discardShadowVMSenderChainBalanceMismatchesCounter = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/balance_trace_mismatches", nil)
	discardShadowVMSenderChainErrorsCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/errors", nil)
	discardShadowVMSenderChainWallNanosCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/wall_nanos", nil)
	discardShadowVMSenderChainPublicNetCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/candidates", nil)
	discardShadowVMSenderChainPublicNetOnlyCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/write_set_mismatches/public_net_only", nil)
	discardShadowVMSenderChainOtherWriteCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/write_set_mismatches/other", nil)
	discardShadowVMSenderChainResultErrorsCounter      = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/result", nil)
	discardShadowVMSenderChainMissingInfoCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/missing_info", nil)
	discardShadowVMSenderChainWriteErrorsCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/write_set", nil)
	discardShadowVMSenderChainApplyUnsupportedCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/apply_unsupported", nil)
	discardShadowVMSenderChainApplyErrorsCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/apply", nil)
	discardShadowVMSenderChainApplyMismatchCounter     = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/apply_mismatch", nil)
	discardShadowVMSenderChainReadinessCounter         = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/error/readiness", nil)
	discardShadowVMPublicNetProjectionCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/candidates", nil)
	discardShadowVMPublicNetAdmittedCounter            = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/admitted", nil)
	discardShadowVMPublicNetRebasedCounter             = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/rebased", nil)
	discardShadowVMPublicNetLimitRejectedCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/limit_rejected", nil)
	discardShadowVMPublicNetProjectionMatchesCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/write_set_matches", nil)
	discardShadowVMPublicNetProjectionMismatchCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/write_set_mismatches", nil)
	discardShadowVMPublicNetProjectionMissingCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/public_net/projection/missing", nil)
	discardShadowVMBlockEnergyProjectionCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/block_energy/projection/candidates", nil)
	discardShadowVMBlockEnergyObservedCounter          = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/block_energy/projection/observed", nil)
	discardShadowVMBlockEnergyMatchesCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/block_energy/projection/matches", nil)
	discardShadowVMBlockEnergyMismatchesCounter        = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/block_energy/projection/mismatches", nil)
	discardShadowVMBlockEnergyMissingCounter           = metrics.NewRegisteredCounter("core/versioned_shadow/vm_sender_chain/block_energy/projection/missing", nil)
)

var (
	parallelTransferRetryCandidatesCounter                = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/candidates", nil)
	parallelTransferRetryPublishedCounter                 = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/published", nil)
	parallelTransferRetryPreflightFallbackCounter         = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/fallback/preflight", nil)
	parallelTransferRetryPublicNetFallbackCounter         = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/fallback/public_net", nil)
	parallelTransferRetryErrorsCounter                    = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/errors", nil)
	parallelTransferRetryPublicationNanosCounter          = metrics.NewRegisteredCounter("core/parallel_transfer/sender_retry/publication_nanos", nil)
	discardShadowRetryActualPublishedCounter              = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/published", nil)
	discardShadowRetryActualPublishedWriteOKCounter       = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/published/write_set_matches", nil)
	discardShadowRetryActualPublishedWriteMismatchCounter = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/published/write_set_mismatches", nil)
)

var (
	discardShadowRetryActualWorkerPrefixJobsCounter    = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/worker/prefix_jobs", nil)
	discardShadowRetryActualWorkerPrefixAdvanceCounter = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/worker/prefix_advances", nil)
	discardShadowRetryActualWorkerPrefixNanosCounter   = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/worker/prefix_nanos", nil)
	discardShadowRetryActualWorkerPrefixErrorsCounter  = metrics.NewRegisteredCounter("core/versioned_shadow/sender_retry/async_actual/worker/prefix_errors", nil)
)

// discardKVOverlay isolates rawdb writes performed by actuators. Reads first
// see the current transaction, then post-images explicitly forwarded by the
// current sender chain, and finally the immutable canonical block view.
type discardKVOverlay struct {
	parent           actuator.BufferedKVStore
	recorder         *state.TransactionAccessRecorder
	puts             map[string][]byte
	deletes          map[string]struct{}
	forwardedPuts    map[string][]byte
	forwardedDeletes map[string]struct{}
}

var errDiscardShadowFrozenRawMiss = errors.New("discard shadow frozen raw key was not captured")

// discardShadowFrozenKV is the immutable raw-KV capability handed to a
// background retry. Every permitted key is copied at the settled canonical
// boundary. An unexpected key is an execution error instead of falling
// through to the live block buffer, which may already contain later writes.
type discardShadowFrozenKV struct {
	values  map[string][]byte
	present map[string]bool
	misses  int64
}

func (db *discardShadowFrozenKV) Has(key []byte) (bool, error) {
	if db == nil {
		return false, errDiscardShadowFrozenRawMiss
	}
	present, ok := db.present[string(key)]
	if !ok {
		db.misses++
		return false, errDiscardShadowFrozenRawMiss
	}
	return present, nil
}

func (db *discardShadowFrozenKV) Get(key []byte) ([]byte, error) {
	if db == nil {
		return nil, errDiscardShadowFrozenRawMiss
	}
	keyString := string(key)
	present, ok := db.present[keyString]
	if !ok {
		db.misses++
		return nil, errDiscardShadowFrozenRawMiss
	}
	if !present {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), db.values[keyString]...), nil
}

func (db *discardShadowFrozenKV) Put([]byte, []byte) error {
	return errors.New("discard shadow frozen raw view is read-only")
}

func (db *discardShadowFrozenKV) Delete([]byte) error {
	return errors.New("discard shadow frozen raw view is read-only")
}

func (db *discardKVOverlay) reset() {
	clear(db.puts)
	clear(db.deletes)
}

func (db *discardKVOverlay) resetForwarded() {
	db.reset()
	clear(db.forwardedPuts)
	clear(db.forwardedDeletes)
}

// forward promotes the current transaction's isolated raw mutations into the
// sender-chain view. Values remain worker-local and are discarded when that
// chain finishes.
func (db *discardKVOverlay) forward() {
	for key, value := range db.puts {
		if db.forwardedPuts == nil {
			db.forwardedPuts = make(map[string][]byte)
		}
		db.forwardedPuts[key] = append(db.forwardedPuts[key][:0], value...)
		delete(db.forwardedDeletes, key)
	}
	for key := range db.deletes {
		if db.forwardedDeletes == nil {
			db.forwardedDeletes = make(map[string]struct{})
		}
		delete(db.forwardedPuts, key)
		db.forwardedDeletes[key] = struct{}{}
	}
	db.reset()
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
	if _, ok := db.forwardedPuts[keyString]; ok {
		return true, nil
	}
	if _, ok := db.forwardedDeletes[keyString]; ok {
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
	if value, ok := db.forwardedPuts[keyString]; ok {
		return append([]byte(nil), value...), nil
	}
	if _, ok := db.forwardedDeletes[keyString]; ok {
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
	sampled   bool
}

func prepareDiscardShadowBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, blockNum uint64) *discardShadowBlock {
	return prepareTransferExecutionBlock(statedb, dynProps, blockNum, false)
}

func prepareTransferExecutionBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, blockNum uint64, force bool) *discardShadowBlock {
	sampled := blockNum%discardShadowSampleInterval == 0
	if statedb == nil || dynProps == nil || (!sampled && !force) {
		return nil
	}
	started := time.Now()
	base, err := statedb.CopyBlockExecutionBase()
	if err != nil {
		discardShadowErrorsCounter.Inc(1)
		return nil
	}
	base.SetDynamicProperties(dynProps.Copy())
	return &discardShadowBlock{base: base, copyNanos: time.Since(started).Nanoseconds(), sampled: sampled}
}

type discardShadowTaskResult struct {
	txIndex              int
	class                discardShadowTransactionClass
	mismatch             discardShadowMismatch
	coreMatch            bool
	matched              bool
	writeSetMatch        bool
	writeSetErr          error
	applyEligible        bool
	applyUnsupported     discardShadowApplyUnsupported
	applyMatch           bool
	applyMismatch        discardShadowApplyMismatch
	applyErr             error
	writes               state.TransactionWriteSet
	reads                state.TransactionReadSet
	info                 *corepb.TransactionInfo
	balanceTrace         *contractpb.TransactionBalanceTrace
	publicNet            state.PublicNetReservation
	publicNetValid       bool
	senderPredecessor    int
	senderVersioned      bool
	settledPrefix        int
	hasSettledPrefix     bool
	incarnation          uint32
	retryStartTx         int
	retryCompletionNanos int64
	err                  error
}

type discardShadowOrderedApplyStats struct {
	candidates int64
	matches    int64
	mismatches int64
	errors     int64
}

type discardShadowPreexecution struct {
	results       []discardShadowTaskResult
	resultByTx    []int
	readVersions  []discardShadowReadVersionResult
	readValidated []bool
	published     []bool
	senderTasks   []discardShadowSenderChainTask
	senderTaskOK  []bool
	senderNext    []int
	groups        int
	wallNanos     int64
	retryStates   []*state.StateDB
	publicNet     []discardShadowPublicNetProjection
	blockEnergy   []discardShadowBlockEnergyProjection
}

type discardShadowPublicNetProjection struct {
	observed        bool
	admitted        bool
	rebased         bool
	expectedUsage   int64
	expectedTime    int64
	expectedTimeSet bool
}

type discardShadowBlockEnergyProjection struct {
	observed  bool
	expected  int64
	validated bool
	match     bool
}

type discardShadowSenderChainStats struct {
	groups                int64
	executed              int64
	forwarded             int64
	candidates            int64
	validated             int64
	forwardedValidated    int64
	readConflicts         int64
	senderConflicts       int64
	infoMismatches        int64
	writeMismatches       int64
	balanceMismatches     int64
	errors                int64
	publicNetCandidates   int64
	publicNetOnly         int64
	otherWriteMismatch    int64
	resultErrors          int64
	missingInfo           int64
	writeSetErrors        int64
	applyUnsupported      int64
	applyErrors           int64
	applyMismatches       int64
	readinessRejected     int64
	publicNetProjected    int64
	publicNetAdmitted     int64
	publicNetRebased      int64
	publicNetRejected     int64
	projectionMatches     int64
	projectionMismatches  int64
	projectionMissing     int64
	blockEnergyCandidates int64
	blockEnergyObserved   int64
	blockEnergyMatches    int64
	blockEnergyMismatches int64
	blockEnergyMissing    int64
}

type discardShadowAsyncPrefixStats struct {
	jobs     int64
	advances int64
	nanos    int64
	errors   int64
}

type discardShadowAsyncPublishStats struct {
	published       int64
	writeMatches    int64
	writeMismatches int64
}

type discardShadowSenderRetryStats struct {
	attempts            int64
	executed            int64
	candidates          int64
	recovered           int64
	validated           int64
	infoMismatches      int64
	writeMismatches     int64
	balanceMismatches   int64
	errors              int64
	budgetSkipped       int64
	copyNanos           int64
	prefixRefreshes     int64
	prefixReuses        int64
	prefixAdvances      int64
	prefixAdvanceNanos  int64
	executionNanos      int64
	asyncCandidates     int64
	asyncReady          int64
	asyncLate           int64
	asyncUnknown        int64
	asyncValidated      int64
	asyncRecovered      int64
	asyncReadySlackNs   int64
	asyncLateNs         int64
	actualJobs          int64
	actualBusySkipped   int64
	actualCapacity      int64
	actualMaxInflight   int64
	actualDeferred      int64
	actualSuperseded    int64
	actualQueueEnqueued int64
	actualQueueDequeued int64
	actualQueueBusy     int64
	actualQueueDropped  int64
	actualQueueMaxDepth int64
	actualQueueWaitNs   int64
	workerPrefix        discardShadowAsyncPrefixStats
	publish             discardShadowAsyncPublishStats
	actualExecuted      int64
	actualReady         int64
	actualLate          int64
	actualStale         int64
	actualCandidates    int64
	actualRejected      int64
	actualReadConflict  int64
	actualSender        int64
	actualBarrier       int64
	actualUnsupported   int64
	actualDeltaInvalid  int64
	actualValidated     int64
	actualRecovered     int64
	actualErrors        int64
	actualRawKeys       int64
	actualRawMisses     int64
	actualVersionCells  int64
	actualDispatchNs    int64
	actualPrefixNs      int64
	actualRefreshes     int64
	actualReuses        int64
	actualAdvances      int64
	actualCopyNs        int64
	actualAdvanceNs     int64
	actualRawFreezeNs   int64
	actualVersionNs     int64
	actualExecutionNs   int64
	actualFinishWaitNs  int64
	actualPrewarmed     int64
	sharedStateJobs     int64
	sharedStateCopyNs   int64
	sharedStateErrors   int64
}

type discardShadowAsyncRetryEvent struct {
	result             *discardShadowTaskResult
	runner             *discardShadowRetryRunner
	done               bool
	nanos              int64
	rawMisses          int64
	superseded         int64
	dropped            int64
	prefixAdvances     int64
	prefixAdvanceNanos int64
	prefixNanos        int64
	prefixError        bool
	sharedState        bool
	sharedStateCopyNs  int64
}

// discardShadowRetryRunner owns either the reusable private-prefix worker used
// by the synchronous observer or an immutable block-start template used by the
// asynchronous shared-version scheduler. A busy async runner is exclusively
// owned by its incarnation until the done event.
type discardShadowRetryRunner struct {
	worker         *discardShadowWorker
	blockBase      *state.StateDB
	prefixRaw      discardKVOverlay
	prefixRecorder state.TransactionAccessRecorder
	settledThrough int
	busy           bool
}

type discardShadowAsyncRetryTask struct {
	txIndex           int
	incarnation       uint32
	senderPredecessor int
	senderVersioned   bool
}

// discardShadowAsyncRetryRequest is a retry incarnation frozen at its
// canonical conflict boundary. Keeping raw/version/dynamic inputs on the
// request makes it safe to wait in the block-scoped priority queue while the
// fixed runner pool is busy.
type discardShadowAsyncRetryRequest struct {
	txIndex     int
	tasks       []discardShadowAsyncRetryTask
	frozenRaw   *discardShadowFrozenKV
	versionView versionedAccessShadow
	dynProps    *state.DynamicProperties
	blockBase   *state.StateDB
	enqueuedAt  time.Time
}

type discardShadowAsyncRetryQueue []*discardShadowAsyncRetryRequest

func (queue discardShadowAsyncRetryQueue) Len() int { return len(queue) }

func (queue discardShadowAsyncRetryQueue) Less(left, right int) bool {
	if queue[left].txIndex != queue[right].txIndex {
		return queue[left].txIndex < queue[right].txIndex
	}
	return queue[left].tasks[0].incarnation > queue[right].tasks[0].incarnation
}

func (queue discardShadowAsyncRetryQueue) Swap(left, right int) {
	queue[left], queue[right] = queue[right], queue[left]
}

func (queue *discardShadowAsyncRetryQueue) Push(value any) {
	*queue = append(*queue, value.(*discardShadowAsyncRetryRequest))
}

func (queue *discardShadowAsyncRetryQueue) Pop() any {
	old := *queue
	last := len(old) - 1
	request := old[last]
	old[last] = nil
	*queue = old[:last]
	return request
}

// discardShadowSenderRetry holds the newest sampled incarnation for each
// sender-chain transaction. It never publishes state. At the real canonical
// boundary it may rebuild a failed suffix from a reusable settled-prefix
// runner, then freezes the result selected for later serial comparison.
type discardShadowSenderRetry struct {
	source             *discardShadowPreexecution
	results            []discardShadowTaskResult
	available          []bool
	selected           []discardShadowTaskResult
	selectedOK         []bool
	selectedPublished  []bool
	selectedAsyncReady []bool
	incarnations       []uint32
	asyncIncarnations  []atomic.Uint32
	runner             *discardShadowRetryRunner
	async              bool
	publish            bool
	asyncRunners       []*discardShadowRetryRunner
	asyncActive        int
	asyncScheduled     int64
	asyncEvents        chan discardShadowAsyncRetryEvent
	asyncQueue         discardShadowAsyncRetryQueue
	stats              discardShadowSenderRetryStats
}

type discardShadowPreexecutionStats struct {
	transfers         int64
	executed          int64
	candidates        int64
	infoMatches       int64
	infoMismatches    int64
	writeMatches      int64
	writeMismatches   int64
	applyMatches      int64
	applyMismatches   int64
	applyUnsupported  int64
	validated         int64
	orderedCandidates int64
	orderedMatches    int64
	orderedMismatches int64
	orderedErrors     int64
	errors            int64
	balanceMatches    int64
	balanceMismatches int64
	readCandidates    int64
	readPublishable   int64
	readConflicts     int64
	readUnsupported   int64
	readDeltaInvalid  int64
	readSender        int64
	readBarrier       int64
	readDAGMatches    int64
	readDAGMismatches int64
}

type discardShadowReadVersionResult struct {
	publishable  bool
	readConflict bool
	unsupported  bool
	deltaInvalid bool
	sender       bool
	predecessor  bool
	barrier      bool
}

type discardShadowApplyMismatch uint32

const (
	discardShadowApplyMismatchMissing discardShadowApplyMismatch = 1 << iota
	discardShadowApplyMismatchExtra
	discardShadowApplyMismatchPresence
	discardShadowApplyMismatchCommutative
	discardShadowApplyMismatchValue
	discardShadowApplyMismatchAccount
	discardShadowApplyMismatchAccountField
	discardShadowApplyMismatchWitness
	discardShadowApplyMismatchStorage
	discardShadowApplyMismatchCode
	discardShadowApplyMismatchMetadata
	discardShadowApplyMismatchAccountKV
	discardShadowApplyMismatchTransient
	discardShadowApplyMismatchDynamic
	discardShadowApplyMismatchRaw
	discardShadowApplyMismatchOther
)

func addDiscardShadowApplyMismatchKind(mismatch discardShadowApplyMismatch, key state.TransactionAccessKey) discardShadowApplyMismatch {
	switch key.Kind {
	case state.TransactionAccessAccount:
		return mismatch | discardShadowApplyMismatchAccount
	case state.TransactionAccessAccountField:
		return mismatch | discardShadowApplyMismatchAccountField
	case state.TransactionAccessWitness:
		return mismatch | discardShadowApplyMismatchWitness
	case state.TransactionAccessStorage:
		return mismatch | discardShadowApplyMismatchStorage
	case state.TransactionAccessCode:
		return mismatch | discardShadowApplyMismatchCode
	case state.TransactionAccessContractMetadata:
		return mismatch | discardShadowApplyMismatchMetadata
	case state.TransactionAccessAccountKV, state.TransactionAccessAccountKVGeneration:
		return mismatch | discardShadowApplyMismatchAccountKV
	case state.TransactionAccessTransientStorage:
		return mismatch | discardShadowApplyMismatchTransient
	case state.TransactionAccessDynamicInt, state.TransactionAccessDynamicString, state.TransactionAccessDynamicHash:
		return mismatch | discardShadowApplyMismatchDynamic
	case state.TransactionAccessRawKV:
		return mismatch | discardShadowApplyMismatchRaw
	default:
		return mismatch | discardShadowApplyMismatchOther
	}
}

func classifyDiscardShadowApplyMismatch(applied, expected state.TransactionWriteSet) discardShadowApplyMismatch {
	var mismatch discardShadowApplyMismatch
	for key, expectedValue := range expected {
		appliedValue, ok := applied[key]
		if !ok {
			mismatch = addDiscardShadowApplyMismatchKind(mismatch|discardShadowApplyMismatchMissing, key)
			continue
		}
		if expectedValue.Exists != appliedValue.Exists {
			mismatch |= discardShadowApplyMismatchPresence
		}
		if expectedValue.Commutative != appliedValue.Commutative {
			mismatch |= discardShadowApplyMismatchCommutative
		}
		if !bytes.Equal(expectedValue.Value, appliedValue.Value) {
			mismatch |= discardShadowApplyMismatchValue
		}
		if expectedValue.Exists != appliedValue.Exists || expectedValue.Commutative != appliedValue.Commutative || !bytes.Equal(expectedValue.Value, appliedValue.Value) {
			mismatch = addDiscardShadowApplyMismatchKind(mismatch, key)
		}
	}
	for key := range applied {
		if _, ok := expected[key]; !ok {
			mismatch = addDiscardShadowApplyMismatchKind(mismatch|discardShadowApplyMismatchExtra, key)
		}
	}
	return mismatch
}

type discardShadowApplyUnsupported uint8

const (
	discardShadowApplyUnsupportedAccount discardShadowApplyUnsupported = 1 << iota
	discardShadowApplyUnsupportedGeneration
	discardShadowApplyUnsupportedSelfDestruct
	discardShadowApplyUnsupportedField
	discardShadowApplyUnsupportedOther
)

func classifyDiscardShadowApplyUnsupported(writes state.TransactionWriteSet) discardShadowApplyUnsupported {
	var unsupported discardShadowApplyUnsupported
	for key := range writes {
		switch key.Kind {
		case state.TransactionAccessAccount:
			unsupported |= discardShadowApplyUnsupportedAccount
		case state.TransactionAccessAccountKVGeneration:
			unsupported |= discardShadowApplyUnsupportedGeneration
		case state.TransactionAccessSelfDestruct:
			unsupported |= discardShadowApplyUnsupportedSelfDestruct
		case state.TransactionAccessAccountField:
			switch key.AccountField {
			case state.TransactionAccountFieldAccountType,
				state.TransactionAccountFieldBalance,
				state.TransactionAccountFieldAllowance,
				state.TransactionAccountFieldLatestWithdrawTime,
				state.TransactionAccountFieldNetUsage,
				state.TransactionAccountFieldLatestOperationTime,
				state.TransactionAccountFieldLatestConsumeTime,
				state.TransactionAccountFieldFreeNetUsage,
				state.TransactionAccountFieldLatestConsumeFreeTime,
				state.TransactionAccountFieldNetWindow:
			default:
				unsupported |= discardShadowApplyUnsupportedField
			}
		}
	}
	if unsupported == 0 {
		unsupported = discardShadowApplyUnsupportedOther
	}
	return unsupported
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
	// Shadow execution is diagnostic and must never take down canonical block
	// import. A failed speculative execution has no TransactionInfo; classify
	// that as a mismatch instead of dereferencing the absent protobuf below.
	if shadow == nil || canonical == nil {
		return discardShadowMismatchOtherField
	}
	var mismatch discardShadowMismatch
	if !proto.Equal(shadow.GetReceipt(), canonical.GetReceipt()) {
		mismatch |= discardShadowMismatchReceipt
		shadowReceipt := &corepb.ResourceReceipt{}
		if receipt := shadow.GetReceipt(); receipt != nil {
			shadowReceipt = proto.Clone(receipt).(*corepb.ResourceReceipt)
		}
		canonicalReceipt := &corepb.ResourceReceipt{}
		if receipt := canonical.GetReceipt(); receipt != nil {
			canonicalReceipt = proto.Clone(receipt).(*corepb.ResourceReceipt)
		}
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
	canonicalBalanceTraces  []*contractpb.TransactionBalanceTrace
	canonicalWriteSets      []state.TransactionWriteSet
	captureBalanceTrace     bool
	retainInfos             bool
}

type discardShadowRunStats struct {
	candidates int64
	executed   int64
	matches    int64
	mismatches int64
	errors     int64
}

// preexecuteTransfers runs the first deliberately narrow speculative cohort
// before canonical serial execution begins. Workers share only immutable
// block-start copies and execute concurrently with each other. The retained
// results are observe-only until finishTransferPreexecution validates them
// against canonical execution and the captured dependency graph.
func (shadow *discardShadowBlock) preexecuteTransfers(cfg discardShadowRunConfig) *discardShadowPreexecution {
	if shadow == nil || shadow.base == nil || cfg.block == nil {
		return nil
	}
	candidates := make([]int, 0, discardShadowWorkerCount*2)
	for txIndex, tx := range cfg.transactions {
		if tx != nil && tx.ContractType() == corepb.Transaction_Contract_TransferContract {
			candidates = append(candidates, txIndex)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	started := time.Now()
	workerCount := min(discardShadowWorkerCount, len(candidates))
	workerStates := make([]*state.StateDB, 0, workerCount)
	workerStates = append(workerStates, shadow.base)
	for len(workerStates) < workerCount {
		workerState, err := shadow.base.Copy()
		if err != nil {
			break
		}
		workerState.SetDynamicProperties(shadow.base.DynamicProperties().Copy())
		workerStates = append(workerStates, workerState)
	}
	if len(workerStates) == 0 {
		return nil
	}
	if cfg.captureBalanceTrace {
		blockHash := cfg.block.Hash()
		for _, workerState := range workerStates {
			workerState.BeginBalanceTrace(int64(cfg.block.Number()), blockHash.Bytes(), cfg.block.Timestamp())
		}
	}

	preCfg := cfg
	preCfg.canonicalInfos = nil
	preCfg.canonicalWriteSets = nil
	preCfg.retainInfos = true
	jobs := make(chan int)
	results := make(chan discardShadowTaskResult, len(candidates))
	var workers sync.WaitGroup
	for _, workerState := range workerStates {
		workers.Add(1)
		go func(workerState *state.StateDB) {
			defer workers.Done()
			worker := discardShadowWorker{
				state:     workerState,
				dynProps:  workerState.DynamicProperties(),
				db:        discardKVOverlay{parent: preCfg.db},
				forkCache: forks.NewVersionPassCache().BlockScope(),
			}
			worker.db.recorder = &worker.recorder
			for txIndex := range jobs {
				results <- worker.execute(txIndex, preCfg)
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

	retained := make([]discardShadowTaskResult, 0, len(candidates))
	for result := range results {
		retained = append(retained, result)
	}
	resultByTx := make([]int, len(cfg.transactions))
	for txIndex := range resultByTx {
		resultByTx[txIndex] = -1
	}
	for resultIndex, result := range retained {
		if result.txIndex >= 0 && result.txIndex < len(resultByTx) {
			resultByTx[result.txIndex] = resultIndex
		}
	}
	return &discardShadowPreexecution{
		results:       retained,
		resultByTx:    resultByTx,
		readVersions:  make([]discardShadowReadVersionResult, len(retained)),
		readValidated: make([]bool, len(retained)),
		published:     make([]bool, len(retained)),
		wallNanos:     time.Since(started).Nanoseconds(),
	}
}

type discardShadowSenderChainTask struct {
	txIndex           int
	senderPredecessor int
	senderVersioned   bool
}

// discardShadowRetryWriteFilter is the narrow Transfer counterpart of
// Erigon's VersionMap value lookup. Prefix transactions only materialize
// post-images that a possible retry suffix read at block start. Hierarchical
// Account/AccountField overlap is retained exactly; newly discovered retry
// reads still fail version validation if an omitted predecessor wrote them.
type discardShadowRetryWriteFilter struct {
	exact            map[state.TransactionAccessKey]struct{}
	accountReads     map[tcommon.Address]struct{}
	fullAccountReads map[tcommon.Address]struct{}
}

func (filter *discardShadowRetryWriteFilter) include(key state.TransactionAccessKey) bool {
	if filter == nil {
		return true
	}
	if _, ok := filter.exact[key]; ok {
		return true
	}
	switch key.Kind {
	case state.TransactionAccessAccount:
		_, ok := filter.accountReads[key.Address]
		return ok
	case state.TransactionAccessAccountField:
		_, ok := filter.fullAccountReads[key.Address]
		return ok
	default:
		return false
	}
}

func newDiscardShadowRetryWriteCapture(source *discardShadowPreexecution, transactionCount int) (func(state.TransactionAccessKey) bool, []bool, bool) {
	if source == nil || transactionCount <= 0 {
		return nil, nil, false
	}
	filter := &discardShadowRetryWriteFilter{
		exact:            make(map[state.TransactionAccessKey]struct{}, 32),
		accountReads:     make(map[tcommon.Address]struct{}, 8),
		fullAccountReads: make(map[tcommon.Address]struct{}, 4),
	}
	fullTransactions := make([]bool, transactionCount)
	recorderOnly := true
	for _, result := range source.results {
		txIndex := result.txIndex
		if txIndex < 0 || txIndex >= transactionCount || txIndex >= len(source.senderNext) ||
			(!result.senderVersioned && source.senderNext[txIndex] < 0) {
			continue
		}
		// Any member of a retryable sender chain can become the canonical
		// carrier, so retain its complete WriteSet for post-publication audit.
		fullTransactions[txIndex] = true
		for _, read := range result.reads.Reads {
			if read.Mode&(state.TransactionAccessRead|state.TransactionAccessCommutativeRead) == 0 {
				continue
			}
			// DynamicProperties and exact raw-KV values are frozen from the
			// canonical enqueue boundary in their own request carriers. Replaying
			// them through every intervening prefix WriteSet would duplicate that
			// authoritative snapshot and add serial materialization work.
			switch read.Key.Kind {
			case state.TransactionAccessDynamicInt,
				state.TransactionAccessDynamicString,
				state.TransactionAccessDynamicHash,
				state.TransactionAccessRawKV:
				continue
			}
			if !state.TransactionAccessRecorderCoversWrites(read.Key.Kind) {
				recorderOnly = false
			}
			filter.exact[read.Key] = struct{}{}
			switch read.Key.Kind {
			case state.TransactionAccessAccount:
				filter.accountReads[read.Key.Address] = struct{}{}
				filter.fullAccountReads[read.Key.Address] = struct{}{}
			case state.TransactionAccessAccountField:
				filter.accountReads[read.Key.Address] = struct{}{}
			}
		}
	}
	return filter.include, fullTransactions, recorderOnly
}

type discardShadowSenderChainFilter func(*types.Transaction) bool

func discardShadowTransferFilter(tx *types.Transaction) bool {
	return tx != nil && tx.ContractType() == corepb.Transaction_Contract_TransferContract
}

func discardShadowVMFilter(tx *types.Transaction) bool {
	return classifyDiscardShadowTransaction(tx) == discardShadowVM
}

// discardShadowSenderChains returns independent scheduling units for one
// actuator family. A chain may span transactions from other senders, but it is
// broken when the same sender has an intervening transaction outside the
// selected family because that predecessor was not executed by this worker.
func discardShadowSenderChains(transactions []*types.Transaction, eligible discardShadowSenderChainFilter) [][]discardShadowSenderChainTask {
	chains := make([][]discardShadowSenderChainTask, 0, discardShadowWorkerCount*2)
	lastSenderTx := make(map[tcommon.Address]int, len(transactions)/4+1)
	chainByLastTx := make(map[int]int, len(transactions)/4+1)
	for txIndex, tx := range transactions {
		if tx == nil || tx.Contract() == nil {
			continue
		}
		ownerBytes, shielded, err := tx.ContractOwnerAddress()
		if err != nil || shielded || len(ownerBytes) != tcommon.AddressLength {
			continue
		}
		owner := tcommon.BytesToAddress(ownerBytes)
		if !owner.ValidPrefix() {
			continue
		}
		previous, hasPrevious := lastSenderTx[owner]
		lastSenderTx[owner] = txIndex
		if eligible == nil || !eligible(tx) {
			continue
		}
		task := discardShadowSenderChainTask{txIndex: txIndex, senderPredecessor: previous}
		if chainIndex, ok := chainByLastTx[previous]; hasPrevious && ok {
			task.senderVersioned = true
			chains[chainIndex] = append(chains[chainIndex], task)
			chainByLastTx[txIndex] = chainIndex
			continue
		}
		chains = append(chains, []discardShadowSenderChainTask{task})
		chainByLastTx[txIndex] = len(chains) - 1
	}
	return chains
}

func transferSenderChains(transactions []*types.Transaction) [][]discardShadowSenderChainTask {
	return discardShadowSenderChains(transactions, discardShadowTransferFilter)
}

func vmSenderChains(transactions []*types.Transaction) [][]discardShadowSenderChainTask {
	return discardShadowSenderChains(transactions, discardShadowVMFilter)
}

func annotateSenderChainReadVersions(reads *state.TransactionReadSet, versions *versionedAccessShadow, txIndex int) {
	if reads == nil || versions == nil {
		return
	}
	for readIndex := range reads.Reads {
		read := &reads.Reads[readIndex]
		if read.Mode&state.TransactionAccessRead == 0 {
			continue
		}
		if previous, ok := versions.typedPreviousVersion(read.Key, txIndex); ok {
			read.ExpectedWriter = previous
			read.HasExpectedWriter = true
		}
	}
}

func installSenderChainWrites(versions *versionedAccessShadow, writes state.TransactionWriteSet, txIndex int) {
	for key := range writes {
		versions.installRecordedWrite(key, txIndex)
	}
}

// advanceSenderChain installs one already-verified post-image into the private
// worker state. Typed StateDB and DynamicProperties writes are journaled;
// raw-KV mutations are promoted into the worker-local chain overlay.
func (worker *discardShadowWorker) advanceSenderChain(writes state.TransactionWriteSet) error {
	return worker.advanceSenderChainWrites(writes, false)
}

func (worker *discardShadowWorker) advanceSenderChainWrites(writes state.TransactionWriteSet, forwardRaw bool) error {
	if !forwardRaw {
		for key := range writes {
			if key.Kind == state.TransactionAccessRawKV {
				return errors.New("sender-chain raw KV forwarding is unsupported")
			}
		}
	}
	worker.applyRecorder.Reset(64)
	worker.db.reset()
	worker.db.recorder = &worker.applyRecorder
	if err := worker.state.ApplyTransactionWriteSetRecorded(writes, worker.dynProps, &worker.db, &worker.applyRecorder); err != nil {
		return err
	}
	worker.state.FinalizeTransaction()
	if forwardRaw {
		worker.db.forward()
	} else {
		worker.db.reset()
	}
	worker.db.recorder = &worker.recorder
	return nil
}

// preexecuteTransferSenderChains is the observe-only bridge to Erigon's
// previous-sender scheduler. Each worker executes one sender chain at a time,
// forwarding verified typed post-images between its members. Results are never
// used by canonical publication in this phase; sampled serial execution later
// checks their version provenance and complete observable output.
func (shadow *discardShadowBlock) preexecuteTransferSenderChains(cfg discardShadowRunConfig) *discardShadowPreexecution {
	return shadow.preexecuteTransferSenderChainsWithRetryState(cfg, false)
}

func (shadow *discardShadowBlock) preexecuteTransferSenderChainsWithRetryState(cfg discardShadowRunConfig, retainRetryState bool) *discardShadowPreexecution {
	return shadow.preexecuteSenderChainsWithRetryState(cfg, transferSenderChains(cfg.transactions), preexecutedTransferReady, false, retainRetryState)
}

// preexecuteVMSenderChains is a sampled, observe-only expansion of the same
// scheduler to Trigger/CreateSmartContract. Unlike Transfer publication, VM
// readiness deliberately permits energy-bearing receipts; ordered block
// resource settlement remains serial and no VM result is published here.
func (shadow *discardShadowBlock) preexecuteVMSenderChains(cfg discardShadowRunConfig) *discardShadowPreexecution {
	pre := shadow.preexecuteSenderChainsWithRetryState(cfg, vmSenderChains(cfg.transactions), preexecutedResultReady, true, false)
	if pre != nil {
		pre.publicNet = make([]discardShadowPublicNetProjection, len(pre.results))
		pre.blockEnergy = make([]discardShadowBlockEnergyProjection, len(pre.results))
	}
	return pre
}

func (shadow *discardShadowBlock) preexecuteSenderChainsWithRetryState(cfg discardShadowRunConfig, chains [][]discardShadowSenderChainTask, ready func(*discardShadowTaskResult) bool, forwardRaw, retainRetryState bool) *discardShadowPreexecution {
	if shadow == nil || shadow.base == nil || cfg.block == nil {
		return nil
	}
	if len(chains) == 0 || ready == nil {
		return nil
	}
	senderTasks := make([]discardShadowSenderChainTask, len(cfg.transactions))
	senderTaskOK := make([]bool, len(cfg.transactions))
	senderNext := make([]int, len(cfg.transactions))
	for txIndex := range senderNext {
		senderNext[txIndex] = -1
	}
	for _, chain := range chains {
		for taskIndex, task := range chain {
			senderTasks[task.txIndex] = task
			senderTaskOK[task.txIndex] = true
			if taskIndex+1 < len(chain) {
				senderNext[task.txIndex] = chain[taskIndex+1].txIndex
			}
		}
	}
	started := time.Now()
	workerCount := min(discardShadowWorkerCount, len(chains))
	workerStates := make([]*state.StateDB, 0, workerCount)
	workerStates = append(workerStates, shadow.base)
	for len(workerStates) < workerCount {
		workerState, err := shadow.base.Copy()
		if err != nil {
			break
		}
		workerState.SetDynamicProperties(shadow.base.DynamicProperties().Copy())
		workerStates = append(workerStates, workerState)
	}
	if len(workerStates) == 0 {
		return nil
	}
	var retryStates []*state.StateDB
	if retainRetryState {
		if !shadow.sampled {
			// Ordinary parallel blocks do not run the later discard-only
			// publisher, so every clean sender-chain worker can be transferred
			// directly to the incarnation scheduler without another StateDB copy.
			retryStates = append(retryStates, workerStates...)
		} else if len(workerStates) > 1 {
			// shadow.base remains owned by the later independent finish canary.
			// Every copied observer state is clean after workers.Wait and can
			// become an independently advanceable retry runner at no extra copy.
			retryStates = append(retryStates, workerStates[1:]...)
		} else if spare, err := shadow.base.Copy(); err == nil {
			spare.SetDynamicProperties(shadow.base.DynamicProperties().Copy())
			retryStates = append(retryStates, spare)
		}
	}
	if cfg.captureBalanceTrace {
		blockHash := cfg.block.Hash()
		for _, workerState := range workerStates {
			workerState.BeginBalanceTrace(int64(cfg.block.Number()), blockHash.Bytes(), cfg.block.Timestamp())
		}
		if shadow.sampled && len(workerStates) == 1 {
			for _, retryState := range retryStates {
				retryState.BeginBalanceTrace(int64(cfg.block.Number()), blockHash.Bytes(), cfg.block.Timestamp())
			}
		}
	}
	preCfg := cfg
	preCfg.canonicalInfos = nil
	preCfg.canonicalWriteSets = nil
	preCfg.retainInfos = true
	jobs := make(chan []discardShadowSenderChainTask)
	results := make(chan discardShadowTaskResult, len(cfg.transactions))
	var workers sync.WaitGroup
	for _, workerState := range workerStates {
		workers.Add(1)
		go func(workerState *state.StateDB) {
			defer workers.Done()
			worker := discardShadowWorker{
				state:     workerState,
				dynProps:  workerState.DynamicProperties(),
				db:        discardKVOverlay{parent: preCfg.db},
				forkCache: forks.NewVersionPassCache().BlockScope(),
			}
			worker.db.recorder = &worker.recorder
			for chain := range jobs {
				worker.db.resetForwarded()
				stateSnapshot := worker.state.Snapshot()
				dpSnapshot := worker.dynProps.Snapshot()
				var versions versionedAccessShadow
				versions.Prepare(len(preCfg.transactions))
				for _, task := range chain {
					result := worker.execute(task.txIndex, preCfg)
					result.senderPredecessor = task.senderPredecessor
					result.senderVersioned = task.senderVersioned
					annotateSenderChainReadVersions(&result.reads, &versions, task.txIndex)
					if ready(&result) {
						if err := worker.advanceSenderChainWrites(result.writes, forwardRaw); err != nil {
							result.err = err
						}
					}
					results <- result
					if !ready(&result) {
						break
					}
					installSenderChainWrites(&versions, result.writes, task.txIndex)
				}
				worker.state.RevertToSnapshot(stateSnapshot)
				worker.dynProps.RevertToSnapshot(dpSnapshot)
				worker.db.resetForwarded()
				worker.db.recorder = &worker.recorder
			}
		}(workerState)
	}
	go func() {
		for _, chain := range chains {
			jobs <- chain
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()
	retained := make([]discardShadowTaskResult, 0, len(cfg.transactions))
	for result := range results {
		retained = append(retained, result)
	}
	resultByTx := make([]int, len(cfg.transactions))
	for txIndex := range resultByTx {
		resultByTx[txIndex] = -1
	}
	for resultIndex, result := range retained {
		resultByTx[result.txIndex] = resultIndex
	}
	return &discardShadowPreexecution{
		results:       retained,
		resultByTx:    resultByTx,
		readVersions:  make([]discardShadowReadVersionResult, len(retained)),
		readValidated: make([]bool, len(retained)),
		published:     make([]bool, len(retained)),
		senderTasks:   senderTasks,
		senderTaskOK:  senderTaskOK,
		senderNext:    senderNext,
		groups:        len(chains),
		wallNanos:     time.Since(started).Nanoseconds(),
		retryStates:   retryStates,
	}
}

func newDiscardShadowSenderRetry(source *discardShadowPreexecution, transactionCount int) *discardShadowSenderRetry {
	if source == nil || transactionCount == 0 || len(source.senderNext) != transactionCount {
		return nil
	}
	hasSuffix := false
	for _, next := range source.senderNext {
		if next >= 0 {
			hasSuffix = true
			break
		}
	}
	if !hasSuffix {
		return nil
	}
	return &discardShadowSenderRetry{
		source:             source,
		results:            make([]discardShadowTaskResult, transactionCount),
		available:          make([]bool, transactionCount),
		selected:           make([]discardShadowTaskResult, transactionCount),
		selectedOK:         make([]bool, transactionCount),
		selectedPublished:  make([]bool, transactionCount),
		selectedAsyncReady: make([]bool, transactionCount),
		incarnations:       make([]uint32, transactionCount),
		runner:             newDiscardShadowRetryRunner(nil),
	}
}

func newDiscardShadowRetryRunner(retryState *state.StateDB) *discardShadowRetryRunner {
	runner := &discardShadowRetryRunner{settledThrough: -1}
	runner.prefixRaw.recorder = &runner.prefixRecorder
	if retryState != nil {
		runner.worker = &discardShadowWorker{
			state:     retryState,
			dynProps:  retryState.DynamicProperties(),
			db:        discardKVOverlay{parent: &runner.prefixRaw},
			forkCache: forks.NewVersionPassCache().BlockScope(),
		}
		runner.worker.db.recorder = &runner.worker.recorder
	}
	return runner
}

func newDiscardShadowSharedRetryRunner(blockBase *state.StateDB) *discardShadowRetryRunner {
	return &discardShadowRetryRunner{blockBase: blockBase, settledThrough: -1}
}

func newDiscardShadowAsyncSenderRetry(source *discardShadowPreexecution, transactionCount int) *discardShadowSenderRetry {
	retry := newDiscardShadowSenderRetry(source, transactionCount)
	if retry == nil {
		if source != nil {
			// Retained clean states have no consumer when the block contains no
			// sender suffix. Drop them before canonical execution so copied
			// workers can be reclaimed immediately.
			source.retryStates = nil
		}
		return nil
	}
	retry.async = true
	retry.asyncIncarnations = make([]atomic.Uint32, transactionCount)
	// The global execution budget bounds result events, while every dispatched
	// job adds one ownership-return event. A fully buffered channel prevents a
	// runner from being paced by canonical execution.
	retry.asyncEvents = make(chan discardShadowAsyncRetryEvent, int(discardShadowRetryMaxExecutions+discardShadowRetryMaxAttempts))
	for _, retryState := range source.retryStates {
		if retryState != nil {
			retry.asyncRunners = append(retry.asyncRunners, newDiscardShadowSharedRetryRunner(retryState))
		}
	}
	retry.stats.actualPrewarmed = int64(len(retry.asyncRunners))
	source.retryStates = nil
	if len(retry.asyncRunners) == 0 {
		// Preserve a safe lazy-copy fallback if observer copies were unavailable.
		// It is refreshed from the exact enqueue boundary and never reused as a
		// block-start template for a later boundary.
		retry.asyncRunners = append(retry.asyncRunners, newDiscardShadowSharedRetryRunner(nil))
	}
	retry.stats.actualCapacity = int64(len(retry.asyncRunners))
	retry.runner = nil
	return retry
}

func (retry *discardShadowSenderRetry) asyncTaskCurrent(task discardShadowAsyncRetryTask) bool {
	return retry != nil && task.txIndex >= 0 && task.txIndex < len(retry.asyncIncarnations) &&
		retry.asyncIncarnations[task.txIndex].Load() == task.incarnation
}

func (retry *discardShadowSenderRetry) idleAsyncRunner() *discardShadowRetryRunner {
	if retry == nil {
		return nil
	}
	for _, runner := range retry.asyncRunners {
		if runner != nil && !runner.busy {
			return runner
		}
	}
	return nil
}

// idleAsyncRunnerFor returns an idle immutable block-start template. Shared
// version floor reads make the template independent of the requested prefix;
// no private runner is advanced or rewound between incarnations.
func (retry *discardShadowSenderRetry) idleAsyncRunnerFor(_ int) *discardShadowRetryRunner {
	if retry == nil {
		return nil
	}
	for _, runner := range retry.asyncRunners {
		if runner == nil || runner.busy {
			continue
		}
		return runner
	}
	return nil
}

// snapshotDiscardShadowVersionView freezes only the version cells the source
// sender suffix actually read. Transfer branches are stable in the common
// case, so cloning the entire block version graph at every retry is wasted
// canonical-thread work. If a retry discovers a new key whose prefix writer
// was not captured, it carries no expected writer and the publication
// validator conservatively rejects it when that canonical writer is present.
func snapshotDiscardShadowVersionView(source *versionedAccessShadow, pre *discardShadowPreexecution, txIndex int) (versionedAccessShadow, int) {
	view := versionedAccessShadow{
		versions:             make(map[state.TransactionAccessKey]int, 16),
		accountFullVersions:  make(map[tcommon.Address]int, 4),
		accountAnyVersions:   make(map[tcommon.Address]int, 4),
		accountFieldVersions: make(map[state.TransactionAccountFieldKey]int, 8),
	}
	if source == nil || pre == nil {
		return view, 0
	}
	for current, traversed := txIndex, 0; current >= 0 && current < len(pre.senderNext) && traversed < len(pre.senderNext); traversed++ {
		if current < len(pre.resultByTx) {
			resultIndex := pre.resultByTx[current]
			if resultIndex >= 0 && resultIndex < len(pre.results) {
				for _, read := range pre.results[resultIndex].reads.Reads {
					if read.Mode&state.TransactionAccessRead == 0 {
						continue
					}
					switch read.Key.Kind {
					case state.TransactionAccessAccount:
						if version, ok := source.accountAnyVersions[read.Key.Address]; ok {
							view.accountAnyVersions[read.Key.Address] = version
						}
					case state.TransactionAccessAccountField:
						if version, ok := source.accountFullVersions[read.Key.Address]; ok {
							view.accountFullVersions[read.Key.Address] = version
						}
						fieldKey := state.TransactionAccountFieldKey{Address: read.Key.Address, Field: read.Key.AccountField}
						if version, ok := source.accountFieldVersions[fieldKey]; ok {
							view.accountFieldVersions[fieldKey] = version
						}
					default:
						if version, ok := source.versions[read.Key]; ok {
							view.versions[read.Key] = version
						}
					}
				}
			}
		}
		current = pre.senderNext[current]
	}
	cells := len(view.versions) + len(view.accountFullVersions) + len(view.accountAnyVersions) + len(view.accountFieldVersions)
	return view, cells
}

func (retry *discardShadowSenderRetry) freezeAsyncRawViewFrom(parent actuator.BufferedKVStore, txIndex int) (*discardShadowFrozenKV, int, error) {
	if retry == nil || retry.source == nil {
		return nil, 0, errors.New("missing async retry source")
	}
	keys := make(map[string]struct{}, 4)
	for current, traversed := txIndex, 0; current >= 0 && current < len(retry.source.senderNext) && traversed < len(retry.source.senderNext); traversed++ {
		if current < len(retry.source.resultByTx) {
			resultIndex := retry.source.resultByTx[current]
			if resultIndex >= 0 && resultIndex < len(retry.source.results) {
				for _, read := range retry.source.results[resultIndex].reads.Reads {
					if read.Key.Kind == state.TransactionAccessRawKV && read.Mode&state.TransactionAccessRead != 0 {
						keys[read.Key.LogicalKey] = struct{}{}
					}
				}
			}
		}
		current = retry.source.senderNext[current]
	}
	frozen := &discardShadowFrozenKV{
		values:  make(map[string][]byte, len(keys)),
		present: make(map[string]bool, len(keys)),
	}
	for key := range keys {
		if parent == nil {
			frozen.present[key] = false
			continue
		}
		exists, err := parent.Has([]byte(key))
		if err != nil {
			return nil, 0, err
		}
		frozen.present[key] = exists
		if !exists {
			continue
		}
		value, err := parent.Get([]byte(key))
		if err != nil {
			return nil, 0, err
		}
		frozen.values[key] = append([]byte(nil), value...)
	}
	return frozen, len(keys), nil
}

func (retry *discardShadowSenderRetry) freezeAsyncRawView(runner *discardShadowRetryRunner, txIndex int) (*discardShadowFrozenKV, int, error) {
	if runner == nil {
		return nil, 0, errors.New("missing async retry runner")
	}
	return retry.freezeAsyncRawViewFrom(&runner.prefixRaw, txIndex)
}

func annotateSenderRetryReadVersions(reads *state.TransactionReadSet, base, forwarded *versionedAccessShadow, txIndex int) {
	if reads == nil {
		return
	}
	for readIndex := range reads.Reads {
		read := &reads.Reads[readIndex]
		if read.Mode&state.TransactionAccessRead == 0 {
			continue
		}
		previous := -1
		if basePrevious, ok := base.typedPreviousVersion(read.Key, txIndex); ok {
			previous = basePrevious
		}
		if forwardedPrevious, ok := forwarded.typedPreviousVersion(read.Key, txIndex); ok && forwardedPrevious > previous {
			previous = forwardedPrevious
		}
		if previous >= 0 {
			read.ExpectedWriter = previous
			read.HasExpectedWriter = true
		}
	}
}

func senderRetryOwnerPredecessor(versioned *versionedAccessShadow, tx *types.Transaction) (int, bool) {
	if versioned == nil || tx == nil || tx.Contract() == nil {
		return 0, false
	}
	ownerBytes, shielded, err := tx.ContractOwnerAddress()
	if err != nil || shielded || len(ownerBytes) != tcommon.AddressLength {
		return 0, false
	}
	owner := tcommon.BytesToAddress(ownerBytes)
	if !owner.ValidPrefix() {
		return 0, false
	}
	previous, ok := versioned.lastSenderTx[owner]
	return previous, ok
}

// refreshSettledPrefix takes one exact copy of the live canonical prefix. It
// is used lazily for the first retry in a block and as a fallback when an
// intervening canonical WriteSet contains a family the narrow ordered applier
// cannot replay yet.
func (retry *discardShadowSenderRetry) refreshRunnerSettledPrefix(runner *discardShadowRetryRunner, target int, statedb *state.StateDB, dynProps *state.DynamicProperties, cfg discardShadowRunConfig) bool {
	if retry == nil || runner == nil || statedb == nil || dynProps == nil || target < -1 {
		return false
	}
	started := time.Now()
	prefixState, err := statedb.Copy()
	retry.stats.copyNanos += time.Since(started).Nanoseconds()
	retry.stats.prefixRefreshes++
	if err != nil {
		runner.worker = nil
		retry.stats.errors++
		return false
	}
	prefixState.SetDynamicProperties(dynProps.Copy())
	if cfg.captureBalanceTrace {
		prefixState.BeginBalanceTrace(int64(cfg.block.Number()), cfg.block.Hash().Bytes(), cfg.block.Timestamp())
	}
	runner.prefixRaw.reset()
	runner.prefixRaw.parent = cfg.db
	runner.prefixRaw.recorder = &runner.prefixRecorder
	runner.worker = &discardShadowWorker{
		state:     prefixState,
		dynProps:  prefixState.DynamicProperties(),
		db:        discardKVOverlay{parent: &runner.prefixRaw},
		forkCache: forks.NewVersionPassCache().BlockScope(),
	}
	runner.worker.db.recorder = &runner.worker.recorder
	runner.settledThrough = target
	return true
}

// ensureSettledPrefix advances the reusable retry state through the canonical
// WriteSets which settled since the previous incarnation. This mirrors
// Erigon's shared version map: a retry reads the newest validated prefix while
// avoiding another full StateDB copy. Unsupported prefix writes refresh from
// the live canonical view, preserving correctness while coverage expands.
func (retry *discardShadowSenderRetry) ensureRunnerSettledPrefix(runner *discardShadowRetryRunner, target int, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) bool {
	if retry == nil || runner == nil || statedb == nil || dynProps == nil || versioned == nil || target < -1 {
		return false
	}
	if runner.worker == nil {
		return retry.refreshRunnerSettledPrefix(runner, target, statedb, dynProps, cfg)
	}
	if runner.prefixRaw.parent == nil {
		runner.prefixRaw.parent = cfg.db
		runner.prefixRaw.recorder = &runner.prefixRecorder
	}
	if target < runner.settledThrough {
		retry.stats.errors++
		return false
	}
	started := time.Now()
	for txIndex := runner.settledThrough + 1; txIndex <= target; txIndex++ {
		if txIndex < 0 || txIndex >= len(versioned.transactionWritesOK) || !versioned.transactionWritesOK[txIndex] ||
			txIndex >= len(versioned.transactionWriteSets) {
			retry.stats.prefixAdvanceNanos += time.Since(started).Nanoseconds()
			return retry.refreshRunnerSettledPrefix(runner, target, statedb, dynProps, cfg)
		}
		writes := versioned.transactionWriteSets[txIndex]
		if len(writes) == 0 {
			runner.settledThrough = txIndex
			retry.stats.prefixAdvances++
			continue
		}
		runner.prefixRecorder.Reset(64)
		runner.prefixRaw.recorder = &runner.prefixRecorder
		if err := runner.worker.state.ApplyTransactionWriteSet(
			writes, runner.worker.dynProps, &runner.prefixRaw,
		); err != nil {
			retry.stats.prefixAdvanceNanos += time.Since(started).Nanoseconds()
			return retry.refreshRunnerSettledPrefix(runner, target, statedb, dynProps, cfg)
		}
		runner.worker.state.FinalizeTransaction()
		runner.settledThrough = txIndex
		retry.stats.prefixAdvances++
	}
	dynProps.CopyInto(runner.worker.dynProps)
	retry.stats.prefixAdvanceNanos += time.Since(started).Nanoseconds()
	retry.stats.prefixReuses++
	return true
}

func (retry *discardShadowSenderRetry) ensureSettledPrefix(target int, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) bool {
	if retry == nil {
		return false
	}
	return retry.ensureRunnerSettledPrefix(retry.runner, target, statedb, dynProps, versioned, cfg)
}

// retryFrom rebuilds the conflicted transaction and its remaining sender
// suffix from the reusable settled canonical prefix. The suffix is isolated by
// journal snapshots and wholly reverted after its newest incarnations have
// been retained.
func (retry *discardShadowSenderRetry) retryFrom(txIndex int, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	if retry == nil || retry.source == nil || statedb == nil || dynProps == nil || versioned == nil ||
		txIndex < 0 || txIndex >= len(retry.source.senderTaskOK) || !retry.source.senderTaskOK[txIndex] {
		return
	}
	retry.stats.attempts++
	// The new incarnation supersedes every retained result in its suffix. Clear
	// them before copying/executing so a failed retry cannot expose a stale
	// descendant from an older incarnation at a later canonical boundary.
	for current, traversed := txIndex, 0; current >= 0 && current < len(retry.available) && traversed < len(retry.available); traversed++ {
		retry.available[current] = false
		retry.selectedOK[current] = false
		retry.selectedAsyncReady[current] = false
		current = retry.source.senderNext[current]
	}
	if !retry.ensureSettledPrefix(txIndex-1, statedb, dynProps, versioned, cfg) {
		return
	}
	worker := retry.runner.worker
	prefixStateSnapshot := worker.state.Snapshot()
	prefixDPSnapshot := worker.dynProps.Snapshot()
	retryCfg := cfg
	retryCfg.canonicalInfos = nil
	retryCfg.canonicalWriteSets = nil
	retryCfg.retainInfos = true
	var forwarded versionedAccessShadow
	forwarded.Prepare(len(cfg.transactions))
	executionStarted := time.Now()
	current := txIndex
	first := true
	for current >= 0 && current < len(cfg.transactions) && retry.source.senderTaskOK[current] && retry.stats.executed < discardShadowRetryMaxExecutions {
		task := retry.source.senderTasks[current]
		retry.incarnations[current]++
		result := worker.execute(current, retryCfg)
		result.settledPrefix = txIndex - 1
		result.hasSettledPrefix = true
		result.incarnation = retry.incarnations[current]
		result.retryStartTx = txIndex
		if first {
			if previous, ok := senderRetryOwnerPredecessor(versioned, cfg.transactions[current]); ok {
				result.senderPredecessor = previous
				result.senderVersioned = true
			}
		} else {
			result.senderPredecessor = task.senderPredecessor
			result.senderVersioned = task.senderVersioned
		}
		annotateSenderRetryReadVersions(&result.reads, versioned, &forwarded, current)
		if preexecutedTransferReady(&result) {
			if advanceErr := worker.advanceSenderChain(result.writes); advanceErr != nil {
				result.err = advanceErr
			}
		}
		result.retryCompletionNanos = time.Since(executionStarted).Nanoseconds()
		ready := preexecutedTransferReady(&result)
		retry.results[current] = result
		retry.available[current] = ready
		retry.selectedOK[current] = false
		retry.stats.executed++
		if !ready {
			retry.stats.errors++
			break
		}
		installSenderChainWrites(&forwarded, result.writes, current)
		first = false
		current = retry.source.senderNext[current]
	}
	retry.stats.executionNanos += time.Since(executionStarted).Nanoseconds()
	worker.state.RevertToSnapshot(prefixStateSnapshot)
	worker.dynProps.RevertToSnapshot(prefixDPSnapshot)
	worker.db.resetForwarded()
	worker.db.recorder = &worker.recorder
}

func (retry *discardShadowSenderRetry) invalidateAsyncSuffix(txIndex int, taskLimit int64) ([]discardShadowAsyncRetryTask, int64) {
	if retry == nil || retry.source == nil || txIndex < 0 || txIndex >= len(retry.available) {
		return nil, 0
	}
	// Concurrent runners reserve from one block-wide execution budget before
	// launch. Counting only completed events would let overlapping jobs each
	// reserve the full budget and could also exceed the result-channel bound.
	remaining := discardShadowRetryMaxExecutions - retry.asyncScheduled
	if taskLimit >= 0 && taskLimit < remaining {
		remaining = taskLimit
	}
	capacity := 0
	if remaining > 0 {
		capacity = min(int(remaining), 8)
	}
	tasks := make([]discardShadowAsyncRetryTask, 0, capacity)
	var deferred int64
	for current, traversed := txIndex, 0; current >= 0 && current < len(retry.available) && traversed < len(retry.available); traversed++ {
		if current >= len(retry.source.senderTaskOK) || !retry.source.senderTaskOK[current] {
			break
		}
		retry.available[current] = false
		retry.selectedOK[current] = false
		retry.selectedAsyncReady[current] = false
		retry.incarnations[current]++
		if current < len(retry.asyncIncarnations) {
			retry.asyncIncarnations[current].Store(retry.incarnations[current])
		}
		if int64(len(tasks)) < remaining {
			task := retry.source.senderTasks[current]
			tasks = append(tasks, discardShadowAsyncRetryTask{
				txIndex:           current,
				incarnation:       retry.incarnations[current],
				senderPredecessor: task.senderPredecessor,
				senderVersioned:   task.senderVersioned,
			})
		} else {
			deferred++
		}
		current = retry.source.senderNext[current]
	}
	return tasks, deferred
}

// enqueueAsyncRetry freezes an incarnation at its original canonical boundary
// and inserts it into a block-scoped min-transaction heap. This mirrors
// Erigon's QueueWithRetry rule: earlier retries are dispatched before later
// work, and runner saturation delays a safe frozen request instead of dropping
// the opportunity outright.
func (retry *discardShadowSenderRetry) enqueueAsyncRetry(txIndex int, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	if retry == nil || !retry.async || statedb == nil || dynProps == nil || versioned == nil || retry.asyncEvents == nil {
		return
	}
	dispatchStarted := time.Now()
	retry.stats.attempts++
	tasks, deferred := retry.invalidateAsyncSuffix(txIndex, discardShadowRetryLookahead)
	retry.stats.actualDeferred += deferred
	if len(tasks) == 0 {
		retry.stats.budgetSkipped++
		return
	}
	retry.asyncScheduled += int64(len(tasks))
	rawFreezeStarted := time.Now()
	frozenRaw, frozenKeys, err := retry.freezeAsyncRawViewFrom(cfg.db, txIndex)
	retry.stats.actualRawFreezeNs += time.Since(rawFreezeStarted).Nanoseconds()
	if err != nil {
		retry.stats.errors++
		retry.stats.actualErrors++
		retry.stats.actualQueueDropped += int64(len(tasks))
		retry.asyncScheduled -= int64(len(tasks))
		return
	}
	retry.stats.actualRawKeys += int64(frozenKeys)
	versionStarted := time.Now()
	versionView, versionCells := snapshotDiscardShadowVersionView(versioned, retry.source, txIndex)
	retry.stats.actualVersionNs += time.Since(versionStarted).Nanoseconds()
	retry.stats.actualVersionCells += int64(versionCells)
	if previous, ok := senderRetryOwnerPredecessor(versioned, cfg.transactions[tasks[0].txIndex]); ok {
		tasks[0].senderPredecessor = previous
		tasks[0].senderVersioned = true
	}
	request := &discardShadowAsyncRetryRequest{
		txIndex:     txIndex,
		tasks:       tasks,
		frozenRaw:   frozenRaw,
		versionView: versionView,
		dynProps:    dynProps.Copy(),
		enqueuedAt:  time.Now(),
	}
	// If preexecution could not retain a block-start template, freeze one exact
	// StateDB boundary on the request. This rare fallback is safe even while its
	// only runner is busy because the queued request owns the copy.
	if retry.stats.actualPrewarmed == 0 {
		copyStarted := time.Now()
		request.blockBase, err = statedb.CopyBlockExecutionBase()
		retry.stats.actualCopyNs += time.Since(copyStarted).Nanoseconds()
		if err != nil {
			retry.stats.errors++
			retry.stats.actualErrors++
			retry.stats.actualQueueDropped += int64(len(tasks))
			retry.asyncScheduled -= int64(len(tasks))
			return
		}
		request.blockBase.SetDynamicProperties(dynProps.Copy())
	}
	if retry.idleAsyncRunnerFor(txIndex-1) == nil {
		retry.stats.actualQueueBusy++
	}
	heap.Push(&retry.asyncQueue, request)
	retry.stats.actualQueueEnqueued++
	if depth := int64(retry.asyncQueue.Len()); depth > retry.stats.actualQueueMaxDepth {
		retry.stats.actualQueueMaxDepth = depth
	}
	retry.stats.actualDispatchNs += time.Since(dispatchStarted).Nanoseconds()
	retry.dispatchAsyncRetryQueue(txIndex, versioned, cfg)
}

func (retry *discardShadowSenderRetry) dispatchAsyncRetryQueue(boundary int, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	if retry == nil || !retry.async || versioned == nil {
		return
	}
	dispatchStarted := time.Now()
	defer func() {
		retry.stats.actualDispatchNs += time.Since(dispatchStarted).Nanoseconds()
	}()
	for retry.asyncQueue.Len() > 0 {
		request := retry.asyncQueue[0]
		if request == nil || len(request.tasks) == 0 {
			heap.Pop(&retry.asyncQueue)
			continue
		}
		useful := false
		var superseded int64
		for _, task := range request.tasks {
			current := retry.asyncTaskCurrent(task)
			if !current {
				superseded++
			}
			if current && task.txIndex >= boundary {
				useful = true
			}
		}
		if !useful {
			heap.Pop(&retry.asyncQueue)
			dropped := int64(len(request.tasks))
			retry.stats.actualSuperseded += superseded
			retry.stats.actualQueueDropped += dropped
			retry.asyncScheduled -= dropped
			if retry.asyncScheduled < 0 {
				retry.asyncScheduled = 0
			}
			continue
		}
		runner := retry.idleAsyncRunnerFor(request.txIndex - 1)
		if runner == nil {
			return
		}
		heap.Pop(&retry.asyncQueue)
		retry.stats.actualQueueDequeued++
		retry.stats.actualQueueWaitNs += time.Since(request.enqueuedAt).Nanoseconds()
		retry.launchAsyncRetryRequest(runner, request, versioned, cfg)
	}
}

func (retry *discardShadowSenderRetry) launchAsyncRetryRequest(runner *discardShadowRetryRunner, request *discardShadowAsyncRetryRequest, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	runner.busy = true
	retry.asyncActive++
	if int64(retry.asyncActive) > retry.stats.actualMaxInflight {
		retry.stats.actualMaxInflight = int64(retry.asyncActive)
	}
	retry.stats.actualJobs++
	retryCfg := cfg
	retryCfg.db = nil
	retryCfg.canonicalInfos = nil
	retryCfg.canonicalWriteSets = nil
	retryCfg.retainInfos = true
	events := retry.asyncEvents
	go func() {
		copyStarted := time.Now()
		blockBase := runner.blockBase
		if request.blockBase != nil {
			blockBase = request.blockBase
		}
		if blockBase == nil {
			events <- discardShadowAsyncRetryEvent{
				runner: runner, done: true, dropped: int64(len(request.tasks)), prefixError: true,
				sharedState: true, sharedStateCopyNs: time.Since(copyStarted).Nanoseconds(),
			}
			return
		}
		jobState, copyErr := blockBase.CopyBlockExecutionBase()
		copyNanos := time.Since(copyStarted).Nanoseconds()
		if copyErr != nil {
			events <- discardShadowAsyncRetryEvent{
				runner: runner, done: true, dropped: int64(len(request.tasks)), prefixError: true,
				sharedState: true, sharedStateCopyNs: copyNanos,
			}
			return
		}
		jobState.SetDynamicProperties(request.dynProps.Copy())
		jobState.SetTransactionVersionedValueReader(versioned, request.txIndex)
		if retryCfg.captureBalanceTrace {
			jobState.BeginBalanceTrace(int64(retryCfg.block.Number()), retryCfg.block.Hash().Bytes(), retryCfg.block.Timestamp())
		}
		worker := discardShadowWorker{
			state:     jobState,
			dynProps:  jobState.DynamicProperties(),
			db:        discardKVOverlay{parent: request.frozenRaw},
			forkCache: forks.NewVersionPassCache().BlockScope(),
		}
		worker.db.recorder = &worker.recorder
		started := time.Now()
		var forwarded versionedAccessShadow
		forwarded.Prepare(len(retryCfg.transactions))
		var superseded int64
		var dropped int64
		for taskIndex, task := range request.tasks {
			if !retry.asyncTaskCurrent(task) {
				superseded += int64(len(request.tasks) - taskIndex)
				break
			}
			result := worker.execute(task.txIndex, retryCfg)
			result.settledPrefix = request.txIndex - 1
			result.hasSettledPrefix = true
			result.incarnation = task.incarnation
			result.retryStartTx = request.txIndex
			result.senderPredecessor = task.senderPredecessor
			result.senderVersioned = task.senderVersioned
			annotateSenderRetryReadVersions(&result.reads, &request.versionView, &forwarded, task.txIndex)
			if preexecutedTransferReady(&result) {
				if advanceErr := worker.advanceSenderChain(result.writes); advanceErr != nil {
					result.err = advanceErr
				}
			}
			result.retryCompletionNanos = time.Since(started).Nanoseconds()
			resultCopy := result
			events <- discardShadowAsyncRetryEvent{result: &resultCopy}
			if !preexecutedTransferReady(&result) {
				dropped += int64(len(request.tasks) - taskIndex - 1)
				break
			}
			if !retry.asyncTaskCurrent(task) {
				superseded += int64(len(request.tasks) - taskIndex - 1)
				break
			}
			installSenderChainWrites(&forwarded, result.writes, task.txIndex)
		}
		events <- discardShadowAsyncRetryEvent{
			runner: runner, done: true, nanos: time.Since(started).Nanoseconds(), rawMisses: request.frozenRaw.misses,
			superseded: superseded, dropped: dropped, sharedState: true, sharedStateCopyNs: copyNanos,
		}
	}()
}

func (retry *discardShadowSenderRetry) consumeAsyncEvent(event discardShadowAsyncRetryEvent, boundary int) {
	if retry == nil {
		return
	}
	if event.done {
		if event.sharedState {
			retry.stats.sharedStateJobs++
			retry.stats.sharedStateCopyNs += event.sharedStateCopyNs
			if event.prefixError {
				retry.stats.sharedStateErrors++
				retry.stats.errors++
				retry.stats.actualErrors++
			}
		} else {
			retry.stats.workerPrefix.jobs++
			retry.stats.workerPrefix.advances += event.prefixAdvances
			retry.stats.workerPrefix.nanos += event.prefixNanos
			retry.stats.actualAdvances += event.prefixAdvances
			retry.stats.actualAdvanceNs += event.prefixAdvanceNanos
			retry.stats.prefixAdvances += event.prefixAdvances
			retry.stats.prefixAdvanceNanos += event.prefixAdvanceNanos
			if event.prefixError {
				retry.stats.workerPrefix.errors++
				retry.stats.errors++
				retry.stats.actualErrors++
			} else {
				retry.stats.prefixReuses++
				retry.stats.actualReuses++
			}
		}
		if event.superseded > 0 {
			retry.stats.actualSuperseded += event.superseded
			retry.asyncScheduled -= event.superseded
			if retry.asyncScheduled < 0 {
				retry.asyncScheduled = 0
			}
		}
		if event.dropped > 0 {
			retry.stats.actualQueueDropped += event.dropped
			retry.asyncScheduled -= event.dropped
			if retry.asyncScheduled < 0 {
				retry.asyncScheduled = 0
			}
		}
		if event.runner != nil && event.runner.busy {
			event.runner.busy = false
			if retry.asyncActive > 0 {
				retry.asyncActive--
			}
		}
		retry.stats.actualExecutionNs += event.nanos
		retry.stats.executionNanos += event.nanos
		retry.stats.actualRawMisses += event.rawMisses
		return
	}
	if event.result == nil {
		return
	}
	result := *event.result
	retry.stats.executed++
	retry.stats.actualExecuted++
	ready := preexecutedTransferReady(&result)
	if !ready {
		retry.stats.errors++
		retry.stats.actualErrors++
	}
	if result.txIndex < 0 || result.txIndex >= len(retry.results) || result.incarnation != retry.incarnations[result.txIndex] {
		retry.stats.actualStale++
		return
	}
	if result.txIndex < boundary {
		retry.stats.actualLate++
		return
	}
	retry.results[result.txIndex] = result
	retry.available[result.txIndex] = ready
	retry.selectedOK[result.txIndex] = false
	retry.stats.actualReady++
}

func (retry *discardShadowSenderRetry) drainAsyncEvents(boundary int, wait bool) {
	if retry == nil || !retry.async {
		return
	}
	if wait {
		for retry.asyncQueue.Len() > 0 {
			request := heap.Pop(&retry.asyncQueue).(*discardShadowAsyncRetryRequest)
			if request == nil {
				continue
			}
			dropped := int64(len(request.tasks))
			retry.stats.actualQueueDropped += dropped
			retry.asyncScheduled -= dropped
		}
		if retry.asyncScheduled < 0 {
			retry.asyncScheduled = 0
		}
	}
	if wait && retry.asyncActive > 0 {
		started := time.Now()
		for retry.asyncActive > 0 {
			retry.consumeAsyncEvent(<-retry.asyncEvents, boundary)
		}
		retry.stats.actualFinishWaitNs += time.Since(started).Nanoseconds()
	}
	for {
		select {
		case event := <-retry.asyncEvents:
			retry.consumeAsyncEvent(event, boundary)
		default:
			return
		}
	}
}

func (retry *discardShadowSenderRetry) recordAsyncRetryRejection(decision discardShadowReadVersionResult) {
	if retry == nil || decision.publishable {
		return
	}
	retry.stats.actualRejected++
	if decision.readConflict {
		retry.stats.actualReadConflict++
	}
	if decision.sender {
		retry.stats.actualSender++
	}
	if decision.barrier {
		retry.stats.actualBarrier++
	}
	if decision.unsupported {
		retry.stats.actualUnsupported++
	}
	if decision.deltaInvalid {
		retry.stats.actualDeltaInvalid++
	}
}

// projectSenderRetryDeadline asks whether a background worker starting at the
// recorded conflict boundary would have completed this result before serial
// canonical execution reached its publication boundary. It uses measured
// worker completion time and measured canonical transaction durations, so the
// projection excludes the synchronous observer overhead itself.
func projectSenderRetryDeadline(result discardShadowTaskResult, boundary int, versioned *versionedAccessShadow) (ready, known bool, deadlineNanos, deltaNanos int64) {
	if versioned == nil || result.retryCompletionNanos <= 0 || result.retryStartTx < 0 ||
		boundary < result.retryStartTx || boundary > len(versioned.transactionDurations) {
		return false, false, 0, 0
	}
	for txIndex := result.retryStartTx; txIndex < boundary; txIndex++ {
		duration := versioned.transactionDurations[txIndex]
		if duration <= 0 {
			return false, false, deadlineNanos, 0
		}
		deadlineNanos += duration
	}
	if result.retryCompletionNanos <= deadlineNanos {
		return true, true, deadlineNanos, deadlineNanos - result.retryCompletionNanos
	}
	return false, true, deadlineNanos, result.retryCompletionNanos - deadlineNanos
}

// observeBoundary validates the newest incarnation against exactly the
// canonical prefix preceding txIndex. When the existing incarnation is stale
// and has dependent sender work, it immediately builds a new sampled suffix.
func (retry *discardShadowSenderRetry) observeBoundary(txIndex int, tx *types.Transaction, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	if retry == nil || txIndex < 0 || txIndex >= len(retry.results) {
		return
	}
	if retry.async {
		retry.observeAsyncBoundary(txIndex, tx, statedb, dynProps, versioned, cfg)
		return
	}
	_, sourceDecision, sourceAvailable := retry.source.resultForTransaction(txIndex)
	decision := discardShadowReadVersionResult{}
	resultAvailable := false
	if retry.available[txIndex] && retry.results[txIndex].incarnation == retry.incarnations[txIndex] {
		decision = versioned.validateBlockStartReadSet(txIndex, tx, retry.results[txIndex])
		resultAvailable = true
	}
	newestPublishable := sourceAvailable && sourceDecision.publishable
	if resultAvailable {
		newestPublishable = decision.publishable
	}
	if !newestPublishable && txIndex < len(retry.source.senderNext) && retry.source.senderNext[txIndex] >= 0 {
		if retry.stats.attempts >= discardShadowRetryMaxAttempts || retry.stats.executed >= discardShadowRetryMaxExecutions {
			retry.stats.budgetSkipped++
			return
		}
		retry.retryFrom(txIndex, statedb, dynProps, versioned, cfg)
		if retry.available[txIndex] {
			decision = versioned.validateBlockStartReadSet(txIndex, tx, retry.results[txIndex])
			resultAvailable = true
		}
	}
	if !resultAvailable || !decision.publishable {
		return
	}
	retry.selected[txIndex] = retry.results[txIndex]
	retry.selectedOK[txIndex] = true
	ready, known, _, deltaNanos := projectSenderRetryDeadline(retry.results[txIndex], txIndex, versioned)
	retry.stats.asyncCandidates++
	switch {
	case !known:
		retry.stats.asyncUnknown++
	case ready:
		retry.selectedAsyncReady[txIndex] = true
		retry.stats.asyncReady++
		retry.stats.asyncReadySlackNs += deltaNanos
	default:
		retry.stats.asyncLate++
		retry.stats.asyncLateNs += deltaNanos
	}
	retry.stats.candidates++
}

func (retry *discardShadowSenderRetry) observeAsyncBoundary(txIndex int, tx *types.Transaction, statedb *state.StateDB, dynProps *state.DynamicProperties, versioned *versionedAccessShadow, cfg discardShadowRunConfig) {
	retry.drainAsyncEvents(txIndex, false)
	retry.dispatchAsyncRetryQueue(txIndex, versioned, cfg)
	_, sourceDecision, sourceAvailable := retry.source.resultForTransaction(txIndex)
	decision := discardShadowReadVersionResult{}
	resultAvailable := retry.available[txIndex] && retry.results[txIndex].incarnation == retry.incarnations[txIndex]
	if resultAvailable {
		decision = versioned.validateBlockStartReadSet(txIndex, tx, retry.results[txIndex])
	}
	newestPublishable := sourceAvailable && sourceDecision.publishable
	if resultAvailable {
		newestPublishable = decision.publishable
	}
	if retry.asyncActive > 0 && (!newestPublishable || retry.publish && !resultAvailable) {
		// Give a result that completed while the source version was checked one
		// final chance at the boundary before declaring the worker busy/late.
		retry.drainAsyncEvents(txIndex, false)
		retry.dispatchAsyncRetryQueue(txIndex, versioned, cfg)
		resultAvailable = retry.available[txIndex] && retry.results[txIndex].incarnation == retry.incarnations[txIndex]
		if resultAvailable {
			decision = versioned.validateBlockStartReadSet(txIndex, tx, retry.results[txIndex])
			newestPublishable = decision.publishable
		}
	}
	if resultAvailable && !decision.publishable {
		retry.recordAsyncRetryRejection(decision)
	}
	if !newestPublishable && txIndex < len(retry.source.senderNext) && retry.source.senderNext[txIndex] >= 0 {
		if retry.stats.attempts >= discardShadowRetryMaxAttempts || retry.asyncScheduled >= discardShadowRetryMaxExecutions {
			retry.stats.budgetSkipped++
			_, _ = retry.invalidateAsyncSuffix(txIndex, 0)
			return
		}
		retry.enqueueAsyncRetry(txIndex, statedb, dynProps, versioned, cfg)
		return
	}
	if !resultAvailable || !decision.publishable {
		return
	}
	retry.selected[txIndex] = retry.results[txIndex]
	retry.selectedOK[txIndex] = true
	retry.selectedAsyncReady[txIndex] = true
	retry.stats.candidates++
	retry.stats.actualCandidates++
}

func (retry *discardShadowSenderRetry) selectedResultForPublication(txIndex int) (*discardShadowTaskResult, bool) {
	if retry == nil || !retry.async || !retry.publish || txIndex < 0 || txIndex >= len(retry.selected) ||
		txIndex >= len(retry.selectedOK) || txIndex >= len(retry.selectedPublished) ||
		!retry.selectedOK[txIndex] || retry.selectedPublished[txIndex] {
		return nil, false
	}
	result := &retry.selected[txIndex]
	return result, preexecutedTransferReady(result)
}

func (retry *discardShadowSenderRetry) markPublished(txIndex int) {
	if retry == nil || txIndex < 0 || txIndex >= len(retry.selectedPublished) || retry.selectedPublished[txIndex] {
		return
	}
	retry.selectedPublished[txIndex] = true
	retry.stats.publish.published++
}

func (retry *discardShadowSenderRetry) finish(versioned *versionedAccessShadow, cfg discardShadowRunConfig) discardShadowSenderRetryStats {
	if retry == nil || versioned == nil || len(cfg.canonicalInfos) != len(cfg.transactions) {
		return discardShadowSenderRetryStats{}
	}
	if retry.async {
		retry.drainAsyncEvents(len(cfg.transactions), true)
	}
	for txIndex, selected := range retry.selected {
		published := txIndex < len(retry.selectedPublished) && retry.selectedPublished[txIndex]
		if !retry.selectedOK[txIndex] && !published {
			continue
		}
		if !preexecutedTransferReady(&selected) || cfg.canonicalInfos[txIndex] == nil ||
			txIndex >= len(versioned.transactionWritesOK) || !versioned.transactionWritesOK[txIndex] ||
			txIndex >= len(versioned.transactionWriteSets) || (cfg.captureBalanceTrace && txIndex >= len(cfg.canonicalBalanceTraces)) {
			retry.stats.errors++
			if retry.async {
				retry.stats.actualErrors++
			}
			continue
		}
		infoMatch := compareDiscardShadowInfo(selected.info, cfg.canonicalInfos[txIndex]) == 0
		writeMatch := equalSenderChainWriteSets(selected.writes, versioned.transactionWriteSets[txIndex], selected.publicNetValid)
		balanceMatch := !cfg.captureBalanceTrace || proto.Equal(selected.balanceTrace, cfg.canonicalBalanceTraces[txIndex])
		if published {
			if writeMatch {
				retry.stats.publish.writeMatches++
			} else {
				retry.stats.publish.writeMismatches++
				retry.stats.writeMismatches++
				retry.stats.errors++
				if retry.async {
					retry.stats.actualErrors++
				}
			}
			// TransactionInfo and BalanceTrace are the published carriers, so
			// they are not independent serial-equivalence evidence. Keep the
			// two observer cohorts responsible for those validation counters.
			continue
		}
		if !infoMatch {
			retry.stats.infoMismatches++
		}
		if !writeMatch {
			retry.stats.writeMismatches++
		}
		if !balanceMatch {
			retry.stats.balanceMismatches++
		}
		if infoMatch && writeMatch && balanceMatch {
			retry.stats.validated++
			sourceAccepted := false
			if txIndex < len(retry.source.resultByTx) {
				resultIndex := retry.source.resultByTx[txIndex]
				sourceAccepted = resultIndex >= 0 && resultIndex < len(retry.source.published) && retry.source.published[resultIndex]
			}
			if !sourceAccepted {
				retry.stats.recovered++
			}
			if retry.async {
				retry.stats.actualValidated++
				if !sourceAccepted {
					retry.stats.actualRecovered++
				}
			} else if txIndex < len(retry.selectedAsyncReady) && retry.selectedAsyncReady[txIndex] {
				retry.stats.asyncValidated++
				if !sourceAccepted {
					retry.stats.asyncRecovered++
				}
			}
		}
	}
	discardShadowRetryBlocksCounter.Inc(1)
	discardShadowRetryAttemptsCounter.Inc(retry.stats.attempts)
	discardShadowRetryExecutedCounter.Inc(retry.stats.executed)
	discardShadowRetryCandidatesCounter.Inc(retry.stats.candidates)
	discardShadowRetryRecoveredCounter.Inc(retry.stats.recovered)
	discardShadowRetryValidatedCounter.Inc(retry.stats.validated)
	discardShadowRetryInfoMismatchCounter.Inc(retry.stats.infoMismatches)
	discardShadowRetryWriteMismatchCounter.Inc(retry.stats.writeMismatches)
	discardShadowRetryBalanceMismatchCounter.Inc(retry.stats.balanceMismatches)
	discardShadowRetryErrorsCounter.Inc(retry.stats.errors)
	discardShadowRetryBudgetSkippedCounter.Inc(retry.stats.budgetSkipped)
	discardShadowRetryCopyNanosCounter.Inc(retry.stats.copyNanos)
	discardShadowRetryPrefixRefreshCounter.Inc(retry.stats.prefixRefreshes)
	discardShadowRetryPrefixReuseCounter.Inc(retry.stats.prefixReuses)
	discardShadowRetryPrefixAdvanceCounter.Inc(retry.stats.prefixAdvances)
	discardShadowRetryPrefixAdvanceNanosCounter.Inc(retry.stats.prefixAdvanceNanos)
	discardShadowRetryExecutionNanosCounter.Inc(retry.stats.executionNanos)
	discardShadowRetryAsyncCandidatesCounter.Inc(retry.stats.asyncCandidates)
	discardShadowRetryAsyncReadyCounter.Inc(retry.stats.asyncReady)
	discardShadowRetryAsyncLateCounter.Inc(retry.stats.asyncLate)
	discardShadowRetryAsyncUnknownCounter.Inc(retry.stats.asyncUnknown)
	discardShadowRetryAsyncValidatedCounter.Inc(retry.stats.asyncValidated)
	discardShadowRetryAsyncRecoveredCounter.Inc(retry.stats.asyncRecovered)
	discardShadowRetryAsyncReadySlackNanosCounter.Inc(retry.stats.asyncReadySlackNs)
	discardShadowRetryAsyncLateNanosCounter.Inc(retry.stats.asyncLateNs)
	if retry.async {
		discardShadowRetryActualBlocksCounter.Inc(1)
		discardShadowRetryActualJobsCounter.Inc(retry.stats.actualJobs)
		discardShadowRetryActualBusySkippedCounter.Inc(retry.stats.actualBusySkipped)
		discardShadowRetryActualRunnerCapacityCounter.Inc(retry.stats.actualCapacity)
		discardShadowRetryActualMaxInflightCounter.Inc(retry.stats.actualMaxInflight)
		discardShadowRetryActualDeferredCounter.Inc(retry.stats.actualDeferred)
		discardShadowRetryActualSupersededCounter.Inc(retry.stats.actualSuperseded)
		discardShadowRetryActualQueueEnqueuedCounter.Inc(retry.stats.actualQueueEnqueued)
		discardShadowRetryActualQueueDequeuedCounter.Inc(retry.stats.actualQueueDequeued)
		discardShadowRetryActualQueueBusyCounter.Inc(retry.stats.actualQueueBusy)
		discardShadowRetryActualQueueDroppedCounter.Inc(retry.stats.actualQueueDropped)
		discardShadowRetryActualQueueMaxDepthCounter.Inc(retry.stats.actualQueueMaxDepth)
		discardShadowRetryActualQueueWaitNanosCounter.Inc(retry.stats.actualQueueWaitNs)
		discardShadowRetryActualWorkerPrefixJobsCounter.Inc(retry.stats.workerPrefix.jobs)
		discardShadowRetryActualWorkerPrefixAdvanceCounter.Inc(retry.stats.workerPrefix.advances)
		discardShadowRetryActualWorkerPrefixNanosCounter.Inc(retry.stats.workerPrefix.nanos)
		discardShadowRetryActualWorkerPrefixErrorsCounter.Inc(retry.stats.workerPrefix.errors)
		discardShadowRetryActualPublishedCounter.Inc(retry.stats.publish.published)
		discardShadowRetryActualPublishedWriteOKCounter.Inc(retry.stats.publish.writeMatches)
		discardShadowRetryActualPublishedWriteMismatchCounter.Inc(retry.stats.publish.writeMismatches)
		discardShadowRetryActualExecutedCounter.Inc(retry.stats.actualExecuted)
		discardShadowRetryActualReadyCounter.Inc(retry.stats.actualReady)
		discardShadowRetryActualLateCounter.Inc(retry.stats.actualLate)
		discardShadowRetryActualStaleCounter.Inc(retry.stats.actualStale)
		discardShadowRetryActualCandidatesCounter.Inc(retry.stats.actualCandidates)
		discardShadowRetryActualRejectedCounter.Inc(retry.stats.actualRejected)
		discardShadowRetryActualReadConflictCounter.Inc(retry.stats.actualReadConflict)
		discardShadowRetryActualSenderConflictCounter.Inc(retry.stats.actualSender)
		discardShadowRetryActualBarrierCounter.Inc(retry.stats.actualBarrier)
		discardShadowRetryActualUnsupportedCounter.Inc(retry.stats.actualUnsupported)
		discardShadowRetryActualDeltaInvalidCounter.Inc(retry.stats.actualDeltaInvalid)
		discardShadowRetryActualValidatedCounter.Inc(retry.stats.actualValidated)
		discardShadowRetryActualRecoveredCounter.Inc(retry.stats.actualRecovered)
		discardShadowRetryActualErrorsCounter.Inc(retry.stats.actualErrors)
		discardShadowRetryActualRawKeysCounter.Inc(retry.stats.actualRawKeys)
		discardShadowRetryActualRawMissCounter.Inc(retry.stats.actualRawMisses)
		discardShadowRetryActualVersionCellsCounter.Inc(retry.stats.actualVersionCells)
		discardShadowRetryActualDispatchNanosCounter.Inc(retry.stats.actualDispatchNs)
		discardShadowRetryActualPrefixNanosCounter.Inc(retry.stats.actualPrefixNs)
		discardShadowRetryActualPrefixRefreshCounter.Inc(retry.stats.actualRefreshes)
		discardShadowRetryActualPrefixReuseCounter.Inc(retry.stats.actualReuses)
		discardShadowRetryActualPrefixAdvanceCounter.Inc(retry.stats.actualAdvances)
		discardShadowRetryActualPrefixCopyNanosCounter.Inc(retry.stats.actualCopyNs)
		discardShadowRetryActualPrefixAdvanceNsCounter.Inc(retry.stats.actualAdvanceNs)
		discardShadowRetryActualRawFreezeNanosCounter.Inc(retry.stats.actualRawFreezeNs)
		discardShadowRetryActualVersionNanosCounter.Inc(retry.stats.actualVersionNs)
		discardShadowRetryActualPrewarmedCounter.Inc(retry.stats.actualPrewarmed)
		discardShadowRetryActualExecutionNanosCounter.Inc(retry.stats.actualExecutionNs)
		discardShadowRetryActualFinishWaitNanosCounter.Inc(retry.stats.actualFinishWaitNs)
		discardShadowRetrySharedStateJobsCounter.Inc(retry.stats.sharedStateJobs)
		discardShadowRetrySharedStateCopyNanosCounter.Inc(retry.stats.sharedStateCopyNs)
		discardShadowRetrySharedStateErrorsCounter.Inc(retry.stats.sharedStateErrors)
	}
	return retry.stats
}

// validateBlockStartReadSet applies Erigon's read-version rule to one retained
// worker result immediately before canonical execution reaches that index.
// The version maps therefore contain exactly the preceding transactions; a
// later writer cannot hide an earlier conflict by replacing the map entry.
// A block-start read expects no earlier writer. A forwarded sender-chain read
// instead names the exact earlier transaction whose value it consumed; any
// intervening writer invalidates it. Audited commutative reads remain valid
// only when the worker returned an ordered delta for the same path.
func (versioned *versionedAccessShadow) validateBlockStartReadSet(txIndex int, tx *types.Transaction, result discardShadowTaskResult) discardShadowReadVersionResult {
	decision := discardShadowReadVersionResult{unsupported: result.reads.Unsupported}
	if versioned == nil || txIndex < 0 || txIndex >= len(versioned.transactionSupported) {
		decision.unsupported = true
		return decision
	}
	// A retry executed from the immediately preceding settled prefix cannot
	// have an intervening mutation, so even an unknown/range read is current.
	// Suffix results still require exact-key coverage because canonical work may
	// run between the retry snapshot and their publication boundary.
	if result.hasSettledPrefix && result.settledPrefix == txIndex-1 {
		decision.unsupported = false
	}
	for _, read := range result.reads.Reads {
		// public_net_usage/public_net_time form one conditional reservation,
		// not two unconditional block-start reads. The ordered publisher repeats
		// java-tron's recovery and limit check against all predecessor effects.
		if result.publicNetValid && isPublicNetReservationKey(read.Key) {
			continue
		}
		if read.Mode&state.TransactionAccessRead != 0 {
			previous, exists := versioned.typedPreviousVersion(read.Key, txIndex)
			if read.HasExpectedWriter {
				if !exists || previous != read.ExpectedWriter {
					decision.readConflict = true
				}
			} else if exists {
				decision.readConflict = true
			}
		}
		if read.Mode&state.TransactionAccessCommutativeRead != 0 {
			value, ok := result.writes[read.Key]
			if !ok || !value.Commutative {
				decision.deltaInvalid = true
			}
		}
	}
	decision.barrier = versioned.lastBarrierTx >= 0 &&
		(!result.hasSettledPrefix || versioned.lastBarrierTx > result.settledPrefix)
	if tx != nil && tx.Contract() != nil {
		ownerBytes, shielded, err := tx.ContractOwnerAddress()
		if err == nil && !shielded && len(ownerBytes) == tcommon.AddressLength {
			owner := tcommon.BytesToAddress(ownerBytes)
			if owner.ValidPrefix() {
				previous, exists := versioned.lastSenderTx[owner]
				if result.senderVersioned {
					decision.sender = !exists || previous != result.senderPredecessor
				} else {
					decision.sender = exists
				}
			}
		}
	}
	decision.publishable = !decision.unsupported && !decision.readConflict && !decision.deltaInvalid && !decision.sender && !decision.barrier
	return decision
}

func isPublicNetReservationKey(key state.TransactionAccessKey) bool {
	return key.Kind == state.TransactionAccessDynamicInt &&
		(key.LogicalKey == "public_net_usage" || key.LogicalKey == "public_net_time")
}

func publicNetReservationWriteValue(writes state.TransactionWriteSet, logicalKey string) (int64, bool) {
	value, ok := writes[state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: logicalKey}]
	if !ok || !value.Exists || value.Commutative || len(value.Value) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(value.Value)), true
}

func validatePublicNetReservation(reservation state.PublicNetReservation, writes state.TransactionWriteSet, dynProps *state.DynamicProperties) bool {
	if dynProps == nil || reservation.Delta <= 0 || reservation.RecoveredUsage < 0 ||
		reservation.Limit != dynProps.PublicNetLimit() ||
		reservation.RecoveredUsage != recoverUsageForDP(reservation.StartUsage, reservation.StartTime, reservation.ResourceTime, dynProps) ||
		reservation.RecoveredUsage > reservation.Limit || reservation.Delta > reservation.Limit-reservation.RecoveredUsage {
		return false
	}
	usage, usageOK := publicNetReservationWriteValue(writes, "public_net_usage")
	resourceTime, timeWritten := publicNetReservationWriteValue(writes, "public_net_time")
	timeOK := timeWritten && resourceTime == reservation.ResourceTime
	if !timeWritten {
		// DynamicProperties.Set intentionally emits no write when the recovered
		// timestamp already equals this block's resource time.
		timeOK = reservation.StartTime == reservation.ResourceTime
	}
	return usageOK && timeOK && usage == reservation.RecoveredUsage+reservation.Delta
}

type publicNetWriteOverride struct {
	writes      state.TransactionWriteSet
	oldUsage    int64
	oldTime     int64
	timeWritten bool
	reservation bool
	rebased     bool
}

// overridePublicNetReservation repeats java-tron's conditional admission at
// the original transaction position, then temporarily changes the retained
// block-start post-images to the ordered post-images consumed by the generic
// typed publisher. The retained worker result is restored after publication
// so sampled parity diagnostics still see its original output.
func overridePublicNetReservation(result *discardShadowTaskResult, dynProps *state.DynamicProperties) (publicNetWriteOverride, bool) {
	if result == nil || !result.publicNetValid {
		return publicNetWriteOverride{}, true
	}
	reservation := result.publicNet
	publicLimit := dynProps.PublicNetLimit()
	currentUsage := dynProps.PublicNetUsage()
	currentTime := dynProps.PublicNetTime()
	recoveredUsage := recoverUsageForDP(currentUsage, currentTime, reservation.ResourceTime, dynProps)
	if publicLimit != reservation.Limit || recoveredUsage > publicLimit || reservation.Delta > publicLimit-recoveredUsage {
		return publicNetWriteOverride{reservation: true}, false
	}
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	usageValue := result.writes[usageKey]
	timeValue, timeWritten := result.writes[timeKey]
	override := publicNetWriteOverride{
		writes:      result.writes,
		oldUsage:    int64(binary.BigEndian.Uint64(usageValue.Value)),
		timeWritten: timeWritten,
		reservation: true,
		rebased:     currentUsage != reservation.StartUsage || currentTime != reservation.StartTime,
	}
	if timeWritten {
		override.oldTime = int64(binary.BigEndian.Uint64(timeValue.Value))
	}
	binary.BigEndian.PutUint64(usageValue.Value, uint64(recoveredUsage+reservation.Delta))
	if timeWritten {
		binary.BigEndian.PutUint64(timeValue.Value, uint64(reservation.ResourceTime))
	}
	return override, true
}

func (override publicNetWriteOverride) restore() {
	if !override.reservation || override.writes == nil {
		return
	}
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	usageValue := override.writes[usageKey]
	binary.BigEndian.PutUint64(usageValue.Value, uint64(override.oldUsage))
	if override.timeWritten {
		timeValue := override.writes[timeKey]
		binary.BigEndian.PutUint64(timeValue.Value, uint64(override.oldTime))
	}
}

func (pre *discardShadowPreexecution) validateReadVersion(txIndex int, tx *types.Transaction, versioned *versionedAccessShadow) {
	if pre == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex < 0 || resultIndex >= len(pre.results) {
		return
	}
	pre.readVersions[resultIndex] = versioned.validateBlockStartReadSet(txIndex, tx, pre.results[resultIndex])
	pre.readValidated[resultIndex] = true
}

// projectPublicNetBoundary evaluates a retained VM reservation against the
// exact canonical DynamicProperties visible immediately before its serial
// transaction. It records expected serial write values without mutating the
// worker result or canonical state.
func (pre *discardShadowPreexecution) projectPublicNetBoundary(txIndex int, dynProps *state.DynamicProperties) {
	if pre == nil || dynProps == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex < 0 || resultIndex >= len(pre.results) || resultIndex >= len(pre.readValidated) ||
		!pre.readValidated[resultIndex] || !pre.readVersions[resultIndex].publishable ||
		resultIndex >= len(pre.publicNet) {
		return
	}
	result := &pre.results[resultIndex]
	if !preexecutedResultReady(result) || !result.publicNetValid {
		return
	}
	reservation := result.publicNet
	projection := &pre.publicNet[resultIndex]
	projection.observed = true
	currentUsage := dynProps.PublicNetUsage()
	currentTime := dynProps.PublicNetTime()
	_, currentTimeStored := dynProps.Get("public_net_time")
	publicLimit := dynProps.PublicNetLimit()
	recoveredUsage := recoverUsageForDP(currentUsage, currentTime, reservation.ResourceTime, dynProps)
	if publicLimit != reservation.Limit || recoveredUsage < 0 || recoveredUsage > publicLimit ||
		reservation.Delta <= 0 || reservation.Delta > publicLimit-recoveredUsage {
		return
	}
	projection.admitted = true
	projection.rebased = currentUsage != reservation.StartUsage || currentTime != reservation.StartTime
	projection.expectedUsage = recoveredUsage + reservation.Delta
	projection.expectedTime = reservation.ResourceTime
	projection.expectedTimeSet = !currentTimeStored || currentTime != reservation.ResourceTime
}

// projectBlockEnergyBoundary derives the block-level adaptive-energy post-image
// from a retained VM receipt and the exact canonical pre-transaction value.
// It is observe-only; canonical accumulation still runs through
// accumulateBlockEnergyUsage after serial execution.
func (pre *discardShadowPreexecution) projectBlockEnergyBoundary(txIndex int, dynProps *state.DynamicProperties, forkStats forks.ForkStatsReader, prevBlockTime int64, forkPassCache *forks.VersionPassCache) {
	if pre == nil || dynProps == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex < 0 || resultIndex >= len(pre.results) || resultIndex >= len(pre.readValidated) ||
		!pre.readValidated[resultIndex] || !pre.readVersions[resultIndex].publishable ||
		resultIndex >= len(pre.blockEnergy) {
		return
	}
	result := &pre.results[resultIndex]
	if !preexecutedResultReady(result) || result.info.GetReceipt() == nil {
		return
	}
	receipt := result.info.GetReceipt()
	delta := blockEnergyUsageDelta(dynProps, forkStats, prevBlockTime, receipt.GetEnergyUsageTotal(), receipt.GetEnergyUsage(), receipt.GetOriginEnergyUsage(), forkPassCache)
	pre.blockEnergy[resultIndex] = discardShadowBlockEnergyProjection{
		observed: true,
		expected: dynProps.BlockEnergyUsage() + delta,
	}
}

// validateBlockEnergyBoundary checks the canonical post-image immediately
// after the serial transaction's block-energy accumulation, before the next
// transaction can change the accumulator.
func (pre *discardShadowPreexecution) validateBlockEnergyBoundary(txIndex int, dynProps *state.DynamicProperties) {
	if pre == nil || dynProps == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex < 0 || resultIndex >= len(pre.blockEnergy) {
		return
	}
	projection := &pre.blockEnergy[resultIndex]
	if !projection.observed || projection.validated {
		return
	}
	projection.validated = true
	projection.match = dynProps.BlockEnergyUsage() == projection.expected
}

func (pre *discardShadowPreexecution) resultForTransaction(txIndex int) (*discardShadowTaskResult, discardShadowReadVersionResult, bool) {
	if pre == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return nil, discardShadowReadVersionResult{}, false
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex < 0 || resultIndex >= len(pre.results) || resultIndex >= len(pre.readValidated) || !pre.readValidated[resultIndex] {
		return nil, discardShadowReadVersionResult{}, false
	}
	result := &pre.results[resultIndex]
	decision := pre.readVersions[resultIndex]
	if result.senderVersioned {
		predecessorResult := -1
		if result.senderPredecessor >= 0 && result.senderPredecessor < len(pre.resultByTx) {
			predecessorResult = pre.resultByTx[result.senderPredecessor]
		}
		if predecessorResult < 0 || predecessorResult >= len(pre.published) || !pre.published[predecessorResult] {
			decision.sender = true
			decision.predecessor = true
			decision.publishable = false
		}
	}
	return result, decision, true
}

func (pre *discardShadowPreexecution) markPublished(txIndex int) {
	if pre == nil || txIndex < 0 || txIndex >= len(pre.resultByTx) {
		return
	}
	resultIndex := pre.resultByTx[txIndex]
	if resultIndex >= 0 && resultIndex < len(pre.published) {
		pre.published[resultIndex] = true
	}
}

func preexecutedResultReady(result *discardShadowTaskResult) bool {
	if result == nil || result.err != nil || result.info == nil || result.writeSetErr != nil ||
		!result.applyEligible || result.applyErr != nil || !result.applyMatch {
		return false
	}
	return true
}

func preexecutedTransferReady(result *discardShadowTaskResult) bool {
	if !preexecutedResultReady(result) {
		return false
	}
	receipt := result.info.GetReceipt()
	return receipt == nil || (receipt.GetEnergyUsage() == 0 && receipt.GetEnergyFee() == 0 &&
		receipt.GetOriginEnergyUsage() == 0 && receipt.GetEnergyUsageTotal() == 0 && receipt.GetEnergyPenaltyTotal() == 0)
}

func transactionWriteSetChangesDynamic(writes state.TransactionWriteSet) bool {
	for key := range writes {
		switch key.Kind {
		case state.TransactionAccessDynamicInt, state.TransactionAccessDynamicString, state.TransactionAccessDynamicHash:
			return true
		}
	}
	return false
}

// finishTransferPreexecution admits only zero-indegree results: their exact
// canonical read versions remained at block start. It then compares the full
// TransactionInfo, typed/raw WriteSet, and isolated applier result. Canonical
// serial state is never modified by this observer.
func (shadow *discardShadowBlock) finishTransferPreexecution(pre *discardShadowPreexecution, versioned *versionedAccessShadow, cfg discardShadowRunConfig) discardShadowPreexecutionStats {
	var stats discardShadowPreexecutionStats
	if shadow == nil || pre == nil || versioned == nil || len(cfg.canonicalInfos) != len(cfg.transactions) {
		return stats
	}
	stats.transfers = int64(len(pre.results))
	stats.executed = int64(len(pre.results))
	validatedResults := make([]discardShadowTaskResult, 0, len(pre.results))
	for resultIndex, result := range pre.results {
		txIndex := result.txIndex
		writeSetReady := txIndex >= 0 && txIndex < len(versioned.transactionWritesOK) && versioned.transactionWritesOK[txIndex]
		zeroIndegree := txIndex >= 0 && txIndex < len(versioned.dependencyHeads) && versioned.dependencyHeads[txIndex] < 0
		supported := txIndex >= 0 && txIndex < len(versioned.transactionSupported) && versioned.transactionSupported[txIndex]
		if result.err == nil && result.info != nil && result.writeSetErr == nil && result.applyEligible && result.applyErr == nil && result.applyMatch {
			stats.readCandidates++
			decision := discardShadowReadVersionResult{unsupported: true}
			if resultIndex < len(pre.readValidated) && pre.readValidated[resultIndex] {
				decision = pre.readVersions[resultIndex]
			}
			if decision.publishable {
				stats.readPublishable++
			}
			if decision.readConflict {
				stats.readConflicts++
			}
			if decision.unsupported {
				stats.readUnsupported++
			}
			if decision.deltaInvalid {
				stats.readDeltaInvalid++
			}
			if decision.sender {
				stats.readSender++
			}
			if decision.barrier {
				stats.readBarrier++
			}
			// The original DAG deliberately models every ordinary path. A valid
			// public-net carrier replaces its two global edges with an ordered
			// conditional reservation, so it is no longer comparable to that DAG.
			dagCandidate := writeSetReady && zeroIndegree && supported
			if decision.publishable == dagCandidate {
				stats.readDAGMatches++
			} else if !result.publicNetValid {
				stats.readDAGMismatches++
			}
		}
		if !writeSetReady || !zeroIndegree || !supported {
			continue
		}
		stats.candidates++
		if result.err != nil || result.info == nil || result.writeSetErr != nil || txIndex >= len(versioned.transactionWriteSets) {
			stats.errors++
			continue
		}
		infoMatch := compareDiscardShadowInfo(result.info, cfg.canonicalInfos[txIndex]) == 0
		writeMatch := state.EqualTransactionWriteSets(result.writes, versioned.transactionWriteSets[txIndex])
		balanceMatch := !cfg.captureBalanceTrace || (txIndex < len(cfg.canonicalBalanceTraces) && proto.Equal(result.balanceTrace, cfg.canonicalBalanceTraces[txIndex]))
		if infoMatch {
			stats.infoMatches++
		} else {
			stats.infoMismatches++
		}
		if writeMatch {
			stats.writeMatches++
		} else {
			stats.writeMismatches++
		}
		if balanceMatch {
			stats.balanceMatches++
		} else {
			stats.balanceMismatches++
		}
		switch {
		case !result.applyEligible:
			stats.applyUnsupported++
		case result.applyErr != nil:
			stats.errors++
		case result.applyMatch:
			stats.applyMatches++
		default:
			stats.applyMismatches++
		}
		if infoMatch && writeMatch && balanceMatch && result.applyEligible && result.applyErr == nil && result.applyMatch {
			stats.validated++
			result.matched = true
			result.coreMatch = true
			result.writeSetMatch = true
			validatedResults = append(validatedResults, result)
		}
	}
	if len(validatedResults) > 0 {
		publisher, err := shadow.base.Copy()
		if err != nil {
			stats.orderedErrors++
		} else {
			publisher.SetDynamicProperties(shadow.base.DynamicProperties().Copy())
			ordered := verifyOrderedApplyState(publisher, validatedResults, cfg)
			stats.orderedCandidates = ordered.candidates
			stats.orderedMatches = ordered.matches
			stats.orderedMismatches = ordered.mismatches
			stats.orderedErrors = ordered.errors
		}
	}
	discardShadowPreBlocksCounter.Inc(1)
	discardShadowPreTransfersCounter.Inc(stats.transfers)
	discardShadowPreExecutedCounter.Inc(stats.executed)
	discardShadowPreCandidatesCounter.Inc(stats.candidates)
	discardShadowPreInfoMatchesCounter.Inc(stats.infoMatches)
	discardShadowPreInfoMismatchesCounter.Inc(stats.infoMismatches)
	discardShadowPreWriteMatchesCounter.Inc(stats.writeMatches)
	discardShadowPreWriteMismatchesCounter.Inc(stats.writeMismatches)
	discardShadowPreApplyMatchesCounter.Inc(stats.applyMatches)
	discardShadowPreApplyMismatchesCounter.Inc(stats.applyMismatches)
	discardShadowPreApplyUnsupportedCounter.Inc(stats.applyUnsupported)
	discardShadowPreValidatedCounter.Inc(stats.validated)
	discardShadowPreOrderedCandidatesCounter.Inc(stats.orderedCandidates)
	discardShadowPreOrderedMatchesCounter.Inc(stats.orderedMatches)
	discardShadowPreOrderedMismatchesCounter.Inc(stats.orderedMismatches)
	discardShadowPreOrderedErrorsCounter.Inc(stats.orderedErrors)
	discardShadowPreErrorsCounter.Inc(stats.errors)
	discardShadowPreWallNanosCounter.Inc(pre.wallNanos)
	discardShadowPreBalanceTraceMatchesCounter.Inc(stats.balanceMatches)
	discardShadowPreBalanceTraceMismatchesCounter.Inc(stats.balanceMismatches)
	discardShadowReadVersionCandidatesCounter.Inc(stats.readCandidates)
	discardShadowReadVersionPublishableCounter.Inc(stats.readPublishable)
	discardShadowReadVersionConflictsCounter.Inc(stats.readConflicts)
	discardShadowReadVersionUnsupportedCounter.Inc(stats.readUnsupported)
	discardShadowReadVersionDeltaInvalidCounter.Inc(stats.readDeltaInvalid)
	discardShadowReadVersionSenderCounter.Inc(stats.readSender)
	discardShadowReadVersionBarrierCounter.Inc(stats.readBarrier)
	discardShadowReadVersionDAGMatchesCounter.Inc(stats.readDAGMatches)
	discardShadowReadVersionDAGMismatchesCounter.Inc(stats.readDAGMismatches)
	return stats
}

func equalSenderChainWriteSets(worker, canonical state.TransactionWriteSet, ignorePublicNet bool) bool {
	isIgnored := func(key state.TransactionAccessKey) bool {
		return ignorePublicNet && isPublicNetReservationKey(key)
	}
	for key, workerValue := range worker {
		if isIgnored(key) {
			continue
		}
		canonicalValue, ok := canonical[key]
		if !ok || workerValue.Exists != canonicalValue.Exists || workerValue.Commutative != canonicalValue.Commutative ||
			!bytes.Equal(workerValue.Value, canonicalValue.Value) {
			return false
		}
	}
	for key := range canonical {
		if isIgnored(key) {
			continue
		}
		if _, ok := worker[key]; !ok {
			return false
		}
	}
	return true
}

func equalProjectedPublicNetWriteSet(worker, canonical state.TransactionWriteSet, projection discardShadowPublicNetProjection) bool {
	if !projection.observed || !projection.admitted || !equalSenderChainWriteSets(worker, canonical, true) {
		return false
	}
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	usage, ok := canonical[usageKey]
	if !ok || !usage.Exists || usage.Commutative || len(usage.Value) != 8 ||
		int64(binary.BigEndian.Uint64(usage.Value)) != projection.expectedUsage {
		return false
	}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	timeValue, timeSet := canonical[timeKey]
	if !projection.expectedTimeSet {
		return !timeSet
	}
	return timeSet && timeValue.Exists && !timeValue.Commutative && len(timeValue.Value) == 8 &&
		int64(binary.BigEndian.Uint64(timeValue.Value)) == projection.expectedTime
}

// finishTransferSenderChains compares only results whose exact read sources
// still match the serial prefix. public_net_usage/time are excluded from the
// write comparison when a valid reservation carrier is present: their ordered
// values deliberately depend on transfers from other sender chains and are
// already revalidated by the publisher-specific path.
func (shadow *discardShadowBlock) finishTransferSenderChains(pre *discardShadowPreexecution, versioned *versionedAccessShadow, cfg discardShadowRunConfig) discardShadowSenderChainStats {
	if pre == nil {
		return discardShadowSenderChainStats{}
	}
	stats := shadow.finishSenderChains(pre, versioned, cfg, preexecutedTransferReady, true)
	discardShadowSenderChainBlocksCounter.Inc(1)
	discardShadowSenderChainGroupsCounter.Inc(stats.groups)
	discardShadowSenderChainExecutedCounter.Inc(stats.executed)
	discardShadowSenderChainForwardedCounter.Inc(stats.forwarded)
	discardShadowSenderChainCandidatesCounter.Inc(stats.candidates)
	discardShadowSenderChainValidatedCounter.Inc(stats.validated)
	discardShadowSenderChainForwardedOKCounter.Inc(stats.forwardedValidated)
	discardShadowSenderChainReadConflictsCounter.Inc(stats.readConflicts)
	discardShadowSenderChainSenderConflictsCounter.Inc(stats.senderConflicts)
	discardShadowSenderChainInfoMismatchesCounter.Inc(stats.infoMismatches)
	discardShadowSenderChainWriteMismatchesCounter.Inc(stats.writeMismatches)
	discardShadowSenderChainBalanceMismatchesCounter.Inc(stats.balanceMismatches)
	discardShadowSenderChainErrorsCounter.Inc(stats.errors)
	discardShadowSenderChainWallNanosCounter.Inc(pre.wallNanos)
	return stats
}

// finishVMSenderChains keeps public bandwidth writes in the exact comparison.
// The VM canary must expose resource-order differences before a conditional
// reservation carrier is designed; it cannot inherit Transfer's gated rebase.
func (shadow *discardShadowBlock) finishVMSenderChains(pre *discardShadowPreexecution, versioned *versionedAccessShadow, cfg discardShadowRunConfig) discardShadowSenderChainStats {
	if pre == nil {
		return discardShadowSenderChainStats{}
	}
	stats := shadow.finishSenderChains(pre, versioned, cfg, preexecutedResultReady, false)
	discardShadowVMSenderChainBlocksCounter.Inc(1)
	discardShadowVMSenderChainGroupsCounter.Inc(stats.groups)
	discardShadowVMSenderChainExecutedCounter.Inc(stats.executed)
	discardShadowVMSenderChainForwardedCounter.Inc(stats.forwarded)
	discardShadowVMSenderChainCandidatesCounter.Inc(stats.candidates)
	discardShadowVMSenderChainValidatedCounter.Inc(stats.validated)
	discardShadowVMSenderChainForwardedOKCounter.Inc(stats.forwardedValidated)
	discardShadowVMSenderChainReadConflictsCounter.Inc(stats.readConflicts)
	discardShadowVMSenderChainSenderConflictsCounter.Inc(stats.senderConflicts)
	discardShadowVMSenderChainInfoMismatchesCounter.Inc(stats.infoMismatches)
	discardShadowVMSenderChainWriteMismatchesCounter.Inc(stats.writeMismatches)
	discardShadowVMSenderChainBalanceMismatchesCounter.Inc(stats.balanceMismatches)
	discardShadowVMSenderChainErrorsCounter.Inc(stats.errors)
	discardShadowVMSenderChainWallNanosCounter.Inc(pre.wallNanos)
	discardShadowVMSenderChainPublicNetCounter.Inc(stats.publicNetCandidates)
	discardShadowVMSenderChainPublicNetOnlyCounter.Inc(stats.publicNetOnly)
	discardShadowVMSenderChainOtherWriteCounter.Inc(stats.otherWriteMismatch)
	discardShadowVMSenderChainResultErrorsCounter.Inc(stats.resultErrors)
	discardShadowVMSenderChainMissingInfoCounter.Inc(stats.missingInfo)
	discardShadowVMSenderChainWriteErrorsCounter.Inc(stats.writeSetErrors)
	discardShadowVMSenderChainApplyUnsupportedCounter.Inc(stats.applyUnsupported)
	discardShadowVMSenderChainApplyErrorsCounter.Inc(stats.applyErrors)
	discardShadowVMSenderChainApplyMismatchCounter.Inc(stats.applyMismatches)
	discardShadowVMSenderChainReadinessCounter.Inc(stats.readinessRejected)
	discardShadowVMPublicNetProjectionCounter.Inc(stats.publicNetProjected)
	discardShadowVMPublicNetAdmittedCounter.Inc(stats.publicNetAdmitted)
	discardShadowVMPublicNetRebasedCounter.Inc(stats.publicNetRebased)
	discardShadowVMPublicNetLimitRejectedCounter.Inc(stats.publicNetRejected)
	discardShadowVMPublicNetProjectionMatchesCounter.Inc(stats.projectionMatches)
	discardShadowVMPublicNetProjectionMismatchCounter.Inc(stats.projectionMismatches)
	discardShadowVMPublicNetProjectionMissingCounter.Inc(stats.projectionMissing)
	discardShadowVMBlockEnergyProjectionCounter.Inc(stats.blockEnergyCandidates)
	discardShadowVMBlockEnergyObservedCounter.Inc(stats.blockEnergyObserved)
	discardShadowVMBlockEnergyMatchesCounter.Inc(stats.blockEnergyMatches)
	discardShadowVMBlockEnergyMismatchesCounter.Inc(stats.blockEnergyMismatches)
	discardShadowVMBlockEnergyMissingCounter.Inc(stats.blockEnergyMissing)
	return stats
}

func (shadow *discardShadowBlock) finishSenderChains(pre *discardShadowPreexecution, versioned *versionedAccessShadow, cfg discardShadowRunConfig, ready func(*discardShadowTaskResult) bool, ignorePublicNet bool) discardShadowSenderChainStats {
	var stats discardShadowSenderChainStats
	if shadow == nil || pre == nil || versioned == nil || ready == nil || len(cfg.canonicalInfos) != len(cfg.transactions) {
		return stats
	}
	stats.groups = int64(pre.groups)
	stats.executed = int64(len(pre.results))
	accepted := make([]bool, len(pre.results))
	clear(pre.published)
	for txIndex, resultIndex := range pre.resultByTx {
		if resultIndex < 0 || resultIndex >= len(pre.results) {
			continue
		}
		result := pre.results[resultIndex]
		if result.senderVersioned {
			stats.forwarded++
		}
		resultReady := ready(&result)
		switch {
		case result.err != nil:
			stats.resultErrors++
		case result.info == nil:
			stats.missingInfo++
		case result.writeSetErr != nil:
			stats.writeSetErrors++
		case !result.applyEligible:
			stats.applyUnsupported++
		case result.applyErr != nil:
			stats.applyErrors++
		case !result.applyMatch:
			stats.applyMismatches++
		case !resultReady:
			stats.readinessRejected++
		default:
			break
		}
		if !resultReady {
			stats.errors++
			continue
		}
		if resultIndex >= len(pre.readValidated) || !pre.readValidated[resultIndex] {
			stats.errors++
			continue
		}
		decision := pre.readVersions[resultIndex]
		if decision.readConflict {
			stats.readConflicts++
		}
		if result.senderVersioned {
			predecessorResult := -1
			if result.senderPredecessor >= 0 && result.senderPredecessor < len(pre.resultByTx) {
				predecessorResult = pre.resultByTx[result.senderPredecessor]
			}
			if predecessorResult < 0 || predecessorResult >= len(accepted) || !accepted[predecessorResult] {
				decision.sender = true
				decision.publishable = false
			}
		}
		if decision.sender {
			stats.senderConflicts++
		}
		if !decision.publishable {
			continue
		}
		stats.candidates++
		if result.publicNetValid {
			stats.publicNetCandidates++
		}
		if !ignorePublicNet {
			stats.blockEnergyCandidates++
			if resultIndex >= len(pre.blockEnergy) || !pre.blockEnergy[resultIndex].observed {
				stats.blockEnergyMissing++
			} else {
				stats.blockEnergyObserved++
				if !pre.blockEnergy[resultIndex].validated {
					stats.blockEnergyMissing++
				} else if pre.blockEnergy[resultIndex].match {
					stats.blockEnergyMatches++
				} else {
					stats.blockEnergyMismatches++
				}
			}
		}
		if txIndex < 0 || txIndex >= len(versioned.transactionWritesOK) || !versioned.transactionWritesOK[txIndex] ||
			txIndex >= len(versioned.transactionWriteSets) || (cfg.captureBalanceTrace && txIndex >= len(cfg.canonicalBalanceTraces)) {
			stats.errors++
			continue
		}
		infoMatch := compareDiscardShadowInfo(result.info, cfg.canonicalInfos[txIndex]) == 0
		writeMatch := equalSenderChainWriteSets(result.writes, versioned.transactionWriteSets[txIndex], ignorePublicNet && result.publicNetValid)
		balanceMatch := !cfg.captureBalanceTrace || proto.Equal(result.balanceTrace, cfg.canonicalBalanceTraces[txIndex])
		if result.publicNetValid && !ignorePublicNet {
			if resultIndex >= len(pre.publicNet) || !pre.publicNet[resultIndex].observed {
				stats.projectionMissing++
			} else {
				projection := pre.publicNet[resultIndex]
				stats.publicNetProjected++
				if !projection.admitted {
					stats.publicNetRejected++
				} else {
					stats.publicNetAdmitted++
					if projection.rebased {
						stats.publicNetRebased++
					}
					if equalProjectedPublicNetWriteSet(result.writes, versioned.transactionWriteSets[txIndex], projection) {
						stats.projectionMatches++
					} else {
						stats.projectionMismatches++
					}
				}
			}
		}
		if !infoMatch {
			stats.infoMismatches++
		}
		if !writeMatch {
			stats.writeMismatches++
			if !ignorePublicNet && result.publicNetValid &&
				equalSenderChainWriteSets(result.writes, versioned.transactionWriteSets[txIndex], true) {
				stats.publicNetOnly++
			} else {
				stats.otherWriteMismatch++
			}
		}
		if !balanceMatch {
			stats.balanceMismatches++
		}
		if infoMatch && writeMatch && balanceMatch {
			stats.validated++
			accepted[resultIndex] = true
			pre.published[resultIndex] = true
			if result.senderVersioned {
				stats.forwardedValidated++
			}
		}
	}
	return stats
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
	if cfg.captureBalanceTrace {
		blockHash := cfg.block.Hash()
		for _, workerState := range workerStates {
			workerState.BeginBalanceTrace(int64(cfg.block.Number()), blockHash.Bytes(), cfg.block.Timestamp())
		}
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

	orderedResults := make([]discardShadowTaskResult, 0, len(candidates))
	var executed, matches, mismatches, coreMatches, coreMismatches, writeSetMatches, writeSetMismatches, writeSetErrors, applyEligible, applyUnsupported, applyMatches, applyMismatches, applyErrors, applyUnsupportedAccount, applyUnsupportedGeneration, applyUnsupportedSelfDestruct, applyUnsupportedField, applyUnsupportedOther, executionErrors int64
	for result := range results {
		orderedResults = append(orderedResults, result)
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
					switch {
					case result.applyErr != nil:
						applyErrors++
					case result.applyMatch:
						applyMatches++
					default:
						applyMismatches++
						if result.applyMismatch&discardShadowApplyMismatchMissing != 0 {
							discardShadowApplyMismatchMissingCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchExtra != 0 {
							discardShadowApplyMismatchExtraCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchPresence != 0 {
							discardShadowApplyMismatchPresenceCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchCommutative != 0 {
							discardShadowApplyMismatchCommutativeCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchValue != 0 {
							discardShadowApplyMismatchValueCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchAccount != 0 {
							discardShadowApplyMismatchAccountCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchAccountField != 0 {
							discardShadowApplyMismatchAccountFieldCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchWitness != 0 {
							discardShadowApplyMismatchWitnessCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchStorage != 0 {
							discardShadowApplyMismatchStorageCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchCode != 0 {
							discardShadowApplyMismatchCodeCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchMetadata != 0 {
							discardShadowApplyMismatchMetadataCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchAccountKV != 0 {
							discardShadowApplyMismatchAccountKVCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchTransient != 0 {
							discardShadowApplyMismatchTransientCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchDynamic != 0 {
							discardShadowApplyMismatchDynamicCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchRaw != 0 {
							discardShadowApplyMismatchRawCounter.Inc(1)
						}
						if result.applyMismatch&discardShadowApplyMismatchOther != 0 {
							discardShadowApplyMismatchOtherCounter.Inc(1)
						}
					}
				} else {
					applyUnsupported++
					if result.applyUnsupported&discardShadowApplyUnsupportedAccount != 0 {
						applyUnsupportedAccount++
					}
					if result.applyUnsupported&discardShadowApplyUnsupportedGeneration != 0 {
						applyUnsupportedGeneration++
					}
					if result.applyUnsupported&discardShadowApplyUnsupportedSelfDestruct != 0 {
						applyUnsupportedSelfDestruct++
					}
					if result.applyUnsupported&discardShadowApplyUnsupportedField != 0 {
						applyUnsupportedField++
					}
					if result.applyUnsupported&discardShadowApplyUnsupportedOther != 0 {
						applyUnsupportedOther++
					}
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
	discardShadowWriteSetApplyMatchesCounter.Inc(applyMatches)
	discardShadowWriteSetApplyMismatchesCounter.Inc(applyMismatches)
	discardShadowWriteSetApplyErrorsCounter.Inc(applyErrors)
	discardShadowApplyUnsupportedAccountCounter.Inc(applyUnsupportedAccount)
	discardShadowApplyUnsupportedGenerationCounter.Inc(applyUnsupportedGeneration)
	discardShadowApplyUnsupportedSelfDestructCounter.Inc(applyUnsupportedSelfDestruct)
	discardShadowApplyUnsupportedFieldCounter.Inc(applyUnsupportedField)
	discardShadowApplyUnsupportedOtherCounter.Inc(applyUnsupportedOther)
	discardShadowErrorsCounter.Inc(executionErrors)
	discardShadowCopyNanosCounter.Inc(shadow.copyNanos)
	discardShadowExecutionNanosCounter.Inc(executionNanos)
	discardShadowLastCandidatesGauge.Update(int64(len(candidates)))
	discardShadowLastExecutedGauge.Update(executed)
	discardShadowLastMatchesGauge.Update(matches)
	shadow.verifyOrderedApply(orderedResults, cfg)
	return discardShadowRunStats{
		candidates: int64(len(candidates)),
		executed:   executed,
		matches:    matches,
		mismatches: mismatches,
		errors:     executionErrors,
	}
}

// verifyOrderedApply accumulates successful worker write sets in original
// transaction order on the block-start shadow. Unlike each worker's isolated
// reapply, this exercises a shared publisher baseline, including successive
// commutative settlement deltas. The publisher is discarded with the sampled
// block and never reaches canonical state or the backing database.
func (shadow *discardShadowBlock) verifyOrderedApply(results []discardShadowTaskResult, cfg discardShadowRunConfig) discardShadowOrderedApplyStats {
	if shadow == nil || shadow.base == nil || len(results) == 0 {
		return discardShadowOrderedApplyStats{}
	}
	stats := verifyOrderedApplyState(shadow.base, results, cfg)
	discardShadowOrderedApplyCandidatesCounter.Inc(stats.candidates)
	discardShadowOrderedApplyMatchesCounter.Inc(stats.matches)
	discardShadowOrderedApplyMismatchesCounter.Inc(stats.mismatches)
	discardShadowOrderedApplyErrorsCounter.Inc(stats.errors)
	return stats
}

func verifyOrderedApplyState(publisher *state.StateDB, results []discardShadowTaskResult, cfg discardShadowRunConfig) discardShadowOrderedApplyStats {
	var stats discardShadowOrderedApplyStats
	if publisher == nil || len(results) == 0 {
		return stats
	}
	sort.Slice(results, func(i, j int) bool { return results[i].txIndex < results[j].txIndex })
	dynProps := publisher.DynamicProperties()
	var recorder state.TransactionAccessRecorder
	raw := discardKVOverlay{parent: cfg.db, recorder: &recorder}
	for _, result := range results {
		if result.err != nil || !result.matched || result.writeSetErr != nil || !result.writeSetMatch ||
			!result.applyEligible || result.applyErr != nil || !result.applyMatch {
			continue
		}
		stats.candidates++
		recorder.Reset(64)
		raw.recorder = &recorder
		journalMark := publisher.DomainChangeJournalMark()
		if err := publisher.ApplyTransactionWriteSetRecorded(result.writes, dynProps, &raw, &recorder); err != nil {
			stats.errors++
			break
		}
		publisher.FinalizeTransaction()
		applied, known, err := publisher.CaptureTransactionWriteSet(journalMark, &recorder, dynProps)
		switch {
		case err != nil || !known:
			stats.errors++
			break
		case !state.EqualTransactionWriteSets(applied, result.writes):
			stats.mismatches++
			break
		default:
			stats.matches++
			continue
		}
		break
	}
	return stats
}

type discardShadowWorker struct {
	state         *state.StateDB
	dynProps      *state.DynamicProperties
	db            discardKVOverlay
	forkCache     *forks.VersionPassCache
	scratch       applyTransactionScratch
	infoSlot      transactionInfoSlot
	recorder      state.TransactionAccessRecorder
	applyRecorder state.TransactionAccessRecorder
}

func (worker *discardShadowWorker) execute(txIndex int, cfg discardShadowRunConfig) discardShadowTaskResult {
	if txIndex < 0 || txIndex >= len(cfg.transactions) || cfg.block == nil {
		return discardShadowTaskResult{txIndex: txIndex, err: errors.New("missing shadow transaction input")}
	}
	compareCanonical := txIndex < len(cfg.canonicalInfos) && cfg.canonicalInfos[txIndex] != nil
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
		worker.state.RevertToSnapshot(stateSnapshot)
		worker.dynProps.RevertToSnapshot(dpSnapshot)
		return discardShadowTaskResult{txIndex: txIndex, class: class, err: err}
	}

	shadowInfo := worker.infoSlot.build(tx, result, cfg.block.Number(), cfg.block.Timestamp(), worker.dynProps.AllowTransactionFeePool())
	var retainedInfo *corepb.TransactionInfo
	if cfg.retainInfos {
		retainedInfo = proto.Clone(shadowInfo).(*corepb.TransactionInfo)
	}
	var mismatch discardShadowMismatch
	if compareCanonical {
		mismatch = compareDiscardShadowInfo(shadowInfo, cfg.canonicalInfos[txIndex])
	}
	coreMismatch := mismatch &^ (discardShadowMismatchReceipt | discardShadowMismatchOwnerDiagnostic | discardShadowMismatchEnergyDiagnostic)
	vm.ReleaseExecutionLogs(result.Logs)
	result.Logs = nil
	worker.state.FinalizeTransaction()
	worker.state.EndBalanceTraceTransaction(balanceTraceTransactionStatus(result))
	var balanceTrace *contractpb.TransactionBalanceTrace
	if cfg.captureBalanceTrace {
		balanceTrace = worker.state.CopyLastBalanceTraceTransaction(tx.Hash().Bytes())
	}
	worker.state.SetTransactionAccessRecorder(nil)
	worker.dynProps.SetTransactionAccessRecorder(nil)
	writes, known, writeSetErr := worker.state.CaptureTransactionWriteSet(journalMark, &worker.recorder, worker.dynProps)
	reads := worker.recorder.CaptureReadSet()
	publicNet, hasPublicNet := worker.recorder.PublicNetReservation()
	publicNetValid := hasPublicNet && writeSetErr == nil && known && validatePublicNetReservation(publicNet, writes, worker.dynProps)
	if writeSetErr == nil && !known {
		writeSetErr = errors.New("unknown worker state write")
	}
	writeSetMatch := writeSetErr == nil
	applyEligible := false
	var applyUnsupported discardShadowApplyUnsupported
	if writeSetMatch {
		hasCanonicalWriteSet := txIndex < len(cfg.canonicalWriteSets) && cfg.canonicalWriteSets[txIndex] != nil
		if hasCanonicalWriteSet {
			writeSetMatch = state.EqualTransactionWriteSets(writes, cfg.canonicalWriteSets[txIndex])
		} else if !cfg.retainInfos {
			writeSetMatch = false
		}
		applyEligible = state.ValidateTransactionWriteSetApply(writes, worker.dynProps, &worker.db) == nil
		if !applyEligible {
			applyUnsupported = classifyDiscardShadowApplyUnsupported(writes)
		}
	}
	worker.state.RevertToSnapshot(stateSnapshot)
	worker.dynProps.RevertToSnapshot(dpSnapshot)
	if applyEligible {
		if err := worker.state.ValidateTransactionWriteSetApply(writes, worker.dynProps, &worker.db); err != nil {
			applyEligible = false
			applyUnsupported = classifyDiscardShadowApplyUnsupported(writes)
		}
	}
	applyMatch := false
	var applyMismatch discardShadowApplyMismatch
	var applyErr error
	if applyEligible {
		applyStateSnapshot := worker.state.Snapshot()
		applyDPSnapshot := worker.dynProps.Snapshot()
		applyJournalMark := worker.state.DomainChangeJournalMark()
		worker.applyRecorder.Reset(64)
		worker.db.reset()
		worker.db.recorder = &worker.applyRecorder
		applyErr = worker.state.ApplyTransactionWriteSetRecorded(writes, worker.dynProps, &worker.db, &worker.applyRecorder)
		if applyErr == nil {
			worker.state.FinalizeTransaction()
			appliedWrites, appliedKnown, captureErr := worker.state.CaptureTransactionWriteSet(applyJournalMark, &worker.applyRecorder, worker.dynProps)
			switch {
			case captureErr != nil:
				applyErr = captureErr
			case !appliedKnown:
				applyErr = errors.New("unknown applied state write")
			default:
				applyMatch = state.EqualTransactionWriteSets(appliedWrites, writes)
				if !applyMatch {
					applyMismatch = classifyDiscardShadowApplyMismatch(appliedWrites, writes)
				}
			}
		}
		worker.state.RevertToSnapshot(applyStateSnapshot)
		worker.dynProps.RevertToSnapshot(applyDPSnapshot)
		worker.db.reset()
		worker.db.recorder = &worker.recorder
	}
	return discardShadowTaskResult{
		txIndex:          txIndex,
		class:            class,
		mismatch:         mismatch,
		coreMatch:        coreMismatch == 0,
		matched:          mismatch == 0,
		writeSetMatch:    writeSetMatch,
		writeSetErr:      writeSetErr,
		applyEligible:    applyEligible,
		applyUnsupported: applyUnsupported,
		applyMatch:       applyMatch,
		applyMismatch:    applyMismatch,
		applyErr:         applyErr,
		writes:           writes,
		reads:            reads,
		info:             retainedInfo,
		balanceTrace:     balanceTrace,
		publicNet:        publicNet,
		publicNetValid:   publicNetValid,
	}
}
