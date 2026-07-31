package core

import (
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var (
	speculativeShadowBlocksCounter               = metrics.NewRegisteredCounter("core/speculative_shadow/blocks", nil)
	speculativeShadowTransactionsCounter         = metrics.NewRegisteredCounter("core/speculative_shadow/transactions", nil)
	speculativeShadowTransferCandidatesCounter   = metrics.NewRegisteredCounter("core/speculative_shadow/transfer_candidates", nil)
	speculativeShadowEligibleCounter             = metrics.NewRegisteredCounter("core/speculative_shadow/eligible", nil)
	speculativeShadowUnsafeCounter               = metrics.NewRegisteredCounter("core/speculative_shadow/unsafe", nil)
	speculativeShadowDependenciesCounter         = metrics.NewRegisteredCounter("core/speculative_shadow/dependencies", nil)
	speculativeShadowBarriersCounter             = metrics.NewRegisteredCounter("core/speculative_shadow/barriers", nil)
	speculativeShadowWavesCounter                = metrics.NewRegisteredCounter("core/speculative_shadow/waves", nil)
	speculativeShadowParallelTransactionsCounter = metrics.NewRegisteredCounter("core/speculative_shadow/parallel_transactions", nil)
	speculativeShadowLastEligibleGauge           = metrics.NewRegisteredGauge("core/speculative_shadow/last_block/eligible", nil)
	speculativeShadowLastParallelGauge           = metrics.NewRegisteredGauge("core/speculative_shadow/last_block/parallel_transactions", nil)
	speculativeShadowLastMaxWaveWidthGauge       = metrics.NewRegisteredGauge("core/speculative_shadow/last_block/max_wave_width", nil)
)

// speculativeTransferShadow is P4's observe-only conflict planner. It consumes
// the authoritative serial journal after each successful transaction and asks
// which ordinary transfers could have shared one immutable wave snapshot. It
// never executes, validates, reorders, or publishes state.
//
// The first eligibility boundary is deliberately narrow. A transfer is safe to
// model only when its actual writes are existing-account writes on owner and
// recipient, and it does not change dynamic properties. That excludes account
// creation, free/public bandwidth, fee pool/burn counters, blackhole routing,
// multisig/memo fees, and any future unrecognized journal write.
type speculativeTransferShadow struct {
	stats                   speculativeTransferShadowStats
	waveAddresses           map[tcommon.Address]struct{}
	waveAddressCapacityHint int
	waveWidth               int64
}

type speculativeTransferShadowStats struct {
	transactions       int64
	transferCandidates int64
	eligible           int64
	unsafe             int64
	dependencies       int64
	barriers           int64
	waves              int64
	parallelTxs        int64
	maxWaveWidth       int64
}

// Prepare records a bounded lazy capacity hint. Empty/non-Transfer blocks pay
// no map allocation; the first eligible transfer allocates enough room for a
// typical whole-block wave rather than repeatedly growing from four entries.
func (s *speculativeTransferShadow) Prepare(transactionCount int) {
	const maxAddressHint = 512
	hint := transactionCount * 2
	if hint > maxAddressHint {
		hint = maxAddressHint
	}
	if hint < 4 {
		hint = 4
	}
	s.waveAddressCapacityHint = hint
}

func (s *speculativeTransferShadow) Observe(tx *types.Transaction, statedb *state.StateDB, journalMark int, dynamicPropertiesChanged bool) {
	s.stats.transactions++
	if tx == nil || tx.ContractType() != corepb.Transaction_Contract_TransferContract {
		s.barrier()
		return
	}
	s.stats.transferCandidates++

	message, err := tx.DecodedContract()
	transfer, ok := message.(*contractpb.TransferContract)
	if err != nil || !ok || len(transfer.OwnerAddress) != tcommon.AddressLength || len(transfer.ToAddress) != tcommon.AddressLength {
		s.unsafe()
		return
	}
	owner := tcommon.BytesToAddress(transfer.OwnerAddress)
	recipient := tcommon.BytesToAddress(transfer.ToAddress)
	if owner == recipient || dynamicPropertiesChanged || !transferWritesOnlyParticipants(statedb, journalMark, owner, recipient) {
		s.unsafe()
		return
	}

	s.stats.eligible++
	if s.waveConflicts(owner, recipient) {
		s.stats.dependencies++
		s.flushWave()
	}
	if s.waveAddresses == nil {
		hint := s.waveAddressCapacityHint
		if hint < 4 {
			hint = 4
		}
		s.waveAddresses = make(map[tcommon.Address]struct{}, hint)
	}
	s.waveAddresses[owner] = struct{}{}
	s.waveAddresses[recipient] = struct{}{}
	s.waveWidth++
}

func transferWritesOnlyParticipants(statedb *state.StateDB, journalMark int, owner, recipient tcommon.Address) bool {
	if statedb == nil {
		return false
	}
	ownerWritten := false
	recipientWritten := false
	safe := true
	statedb.VisitTransactionWritesSince(journalMark, func(write state.TransactionWrite) bool {
		if write.Kind != state.TransactionWriteAccount {
			safe = false
			return false
		}
		switch write.Address {
		case owner:
			ownerWritten = true
		case recipient:
			recipientWritten = true
		default:
			safe = false
			return false
		}
		return true
	})
	return safe && ownerWritten && recipientWritten
}

func (s *speculativeTransferShadow) waveConflicts(owner, recipient tcommon.Address) bool {
	if len(s.waveAddresses) == 0 {
		return false
	}
	_, ownerConflict := s.waveAddresses[owner]
	_, recipientConflict := s.waveAddresses[recipient]
	return ownerConflict || recipientConflict
}

func (s *speculativeTransferShadow) unsafe() {
	s.stats.unsafe++
	s.barrier()
}

func (s *speculativeTransferShadow) barrier() {
	s.flushWave()
	s.stats.barriers++
}

func (s *speculativeTransferShadow) flushWave() {
	if s.waveWidth == 0 {
		return
	}
	s.stats.waves++
	if s.waveWidth > 1 {
		s.stats.parallelTxs += s.waveWidth
	}
	if s.waveWidth > s.stats.maxWaveWidth {
		s.stats.maxWaveWidth = s.waveWidth
	}
	s.waveWidth = 0
	clear(s.waveAddresses)
}

func (s *speculativeTransferShadow) Finish() speculativeTransferShadowStats {
	s.flushWave()
	return s.stats
}

func (s *speculativeTransferShadow) Publish() {
	stats := s.Finish()
	speculativeShadowBlocksCounter.Inc(1)
	speculativeShadowTransactionsCounter.Inc(stats.transactions)
	speculativeShadowTransferCandidatesCounter.Inc(stats.transferCandidates)
	speculativeShadowEligibleCounter.Inc(stats.eligible)
	speculativeShadowUnsafeCounter.Inc(stats.unsafe)
	speculativeShadowDependenciesCounter.Inc(stats.dependencies)
	speculativeShadowBarriersCounter.Inc(stats.barriers)
	speculativeShadowWavesCounter.Inc(stats.waves)
	speculativeShadowParallelTransactionsCounter.Inc(stats.parallelTxs)
	speculativeShadowLastEligibleGauge.Update(stats.eligible)
	speculativeShadowLastParallelGauge.Update(stats.parallelTxs)
	speculativeShadowLastMaxWaveWidthGauge.Update(stats.maxWaveWidth)
}
