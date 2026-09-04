package core

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
)

// ErrExchangeRejected is returned by ApplyTransaction when an
// ExchangeTransactionContract reaches the block-apply path after the
// VERSION_4_8_0_1 (block version 33) fork has activated. Mirrors java-tron
// Manager.rejectExchangeTransaction (PR #6507, master commit 45e3bf88ca).
// Same error string as core/txpool.ErrExchangeRejected so log-grep
// consumers see one wire-format value across both paths.
var ErrExchangeRejected = errors.New("ExchangeTransactionContract is rejected")

const (
	mainnetCreateTransferFailureRepairBlock        = uint64(12_978_262)
	mainnetCreateTransferFailureBadBalance         = int64(2_115_378)
	mainnetCreateTransferFailureCanonicalBalance   = int64(6_320_008)
	mainnetParallelVMMissedPaymentRepairBlock      = uint64(18_414_381)
	mainnetParallelVMMissedPaymentBadBalance       = int64(67_007_277)
	mainnetParallelVMMissedPaymentCanonicalBalance = int64(78_834_028)
	mainnetParallelVMMissedPaymentContractBalance  = int64(2_533_557_004_363)
	mainnetParallelVMMissedPaymentPayerBalance     = int64(664_136_885_514)
	mainnetParallelVMMissedPaymentBlackholeBalance = int64(-9_223_234_026_935_214_473)
	mainnetParallelVMMissedPaymentAmount           = int64(11_826_751)
	mainnetParallelVMMissedPaymentEnergyFee        = int64(85_360)
	mainnetCOSTMissedRewardRepairBlock             = uint64(20_674_403)
	mainnetCOSTMissedRewardRecipientBadBalance     = int64(5_000_000)
	mainnetCOSTMissedRewardRecipientBalance        = int64(10_000_000)
	mainnetCOSTMissedRewardContractBalance         = int64(5_101_000_000)
	mainnetCOSTMissedRewardPayerBalance            = int64(28_972_799_410)
	mainnetCOSTMissedRewardBlackholeBalance        = int64(-9_223_225_669_149_305_083)
	mainnetCOSTMissedRewardAmount                  = int64(5_000_000)
	mainnetCOSTMissedRewardEnergyFee               = int64(140_870)
	mainnetWINKMissingCodeRepairBlock              = uint64(20_674_403)
	mainnetWINKRuntimeOffset                       = 0x35a
	mainnetWINKRuntimeSize                         = 0x1ae7
)

var (
	mainnetCreateTransferFailureRepairCounter   = metrics.NewRegisteredCounter("core/mainnet_state_repair/create_transfer_failure", nil)
	mainnetParallelVMMissedPaymentRepairCounter = metrics.NewRegisteredCounter("core/mainnet_state_repair/parallel_vm_missed_payment", nil)
	mainnetCOSTMissedRewardRepairCounter        = metrics.NewRegisteredCounter("core/mainnet_state_repair/cost_missed_reward", nil)
	mainnetWINKMissingCodeRepairCounter         = metrics.NewRegisteredCounter("core/mainnet_state_repair/wink_missing_runtime", nil)

	mainnetCreateTransferFailureRepairBlockID = tcommon.HexToHash("0000000000c60856f9bd889435bb483b5c66709a508550c78c72c4c0db2aaab2")
	mainnetCreateTransferFailurePayer         = tcommon.Address{
		0x41, 0x1e, 0x03, 0xe4, 0x33, 0xaa, 0x39, 0xf4, 0xee, 0x45, 0xc6,
		0x4e, 0x78, 0x94, 0xac, 0x41, 0x6f, 0x0a, 0x82, 0x05, 0xfc,
	}
	mainnetParallelVMMissedPaymentRepairBlockID = tcommon.HexToHash("000000000118fb2ded825760c49c8d2588e04dcc283a12cf7d957413f7defed8")
	mainnetParallelVMMissedPaymentRecipient     = tcommon.Address{
		0x41, 0x9f, 0x9e, 0x5e, 0xc2, 0xcc, 0xfb, 0xf8, 0xca, 0x53, 0x25,
		0x50, 0xc8, 0x39, 0x43, 0x12, 0xc8, 0x6a, 0x5b, 0x70, 0x87,
	}
	mainnetParallelVMMissedPaymentContract = tcommon.Address{
		0x41, 0xa0, 0x61, 0x13, 0x7c, 0x9f, 0xcb, 0xa2, 0xff, 0x96, 0x58,
		0x66, 0x0d, 0x49, 0xc1, 0x2b, 0x5c, 0x09, 0x1f, 0xe5, 0x85,
	}
	mainnetParallelVMMissedPaymentPayer = tcommon.Address{
		0x41, 0x98, 0x56, 0x2b, 0x38, 0x71, 0x53, 0x3e, 0x85, 0x3d, 0x6c,
		0xbc, 0xfc, 0x51, 0x11, 0x97, 0xb5, 0x94, 0x30, 0x47, 0xc0,
	}
	mainnetCOSTMissedRewardRepairBlockID = tcommon.HexToHash("00000000013b77633413e57c9a741dbcb6c6faf7ab1f261c1210a19d08d88b7d")
	mainnetCOSTMissedRewardRecipient     = tcommon.Address{
		0x41, 0xe2, 0xe8, 0x86, 0x7d, 0xc6, 0x25, 0x42, 0x6a, 0x62, 0x22,
		0x4a, 0xbd, 0x1f, 0x40, 0x7e, 0xb8, 0x3b, 0x12, 0x73, 0x2e,
	}
	mainnetCOSTMissedRewardContract = tcommon.Address{
		0x41, 0x15, 0x9d, 0x09, 0x6a, 0xb1, 0x2d, 0x5d, 0x35, 0xeb, 0xf8,
		0x35, 0x79, 0x60, 0x32, 0x4f, 0x59, 0x8f, 0xd5, 0x4d, 0x16,
	}
	mainnetCOSTMissedRewardPayer = tcommon.Address{
		0x41, 0x78, 0x90, 0xbf, 0x5b, 0x42, 0x70, 0x64, 0xe7, 0x55, 0xd8,
		0x82, 0xb0, 0x1c, 0xa6, 0x6c, 0xfc, 0x2b, 0x86, 0x23, 0x63,
	}
	mainnetCOSTMissedRewardStorageSlot     = tcommon.HexToHash("0a")
	mainnetCOSTMissedRewardBadStorageValue = tcommon.HexToHash("42ff916e040")
	mainnetCOSTMissedRewardStorageValue    = tcommon.HexToHash("42ff9632b80")
	mainnetWINKMissingCodeRepairBlockID    = tcommon.HexToHash("00000000013b77633413e57c9a741dbcb6c6faf7ab1f261c1210a19d08d88b7d")
	mainnetWINKContract                    = tcommon.Address{
		0x41, 0x74, 0x47, 0x2e, 0x7d, 0x35, 0x39, 0x5a, 0x6b, 0x5a, 0xdd,
		0x42, 0x7e, 0xec, 0xb7, 0xf4, 0xb6, 0x2a, 0xd2, 0xb0, 0x71,
	}
	mainnetWINKCreationCodeHash = tcommon.HexToHash("6d6573f4e5bca2b3c691e783e3fb63a619d993602220158cf84996c4f166b834")
	mainnetWINKRuntimeCodeHash  = tcommon.HexToHash("b8b0efb7d4ff5ce4567d234b4bed40278f1955619f7e496fcff99aa20b6e1c08")
)

type missingRuntimeCodeRepairSpec struct {
	blockNum         uint64
	blockID          tcommon.Hash
	contract         tcommon.Address
	creationCodeHash tcommon.Hash
	runtimeCodeHash  tcommon.Hash
	runtimeOffset    int
	runtimeSize      int
}

// repairMainnetCreateTransferFailureOvercharge heals state materialized by a
// pre-fix gtron binary without affecting a clean replay. Mainnet block
// 12,897,681 tx 740da79d... was a top-level CreateSmartContract whose
// constructor returned TRANSFER_FAILED after 79,537 energy. The old create
// wrapper charged its entire 500,000-energy limit, over-debiting the payer by
// 4,204,630 sun while still accepting the block because contractRet matched.
//
// A fixed replay reaches the next observed block with the canonical 6,320,008
// balance and takes no action. Only the exact canonical block ID plus the exact
// legacy bad balance activates the repair. Applying it inside processBlock's
// snapshot makes a later block failure roll it back and retry atomically.
func repairMainnetCreateTransferFailureOvercharge(statedb *state.StateDB, blockNum uint64, blockID tcommon.Hash) bool {
	if statedb == nil || blockNum != mainnetCreateTransferFailureRepairBlock || blockID != mainnetCreateTransferFailureRepairBlockID {
		return false
	}
	if statedb.GetBalance(mainnetCreateTransferFailurePayer) != mainnetCreateTransferFailureBadBalance {
		return false
	}
	statedb.AddBalance(
		mainnetCreateTransferFailurePayer,
		mainnetCreateTransferFailureCanonicalBalance-mainnetCreateTransferFailureBadBalance,
	)
	return true
}

// repairMainnetParallelVMMissedPayment heals the one state materialized by the
// pre-fix speculative VM publisher. Mainnet block 18,402,304 tx
// 1f63de3942... called groupPayment and java-tron transferred 11,826,751 sun to
// the recipient. The speculative block-start view hydrated the entry contract
// with an empty code edge and published a zero-energy SUCCESS instead. Serial
// replay of the same transaction consumes 8,536 energy and performs the
// transfer. The drift becomes fatal at block 18,414,381 tx 6186f5fe1b... .
//
// A clean/fixed replay already has the canonical balance and takes no action.
// The exact block ID and exact legacy balance make the repair network-specific,
// idempotent, and inert for forks or independently modified state.
func repairMainnetParallelVMMissedPayment(statedb *state.StateDB, blockNum uint64, blockID tcommon.Hash) bool {
	if statedb == nil || blockNum != mainnetParallelVMMissedPaymentRepairBlock || blockID != mainnetParallelVMMissedPaymentRepairBlockID {
		return false
	}
	blackhole := statedb.BlackholeAddress()
	if statedb.GetBalance(mainnetParallelVMMissedPaymentRecipient) != mainnetParallelVMMissedPaymentBadBalance ||
		statedb.GetBalance(mainnetParallelVMMissedPaymentContract) != mainnetParallelVMMissedPaymentContractBalance ||
		statedb.GetBalance(mainnetParallelVMMissedPaymentPayer) != mainnetParallelVMMissedPaymentPayerBalance ||
		statedb.GetBalance(blackhole) != mainnetParallelVMMissedPaymentBlackholeBalance {
		return false
	}
	statedb.AddBalance(mainnetParallelVMMissedPaymentRecipient, mainnetParallelVMMissedPaymentAmount)
	statedb.AddBalance(mainnetParallelVMMissedPaymentContract, -mainnetParallelVMMissedPaymentAmount)
	statedb.AddBalance(mainnetParallelVMMissedPaymentPayer, -mainnetParallelVMMissedPaymentEnergyFee)
	statedb.AddSettlementBalance(blackhole, mainnetParallelVMMissedPaymentEnergyFee)
	return true
}

// repairMainnetCOSTMissedReward heals another state produced by the same
// pre-fix speculative VM publisher. Mainnet block 20,616,256 tx
// 0def93583b... called COST.reward and java-tron transferred 5,000,000 sun from
// the contract to the recipient, charged the payer 140,870 sun for 14,087
// energy, and added 5,000,000 to the contract's total-reward storage slot. The
// legacy gtron state instead contains a zero-energy SUCCESS receipt and none of
// those effects. The missing recipient balance becomes fatal at block
// 20,674,403 tx 59feaaa85d..., whose 5,000,000-sun call then has no balance left
// to buy energy and incorrectly returns OUT_OF_ENERGY.
//
// A clean replay already has the canonical state and takes no action. Requiring
// the canonical repair block ID and every legacy balance/storage pre-image
// keeps the repair mainnet-specific, atomic, and idempotent.
func repairMainnetCOSTMissedReward(statedb *state.StateDB, blockNum uint64, blockID tcommon.Hash) bool {
	if statedb == nil || blockNum != mainnetCOSTMissedRewardRepairBlock || blockID != mainnetCOSTMissedRewardRepairBlockID {
		return false
	}
	blackhole := statedb.BlackholeAddress()
	if statedb.GetBalance(mainnetCOSTMissedRewardRecipient) != mainnetCOSTMissedRewardRecipientBadBalance ||
		statedb.GetBalance(mainnetCOSTMissedRewardContract) != mainnetCOSTMissedRewardContractBalance ||
		statedb.GetBalance(mainnetCOSTMissedRewardPayer) != mainnetCOSTMissedRewardPayerBalance ||
		statedb.GetBalance(blackhole) != mainnetCOSTMissedRewardBlackholeBalance ||
		statedb.GetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot) != mainnetCOSTMissedRewardBadStorageValue {
		return false
	}
	statedb.AddBalance(mainnetCOSTMissedRewardRecipient, mainnetCOSTMissedRewardAmount)
	statedb.AddBalance(mainnetCOSTMissedRewardContract, -mainnetCOSTMissedRewardAmount)
	statedb.AddBalance(mainnetCOSTMissedRewardPayer, -mainnetCOSTMissedRewardEnergyFee)
	statedb.AddSettlementBalance(blackhole, mainnetCOSTMissedRewardEnergyFee)
	statedb.SetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot, mainnetCOSTMissedRewardStorageValue)
	return true
}

// repairMissingRuntimeCodeFromMetadata restores an immutable code blob only
// when the account already references the expected runtime hash and its exact
// creation bytecode is still present in canonical contract metadata. SetCode
// leaves the account's consensus-visible code hash unchanged; committing the
// block merely restores the missing content-addressed code row.
func repairMissingRuntimeCodeFromMetadata(statedb *state.StateDB, blockNum uint64, blockID tcommon.Hash, spec missingRuntimeCodeRepairSpec) bool {
	if statedb == nil || blockNum != spec.blockNum || blockID != spec.blockID ||
		spec.runtimeOffset < 0 || spec.runtimeSize <= 0 {
		return false
	}
	if statedb.GetCodeHash(spec.contract) != spec.runtimeCodeHash || len(statedb.GetCode(spec.contract)) != 0 {
		return false
	}
	meta := statedb.GetContract(spec.contract)
	if meta == nil {
		return false
	}
	creationCode := meta.GetBytecode()
	runtimeEnd := spec.runtimeOffset + spec.runtimeSize
	if runtimeEnd < spec.runtimeOffset || runtimeEnd > len(creationCode) ||
		tcommon.Keccak256(creationCode) != spec.creationCodeHash {
		return false
	}
	runtimeCode := creationCode[spec.runtimeOffset:runtimeEnd]
	if tcommon.Keccak256(runtimeCode) != spec.runtimeCodeHash {
		return false
	}
	statedb.SetCode(spec.contract, runtimeCode)
	return true
}

// repairMainnetWINKMissingRuntimeCode restores the WINK token runtime required
// by mainnet block 20,674,403 tx e6fc287654... . The canonical transaction has
// empty calldata, reaches the contract fallback, consumes 43 energy, and
// returns REVERT. The legacy database retains the canonical account code hash
// and the exact 7,969-byte creation code in SmartContract metadata, but the
// referenced 6,887-byte immutable runtime blob is absent, so an empty-code call
// incorrectly returns SUCCESS.
//
// The canonical block ID plus both code hashes make this repair inert for a
// clean database, a fork, different metadata, or a retry after successful
// persistence. It runs inside processBlock's snapshot and therefore rolls back
// atomically if a later transaction still rejects the block.
func repairMainnetWINKMissingRuntimeCode(statedb *state.StateDB, blockNum uint64, blockID tcommon.Hash) bool {
	return repairMissingRuntimeCodeFromMetadata(statedb, blockNum, blockID, missingRuntimeCodeRepairSpec{
		blockNum:         mainnetWINKMissingCodeRepairBlock,
		blockID:          mainnetWINKMissingCodeRepairBlockID,
		contract:         mainnetWINKContract,
		creationCodeHash: mainnetWINKCreationCodeHash,
		runtimeCodeHash:  mainnetWINKRuntimeCodeHash,
		runtimeOffset:    mainnetWINKRuntimeOffset,
		runtimeSize:      mainnetWINKRuntimeSize,
	})
}

// applyMainnetLegacyStateRepairs makes every one-off state surgery observable
// and asks the production block path to durably disqualify the datadir. A node
// that activates any repair may continue serially for recovery purposes, but
// it did not perform a clean replay and must never pass the speculative
// execution release gate. Exact pre-images in each repair keep this wrapper
// inert for a fixed replay.
func applyMainnetLegacyStateRepairs(
	statedb *state.StateDB,
	blockNum uint64,
	blockID tcommon.Hash,
	hook func(rawdb.ExecutionSafetyIncident) error,
) (bool, error) {
	activated := false
	record := func(name string, kind rawdb.ExecutionSafetyIncidentKind, counter interface{ Inc(int64) }) error {
		activated = true
		counter.Inc(1)
		log.Error("Legacy mainnet state repair activated",
			"repair", name,
			"block", blockNum,
			"hash", blockID,
			"releaseEligible", false,
			"action", "rebuild-from-trusted-pre-divergence-state",
		)
		if hook != nil {
			if err := hook(rawdb.ExecutionSafetyIncident{Kind: kind, BlockNum: blockNum, BlockHash: blockID}); err != nil {
				return fmt.Errorf("persist %s activation: %w", name, err)
			}
		}
		return nil
	}
	if repairMainnetCreateTransferFailureOvercharge(statedb, blockNum, blockID) {
		if err := record("create-transfer-failure-overcharge", rawdb.ExecutionSafetyIncidentCreateTransferRepair, mainnetCreateTransferFailureRepairCounter); err != nil {
			return true, err
		}
	}
	if repairMainnetParallelVMMissedPayment(statedb, blockNum, blockID) {
		if err := record("parallel-vm-missed-payment", rawdb.ExecutionSafetyIncidentParallelVMRepair, mainnetParallelVMMissedPaymentRepairCounter); err != nil {
			return true, err
		}
	}
	if repairMainnetCOSTMissedReward(statedb, blockNum, blockID) {
		if err := record("cost-missed-reward", rawdb.ExecutionSafetyIncidentCOSTRepair, mainnetCOSTMissedRewardRepairCounter); err != nil {
			return true, err
		}
	}
	if repairMainnetWINKMissingRuntimeCode(statedb, blockNum, blockID) {
		if err := record("wink-missing-runtime", rawdb.ExecutionSafetyIncidentWINKCodeRepair, mainnetWINKMissingCodeRepairCounter); err != nil {
			return true, err
		}
	}
	return activated, nil
}

// ApplyTransaction executes a single transaction against the given state.
// Returns the full actuator Result including fee, energy, net, and contract details.
//
// Validation flags:
//   - validate         → run actuator.Validate before Execute (state preconditions)
//   - validateEnvelope → run ValidateTxEnvelope (signature + permission) before
//     anything mutates state. Runs inside the per-tx position rather than as a
//     pre-block sweep so that the statedb reflects prior intra-block effects,
//     matching java-tron Manager.processBlock's interleaved validation. The
//     concrete case it covers: a single block holding an
//     AccountPermissionUpdateContract followed by a TransferContract signed
//     with the post-rotation keys — the transfer's signer is only present in
//     the post-update permission set, so envelope check must see the
//     just-mutated state.
//
// Both flags are independent so tests can keep validate=true / envelope=false
// for unsigned-tx fixtures without sacrificing actuator coverage.
//
// The db parameter accepts either an `ethdb.KeyValueStore` (BuildBlock path)
// or `core/blockbuffer.Buffer` (applyBlock path) — slice 3 of the fork-rewind
// fix widened the type so actuator-side rawdb-direct writes are rewindable.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, HeadSlot(prevBlockTime, 0), blockTime, blockNum, db, activeWitnesses, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false, nil, nil)
}

// ApplyTransactionWithResourceSlot executes a transaction with java-tron's
// resource-window time (`head slot`) separated from millisecond timestamps.
func ApplyTransactionWithResourceSlot(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, headSlot, blockTime, blockNum, db, activeWitnesses, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false, nil, nil)
}

func ApplyTransactionWithResourceSlotAndEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, energyLimitForkBlockNum int64, validate, validateEnvelope bool) (*actuator.Result, error) {
	return applyTransaction(statedb, dynProps, tx, prevBlockTime, true, headSlot, blockTime, blockNum, db, activeWitnesses, energyLimitForkBlockNum, tcommon.Hash{}, tcommon.Address{}, validate, validateEnvelope, false, nil, nil)
}

// applyTransactionScratch owns the per-block actuator objects whose contents
// are consumed synchronously before processBlock advances to the next tx.
type applyTransactionScratch struct {
	context                  actuator.Context
	result                   actuator.Result
	dynamicPropertiesChanged bool
}

func applyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime int64, hasHeadSlot bool, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, energyLimitForkBlockNum int64, genesisHash tcommon.Hash, coinbase tcommon.Address, validate, validateEnvelope bool, trustTransactionRet bool, forkPassCache *forks.VersionPassCache, tracer vm.Tracer) (result *actuator.Result, err error) {
	return applyTransactionWithScratch(statedb, dynProps, tx, prevBlockTime, hasHeadSlot, headSlot, blockTime, blockNum, db, activeWitnesses, energyLimitForkBlockNum, genesisHash, coinbase, validate, validateEnvelope, trustTransactionRet, forkPassCache, tracer, nil, nil, nil)
}

func applyTransactionWithScratch(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, prevBlockTime int64, hasHeadSlot bool, headSlot, blockTime int64, blockNum uint64, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, energyLimitForkBlockNum int64, genesisHash tcommon.Hash, coinbase tcommon.Address, validate, validateEnvelope bool, trustTransactionRet bool, forkPassCache *forks.VersionPassCache, tracer vm.Tracer, scratch *applyTransactionScratch, internalTxArena *vm.InternalTransactionArena, executionLogArena *vm.ExecutionLogArena) (result *actuator.Result, err error) {
	if scratch != nil {
		scratch.dynamicPropertiesChanged = false
	}
	var revertOnOverflow func()
	defer func() {
		if recovered := recover(); recovered != nil {
			if overflow, ok := tcommon.ArithmeticOverflowFromPanic(recovered); ok {
				if revertOnOverflow != nil {
					revertOnOverflow()
				}
				result = nil
				err = overflow
				return
			}
			panic(recovered)
		}
	}()
	stateFailure := func(stage string) error {
		if stateErr := statedb.Error(); stateErr != nil {
			return fmt.Errorf("%s: rooted state access failed: %w", stage, stateErr)
		}
		return nil
	}
	if err := stateFailure("transaction start"); err != nil {
		return nil, err
	}
	if err := ValidateContractCount(tx); err != nil {
		return nil, err
	}
	if forkPassCache == nil {
		forkPassCache = forks.NewVersionPassCache()
	}

	// Block-apply reject for ExchangeTransactionContract once VERSION_4_8_0_1
	// activates. Mirrors java-tron Manager.processBlock's per-tx
	// rejectExchangeTransaction call (master 45e3bf88ca). Nile had already
	// assigned wire version 33 to its earlier release-v4.8.1 deployment when
	// upstream later introduced VERSION_4_8_0_1 as 33. Nile therefore rolled
	// the exchange-disable feature out under version 34; treating its old v33
	// blocks as VERSION_4_8_0_1 rejects canonical pre-rollout transactions
	// (first observed at Nile block 63,172,360). Mainnet keeps upstream's v33
	// gate. java-tron evaluates the gate against the previous block timestamp.
	exchangeRejected := forkPassCache.Pass(statedb, 33, prevBlockTime, dynProps.MaintenanceTimeInterval())
	if genesisHash == params.NileGenesisHash {
		// Nile wire v34 is VERSION_4_8_0_1 at a 70% quorum; mainnet wire
		// v34 is VERSION_4_8_1 at 80%. The common version table describes
		// mainnet, so evaluate this Nile-only gate with its deployed rate.
		exchangeRejected = forks.PassVersionFromStoreWithRate(statedb, 34, prevBlockTime, dynProps.MaintenanceTimeInterval(), 70)
	}
	if tx.ContractType() == corepb.Transaction_Contract_ExchangeTransactionContract && exchangeRejected {
		return nil, ErrExchangeRejected
	}
	// java-tron Manager.validateCommon applies the synthetic "clear ret +
	// two MAX_RESULT_SIZE_IN_TX slots" size gate to pending transactions,
	// but only applies it to in-block transactions after
	// consensus_logic_optimization. Older mainnet blocks can otherwise fail
	// replay even though their actual protobuf bytes are below 500 KiB.
	validateResultSize := !trustTransactionRet || dynProps.ConsensusLogicOptimization()
	wireSizes, err := validateTxCommonWithSizes(tx, prevBlockTime, validateResultSize)
	if err != nil {
		return nil, err
	}
	// java Manager.validateCommon adds an in-block expiration LOWER bound once
	// consensus_logic_optimization is active: the tx must not already be expired
	// as of the next block slot. nextSlotTime = latestBlockHeaderTimestamp +
	// slotCount*BLOCK_INTERVAL, slotCount = 1 (+ MaintenanceSkipSlots when the head
	// was a maintenance block, StateFlag==1). During in-block validation both impls
	// read the head's (prev block's) timestamp + state flag, so prevBlockTime +
	// dynProps.StateFlag() here match java's getNextBlockSlotTime. Canonical blocks
	// never contain a sub-slot-expiration tx (java rejects at produce), so this only
	// adds java's reject of a non-canonical block.
	if dynProps.ConsensusLogicOptimization() {
		slotCount := int64(1)
		if dynProps.StateFlag() == 1 {
			slotCount += int64(params.MaintenanceSkipSlots)
		}
		if tx.Expiration() < prevBlockTime+slotCount*params.BlockProducedInterval {
			return nil, ErrTransactionExpiration
		}
	}

	// java BandwidthProcessor.consume always rejects (in-block too) a tx whose
	// serialized result exceeds MAX_RESULT_SIZE_IN_TX(64) per contract —
	// getResultSerializedSize() = sum of each Result's serialized size. gtron
	// strips ret for the 500KB byte-size gate but lacked this per-ret guard.
	// proto.Size(r) == java result.getSerializedSize() for the same message, and
	// canonical blocks always satisfy it (java enforces at produce), so this only
	// rejects a crafted block carrying an oversized ret.
	{
		retPB := tx.Proto()
		if int64(wireSizes.results) > maxResultSizeInTx*int64(len(retPB.GetRawData().GetContract())) {
			return nil, ErrTransactionResultTooLarge
		}
	}

	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return nil, fmt.Errorf("create actuator: %w", err)
	}

	var ctx *actuator.Context
	var resultSink *actuator.Result
	if scratch == nil {
		ctx = new(actuator.Context)
	} else {
		ctx = &scratch.context
		resultSink = &scratch.result
	}
	*ctx = actuator.Context{
		State:                      statedb,
		DynProps:                   dynProps,
		Tx:                         tx,
		BlockTime:                  blockTime,
		PrevBlockTime:              prevBlockTime,
		HeadSlot:                   headSlot,
		HasHeadSlot:                hasHeadSlot,
		BlockNumber:                blockNum,
		Coinbase:                   coinbase,
		GenesisHash:                genesisHash,
		EnergyLimitForkBlockNum:    energyLimitForkBlockNum,
		HasEnergyLimitForkBlockNum: true,
		DB:                         db,
		ActiveWitnesses:            activeWitnesses,
		TrustTransactionRet:        trustTransactionRet,
		ForkPassCache:              forkPassCache,
		ResultSink:                 resultSink,
		InternalTransactionArena:   internalTxArena,
		ExecutionLogArena:          executionLogArena,
		Tracer:                     tracer,
	}

	if validateEnvelope {
		// VERSION_4_7_1 (value 27): java-tron swapped the multi-sig dedup
		// key from raw signature bytes to recovered address. We mirror by
		// passing the fork-pass result through.
		multiSigByAddress := forkPassCache.Pass(statedb, 27, prevBlockTime, dynProps.MaintenanceTimeInterval())
		if err := ValidateTxEnvelope(tx, statedb, multiSigByAddress, dynProps); err != nil {
			return nil, fmt.Errorf("validate envelope: %w", err)
		}
		// TAPOS read goes through the same buffered db that landed
		// previous-block writes. When applyBlock is mid-replay for block
		// N, this sees the ring as of block N-1 — which is exactly what
		// java-tron uses for the tapos check (ref blocks must precede the
		// referencing block).
		if err := ValidateTAPOS(tx, db); err != nil {
			return nil, fmt.Errorf("validate tapos: %w", err)
		}
	}

	if validate {
		validateErr := act.Validate(ctx)
		if err := stateFailure("validate"); err != nil {
			return nil, err
		}
		if validateErr != nil {
			return nil, fmt.Errorf("validate: %w", validateErr)
		}
	}

	txSnap := statedb.Snapshot()
	dpSnap := dynProps.Snapshot()
	revertTx := func() {
		statedb.RevertToSnapshot(txSnap)
		dynProps.RevertToSnapshot(dpSnap)
	}
	revertOnOverflow = revertTx

	bwResult, err := consumeBandwidthWithResourceTimeAndSizes(statedb, dynProps, tx, prevBlockTime, ctx.ResourceTime(), wireSizes)
	if stateErr := stateFailure("bandwidth"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("bandwidth: %w", err)
	}

	multiSignFee, err := actuator.ConsumeMultiSignFee(ctx)
	if stateErr := stateFailure("multi-sign fee"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("multi-sign fee: %w", err)
	}
	memoFee, err := actuator.ConsumeMemoFee(ctx)
	if stateErr := stateFailure("memo fee"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("memo fee: %w", err)
	}

	result, err = act.Execute(ctx)
	if stateErr := stateFailure("execute"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}
	if err != nil {
		revertTx()
		return nil, fmt.Errorf("execute: %w", err)
	}

	// Settle the energy bill after Execute returns, mirroring java-tron's
	// VMActuator -> TransactionTrace.pay -> ReceiptCapsule.payEnergyBill
	// chain. For non-TVM actuators result.EnergyUsageTotal is zero and
	// PayEnergyBill is a no-op; for VMActuator it does the stake/balance
	// split, debits the caller, and routes the bill (transactionFeePool /
	// burn_trx_amount / blackhole). Failures here are unwound by
	// reverting to the pre-Execute snapshot — keeps state consistent if
	// the caller doesn't have enough TRX to cover the overage. Mirrors
	// java's BalanceInsufficientException re-throw at line 299 of
	// ReceiptCapsule.java.
	if err := actuator.PayEnergyBill(ctx, result); err != nil {
		revertTx()
		return nil, fmt.Errorf("pay energy bill: %w", err)
	}
	if stateErr := stateFailure("pay energy bill"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}

	result.NetUsage = bwResult.NetUsage
	result.NetFee = bwResult.NetFee
	result.NetFeeForBandwidth = bwResult.NetFeeForBandwidth
	result.Fee += multiSignFee + memoFee

	if scratch != nil {
		scratch.dynamicPropertiesChanged = dynProps.SnapshotChanged(dpSnap)
	}
	if stateErr := stateFailure("transaction finalize"); stateErr != nil {
		revertTx()
		return nil, stateErr
	}
	dynProps.CommitSnapshot(dpSnap)
	return result, nil
}

// transactionInfoSlot keeps the fixed-size pieces of one transaction receipt
// in one allocation. Each slot owns its ID and repeated-contract-result cell,
// preserving per-TransactionInfo mutation isolation.
type transactionInfoSlot struct {
	info              corepb.TransactionInfo
	receipt           corepb.ResourceReceipt
	id                tcommon.Hash
	contractAddress   [tcommon.AddressLength]byte
	contractResult    [1][]byte
	internalTxArena   vm.InternalTransactionArena
	executionLogArena vm.ExecutionLogArena
	logs              []*transactionInfoLogSlot
	logPointers       []*corepb.TransactionInfo_Log
}

// transactionInfoLogSlot owns the stable-address protobuf shell and slice
// headers for one receipt log. transactionInfoSlot is recycled only after
// async metadata serialization completes, so these can be reused without
// crossing ownership boundaries while the VM-owned topic/data bytes remain
// borrowed as before. The parent stores pointers so slice growth never copies
// a protobuf message after its internal message state has been used.
type transactionInfoLogSlot struct {
	log     corepb.TransactionInfo_Log
	address [tcommon.AddressLength]byte
	topics  [][]byte
}

// transactionInfoBatch owns the contiguous receipt storage for one block.
// Canonical range execution may recycle a batch after the metadata writer has
// consumed every TransactionInfo; public ProcessBlock callers keep the
// allocation-per-call ownership contract by passing no batch.
type transactionInfoBatch struct {
	slots []transactionInfoSlot
	infos []*corepb.TransactionInfo
}

func (b *transactionInfoBatch) prepare(n int) ([]transactionInfoSlot, []*corepb.TransactionInfo) {
	if n == 0 {
		b.slots = b.slots[:0]
		b.infos = b.infos[:0]
		return b.slots, b.infos
	}
	if cap(b.slots) < n {
		newCap := n
		if grown := cap(b.slots) + cap(b.slots)/2; grown > newCap {
			newCap = grown
		}
		if newCap < 16 {
			newCap = 16
		}
		b.slots = make([]transactionInfoSlot, n, newCap)
		b.infos = make([]*corepb.TransactionInfo, n, newCap)
	} else {
		b.slots = b.slots[:n]
		b.infos = b.infos[:n]
	}
	return b.slots, b.infos
}

// transactionInfoBatchPool is bounded by the commit pipeline depth. A batch
// handed to async commit is returned only after the worker has serialized its
// infos, so foreground execution can never overwrite worker-owned protobufs.
type transactionInfoBatchPool struct {
	free chan *transactionInfoBatch
}

func newTransactionInfoBatchPool(depth int) *transactionInfoBatchPool {
	if depth < 1 {
		depth = 1
	}
	return &transactionInfoBatchPool{free: make(chan *transactionInfoBatch, depth)}
}

func (p *transactionInfoBatchPool) acquire() *transactionInfoBatch {
	select {
	case batch := <-p.free:
		return batch
	default:
		return new(transactionInfoBatch)
	}
}

func (p *transactionInfoBatchPool) release(batch *transactionInfoBatch) {
	if p == nil || batch == nil {
		return
	}
	select {
	case p.free <- batch:
	default:
		// The pool is only a reuse hint. Dropping an unexpected surplus keeps
		// retention bounded without adding synchronization to block execution.
	}
}

// buildTransactionInfo constructs a TransactionInfo. processBlock uses
// transactionInfoSlot.build with a block-sized slot array whose log and
// internal-transaction arenas remain owned until metadata serialization has
// completed; standalone callers keep their supplied immutable result payloads.
func buildTransactionInfo(tx *types.Transaction, result *actuator.Result, blockNum uint64, blockTime int64, supportTransactionFeePool bool) *corepb.TransactionInfo {
	return new(transactionInfoSlot).build(tx, result, blockNum, blockTime, supportTransactionFeePool)
}

func (slot *transactionInfoSlot) build(tx *types.Transaction, result *actuator.Result, blockNum uint64, blockTime int64, supportTransactionFeePool bool) *corepb.TransactionInfo {
	// Break a prior occupant's result reference even when the new transaction
	// does not publish ContractResult. The full info assignment below clears all
	// other pointer-bearing protobuf fields before they are rebuilt.
	slot.contractResult[0] = nil
	slot.id = tx.Hash()
	isVMContract := isVMContractType(tx.ContractType())

	// Receipt fields mirror java-tron `Protocol.ResourceReceipt`: EnergyUsage
	// is the stake-funded portion only (proto field 1), EnergyUsageTotal is
	// the full VM energy spent (proto field 4) and EnergyFee is the
	// balance-paid bill in SUN (proto field 2). The split between
	// EnergyUsed/EnergyFee is set by actuator.PayEnergyBill.
	slot.receipt = corepb.ResourceReceipt{
		EnergyUsage:        result.EnergyUsed,
		EnergyFee:          result.EnergyFee,
		OriginEnergyUsage:  result.OriginEnergyUsage,
		EnergyUsageTotal:   result.EnergyUsageTotal,
		NetUsage:           result.NetUsage,
		NetFee:             result.NetFee,
		EnergyPenaltyTotal: result.EnergyPenaltyTotal,
	}
	info := &slot.info
	*info = corepb.TransactionInfo{
		Id:             slot.id[:],
		Fee:            result.Fee + result.NetFee,
		BlockNumber:    int64(blockNum),
		BlockTimeStamp: blockTime,
		Receipt:        &slot.receipt,
	}
	if isVMContract {
		info.Receipt.Result = corepb.Transaction_ResultContractResult(result.ContractRet)
	}
	if supportTransactionFeePool {
		if result.NetFeeForBandwidth {
			info.PackingFee += result.NetFee
		}
		if corepb.Transaction_ResultContractResult(result.ContractRet) != corepb.Transaction_Result_OUT_OF_TIME {
			info.PackingFee += result.EnergyFee
		}
	}

	if result.ContractResultPresent || len(result.ContractResult) > 0 {
		slot.contractResult[0] = result.ContractResult
		info.ContractResult = slot.contractResult[:]
	} else if !isVMContract && result.ContractRet == int32(corepb.Transaction_Result_SUCCESS) {
		slot.contractResult[0] = []byte{}
		info.ContractResult = slot.contractResult[:]
	}

	if len(result.ContractAddress) > 0 {
		addressLen := copy(slot.contractAddress[:], result.ContractAddress)
		info.ContractAddress = slot.contractAddress[:addressLen:addressLen]
	}
	if result.AssetIssueID != "" {
		info.AssetIssueID = result.AssetIssueID
	}
	if result.WithdrawAmount != 0 {
		info.WithdrawAmount = result.WithdrawAmount
	}
	if result.UnfreezeAmount != 0 {
		info.UnfreezeAmount = result.UnfreezeAmount
	}
	if result.WithdrawExpireAmount != 0 {
		info.WithdrawExpireAmount = result.WithdrawExpireAmount
	}
	if len(result.CancelUnfreezeV2Amount) > 0 {
		info.CancelUnfreezeV2Amount = result.CancelUnfreezeV2Amount
	}
	if result.ExchangeReceivedAmount != 0 {
		info.ExchangeReceivedAmount = result.ExchangeReceivedAmount
	}
	if result.ExchangeInjectAnotherAmount != 0 {
		info.ExchangeInjectAnotherAmount = result.ExchangeInjectAnotherAmount
	}
	if result.ExchangeWithdrawAnotherAmount != 0 {
		info.ExchangeWithdrawAnotherAmount = result.ExchangeWithdrawAnotherAmount
	}
	if result.ShieldedTransactionFee != 0 {
		info.ShieldedTransactionFee = result.ShieldedTransactionFee
	}
	if result.ExchangeID != 0 {
		info.ExchangeId = result.ExchangeID
	}
	if len(result.OrderID) > 0 {
		info.OrderId = result.OrderID
	}
	if len(result.OrderDetails) > 0 {
		info.OrderDetails = result.OrderDetails
	}

	slot.prepareLogs(len(result.Logs))
	for i := range result.Logs {
		l := &result.Logs[i]
		logSlot := slot.logs[i]
		pbLog := &logSlot.log
		*pbLog = corepb.TransactionInfo_Log{
			Address: logSlot.setAddress(l.Address),
			Data:    l.Data,
		}
		pbLog.Topics = logSlot.setTopics(l)
		slot.logPointers[i] = pbLog
	}
	if len(slot.logPointers) > 0 {
		info.Log = slot.logPointers[:len(slot.logPointers):len(slot.logPointers)]
	}
	// java-tron persists ProgramResult.internalTransactions in TransactionInfo
	// only when its node-local saveInternalTx option is enabled. The execution
	// arena is owned by this
	// transactionInfoSlot and the batch is not recycled until synchronous or
	// asynchronous metadata serialization completes, so no deep copy is needed
	// on the canonical hot path. Cap the view to prevent append from reaching
	// arena-owned spare entries.
	if len(result.InternalTransactions) > 0 {
		info.InternalTransactions = result.InternalTransactions[:len(result.InternalTransactions):len(result.InternalTransactions)]
	}
	if result.ContractRet > 1 {
		info.Result = corepb.TransactionInfo_FAILED
		if len(result.ResMessage) > 0 {
			info.ResMessage = result.ResMessage
		}
	}

	return info
}

func buildTransactionResult(result *actuator.Result) *corepb.Transaction_Result {
	ret := &corepb.Transaction_Result{
		Ret:         corepb.Transaction_Result_SUCESS,
		ContractRet: corepb.Transaction_ResultContractResult(result.ContractRet),
	}
	if result.AssetIssueID != "" {
		ret.AssetIssueID = result.AssetIssueID
	}
	if result.WithdrawAmount != 0 {
		ret.WithdrawAmount = result.WithdrawAmount
	}
	if result.UnfreezeAmount != 0 {
		ret.UnfreezeAmount = result.UnfreezeAmount
	}
	if result.WithdrawExpireAmount != 0 {
		ret.WithdrawExpireAmount = result.WithdrawExpireAmount
	}
	if len(result.CancelUnfreezeV2Amount) > 0 {
		ret.CancelUnfreezeV2Amount = result.CancelUnfreezeV2Amount
	}
	if result.ExchangeReceivedAmount != 0 {
		ret.ExchangeReceivedAmount = result.ExchangeReceivedAmount
	}
	if result.ExchangeInjectAnotherAmount != 0 {
		ret.ExchangeInjectAnotherAmount = result.ExchangeInjectAnotherAmount
	}
	if result.ExchangeWithdrawAnotherAmount != 0 {
		ret.ExchangeWithdrawAnotherAmount = result.ExchangeWithdrawAnotherAmount
	}
	if result.ShieldedTransactionFee != 0 {
		ret.ShieldedTransactionFee = result.ShieldedTransactionFee
	}
	if result.ExchangeID != 0 {
		ret.ExchangeId = result.ExchangeID
	}
	if len(result.OrderID) > 0 {
		ret.OrderId = result.OrderID
	}
	if len(result.OrderDetails) > 0 {
		ret.OrderDetails = result.OrderDetails
	}
	return ret
}

func isVMContractType(contractType corepb.Transaction_Contract_ContractType) bool {
	return contractType == corepb.Transaction_Contract_CreateSmartContract ||
		contractType == corepb.Transaction_Contract_TriggerSmartContract
}

func (slot *transactionInfoLogSlot) setAddress(addr tcommon.Address) []byte {
	if addr[0] == tcommon.AddressPrefixMainnet {
		copy(slot.address[:tcommon.AccountIDLength], addr[1:])
		return slot.address[:tcommon.AccountIDLength:tcommon.AccountIDLength]
	}
	copy(slot.address[:], addr[:])
	return slot.address[:tcommon.AddressLength:tcommon.AddressLength]
}

func (slot *transactionInfoLogSlot) setTopics(log *vm.Log) [][]byte {
	topicCount := log.TopicCount()
	oldLen := len(slot.topics)
	if cap(slot.topics) < topicCount {
		slot.topics = make([][]byte, topicCount)
	} else {
		if topicCount < oldLen {
			clear(slot.topics[topicCount:oldLen])
		}
		slot.topics = slot.topics[:topicCount]
	}
	for i := range topicCount {
		slot.topics[i] = log.Topic(i)
	}
	if len(slot.topics) > 0 {
		return slot.topics[:len(slot.topics):len(slot.topics)]
	}
	return nil
}

func (slot *transactionInfoSlot) prepareLogs(n int) {
	oldLogs := len(slot.logs)
	for i := n; i < oldLogs; i++ {
		slot.logs[i].log = corepb.TransactionInfo_Log{}
		clear(slot.logs[i].topics)
		slot.logs[i].topics = slot.logs[i].topics[:0]
	}
	if cap(slot.logs) < n {
		old := slot.logs[:cap(slot.logs)]
		slot.logs = make([]*transactionInfoLogSlot, n)
		copy(slot.logs, old)
	} else {
		slot.logs = slot.logs[:n]
	}
	for i := oldLogs; i < n; i++ {
		if slot.logs[i] == nil {
			slot.logs[i] = new(transactionInfoLogSlot)
		}
	}
	oldPointers := len(slot.logPointers)
	if cap(slot.logPointers) < n {
		slot.logPointers = make([]*corepb.TransactionInfo_Log, n)
	} else {
		if n < oldPointers {
			clear(slot.logPointers[n:oldPointers])
		}
		slot.logPointers = slot.logPointers[:n]
	}
}

// ProcessBlock executes all transactions in a block and pays the block reward.
// It does NOT commit state — the caller (InsertBlock/BuildBlock) is responsible
// for committing after any post-processing (e.g., maintenance).
// Returns the TransactionInfos for all executed transactions.
//
// validateEnvelope toggles per-tx signature/permission verification inside
// the tx loop. Production callers (BlockChain.applyBlock when the engine
// is wired) pass true; test fixtures that bypass envelope checks pass false.
//
// The db parameter carries non-rooted chain/runtime data visible during
// execution, such as TAPOS references and genesis witness metadata. Mutable
// state writes go through StateDB typed stores.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, validateEnvelope bool, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, params.DefaultBlockNumForEnergyLimit, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil, nil, nil, true, -1, nil)
	return txInfos, err
}

func ProcessBlockWithJavaAccountStateRoot(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, validateEnvelope bool, parentAccountStateRoot tcommon.Hash, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, tcommon.Hash, error) {
	return processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, params.DefaultBlockNumForEnergyLimit, validateEnvelope, optionalGenesisHash(genesisHashOpt), &parentAccountStateRoot, nil, nil, nil, nil, true, -1, nil)
}

func ProcessBlockWithEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil, nil, nil, true, -1, nil)
	return txInfos, err
}

func ProcessBlockWithJavaAccountStateRootAndEnergyFork(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, parentAccountStateRoot tcommon.Hash, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, tcommon.Hash, error) {
	return processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), &parentAccountStateRoot, nil, nil, nil, nil, true, -1, nil)
}

// ProcessBlockTraced re-executes block against the supplied (copied) PARENT
// post-state, installing tracer only on the transaction at traceTxIndex. It is
// the debug_traceTransaction replay entry point: it does NOT commit, and the
// tracer captures just the target tx's opcode/call stream (every other tx runs
// with a nil tracer at zero overhead). genesisHash is optional.
func ProcessBlockTraced(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, forkPassCache *forks.VersionPassCache, traceTxIndex int, tracer vm.Tracer, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil, forkPassCache, nil, true, traceTxIndex, tracer)
	return txInfos, err
}

func processBlockTracedEach(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, forkPassCache *forks.VersionPassCache, tracerForTx func(index int, tx *types.Transaction) vm.Tracer, genesisHashOpt ...tcommon.Hash) ([]*corepb.TransactionInfo, error) {
	txInfos, _, err := processBlock(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, optionalGenesisHash(genesisHashOpt), nil, nil, nil, forkPassCache, nil, true, -1, nil, tracerForTx)
	return txInfos, err
}

func optionalGenesisHash(values []tcommon.Hash) tcommon.Hash {
	if len(values) == 0 {
		return tcommon.Hash{}
	}
	return values[0]
}

type processBlockOptions struct {
	parallelTransfers             bool
	parallelVM                    bool
	captureBalanceTrace           bool
	minParallelTransferCandidates int
	timing                        *processBlockTiming
	// Test-only hooks exercise both sides of the publication mutation boundary.
	// Production always leaves them nil.
	speculativePreOracleTestHook   func(family string, txIndex int, result *discardShadowTaskResult)
	speculativePostOracleTestHook  func(family string, txIndex int, result *discardShadowTaskResult)
	speculativePreApplyTestHook    func(family string, txIndex int, writes state.TransactionWriteSet)
	speculativePostApplyTestHook   func(family string, txIndex int, writes state.TransactionWriteSet)
	legacyStateRepairHook          func(rawdb.ExecutionSafetyIncident) error
	saveInternalTx                 bool
	saveFeaturedInternalTx         bool
	saveCancelAllUnfreezeV2Details bool
}

// errSpeculativePublicationAudit marks a publication attempt that cannot
// safely continue, including a post-preflight apply failure or an invariant
// discovered after canonical state was touched. The processBlock snapshot
// rolls that attempt back; BlockChain then opens its sticky safety circuit and
// retries the complete block with both speculative publishers disabled.
var errSpeculativePublicationAudit = errors.New("speculative publication safety audit failed")

// processBlockTiming contains nested, diagnostic-only slices of applyBlock's
// Execute phase. It is deliberately passed through processBlockOptions so
// replay/debug callers pay no allocation and consensus behavior is untouched.
type processBlockTiming struct {
	Transactions       time.Duration
	VMTransactions     int
	NativeTransactions int
	VMExecution        time.Duration
	VMRawEnergyUsage   int64
	AccountStateRoot   time.Duration
	AdaptiveEnergy     time.Duration
	Rewards            time.Duration
}

func (t *processBlockTiming) observeTransactionType(contractType corepb.Transaction_Contract_ContractType) {
	if t == nil {
		return
	}
	if isVMContractType(contractType) {
		t.VMTransactions++
	} else {
		t.NativeTransactions++
	}
}

func (t *processBlockTiming) addVMExecution(duration time.Duration, rawEnergy int64) {
	if t == nil {
		return
	}
	t.VMExecution += duration
	t.VMRawEnergyUsage += rawEnergy
}

func processBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, genesisHash tcommon.Hash, parentAccountStateRoot *tcommon.Hash, standbyPaySet *standbyWitnessPaySet, domainChanges *state.DomainChangeStage, forkPassCache *forks.VersionPassCache, txInfoBatch *transactionInfoBatch, collectTxInfos bool, traceTxIndex int, traceTracer vm.Tracer, traceForTxOpt ...func(index int, tx *types.Transaction) vm.Tracer) (txInfos []*corepb.TransactionInfo, javaAccountStateRoot tcommon.Hash, err error) {
	return processBlockWithOptions(statedb, dynProps, block, db, activeWitnesses, genesisTimestamp, energyLimitForkBlockNum, validateEnvelope, genesisHash, parentAccountStateRoot, standbyPaySet, domainChanges, forkPassCache, txInfoBatch, collectTxInfos, traceTxIndex, traceTracer, processBlockOptions{}, traceForTxOpt...)
}

func processBlockWithOptions(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block, db actuator.BufferedKVStore, activeWitnesses []tcommon.Address, genesisTimestamp int64, energyLimitForkBlockNum int64, validateEnvelope bool, genesisHash tcommon.Hash, parentAccountStateRoot *tcommon.Hash, standbyPaySet *standbyWitnessPaySet, domainChanges *state.DomainChangeStage, forkPassCache *forks.VersionPassCache, txInfoBatch *transactionInfoBatch, collectTxInfos bool, traceTxIndex int, traceTracer vm.Tracer, options processBlockOptions, traceForTxOpt ...func(index int, tx *types.Transaction) vm.Tracer) (txInfos []*corepb.TransactionInfo, javaAccountStateRoot tcommon.Hash, err error) {
	// Fork stats and prevBlockTime are immutable throughout this block. Share
	// permanently-passed versions with the chain cache, but keep pending/false
	// results in a disposable block view so per-tx gates read each version only
	// once without delaying a quorum transition at the next block boundary.
	if forkPassCache == nil {
		forkPassCache = forks.NewVersionPassCache()
	}
	forkPassCache = forkPassCache.BlockScope()

	blockSnap := statedb.Snapshot()
	dpSnap := dynProps.Snapshot()
	defer func() {
		if err != nil {
			statedb.RevertToSnapshot(blockSnap)
			dynProps.RevertToSnapshot(dpSnap)
		} else {
			dynProps.CommitSnapshot(dpSnap)
		}
	}()
	repairActivated, repairErr := applyMainnetLegacyStateRepairs(statedb, block.Number(), block.Hash(), options.legacyStateRepairHook)
	if repairErr != nil {
		return nil, tcommon.Hash{}, repairErr
	}
	if repairActivated {
		// A legacy-corrupted pre-image is incompatible with a clean rollout.
		// Even recovery execution in this process must remain serial from the
		// repair boundary onward.
		options.parallelTransfers = false
		options.parallelVM = false
	}

	// Reset per-block energy accumulator (matches java-tron Manager.processBlock).
	dynProps.SetBlockEnergyUsage(0)

	// Snapshot the chain head's timestamp before the tx loop. java-tron's
	// Manager.applyBlock runs processTransaction *before*
	// updateDynamicProperties(block), so during tx Execute the DP value
	// LatestBlockHeaderTimestamp is still the *previous* block's timestamp.
	// blockchain.go advances `LatestBlockHeaderTimestamp` only after this
	// function returns, so reading the DP here yields the prev-block
	// timestamp for the entire block.
	prevBlockTime := dynProps.LatestBlockHeaderTimestamp()
	prevBlockHeadSlot := HeadSlot(prevBlockTime, genesisTimestamp)

	writeHistoryBlockHash(statedb, dynProps, block.Number(), block.ParentHash())
	accountStateMark := statedb.JournalMark()
	var txScratch applyTransactionScratch
	transactionStarted := time.Now()
	transactions := block.Transactions()
	sampledShadow := block.Number()%discardShadowSampleInterval == 0
	if options.parallelTransfers && !sampledShadow && options.minParallelTransferCandidates > 0 {
		transferCandidates := countPlainTransferCandidates(transactions)
		if !shouldRunParallelTransferBlock(true, false, options.minParallelTransferCandidates, transferCandidates) {
			options.parallelTransfers = false
			parallelTransferAdmissionSkippedBlocksCounter.Inc(1)
			parallelTransferAdmissionSkippedCandidatesCounter.Inc(int64(transferCandidates))
		}
	}
	// VM publication is sampled/canary-only. Do not pay version recording on
	// ordinary blocks merely because the global operational switch is enabled.
	if options.parallelVM && !sampledShadow {
		options.parallelVM = false
	}
	shadowEnabled := txInfoBatch != nil && (sampledShadow || options.parallelTransfers || options.parallelVM)
	var transferShadow speculativeTransferShadow
	var versionedShadow *versionedAccessShadow
	if shadowEnabled {
		transferShadow.Prepare(len(transactions))
		versionedShadow = acquireVersionedAccessShadow(len(transactions))
		defer releaseVersionedAccessShadow(versionedShadow)
		defer versionedShadow.Finish(statedb, dynProps)
	}
	transactionDB := db
	var recordedTransactionDB transactionRecordingKVStore
	if shadowEnabled && db != nil {
		recordedTransactionDB = transactionRecordingKVStore{parent: db, recorder: &versionedShadow.recorder}
		transactionDB = &recordedTransactionDB
	}
	var txInfoSlots []transactionInfoSlot
	if txInfoBatch != nil {
		preparedSlots, preparedInfos := txInfoBatch.prepare(len(transactions))
		txInfoSlots = preparedSlots
		if collectTxInfos {
			txInfos = preparedInfos
		}
	} else {
		txInfoSlots = make([]transactionInfoSlot, len(transactions))
		if collectTxInfos {
			txInfos = make([]*corepb.TransactionInfo, len(transactions))
		}
	}
	var discardShadow *discardShadowBlock
	var discardCfg discardShadowRunConfig
	var transferPreexecution *discardShadowPreexecution
	var senderChainPreexecution *discardShadowPreexecution
	var vmSenderChainPreexecution *discardShadowPreexecution
	var vmSenderChainPublication bool
	var senderRetry *discardShadowSenderRetry
	var vmSenderRetry *discardShadowSenderRetry
	if shadowEnabled && collectTxInfos {
		discardShadow = prepareTransferExecutionBlock(statedb, dynProps, block.Number(), options.parallelTransfers)
		if discardShadow != nil {
			// Sampled async observers remain independent of canonical enablement.
			// Ordinary blocks join the incarnation scheduler only when the narrow
			// Transfer publisher itself is enabled.
			actualAsyncRetry := useDiscardShadowAsyncRetry(block.Number()) || (options.parallelTransfers && !discardShadow.sampled)
			discardCfg = discardShadowRunConfig{
				block:                   block,
				db:                      db,
				validateEnvelope:        validateEnvelope,
				activeWitnesses:         activeWitnesses,
				genesisTimestamp:        genesisTimestamp,
				energyLimitForkBlockNum: energyLimitForkBlockNum,
				genesisHash:             genesisHash,
				transactions:            transactions,
				captureBalanceTrace:     options.captureBalanceTrace,
			}
			if options.parallelTransfers && !discardShadow.sampled {
				// The ordinary canonical publisher already builds sender-chain
				// workers. Retain their clean block-start states for Erigon-style
				// retry incarnations instead of running a second preexecution pass
				// or copying StateDB at the first conflict.
				transferPreexecution = discardShadow.preexecuteTransferSenderChainsWithRetryState(discardCfg, actualAsyncRetry)
				if actualAsyncRetry {
					senderRetry = newDiscardShadowAsyncSenderRetry(transferPreexecution, len(transactions))
					if senderRetry != nil {
						senderRetry.publish = true
					}
				}
			} else {
				// Keep sampled blocks on the independent block-start/serial
				// canary so sender-chain results retain a production reference.
				transferPreexecution = discardShadow.preexecuteTransfers(discardCfg)
			}
			if discardShadow.sampled {
				senderChainPreexecution = discardShadow.preexecuteTransferSenderChainsWithRetryState(discardCfg, actualAsyncRetry)
				// VM is the dominant historical-sync family. Every sampled block
				// retains the serial reference; one narrow cohort may publish only
				// after the ordered bandwidth and block-energy carriers admit it.
				vmAsyncRetry := options.parallelVM && useVMSenderRetryObservation(block.Number())
				vmSenderChainPreexecution = discardShadow.preexecuteVMSenderChains(discardCfg, vmAsyncRetry)
				if options.parallelVM && useVMSenderChainPublication(block.Number()) {
					parallelVMBlocksCounter.Inc(1)
					if vmSenderChainPreexecution != nil {
						vmSenderChainPublication = true
						parallelVMPreexecutedCounter.Inc(int64(len(vmSenderChainPreexecution.results)))
						parallelVMPreexecutionNanosCounter.Inc(vmSenderChainPreexecution.wallNanos)
					}
				}
				if vmAsyncRetry {
					vmSenderRetry = newDiscardShadowAsyncVMSenderRetry(vmSenderChainPreexecution, len(transactions))
					if vmSenderRetry != nil {
						vmSenderRetry.publish = useVMSenderRetryPublication(block.Number())
						if vmSenderRetry.publish {
							parallelVMAsyncRetryPublishBlocksCounter.Inc(1)
						}
					}
				}
				if actualAsyncRetry {
					// Use three sampled cohorts for the real background retry
					// canary. The remaining cohort retains the synchronous observer
					// and its timing projection as a stable reference.
					senderRetry = newDiscardShadowAsyncSenderRetry(senderChainPreexecution, len(transactions))
					if senderRetry != nil {
						senderRetry.publish = options.parallelTransfers && useDiscardShadowAsyncRetryPublication(block.Number())
					}
				} else {
					senderRetry = newDiscardShadowSenderRetry(senderChainPreexecution, len(transactions))
				}
			}
			if discardShadow.sampled {
				versionedShadow.EnableWriteSetCapture(len(transactions))
			} else if transferPreexecution != nil {
				include, fullTransactions, recorderOnly := newDiscardShadowRetryWriteCapture(transferPreexecution, len(transactions))
				versionedShadow.EnableWriteSetCaptureFiltered(len(transactions), include, fullTransactions, recorderOnly)
			}
			if (senderRetry != nil && senderRetry.async) || (vmSenderRetry != nil && vmSenderRetry.async) {
				// Async incarnations consume immutable canonical post-images directly
				// from the block-local version carrier instead of replaying every
				// intervening WriteSet into private prefix states.
				versionedShadow.EnableSharedVersionValues(len(transactions))
			}
			if discardShadow.sampled || senderRetry != nil {
				if discardCfg.captureBalanceTrace {
					discardCfg.canonicalBalanceTraces = make([]*contractpb.TransactionBalanceTrace, len(transactions))
				}
			}
			if options.parallelTransfers {
				parallelTransferBlocksCounter.Inc(1)
				if transferPreexecution != nil {
					parallelTransferPreexecutedCounter.Inc(int64(len(transferPreexecution.results)))
					parallelTransferPreexecutionNanosCounter.Inc(transferPreexecution.wallNanos)
					for resultIndex := range transferPreexecution.results {
						if transferPreexecution.results[resultIndex].senderVersioned {
							parallelTransferChainPreexecutedCounter.Inc(1)
						}
					}
				}
			}
		}
	}
	// A rejected canonical transaction can return before sampled diagnostics
	// reach finish(). Always reclaim the block-scoped async worker first; it
	// owns only frozen inputs, but must not outlive their StateDB/database scope.
	defer func() {
		if senderRetry != nil && senderRetry.async {
			senderRetry.drainAsyncEvents(len(transactions), true)
		}
		if vmSenderRetry != nil && vmSenderRetry.async {
			vmSenderRetry.drainAsyncEvents(len(transactions), true)
		}
	}()
	flushDomainChanges := func(txIndex int, mark int) error {
		if domainChanges != nil {
			if err := domainChanges.FlushOrdinal(mark, uint64(txIndex)); err != nil {
				return fmt.Errorf("tx %d domain changes: %w", txIndex, err)
			}
			return nil
		}
		txNum, err := statedb.DomainChangeTxNumAtOrdinal(uint64(txIndex))
		if err != nil {
			return fmt.Errorf("tx %d state txNum: %w", txIndex, err)
		}
		if err := statedb.FlushDomainChangesSince(mark, txNum); err != nil {
			return fmt.Errorf("tx %d domain changes: %w", txIndex, err)
		}
		return nil
	}

	for i, tx := range transactions {
		options.timing.observeTransactionType(tx.ContractType())
		if transferPreexecution != nil {
			transferPreexecution.validateReadVersion(i, tx, versionedShadow)
		}
		if senderChainPreexecution != nil {
			senderChainPreexecution.validateReadVersion(i, tx, versionedShadow)
		}
		if vmSenderChainPreexecution != nil {
			vmSenderChainPreexecution.validateReadVersion(i, tx, versionedShadow)
			vmSenderChainPreexecution.projectPublicNetBoundary(i, dynProps)
			vmSenderChainPreexecution.projectBlockEnergyBoundary(i, dynProps, statedb, prevBlockTime, forkPassCache)
		}
		if senderRetry != nil {
			senderRetry.observeBoundary(i, tx, statedb, dynProps, versionedShadow, discardCfg)
		}
		if vmSenderRetry != nil {
			vmSenderRetry.observeBoundary(i, tx, statedb, dynProps, versionedShadow, discardCfg)
		}
		domainChangeMark := statedb.DomainChangeJournalMark()
		if domainChanges != nil {
			domainChangeMark = domainChanges.JournalMark()
		}
		if shadowEnabled {
			versionedShadow.BeginTransaction(i, statedb, dynProps)
		}
		if dynProps.ConsensusLogicOptimization() {
			if err := ValidateTxRetCount(tx); err != nil {
				return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
			}
		}
		if options.parallelTransfers && senderRetry != nil {
			if retryResult, found := senderRetry.selectedResultForPublication(i); found {
				parallelTransferRetryCandidatesCounter.Inc(1)
				parallelTransferCandidatesCounter.Inc(1)
				parallelTransferChainCandidatesCounter.Inc(1)
				if retryResult.publicNetValid {
					parallelTransferPublicNetReservationsCounter.Inc(1)
				}
				publicNetOverride, publicNetAdmitted := overridePublicNetReservation(retryResult, dynProps)
				switch {
				case !publicNetAdmitted:
					parallelTransferRetryPublicNetFallbackCounter.Inc(1)
					parallelTransferPublicNetLimitFallbackCounter.Inc(1)
				case statedb.ValidateTransactionWriteSetApply(retryResult.writes, dynProps, transactionDB) != nil:
					publicNetOverride.restore()
					parallelTransferRetryPreflightFallbackCounter.Inc(1)
					parallelTransferPreflightFallbackCounter.Inc(1)
				default:
					if options.speculativePreOracleTestHook != nil {
						options.speculativePreOracleTestHook("Transfer", i, retryResult)
					}
					balanceMatch, balanceErr := validateTransferBalancePostImages(statedb, tx, retryResult, discardCfg, i)
					if balanceErr != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, balanceErr
					}
					if !balanceMatch {
						publicNetOverride.restore()
						break
					}
					canonicalResult, oracleErr := validateTransferResultAtCanonicalBoundary(statedb, dynProps, i, retryResult, discardCfg, versionedShadow)
					if oracleErr != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, oracleErr
					}
					writeSeal, sealErr := newCanonicalPublicationWriteSeal("Transfer", retryResult, canonicalResult)
					if sealErr != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, sealErr
					}
					if options.speculativePostOracleTestHook != nil {
						options.speculativePostOracleTestHook("Transfer", i, retryResult)
					}
					if options.speculativePreApplyTestHook != nil {
						options.speculativePreApplyTestHook("Transfer", i, retryResult.writes)
					}
					if err := writeSeal.validateSource("before apply", retryResult); err != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					publishedStarted := time.Now()
					versionedShadow.recorder.RestoreReadSet(writeSeal.reads)
					publishErr := statedb.ApplyTransactionWriteSetRecorded(writeSeal.writes, dynProps, transactionDB, &versionedShadow.recorder)
					if publishErr != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish async sender retry: %v", errSpeculativePublicationAudit, i, publishErr)
					}
					if options.speculativePostApplyTestHook != nil {
						options.speculativePostApplyTestHook("Transfer", i, retryResult.writes)
					}
					if err := writeSeal.validateSource("after apply", retryResult); err != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					writeSeal.markMatched()
					statedb.FinalizeTransaction()
					statedb.AppendBalanceTraceTransaction(writeSeal.balanceTrace)
					if collectTxInfos {
						txInfos[i] = writeSeal.info
					}
					txHash := tx.Hash()
					if len(discardCfg.canonicalBalanceTraces) == len(transactions) {
						discardCfg.canonicalBalanceTraces[i] = statedb.CopyLastBalanceTraceTransaction(txHash.Bytes())
					}
					versionedShadow.ObserveTransaction(i, tx, statedb, dynProps, domainChangeMark)
					if err := validateCanonicalPublicationWriteSet("Transfer", i, writeSeal.writes, versionedShadow); err != nil {
						publicNetOverride.restore()
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					publicNetOverride.restore()
					transferShadow.Observe(tx, statedb, domainChangeMark, transactionWriteSetChangesDynamic(writeSeal.writes))
					if err := flushDomainChanges(i, domainChangeMark); err != nil {
						parallelTransferRetryErrorsCounter.Inc(1)
						parallelTransferErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: publish async Transfer domain changes: %v", errSpeculativePublicationAudit, err)
					}
					senderRetry.markPublished(i)
					parallelTransferRetryPublishedCounter.Inc(1)
					parallelTransferPublishedCounter.Inc(1)
					parallelTransferChainPublishedCounter.Inc(1)
					if retryResult.publicNetValid {
						parallelTransferPublicNetPublishedCounter.Inc(1)
						if publicNetOverride.rebased {
							parallelTransferPublicNetRebasedCounter.Inc(1)
						}
					}
					elapsed := time.Since(publishedStarted).Nanoseconds()
					parallelTransferRetryPublicationNanosCounter.Inc(elapsed)
					parallelTransferPublicationNanosCounter.Inc(elapsed)
					continue
				}
			}
		}
		if options.parallelTransfers {
			preResult, readVersion, found := transferPreexecution.resultForTransaction(i)
			switch {
			case !found:
			case !preexecutedTransferReady(preResult):
				parallelTransferUnavailableFallbackCounter.Inc(1)
			case !readVersion.publishable:
				parallelTransferConflictFallbackCounter.Inc(1)
				if readVersion.predecessor {
					parallelTransferChainPredFallbackCounter.Inc(1)
				}
			default:
				parallelTransferCandidatesCounter.Inc(1)
				if preResult.senderVersioned {
					parallelTransferChainCandidatesCounter.Inc(1)
				}
				if preResult.publicNetValid {
					parallelTransferPublicNetReservationsCounter.Inc(1)
				}
				publicNetOverride, publicNetAdmitted := overridePublicNetReservation(preResult, dynProps)
				if !publicNetAdmitted {
					parallelTransferPublicNetLimitFallbackCounter.Inc(1)
					break
				}
				if err := statedb.ValidateTransactionWriteSetApply(preResult.writes, dynProps, transactionDB); err != nil {
					publicNetOverride.restore()
					parallelTransferPreflightFallbackCounter.Inc(1)
					break
				}
				if options.speculativePreOracleTestHook != nil {
					options.speculativePreOracleTestHook("Transfer", i, preResult)
				}
				balanceMatch, balanceErr := validateTransferBalancePostImages(statedb, tx, preResult, discardCfg, i)
				if balanceErr != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, balanceErr
				}
				if !balanceMatch {
					publicNetOverride.restore()
					break
				}
				canonicalResult, oracleErr := validateTransferResultAtCanonicalBoundary(statedb, dynProps, i, preResult, discardCfg, versionedShadow)
				if oracleErr != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, oracleErr
				}
				writeSeal, sealErr := newCanonicalPublicationWriteSeal("Transfer", preResult, canonicalResult)
				if sealErr != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, sealErr
				}
				if options.speculativePostOracleTestHook != nil {
					options.speculativePostOracleTestHook("Transfer", i, preResult)
				}
				if options.speculativePreApplyTestHook != nil {
					options.speculativePreApplyTestHook("Transfer", i, preResult.writes)
				}
				if err := writeSeal.validateSource("before apply", preResult); err != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				publishedStarted := time.Now()
				versionedShadow.recorder.RestoreReadSet(writeSeal.reads)
				publishErr := statedb.ApplyTransactionWriteSetRecorded(writeSeal.writes, dynProps, transactionDB, &versionedShadow.recorder)
				if publishErr != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish pre-executed transfer: %v", errSpeculativePublicationAudit, i, publishErr)
				}
				if options.speculativePostApplyTestHook != nil {
					options.speculativePostApplyTestHook("Transfer", i, preResult.writes)
				}
				if err := writeSeal.validateSource("after apply", preResult); err != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				writeSeal.markMatched()
				statedb.FinalizeTransaction()
				statedb.AppendBalanceTraceTransaction(writeSeal.balanceTrace)
				if collectTxInfos {
					txInfos[i] = writeSeal.info
				}
				txHash := tx.Hash()
				if len(discardCfg.canonicalBalanceTraces) == len(transactions) {
					discardCfg.canonicalBalanceTraces[i] = statedb.CopyLastBalanceTraceTransaction(txHash.Bytes())
				}
				versionedShadow.ObserveTransaction(i, tx, statedb, dynProps, domainChangeMark)
				if err := validateCanonicalPublicationWriteSet("Transfer", i, writeSeal.writes, versionedShadow); err != nil {
					publicNetOverride.restore()
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				publicNetOverride.restore()
				transferShadow.Observe(tx, statedb, domainChangeMark, transactionWriteSetChangesDynamic(writeSeal.writes))
				if err := flushDomainChanges(i, domainChangeMark); err != nil {
					parallelTransferErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: publish Transfer domain changes: %v", errSpeculativePublicationAudit, err)
				}
				parallelTransferPublishedCounter.Inc(1)
				if preResult.senderVersioned {
					parallelTransferChainPublishedCounter.Inc(1)
				}
				transferPreexecution.markPublished(i)
				if preResult.publicNetValid {
					parallelTransferPublicNetPublishedCounter.Inc(1)
					if publicNetOverride.rebased {
						parallelTransferPublicNetRebasedCounter.Inc(1)
					}
				}
				parallelTransferPublicationNanosCounter.Inc(time.Since(publishedStarted).Nanoseconds())
				continue
			}
		}
		if options.parallelVM && vmSenderRetry != nil {
			if retryResult, found := vmSenderRetry.selectedResultForPublication(i); found {
				parallelVMAsyncRetryPublishCandidatesCounter.Inc(1)
				parallelVMCandidatesCounter.Inc(1)
				if retryResult.senderVersioned {
					parallelVMChainCandidatesCounter.Inc(1)
				}
				if retryResult.publicNetValid {
					parallelVMPublicNetReservationsCounter.Inc(1)
				}
				blockEnergyBaseline, blockEnergyExpected, blockEnergyAdmitted := vmRetryBlockEnergyBoundary(
					retryResult, dynProps, statedb, prevBlockTime, forkPassCache,
				)
				switch {
				case !blockEnergyAdmitted:
					parallelVMAsyncRetryPublishEnergyFallbackCounter.Inc(1)
					parallelVMBlockEnergyFallbackCounter.Inc(1)
				case !preexecutedVMEntryCodeMatches(statedb, retryResult):
					// A version-clean read can still originate from a stale or
					// incomplete block-start base. Fall back to authoritative serial
					// execution when the entry bytecode edge differs.
					parallelVMAsyncRetryPublishPreflightCounter.Inc(1)
					parallelVMPreflightFallbackCounter.Inc(1)
				default:
					publicNetOverride, publicNetAdmitted := overridePublicNetReservation(retryResult, dynProps)
					if !publicNetAdmitted {
						parallelVMAsyncRetryPublishNetFallbackCounter.Inc(1)
						parallelVMPublicNetFallbackCounter.Inc(1)
						break
					}
					if err := statedb.ValidateTransactionWriteSetApply(retryResult.writes, dynProps, transactionDB); err != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishPreflightCounter.Inc(1)
						parallelVMPreflightFallbackCounter.Inc(1)
						break
					}
					if options.speculativePreOracleTestHook != nil {
						options.speculativePreOracleTestHook("VM", i, retryResult)
					}
					// Retry publication must pass the same independent serial oracle as
					// block-start publication. Read-version validation proves only the
					// dependencies the worker recorded; it cannot prove that an omitted
					// read or a shared bad execution base did not affect the result.
					canonicalResult, oracleErr := validateVMResultAtCanonicalBoundary(statedb, dynProps, i, retryResult, discardCfg)
					if oracleErr != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, oracleErr
					}
					writeSeal, sealErr := newCanonicalPublicationWriteSeal("VM", retryResult, canonicalResult)
					if sealErr != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, sealErr
					}
					if options.speculativePostOracleTestHook != nil {
						options.speculativePostOracleTestHook("VM", i, retryResult)
					}
					if options.speculativePreApplyTestHook != nil {
						options.speculativePreApplyTestHook("VM", i, retryResult.writes)
					}
					if err := writeSeal.validateSource("before apply", retryResult); err != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					publishedStarted := time.Now()
					versionedShadow.recorder.RestoreReadSet(writeSeal.reads)
					publishErr := statedb.ApplyTransactionWriteSetRecorded(writeSeal.writes, dynProps, transactionDB, &versionedShadow.recorder)
					if publishErr != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish async VM sender retry: %v", errSpeculativePublicationAudit, i, publishErr)
					}
					if options.speculativePostApplyTestHook != nil {
						options.speculativePostApplyTestHook("VM", i, retryResult.writes)
					}
					if err := writeSeal.validateSource("after apply", retryResult); err != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					writeSeal.markMatched()
					statedb.FinalizeTransaction()
					statedb.AppendBalanceTraceTransaction(writeSeal.balanceTrace)
					if collectTxInfos {
						txInfos[i] = writeSeal.info
					}
					txHash := tx.Hash()
					if len(discardCfg.canonicalBalanceTraces) == len(transactions) {
						discardCfg.canonicalBalanceTraces[i] = statedb.CopyLastBalanceTraceTransaction(txHash.Bytes())
					}
					versionedShadow.ObserveTransaction(i, tx, statedb, dynProps, domainChangeMark)
					if err := validateCanonicalPublicationWriteSet("VM", i, writeSeal.writes, versionedShadow); err != nil {
						publicNetOverride.restore()
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, err
					}
					publicNetOverride.restore()
					transferShadow.Observe(tx, statedb, domainChangeMark, transactionWriteSetChangesDynamic(writeSeal.writes))
					if err := flushDomainChanges(i, domainChangeMark); err != nil {
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: publish async VM domain changes: %v", errSpeculativePublicationAudit, err)
					}
					if dynProps.BlockEnergyUsage() != blockEnergyBaseline {
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish async VM retry changed block energy before settlement", errSpeculativePublicationAudit, i)
					}
					accumulateBlockEnergyUsageFromReceipt(dynProps, statedb, prevBlockTime, writeSeal.info.GetReceipt(), forkPassCache)
					if dynProps.BlockEnergyUsage() != blockEnergyExpected {
						parallelVMAsyncRetryPublishErrorsCounter.Inc(1)
						parallelVMErrorsCounter.Inc(1)
						return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish async VM retry block energy mismatch: got %d want %d", errSpeculativePublicationAudit, i, dynProps.BlockEnergyUsage(), blockEnergyExpected)
					}
					vmSenderRetry.markPublished(i)
					parallelVMAsyncRetryPublishedCounter.Inc(1)
					parallelVMPublishedCounter.Inc(1)
					parallelVMBlockEnergyPublishedCounter.Inc(1)
					if retryResult.senderVersioned {
						parallelVMChainPublishedCounter.Inc(1)
					}
					if retryResult.publicNetValid {
						parallelVMPublicNetPublishedCounter.Inc(1)
						if publicNetOverride.rebased {
							parallelVMPublicNetRebasedCounter.Inc(1)
						}
					}
					elapsed := time.Since(publishedStarted).Nanoseconds()
					parallelVMAsyncRetryPublishNanosCounter.Inc(elapsed)
					parallelVMPublicationNanosCounter.Inc(elapsed)
					options.timing.addVMExecution(retryResult.vmExecutionDuration, retryResult.vmRawEnergyUsage)
					continue
				}
			}
		}
		if vmSenderChainPublication {
			preResult, readVersion, found := vmSenderChainPreexecution.resultForTransaction(i)
			switch {
			case !found:
			case !preexecutedResultReady(preResult):
				parallelVMUnavailableFallbackCounter.Inc(1)
			case !readVersion.publishable:
				parallelVMConflictFallbackCounter.Inc(1)
				if readVersion.predecessor {
					parallelVMChainPredFallbackCounter.Inc(1)
				}
			case !preexecutedVMEntryCodeMatches(statedb, preResult):
				parallelVMPreflightFallbackCounter.Inc(1)
			default:
				parallelVMCandidatesCounter.Inc(1)
				if preResult.senderVersioned {
					parallelVMChainCandidatesCounter.Inc(1)
				}
				if preResult.publicNetValid {
					parallelVMPublicNetReservationsCounter.Inc(1)
				}
				blockEnergyBaseline, blockEnergyExpected, blockEnergyAdmitted := vmSenderChainPreexecution.blockEnergyBoundaryForPublication(i, dynProps)
				if !blockEnergyAdmitted {
					parallelVMBlockEnergyFallbackCounter.Inc(1)
					break
				}
				publicNetOverride, publicNetAdmitted := overridePublicNetReservation(preResult, dynProps)
				if !publicNetAdmitted {
					parallelVMPublicNetFallbackCounter.Inc(1)
					break
				}
				if err := statedb.ValidateTransactionWriteSetApply(preResult.writes, dynProps, transactionDB); err != nil {
					publicNetOverride.restore()
					parallelVMPreflightFallbackCounter.Inc(1)
					break
				}
				if options.speculativePreOracleTestHook != nil {
					options.speculativePreOracleTestHook("VM", i, preResult)
				}
				canonicalResult, oracleErr := validateVMResultAtCanonicalBoundary(statedb, dynProps, i, preResult, discardCfg)
				if oracleErr != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, oracleErr
				}
				writeSeal, sealErr := newCanonicalPublicationWriteSeal("VM", preResult, canonicalResult)
				if sealErr != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, sealErr
				}
				if options.speculativePostOracleTestHook != nil {
					options.speculativePostOracleTestHook("VM", i, preResult)
				}
				if options.speculativePreApplyTestHook != nil {
					options.speculativePreApplyTestHook("VM", i, preResult.writes)
				}
				if err := writeSeal.validateSource("before apply", preResult); err != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				publishedStarted := time.Now()
				versionedShadow.recorder.RestoreReadSet(writeSeal.reads)
				publishErr := statedb.ApplyTransactionWriteSetRecorded(writeSeal.writes, dynProps, transactionDB, &versionedShadow.recorder)
				if publishErr != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish pre-executed VM: %v", errSpeculativePublicationAudit, i, publishErr)
				}
				if options.speculativePostApplyTestHook != nil {
					options.speculativePostApplyTestHook("VM", i, preResult.writes)
				}
				if err := writeSeal.validateSource("after apply", preResult); err != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				writeSeal.markMatched()
				statedb.FinalizeTransaction()
				statedb.AppendBalanceTraceTransaction(writeSeal.balanceTrace)
				if collectTxInfos {
					txInfos[i] = writeSeal.info
				}
				txHash := tx.Hash()
				if len(discardCfg.canonicalBalanceTraces) == len(transactions) {
					discardCfg.canonicalBalanceTraces[i] = statedb.CopyLastBalanceTraceTransaction(txHash.Bytes())
				}
				versionedShadow.ObserveTransaction(i, tx, statedb, dynProps, domainChangeMark)
				if err := validateCanonicalPublicationWriteSet("VM", i, writeSeal.writes, versionedShadow); err != nil {
					publicNetOverride.restore()
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, err
				}
				publicNetOverride.restore()
				transferShadow.Observe(tx, statedb, domainChangeMark, transactionWriteSetChangesDynamic(writeSeal.writes))
				if err := flushDomainChanges(i, domainChangeMark); err != nil {
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: publish VM domain changes: %v", errSpeculativePublicationAudit, err)
				}
				if dynProps.BlockEnergyUsage() != blockEnergyBaseline {
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish pre-executed VM changed block energy before settlement", errSpeculativePublicationAudit, i)
				}
				accumulateBlockEnergyUsageFromReceipt(dynProps, statedb, prevBlockTime, writeSeal.info.GetReceipt(), forkPassCache)
				vmSenderChainPreexecution.validateBlockEnergyBoundary(i, dynProps)
				if !vmSenderChainPreexecution.blockEnergyBoundaryMatched(i) {
					parallelVMErrorsCounter.Inc(1)
					return nil, tcommon.Hash{}, fmt.Errorf("%w: tx %d publish pre-executed VM block energy mismatch: got %d want %d", errSpeculativePublicationAudit, i, dynProps.BlockEnergyUsage(), blockEnergyExpected)
				}
				vmSenderChainPreexecution.markPublished(i)
				parallelVMPublishedCounter.Inc(1)
				parallelVMBlockEnergyPublishedCounter.Inc(1)
				if preResult.senderVersioned {
					parallelVMChainPublishedCounter.Inc(1)
				}
				if preResult.publicNetValid {
					parallelVMPublicNetPublishedCounter.Inc(1)
					if publicNetOverride.rebased {
						parallelVMPublicNetRebasedCounter.Inc(1)
					}
				}
				parallelVMPublicationNanosCounter.Inc(time.Since(publishedStarted).Nanoseconds())
				options.timing.addVMExecution(preResult.vmExecutionDuration, preResult.vmRawEnergyUsage)
				continue
			}
		}
		// validate=true: replay calls actuator.Validate (P0-2a). Every
		// actuator's Validate is read-only (audited 2026-05-15) so re-running
		// on replay matches java-tron Manager.processBlock parity.
		//
		// validateEnvelope is per-tx so a same-block tx2 sees tx1's effects
		// (e.g. an AccountPermissionUpdate followed by a Transfer signed with
		// the post-rotation key).
		txHash := tx.Hash()
		statedb.BeginBalanceTraceTransaction(txHash.Bytes(), tx.ContractType().String())
		//
		// traceTracer is installed only on the target tx index (debug_traceTransaction
		// replay); -1 disables it for the normal block-apply path.
		var txTracer vm.Tracer
		if len(traceForTxOpt) > 0 && traceForTxOpt[0] != nil {
			txTracer = traceForTxOpt[0](i, tx)
		} else if traceTxIndex >= 0 && i == traceTxIndex {
			txTracer = traceTracer
		}
		internalTxArena := &txInfoSlots[i].internalTxArena
		internalTxArena.Reset()
		executionLogArena := &txInfoSlots[i].executionLogArena
		executionLogArena.Reset()
		result, err := applyTransactionWithScratch(statedb, dynProps, tx, prevBlockTime, true, prevBlockHeadSlot, block.Timestamp(), block.Number(), transactionDB, activeWitnesses, energyLimitForkBlockNum, genesisHash, block.WitnessAddress(), true, validateEnvelope, true, forkPassCache, txTracer, &txScratch, internalTxArena, executionLogArena)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
		if err := ValidateTxVMContractRet(tx, corepb.Transaction_ResultContractResult(result.ContractRet)); err != nil {
			vm.ReleaseExecutionLogs(result.Logs)
			result.Logs = nil
			return nil, tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
		if collectTxInfos {
			txInfos[i] = txInfoSlots[i].build(tx, result, block.Number(), block.Timestamp(), dynProps.AllowTransactionFeePool())
		}
		balanceTraceStatus := balanceTraceTransactionStatus(result)
		// When collected, TransactionInfo now owns copies of the log slice headers
		// while their immutable payload bytes stay independently allocated. Stored
		// replay with an archived TransactionRet needs neither copy. In both cases,
		// recycle the VM's []Log backing before the block-local Result sink is
		// cleared by the next transaction.
		vm.ReleaseExecutionLogs(result.Logs)
		result.Logs = nil
		statedb.FinalizeTransaction()
		statedb.EndBalanceTraceTransaction(balanceTraceStatus)
		if len(discardCfg.canonicalBalanceTraces) == len(transactions) {
			discardCfg.canonicalBalanceTraces[i] = statedb.CopyLastBalanceTraceTransaction(txHash.Bytes())
		}
		if shadowEnabled {
			versionedShadow.ObserveTransaction(i, tx, statedb, dynProps, domainChangeMark)
			transferShadow.Observe(tx, statedb, domainChangeMark, txScratch.dynamicPropertiesChanged)
		}
		if err := flushDomainChanges(i, domainChangeMark); err != nil {
			return nil, tcommon.Hash{}, err
		}

		accumulateBlockEnergyUsage(dynProps, statedb, prevBlockTime, result, forkPassCache)
		if isVMContractType(tx.ContractType()) {
			options.timing.addVMExecution(result.VMExecutionDuration, result.VMRawEnergyUsage)
		}
		if vmSenderChainPreexecution != nil {
			vmSenderChainPreexecution.validateBlockEnergyBoundary(i, dynProps)
		}
	}

	if discardShadow != nil && (discardShadow.sampled || senderRetry != nil) {
		discardCfg.canonicalInfos = txInfos
	}
	if discardShadow != nil && discardShadow.sampled {
		_ = discardShadow.finishTransferPreexecution(transferPreexecution, versionedShadow, discardCfg)
		_ = discardShadow.finishTransferSenderChains(senderChainPreexecution, versionedShadow, discardCfg)
		_ = discardShadow.finishVMSenderChains(vmSenderChainPreexecution, versionedShadow, discardCfg)
	}
	if senderRetry != nil {
		stats := senderRetry.finish(versionedShadow, discardCfg)
		if err := validatePublishedRetryAudit("Transfer", stats); err != nil {
			return nil, tcommon.Hash{}, err
		}
	}
	if vmSenderRetry != nil {
		stats := vmSenderRetry.finish(versionedShadow, discardCfg)
		if vmSenderRetry.async {
			recordVMAsyncSenderRetryStats(stats)
		} else {
			recordVMSenderRetryStats(stats)
		}
		if err := validatePublishedRetryAudit("VM", stats); err != nil {
			return nil, tcommon.Hash{}, err
		}
	}
	if discardShadow != nil && discardShadow.sampled {
		_ = discardShadow.run(versionedShadow, discardCfg)
	}
	if options.timing != nil {
		options.timing.Transactions = time.Since(transactionStarted)
	}

	if parentAccountStateRoot != nil {
		phaseStart := time.Now()
		javaAccountStateRoot, err = defaultStateRootAdapter.JavaAccountStateRoot(statedb, *parentAccountStateRoot, accountStateMark)
		if err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("account state root: %w", err)
		}
		if options.timing != nil {
			options.timing.AccountStateRoot = time.Since(phaseStart)
		}
	}

	// Per-block adaptive energy limit adjustment. Under harden, an overflow in the
	// ceiling computation rejects the block (java ArithmeticException unwinds the
	// block-processing stack); the named-return + deferred revert above mirror that.
	if dynProps.AllowAdaptiveEnergy() {
		phaseStart := time.Now()
		UpdateTotalEnergyAverageUsage(dynProps, genesisTimestamp)
		if err = UpdateAdaptiveTotalEnergyLimit(dynProps); err != nil {
			return nil, tcommon.Hash{}, fmt.Errorf("adaptive total energy limit: %w", err)
		}
		if options.timing != nil {
			options.timing.AdaptiveEnergy = time.Since(phaseStart)
		}
	}

	// Pay block reward to witness (and standby top-127 when change_delegation
	// is active — the new-algorithm reward path goes through payBlockReward
	// which splits by brokerage and accumulates the voter pool). java-tron
	// runs this after adaptive-energy updates, then pays the transaction-fee
	// pool reward from the same payReward path.
	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		phaseStart := time.Now()
		payBlockReward(db, statedb, dynProps, witnessAddr, dynProps.WitnessPayPerBlock())
		// java-tron observes WitnessStore at reward time, after transactions.
		// The cached set is reusable only while StateDB's membership/vote
		// generation still matches; payStandbyWitnessWithSet rebuilds from the
		// post-transaction view on any mutation, preserving the same result.
		payStandbyWitnessWithSet(db, statedb, dynProps, standbyPaySet)
		payTransactionFeeReward(db, statedb, dynProps, witnessAddr)
		if options.timing != nil {
			options.timing.Rewards = time.Since(phaseStart)
		}
	}
	if shadowEnabled {
		transferShadow.Publish()
		versionedShadow.Publish(statedb, dynProps)
	}
	filterTransactionInfoInternalTransactions(txInfos, options.saveInternalTx, options.saveFeaturedInternalTx, options.saveCancelAllUnfreezeV2Details)

	return txInfos, javaAccountStateRoot, nil
}

// filterTransactionInfoInternalTransactions mirrors java-tron's
// TransactionUtil.newTransactionInfo node-local vm.save* filtering. It runs
// after every speculative/serial parity audit so the oracles always compare the
// complete ProgramResult, then narrows only the persisted API receipt view.
func filterTransactionInfoInternalTransactions(infos []*corepb.TransactionInfo, saveInternal, saveFeatured, saveCancelDetails bool) {
	for _, info := range infos {
		if info == nil || len(info.InternalTransactions) == 0 {
			continue
		}
		if !saveInternal {
			info.InternalTransactions = nil
			continue
		}
		if !saveFeatured {
			kept := info.InternalTransactions[:0]
			for _, internal := range info.InternalTransactions {
				if internal == nil {
					continue
				}
				note := internal.Note
				if bytes.Equal(note, []byte("call")) || bytes.Equal(note, []byte("create")) || bytes.Equal(note, []byte("suicide")) {
					kept = append(kept, internal)
				}
			}
			if len(kept) == 0 {
				info.InternalTransactions = nil
			} else {
				info.InternalTransactions = kept[:len(kept):len(kept)]
			}
			continue
		}
		if !saveCancelDetails {
			for _, internal := range info.InternalTransactions {
				if internal != nil && bytes.Equal(internal.Note, []byte("cancelAllUnfreezeV2")) {
					internal.Extra = ""
				}
			}
		}
	}
}

func countPlainTransferCandidates(transactions []*types.Transaction) int {
	count := 0
	for _, tx := range transactions {
		if tx != nil && tx.ContractType() == corepb.Transaction_Contract_TransferContract {
			count++
		}
	}
	return count
}

func shouldRunParallelTransferBlock(enabled, sampled bool, minimumCandidates, candidates int) bool {
	if !enabled {
		return false
	}
	return sampled || minimumCandidates <= 0 || candidates >= minimumCandidates
}

func balanceTraceTransactionStatus(result *actuator.Result) string {
	if result == nil {
		return "SUCCESS"
	}
	ret := corepb.Transaction_ResultContractResult(result.ContractRet)
	if ret == corepb.Transaction_Result_DEFAULT {
		return "SUCCESS"
	}
	status := ret.String()
	if status == "" {
		return "SUCCESS"
	}
	return status
}
