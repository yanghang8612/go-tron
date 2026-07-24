package actuator

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

// BufferedKVStore is the read+write capability that actuators need from
// `Context.DB`. It is satisfied by both the on-disk `ethdb.KeyValueStore`
// (used by tests and the producer's BuildBlock path) and by
// `core/blockbuffer.Buffer` (used by `BlockChain.applyBlock` so that
// rawdb-direct writes inside `act.Execute` are rewindable on switchFork).
//
// Slice 3 of the fork-rewind fix widened this from `ethdb.KeyValueStore`
// to a Reader+Writer interface so the in-memory buffer can be plugged in
// without changing every actuator's call signature — every rawdb accessor
// already uses narrow `ethdb.KeyValueReader` / `ethdb.KeyValueWriter`
// signatures, which `BufferedKVStore` composes.
type BufferedKVStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

type Context struct {
	State    *state.StateDB
	DynProps *state.DynamicProperties
	Tx       *types.Transaction
	// BlockTime is the timestamp of the block currently being applied
	// (matches the EVM's TIMESTAMP opcode, java-tron `block.getTimeStamp()`).
	BlockTime int64
	// PrevBlockTime is the chain head's timestamp at the moment this tx
	// starts executing — i.e. the timestamp of the block *before* the one
	// being applied. java-tron actuators read this via
	// `chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderTimestamp()`
	// because `Manager.applyBlock` calls `processTransaction` *before*
	// `updateDynamicProperties(block)`, so the DP value is still the prev
	// block's timestamp during tx Execute. Consensus-affecting reads
	// (freeze expiry, withdraw cooldown, proposal create_time, etc.) must
	// use this field; only the VM's TIMESTAMP opcode reads `BlockTime`.
	PrevBlockTime int64
	// HeadSlot is the chain head slot at the moment this tx starts executing,
	// i.e. java-tron's ResourceProcessor/EnergyProcessor `now`. Resource
	// sliding-window fields (latest_consume_time*, public_net_time, TRC10
	// asset net times) are denominated in slots, while operation/expiry times
	// stay in milliseconds via PrevBlockTime.
	HeadSlot    int64
	HasHeadSlot bool
	BlockNumber uint64
	// Coinbase is the block producer's witness address, surfaced to the VM's
	// COINBASE opcode. java-tron derives it from the block header's
	// witnessAddress (ProgramInvokeFactory). Zero outside block processing.
	Coinbase common.Address
	// GenesisHash identifies the chain for narrow historical exceptions.
	// Production block processing can derive it from DB when this is zero;
	// tests may set it explicitly when they do not materialize genesis.
	GenesisHash common.Hash
	// EnergyLimitForkBlockNum mirrors java-tron's `enery.limit.block.num`.
	// HasEnergyLimitForkBlockNum distinguishes an explicit 0 (active at
	// genesis) from the zero value of older tests.
	EnergyLimitForkBlockNum    int64
	HasEnergyLimitForkBlockNum bool
	DB                         BufferedKVStore  // rawdb access for governance/brokerage; buffer-aware on InsertBlock
	ActiveWitnesses            []common.Address // active witness set for governance checks
	// TrustTransactionRet is true only when replaying a signed block. Pending
	// transactions carry unsigned Ret data, so producers and txpool validation
	// must ignore it.
	TrustTransactionRet bool
	// ForkPassCache memoizes already-activated SR fork versions across blocks
	// and, on the block-apply path, pending versions within the current block.
	// This skips identical fork-stats reads from every transaction while a fresh
	// block scope still observes quorum transitions at the next boundary.
	// Node-local and reset on reorg; nil contexts fall through to the uncached
	// store tally.
	ForkPassCache *forks.VersionPassCache
	// ResultSink is an optional block-local scratch result. The canonical block
	// replay path consumes an actuator result completely before executing the
	// next transaction, so simple actuators can fill this object instead of
	// allocating one result per transaction. Callers that return Result to
	// their own caller leave this nil and retain the existing ownership model.
	// Execute implementations must never retain the sink after returning.
	ResultSink *Result
	// Tracer, when non-nil, is installed into the TVM config for this tx so the
	// debug_traceTransaction replay captures the opcode/call stream. Nil on every
	// production path (block-apply, producer, pool) — zero overhead.
	Tracer vm.Tracer
}

// PassVersion reports whether SR software-fork `version` has activated as of
// this transaction's context (ceil-aligned HardForkTime + vote-rate quorum,
// java-tron ForkController.pass). It routes through ForkPassCache when the
// block-execution path supplied one — answering an already-activated version
// across blocks and a pending version within the current block without
// re-reading and re-tallying its fork-stats bitmap once per tx. A nil cache
// falls through to the plain uncached store tally, so the result is
// byte-identical either way. Returns false when State or DynProps is absent,
// matching the defensive guards the call sites carried before.
func (ctx *Context) PassVersion(version int32) bool {
	if ctx == nil || ctx.State == nil || ctx.DynProps == nil {
		return false
	}
	if ctx.passVersion(version) {
		return true
	}

	// Nile activated the release-v4.8.1 feature set while java-tron's enum
	// still assigned VERSION_4_8_1 the wire value 33. Upstream later inserted
	// VERSION_4_8_0_1 at 33 and renumbered VERSION_4_8_1 to 34 before merging
	// that release to master. Historical Nile blocks therefore legitimately
	// gate v4.8.1 behavior on 33 (not 34); mainnet was never deployed with the
	// old numbering and must continue to require 34.
	if version == 34 && ctx.isNile() {
		return forks.PassVersionFromStoreWithRate(ctx.State, 33, ctx.PrevBlockTime, ctx.DynProps.MaintenanceTimeInterval(), 80)
	}
	return false
}

func (ctx *Context) passVersion(version int32) bool {
	return ctx.ForkPassCache.Pass(ctx.State, version, ctx.PrevBlockTime, ctx.DynProps.MaintenanceTimeInterval())
}

func (ctx *Context) isNile() bool {
	genesisHash := ctx.GenesisHash
	if genesisHash == (common.Hash{}) && ctx.DB != nil {
		genesisHash = rawdb.ReadBlockHashByNumber(ctx.DB, 0)
	}
	return genesisHash == params.NileGenesisHash
}

func (ctx *Context) ResourceTime() int64 {
	if ctx != nil && ctx.HasHeadSlot {
		return ctx.HeadSlot
	}
	if ctx == nil {
		return 0
	}
	return ctx.PrevBlockTime
}

// Result captures the outcome of an actuator's Execute() call. The energy
// fields mirror java-tron's `Protocol.ResourceReceipt` semantics so that
// `core/state_processor.buildTransactionInfo` can map them onto proto
// fields without further translation:
//
//   - EnergyUsageTotal: total VM energy consumed by the call (proto field 4).
//     Set by VMActuator.executeCreate/executeTrigger.
//   - EnergyUsed:       fraction of EnergyUsageTotal that was paid from the
//     caller's frozen-energy stake (proto field 1). 0 if
//     the entire bill was paid from balance. Set by
//     PayEnergyBill.
//   - OriginEnergyUsage: fraction paid from the contract origin's frozen
//     energy under the consume_user_resource_percent split
//     (proto field 3). Set by PayEnergyBill when java-tron's origin/caller
//     split applies.
//   - EnergyFee:        balance-paid portion of the energy bill in SUN
//     (proto field 2). Set by PayEnergyBill.
//   - Fee:              total transaction fee in SUN. Sum of EnergyFee plus
//     any actuator-specific fees (asset issue, exchange
//     create, etc.). Bandwidth NetFee is *not* included
//     here — it's added in buildTransactionInfo.
type Result struct {
	Fee                 int64
	EnergyUsageTotal    int64
	EnergyUsed          int64
	EnergyFee           int64
	OriginEnergyUsage   int64
	CallerEnergyLeft    int64
	OriginEnergyLeft    int64
	HasCallerEnergyLeft bool
	HasOriginEnergyLeft bool
	// Diagnostic (cross-impl parity), non-consensus — surfaced in
	// TransactionInfo.ResourceReceipt fields 11-19. Owner* describe the tx
	// fee-payer and are filled for every tx type (set in core.applyTransaction);
	// *EnergyWindow are filled for smart-contract txs (set in vm_actuator).
	OwnerBalance                int64
	OwnerFreeNetLeft            int64
	OwnerFrozenNetLeft          int64
	OwnerNetLastConsumeTime     int64
	OwnerFreeNetLastConsumeTime int64
	OwnerFrozenForNet           int64
	OwnerFrozenForEnergy        int64
	OriginEnergyWindow          int64
	CallerEnergyWindow          int64
	// Diagnostic (cross-impl parity), non-consensus — ResourceReceipt fields
	// 20-28, filled for smart-contract txs in vm_actuator. They decompose the
	// energy bill: recovered_usage = energy_limit - energy_left, and the limit
	// is floor(frozen_for_energy/TRX * TotalEnergyCurrentLimit/TotalEnergyWeight).
	CallerEnergyLimit           int64
	OriginEnergyLimit           int64
	OriginFrozenForEnergy       int64
	CallerEnergyUsagePre        int64
	OriginEnergyUsagePre        int64
	CallerEnergyLastConsumeTime int64
	OriginEnergyLastConsumeTime int64
	TotalEnergyWeight           int64
	TotalEnergyCurrentLimit     int64

	// energyPreCharges records the pre-VM energy pre-charge applied to the
	// caller (and, under the consume_user_resource_percent split, the origin),
	// mirroring java-tron VMActuator.getAccountEnergyLimitWithFixRatio /
	// getTotalEnergyLimitWithFixRatio. The pre-charge raises the account's
	// energy_usage toward its limit in the VM-visible state BEFORE execution so a
	// contract reading its own caller/origin energy usage mid-VM (the staking-
	// query precompiles) observes the same charged value java does. It is undone
	// after the VM (restoreEnergyPreCharges) — java resetAccountUsage on success,
	// discard on revert — leaving the post-VM energy settle (PayEnergyBill) byte-
	// identical. Non-consensus carrier; never serialised.
	energyPreCharges []energyPreCharge

	NetUsage                      int64
	NetFee                        int64
	NetFeeForBandwidth            bool
	AssetIssueID                  string
	WithdrawAmount                int64
	UnfreezeAmount                int64
	WithdrawExpireAmount          int64
	CancelUnfreezeV2Amount        map[string]int64
	ExchangeReceivedAmount        int64
	ExchangeInjectAnotherAmount   int64
	ExchangeWithdrawAnotherAmount int64
	ShieldedTransactionFee        int64
	ExchangeID                    int64
	OrderID                       []byte
	OrderDetails                  []*corepb.MarketOrderDetail
	ContractResult                []byte
	ContractResultPresent         bool
	ContractAddress               []byte
	Logs                          []vm.Log
	InternalTransactions          []*corepb.InternalTransaction
	ContractRet                   int32
	ResMessage                    []byte
}

// newResult returns the caller-provided block-local result after clearing all
// scalar and reference fields. The nil-sink path preserves the public
// Execute/ApplyTransaction contract: the returned result is independently
// owned and remains valid after the next transaction executes.
func (ctx *Context) newResult() *Result {
	if ctx != nil && ctx.ResultSink != nil {
		*ctx.ResultSink = Result{}
		return ctx.ResultSink
	}
	return new(Result)
}

type Actuator interface {
	Validate(ctx *Context) error
	Execute(ctx *Context) (*Result, error)
}

// decodedContract returns the transaction-owned, read-only contract message.
// Validation, bandwidth charging, actuator execution and energy settlement can
// all inspect the first contract, so reusing one message avoids unmarshalling
// the same Any again in both Validate and Execute. Actuators that mutate their
// input must keep a private decode or clone instead.
func decodedContract[T proto.Message](ctx *Context, typeName string) (T, error) {
	var zero T
	if ctx.Tx.Contract() == nil {
		return zero, errors.New("no contract in transaction")
	}
	msg, err := ctx.Tx.DecodedContract()
	if err != nil {
		return zero, errors.New("failed to unmarshal " + typeName)
	}
	contract, ok := msg.(T)
	if !ok {
		return zero, errors.New("failed to unmarshal " + typeName)
	}
	return contract, nil
}

func CreateActuator(tx *types.Transaction) (Actuator, error) {
	ct := tx.ContractType()
	switch ct {
	case corepb.Transaction_Contract_AssetIssueContract:
		return &AssetIssueActuator{}, nil
	case corepb.Transaction_Contract_TransferContract:
		return &TransferActuator{}, nil
	case corepb.Transaction_Contract_TransferAssetContract:
		return &TransferAssetActuator{}, nil
	case corepb.Transaction_Contract_AccountCreateContract:
		return &CreateAccountActuator{}, nil
	case corepb.Transaction_Contract_WitnessCreateContract:
		return &WitnessCreateActuator{}, nil
	case corepb.Transaction_Contract_FreezeBalanceContract:
		return &FreezeBalanceActuator{}, nil
	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		return &UnfreezeBalanceActuator{}, nil
	case corepb.Transaction_Contract_FreezeBalanceV2Contract:
		return &FreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
		return &UnfreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_VoteWitnessContract:
		return &VoteWitnessActuator{}, nil
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		return &WithdrawBalanceActuator{}, nil
	case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
		return &WithdrawExpireUnfreezeActuator{}, nil
	case corepb.Transaction_Contract_CreateSmartContract:
		return &VMActuator{}, nil
	case corepb.Transaction_Contract_TriggerSmartContract:
		return &VMActuator{}, nil
	case corepb.Transaction_Contract_WitnessUpdateContract:
		return &WitnessUpdateActuator{}, nil
	case corepb.Transaction_Contract_UpdateSettingContract:
		return &UpdateSettingActuator{}, nil
	case corepb.Transaction_Contract_AccountUpdateContract:
		return &AccountUpdateActuator{}, nil
	case corepb.Transaction_Contract_SetAccountIdContract:
		return &SetAccountIdActuator{}, nil
	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		return &AccountPermissionUpdateActuator{}, nil
	case corepb.Transaction_Contract_UpdateEnergyLimitContract:
		return &UpdateEnergyLimitActuator{}, nil
	case corepb.Transaction_Contract_UpdateBrokerageContract:
		return &UpdateBrokerageActuator{}, nil
	case corepb.Transaction_Contract_ClearABIContract:
		return &ClearABIActuator{}, nil
	case corepb.Transaction_Contract_ProposalCreateContract:
		return &ProposalCreateActuator{}, nil
	case corepb.Transaction_Contract_ProposalApproveContract:
		return &ProposalApproveActuator{}, nil
	case corepb.Transaction_Contract_ProposalDeleteContract:
		return &ProposalDeleteActuator{}, nil
	case corepb.Transaction_Contract_DelegateResourceContract:
		return &DelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_UnDelegateResourceContract:
		return &UnDelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_CancelAllUnfreezeV2Contract:
		return &CancelAllUnfreezeV2Actuator{}, nil
	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		return &ParticipateAssetIssueActuator{}, nil
	case corepb.Transaction_Contract_UpdateAssetContract:
		return &UpdateAssetActuator{}, nil
	case corepb.Transaction_Contract_UnfreezeAssetContract:
		return &UnfreezeAssetActuator{}, nil
	case corepb.Transaction_Contract_MarketSellAssetContract:
		return &MarketSellAssetActuator{}, nil
	case corepb.Transaction_Contract_MarketCancelOrderContract:
		return &MarketCancelOrderActuator{}, nil
	case corepb.Transaction_Contract_ExchangeCreateContract:
		return &ExchangeCreateActuator{}, nil
	case corepb.Transaction_Contract_ExchangeInjectContract:
		return &ExchangeInjectActuator{}, nil
	case corepb.Transaction_Contract_ExchangeWithdrawContract:
		return &ExchangeWithdrawActuator{}, nil
	case corepb.Transaction_Contract_ExchangeTransactionContract:
		return &ExchangeTransactionActuator{}, nil
	case corepb.Transaction_Contract_ShieldedTransferContract:
		return &ShieldedTransferActuator{}, nil
	default:
		return nil, errors.New("unsupported contract type")
	}
}
