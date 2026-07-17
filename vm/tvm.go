package vm

import (
	"encoding/binary"
	"math"
	"sort"
	"strconv"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"golang.org/x/crypto/sha3"
)

// maxCallDepth mirrors java-tron Program.MAX_DEPTH = 64: the TVM caps the
// message-call/create stack at 64 nested frames, NOT the EVM's 1024. With no
// 63/64 energy reservation in TRON a deep self-recursion only terminates via
// this limit, so the geth value let recursion run ~10× deeper than java and
// flipped results (Nile block 11,359,658: java REVERT vs gtron OUT_OF_ENERGY).
// tvm.Depth is 1-based while a frame executes (runContract increments before
// Run); java's getCallDeep() is 0-based. java refuses a spawn when
// `getCallDeep() == MAX_DEPTH`, which maps to `tvm.Depth > maxCallDepth`.
const maxCallDepth = 64

// KVReadWriter is the narrow ethdb capability the TVM still needs for
// immutable chain data lookups such as BLOCKHASH. Mutable contract runtime
// state is stored through StateDB contract domains.
type KVReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// TVM is the top-level TVM execution context.
type TVM struct {
	StateDB              *state.StateDB
	DB                   KVReadWriter
	DynProps             *state.DynamicProperties
	Origin               tcommon.Address // tx.origin
	BlockNumber          uint64
	Timestamp            int64
	HeadSlot             int64
	HasHeadSlot          bool
	Coinbase             tcommon.Address // block producer
	ChainID              int64
	Depth                int    // call depth
	Nonce                uint64 // java-tron Program nonce for internal transactions
	RootTxID             tcommon.Hash
	GenesisHash          tcommon.Hash
	TrustTransactionRet  bool
	ExpectedContractRet  corepb.Transaction_ResultContractResult
	BlackholeAddress     tcommon.Address
	Logs                 []Log // accumulated log events from this execution
	InternalTransactions []*corepb.InternalTransaction

	cfg                 TVMConfig
	interpreter         *Interpreter
	newContracts        map[tcommon.Address]bool
	internalTxHashStack []tcommon.Hash
}

func (tvm *TVM) LogSnapshot() int {
	return len(tvm.Logs)
}

func (tvm *TVM) RevertLogs(snapshot int) {
	tvm.Logs = tvm.Logs[:snapshot]
}

func (tvm *TVM) InternalTransactionSnapshot() int {
	return len(tvm.InternalTransactions)
}

func (tvm *TVM) rejectInternalTransactionsFrom(snapshot int) {
	for i := snapshot; i < len(tvm.InternalTransactions); i++ {
		tvm.InternalTransactions[i].Rejected = true
	}
}

func (tvm *TVM) runContract(contract *Contract) ([]byte, error) {
	if contract.InternalTxHash.IsEmpty() {
		contract.InternalTxHash = tvm.RootTxID
	}
	tvm.internalTxHashStack = append(tvm.internalTxHashStack, contract.InternalTxHash)
	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--
	tvm.internalTxHashStack = tvm.internalTxHashStack[:len(tvm.internalTxHashStack)-1]
	return ret, err
}

func (tvm *TVM) contractVersion(addr tcommon.Address) int32 {
	if meta := tvm.StateDB.GetContract(addr); meta != nil {
		return meta.GetVersion()
	}
	return 0
}

func (tvm *TVM) defaultCreateVersion(caller tcommon.Address) int32 {
	if meta := tvm.StateDB.GetContract(caller); meta != nil {
		return meta.GetVersion()
	}
	if tvm.cfg.Compatibility {
		return 1
	}
	return 0
}

func (tvm *TVM) adjustedCallEnergy(contract *Contract, requested uint64) uint64 {
	available := contract.Energy
	if tvm.cfg.Compatibility && contract.Version == 1 {
		available -= available / 64
	}
	if requested > available {
		return available
	}
	return requested
}

func (tvm *TVM) adjustedCreateEnergy(contract *Contract) uint64 {
	available := contract.Energy
	if tvm.cfg.Compatibility && contract.Version == 1 {
		available -= available / 64
	}
	return available
}

func (tvm *TVM) currentInternalTxHash() tcommon.Hash {
	if n := len(tvm.internalTxHashStack); n > 0 {
		return tvm.internalTxHashStack[n-1]
	}
	return tvm.RootTxID
}

func (tvm *TVM) addInternalTransaction(caller, transferTo tcommon.Address, value int64, data []byte, note string, tokenID, tokenValue int64) *corepb.InternalTransaction {
	var tokenInfo map[string]int64
	if tokenID > 0 {
		tokenInfo = map[string]int64{strconv.FormatInt(tokenID, 10): tokenValue}
	}
	return tvm.addInternalTransactionWithTokenInfo(caller, transferTo, value, data, note, tokenInfo)
}

func (tvm *TVM) addInternalTransactionWithTokenInfo(caller, transferTo tcommon.Address, value int64, data []byte, note string, tokenInfo map[string]int64) *corepb.InternalTransaction {
	parentHash := tvm.currentInternalTxHash()
	receiveAddress := transferTo.Bytes()
	if note == "create" {
		receiveAddress = nil
	}

	var valueBytes [8]byte
	binary.BigEndian.PutUint64(valueBytes[:], uint64(value))
	raw := make([]byte, 0, len(parentHash)+len(receiveAddress)+len(data)+len(valueBytes))
	raw = append(raw, parentHash[:]...)
	raw = append(raw, receiveAddress...)
	raw = append(raw, data...)
	raw = append(raw, valueBytes[:]...)

	var nonceBytes [8]byte
	binary.BigEndian.PutUint64(nonceBytes[:], tvm.Nonce)
	hash := tcommon.Keccak256(append(raw, nonceBytes[:]...))

	it := &corepb.InternalTransaction{
		Hash:              hash.Bytes(),
		CallerAddress:     caller.Bytes(),
		TransferToAddress: transferTo.Bytes(),
		CallValueInfo: []*corepb.InternalTransaction_CallValueInfo{{
			CallValue: value,
		}},
		Note: []byte(note),
	}
	if len(tokenInfo) > 0 {
		tokenIDs := make([]string, 0, len(tokenInfo))
		for tokenID := range tokenInfo {
			tokenIDs = append(tokenIDs, tokenID)
		}
		sort.Strings(tokenIDs)
		for _, tokenID := range tokenIDs {
			it.CallValueInfo = append(it.CallValueInfo, &corepb.InternalTransaction_CallValueInfo{
				TokenId:   tokenID,
				CallValue: tokenInfo[tokenID],
			})
		}
	}
	tvm.InternalTransactions = append(tvm.InternalTransactions, it)
	return it
}

func (tvm *TVM) ResourceTime() int64 {
	if tvm != nil && tvm.HasHeadSlot {
		return tvm.HeadSlot
	}
	if tvm == nil {
		return 0
	}
	return tvm.Timestamp
}

// NewTVM creates a new TVM instance.
//
// dp may be nil for legacy/test paths that do not exercise the
// allow_tvm_solidity059 auto-create branch; production callers in
// actuator/vm_actuator.go and core/tron_backend.go must pass a real
// *DynamicProperties so the CALL/CALLTOKEN/SUICIDE → createNormalAccount
// parity (slice 2c) fires.
func NewTVM(stateDB *state.StateDB, dp *state.DynamicProperties, origin tcommon.Address, blockNum uint64, timestamp int64, coinbase tcommon.Address, chainID int64, cfg TVMConfig) *TVM {
	tvm := &TVM{
		StateDB:      stateDB,
		DynProps:     dp,
		Origin:       origin,
		BlockNumber:  blockNum,
		Timestamp:    timestamp,
		Coinbase:     coinbase,
		ChainID:      chainID,
		cfg:          cfg,
		newContracts: make(map[tcommon.Address]bool),
	}
	tvm.interpreter = NewInterpreter(tvm, cfg)
	return tvm
}

// SetDB sets the rawdb store used for access to per-contract state
// (ContractState for dynamic energy factor tracking, etc.).
func (tvm *TVM) SetDB(db KVReadWriter) {
	tvm.DB = db
}

// SetRootTransactionID sets the top-level transaction id used by java-tron
// for CREATE address derivation: keccak(rootTxID || int64 nonce), last 20
// bytes with the TRON prefix.
func (tvm *TVM) SetRootTransactionID(id tcommon.Hash) {
	tvm.RootTxID = id
}

// SetBlackholeAddress sets the genesis Blackhole account address for this
// chain. java-tron reads it from genesis, so custom networks cannot rely on a
// global constant.
func (tvm *TVM) SetBlackholeAddress(addr tcommon.Address) {
	tvm.BlackholeAddress = addr
}

func (tvm *TVM) blackholeAddress() tcommon.Address {
	if tvm != nil && !tvm.BlackholeAddress.IsEmpty() {
		return tvm.BlackholeAddress
	}
	return params.BlackholeAddress
}

// maybeCreateNormalAccountForValueTransfer mirrors java-tron
// `Program.createAccountIfNotExist` (Program.java:1874-1882) which is invoked
// from `Program.callToAddress` (1083) and `Program.suicide`/`suicide2`
// (483, 555) before the value transfer. The path is gated on
// `VMConfig.allowTvmSolidity059()`; the underlying
// `RepositoryImpl.createNormalAccount` (RepositoryImpl.java:1103-1114)
// stamps `Account.create_time = latestBlockHeaderTimestamp` and, when
// `AllowMultiSign` is set, installs default Owner/Active permissions.
//
// No-op if Solidity059 is off, the account already exists, or DP is nil
// (test paths that don't exercise this fork).
func (tvm *TVM) maybeCreateNormalAccountForValueTransfer(addr tcommon.Address) {
	if !tvm.cfg.Solidity059 {
		return
	}
	if tvm.DynProps == nil {
		return
	}
	if tvm.StateDB.AccountExists(addr) {
		return
	}
	tvm.StateDB.CreateAccountWithTime(addr, corepb.AccountType_Normal, tvm.DynProps.LatestBlockHeaderTimestamp())
	if tvm.DynProps.AllowMultiSign() {
		tvm.StateDB.ApplyDefaultAccountPermissions(addr, tvm.DynProps)
	}
}

// validatePrecompileEndowment mirrors the transfer leg of java-tron
// Program.callToPrecompiledAddress for a TRX endowment: MUtil.transfer ->
// VMUtils.validateForSmartContract requires the TARGET account to already
// exist ("Validate InternalTransfer error, no ToAccount...") and the credit
// not to overflow long. Precompile addresses normally have no account and
// are never auto-created on this path, so a value-bearing CALL into one
// throws BytecodeExecutionException("transfer failure") in java — which is
// not a TransferException, so VM.play spends ALL energy and the receipt
// records UNKNOWN (Nile block 18,112,819, contract "Test".test(address(2))).
// Returns nil for non-precompile targets; java's plain-contract path
// auto-creates the recipient instead.
func (tvm *TVM) validatePrecompileEndowment(addr tcommon.Address, value int64) error {
	if getPrecompile(addr, tvm.cfg, tvm.GenesisHash) == nil {
		return nil
	}
	if !tvm.StateDB.AccountExists(addr) {
		return ErrPrecompileTransferFailure
	}
	if tvm.StateDB.GetBalance(addr) > math.MaxInt64-value {
		return ErrPrecompileTransferFailure
	}
	return nil
}

// transferDelegatedResourceToInheritor mirrors java-tron
// Program.transferDelegatedResourceToInheritor (Program.java:588-618), invoked
// from suicide()/suicide2() when allow_tvm_freeze is active. It releases the
// destroyed contract's V1 frozen bandwidth (the first frozen slot only, per
// java's getFrozenList().get(0) guarded by getFrozenCount() != 0) and its V1
// frozen energy from the global total_net_weight/total_energy_weight, credits
// their summed balance to the inheritor, and — only under
// allow_tvm_selfdestruct_restriction — zeroes the owner's frozen slots in place
// (clearOwnerFreeze). The caller decides the inheritor: the blackhole address
// when owner == obtainer, otherwise the obtainer.
//
// Omitting this release is what drifted go-tron's total_energy_weight above
// java-tron's and over-billed contract-origin energy at Nile block 19,716,962.
func (tvm *TVM) transferDelegatedResourceToInheritor(owner, inheritor tcommon.Address) {
	ownerAccount := tvm.StateDB.GetAccount(owner)
	if ownerAccount == nil {
		return
	}

	var frozenBalanceForBandwidth int64
	if frozen := ownerAccount.FrozenBandwidthList(); len(frozen) != 0 {
		frozenBalanceForBandwidth = frozen[0].FrozenBalance
	}
	frozenBalanceForEnergy := ownerAccount.FrozenEnergyAmount()

	// Journaled so a reverting frame rolls the release back, matching java's
	// discardable Repository (see StateDB.AddResourceWeightJournaled).
	tvm.StateDB.AddResourceWeightJournaled(tvm.DynProps, corepb.ResourceCode_BANDWIDTH, -frozenBalanceForBandwidth/tvmTRXPrecision)
	tvm.StateDB.AddResourceWeightJournaled(tvm.DynProps, corepb.ResourceCode_ENERGY, -frozenBalanceForEnergy/tvmTRXPrecision)
	// java unconditionally calls repo.addBalance(inheritor, sum), but in the
	// suicide flow the inheritor always pre-exists (createAccountIfNotExist for
	// the obtainer, genesis for the blackhole), so addBalance(inheritor, 0) is a
	// no-op. Guard on a positive credit so a zero-frozen contract (the common
	// case) does not spuriously materialise a bare inheritor account here —
	// go-tron's AddBalance would GetOrCreate it — keeping this change scoped to
	// the weight release.
	if sum := frozenBalanceForBandwidth + frozenBalanceForEnergy; sum > 0 {
		tvm.StateDB.AddBalance(inheritor, sum)
	}

	if tvm.cfg.SelfdestructRestrict {
		tvm.StateDB.ClearV1Freeze(owner)
	}
}

// transferFrozenV2BalanceToInheritor mirrors java-tron
// Program.transferFrozenV2BalanceToInheritor (Program.java:620-681), invoked
// from suicide()/suicide2() when allow_tvm_freeze_v2 (Stake 2.0) is active. It
// moves the destroyed contract's self-frozen V2 balances (BANDWIDTH/ENERGY/
// TRON_POWER) to the inheritor, folds the owner's recovered resource usage into
// the inheritor's recovery window (unDelegateIncrease), withdraws any expired
// pending V2 unfreeze to the inheritor's liquid balance, and clears the owner's
// V2 freeze/usage/window/unfreeze state (clearOwnerFreezeV2). Unlike the V1
// release the global total_net_weight/total_energy_weight is left untouched: the
// frozen weight follows the balance to the inheritor. Returns the expired
// unfreeze balance, which the caller adds to the suicide internal-tx value.
func (tvm *TVM) transferFrozenV2BalanceToInheritor(owner, inheritor tcommon.Address) int64 {
	ownerAccount := tvm.StateDB.GetAccount(owner)
	if ownerAccount == nil {
		return 0
	}
	// java reads inheritorCapsule = repo.getAccount(inheritor) after the obtainer
	// was materialised by createAccountIfNotExist in the balance-transfer step
	// (always active alongside allow_tvm_freeze_v2). Ensure it exists so the
	// frozen-V2 move and the usage fold are not silently dropped: AddFreezeV2 and
	// the fold no-op on a missing account, unlike AddBalance which GetOrCreates.
	tvm.maybeCreateNormalAccountForValueTransfer(inheritor)

	// 1. Move the owner's self-frozen V2 balances to the inheritor (java
	//    getFrozenV2List().forEach addFrozenBalanceForXxxV2). The global weight is
	//    conserved — owner loses, inheritor gains — so there is no addTotal*Weight.
	for _, resource := range []corepb.ResourceCode{
		corepb.ResourceCode_BANDWIDTH,
		corepb.ResourceCode_ENERGY,
		corepb.ResourceCode_TRON_POWER,
	} {
		if amount := ownerAccount.GetFrozenV2Amount(resource); amount > 0 {
			tvm.StateDB.AddFreezeV2(inheritor, resource, amount)
		}
	}

	// 2. Fold the owner's recovered usage windows into the inheritor
	//    (updateUsageForDelegated/updateUsage + unDelegateIncrease).
	now := tvm.ResourceTime()
	delegation.MergeUsageToInheritor(tvm.StateDB, tvm.DynProps, owner, inheritor, corepb.ResourceCode_BANDWIDTH, now)
	delegation.MergeUsageToInheritor(tvm.StateDB, tvm.DynProps, owner, inheritor, corepb.ResourceCode_ENERGY, now)

	// 3. Withdraw the owner's expired pending V2 unfreezes to the inheritor.
	var expireUnfrozenBalance int64
	nowTimestamp := tvm.DynProps.LatestBlockHeaderTimestamp()
	for _, u := range ownerAccount.UnfrozenV2() {
		if u.UnfreezeAmount > 0 && u.UnfreezeExpireTime <= nowTimestamp {
			expireUnfrozenBalance += u.UnfreezeAmount
		}
	}
	if expireUnfrozenBalance > 0 {
		tvm.StateDB.AddBalance(inheritor, expireUnfrozenBalance)
		tvm.Nonce++
		tvm.addInternalTransaction(owner, inheritor, expireUnfrozenBalance, nil, "withdrawExpireUnfreezeWhileSuiciding", 0, 0)
	}

	// 4. clearOwnerFreezeV2: zero the owner's V2 freeze/usage/window/unfreeze.
	tvm.StateDB.ClearV2Freeze(owner)
	return expireUnfrozenBalance
}

// canSelfDestruct mirrors java-tron OperationActions.suicideAction/suicideAction2's
// canSuicide()/canSuicide2() guard (Program.java): a contract still holding frozen
// resources cannot self-destruct — the SELFDESTRUCT reverts. oldSuicide selects
// canSuicide() (only the delegated-V1 check) vs canSuicide2() (also rejects the
// owner's OWN unexpired V1 frozen bandwidth/energy). Both additionally run the V2
// check (allow_tvm_freeze_v2): reject any delegated-V2 balance or unexpired pending
// V2 unfreeze. Returns true (allowed) when the relevant forks are inactive. Without
// this guard go-tron destroys a contract java would revert, and reaches the
// inheritor-transfer with non-zero delegated balance java never sees.
func (tvm *TVM) canSelfDestruct(owner tcommon.Address, oldSuicide bool) bool {
	acct := tvm.StateDB.GetAccount(owner)
	if acct == nil {
		return true
	}
	var now int64
	if tvm.DynProps != nil {
		now = tvm.DynProps.LatestBlockHeaderTimestamp()
	}
	if tvm.cfg.Freeze {
		if !oldSuicide {
			// canSuicide2 (freezeV1Check): reject the owner's own unexpired V1 frozen.
			for _, f := range acct.FrozenBandwidthList() {
				if f.GetExpireTime() > now {
					return false
				}
			}
			if acct.FrozenEnergyAmount() > 0 && acct.FrozenEnergyExpireTime() > now {
				return false
			}
		}
		// canSuicide and canSuicide2: reject delegated V1.
		if acct.DelegatedFrozenBandwidth() != 0 || acct.DelegatedFrozenEnergy() != 0 {
			return false
		}
	}
	if tvm.cfg.StakingV2 {
		// freezeV2Check (allow_tvm_freeze_v2): reject delegated V2 + unexpired V2 unfreeze.
		if acct.DelegatedFrozenV2BalanceForBandwidth() != 0 || acct.DelegatedFrozenV2BalanceForEnergy() != 0 {
			return false
		}
		for _, u := range acct.UnfrozenV2() {
			if u.GetUnfreezeExpireTime() > now {
				return false
			}
		}
	}
	return true
}

// Create deploys a new contract.
func (tvm *TVM) Create(caller tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	contractAddr := tvm.createAddress(tvm.Nonce)
	tvm.Nonce++

	return tvm.create(caller, contractAddr, code, energy, value, 0, 0, true, false, nil, tvm.defaultCreateVersion(caller))
}

func (tvm *TVM) createWithVersion(caller tcommon.Address, code []byte, energy uint64, value int64, version int32) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	contractAddr := tvm.createAddress(tvm.Nonce)
	tvm.Nonce++

	return tvm.create(caller, contractAddr, code, energy, value, 0, 0, true, false, nil, version)
}

// CreateAt deploys a top-level contract at a caller-supplied address. TRON
// external CreateSmartContract transactions derive the address from the
// transaction raw-data hash and owner address in the actuator, while VM CREATE
// opcodes continue to use Create's nonce-based derivation.
func (tvm *TVM) CreateAt(caller, contractAddr tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}
	return tvm.create(caller, contractAddr, code, energy, value, 0, 0, false, false, nil, 0)
}

// CreateAtWithToken deploys a top-level contract with TRC-10 message context.
// External CreateSmartContract transactions in java-tron transfer call_value
// and call_token_value to the new contract before constructor execution, and
// ProgramInvoke exposes tokenId/tokenValue through CALLTOKENID/CALLTOKENVALUE.
func (tvm *TVM) CreateAtWithToken(caller, contractAddr tcommon.Address, code []byte, energy uint64, value int64, tokenID int64, tokenValue int64) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}
	return tvm.create(caller, contractAddr, code, energy, value, tokenID, tokenValue, false, false, nil, 0)
}

// CreateAtWithTokenAndContract deploys a top-level contract after preloading
// the SmartContract metadata that java-tron exposes during constructor
// execution.
func (tvm *TVM) CreateAtWithTokenAndContract(caller, contractAddr tcommon.Address, code []byte, energy uint64, value int64, tokenID int64, tokenValue int64, contractMeta *contractpb.SmartContract) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}
	return tvm.create(caller, contractAddr, code, energy, value, tokenID, tokenValue, false, false, contractMeta, 0)
}

// Create2 deploys a new contract with a deterministic address.
func (tvm *TVM) Create2(caller tcommon.Address, code []byte, energy uint64, value int64, salt [32]byte) ([]byte, tcommon.Address, uint64, error) {
	if (tvm.cfg.Compatibility || tvm.cfg.Osaka) && tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	contractAddr := create2Address(caller, code, salt)

	tvm.Nonce++
	return tvm.create(caller, contractAddr, code, energy, value, 0, 0, true, true, nil, tvm.defaultCreateVersion(caller))
}

func (tvm *TVM) create2WithVersion(caller, addressSeed tcommon.Address, code []byte, energy uint64, value int64, salt [32]byte, version int32) ([]byte, tcommon.Address, uint64, error) {
	if (tvm.cfg.Compatibility || tvm.cfg.Osaka) && tvm.Depth > maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	contractAddr := create2Address(addressSeed, code, salt)

	tvm.Nonce++
	return tvm.create(caller, contractAddr, code, energy, value, 0, 0, true, true, nil, version)
}

func create2Address(seed tcommon.Address, code []byte, salt [32]byte) tcommon.Address {
	codeHash := keccak256(code)
	var buf []byte
	buf = append(buf, seed[:]...)
	buf = append(buf, salt[:]...)
	buf = append(buf, codeHash[:]...)
	hash := keccak256(buf)

	var contractAddr tcommon.Address
	contractAddr[0] = 0x41
	copy(contractAddr[1:], hash[12:32])
	return contractAddr
}

func (tvm *TVM) createAddress(nonce uint64) tcommon.Address {
	var nonceBytes [8]byte
	binary.BigEndian.PutUint64(nonceBytes[:], nonce)
	buf := make([]byte, 0, len(tvm.RootTxID)+len(nonceBytes))
	buf = append(buf, tvm.RootTxID[:]...)
	buf = append(buf, nonceBytes[:]...)
	hash := tcommon.Keccak256(buf)

	var addr tcommon.Address
	addr[0] = 0x41
	copy(addr[1:], hash[12:])
	return addr
}

func keccak256(data []byte) [32]byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

func (tvm *TVM) create(caller tcommon.Address, contractAddr tcommon.Address, code []byte, energy uint64, value int64, tokenID int64, tokenValue int64, internal bool, isCreate2 bool, contractMeta *contractpb.SmartContract, contractVersion int32) (data []byte, newAddr tcommon.Address, leftover uint64, outErr error) {
	if tracer := tvm.cfg.Tracer; tracer != nil {
		createOp := CREATE
		if isCreate2 {
			createOp = CREATE2
		}
		tvm.captureFrameStart(tracer, createOp, caller, contractAddr, true, code, energy, value)
		defer func() { tvm.captureFrameEnd(tracer, data, energy-leftover, outErr) }()
	}
	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()
	internalTxSnap := tvm.InternalTransactionSnapshot()

	if value > 0 && tvm.StateDB.GetBalance(caller) < value {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, energy, ErrInsufficientBalance
	}
	if tokenValue > 0 && tokenID > 0 && tvm.StateDB.GetTRC10Balance(caller, tokenID) < tokenValue {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, energy, ErrInsufficientBalance
	}
	if tvm.StateDB.AccountExists(contractAddr) && tvm.StateDB.IsContract(contractAddr) {
		if internal {
			tvm.addInternalTransaction(caller, contractAddr, value, code, "create", 0, 0)
			tvm.rejectInternalTransactionsFrom(internalTxSnap)
		}
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrContractAlreadyExists
	}

	if internal {
		tvm.createInternalContractAccount(caller, contractAddr, isCreate2, contractVersion)
	} else {
		tvm.createExternalContractAccount(caller, contractAddr, contractMeta)
		if !tvm.cfg.Constantinople {
			tvm.StateDB.SetCode(contractAddr, legacyCreateContractCode(code))
		}
	}
	wasNew := tvm.newContracts[contractAddr]
	tvm.newContracts[contractAddr] = true

	if value > 0 {
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.restoreNewContractMark(contractAddr, wasNew)
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, tcommon.Address{}, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(contractAddr, value)
	}
	if tokenValue > 0 && tokenID > 0 {
		if err := tvm.StateDB.SubTRC10Balance(caller, tokenID, tokenValue); err != nil {
			tvm.restoreNewContractMark(contractAddr, wasNew)
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, tcommon.Address{}, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddTRC10Balance(contractAddr, tokenID, tokenValue)
	}

	var internalTx *corepb.InternalTransaction
	if internal {
		internalTx = tvm.addInternalTransaction(caller, contractAddr, value, code, "create", 0, 0)
	}

	contract := NewContract(caller, contractAddr, value, energy)
	contract.Version = tvm.contractVersion(contractAddr)
	if internalTx != nil {
		contract.InternalTxHash = tcommon.BytesToHash(internalTx.Hash)
	} else {
		contract.InternalTxHash = tvm.RootTxID
	}
	contract.TokenID = tokenID
	contract.TokenValue = tokenValue
	contract.SetCode(contractAddr, code)

	ret, err := tvm.runContract(contract)

	if err != nil {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		tvm.restoreNewContractMark(contractAddr, wasNew)
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted || isTransferFailure(err) {
			return ret, tcommon.Address{}, contract.Energy, err
		}
		return nil, tcommon.Address{}, 0, err
	}

	if len(ret) != 0 && tvm.cfg.London && ret[0] == 0xEF {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		tvm.restoreNewContractMark(contractAddr, wasNew)
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return ret, tcommon.Address{}, 0, ErrInvalidCode
	}

	depositCost := uint64(len(ret)) * EnergyCodeDeposit
	if !contract.UseEnergy(depositCost) {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		tvm.restoreNewContractMark(contractAddr, wasNew)
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrOutOfEnergy
	}

	if internal || tvm.cfg.Constantinople {
		tvm.StateDB.SetCode(contractAddr, ret)
	}
	return ret, contractAddr, contract.Energy, nil
}

func legacyCreateContractCode(ops []byte) []byte {
	for i := 0; i < len(ops); i++ {
		op := OpCode(ops[i])
		if op == RETURN && i+1 < len(ops) && OpCode(ops[i+1]) == STOP {
			code := make([]byte, len(ops)-i-2)
			copy(code, ops[i+2:])
			return code
		}
		if op >= PUSH1 && op <= PUSH32 {
			i += int(op-PUSH1) + 1
		}
	}
	return make([]byte, 32)
}

func (tvm *TVM) createInternalContractAccount(origin, contractAddr tcommon.Address, isCreate2 bool, contractVersion int32) {
	existed := tvm.StateDB.AccountExists(contractAddr)
	tvm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	if existed {
		tvm.StateDB.ClearAcquiredDelegatedResource(contractAddr)
	} else {
		tvm.StateDB.SetAccountName(contractAddr, "CreatedByContract")
	}

	meta := &contractpb.SmartContract{
		OriginAddress:              origin.Bytes(),
		ContractAddress:            contractAddr.Bytes(),
		ConsumeUserResourcePercent: 100,
	}
	if tvm.cfg.Compatibility {
		meta.Version = contractVersion
	}
	if isCreate2 {
		meta.TrxHash = tvm.RootTxID.Bytes()
	}
	tvm.StateDB.SetContract(contractAddr, meta)
}

func (tvm *TVM) createExternalContractAccount(origin, contractAddr tcommon.Address, contractMeta *contractpb.SmartContract) {
	tvm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	if contractMeta == nil {
		contractMeta = &contractpb.SmartContract{
			OriginAddress:              origin.Bytes(),
			ContractAddress:            contractAddr.Bytes(),
			ConsumeUserResourcePercent: 100,
		}
		if tvm.cfg.Compatibility {
			contractMeta.Version = 1
		}
	}
	if contractMeta.ContractAddress == nil {
		contractMeta.ContractAddress = contractAddr.Bytes()
	}
	tvm.StateDB.SetAccountName(contractAddr, contractMeta.GetName())
	tvm.StateDB.SetContract(contractAddr, contractMeta)
}

func (tvm *TVM) restoreNewContractMark(addr tcommon.Address, wasNew bool) {
	if wasNew {
		tvm.newContracts[addr] = true
		return
	}
	delete(tvm.newContracts, addr)
}

func (tvm *TVM) isNewContract(addr tcommon.Address) bool {
	return tvm.newContracts[addr]
}

// Call executes a contract call.
func (tvm *TVM) Call(caller, addr tcommon.Address, input []byte, energy uint64, value int64) (data []byte, leftover uint64, outErr error) {
	if tracer := tvm.cfg.Tracer; tracer != nil {
		tvm.captureFrameStart(tracer, CALL, caller, addr, false, input, energy, value)
		defer func() { tvm.captureFrameEnd(tracer, data, energy-leftover, outErr) }()
	}
	if tvm.Depth > maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()
	internalTxSnap := tvm.InternalTransactionSnapshot()

	if value > 0 {
		// java-tron Program.callToAddress (Program.java:1081-1083):
		// createAccountIfNotExist is gated on TRX endowment > 0. Java
		// dispatches precompile targets to `callToPrecompiledAddress`
		// BEFORE entering callToAddress (OperationActions.java:1034-1042),
		// so precompile addresses never reach `createAccountIfNotExist` —
		// guard with the same precompile check to preserve wire format.
		if getPrecompile(addr, tvm.cfg, tvm.GenesisHash) == nil {
			tvm.maybeCreateNormalAccountForValueTransfer(addr)
		}
		if tvm.StateDB.GetBalance(caller) < value {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		if caller == addr {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrTransferFailed
		}
		if err := tvm.validatePrecompileEndowment(addr, value); err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(addr, value)
	}

	// Check for precompiled contract
	if p := getPrecompile(addr, tvm.cfg, tvm.GenesisHash); p != nil {
		ret, energyUsed, success, err := runPrecompile(tvm, p, caller, input, energy)
		if err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		if !success {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return ret, 0, errPrecompileFailure
		}
		remaining := energy - energyUsed
		return ret, remaining, nil
	}

	if tvm.Depth > 0 {
		tvm.Nonce++
	}
	var internalTx *corepb.InternalTransaction
	if tvm.Depth > 0 {
		internalTx = tvm.addInternalTransaction(caller, addr, value, input, "call", 0, 0)
	}
	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, value, energy)
	contract.Version = tvm.contractVersion(addr)
	if internalTx != nil {
		contract.InternalTxHash = tcommon.BytesToHash(internalTx.Hash)
	} else {
		contract.InternalTxHash = tvm.RootTxID
	}
	contract.CodeHash = tvm.StateDB.GetCodeHash(addr) // reuse state's keccak(code) to key the jumpdest cache
	contract.SetCode(addr, code)
	contract.SetInput(input)

	ret, err := tvm.runContract(contract)

	tvm.interpreter.returnData = ret

	if err != nil {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		// A transfer failure aborts only the frame that raised it. java-tron
		// surfaces TRANSFER_FAILED solely when that frame is the entry frame
		// (RuntimeImpl maps the entry result's TransferException); for a nested
		// frame VM.play stores the exception in the child result and the caller's
		// CALL opcode pushes 0 (Program.java:1157-1168) and continues, billed the
		// full forwarded energy with no refund. Only surface it at Depth 0;
		// otherwise hand back a childCallFailure so the caller's opCall pushes 0.
		// (Nile 23,077,310 tx a5580051… expected REVERT, not TRANSFER_FAILED.)
		if isTransferFailure(err) && tvm.Depth == 0 {
			return ret, contract.Energy, err
		}
		if tvm.Depth == 0 {
			return nil, 0, err
		}
		return nil, 0, childCallFailure(err)
	}
	return ret, contract.Energy, nil
}

// CallToken executes a contract call with a TRC-10 token transfer.
func (tvm *TVM) CallToken(caller, addr tcommon.Address, input []byte, energy uint64, value int64, tokenID int64, tokenValue int64) (data []byte, leftover uint64, outErr error) {
	if tracer := tvm.cfg.Tracer; tracer != nil {
		tvm.captureFrameStart(tracer, CALLTOKEN, caller, addr, false, input, energy, value)
		defer func() { tvm.captureFrameEnd(tracer, data, energy-leftover, outErr) }()
	}
	if tvm.Depth > maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()
	internalTxSnap := tvm.InternalTransactionSnapshot()

	if value > 0 {
		// Mirror java-tron `endowment > 0` gate for TRX value transfers
		// (Program.java:1081-1083). Precompile-targeted calls take a
		// different java dispatch path
		// (OperationActions.java:1034-1042 → callToPrecompiledAddress) and
		// don't materialize the destination account; skip the helper to
		// preserve that wire format.
		if getPrecompile(addr, tvm.cfg, tvm.GenesisHash) == nil {
			tvm.maybeCreateNormalAccountForValueTransfer(addr)
		}
		if tvm.StateDB.GetBalance(caller) < value {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		if caller == addr {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrTransferFailed
		}
		if err := tvm.validatePrecompileEndowment(addr, value); err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(addr, value)
	}
	if tokenValue > 0 && tokenID > 0 {
		if getPrecompile(addr, tvm.cfg, tvm.GenesisHash) == nil {
			tvm.maybeCreateNormalAccountForValueTransfer(addr)
		}
		if tvm.StateDB.GetTRC10Balance(caller, tokenID) < tokenValue {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		if caller == addr {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrTokenTransferFailed
		}
		if !tvm.StateDB.AccountExists(addr) {
			// Symmetric to the TRX leg's validatePrecompileEndowment: java's
			// callToPrecompiledAddress token path runs
			// VMUtils.validateForSmartContract(..., tokenId, amount), which
			// rejects a destination with no account ("...no ToAccount. And not
			// allowed to create account in smart contract.", VMUtils.java:241)
			// and is re-thrown as BytecodeExecutionException("transfer failure")
			// (Program.java:1710-1716) — NOT a TransferException, so VM.play
			// spends ALL energy and the receipt records UNKNOWN. Precompile
			// addresses are never auto-created on this path, so surface
			// ErrPrecompileTransferFailure (propagated by shouldPropagateCallError
			// → spend-all) instead of the swallowed ErrInsufficientBalance.
			// Plain-contract/plain-address targets are pre-created above by
			// maybeCreateNormalAccountForValueTransfer, so they never reach here.
			if getPrecompile(addr, tvm.cfg, tvm.GenesisHash) != nil {
				tvm.RevertLogs(logSnap)
				tvm.StateDB.RevertToSnapshot(snap)
				return nil, 0, ErrPrecompileTransferFailure
			}
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		if err := tvm.StateDB.SubTRC10Balance(caller, tokenID, tokenValue); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddTRC10Balance(addr, tokenID, tokenValue)
	}

	// Check for precompiled contract
	if p := getPrecompile(addr, tvm.cfg, tvm.GenesisHash); p != nil {
		ret, energyUsed, success, err := runPrecompile(tvm, p, caller, input, energy)
		if err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		if !success {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return ret, 0, errPrecompileFailure
		}
		remaining := energy - energyUsed
		return ret, remaining, nil
	}

	if tvm.Depth > 0 {
		tvm.Nonce++
	}
	var internalTx *corepb.InternalTransaction
	if tvm.Depth > 0 {
		callValue := value
		internalTokenID := int64(0)
		internalTokenValue := int64(0)
		if tokenID > 0 {
			callValue = 0
			internalTokenID = tokenID
			internalTokenValue = tokenValue
		}
		internalTx = tvm.addInternalTransaction(caller, addr, callValue, input, "call", internalTokenID, internalTokenValue)
	}
	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, value, energy)
	contract.Version = tvm.contractVersion(addr)
	if internalTx != nil {
		contract.InternalTxHash = tcommon.BytesToHash(internalTx.Hash)
	} else {
		contract.InternalTxHash = tvm.RootTxID
	}
	contract.CodeHash = tvm.StateDB.GetCodeHash(addr) // reuse state's keccak(code) to key the jumpdest cache
	contract.SetCode(addr, code)
	contract.SetInput(input)
	contract.TokenID = tokenID
	contract.TokenValue = tokenValue

	ret, err := tvm.runContract(contract)

	tvm.interpreter.returnData = ret

	if err != nil {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		// Transfer failures (e.g. "Cannot transfer TRX/TRC10 to yourself")
		// must keep the remaining energy, exactly like ErrExecutionReverted and
		// the Call path above. java-tron refunds the message energy on a
		// transfer failure (Program.callToAddress → refundEnergy) and only
		// bills the energy actually executed; billing the full limit here
		// drained the caller and broke cross-impl sync (stress harness ~blk 90).
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		// A transfer failure aborts only the frame that raised it. java-tron
		// surfaces TRANSFER_FAILED solely when that frame is the entry frame
		// (RuntimeImpl maps the entry result's TransferException); for a nested
		// frame VM.play stores the exception in the child result and the caller's
		// CALL opcode pushes 0 (Program.java:1157-1168) and continues, billed the
		// full forwarded energy with no refund. Only surface it at Depth 0;
		// otherwise hand back a childCallFailure so the caller's opCall pushes 0.
		// (Nile 23,077,310 tx a5580051… expected REVERT, not TRANSFER_FAILED.)
		if isTransferFailure(err) && tvm.Depth == 0 {
			return ret, contract.Energy, err
		}
		if tvm.Depth == 0 {
			return nil, 0, err
		}
		return nil, 0, childCallFailure(err)
	}
	return ret, contract.Energy, nil
}

// StaticCall executes a call without state modifications.
func (tvm *TVM) StaticCall(caller, addr tcommon.Address, input []byte, energy uint64) (data []byte, leftover uint64, outErr error) {
	if tracer := tvm.cfg.Tracer; tracer != nil {
		tvm.captureFrameStart(tracer, STATICCALL, caller, addr, false, input, energy, 0)
		defer func() { tvm.captureFrameEnd(tracer, data, energy-leftover, outErr) }()
	}
	if tvm.Depth > maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	if p := getPrecompile(addr, tvm.cfg, tvm.GenesisHash); p != nil {
		ret, energyUsed, success, err := runPrecompile(tvm, p, caller, input, energy)
		if err != nil {
			return nil, 0, err
		}
		if !success {
			return ret, 0, errPrecompileFailure
		}
		remaining := energy - energyUsed
		return ret, remaining, nil
	}

	if tvm.Depth > 0 {
		tvm.Nonce++
	}
	internalTxSnap := tvm.InternalTransactionSnapshot()
	var internalTx *corepb.InternalTransaction
	if tvm.Depth > 0 {
		internalTx = tvm.addInternalTransaction(caller, addr, 0, input, "call", 0, 0)
	}
	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, 0, energy)
	contract.Version = tvm.contractVersion(addr)
	if internalTx != nil {
		contract.InternalTxHash = tcommon.BytesToHash(internalTx.Hash)
	} else {
		contract.InternalTxHash = tvm.RootTxID
	}
	contract.CodeHash = tvm.StateDB.GetCodeHash(addr) // reuse state's keccak(code) to key the jumpdest cache
	contract.SetCode(addr, code)
	contract.SetInput(input)

	prevReadOnly := tvm.interpreter.readOnly
	tvm.interpreter.readOnly = true

	ret, err := tvm.runContract(contract)

	tvm.interpreter.readOnly = prevReadOnly
	tvm.interpreter.returnData = ret

	// Unlike Call/CallToken/DelegateCall, StaticCall deliberately omits the
	// isTransferFailure(err) branch: a static frame is readOnly, so opCall /
	// opCallToken reject any value or token transfer with ErrWriteProtection
	// before reaching the caller == addr check that yields ErrTransferFailed /
	// ErrTokenTransferFailed. The transfer-failure path is unreachable here, so
	// there is no message energy to refund.
	if err != nil && err != ErrExecutionReverted {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		if tvm.Depth == 0 {
			return nil, 0, err
		}
		return nil, 0, childCallFailure(err)
	}
	if err == ErrExecutionReverted {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
	}
	return ret, contract.Energy, err
}

// DelegateCall executes code at addr while preserving the supplied message
// caller and storage/address context. java-tron keeps these separate:
// DELEGATECALL uses the parent caller plus the current contract context,
// while CALLCODE uses the current contract for both.
func (tvm *TVM) DelegateCall(caller, context, addr tcommon.Address, input []byte, energy uint64, value int64, internalValue int64) (data []byte, leftover uint64, outErr error) {
	if tracer := tvm.cfg.Tracer; tracer != nil {
		tvm.captureFrameStart(tracer, DELEGATECALL, caller, addr, false, input, energy, value)
		defer func() { tvm.captureFrameEnd(tracer, data, energy-leftover, outErr) }()
	}
	if tvm.Depth > maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	if p := getPrecompile(addr, tvm.cfg, tvm.GenesisHash); p != nil {
		ret, energyUsed, success, err := runPrecompile(tvm, p, caller, input, energy)
		if err != nil {
			return nil, 0, err
		}
		if !success {
			return ret, 0, errPrecompileFailure
		}
		remaining := energy - energyUsed
		return ret, remaining, nil
	}

	if tvm.Depth > 0 {
		tvm.Nonce++
	}
	internalTxSnap := tvm.InternalTransactionSnapshot()
	var internalTx *corepb.InternalTransaction
	if tvm.Depth > 0 {
		internalTx = tvm.addInternalTransaction(context, context, internalValue, input, "call", 0, 0)
	}
	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, context, value, energy)
	contract.Version = tvm.contractVersion(addr)
	if internalTx != nil {
		contract.InternalTxHash = tcommon.BytesToHash(internalTx.Hash)
	} else {
		contract.InternalTxHash = tvm.RootTxID
	}
	contract.CodeHash = tvm.StateDB.GetCodeHash(addr) // reuse state's keccak(code) to key the jumpdest cache
	contract.SetCode(addr, code)
	contract.SetInput(input)

	ret, err := tvm.runContract(contract)

	tvm.interpreter.returnData = ret

	if err != nil {
		tvm.rejectInternalTransactionsFrom(internalTxSnap)
		// A transfer failure here means the delegated code (running in the
		// parent, non-readOnly context) issued a CALL/CALLTOKEN that moved
		// TRX/TRC10 to the context address itself (caller == addr). Mirror
		// Call/CallToken/create: keep the remaining energy, exactly like
		// ErrExecutionReverted. java-tron refunds the message energy on a
		// transfer failure (Program.callToAddress → refundEnergy) and bills
		// only the energy actually executed; billing the full limit here would
		// drain the caller and break cross-impl consensus.
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		// A transfer failure aborts only the frame that raised it. java-tron
		// surfaces TRANSFER_FAILED solely when that frame is the entry frame
		// (RuntimeImpl maps the entry result's TransferException); for a nested
		// frame VM.play stores the exception in the child result and the caller's
		// CALL opcode pushes 0 (Program.java:1157-1168) and continues, billed the
		// full forwarded energy with no refund. Only surface it at Depth 0;
		// otherwise hand back a childCallFailure so the caller's opCall pushes 0.
		// (Nile 23,077,310 tx a5580051… expected REVERT, not TRANSFER_FAILED.)
		if isTransferFailure(err) && tvm.Depth == 0 {
			return ret, contract.Energy, err
		}
		if tvm.Depth == 0 {
			return nil, 0, err
		}
		return nil, 0, childCallFailure(err)
	}
	return ret, contract.Energy, err
}
