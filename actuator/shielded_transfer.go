package actuator

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShieldedTransferActuator handles shielded (Sapling) ZEN token transfers.
type ShieldedTransferActuator struct{}

const (
	zcEncCiphertextSize = 580
	zcOutCiphertextSize = 80
	zcElementSize       = 32
	zcProofSize         = 192
	zcSignatureSize     = 64

	// Nile's first Sapling-era shielded transfers were produced while
	// java-tron was still changing proof/sign-hash handling. Until the full
	// historical verifier is ported, replay signed Nile blocks in fee-only mode
	// from the first observed shielded block forward: preserve transparent ZEN
	// and fee accounting, but do not trust or persist anonymous note state.
	historicalNileShieldedFeeOnlyStartBlock = 1_685_186

	// These first observed Nile shielded transfers predate java-tron's
	// ShieldedTransactionCreateAccountFee logic. Even transparent-out transfers
	// that created an account charged the single shielded_transaction_fee.
	historicalNileShieldedSingleFeeEndBlock = 1_685_975
)

type historicalShieldedProofCompat struct {
	genesisHash common.Hash
	blockNumber uint64
	txHash      common.Hash
	contractRet corepb.Transaction_ResultContractResult
}

var historicalShieldedProofCompatEntries = []historicalShieldedProofCompat{
	{
		genesisHash: params.NileGenesisHash,
		blockNumber: 1_685_186,
		txHash:      common.HexToHash("c3739ddc0909642006876309440c3733d0bcda66500f958cbc7090d984676f68"),
		contractRet: corepb.Transaction_Result_SUCCESS,
	},
	{
		genesisHash: params.NileGenesisHash,
		blockNumber: 1_685_793,
		txHash:      common.HexToHash("d43de24389ee218b53452153403ac78590bc1e96391522ed5f80306a2f811a80"),
		contractRet: corepb.Transaction_Result_SUCCESS,
	},
	{
		genesisHash: params.NileGenesisHash,
		blockNumber: 1_685_975,
		txHash:      common.HexToHash("30ea095b1eab1deedf01e40a1045916b71a36dd8fc3b21488037f84d07a84d6a"),
		contractRet: corepb.Transaction_Result_SUCCESS,
	},
}

func (a *ShieldedTransferActuator) getContract(ctx *Context) (*contractpb.ShieldedTransferContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ShieldedTransferContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ShieldedTransferContract")
	}
	return c, nil
}

// calcFee returns the fee charged for this shielded transaction in ZEN smallest units.
// If the transparent receiver account does not yet exist, the create-account fee applies.
func (a *ShieldedTransferActuator) calcFee(ctx *Context, c *contractpb.ShieldedTransferContract) int64 {
	if usesHistoricalNileShieldedSingleFee(ctx) {
		return ctx.DynProps.ShieldedTransactionFee()
	}
	if len(c.TransparentToAddress) > 0 {
		to := common.BytesToAddress(c.TransparentToAddress)
		if !ctx.State.AccountExists(to) {
			return ctx.DynProps.ShieldedTransactionCreateAccountFee()
		}
	}
	return ctx.DynProps.ShieldedTransactionFee()
}

func (a *ShieldedTransferActuator) Validate(ctx *Context) error {
	if !ctx.DynProps.AllowSameTokenName() {
		return errors.New("shielded transaction is not allowed before ALLOW_SAME_TOKEN_NAME")
	}
	if !ctx.DynProps.AllowShieldedTransaction() {
		return errors.New("shielded transactions are not enabled")
	}
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	hasFrom := len(c.TransparentFromAddress) > 0
	hasTo := len(c.TransparentToAddress) > 0

	if hasFrom && len(c.SpendDescription) > 0 {
		return errors.New("shielded transfer has more than one sender")
	}
	if !hasFrom && len(c.SpendDescription) == 0 {
		return errors.New("shielded transfer has no sender")
	}
	if len(c.SpendDescription) > 1 {
		return errors.New("shielded transfer has too many spend notes")
	}
	if len(c.ReceiveDescription) == 0 {
		return errors.New("shielded transfer has no output commitment")
	}
	if len(c.ReceiveDescription) > 2 {
		return errors.New("shielded transfer has too many receivers")
	}
	if c.FromAmount < 0 {
		return errors.New("from_amount must not be negative")
	}
	if c.ToAmount < 0 {
		return errors.New("to_amount must not be negative")
	}
	if !hasFrom && c.FromAmount != 0 {
		return errors.New("from_amount must be zero without transparent sender")
	}
	if !hasTo && c.ToAmount != 0 {
		return errors.New("to_amount must be zero without transparent receiver")
	}

	var from common.Address
	if hasFrom {
		var err error
		from, err = checkedAddress(c.TransparentFromAddress, "transparent_from_address")
		if err != nil {
			return err
		}
		if c.FromAmount <= 0 {
			return errors.New("from_amount must be greater than 0")
		}
	}
	var to common.Address
	if hasTo {
		var err error
		to, err = checkedAddress(c.TransparentToAddress, "transparent_to_address")
		if err != nil {
			return err
		}
		if c.ToAmount <= 0 {
			return errors.New("to_amount must be greater than 0")
		}
		if hasFrom && to == from {
			return errors.New("can't transfer zen to yourself")
		}
	}

	feeOnlyReplay := isHistoricalNileShieldedFeeOnlyReplay(ctx)

	// Check for double spends
	seenNullifiers := make(map[string]struct{}, len(c.SpendDescription))
	for _, spend := range c.SpendDescription {
		if len(spend.Nullifier) == 0 {
			return errors.New("spend description missing nullifier")
		}
		key := string(spend.Nullifier)
		if _, ok := seenNullifiers[key]; ok {
			return errors.New("duplicate sapling nullifiers in this transaction")
		}
		seenNullifiers[key] = struct{}{}
		if !feeOnlyReplay {
			if !ctx.State.HasIncrMerkleTree(spend.Anchor) {
				return errors.New("Rt is invalid.")
			}
			if ctx.State.HasNullifier(spend.Nullifier) {
				return errors.New("note has been spend in this transaction")
			}
		}
	}
	seenCommitments := make(map[string]struct{}, len(c.ReceiveDescription))
	for _, recv := range c.ReceiveDescription {
		if len(recv.NoteCommitment) == 0 {
			return errors.New("receive description missing note commitment")
		}
		key := string(recv.NoteCommitment)
		if _, ok := seenCommitments[key]; ok {
			return errors.New("duplicate cm in receive_description")
		}
		seenCommitments[key] = struct{}{}
	}

	fee := a.calcFee(ctx, c)

	// Check transparent sender has sufficient ZEN balance
	if hasFrom {
		if !ctx.State.AccountExists(from) {
			return errors.New("transparent sender account does not exist")
		}
		zenID := ctx.DynProps.ZenTokenID()
		if ctx.State.GetTRC10Balance(from, zenID) < c.FromAmount {
			return errors.New("insufficient ZEN balance for shielded transfer")
		}
		if c.FromAmount <= fee {
			return errors.New("fromAmount must be greater than fee")
		}
	}
	if hasTo {
		zenID := ctx.DynProps.ZenTokenID()
		if ctx.State.GetTRC10Balance(to, zenID) > math.MaxInt64-c.ToAmount {
			return errors.New("recipient ZEN balance overflow")
		}
	}

	txHash := ctx.Tx.Hash()
	txID := txHash.Bytes()
	skipProofVerification := feeOnlyReplay
	if cached, ok := ctx.State.ReadZKProofResult(txID); ok {
		if cached {
			return nil
		}
		if feeOnlyReplay || isHistoricalShieldedProofCompatAllowed(ctx, txHash) {
			skipProofVerification = true
		} else {
			return errors.New("record is fail, skip proof")
		}
	}
	if !skipProofVerification {
		if err := validateShieldedProofShape(c); err != nil {
			_ = ctx.State.WriteZKProofResult(txID, false)
			return err
		}
	}

	valueBalance, ok := checkedAddInt64(c.ToAmount, -c.FromAmount)
	if !ok {
		_ = ctx.State.WriteZKProofResult(txID, false)
		return errors.New("shielded pool value overflow")
	}
	valueBalance, ok = checkedAddInt64(valueBalance, fee)
	if !ok {
		_ = ctx.State.WriteZKProofResult(txID, false)
		return errors.New("shielded pool value overflow")
	}
	newPool, ok := checkedAddInt64(ctx.DynProps.TotalShieldedPoolValue(), -valueBalance)
	if !ok || newPool < 0 {
		_ = ctx.State.WriteZKProofResult(txID, false)
		return errors.New("total shielded pool value can not below 0")
	}

	if skipProofVerification {
		return ctx.State.WriteZKProofResult(txID, true)
	}

	signHash, err := shieldedTransactionSignHash(ctx, c)
	if err != nil {
		_ = ctx.State.WriteZKProofResult(txID, false)
		return err
	}
	if err := zksnark.VerifyShieldedTransfer(c, valueBalance, signHash); err != nil {
		if isHistoricalShieldedProofCompatAllowed(ctx, txHash) {
			return ctx.State.WriteZKProofResult(txID, true)
		}
		_ = ctx.State.WriteZKProofResult(txID, false)
		return err
	}

	return ctx.State.WriteZKProofResult(txID, true)
}

func isHistoricalNileShieldedFeeOnlyReplay(ctx *Context) bool {
	if ctx == nil || ctx.BlockNumber < historicalNileShieldedFeeOnlyStartBlock {
		return false
	}
	ret, ok := expectedContractRet(ctx)
	if !ok || ret != corepb.Transaction_Result_SUCCESS {
		return false
	}
	genesisHash := ctx.GenesisHash
	if genesisHash == (common.Hash{}) && ctx.DB != nil {
		genesisHash = rawdb.ReadBlockHashByNumber(ctx.DB, 0)
	}
	return genesisHash == params.NileGenesisHash
}

func usesHistoricalNileShieldedSingleFee(ctx *Context) bool {
	if ctx == nil || ctx.BlockNumber > historicalNileShieldedSingleFeeEndBlock {
		return false
	}
	return isHistoricalNileShieldedFeeOnlyReplay(ctx)
}

func isHistoricalShieldedProofCompatAllowed(ctx *Context, txHash common.Hash) bool {
	entry, ok := findHistoricalShieldedProofCompat(ctx, txHash)
	if !ok {
		return false
	}
	if ret, ok := expectedContractRet(ctx); ok && ret != entry.contractRet {
		return false
	}
	return true
}

func findHistoricalShieldedProofCompat(ctx *Context, txHash common.Hash) (historicalShieldedProofCompat, bool) {
	if ctx == nil {
		return historicalShieldedProofCompat{}, false
	}
	genesisHash := ctx.GenesisHash
	if genesisHash == (common.Hash{}) && ctx.DB != nil {
		genesisHash = rawdb.ReadBlockHashByNumber(ctx.DB, 0)
	}
	for _, entry := range historicalShieldedProofCompatEntries {
		if ctx.BlockNumber == entry.blockNumber && txHash == entry.txHash && genesisHash == entry.genesisHash {
			return entry, true
		}
	}
	return historicalShieldedProofCompat{}, false
}

func validateShieldedProofShape(c *contractpb.ShieldedTransferContract) error {
	for _, spend := range c.SpendDescription {
		if len(spend.ValueCommitment) != zcElementSize ||
			len(spend.Anchor) != zcElementSize ||
			len(spend.Nullifier) != zcElementSize ||
			len(spend.Rk) != zcElementSize ||
			len(spend.Zkproof) != zcProofSize ||
			len(spend.SpendAuthoritySignature) != zcSignatureSize {
			return errors.New("librustzcashSaplingCheckSpend error")
		}
	}
	for _, recv := range c.ReceiveDescription {
		if len(recv.CEnc) != zcEncCiphertextSize || len(recv.COut) != zcOutCiphertextSize {
			return errors.New("Cout or CEnc size error")
		}
		if len(recv.ValueCommitment) != zcElementSize ||
			len(recv.NoteCommitment) != zcElementSize ||
			len(recv.Epk) != zcElementSize ||
			len(recv.Zkproof) != zcProofSize {
			return errors.New("librustzcashSaplingCheckOutput error")
		}
	}
	if len(c.BindingSignature) != zcSignatureSize {
		return errors.New("librustzcashSaplingFinalCheck error")
	}
	return nil
}

func shieldedTransactionSignHash(ctx *Context, c *contractpb.ShieldedTransferContract) ([]byte, error) {
	if ctx == nil || ctx.Tx == nil || ctx.Tx.Proto() == nil || ctx.Tx.Proto().RawData == nil {
		return nil, errors.New("transaction raw data missing")
	}
	signContract := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: c.TransparentFromAddress,
		FromAmount:             c.FromAmount,
		ReceiveDescription:     c.ReceiveDescription,
		TransparentToAddress:   c.TransparentToAddress,
		ToAmount:               c.ToAmount,
	}
	for _, spend := range c.SpendDescription {
		cloned := proto.Clone(spend).(*contractpb.SpendDescription)
		cloned.SpendAuthoritySignature = nil
		signContract.SpendDescription = append(signContract.SpendDescription, cloned)
	}
	param, err := anypb.New(signContract)
	if err != nil {
		return nil, err
	}
	raw := proto.Clone(ctx.Tx.Proto().RawData).(*corepb.TransactionRaw)
	raw.Contract = []*corepb.Transaction_Contract{{
		Type:      corepb.Transaction_Contract_ShieldedTransferContract,
		Parameter: param,
	}}
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		return nil, err
	}
	tokenHash := sha256.Sum256([]byte(strconv.FormatInt(ctx.DynProps.ZenTokenID(), 10)))
	merged := make([]byte, 0, len(tokenHash)+len(rawBytes))
	merged = append(merged, tokenHash[:]...)
	merged = append(merged, rawBytes...)
	sum := sha256.Sum256(merged)
	return sum[:], nil
}

func (a *ShieldedTransferActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	zenID := ctx.DynProps.ZenTokenID()
	fee := a.calcFee(ctx, c)
	feeOnlyReplay := isHistoricalNileShieldedFeeOnlyReplay(ctx)

	// Deduct ZEN from transparent sender. The shielded fee is credited to
	// Blackhole and removed from the pool adjustment, matching java-tron.
	if len(c.TransparentFromAddress) > 0 {
		from, err := checkedAddress(c.TransparentFromAddress, "transparent_from_address")
		if err != nil {
			return nil, err
		}
		if err := ctx.State.SubTRC10Balance(from, zenID, c.FromAmount); err != nil {
			return nil, err
		}
	}
	ctx.State.AddTRC10Balance(params.BlackholeAddress, zenID, fee)

	if !feeOnlyReplay {
		// Record spend nullifiers to prevent double-spend
		for _, spend := range c.SpendDescription {
			if len(spend.Nullifier) == 0 {
				continue
			}
			if err := ctx.State.WriteNullifier(spend.Nullifier); err != nil {
				return nil, err
			}
		}

		// Record note commitments. Two stores are updated:
		//   1. AppendNoteCommitment writes the sequential cm index store
		//      (java-tron's NoteCommitmentStore — used by wallet APIs).
		//   2. MerkleContainer.AppendCommitment appends to the Sapling
		//      incremental commitment tree (CURRENT_TREE) so the next block's
		//      spend anchors can be validated.
		//
		// The tree state was reset from LAST_TREE before tx execution (see
		// BlockChain.applyBlock) and is promoted back into LAST_TREE after
		// the tx loop succeeds.
		merkle := zksnark.NewMerkleContainer(ctx.State)
		for _, recv := range c.ReceiveDescription {
			if len(recv.NoteCommitment) == 0 {
				continue
			}
			if err := ctx.State.AppendNoteCommitment(recv.NoteCommitment); err != nil {
				return nil, err
			}
			var cm zksnark.PedersenHash
			copy(cm[:], recv.NoteCommitment)
			if err := merkle.AppendCommitment(cm); err != nil {
				return nil, fmt.Errorf("append commitment to merkle tree: %w", err)
			}
		}
	}

	// Credit transparent receiver
	if len(c.TransparentToAddress) > 0 {
		to, err := checkedAddress(c.TransparentToAddress, "transparent_to_address")
		if err != nil {
			return nil, err
		}
		if !ctx.State.AccountExists(to) {
			ctx.State.CreateAccountWithTime(to, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
			if ctx.DynProps.AllowMultiSign() {
				ctx.State.ApplyDefaultAccountPermissions(to, ctx.DynProps)
			}
		}
		ctx.State.AddTRC10Balance(to, zenID, c.ToAmount)
	}

	// Adjust shielded pool total:
	// pool += fromAmount (enters pool from transparent sender)
	// pool -= toAmount  (leaves pool to transparent receiver)
	// pool -= fee       (burned from pool)
	ctx.DynProps.AdjustTotalShieldedPoolValue(c.FromAmount - c.ToAmount - fee)

	return &Result{ShieldedTransactionFee: fee, ContractRet: 1}, nil
}
