package actuator

import (
	"encoding/hex"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const zenTokenID = int64(1_000_016)

func shieldedReceive(commitment []byte) *contractpb.ReceiveDescription {
	cm := make([]byte, zcElementSize)
	copy(cm, commitment)
	return &contractpb.ReceiveDescription{
		ValueCommitment: make([]byte, zcElementSize),
		NoteCommitment:  cm,
		Epk:             make([]byte, zcElementSize),
		CEnc:            make([]byte, zcEncCiphertextSize),
		COut:            make([]byte, zcOutCiphertextSize),
		Zkproof:         make([]byte, zcProofSize),
	}
}

func seedShieldedAnchor(t *testing.T, ctx *Context, anchor []byte) {
	t.Helper()
	if err := ctx.State.WriteIncrMerkleTree(anchor, &contractpb.IncrementalMerkleTree{}); err != nil {
		t.Fatalf("WriteIncrMerkleTree: %v", err)
	}
}

func fixedShieldedBytes(label string, size int) []byte {
	out := make([]byte, size)
	copy(out, label)
	return out
}

func setupShieldedCtx(t *testing.T, c *contractpb.ShieldedTransferContract) *Context {
	t.Helper()
	ctx := newTestContext(t, corepb.Transaction_Contract_ShieldedTransferContract, c, 0)
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.DynProps.SetAllowShieldedTransaction(true)
	return ctx
}

// TestShieldedTransferDisabled verifies that validate fails when the feature is not enabled.
func TestShieldedTransferDisabled(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             1_000_000,
		SpendDescription:       []*contractpb.SpendDescription{{Nullifier: []byte("nullifier1")}},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ShieldedTransferContract, c, 0)
	// AllowShieldedTransaction defaults to false

	act := &ShieldedTransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error when shielded transactions are disabled")
	}
}

// TestShieldedTransferValidateTransparentFrom validates a transparent-in transaction.
func TestShieldedTransferValidateTransparentFrom(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)

	act := &ShieldedTransferActuator{}

	// No account yet
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing sender account")
	}

	// Create account with insufficient balance
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 100_000) // only 100k, needs 500k + fee
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient ZEN balance")
	}

	// Fund account properly: fromAmount(500k) + fee(100k) = 600k
	ctx.State.SetTRC10Balance(owner, zenTokenID, 600_000)
	if err := ctx.State.WriteZKProofResult(ctx.Tx.Hash().Bytes(), true); err != nil {
		t.Fatal(err)
	}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

// TestShieldedTransferDoubleSpend checks that reusing a nullifier is rejected.
func TestShieldedTransferDoubleSpend(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	nullifier := []byte("testnullifier32bytes____________")
	anchor := []byte("anchor_for_double_spend________")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:   []*contractpb.SpendDescription{{Nullifier: nullifier, Anchor: anchor}},
		ReceiveDescription: []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)
	seedShieldedAnchor(t, ctx, anchor)

	// Pre-record the nullifier to simulate double spend
	if err := ctx.State.WriteNullifier(nullifier); err != nil {
		t.Fatal(err)
	}

	act := &ShieldedTransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for double spend")
	}
}

func TestShieldedTransferValidateRejectsUnknownAnchor(t *testing.T) {
	nullifier := []byte("testnullifier32bytes_anchor____")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:   []*contractpb.SpendDescription{{Nullifier: nullifier, Anchor: []byte("missing_anchor")}},
		ReceiveDescription: []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
	}
	ctx := setupShieldedCtx(t, c)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected invalid anchor")
	}
	if err.Error() != "Rt is invalid." {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsCiphertextSize(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{{NoteCommitment: []byte("cm1"), CEnc: make([]byte, zcEncCiphertextSize-1), COut: make([]byte, zcOutCiphertextSize)}},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected ciphertext size rejection")
	}
	if err.Error() != "Cout or CEnc size error" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsOutputProofShape(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription: []*contractpb.ReceiveDescription{{
			ValueCommitment: make([]byte, zcElementSize),
			NoteCommitment:  make([]byte, zcElementSize),
			Epk:             nil,
			CEnc:            make([]byte, zcEncCiphertextSize),
			COut:            make([]byte, zcOutCiphertextSize),
			Zkproof:         make([]byte, zcProofSize),
		}},
		BindingSignature: make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected output proof shape rejection")
	}
	if err.Error() != "librustzcashSaplingCheckOutput error" {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := ctx.Tx.Hash()
	if cached, ok := ctx.State.ReadZKProofResult(txID.Bytes()); !ok || cached {
		t.Fatalf("failed proof cache: got (%v,%v), want (false,true)", cached, ok)
	}

	err = (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected cached proof failure")
	}
	if err.Error() != "record is fail, skip proof" {
		t.Fatalf("unexpected cached error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsUnverifiedProof(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	if err := (&ShieldedTransferActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected proof verification rejection")
	}
	txID := ctx.Tx.Hash()
	if cached, ok := ctx.State.ReadZKProofResult(txID.Bytes()); !ok || cached {
		t.Fatalf("failed proof cache: got (%v,%v), want (false,true)", cached, ok)
	}
}

func TestShieldedTransferValidateCachedProofSuccess(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	if err := ctx.State.WriteZKProofResult(ctx.Tx.Hash().Bytes(), true); err != nil {
		t.Fatal(err)
	}
	if err := (&ShieldedTransferActuator{}).Validate(ctx); err != nil {
		t.Fatalf("cached validate should pass: %v", err)
	}
}

func TestShieldedTransferValidateAcceptsHistoricalNileOutputProof(t *testing.T) {
	tx := historicalNileShieldedTransferTx(t)
	entry := historicalShieldedProofCompatEntries[0]
	if tx.Hash() != entry.txHash {
		t.Fatalf("tx hash mismatch: got %s, want %s", tx.Hash(), entry.txHash)
	}

	run := func(t *testing.T, seedFailedCache bool) {
		t.Helper()
		statedb := setupStateDB(t)
		ctx := setupContext(t, statedb, tx)
		ctx.DynProps.SetAllowSameTokenName(true)
		ctx.DynProps.SetAllowShieldedTransaction(true)
		ctx.BlockNumber = entry.blockNumber
		ctx.GenesisHash = entry.genesisHash
		ctx.TrustTransactionRet = true

		c := &contractpb.ShieldedTransferContract{}
		if err := tx.Contract().Parameter.UnmarshalTo(c); err != nil {
			t.Fatalf("unmarshal shielded transfer: %v", err)
		}
		owner := tcommon.BytesToAddress(c.TransparentFromAddress)
		ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
		ctx.State.SetTRC10Balance(owner, ctx.DynProps.ZenTokenID(), c.FromAmount)

		if seedFailedCache {
			if err := ctx.State.WriteZKProofResult(tx.Hash().Bytes(), false); err != nil {
				t.Fatal(err)
			}
		}

		if err := (&ShieldedTransferActuator{}).Validate(ctx); err != nil {
			t.Fatalf("historical Nile shielded transfer should validate: %v", err)
		}
		if cached, ok := ctx.State.ReadZKProofResult(tx.Hash().Bytes()); !ok || !cached {
			t.Fatalf("proof cache: got (%v,%v), want (true,true)", cached, ok)
		}
	}

	t.Run("fresh proof check", func(t *testing.T) {
		run(t, false)
	})
	t.Run("failed proof cache from previous binary", func(t *testing.T) {
		run(t, true)
	})
	t.Run("ret mismatch rejected", func(t *testing.T) {
		pb := proto.Clone(tx.Proto()).(*corepb.Transaction)
		pb.Ret[0].ContractRet = corepb.Transaction_Result_REVERT
		failedRetTx := types.NewTransactionFromPB(pb)

		statedb := setupStateDB(t)
		ctx := setupContext(t, statedb, failedRetTx)
		ctx.DynProps.SetAllowSameTokenName(true)
		ctx.DynProps.SetAllowShieldedTransaction(true)
		ctx.BlockNumber = entry.blockNumber
		ctx.GenesisHash = entry.genesisHash
		ctx.TrustTransactionRet = true

		c := &contractpb.ShieldedTransferContract{}
		if err := failedRetTx.Contract().Parameter.UnmarshalTo(c); err != nil {
			t.Fatalf("unmarshal shielded transfer: %v", err)
		}
		owner := tcommon.BytesToAddress(c.TransparentFromAddress)
		ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
		ctx.State.SetTRC10Balance(owner, ctx.DynProps.ZenTokenID(), c.FromAmount)

		if err := (&ShieldedTransferActuator{}).Validate(ctx); err == nil {
			t.Fatalf("ret mismatch should not use historical shielded proof compat")
		}
	})
}

func TestHistoricalShieldedProofCompatEntriesRequireMatchingRet(t *testing.T) {
	for _, entry := range historicalShieldedProofCompatEntries {
		ctx := &Context{
			BlockNumber:         entry.blockNumber,
			GenesisHash:         entry.genesisHash,
			TrustTransactionRet: true,
			Tx: types.NewTransactionFromPB(&corepb.Transaction{Ret: []*corepb.Transaction_Result{{
				ContractRet: entry.contractRet,
			}}}),
		}
		if !isHistoricalShieldedProofCompatAllowed(ctx, entry.txHash) {
			t.Fatalf("compat entry should allow matching ret: block=%d tx=%s ret=%s", entry.blockNumber, entry.txHash, entry.contractRet)
		}

		ctx.Tx.Proto().Ret[0].ContractRet = corepb.Transaction_Result_REVERT
		if isHistoricalShieldedProofCompatAllowed(ctx, entry.txHash) {
			t.Fatalf("compat entry should reject mismatched ret: block=%d tx=%s", entry.blockNumber, entry.txHash)
		}
	}
}

func TestHistoricalNileShieldedFeeOnlyReplayCoversKnownFailureBlocks(t *testing.T) {
	for _, entry := range historicalShieldedProofCompatEntries {
		ctx := &Context{
			BlockNumber:         entry.blockNumber,
			GenesisHash:         entry.genesisHash,
			TrustTransactionRet: true,
			Tx: types.NewTransactionFromPB(&corepb.Transaction{Ret: []*corepb.Transaction_Result{{
				ContractRet: entry.contractRet,
			}}}),
		}
		if !isHistoricalNileShieldedFeeOnlyReplay(ctx) {
			t.Fatalf("known Nile shielded block should use fee-only replay: block=%d tx=%s", entry.blockNumber, entry.txHash)
		}
	}
}

func TestHistoricalNileShieldedFeeOnlyReplaySkipsAnonymousValidation(t *testing.T) {
	nullifier := fixedShieldedBytes("historical fee-only nullifier", zcElementSize)
	c := &contractpb.ShieldedTransferContract{
		SpendDescription: []*contractpb.SpendDescription{{
			Nullifier: nullifier,
			Anchor:    fixedShieldedBytes("missing historical anchor", zcElementSize),
		}},
		ReceiveDescription: []*contractpb.ReceiveDescription{{
			NoteCommitment: fixedShieldedBytes("historical fee-only cm", zcElementSize),
		}},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.BlockNumber = 1_685_975
	ctx.GenesisHash = params.NileGenesisHash
	ctx.TrustTransactionRet = true
	ctx.Tx.Proto().Ret = []*corepb.Transaction_Result{{
		ContractRet: corepb.Transaction_Result_SUCCESS,
	}}
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)
	if err := ctx.State.WriteZKProofResult(ctx.Tx.Hash().Bytes(), false); err != nil {
		t.Fatal(err)
	}

	if err := (&ShieldedTransferActuator{}).Validate(ctx); err != nil {
		t.Fatalf("historical Nile fee-only replay should skip anonymous validation: %v", err)
	}
	if cached, ok := ctx.State.ReadZKProofResult(ctx.Tx.Hash().Bytes()); !ok || !cached {
		t.Fatalf("proof cache: got (%v,%v), want (true,true)", cached, ok)
	}
}

func TestHistoricalNileShieldedFeeOnlyReplayExecuteVisibleAccountingByShape(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	to := tcommon.Address{0x41, 0x22}

	tests := []struct {
		name          string
		contract      *contractpb.ShieldedTransferContract
		ownerStart    int64
		poolStart     int64
		wantOwner     int64
		wantTo        int64
		wantBlackhole int64
		wantPool      int64
	}{
		{
			name: "transparent in",
			contract: &contractpb.ShieldedTransferContract{
				TransparentFromAddress: owner[:],
				FromAmount:             50_000_000,
				ReceiveDescription: []*contractpb.ReceiveDescription{{
					NoteCommitment: fixedShieldedBytes("historical transparent in cm", zcElementSize),
				}},
			},
			ownerStart:    80_000_000,
			wantOwner:     30_000_000,
			wantBlackhole: 10_000_000,
			wantPool:      40_000_000,
		},
		{
			name: "shielded only",
			contract: &contractpb.ShieldedTransferContract{
				SpendDescription: []*contractpb.SpendDescription{{
					Nullifier: fixedShieldedBytes("historical shielded only nullifier", zcElementSize),
				}},
				ReceiveDescription: []*contractpb.ReceiveDescription{{
					NoteCommitment: fixedShieldedBytes("historical shielded only cm", zcElementSize),
				}},
			},
			poolStart:     10_900_000,
			wantBlackhole: 10_000_000,
			wantPool:      900_000,
		},
		{
			name: "transparent out",
			contract: &contractpb.ShieldedTransferContract{
				SpendDescription: []*contractpb.SpendDescription{{
					Nullifier: fixedShieldedBytes("historical transparent out nullifier", zcElementSize),
				}},
				ReceiveDescription: []*contractpb.ReceiveDescription{{
					NoteCommitment: fixedShieldedBytes("historical transparent out cm", zcElementSize),
				}},
				TransparentToAddress: to[:],
				ToAmount:             10_000_000,
			},
			poolStart:     25_000_000,
			wantTo:        10_000_000,
			wantBlackhole: 10_000_000,
			wantPool:      5_000_000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupShieldedCtx(t, tc.contract)
			ctx.BlockNumber = 1_685_975
			ctx.GenesisHash = params.NileGenesisHash
			ctx.TrustTransactionRet = true
			ctx.Tx.Proto().Ret = []*corepb.Transaction_Result{{
				ContractRet: corepb.Transaction_Result_SUCCESS,
			}}
			ctx.DynProps.Set("shielded_transaction_fee", 10_000_000)
			if tc.poolStart != 0 {
				ctx.DynProps.AdjustTotalShieldedPoolValue(tc.poolStart)
			}
			if len(tc.contract.TransparentFromAddress) > 0 {
				ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
				ctx.State.SetTRC10Balance(owner, zenTokenID, tc.ownerStart)
			}

			result, err := (&ShieldedTransferActuator{}).Execute(ctx)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if result.ShieldedTransactionFee != 10_000_000 {
				t.Fatalf("shielded fee: want 10000000, got %d", result.ShieldedTransactionFee)
			}
			if got := ctx.State.GetTRC10Balance(owner, zenTokenID); got != tc.wantOwner {
				t.Fatalf("owner ZEN balance: want %d, got %d", tc.wantOwner, got)
			}
			if got := ctx.State.GetTRC10Balance(to, zenTokenID); got != tc.wantTo {
				t.Fatalf("recipient ZEN balance: want %d, got %d", tc.wantTo, got)
			}
			if got := ctx.State.GetTRC10Balance(params.BlackholeAddress, zenTokenID); got != tc.wantBlackhole {
				t.Fatalf("blackhole ZEN balance: want %d, got %d", tc.wantBlackhole, got)
			}
			if got := ctx.DynProps.TotalShieldedPoolValue(); got != tc.wantPool {
				t.Fatalf("pool value: want %d, got %d", tc.wantPool, got)
			}
			for _, spend := range tc.contract.SpendDescription {
				if ctx.State.HasNullifier(spend.Nullifier) {
					t.Fatal("historical fee-only replay must not persist nullifiers")
				}
			}
			if got := ctx.State.NoteCommitmentCount(); got != 0 {
				t.Fatalf("historical fee-only replay must not persist note commitments, got %d", got)
			}
		})
	}
}

func TestHistoricalNileShieldedFeeOnlyReplayRequiresTrustedSuccessRet(t *testing.T) {
	c := &contractpb.ShieldedTransferContract{
		SpendDescription: []*contractpb.SpendDescription{{
			Nullifier: fixedShieldedBytes("historical fee-only nullifier", zcElementSize),
			Anchor:    fixedShieldedBytes("missing historical anchor", zcElementSize),
		}},
		ReceiveDescription: []*contractpb.ReceiveDescription{{
			NoteCommitment: fixedShieldedBytes("historical fee-only cm", zcElementSize),
		}},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.BlockNumber = 1_685_975
	ctx.GenesisHash = params.NileGenesisHash
	ctx.Tx.Proto().Ret = []*corepb.Transaction_Result{{
		ContractRet: corepb.Transaction_Result_SUCCESS,
	}}
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("untrusted ret should not enable historical Nile fee-only replay")
	}
	if err.Error() != "Rt is invalid." {
		t.Fatalf("unexpected error: %v", err)
	}
}

func historicalNileShieldedTransferTx(t *testing.T) *types.Transaction {
	t.Helper()
	const rawDataHex = "0a02b6bd2208a6a9d6d858aec6ca40f8afb08af52d5ae308083312de080a35747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e536869656c6465645472616e73666572436f6e747261637412a4080a15411bbda21b480e295da75f80ac9360fdc71fdf3f031080c8afa02522c2070a20ea912dfa5546ecde523db49ccf292d03f16e47eb7e889a333c66dd31e4e4cca11220e8cd94ab7212742e366935c37bc9c216109d85f486f043b764cd28c3f1edbf271a20cc2dc1ee589b44c1e1396223cf6eafbb3a3bc7a232c123f61dd2ae471f70b7b822c404c50c68e28b3794ae003a270682d6abe3c3874be3b3d581a0bce47df079af6b048e8a6c064ab4b34f1ce7fb26975b2cbe39b21787b1585ec7eba81fbd087e1430f0d5eb496b52887f6e8aa6b1dd41cf9a5ba54eb46fd56f68d1615d69d561c6582aa9623af2cb79926fd49c344bcc3c32765ae041989e43e8c1ddae2675c61ab45d2efc2b1a26da465f3649e287933b7c239995a86d67f1e3011e874121a0a18b82a09a51f538434f4dade8c976b19af70f3706f0a4c45972eeb443b5dbd2cda34d3a34a31ebb20f986a26db44dc4ed72e7072e7a1d8edf6a1dadd332ed922e035c75012f2de38fbdab48699f36eb33289d79f898d2e9b255228cb4e41b960223bc74e4e483336452e00e2281a71a3a2f0fdabd7786e16337d4d995cf90cd97219c1f7e61a95bb82ad3b8dca903d3b41567c858afb77ea349f65cdc4974a3ed27011bcf48ce524f7c8147ec5ffa740e0551e0199eeb66fb0a42213c9fbfe22b0fb53ff2ea4a6aef0ee7f423b82d266c649e119eef375636c1c172b07c8d4a1d5fbd01a31d6d1337add2769e2995aef1ea00e2df23227cb1c0309a9e6ff2f5ff1d539f32c197341a87c3521d001df2e30536dae78c5d6912434c70acf2e2865ca713cd7c23175be13f28ea02cac468070f60cb0e3ef594c3dfe4e2789c02e597dbf8e12c379d619fdf02884bec4a43c06153fb173041c91cfb5b63f1da4e8b40bd50cbfd5e34fd3d5dc5d94c134173fb3f0911b386945c0158d7b52086bea388b99f3877ae95ce25d6eaaaf173862a70933c37a47f3a9a8c80a6b340cd22da621a9e41679a2a50ca9816620bd3a08dda6961a1a7fc61644a8807ac841bc5e02b4686839c1431389063e101a81349dc47af07589d21743459968c53c6a589149e1635ae2778cea58e624a091b6f94690b2e233f965c8a4032c001b992c94e3ef1d5c430a8d2f6ef9d096a46a2d9b96dc832be685172b8a1d197f3f665fd1fe1a0dddb2652dd02c72f9ef8a2a16970984e5f0fe283b2dc82da98fb6161d15059a4ecef7bc2856fd8c902bb1060326a7fb4d5abf885affe7c3aa95801c9bd4a588c7e8d863bc0936157df77140c8c0e33385821cdb0bdf3075f4c1d04df3d01fcb07059d418fb7428d13722864c6f2383a362c1c440ac869c5c57847f7ee8418238b418e5c79a94e7444b8d9c8ac720bf570483f535e9f8a25ef42d2a40f341adcec9cad051bb6ac3a996e3a132e3e48fb2545f855d165b3e17d22a7321a88c4a42bb94757dbbbe0d1f35acc6a7734007b646a7d59d61e0d30dba280c0570b8f2ac8af52d"
	rawData, err := hex.DecodeString(rawDataHex)
	if err != nil {
		t.Fatalf("decode raw_data_hex: %v", err)
	}
	raw := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, raw); err != nil {
		t.Fatalf("unmarshal raw_data: %v", err)
	}
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: raw,
		Ret: []*corepb.Transaction_Result{{
			ContractRet: corepb.Transaction_Result_SUCCESS,
		}},
	})
}

// TestShieldedTransferExecuteTransparentIn tests shielding ZEN into the pool.
func TestShieldedTransferExecuteTransparentIn(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	nullifier := []byte("nullifier_for_spend_desc_______1")
	commitment := []byte("notecommitment_for_receive______")
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		SpendDescription:       []*contractpb.SpendDescription{{Nullifier: nullifier}},
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive(commitment)},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	// Fund: 500_000 (from) + 100_000 (fee) = 600_000
	ctx.State.SetTRC10Balance(owner, zenTokenID, 600_000)

	act := &ShieldedTransferActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if result.Fee != 0 || result.ShieldedTransactionFee != 100_000 {
		t.Fatalf("receipt fees: want Fee=0 shielded=100000, got Fee=%d shielded=%d", result.Fee, result.ShieldedTransactionFee)
	}

	// Sender pays fromAmount; the fee is credited to Blackhole and removed from the pool.
	if got := ctx.State.GetTRC10Balance(owner, zenTokenID); got != 100_000 {
		t.Fatalf("sender ZEN balance: want 100000, got %d", got)
	}
	if got := ctx.State.GetTRC10Balance(params.BlackholeAddress, zenTokenID); got != 100_000 {
		t.Fatalf("blackhole ZEN balance: want 100000, got %d", got)
	}
	// Nullifier should be recorded
	if !ctx.State.HasNullifier(nullifier) {
		t.Fatal("nullifier should be recorded after execute")
	}
	// Note commitment should be recorded
	if got := ctx.State.NoteCommitmentCount(); got != 1 {
		t.Fatalf("note commitment count: want 1, got %d", got)
	}
	// Pool value: fromAmount - toAmount(0) - fee = 500k - 0 - 100k = 400k
	if got := ctx.DynProps.TotalShieldedPoolValue(); got != 400_000 {
		t.Fatalf("shielded pool value: want 400000, got %d", got)
	}
}

// TestShieldedTransferExecuteTransparentOut tests unshielding ZEN from the pool.
func TestShieldedTransferExecuteTransparentOut(t *testing.T) {
	to := tcommon.Address{0x41, 0x02}
	nullifier := []byte("nullifier_for_spend_desc_______2")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:     []*contractpb.SpendDescription{{Nullifier: nullifier}},
		TransparentToAddress: to[:],
		ToAmount:             300_000,
	}
	ctx := setupShieldedCtx(t, c)
	// Pre-create recipient so regular fee (100k) applies, not create-account fee.
	ctx.State.CreateAccount(to, corepb.AccountType_Normal)
	// Pre-seed pool value so the deduction makes sense
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)

	act := &ShieldedTransferActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	// Recipient should be created with toAmount
	if !ctx.State.AccountExists(to) {
		t.Fatal("recipient account should have been created")
	}
	if got := ctx.State.GetTRC10Balance(to, zenTokenID); got != 300_000 {
		t.Fatalf("recipient ZEN balance: want 300000, got %d", got)
	}
	// Nullifier recorded
	if !ctx.State.HasNullifier(nullifier) {
		t.Fatal("nullifier should be recorded")
	}
	// Pool: 1_000_000 + 0 - 300_000 - 100_000 (fee) = 600_000
	if got := ctx.DynProps.TotalShieldedPoolValue(); got != 600_000 {
		t.Fatalf("pool value: want 600000, got %d", got)
	}
}

func TestHistoricalNileShieldedFeeOnlyReplayExecuteKeepsVisibleAccountingOnly(t *testing.T) {
	nullifier := fixedShieldedBytes("historical fee-only nullifier", zcElementSize)
	commitment := fixedShieldedBytes("historical fee-only cm", zcElementSize)
	c := &contractpb.ShieldedTransferContract{
		SpendDescription: []*contractpb.SpendDescription{{
			Nullifier: nullifier,
		}},
		ReceiveDescription: []*contractpb.ReceiveDescription{{
			NoteCommitment: commitment,
		}},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.BlockNumber = 1_685_975
	ctx.GenesisHash = params.NileGenesisHash
	ctx.TrustTransactionRet = true
	ctx.Tx.Proto().Ret = []*corepb.Transaction_Result{{
		ContractRet: corepb.Transaction_Result_SUCCESS,
	}}
	ctx.DynProps.Set("shielded_transaction_fee", 10_000_000)
	ctx.DynProps.AdjustTotalShieldedPoolValue(10_900_000)

	result, err := (&ShieldedTransferActuator{}).Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ShieldedTransactionFee != 10_000_000 {
		t.Fatalf("shielded fee: want 10000000, got %d", result.ShieldedTransactionFee)
	}
	if got := ctx.State.GetTRC10Balance(params.BlackholeAddress, zenTokenID); got != 10_000_000 {
		t.Fatalf("blackhole ZEN balance: want 10000000, got %d", got)
	}
	if ctx.State.HasNullifier(nullifier) {
		t.Fatal("historical fee-only replay must not persist nullifiers")
	}
	if got := ctx.State.NoteCommitmentCount(); got != 0 {
		t.Fatalf("historical fee-only replay must not persist note commitments, got %d", got)
	}
	if got := ctx.State.ReadNoteCommitment(0); got != nil {
		t.Fatalf("historical fee-only replay persisted commitment: %x", got)
	}
	if got := ctx.DynProps.TotalShieldedPoolValue(); got != 900_000 {
		t.Fatalf("pool value: want 900000, got %d", got)
	}
}
