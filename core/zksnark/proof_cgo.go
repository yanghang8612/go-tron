//go:build sapling

package zksnark

/*
#cgo CFLAGS: -I${SRCDIR}

#include <stddef.h>
#include <stdlib.h>
#include "zksnark_capi.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const (
	saplingSpendParamHash  = "25fd9a0d1c1be0526c14662947ae95b758fe9f3d7fb7f55e9b4437830dcc6215a7ce3ea465914b157715b7a4d681389ea4aa84438190e185d5e4c93574d3a19a"
	saplingOutputParamHash = "a1cb23b93256adce5bce2cb09cefbc96a1d16572675ceb691e9a3626ec15b5b546926ff1c536cfe3a9df07d796b32fdfc3e5d99d65567257bf286cd2858d71a6"
)

var (
	initProofParamsOnce sync.Once
	initProofParamsErr  error
)

func verifyShieldedTransfer(c *contractpb.ShieldedTransferContract, valueBalance int64, sighash []byte) error {
	if err := ensureProofParams(); err != nil {
		return err
	}
	if len(sighash) != 32 {
		return errors.New("sighash must be 32 bytes")
	}

	ctx := C.librustzcash_sapling_verification_ctx_init()
	if ctx == nil {
		return errors.New("librustzcashSaplingVerificationCtxInit error")
	}
	defer C.librustzcash_sapling_verification_ctx_free(ctx)

	for _, spend := range c.SpendDescription {
		ok := C.librustzcash_sapling_check_spend(
			ctx,
			cuchar(spend.ValueCommitment),
			cuchar(spend.Anchor),
			cuchar(spend.Nullifier),
			cuchar(spend.Rk),
			cuchar(spend.Zkproof),
			cuchar(spend.SpendAuthoritySignature),
			cuchar(sighash),
		)
		if !bool(ok) {
			return errors.New("librustzcashSaplingCheckSpend error")
		}
	}

	for _, recv := range c.ReceiveDescription {
		ok := C.librustzcash_sapling_check_output(
			ctx,
			cuchar(recv.ValueCommitment),
			cuchar(recv.NoteCommitment),
			cuchar(recv.Epk),
			cuchar(recv.Zkproof),
		)
		if !bool(ok) {
			return errors.New("librustzcashSaplingCheckOutput error")
		}
	}

	ok := C.librustzcash_sapling_final_check(
		ctx,
		C.int64_t(valueBalance),
		cuchar(c.BindingSignature),
		cuchar(sighash),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingFinalCheck error")
	}
	return nil
}

func verifyShieldedTRC20Mint(cm, cv, epk, proof, bindingSig, sighash []byte, value int64) error {
	if err := ensureProofParams(); err != nil {
		return err
	}
	if err := validateShieldedTRC20Receive(ShieldedTRC20Receive{
		NoteCommitment:  cm,
		ValueCommitment: cv,
		Epk:             epk,
		Proof:           proof,
	}); err != nil {
		return err
	}
	if err := fixedLen(bindingSig, 64, "binding signature"); err != nil {
		return err
	}
	if err := fixedLen(sighash, 32, "sighash"); err != nil {
		return err
	}
	if value == math.MinInt64 {
		return errors.New("value balance overflow")
	}

	ctx := C.librustzcash_sapling_verification_ctx_init()
	if ctx == nil {
		return errors.New("librustzcashSaplingVerificationCtxInit error")
	}
	defer C.librustzcash_sapling_verification_ctx_free(ctx)

	ok := C.librustzcash_sapling_check_output(
		ctx,
		cuchar(cv),
		cuchar(cm),
		cuchar(epk),
		cuchar(proof),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingCheckOutput error")
	}
	ok = C.librustzcash_sapling_final_check(
		ctx,
		C.int64_t(-value),
		cuchar(bindingSig),
		cuchar(sighash),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingFinalCheck error")
	}
	return nil
}

func verifyShieldedTRC20Transfer(spends []ShieldedTRC20Spend, receives []ShieldedTRC20Receive, bindingSig, sighash []byte, valueBalance int64) error {
	if err := ensureProofParams(); err != nil {
		return err
	}
	if len(spends) == 0 {
		return errors.New("spend descriptions missing")
	}
	if len(receives) == 0 {
		return errors.New("receive descriptions missing")
	}
	if err := fixedLen(bindingSig, 64, "binding signature"); err != nil {
		return err
	}
	if err := fixedLen(sighash, 32, "sighash"); err != nil {
		return err
	}

	spendCVs := make([]byte, 0, len(spends)*32)
	for _, spend := range spends {
		if err := validateShieldedTRC20Spend(spend); err != nil {
			return err
		}
		ok := C.librustzcash_sapling_check_spend_new(
			cuchar(spend.ValueCommitment),
			cuchar(spend.Anchor),
			cuchar(spend.Nullifier),
			cuchar(spend.Rk),
			cuchar(spend.Proof),
			cuchar(spend.SpendAuthoritySignature),
			cuchar(sighash),
		)
		if !bool(ok) {
			return errors.New("librustzcashSaplingCheckSpend error")
		}
		spendCVs = append(spendCVs, spend.ValueCommitment...)
	}

	receiveCVs := make([]byte, 0, len(receives)*32)
	for _, recv := range receives {
		if err := validateShieldedTRC20Receive(recv); err != nil {
			return err
		}
		ok := C.librustzcash_sapling_check_output_new(
			cuchar(recv.ValueCommitment),
			cuchar(recv.NoteCommitment),
			cuchar(recv.Epk),
			cuchar(recv.Proof),
		)
		if !bool(ok) {
			return errors.New("librustzcashSaplingCheckOutput error")
		}
		receiveCVs = append(receiveCVs, recv.ValueCommitment...)
	}

	ok := C.librustzcash_sapling_final_check_new(
		C.int64_t(valueBalance),
		cuchar(bindingSig),
		cuchar(sighash),
		cuchar(spendCVs),
		C.size_t(len(spendCVs)),
		cuchar(receiveCVs),
		C.size_t(len(receiveCVs)),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingFinalCheck error")
	}
	return nil
}

func verifyShieldedTRC20Burn(spend ShieldedTRC20Spend, bindingSig, sighash []byte, value int64) error {
	if err := ensureProofParams(); err != nil {
		return err
	}
	if err := validateShieldedTRC20Spend(spend); err != nil {
		return err
	}
	if err := fixedLen(bindingSig, 64, "binding signature"); err != nil {
		return err
	}
	if err := fixedLen(sighash, 32, "sighash"); err != nil {
		return err
	}

	ctx := C.librustzcash_sapling_verification_ctx_init()
	if ctx == nil {
		return errors.New("librustzcashSaplingVerificationCtxInit error")
	}
	defer C.librustzcash_sapling_verification_ctx_free(ctx)

	ok := C.librustzcash_sapling_check_spend(
		ctx,
		cuchar(spend.ValueCommitment),
		cuchar(spend.Anchor),
		cuchar(spend.Nullifier),
		cuchar(spend.Rk),
		cuchar(spend.Proof),
		cuchar(spend.SpendAuthoritySignature),
		cuchar(sighash),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingCheckSpend error")
	}
	ok = C.librustzcash_sapling_final_check(
		ctx,
		C.int64_t(value),
		cuchar(bindingSig),
		cuchar(sighash),
	)
	if !bool(ok) {
		return errors.New("librustzcashSaplingFinalCheck error")
	}
	return nil
}

func ensureProofParams() error {
	initProofParamsOnce.Do(func() {
		spend, output, ok := findProofParams()
		if !ok {
			initProofParamsErr = ErrProofParamsUnavailable
			return
		}
		spendBytes := []byte(spend)
		outputBytes := []byte(output)
		spendHash := C.CString(saplingSpendParamHash)
		outputHash := C.CString(saplingOutputParamHash)
		defer C.free(unsafe.Pointer(spendHash))
		defer C.free(unsafe.Pointer(outputHash))
		C.librustzcash_init_zksnark_params(
			cuchar(spendBytes),
			C.size_t(len(spendBytes)),
			spendHash,
			cuchar(outputBytes),
			C.size_t(len(outputBytes)),
			outputHash,
		)
	})
	return initProofParamsErr
}

func findProofParams() (string, string, bool) {
	dirs := []string{
		os.Getenv("GTRON_ZKSNARK_PARAMS"),
		os.Getenv("ZCASH_PARAMS_DIR"),
	}
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".zcash-params"))
	}
	dirs = append(dirs,
		"params",
		filepath.Join("build", "params"),
		filepath.Join("..", "java-tron", "framework", "src", "main", "resources", "params"),
		filepath.Join("..", "..", "java-tron", "framework", "src", "main", "resources", "params"),
		filepath.Join("..", "..", "..", "tron", "java-tron", "framework", "src", "main", "resources", "params"),
	)

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		spend := filepath.Join(dir, "sapling-spend.params")
		output := filepath.Join(dir, "sapling-output.params")
		if fileExists(spend) && fileExists(output) {
			return spend, output, true
		}
	}
	return "", "", false
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func validateShieldedTRC20Spend(spend ShieldedTRC20Spend) error {
	if err := fixedLen(spend.Nullifier, 32, "nullifier"); err != nil {
		return err
	}
	if err := fixedLen(spend.Anchor, 32, "anchor"); err != nil {
		return err
	}
	if err := fixedLen(spend.ValueCommitment, 32, "value commitment"); err != nil {
		return err
	}
	if err := fixedLen(spend.Rk, 32, "rk"); err != nil {
		return err
	}
	if err := fixedLen(spend.Proof, 192, "proof"); err != nil {
		return err
	}
	return fixedLen(spend.SpendAuthoritySignature, 64, "spend authority signature")
}

func validateShieldedTRC20Receive(recv ShieldedTRC20Receive) error {
	if err := fixedLen(recv.NoteCommitment, 32, "note commitment"); err != nil {
		return err
	}
	if err := fixedLen(recv.ValueCommitment, 32, "value commitment"); err != nil {
		return err
	}
	if err := fixedLen(recv.Epk, 32, "epk"); err != nil {
		return err
	}
	return fixedLen(recv.Proof, 192, "proof")
}

func fixedLen(b []byte, n int, name string) error {
	if len(b) != n {
		return fmt.Errorf("%s must be %d bytes", name, n)
	}
	return nil
}

func cuchar(b []byte) *C.uchar {
	return (*C.uchar)(unsafe.Pointer(&b[0]))
}
