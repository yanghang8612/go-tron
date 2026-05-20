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

func cuchar(b []byte) *C.uchar {
	return (*C.uchar)(unsafe.Pointer(&b[0]))
}
