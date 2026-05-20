package zksnark

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var (
	ErrProofVerificationUnavailable = errors.New("zksnark: Sapling proof verification backend not built")
	ErrProofParamsUnavailable       = errors.New("zksnark: Sapling parameter files not found")
)

func VerifyShieldedTransfer(c *contractpb.ShieldedTransferContract, valueBalance int64, sighash []byte) error {
	return verifyShieldedTransfer(c, valueBalance, sighash)
}
