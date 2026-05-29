//go:build !sapling

package zksnark

import contractpb "github.com/tronprotocol/go-tron/proto/core/contract"

func verifyShieldedTransfer(_ *contractpb.ShieldedTransferContract, _ int64, _ []byte) error {
	return ErrProofVerificationUnavailable
}

func verifyShieldedTRC20Mint(_, _, _, _, _, _ []byte, _ int64) error {
	return ErrProofVerificationUnavailable
}

func verifyShieldedTRC20Transfer(_ []ShieldedTRC20Spend, _ []ShieldedTRC20Receive, _, _ []byte, _ int64) error {
	return ErrProofVerificationUnavailable
}

func verifyShieldedTRC20Burn(_ ShieldedTRC20Spend, _, _ []byte, _ int64) error {
	return ErrProofVerificationUnavailable
}
