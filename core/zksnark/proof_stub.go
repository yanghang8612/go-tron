//go:build !sapling

package zksnark

import contractpb "github.com/tronprotocol/go-tron/proto/core/contract"

func verifyShieldedTransfer(_ *contractpb.ShieldedTransferContract, _ int64, _ []byte) error {
	return ErrProofVerificationUnavailable
}
