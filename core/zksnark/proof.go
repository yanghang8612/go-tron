package zksnark

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var (
	ErrProofVerificationUnavailable = errors.New("zksnark: Sapling proof verification backend not built")
	ErrProofParamsUnavailable       = errors.New("zksnark: Sapling parameter files not found")
)

type ShieldedTRC20Spend struct {
	Nullifier               []byte
	Anchor                  []byte
	ValueCommitment         []byte
	Rk                      []byte
	Proof                   []byte
	SpendAuthoritySignature []byte
}

type ShieldedTRC20Receive struct {
	NoteCommitment  []byte
	ValueCommitment []byte
	Epk             []byte
	Proof           []byte
}

func VerifyShieldedTransfer(c *contractpb.ShieldedTransferContract, valueBalance int64, sighash []byte) error {
	return verifyShieldedTransfer(c, valueBalance, sighash)
}

func VerifyShieldedTRC20Mint(cm, cv, epk, proof, bindingSig, sighash []byte, value int64) error {
	return verifyShieldedTRC20Mint(cm, cv, epk, proof, bindingSig, sighash, value)
}

func VerifyShieldedTRC20Transfer(spends []ShieldedTRC20Spend, receives []ShieldedTRC20Receive, bindingSig, sighash []byte, valueBalance int64) error {
	return verifyShieldedTRC20Transfer(spends, receives, bindingSig, sighash, valueBalance)
}

func VerifyShieldedTRC20Burn(spend ShieldedTRC20Spend, bindingSig, sighash []byte, value int64) error {
	return verifyShieldedTRC20Burn(spend, bindingSig, sighash, value)
}
