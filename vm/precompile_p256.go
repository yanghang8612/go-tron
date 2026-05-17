package vm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
)

type p256Verify struct{}

func (c *p256Verify) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 6900
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 160 {
		return nil, cost, nil
	}

	hash := input[:32]
	r := new(big.Int).SetBytes(input[32:64])
	s := new(big.Int).SetBytes(input[64:96])
	qx := new(big.Int).SetBytes(input[96:128])
	qy := new(big.Int).SetBytes(input[128:160])

	curve := elliptic.P256()
	params := curve.Params()
	if r.Sign() <= 0 || r.Cmp(params.N) >= 0 || s.Sign() <= 0 || s.Cmp(params.N) >= 0 {
		return nil, cost, nil
	}
	if qx.Cmp(params.P) >= 0 || qy.Cmp(params.P) >= 0 || (qx.Sign() == 0 && qy.Sign() == 0) {
		return nil, cost, nil
	}
	if !curve.IsOnCurve(qx, qy) {
		return nil, cost, nil
	}

	pub := ecdsa.PublicKey{Curve: curve, X: qx, Y: qy}
	if !ecdsa.Verify(&pub, hash, r, s) {
		return nil, cost, nil
	}
	out := make([]byte, 32)
	out[31] = 1
	return out, cost, nil
}
