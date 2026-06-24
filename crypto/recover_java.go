package crypto

import (
	"errors"

	decdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/tronprotocol/go-tron/common"
)

// emptyKeccakAddr20 is keccak256("")[12:] — the 20-byte address java-tron's
// ECKey.recoverPubBytesFromSignature produces when ECDSA recovery yields the
// point at infinity. java does NOT reject that case: it encodes the infinity
// point as a single 0x00 byte and Hash.computeAddress then hashes the empty
// slice. The constant is computed once at init from Keccak256(nil).
var emptyKeccakAddr20 = ethcrypto.Keccak256(nil)[12:]

// ErrPointAtInfinity is returned (decred's sentinel text) when recovery lands
// on the point at infinity; we match on it to mirror java's behavior.
var errInfinity = errors.New("recovered pubkey is the point at infinity")

// SigToAddressJavaCompat recovers the TRON signer address from a transaction
// signature, reproducing java-tron's ECKey.signatureToAddress EXACTLY —
// including the point-at-infinity quirk that go-ethereum/decred reject.
//
// recoverySig must be the 65-byte [r||s||v] form with v already normalized to
// a geth-style recovery id (0/1), i.e. the output of signatureForRecovery.
//
// java's recoverPubBytesFromSignature never checks whether the recovered
// public key Q is the point at infinity; for such a signature it returns a
// single 0x00 byte, and computeAddress hashes the empty slice to
// keccak256("")[12:]. Nile block 18,278,266 carries a TransferContract
// "signed" by exactly this ghost address (its owner == keccak256("")[12:]),
// which every java node accepted — so gtron must mirror it bit-for-bit rather
// than reject the block. Any OTHER recovery failure (malformed R, r/s out of
// range) is a genuine bad signature that java also rejects, so it propagates.
func SigToAddressJavaCompat(hash, recoverySig []byte) (common.Address, error) {
	pub, err := SigToPub(hash, recoverySig)
	if err == nil {
		return PubkeyToAddress(pub), nil
	}
	if recoveryYieldsInfinity(hash, recoverySig) {
		var addr common.Address
		addr[0] = common.AddressPrefixMainnet
		copy(addr[1:], emptyKeccakAddr20)
		return addr, nil
	}
	return common.Address{}, err
}

// recoveryYieldsInfinity reports whether ECDSA recovery for this signature
// lands on the point at infinity (java's silent-success case). It uses the
// pure-go decred implementation, whose recovery algorithm matches java's
// BouncyCastle SEC1 §4.1.6 steps and surfaces the infinity case as a distinct
// error rather than a generic failure.
func recoveryYieldsInfinity(hash, recoverySig []byte) bool {
	if len(recoverySig) != 65 {
		return false
	}
	recid := recoverySig[64]
	if recid > 3 {
		return false
	}
	// decred RecoverCompact wants a header byte of 27+recid (uncompressed).
	compact := make([]byte, 65)
	compact[0] = 27 + recid
	copy(compact[1:], recoverySig[:64])
	_, _, err := decdsa.RecoverCompact(compact, hash)
	return err != nil && err.Error() == errInfinity.Error()
}
