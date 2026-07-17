// Package falcon contains the Falcon-512 verification subset derived from
// Thomas Pornin's public-domain go-fn-dsa implementation. TRON's FN_DSA_512
// wire scheme uses BouncyCastle's original Falcon compressed encoding, not
// the later fixed-size FIPS-206 encoding.
package falcon

import "crypto"

const (
	PublicKeySize    = 896
	SignatureMinSize = 617
	SignatureMaxSize = 667
)

// Verify512 verifies a BouncyCastle Falcon-512 compressed signature. publicKey
// omits Falcon's 0x09 header, while signature includes the canonical 0x39
// compressed-signature header, exactly as java-tron's FNDSA512 wrapper stores
// them in PQAuthSig.
func Verify512(publicKey, message, signature []byte) bool {
	if len(publicKey) != PublicKeySize || len(signature) < SignatureMinSize ||
		len(signature) > SignatureMaxSize || signature[0] != 0x39 {
		return false
	}

	const logn = uint(9)
	n := 1 << logn
	s2 := make([]int16, n)
	t1 := make([]uint16, n)
	t2 := make([]uint16, n)
	if _, err := modq_decode(logn, publicKey, t1); err != nil {
		return false
	}
	if err := comp_decode(logn, signature[41:], s2); err != nil {
		return false
	}
	nonce := signature[1:41]

	norm2 := signed_poly_sqnorm(logn, s2)
	mqpoly_ext_to_int(logn, t1)
	mqpoly_int_to_ntt(logn, t1)
	mqpoly_signed_to_int(logn, s2, t2)
	mqpoly_int_to_ntt(logn, t2)
	mqpoly_mul_ntt(logn, t1, t2)
	mqpoly_ntt_to_int(logn, t1)

	// crypto.Hash(0xffffffff) selects the original Falcon transcript:
	// SHAKE256(nonce || message), which is what BouncyCastle FalconSigner uses.
	if err := hash_to_point(logn, nonce, nil, nil, crypto.Hash(0xffffffff), message, t2); err != nil {
		return false
	}
	mqpoly_ext_to_int(logn, t2)
	mqpoly_sub_int(logn, t2, t1)

	norm1 := mqpoly_sqnorm(logn, t2)
	return norm1 < -norm2 && mqpoly_sqnorm_is_acceptable(logn, norm1+norm2)
}
