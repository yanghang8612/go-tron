package pq

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/cloudflare/circl/sign/mldsa/mldsa44"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto/pq/falcon"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"golang.org/x/crypto/sha3"
)

const (
	FnDsa512PublicKeySize    = falcon.PublicKeySize
	FnDsa512SignatureMinSize = falcon.SignatureMinSize
	FnDsa512SignatureMaxSize = falcon.SignatureMaxSize
	MlDsa44PublicKeySize     = mldsa44.PublicKeySize
	MlDsa44SignatureSize     = mldsa44.SignatureSize
)

var (
	ErrUnknownScheme    = errors.New("unknown post-quantum signature scheme")
	ErrMalformedAuthSig = errors.New("malformed post-quantum authentication signature")
	ErrInvalidSignature = errors.New("invalid post-quantum signature")
	ErrUnexpectedFields = errors.New("post-quantum authentication signature contains unknown fields")
)

func PublicKeySize(scheme corepb.PQScheme) (int, bool) {
	switch scheme {
	case corepb.PQScheme_FN_DSA_512:
		return FnDsa512PublicKeySize, true
	case corepb.PQScheme_ML_DSA_44:
		return MlDsa44PublicKeySize, true
	default:
		return 0, false
	}
}

func ValidSignatureSize(scheme corepb.PQScheme, size int) bool {
	switch scheme {
	case corepb.PQScheme_FN_DSA_512:
		return size >= FnDsa512SignatureMinSize && size <= FnDsa512SignatureMaxSize
	case corepb.PQScheme_ML_DSA_44:
		return size == MlDsa44SignatureSize
	default:
		return false
	}
}

// AuthSigWireSizeUpperBound mirrors java-tron's computePQAuthSigWireSize.
// It includes transaction field 6's tag/length and reserves Falcon's maximum
// variable signature length, as used by delegate/create-account size estimates.
func AuthSigWireSizeUpperBound(scheme corepb.PQScheme) (int, bool) {
	pkLen, ok := PublicKeySize(scheme)
	if !ok {
		return 0, false
	}
	var sigLen int
	switch scheme {
	case corepb.PQScheme_FN_DSA_512:
		sigLen = FnDsa512SignatureMaxSize
	case corepb.PQScheme_ML_DSA_44:
		sigLen = MlDsa44SignatureSize
	}
	body := 2 + 1 + varintSize(pkLen) + pkLen + 1 + varintSize(sigLen) + sigLen
	return 1 + varintSize(body) + body, true
}

func varintSize(value int) int {
	switch {
	case value < 1<<7:
		return 1
	case value < 1<<14:
		return 2
	case value < 1<<21:
		return 3
	default:
		return 4
	}
}

// Address derives the 21-byte TRON address used by java-tron PQSchemeRegistry:
// 0x41 followed by the rightmost 20 bytes of Keccak-256(publicKey).
func Address(scheme corepb.PQScheme, publicKey []byte) (common.Address, error) {
	want, ok := PublicKeySize(scheme)
	if !ok {
		return common.Address{}, ErrUnknownScheme
	}
	if len(publicKey) != want {
		return common.Address{}, fmt.Errorf("%w: public key length %d, want %d", ErrMalformedAuthSig, len(publicKey), want)
	}
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write(publicKey)
	digest := h.Sum(nil)
	var out common.Address
	out[0] = 0x41
	copy(out[1:], digest[len(digest)-20:])
	return out, nil
}

func Verify(scheme corepb.PQScheme, publicKey, message, signature []byte) bool {
	switch scheme {
	case corepb.PQScheme_FN_DSA_512:
		return falcon.Verify512(publicKey, message, signature)
	case corepb.PQScheme_ML_DSA_44:
		if len(publicKey) != MlDsa44PublicKeySize || len(signature) != MlDsa44SignatureSize {
			return false
		}
		var pk mldsa44.PublicKey
		if err := pk.UnmarshalBinary(publicKey); err != nil {
			return false
		}
		return mldsa44.Verify(&pk, message, nil, signature)
	default:
		return false
	}
}

// Validate verifies field canonicality, address binding and the signature.
func Validate(auth *corepb.PQAuthSig, expectedAddress common.Address, message []byte) error {
	if auth == nil {
		return fmt.Errorf("%w: missing pq_auth_sig", ErrMalformedAuthSig)
	}
	if len(auth.ProtoReflect().GetUnknown()) != 0 {
		return ErrUnexpectedFields
	}
	want, ok := PublicKeySize(auth.Scheme)
	if !ok {
		return ErrUnknownScheme
	}
	if len(auth.PublicKey) != want || !ValidSignatureSize(auth.Scheme, len(auth.Signature)) {
		return fmt.Errorf("%w: key/signature length mismatch", ErrMalformedAuthSig)
	}
	derived, err := Address(auth.Scheme, auth.PublicKey)
	if err != nil {
		return err
	}
	if !bytes.Equal(derived.Bytes(), expectedAddress.Bytes()) {
		return fmt.Errorf("%w: public key does not match permission address", ErrMalformedAuthSig)
	}
	if !Verify(auth.Scheme, auth.PublicKey, message, auth.Signature) {
		return ErrInvalidSignature
	}
	return nil
}
