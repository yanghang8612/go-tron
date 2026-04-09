package crypto

import (
	"crypto/ecdsa"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ethcrypto.GenerateKey()
}

func Sign(hash []byte, prv *ecdsa.PrivateKey) ([]byte, error) {
	return ethcrypto.Sign(hash, prv)
}

func SigToPub(hash, sig []byte) (*ecdsa.PublicKey, error) {
	return ethcrypto.SigToPub(hash, sig)
}

func PubkeyToBytes(pub *ecdsa.PublicKey) []byte {
	return ethcrypto.FromECDSAPub(pub)
}

func PrivateKeyToBytes(prv *ecdsa.PrivateKey) []byte {
	return ethcrypto.FromECDSA(prv)
}

func BytesToPrivateKey(b []byte) (*ecdsa.PrivateKey, error) {
	return ethcrypto.ToECDSA(b)
}

func Keccak256(data []byte) []byte {
	return ethcrypto.Keccak256(data)
}
