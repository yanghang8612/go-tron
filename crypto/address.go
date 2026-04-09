package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"

	"github.com/mr-tron/base58"
	"github.com/tronprotocol/go-tron/common"
)

func PubkeyToAddress(pub *ecdsa.PublicKey) common.Address {
	pubBytes := PubkeyToBytes(pub)
	hash := Keccak256(pubBytes[1:]) // strip 0x04 uncompressed prefix
	var addr common.Address
	addr[0] = common.AddressPrefixMainnet
	copy(addr[1:], hash[len(hash)-20:])
	return addr
}

func AddressToBase58(addr common.Address) string {
	b := addr.Bytes()
	h1 := sha256.Sum256(b)
	h2 := sha256.Sum256(h1[:])
	payload := make([]byte, len(b)+4)
	copy(payload, b)
	copy(payload[len(b):], h2[:4])
	return base58.Encode(payload)
}

func Base58ToAddress(s string) (common.Address, error) {
	var addr common.Address
	b, err := base58.Decode(s)
	if err != nil {
		return addr, err
	}
	if len(b) != common.AddressLength+4 {
		return addr, errors.New("invalid base58 address length")
	}
	payload := b[:common.AddressLength]
	checksum := b[common.AddressLength:]
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])
	if h2[0] != checksum[0] || h2[1] != checksum[1] ||
		h2[2] != checksum[2] || h2[3] != checksum[3] {
		return addr, errors.New("invalid base58 checksum")
	}
	copy(addr[:], payload)
	return addr, nil
}
