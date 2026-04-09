package common

import (
	"crypto/sha256"
	"encoding/hex"
)

const HashLength = 32

type Hash [HashLength]byte

func BytesToHash(b []byte) Hash {
	var h Hash
	if len(b) > HashLength {
		b = b[len(b)-HashLength:]
	}
	copy(h[HashLength-len(b):], b)
	return h
}

func HexToHash(s string) Hash {
	b, _ := hex.DecodeString(s)
	return BytesToHash(b)
}

func Sha256(data []byte) Hash {
	return sha256.Sum256(data)
}

func (h Hash) Bytes() []byte   { return h[:] }
func (h Hash) Hex() string     { return hex.EncodeToString(h[:]) }
func (h Hash) String() string  { return h.Hex() }

func (h Hash) IsEmpty() bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}
