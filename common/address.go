package common

import "encoding/hex"

const (
	AddressLength        = 21
	AddressPrefixMainnet = 0x41
	AddressPrefixTestnet = 0xa0
)

type Address [AddressLength]byte

func BytesToAddress(b []byte) Address {
	var a Address
	if len(b) > AddressLength {
		b = b[len(b)-AddressLength:]
	}
	copy(a[AddressLength-len(b):], b)
	return a
}

func (a Address) Bytes() []byte  { return a[:] }
func (a Address) Hex() string    { return hex.EncodeToString(a[:]) }
func (a Address) String() string { return a.Hex() }

func (a Address) IsEmpty() bool {
	for _, b := range a {
		if b != 0 {
			return false
		}
	}
	return true
}
