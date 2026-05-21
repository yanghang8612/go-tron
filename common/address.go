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

// AccountIDLength is the rooted-state account identity size: a TRON address
// with its 1-byte network prefix (0x41 / 0xa0) stripped.
const AccountIDLength = AddressLength - 1

// AccountID is the 20-byte rooted-state owner identity. It matches the
// Solidity/TVM ABI address-word shape and is used for all internal rooted-state
// keying. Protocol boundaries keep the 21-byte Address; only the state layer
// normalizes to AccountID.
type AccountID [AccountIDLength]byte

// AccountID strips the network prefix byte. The caller is responsible for
// prefix validation at protocol boundaries; the state layer keys by identity.
func (a Address) AccountID() AccountID {
	var id AccountID
	copy(id[:], a[1:])
	return id
}

// ValidPrefix reports whether the address carries a known TRON network prefix.
func (a Address) ValidPrefix() bool {
	return a[0] == AddressPrefixMainnet || a[0] == AddressPrefixTestnet
}

func (id AccountID) Bytes() []byte { return id[:] }

// Address re-attaches a network prefix to produce a 21-byte TRON address.
func (id AccountID) Address(prefix byte) Address {
	var a Address
	a[0] = prefix
	copy(a[1:], id[:])
	return a
}
