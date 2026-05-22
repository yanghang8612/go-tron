package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
)

func validAddressBytes(addr []byte) bool {
	if len(addr) != common.AddressLength {
		return false
	}
	// Only the mainnet prefix 0x41 is accepted (java-tron parity; modern Nile
	// also uses 0x41). The legacy 0xa0 testnet prefix is rejected — accepting it
	// would let a 0xa0 address collide with a 0x41 address on the same AccountID.
	return addr[0] == common.AddressPrefixMainnet
}

func checkedAddress(addr []byte, field string) (common.Address, error) {
	if !validAddressBytes(addr) {
		return common.Address{}, fmt.Errorf("invalid %s", field)
	}
	return common.BytesToAddress(addr), nil
}
