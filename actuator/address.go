package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
)

func validAddressBytes(addr []byte) bool {
	if len(addr) != common.AddressLength {
		return false
	}
	return addr[0] == common.AddressPrefixMainnet || addr[0] == common.AddressPrefixTestnet
}

func checkedAddress(addr []byte, field string) (common.Address, error) {
	if !validAddressBytes(addr) {
		return common.Address{}, fmt.Errorf("invalid %s", field)
	}
	return common.BytesToAddress(addr), nil
}
