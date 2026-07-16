package jsonrpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tronprotocol/go-tron/common"
)

// invalidParamsError makes validation failures retain the JSON-RPC -32602
// code when they pass through the reflection-based RPC server.
type invalidParamsError struct {
	message string
}

func (e *invalidParamsError) Error() string  { return e.message }
func (e *invalidParamsError) ErrorCode() int { return codeInvalidParams }

func invalidAddressError() error {
	return &invalidParamsError{message: "invalid address hash value"}
}

// decodeAddressHex mirrors java-tron's ByteArray.fromHexString for address
// inputs: a lowercase 0x prefix is optional and an odd nibble count is padded
// with a leading zero. Unlike common.FromHex, malformed input is not silently
// converted to nil.
func decodeAddressHex(input string) ([]byte, error) {
	hexInput := input
	if strings.HasPrefix(hexInput, "0x") {
		hexInput = hexInput[2:]
	}
	if len(hexInput)%2 != 0 {
		hexInput = "0" + hexInput
	}
	decoded, err := hex.DecodeString(hexInput)
	if err != nil {
		return nil, invalidAddressError()
	}
	return decoded, nil
}

// parseCompatibleAddress mirrors java-tron's addressCompatibleToByteArray.
// Ethereum-style 20-byte addresses gain TRON's 0x41 prefix; 21-byte inputs
// must already carry that prefix.
func parseCompatibleAddress(input string) (common.Address, error) {
	decoded, err := decodeAddressHex(input)
	if err != nil {
		return common.Address{}, err
	}

	switch len(decoded) {
	case common.AccountIDLength:
		var id common.AccountID
		copy(id[:], decoded)
		return id.Address(common.AddressPrefixMainnet), nil
	case common.AddressLength:
		addr := common.BytesToAddress(decoded)
		if !addr.ValidPrefix() {
			return common.Address{}, invalidAddressError()
		}
		return addr, nil
	default:
		return common.Address{}, invalidAddressError()
	}
}

// parseLogAddress mirrors java-tron's addressToByteArray. Log filters use the
// Ethereum-facing 20-byte address shape and deliberately reject 21-byte TRON
// addresses, unlike account and call methods.
func parseLogAddress(input string) (common.Address, error) {
	decoded, err := decodeAddressHex(input)
	if err != nil || len(decoded) != common.AccountIDLength {
		return common.Address{}, &invalidParamsError{message: fmt.Sprintf("invalid address: %s", input)}
	}
	var id common.AccountID
	copy(id[:], decoded)
	return id.Address(common.AddressPrefixMainnet), nil
}

// parseFilterAddresses handles the string-or-array address field shared by
// eth_getLogs, eth_newFilter, and WebSocket log subscriptions.
func parseFilterAddresses(raw []byte) ([]common.Address, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		addr, err := parseLogAddress(single)
		if err != nil {
			return nil, err
		}
		return []common.Address{addr}, nil
	}

	var inputs []string
	if err := json.Unmarshal(raw, &inputs); err != nil {
		return nil, &invalidParamsError{message: "invalid addresses in query"}
	}
	addresses := make([]common.Address, 0, len(inputs))
	for i, input := range inputs {
		addr, err := parseLogAddress(input)
		if err != nil {
			return nil, &invalidParamsError{message: fmt.Sprintf("invalid address at index %d: %s", i, input)}
		}
		addresses = append(addresses, addr)
	}
	return addresses, nil
}
