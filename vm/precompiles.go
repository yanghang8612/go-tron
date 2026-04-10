package vm

import (
	"crypto/sha256"

	tcommon "github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/ripemd160"
)

// PrecompiledContract is the interface for precompiled contracts.
type PrecompiledContract interface {
	RequiredEnergy(input []byte) uint64
	Run(input []byte, energy uint64) ([]byte, uint64, error)
}

// precompiles maps addresses to precompiled contract implementations.
var precompiles = map[tcommon.Address]PrecompiledContract{
	precompileAddr(1): &ecRecover{},
	precompileAddr(2): &sha256hash{},
	precompileAddr(3): &ripemd160hash{},
	precompileAddr(4): &dataCopy{},
}

func precompileAddr(n byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[tcommon.AddressLength-1] = n
	return addr
}

// --- ECRecover (0x01) ---

type ecRecover struct{}

func (c *ecRecover) RequiredEnergy(_ []byte) uint64 { return 3000 }

func (c *ecRecover) Run(input []byte, energy uint64) ([]byte, uint64, error) {
	cost := c.RequiredEnergy(input)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	// Simplified: full secp256k1 recovery is deferred.
	// Return zeros for now — callers will get an empty address.
	return make([]byte, 32), cost, nil
}

// --- SHA256 (0x02) ---

type sha256hash struct{}

func (c *sha256hash) RequiredEnergy(input []byte) uint64 {
	words := toWordSize(uint64(len(input)))
	return 60 + 12*words
}

func (c *sha256hash) Run(input []byte, energy uint64) ([]byte, uint64, error) {
	cost := c.RequiredEnergy(input)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	hash := sha256.Sum256(input)
	return hash[:], cost, nil
}

// --- RIPEMD160 (0x03) ---

type ripemd160hash struct{}

func (c *ripemd160hash) RequiredEnergy(input []byte) uint64 {
	words := toWordSize(uint64(len(input)))
	return 600 + 120*words
}

func (c *ripemd160hash) Run(input []byte, energy uint64) ([]byte, uint64, error) {
	cost := c.RequiredEnergy(input)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	h := ripemd160.New()
	h.Write(input)
	digest := h.Sum(nil)
	result := make([]byte, 32)
	copy(result[32-len(digest):], digest)
	return result, cost, nil
}

// --- Identity / DataCopy (0x04) ---

type dataCopy struct{}

func (c *dataCopy) RequiredEnergy(input []byte) uint64 {
	words := toWordSize(uint64(len(input)))
	return 15 + 3*words
}

func (c *dataCopy) Run(input []byte, energy uint64) ([]byte, uint64, error) {
	cost := c.RequiredEnergy(input)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	output := make([]byte, len(input))
	copy(output, input)
	return output, cost, nil
}
