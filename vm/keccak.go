package vm

import (
	"encoding/binary"
	"hash"
	"sync"

	gethkeccak "github.com/ethereum/go-ethereum/crypto/keccak"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// vmKeccakPool serves opcode hashing, CREATE address derivation and internal
// transaction identity construction. These are all short, non-overlapping
// digest operations. Reusing the sponge avoids allocating its state for every
// SHA3/CALL, while the destructive Read path avoids hash.Hash.Sum's state copy.
var vmKeccakPool = sync.Pool{
	New: func() any {
		return &vmKeccak{keccakState: gethkeccak.NewLegacyKeccak256().(keccakState)}
	},
}

type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type vmKeccak struct {
	keccakState
	digest     [tcommon.HashLength]byte
	address    [tcommon.AddressLength]byte
	valueNonce [16]byte
}

func internalTransactionHash(parent []byte, transferTo tcommon.Address, includeReceiver bool, data []byte, value int64, nonce uint64) (out tcommon.Hash) {
	h := vmKeccakPool.Get().(*vmKeccak)
	h.Reset()
	_, _ = h.Write(parent)
	if includeReceiver {
		copy(h.address[:], transferTo[:])
		_, _ = h.Write(h.address[:])
	}
	_, _ = h.Write(data)
	binary.BigEndian.PutUint64(h.valueNonce[:8], uint64(value))
	binary.BigEndian.PutUint64(h.valueNonce[8:], nonce)
	_, _ = h.Write(h.valueNonce[:])
	_, _ = h.Read(h.digest[:])
	copy(out[:], h.digest[:])
	vmKeccakPool.Put(h)
	return out
}

func keccak256Parts(parts ...[]byte) (out tcommon.Hash) {
	h := vmKeccakPool.Get().(*vmKeccak)
	h.Reset()
	for _, part := range parts {
		_, _ = h.Write(part)
	}
	_, _ = h.Read(h.digest[:])
	copy(out[:], h.digest[:])
	vmKeccakPool.Put(h)
	return out
}

func keccak256(data []byte) tcommon.Hash {
	return keccak256Parts(data)
}
