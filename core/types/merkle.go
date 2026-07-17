package types

import (
	"crypto/sha256"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// MerkleRoot returns the root of a binary Merkle tree built over the given
// leaf hashes, matching java-tron's `MerkleTree.createTree` semantics
// (`chainbase/.../capsule/utils/MerkleTree.java`):
//
//   - empty input → all-zero hash (java-tron's `txTrieRoot` for an empty
//     transaction list serializes to a 32-byte zero `Sha256Hash`).
//   - single leaf → that leaf's hash, unchanged.
//   - otherwise pair (i, i+1); parent = SHA256(left.bytes || right.bytes).
//     If a level has odd length, the trailing leaf carries up unchanged
//     (no doubling).
//
// Used for the genesis-block `tx_trie_root`, which feeds into the genesis
// block's SHA-256 and therefore into cross-implementation genesis-hash
// parity with java-tron.
func MerkleRoot(leaves []common.Hash) common.Hash {
	if len(leaves) == 0 {
		return common.Hash{}
	}
	level := make([]common.Hash, len(leaves))
	copy(level, leaves)
	for len(level) > 1 {
		next := make([]common.Hash, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			h := sha256.New()
			h.Write(level[i][:])
			h.Write(level[i+1][:])
			var sum common.Hash
			copy(sum[:], h.Sum(nil))
			next = append(next, sum)
		}
		level = next
	}
	return level[0]
}

// TransactionMerkleRoot hashes each transaction's complete protobuf bytes
// (raw_data, signatures and ret) before applying MerkleRoot. This is distinct
// from Transaction.Hash(), which is the txid and hashes raw_data only.
func TransactionMerkleRoot(transactions []*corepb.Transaction) (common.Hash, error) {
	leaves := make([]common.Hash, len(transactions))
	for i, tx := range transactions {
		data, err := proto.Marshal(tx)
		if err != nil {
			return common.Hash{}, fmt.Errorf("marshal transaction %d: %w", i, err)
		}
		leaves[i] = sha256.Sum256(data)
	}
	return MerkleRoot(leaves), nil
}
