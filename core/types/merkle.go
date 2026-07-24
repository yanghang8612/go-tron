package types

import (
	"crypto/sha256"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	if len(leaves) == 1 {
		return leaves[0]
	}
	level := make([]common.Hash, len(leaves))
	copy(level, leaves)
	return merkleRootOwned(level)
}

// merkleRootOwned folds a caller-owned leaf slice in place. Each output level
// is shorter than its input, so writing parents into the front cannot overwrite
// unread children. Pairing and odd-leaf carry semantics match MerkleRoot.
func merkleRootOwned(level []common.Hash) common.Hash {
	for count := len(level); count > 1; {
		parents := 0
		for child := 0; child < count; child += 2 {
			if child+1 == count {
				level[parents] = level[child]
			} else {
				var pair [2 * sha256.Size]byte
				copy(pair[:sha256.Size], level[child][:])
				copy(pair[sha256.Size:], level[child+1][:])
				level[parents] = sha256.Sum256(pair[:])
			}
			parents++
		}
		count = parents
	}
	if len(level) == 0 {
		return common.Hash{}
	}
	return level[0]
}

// TransactionMerkleRoot hashes each transaction's complete protobuf bytes
// (raw_data, signatures and ret) before applying MerkleRoot. This is distinct
// from Transaction.Hash(), which is the txid and hashes raw_data only.
func TransactionMerkleRoot(transactions []*corepb.Transaction) (common.Hash, error) {
	leaves := make([]common.Hash, len(transactions))
	for i, tx := range transactions {
		hash, err := hashProtoMessage(tx)
		if err != nil {
			return common.Hash{}, fmt.Errorf("marshal transaction %d: %w", i, err)
		}
		leaves[i] = hash
	}
	return merkleRootOwned(leaves), nil
}
