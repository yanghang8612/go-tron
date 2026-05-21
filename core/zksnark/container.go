package zksnark

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"google.golang.org/protobuf/proto"
)

// DB is the read+write capability MerkleContainer needs from rawdb-or-buffer.
// Mirrors actuator.BufferedKVStore but is declared here so the zksnark
// package does not depend on the actuator package.
type DB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// MerkleContainer orchestrates the LAST_TREE / CURRENT_TREE / root-keyed
// store the way java-tron's `org.tron.common.zksnark.MerkleContainer`
// does. It is intentionally stateless: every method reads + writes through
// the provided `db`, so a `bc.buffer.Buffer` view rolls back together with
// the block on switchFork / discard.
//
// All operations use the production Sapling depth (`Depth = 32`).
type MerkleContainer struct {
	db DB
}

// NewMerkleContainer returns a container wrapping db.
func NewMerkleContainer(db DB) *MerkleContainer {
	return &MerkleContainer{db: db}
}

// GetBest returns the best (most-recently-saved) tree. Empty when no shielded
// receive has fired yet on this chain.
func (c *MerkleContainer) GetBest() *IncrementalMerkleTree {
	if pb := rawdb.ReadLastMerkleTree(c.db); pb != nil {
		return FromProto(pb)
	}
	return NewTree()
}

// GetCurrent returns the working tree being mutated by this block's shielded
// receives. Falls back to GetBest (and then the empty tree) when the
// CURRENT_TREE sentinel is absent — same fallback chain as java-tron's
// `MerkleContainer.getCurrentMerkle`.
func (c *MerkleContainer) GetCurrent() *IncrementalMerkleTree {
	if pb := rawdb.ReadCurrentMerkleTree(c.db); pb != nil {
		return FromProto(pb)
	}
	return c.GetBest()
}

// ResetCurrent copies the best tree into the CURRENT_TREE sentinel. Called
// from applyBlock before transaction execution.
func (c *MerkleContainer) ResetCurrent() error {
	best := c.GetBest()
	currentPB := rawdb.ReadCurrentMerkleTree(c.db)
	if currentPB == nil && best.Size() == 0 {
		return nil
	}
	if currentPB != nil && proto.Equal(currentPB, best.Proto()) {
		return nil
	}
	return rawdb.WriteCurrentMerkleTree(c.db, best.Proto())
}

// AppendCommitment is the actuator-side hook: load CURRENT_TREE, append the
// note commitment, persist. Mirrors java-tron's
// `MerkleContainer.saveCmIntoMerkleTree`.
//
// Does not compute the new tree root — that's deferred to SaveCurrentAsBest,
// keeping append cheap and (critically) not requiring the Pedersen backend
// when the tree state happens to fit in the bottom slots (which lets unit
// tests of the actuator pass on the default no-CGO build).
func (c *MerkleContainer) AppendCommitment(cm PedersenHash) error {
	cur := c.GetCurrent()
	if err := cur.Append(cm); err != nil {
		return err
	}
	return rawdb.WriteCurrentMerkleTree(c.db, cur.Proto())
}

// SaveCurrentAsBest promotes CURRENT_TREE to LAST_TREE, stores the tree
// under its root (so anchor lookups succeed), and records the
// blockNum → root mapping.
//
// Mirrors java-tron's `MerkleContainer.saveCurrentMerkleTreeAsBestMerkleTree`.
func (c *MerkleContainer) SaveCurrentAsBest(blockNum int64) error {
	cur := c.GetCurrent()
	last := rawdb.ReadLastMerkleTree(c.db)
	if root := rawdb.ReadMerkleTreeRootByBlock(c.db, blockNum-1); len(root) == len(PedersenHash{}) {
		if last != nil && proto.Equal(cur.Proto(), last) {
			return rawdb.WriteMerkleTreeRootByBlock(c.db, blockNum, root)
		}
		if last == nil && cur.Size() == 0 &&
			rawdb.HasIncrMerkleTree(c.db, root) &&
			rawdb.ReadIncrMerkleTree(c.db, root) == nil {
			// Empty IncrementalMerkleTree marshals to zero bytes, so rawdb
			// intentionally reads LAST_TREE back as nil. The previous block's
			// root-keyed store entry still proves the best tree is the same
			// empty tree; reuse it instead of recomputing the 32-level
			// Sapling empty root on every post-activation transparent block.
			return rawdb.WriteMerkleTreeRootByBlock(c.db, blockNum, root)
		}
	}
	root, err := cur.MerkleTreeKey()
	if err != nil {
		return err
	}
	return c.writeBestRootIndex(blockNum, root, cur)
}

func (c *MerkleContainer) writeBestRootIndex(blockNum int64, root []byte, tree *IncrementalMerkleTree) error {
	if err := rawdb.WriteLastMerkleTree(c.db, tree.Proto()); err != nil {
		return err
	}
	if err := rawdb.WriteIncrMerkleTree(c.db, root, tree.Proto()); err != nil {
		return err
	}
	return rawdb.WriteMerkleTreeRootByBlock(c.db, blockNum, root)
}

// AnchorExists reports whether the given root is a valid spend anchor —
// i.e. a tree root we previously committed in some prior block. Mirrors
// `MerkleContainer.merkleRootExist`.
func (c *MerkleContainer) AnchorExists(root []byte) bool {
	return rawdb.HasIncrMerkleTree(c.db, root)
}
