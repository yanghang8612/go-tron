package zksnark

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// DB is the read+write capability MerkleContainer needs from rawdb-or-buffer.
// Mirrors actuator.BufferedKVStore but is declared here so the zksnark
// package does not depend on the actuator package.
type DB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

type Store interface {
	ReadLastMerkleTree() *shieldpb.IncrementalMerkleTree
	WriteLastMerkleTree(*shieldpb.IncrementalMerkleTree) error
	ReadCurrentMerkleTree() *shieldpb.IncrementalMerkleTree
	WriteCurrentMerkleTree(*shieldpb.IncrementalMerkleTree) error
	ReadIncrMerkleTree(root []byte) *shieldpb.IncrementalMerkleTree
	WriteIncrMerkleTree(root []byte, tree *shieldpb.IncrementalMerkleTree) error
	HasIncrMerkleTree(root []byte) bool
	ReadMerkleTreeRootByBlock(blockNum int64) []byte
	WriteMerkleTreeRootByBlock(blockNum int64, root []byte) error
}

type rawDBStore struct {
	db DB
}

func (s rawDBStore) ReadLastMerkleTree() *shieldpb.IncrementalMerkleTree {
	return rawdb.ReadLastMerkleTree(s.db)
}

func (s rawDBStore) WriteLastMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	return rawdb.WriteLastMerkleTree(s.db, tree)
}

func (s rawDBStore) ReadCurrentMerkleTree() *shieldpb.IncrementalMerkleTree {
	return rawdb.ReadCurrentMerkleTree(s.db)
}

func (s rawDBStore) WriteCurrentMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	return rawdb.WriteCurrentMerkleTree(s.db, tree)
}

func (s rawDBStore) ReadIncrMerkleTree(root []byte) *shieldpb.IncrementalMerkleTree {
	return rawdb.ReadIncrMerkleTree(s.db, root)
}

func (s rawDBStore) WriteIncrMerkleTree(root []byte, tree *shieldpb.IncrementalMerkleTree) error {
	return rawdb.WriteIncrMerkleTree(s.db, root, tree)
}

func (s rawDBStore) HasIncrMerkleTree(root []byte) bool {
	return rawdb.HasIncrMerkleTree(s.db, root)
}

func (s rawDBStore) ReadMerkleTreeRootByBlock(blockNum int64) []byte {
	return rawdb.ReadMerkleTreeRootByBlock(s.db, blockNum)
}

func (s rawDBStore) WriteMerkleTreeRootByBlock(blockNum int64, root []byte) error {
	return rawdb.WriteMerkleTreeRootByBlock(s.db, blockNum, root)
}

// MerkleContainer orchestrates the LAST_TREE / CURRENT_TREE / root-keyed
// store the way java-tron's `org.tron.common.zksnark.MerkleContainer`
// does. It is intentionally stateless: every method reads + writes through
// the provided `db`, so a `bc.buffer.Buffer` view rolls back together with
// the block on switchFork / discard.
//
// All operations use the production Sapling depth (`Depth = 32`).
type MerkleContainer struct {
	store Store
}

// NewMerkleContainer returns a container wrapping a typed shielded store.
func NewMerkleContainer(store Store) *MerkleContainer {
	return &MerkleContainer{store: store}
}

// NewMerkleContainerFromDB returns a compatibility container wrapping a raw
// key/value DB. Production state execution should use NewMerkleContainer with
// StateDB's SystemShielded store.
func NewMerkleContainerFromDB(db DB) *MerkleContainer {
	return NewMerkleContainer(rawDBStore{db: db})
}

// GetBest returns the best (most-recently-saved) tree. Empty when no shielded
// receive has fired yet on this chain.
func (c *MerkleContainer) GetBest() *IncrementalMerkleTree {
	if pb := c.store.ReadLastMerkleTree(); pb != nil {
		return FromProto(pb)
	}
	return NewTree()
}

// GetCurrent returns the working tree being mutated by this block's shielded
// receives. Falls back to GetBest (and then the empty tree) when the
// CURRENT_TREE sentinel is absent — same fallback chain as java-tron's
// `MerkleContainer.getCurrentMerkle`.
func (c *MerkleContainer) GetCurrent() *IncrementalMerkleTree {
	if pb := c.store.ReadCurrentMerkleTree(); pb != nil {
		return FromProto(pb)
	}
	return c.GetBest()
}

// ResetCurrent copies the best tree into the CURRENT_TREE sentinel. Called
// from applyBlock before transaction execution.
func (c *MerkleContainer) ResetCurrent() error {
	best := c.GetBest()
	currentPB := c.store.ReadCurrentMerkleTree()
	if currentPB == nil && best.Size() == 0 {
		return nil
	}
	if currentPB != nil && proto.Equal(currentPB, best.Proto()) {
		return nil
	}
	return c.store.WriteCurrentMerkleTree(best.Proto())
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
	return c.store.WriteCurrentMerkleTree(cur.Proto())
}

// SaveCurrentAsBest promotes CURRENT_TREE to LAST_TREE, stores the tree
// under its root (so anchor lookups succeed), and records the
// blockNum → root mapping.
//
// Mirrors java-tron's `MerkleContainer.saveCurrentMerkleTreeAsBestMerkleTree`.
func (c *MerkleContainer) SaveCurrentAsBest(blockNum int64) error {
	cur := c.GetCurrent()
	last := c.store.ReadLastMerkleTree()
	if root := c.store.ReadMerkleTreeRootByBlock(blockNum - 1); len(root) == len(PedersenHash{}) {
		if last != nil && proto.Equal(cur.Proto(), last) {
			return c.store.WriteMerkleTreeRootByBlock(blockNum, root)
		}
		if last == nil && cur.Size() == 0 &&
			c.store.HasIncrMerkleTree(root) &&
			c.store.ReadIncrMerkleTree(root) == nil {
			// Empty IncrementalMerkleTree marshals to zero bytes, so rawdb
			// intentionally reads LAST_TREE back as nil. The previous block's
			// root-keyed store entry still proves the best tree is the same
			// empty tree; reuse it instead of recomputing the 32-level
			// Sapling empty root on every post-activation transparent block.
			return c.store.WriteMerkleTreeRootByBlock(blockNum, root)
		}
	}
	root, err := cur.MerkleTreeKey()
	if err != nil {
		return err
	}
	return c.writeBestRootIndex(blockNum, root, cur)
}

func (c *MerkleContainer) writeBestRootIndex(blockNum int64, root []byte, tree *IncrementalMerkleTree) error {
	if err := c.store.WriteLastMerkleTree(tree.Proto()); err != nil {
		return err
	}
	if err := c.store.WriteIncrMerkleTree(root, tree.Proto()); err != nil {
		return err
	}
	return c.store.WriteMerkleTreeRootByBlock(blockNum, root)
}

// AnchorExists reports whether the given root is a valid spend anchor —
// i.e. a tree root we previously committed in some prior block. Mirrors
// `MerkleContainer.merkleRootExist`.
func (c *MerkleContainer) AnchorExists(root []byte) bool {
	return c.store.HasIncrMerkleTree(root)
}
