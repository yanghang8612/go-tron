package domains

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type CommitmentDB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

// CommitmentSnapshotSource exposes only a historical digest anchor. It cannot
// reconstruct a complete accumulator or repair a missing HEAD.
type CommitmentSnapshotSource interface {
	GetCommitmentRoot(txNum uint64) (common.Hash, bool, error)
}
type CommitmentSnapshotRepair struct {
	Source CommitmentSnapshotSource
	TxNum  uint64
}
