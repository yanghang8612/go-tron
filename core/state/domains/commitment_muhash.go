package domains

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// CommitmentIdentity binds a canonical source cut; zero Hash is reserved for
// explicitly initialized standalone state/genesis construction, never startup.
type CommitmentIdentity struct {
	BlockNum              uint64
	BlockHash, ParentHash common.Hash
	TxNum                 uint64
}

func ApplyMuHashCommitment(db CommitmentDB, updates []rawdb.StateCommitmentUpdate, id CommitmentIdentity) (common.Hash, error) {
	deltas, err := rawdb.StateCommitmentDeltasFromUpdates(updates)
	if err != nil {
		return common.Hash{}, err
	}
	return ApplyMuHashDeltas(db, deltas, id, common.Hash{})
}
func ApplyMuHashDeltas(db CommitmentDB, deltas []rawdb.StateCommitmentDelta, id CommitmentIdentity, expected common.Hash) (common.Hash, error) {
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return common.Hash{}, err
	}
	if !present {
		return common.Hash{}, rawdb.ErrStateCommitmentHead
	}
	currentRoot, hasRoot, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return common.Hash{}, err
	}
	if !hasRoot || currentRoot != head.Root {
		return common.Hash{}, errors.New("commitment HEAD/latest root mismatch")
	}
	deltas, err = rawdb.CoalesceStateCommitmentDeltas(deltas)
	if err != nil {
		return common.Hash{}, err
	}
	deltaID := rawdb.StateCommitmentDeltaID(deltas)
	bound := id.BlockHash != (common.Hash{})
	if bound && head.BlockHash == id.BlockHash && head.BlockNum == id.BlockNum {
		if head.ParentHash != id.ParentHash || head.TxNum != id.TxNum || head.DeltaID != deltaID || (expected != (common.Hash{}) && head.Root != expected) {
			return common.Hash{}, errors.New("commitment current identity retry conflicts with parent/delta")
		}
		return head.Root, nil
	}
	if !bound && head.BlockHash != (common.Hash{}) {
		return common.Hash{}, errors.New("unbound mutation cannot update canonical commitment HEAD")
	}
	if bound && expected == (common.Hash{}) {
		if id.ParentHash != head.BlockHash || id.BlockNum != head.BlockNum+1 {
			return common.Hash{}, fmt.Errorf("commitment parent cut mismatch: head=%d/%x requested=%d/%x parent=%x", head.BlockNum, head.BlockHash, id.BlockNum, id.BlockHash, id.ParentHash)
		}
	}
	acc, err := muhash3072.FromState(head.State)
	if err != nil {
		return common.Hash{}, err
	}
	acc, err = acc.Clone()
	if err != nil {
		return common.Hash{}, err
	}
	for _, d := range deltas {
		if d.BeforeExists {
			element, e := rawdb.EncodeStateCommitmentElement(d.Key, d.Before)
			if e != nil {
				return common.Hash{}, e
			}
			if e = acc.Remove(element); e != nil {
				return common.Hash{}, e
			}
		}
		if d.AfterExists {
			element, e := rawdb.EncodeStateCommitmentElement(d.Key, d.After)
			if e != nil {
				return common.Hash{}, e
			}
			if e = acc.Insert(element); e != nil {
				return common.Hash{}, e
			}
		}
	}
	final, err := acc.Finalize()
	if err != nil {
		return common.Hash{}, err
	}
	root := common.Hash(final.Digest)
	if expected != (common.Hash{}) && root != expected {
		return common.Hash{}, fmt.Errorf("MuHash inverse transition root mismatch: got%x want%x", root, expected)
	}
	// Materialize the captured source cut into the SAME bound layer. A scoped
	// latest batch may still retain identical puts for deferred overlay pruning;
	// canonical layer maps coalesce these, while HEAD never outruns its sources.
	for _, d := range deltas {
		if d.AfterExists {
			err = db.Put(d.Key, d.After)
		} else {
			err = db.Delete(d.Key)
		}
		if err != nil {
			return common.Hash{}, err
		}
	}
	next := head
	next.State = final.State
	next.Root = root
	next.DeltaID = deltaID
	next.BlockNum = id.BlockNum
	next.TxNum = id.TxNum
	if bound {
		next.BlockNum = id.BlockNum
		next.BlockHash = id.BlockHash
		next.ParentHash = id.ParentHash
		next.TxNum = id.TxNum
	}
	if err = rawdb.WriteStateCommitmentHead(db, next); err != nil {
		return common.Hash{}, err
	}
	if err = rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// UnwindMuHashCommitment requires an explicit current source identity and a
// historical target digest. The caller owns a disposable atomic writer.
func UnwindMuHashCommitment(db CommitmentDB, from CommitmentIdentity, target CommitmentIdentity, expected common.Hash) (common.Hash, error) {
	if expected == (common.Hash{}) || target.BlockHash == (common.Hash{}) || target.BlockNum >= from.BlockNum {
		return common.Hash{}, errors.New("invalid inverse commitment target")
	}
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return common.Hash{}, err
	}
	if !present || head.BlockHash != from.BlockHash || head.BlockNum != from.BlockNum || head.TxNum != from.TxNum {
		return common.Hash{}, errors.New("inverse commitment current cut mismatch")
	}
	updates, err := rawdb.CollectStateUnwind(db, from.BlockNum, target.BlockNum)
	if err != nil {
		return common.Hash{}, err
	}
	deltas, err := rawdb.StateCommitmentDeltasFromUpdates(updates)
	if err != nil {
		return common.Hash{}, err
	}
	return ApplyMuHashDeltas(db, deltas, target, expected)
}

// RebuildMuHashCommitment is an explicit offline operation, never a startup or
// failed-update fallback. Every physical row (including old KV generations) is
// authenticated; malformed source keys fail closed.
func RebuildMuHashCommitment(db CommitmentDB) (common.Hash, error) {
	if rebuildSpyHook != nil {
		rebuildSpyHook()
	}
	acc := muhash3072.NewAccumulator()
	if err := rawdb.IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
		element, err := rawdb.EncodeStateCommitmentElement(k, v)
		if err != nil {
			return false, err
		}
		return true, acc.Insert(element)
	}); err != nil {
		return common.Hash{}, err
	}
	final, err := acc.Finalize()
	if err != nil {
		return common.Hash{}, err
	}
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return common.Hash{}, err
	}
	if !present {
		head = rawdb.NewStateCommitmentHead()
	}
	head.State = final.State
	head.Root = common.Hash(final.Digest)
	if err = rawdb.WriteStateCommitmentHead(db, head); err != nil {
		return common.Hash{}, err
	}
	if err = rawdb.WriteLatestDomainCommitmentRoot(db, head.Root); err != nil {
		return common.Hash{}, err
	}
	return head.Root, nil
}

var rebuildSpyHook func()

// SetRebuildSpyHook instruments explicit offline rebuilds in tests.
func SetRebuildSpyHook(fn func()) { rebuildSpyHook = fn }
