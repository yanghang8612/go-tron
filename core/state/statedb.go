package state

import (
	"errors"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// StateDB manages in-memory account state with MPT-backed commits.
type StateDB struct {
	db   *Database
	trie *trie.Trie

	stateObjects map[tcommon.Address]*stateObject
	witnesses    map[tcommon.Address]*types.Witness

	journal   *journal
	snapshots []int // journal length at each snapshot

	dynProps *DynamicProperties

	// originRoot is the trie root at the time of the last successful Commit (or
	// the root passed to New). It is used as the parent root when updating the
	// triedb so that the hashdb reference graph is correct across multiple blocks.
	originRoot ethcommon.Hash
}

// New creates a StateDB from the given state root.
func New(root tcommon.Hash, db *Database) (*StateDB, error) {
	tr, err := db.OpenTrie(ethcommon.Hash(root))
	if err != nil {
		return nil, err
	}
	return &StateDB{
		db:           db,
		trie:         tr,
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
		journal:      newJournal(),
		dynProps:     NewDynamicProperties(),
		originRoot:   ethcommon.Hash(root),
	}, nil
}

// GetAccount returns the account at addr, or nil if not found.
func (s *StateDB) GetAccount(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// GetOrCreateAccount returns the state object at addr, creating it if it doesn't exist.
// When a new account is created, a nil-prev journal entry is recorded so that
// snapshot revert can delete it.
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject {
	obj := s.getStateObject(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	// Journal a nil-prev entry so revert can delete this new account.
	s.journal.append(journalEntry{
		address: addr,
		prev:    nil,
	})
	obj = newEmptyStateObject(addr)
	s.stateObjects[addr] = obj
	return obj
}

// GetBalance returns the TRX balance of the account.
func (s *StateDB) GetBalance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Balance()
}

// AddBalance adds amount to the account's balance.
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() + amount)
	obj.markDirty()
}

// SubBalance subtracts amount from the account's balance.
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ErrInsufficientBalance
	}
	if obj.account.Balance() < amount {
		return ErrInsufficientBalance
	}
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() - amount)
	obj.markDirty()
	return nil
}

// AddFreezeV2 adds a freeze entry for the given resource type.
func (s *StateDB) AddFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFreezeV2(resourceType, amount)
	obj.markDirty()
}

// GetWitness returns the witness at addr.
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness {
	return s.witnesses[addr]
}

// PutWitness stores a witness.
func (s *StateDB) PutWitness(addr tcommon.Address, url string) {
	s.witnesses[addr] = types.NewWitness(addr, url)
}

// DynamicProperties returns the dynamic properties.
func (s *StateDB) DynamicProperties() *DynamicProperties {
	return s.dynProps
}

// SetDynamicProperties sets the dynamic properties (used during genesis setup).
func (s *StateDB) SetDynamicProperties(dp *DynamicProperties) {
	s.dynProps = dp
}

// Snapshot returns a snapshot ID for later revert.
func (s *StateDB) Snapshot() int {
	id := len(s.snapshots)
	s.snapshots = append(s.snapshots, s.journal.length())
	return id
}

// RevertToSnapshot reverts state changes to the given snapshot.
func (s *StateDB) RevertToSnapshot(id int) {
	if id >= len(s.snapshots) {
		return
	}
	journalLen := s.snapshots[id]
	s.journal.revert(s.stateObjects, journalLen)
	s.snapshots = s.snapshots[:id]
}

// Commit writes all dirty accounts to the MPT and returns the new root hash.
func (s *StateDB) Commit() (tcommon.Hash, error) {
	for addr, obj := range s.stateObjects {
		if !obj.dirty {
			continue
		}
		if obj.deleted {
			if err := s.trie.Delete(trieKey(addr)); err != nil {
				return tcommon.Hash{}, err
			}
			obj.dirty = false // Issue 2: clear dirty flag for deleted objects
			continue
		}
		data, err := obj.account.Marshal()
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.trie.Update(trieKey(addr), data); err != nil {
			return tcommon.Hash{}, err
		}
		obj.dirty = false
	}

	root, nodes := s.trie.Commit(false)
	if nodes != nil {
		// Issue 3: pass s.originRoot as parent so the hashdb reference graph is correct.
		if err := s.db.TrieDB().Update(root, s.originRoot, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}

	// Issue 1: reopen the trie from the new root so StateDB remains usable.
	newTrie, err := s.db.OpenTrie(root)
	if err != nil {
		return tcommon.Hash{}, err
	}
	s.trie = newTrie

	// Issue 3: advance originRoot for the next commit.
	s.originRoot = root

	// Issue 4: clear journal and snapshots after a successful commit.
	s.journal = newJournal()
	s.snapshots = s.snapshots[:0]

	return tcommon.Hash(root), nil
}

// getStateObject returns the state object for addr, loading from trie if needed.
func (s *StateDB) getStateObject(addr tcommon.Address) *stateObject {
	if obj, ok := s.stateObjects[addr]; ok {
		return obj
	}
	data, err := s.trie.Get(trieKey(addr))
	if err != nil || data == nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	obj := newStateObject(addr, acc)
	s.stateObjects[addr] = obj
	return obj
}

// journalAccount records the current state of an account for revert.
func (s *StateDB) journalAccount(addr tcommon.Address, obj *stateObject) {
	var prev []byte
	if obj != nil && obj.account != nil {
		prev, _ = obj.account.Marshal()
	}
	s.journal.append(journalEntry{
		address: addr,
		prev:    prev,
	})
}

// trieKey returns the MPT key for a TRON address: Keccak256(address).
func trieKey(addr tcommon.Address) []byte {
	return crypto.Keccak256(addr.Bytes())
}
