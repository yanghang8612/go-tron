package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// journalChange represents a single undoable state change.
type journalChange interface {
	revert(stateObjects map[tcommon.Address]*stateObject, witnesses map[tcommon.Address]*types.Witness)
}

// accountChange records the previous account state for revert.
type accountChange struct {
	address          tcommon.Address
	prev             []byte // serialized Account protobuf before mutation, nil if account didn't exist
	prevLatest       []byte // encoded flat account-latest envelope before mutation
	prevDeleted      bool
	prevCreated      bool
	prevSelfDestruct bool
}

func (e accountChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	if e.prev == nil {
		delete(stateObjects, e.address)
	} else {
		acc, err := types.UnmarshalAccount(e.prev)
		if err != nil {
			return
		}
		obj := stateObjects[e.address]
		if obj == nil {
			obj = newStateObject(e.address, acc)
			stateObjects[e.address] = obj
		} else {
			obj.account = acc
		}
		// accountChange pre-images are produced by deterministicAccountProto,
		// so the restored object can reuse the exact bytes until its next
		// mutation. The journal owns the backing slice for as long as the object
		// can reference it, including after the entry is removed on revert.
		obj.accountProto = e.prev
		obj.dirty = true
		obj.accountDirty = true
		obj.deleted = e.prevDeleted
		obj.created = e.prevCreated
		obj.selfDestructed = e.prevSelfDestruct
	}
}

// witnessChange records the previous witness state for revert.
type witnessChange struct {
	address tcommon.Address
	prev    *types.Witness // nil means witness didn't exist before
}

func (e witnessChange) revert(_ map[tcommon.Address]*stateObject, witnesses map[tcommon.Address]*types.Witness) {
	if e.prev == nil {
		delete(witnesses, e.address)
	} else {
		witnesses[e.address] = e.prev
	}
}

// storageChange records a single storage slot change for revert.
type storageChange struct {
	address    tcommon.Address
	key        tcommon.Hash
	prev       tcommon.Hash
	prevExists bool
	prevDirty  bool
}

func (e storageChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj != nil {
		if e.prev == (tcommon.Hash{}) && !e.prevExists {
			delete(obj.storage, e.key)
			delete(obj.storageExists, e.key)
		} else {
			obj.storage[e.key] = e.prev
			obj.storageExists[e.key] = e.prevExists
		}
		if obj.dirtyStorage == nil {
			obj.dirtyStorage = make(map[tcommon.Hash]struct{})
		}
		if e.prevDirty {
			obj.dirtyStorage[e.key] = struct{}{}
		} else {
			delete(obj.dirtyStorage, e.key)
		}
		obj.markDirty()
	}
}

// codeChange records a code change for revert.
type codeChange struct {
	address    tcommon.Address
	prevCode   []byte
	prevHash   tcommon.Hash
	prevLatest []byte
}

func (e codeChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		if len(e.prevCode) == 0 {
			return
		}
		obj = newEmptyStateObject(e.address)
		stateObjects[e.address] = obj
	}
	obj.code = e.prevCode
	obj.codeHash = e.prevHash
	obj.codeDirty = true
}

// contractMetaChange records a contract metadata change for revert.
type contractMetaChange struct {
	address  tcommon.Address
	prevMeta *contractpb.SmartContract
}

func (e contractMetaChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		if e.prevMeta == nil {
			return
		}
		obj = newEmptyStateObject(e.address)
		stateObjects[e.address] = obj
	}
	obj.contractMeta = e.prevMeta
	obj.contractMetaDirty = e.prevMeta != nil
}

// selfDestructChange records a self-destruct for revert.
type selfDestructChange struct {
	address tcommon.Address
	prev    bool
}

func (e selfDestructChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj != nil {
		obj.selfDestructed = e.prev
	}
}

// kvChange records a single generic-KV overlay change for revert.
type kvChange struct {
	address   tcommon.Address
	mapKey    string
	hadEntry  bool
	prevEntry kvEntry
}

func (e kvChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		return
	}
	if e.hadEntry {
		obj.kvDirty[e.mapKey] = e.prevEntry
	} else {
		delete(obj.kvDirty, e.mapKey)
	}
}

// kvResetChange records a generic-KV reset (generation bump) for revert. It
// snapshots the prior root, generation, AND the dirty overlay, because the
// reset clears the overlay and the post-reset overlay belongs to a new generation.
type kvResetChange struct {
	address              tcommon.Address
	prevRoot             tcommon.Hash
	prevGeneration       uint64
	prevGenerationExists bool
	prevGenerationDirty  bool
	prevDirty            map[string]kvEntry
}

func (e kvResetChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		return
	}
	obj.accountKVRoot = e.prevRoot
	obj.accountKVGeneration = e.prevGeneration
	obj.accountKVGenerationDirty = e.prevGenerationDirty
	obj.kvDirty = e.prevDirty
}

// transientStorageChange records a single EIP-1153 transient storage write for
// revert. It captures the StateDB.transientStorage map by reference (maps are
// reference types) rather than the *StateDB, because the journal revert
// signature only exposes stateObjects/witnesses. Restoring prev (which may be
// the zero hash for a slot that was previously unset) is sufficient: a reader
// cannot distinguish an absent slot from one holding the zero hash.
type transientStorageChange struct {
	storage map[transientStorageKey]tcommon.Hash
	tk      transientStorageKey
	prev    tcommon.Hash
}

func (e transientStorageChange) revert(_ map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	e.storage[e.tk] = e.prev
}

// resourceWeightChange records a total_*_weight delta applied to the dynamic
// properties inside a snapshot-scoped frame — a TVM resource-staking opcode
// (FREEZE/UNFREEZE) or the selfdestruct release. java applies these to a
// discardable Repository whose total_*_weight delta is dropped on revert; gtron
// mutates the shared DynamicProperties directly and DynamicProperties.Set is not
// journaled, so without this a freeze-opcode-then-revert would leak the weight
// and over-count total_energy_weight. Like transientStorageChange it captures
// the *DynamicProperties target by reference (the revert signature only exposes
// stateObjects/witnesses) and applies the inverse through the non-journaled
// applyResourceWeight, so reverting does not itself re-journal.
type resourceWeightChange struct {
	dp       *DynamicProperties
	resource corepb.ResourceCode
	delta    int64
}

func (e resourceWeightChange) revert(_ map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	applyResourceWeight(e.dp, e.resource, -e.delta)
}

// journal tracks state changes for snapshot/revert.
type journal struct {
	entries []journalChange
}

func newJournal() *journal {
	return &journal{}
}

func (j *journal) append(entry journalChange) {
	j.entries = append(j.entries, entry)
}

func (j *journal) length() int {
	return len(j.entries)
}

func (j *journal) revert(stateObjects map[tcommon.Address]*stateObject, witnesses map[tcommon.Address]*types.Witness, to int) {
	for i := len(j.entries) - 1; i >= to; i-- {
		j.entries[i].revert(stateObjects, witnesses)
	}
	j.entries = j.entries[:to]
}
