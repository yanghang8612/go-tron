package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// journalChange represents a single undoable state change.
type journalChange interface {
	revert(stateObjects map[tcommon.Address]*stateObject, witnesses map[tcommon.Address]*types.Witness)
}

// accountChange records the previous account state for revert.
type accountChange struct {
	address tcommon.Address
	prev    []byte // serialized Account protobuf before mutation, nil if account didn't exist
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
		if obj != nil {
			obj.account = acc
			obj.dirty = true
		}
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
