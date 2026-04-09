package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// journalEntry represents a single undo operation.
type journalEntry struct {
	address tcommon.Address
	prev    []byte // serialized Account protobuf before mutation, nil if account didn't exist
}

// journal tracks state changes for snapshot/revert.
type journal struct {
	entries []journalEntry
}

func newJournal() *journal {
	return &journal{}
}

func (j *journal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
}

func (j *journal) length() int {
	return len(j.entries)
}

func (j *journal) revert(stateObjects map[tcommon.Address]*stateObject, to int) {
	for i := len(j.entries) - 1; i >= to; i-- {
		entry := j.entries[i]
		if entry.prev == nil {
			delete(stateObjects, entry.address)
		} else {
			acc, err := types.UnmarshalAccount(entry.prev)
			if err != nil {
				continue
			}
			obj := stateObjects[entry.address]
			if obj != nil {
				obj.account = acc
				obj.dirty = true
			}
		}
	}
	j.entries = j.entries[:to]
}
