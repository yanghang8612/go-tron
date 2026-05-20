package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	historypb "github.com/tronprotocol/go-tron/proto/core/historystate"
	"google.golang.org/protobuf/proto"
)

// SetHistoryEnabled toggles the State History Index capture path. Off by
// default; applyBlock turns it on per-block when the chain config opts in.
// Tests use this directly to exercise AccumulateHistory.
func (s *StateDB) SetHistoryEnabled(on bool) {
	s.historyEnabled = on
}

// HistoryEnabled reports whether SHI capture is on for this StateDB. Used by
// tests and by the BlockChain belt-and-braces gate.
func (s *StateDB) HistoryEnabled() bool {
	return s.historyEnabled
}

// AccumulateHistory walks the per-block journal in-order and flushes one
// AccountDelta row per touched account, one SlotDelta row per touched
// (account, slot), and a StateHistoryMeta record into `buf`. All writes
// route through `buf` (typically *blockbuffer.Buffer) so that switchFork's
// DiscardBlock rewinds them automatically on orphan-branch discard.
//
// IMPORTANT: must run BEFORE StateDB.Commit(), which truncates the journal
// (newJournal()). applyBlock invokes us in that exact window.
//
// Capture rule — FIRST-seen wins per key:
//
//   - The first accountChange / codeChange / contractMetaChange for an addr
//     carries the pre-block value (prev = serialized account proto, prevCode,
//     or prevMeta clone), because each mutator stamps it via journalAccount/
//     codeChange/contractMetaChange BEFORE the actual mutation runs. Later
//     journal entries for the same addr already see the in-block-mutated
//     state, so we ignore them.
//
//   - Same for storageChange: the first occurrence per (addr, slot) holds
//     the pre-block slot value (zero hash if the slot was empty pre-block,
//     a real hash otherwise). Later writes to the same slot stack on top
//     and must NOT overwrite the captured pre-value.
//
// Snapshot/revert truncates the journal, so any entry an inner-tx revert
// rolled back is already gone before we walk — RevertToSnapshot's
// j.entries = j.entries[:to] guarantees only surviving mutations remain.
//
// Edge cases (verified against journal.go semantics):
//
//   - New account created this block: first accountChange.prev == nil; we
//     emit AccountDelta{ExistedPre: false} with all *Pre fields empty.
//   - SELFDESTRUCT-then-CREATE2 in the same block: the journal contains
//     the original accountChange (prev=<original>) FIRST, then later a
//     prev=nil entry from re-creation. First-seen captures the original.
//   - Account fully reverted out: journal entries truncated, never seen.
//   - Pure-SSTORE-touched contract (no Account proto mutation): no
//     accountChange entry, so no AccountDelta row — only SlotDelta rows.
//     Reader callers must consult both prefixes; the inverse indexes
//     keyed on addr respectively (addr,slot) tell them where to look.
//
// Returns nil immediately when capture is disabled — non-archive operators
// pay one bool check per block.
func (s *StateDB) AccumulateHistory(buf ethdb.KeyValueWriter, blockNum uint64, blockHash tcommon.Hash) error {
	if !s.historyEnabled {
		return nil
	}

	// First-seen trackers. Presence in a map = the per-block first capture
	// has already happened for that key. We use sentinel structs over
	// boolean maps so we can carry the captured value alongside.
	type accSeen struct {
		existedPre bool
		protoPre   []byte
	}
	type metaSeen struct {
		hasPrev  bool
		prevMeta *contractpb.SmartContract
	}
	type codeSeen struct {
		prevCode []byte
	}

	// Size hint: at most one entry per journal record. Worst case is every
	// entry being a distinct (address, *) — over-allocates slightly but
	// avoids growth-related rehashes on archive-heavy blocks.
	hint := len(s.journal.entries)
	firstAcc := make(map[tcommon.Address]accSeen, hint)
	firstCode := make(map[tcommon.Address]codeSeen, hint)
	firstMeta := make(map[tcommon.Address]metaSeen, hint)
	firstSlot := make(map[tcommon.Address]map[tcommon.Hash]tcommon.Hash, hint)

	for _, entry := range s.journal.entries {
		switch e := entry.(type) {
		case accountChange:
			if _, seen := firstAcc[e.address]; seen {
				continue
			}
			existedPre := e.prev != nil && !e.prevDeleted
			var protoPre []byte
			if existedPre {
				protoPre = e.prev
			}
			firstAcc[e.address] = accSeen{
				existedPre: existedPre,
				protoPre:   protoPre,
			}
		case codeChange:
			if _, seen := firstCode[e.address]; seen {
				continue
			}
			// Defensive copy: journal hands us the raw slice from the
			// stateObject; we don't want a later mutation to alias.
			var pc []byte
			if len(e.prevCode) > 0 {
				pc = append([]byte(nil), e.prevCode...)
			}
			firstCode[e.address] = codeSeen{prevCode: pc}
		case contractMetaChange:
			if _, seen := firstMeta[e.address]; seen {
				continue
			}
			firstMeta[e.address] = metaSeen{
				hasPrev:  e.prevMeta != nil,
				prevMeta: e.prevMeta,
			}
		case storageChange:
			perAddr, ok := firstSlot[e.address]
			if !ok {
				perAddr = make(map[tcommon.Hash]tcommon.Hash)
				firstSlot[e.address] = perAddr
			}
			if _, seen := perAddr[e.key]; seen {
				continue
			}
			perAddr[e.key] = e.prev
		}
	}

	// Union of addrs across the three account-shape trackers — a contract
	// touched only via codeChange / contractMetaChange (no accountChange)
	// still needs an AccountDelta row because code_pre / contract_meta_pre
	// live in that row, not in sh-s-.
	addrUnion := make(map[tcommon.Address]struct{}, len(firstAcc)+len(firstCode)+len(firstMeta))
	for a := range firstAcc {
		addrUnion[a] = struct{}{}
	}
	for a := range firstCode {
		addrUnion[a] = struct{}{}
	}
	for a := range firstMeta {
		addrUnion[a] = struct{}{}
	}

	var totalSlots uint32
	for _, perAddr := range firstSlot {
		totalSlots += uint32(len(perAddr))
	}

	// Account deltas + addr inverse index.
	for addr := range addrUnion {
		acc := firstAcc[addr] // zero value if absent → existedPre=false, protoPre=nil
		code := firstCode[addr]
		meta := firstMeta[addr]

		var metaPreBytes []byte
		if meta.hasPrev && meta.prevMeta != nil {
			b, err := proto.Marshal(meta.prevMeta)
			if err != nil {
				return err
			}
			metaPreBytes = b
		}

		delta := &historypb.AccountDelta{
			Addr:            addr.Bytes(),
			ExistedPre:      acc.existedPre,
			AccountProtoPre: acc.protoPre,
			CodePre:         code.prevCode,
			ContractMetaPre: metaPreBytes,
		}
		if err := rawdb.WriteAccountDelta(buf, blockNum, addr, delta); err != nil {
			return err
		}
		if err := rawdb.WriteAddrInverse(buf, addr, blockNum); err != nil {
			return err
		}
	}

	// Slot deltas + slot inverse index.
	for addr, slots := range firstSlot {
		for slot, preVal := range slots {
			if err := rawdb.WriteSlotDelta(buf, blockNum, addr, slot, preVal); err != nil {
				return err
			}
			if err := rawdb.WriteSlotInverse(buf, addr, slot, blockNum); err != nil {
				return err
			}
		}
	}

	// Per-block manifest. NumAddrs counts every touched account regardless
	// of which mutation surface fired; NumSlots is the total across addrs.
	meta := &historypb.StateHistoryMeta{
		BlockNum:  blockNum,
		BlockHash: blockHash.Bytes(),
		NumAddrs:  uint32(len(addrUnion)),
		NumSlots:  totalSlots,
		SchemaVer: rawdb.HistorySchemaVersion,
	}
	return rawdb.WriteHistoryMeta(buf, blockNum, meta)
}
