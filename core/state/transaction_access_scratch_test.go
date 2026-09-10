package state

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// StateDB lends the same four-byte scratch to successive vote-slot reads.
// Repeated reads and mode upgrades must never replace owned map keys with it.
func TestTransactionAccessAccountKVRetainsScratchKeys(t *testing.T) {
	for _, modes := range [][]TransactionAccessMode{
		{TransactionAccessRead, TransactionAccessRead},
		{TransactionAccessRead, TransactionAccessWrite},
		{TransactionAccessWrite, TransactionAccessRead},
	} {
		t.Run(modesName(modes), func(t *testing.T) {
			var recorder TransactionAccessRecorder
			recorder.Reset(64)
			owner := tcommon.Address{0x41, 1}
			var scratch [4]byte
			want := make(map[TransactionAccessKey]TransactionAccessMode)
			for slot := uint32(0); slot < 30; slot++ {
				binary.BigEndian.PutUint32(scratch[:], slot)
				key := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: owner,
					KVDomain: kvdomains.AccountVotesAux, LogicalKey: string(scratch[:])}
				for _, mode := range modes {
					recorder.recordAccountKV(owner, kvdomains.AccountVotesAux, scratch[:], mode)
					want[key] |= mode
				}
			}
			binary.BigEndian.PutUint32(scratch[:], 999)
			if len(recorder.accesses) != len(want) {
				t.Fatalf("recorded %d keys, want %d", len(recorder.accesses), len(want))
			}
			for key, mode := range want {
				if got := recorder.accesses[key]; got != mode {
					t.Fatalf("slot %x mode = %v, want %v", key.LogicalKey, got, mode)
				}
			}
			captured := recorder.CaptureReadSet()
			writes := make(map[TransactionAccessKey]bool)
			recorder.VisitWrites(func(key TransactionAccessKey, _ TransactionAccessMode) bool {
				writes[key] = true
				return true
			})
			if len(captured.Reads) != 30 {
				t.Fatalf("captured %d reads, want 30", len(captured.Reads))
			}
			expectWrites := modes[0]&TransactionAccessWrite != 0 || modes[1]&TransactionAccessWrite != 0
			if expectWrites && len(writes) != 30 || !expectWrites && len(writes) != 0 {
				t.Fatalf("captured %d writes, expected writes=%v", len(writes), expectWrites)
			}
			recorder.Reset(64)
			for _, read := range captured.Reads {
				if want[read.Key]&TransactionAccessRead == 0 || read.Mode != TransactionAccessRead {
					t.Fatalf("captured read changed after scratch reuse/reset: %+v", read)
				}
				if expectWrites && !writes[read.Key] {
					t.Fatalf("write key lost for slot %x", read.Key.LogicalKey)
				}
			}
		})
	}
}

func modesName(modes []TransactionAccessMode) string {
	if modes[0] == TransactionAccessWrite {
		return "write_then_read"
	}
	if modes[1] == TransactionAccessWrite {
		return "read_then_write"
	}
	return "repeated_read"
}
