package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestV4RewardVotesPhysicalBoundaries(t *testing.T) {
	owner := common.SystemAccountAddress
	logical := CycleAccountVoteStateKey(9, common.Address{0x41, 1}.Bytes())
	key := stateKVLatestKey(owner, 0, kvdomains.SystemReward, logical)
	legacy, err := statecodec.Marshal(&corepb.Account{Balance: 1, Votes: []*corepb.Vote{{VoteCount: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := statecodec.MarshalRewardVotes(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		name  string
		value []byte
		valid bool
	}{
		{"legacy-account", legacy, false}, {"empty-bytes", nil, false}, {"present-empty-votes", valid, true},
	} {
		t.Run(input.name, func(t *testing.T) {
			wrapped := EncodeStateKVLatestValue(input.value)
			writers := []func(*memoryWriter) error{
				func(w *memoryWriter) error {
					return WriteStateKVLatestEncoded(w, owner, 0, kvdomains.SystemReward, logical, wrapped)
				},
				func(w *memoryWriter) error {
					return WriteStateKVLatestEncodedOwned(w, owner, 0, kvdomains.SystemReward, logical, wrapped)
				},
				func(w *memoryWriter) error { return WriteStateKVLatestEncodedOwnedByKey(w, key, wrapped) },
			}
			for i, write := range writers {
				w := &memoryWriter{}
				err := write(w)
				if (err == nil) != input.valid || w.puts != boolInt(input.valid) {
					t.Fatalf("writer %d: writes=%d err=%v", i, w.puts, err)
				}
			}
			if _, err := EncodeStateCommitmentElement(key, wrapped); (err == nil) != input.valid {
				t.Fatalf("element validation: %v", err)
			}
			dst := []byte("untouched")
			if _, err := AppendStateCommitmentElement(dst, key, wrapped); (err == nil) != input.valid {
				t.Fatalf("append validation: %v", err)
			}
			if !bytes.Equal(dst, []byte("untouched")) {
				t.Fatal("mutated destination")
			}
		})
	}
	// A similarly named reward counter is not a Votes snapshot.
	if err := ValidateStateKVPayload(kvdomains.SystemReward, append(bytes.Clone(logical), 'x'), []byte("counter")); err != nil {
		t.Fatal(err)
	}
}

type memoryWriter struct{ puts int }

func (w *memoryWriter) Put(_, _ []byte) error { w.puts++; return nil }
func (w *memoryWriter) Delete(_ []byte) error { return nil }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
