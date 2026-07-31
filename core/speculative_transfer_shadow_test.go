package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSpeculativeTransferShadowBuildsConflictFreeWaves(t *testing.T) {
	statedb := newTestState(t)
	for _, suffix := range []byte{1, 2, 3, 4, 5} {
		statedb.CreateAccount(testProcessorAddr(suffix), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(suffix), 1_000)
	}

	var shadow speculativeTransferShadow
	recordShadowTransferWrites(t, &shadow, statedb, 1, 2, false)
	recordShadowTransferWrites(t, &shadow, statedb, 3, 4, false)
	// Reusing owner 1 conflicts with the first wave and starts a second wave.
	recordShadowTransferWrites(t, &shadow, statedb, 1, 5, false)
	// An unsupported transaction is a publication barrier.
	shadow.Observe(types.NewTransactionFromPB(&corepb.Transaction{}), statedb, statedb.DomainChangeJournalMark(), false)

	got := shadow.Finish()
	want := speculativeTransferShadowStats{
		transactions:       4,
		transferCandidates: 3,
		eligible:           3,
		dependencies:       1,
		barriers:           1,
		waves:              2,
		parallelTxs:        2,
		maxWaveWidth:       2,
	}
	if got != want {
		t.Fatalf("shadow stats = %+v, want %+v", got, want)
	}
}

func TestSpeculativeTransferShadowRejectsSystemAndCreationWrites(t *testing.T) {
	statedb := newTestState(t)
	for _, suffix := range []byte{1, 2, 3, 4, 9} {
		statedb.CreateAccount(testProcessorAddr(suffix), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(suffix), 1_000)
	}

	var shadow speculativeTransferShadow
	// A real dynamic-property mutation makes every later DP read versioned.
	recordShadowTransferWrites(t, &shadow, statedb, 1, 2, true)

	// An extra blackhole/system-account write is outside the participant set.
	mark := statedb.DomainChangeJournalMark()
	statedb.Snapshot()
	if err := statedb.SubBalance(testProcessorAddr(1), 1); err != nil {
		t.Fatal(err)
	}
	statedb.AddBalance(testProcessorAddr(3), 1)
	statedb.AddBalance(testProcessorAddr(9), 1)
	shadow.Observe(makeTestTransferTx(1, 3, 1), statedb, mark, false)

	// A nil-preimage account journal entry identifies recipient creation.
	mark = statedb.DomainChangeJournalMark()
	statedb.Snapshot()
	statedb.CreateAccountWithTime(testProcessorAddr(8), corepb.AccountType_Normal, 1)
	if err := statedb.SubBalance(testProcessorAddr(4), 1); err != nil {
		t.Fatal(err)
	}
	statedb.AddBalance(testProcessorAddr(8), 1)
	shadow.Observe(makeTestTransferTx(4, 8, 1), statedb, mark, false)

	got := shadow.Finish()
	if got.transactions != 3 || got.transferCandidates != 3 || got.eligible != 0 || got.unsafe != 3 || got.barriers != 3 || got.waves != 0 {
		t.Fatalf("unsafe shadow stats = %+v", got)
	}
}

func recordShadowTransferWrites(t *testing.T, shadow *speculativeTransferShadow, statedb *state.StateDB, owner, recipient byte, dynamicPropertiesChanged bool) {
	t.Helper()
	mark := statedb.DomainChangeJournalMark()
	statedb.Snapshot()
	if err := statedb.SubBalance(testProcessorAddr(owner), 1); err != nil {
		t.Fatal(err)
	}
	statedb.AddBalance(testProcessorAddr(recipient), 1)
	journalEnd := statedb.DomainChangeJournalMark()
	ownerBalance := statedb.GetBalance(testProcessorAddr(owner))
	recipientBalance := statedb.GetBalance(testProcessorAddr(recipient))
	shadow.Observe(makeTestTransferTx(owner, recipient, 1), statedb, mark, dynamicPropertiesChanged)
	if got := statedb.DomainChangeJournalMark(); got != journalEnd {
		t.Fatalf("shadow observer changed journal mark: got %d, want %d", got, journalEnd)
	}
	if got := statedb.GetBalance(testProcessorAddr(owner)); got != ownerBalance {
		t.Fatalf("shadow observer changed owner balance: got %d, want %d", got, ownerBalance)
	}
	if got := statedb.GetBalance(testProcessorAddr(recipient)); got != recipientBalance {
		t.Fatalf("shadow observer changed recipient balance: got %d, want %d", got, recipientBalance)
	}
}

func BenchmarkSpeculativeTransferShadowOverhead(b *testing.B) {
	const transfers = 64
	txs := make([]*types.Transaction, transfers)
	for i := range txs {
		txs[i] = makeTestTransferTx(byte(i+1), byte(i+1+transfers), 1)
		_, _ = txs[i].DecodedContract()
	}
	for _, observe := range []bool{false, true} {
		name := "serial_only"
		if observe {
			name = "with_shadow"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				statedb := newTestState(b)
				for suffix := 1; suffix <= 2*transfers; suffix++ {
					address := testProcessorAddr(byte(suffix))
					statedb.CreateAccount(address, corepb.AccountType_Normal)
					statedb.AddBalance(address, 1_000)
				}
				var shadow speculativeTransferShadow
				shadow.Prepare(len(txs))
				b.StartTimer()
				for i, tx := range txs {
					mark := statedb.DomainChangeJournalMark()
					statedb.Snapshot()
					if err := statedb.SubBalance(testProcessorAddr(byte(i+1)), 1); err != nil {
						b.Fatal(err)
					}
					statedb.AddBalance(testProcessorAddr(byte(i+1+transfers)), 1)
					if observe {
						shadow.Observe(tx, statedb, mark, false)
					}
				}
				if observe {
					stats := shadow.Finish()
					if stats.parallelTxs != transfers || stats.maxWaveWidth != transfers {
						b.Fatalf("shadow stats = %+v", stats)
					}
				}
			}
		})
	}
}
