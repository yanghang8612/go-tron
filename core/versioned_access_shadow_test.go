package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestVersionedAccessShadowValidatesReadVersionsAcrossStateFamilies(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	for _, suffix := range []byte{1, 2, 3, 4} {
		statedb.CreateAccount(testProcessorAddr(suffix), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(suffix), 1_000)
	}
	contract := testProcessorAddr(3)

	var shadow versionedAccessShadow
	shadow.Prepare(10)
	tx := types.NewTransactionFromPB(&corepb.Transaction{})

	// tx 0 writes account 1; tx 1 is disjoint and both validate on the first
	// block-start attempt.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 0, tx, func() {
		if err := statedb.SubBalance(testProcessorAddr(1), 1); err != nil {
			t.Fatal(err)
		}
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 1, tx, func() {
		if err := statedb.SubBalance(testProcessorAddr(2), 1); err != nil {
			t.Fatal(err)
		}
	})
	// tx 2 reads tx 0's account version.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 2, tx, func() {
		_ = statedb.GetBalance(testProcessorAddr(1))
	})

	var slot [32]byte
	slot[31] = 9
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 3, tx, func() {
		statedb.SetState(contract, slot, slot)
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 4, tx, func() {
		_ = statedb.GetState(contract, slot)
	})

	kvOwner := testProcessorAddr(4)
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 5, tx, func() {
		if err := statedb.SetAccountKV(kvOwner, kvdomains.AccountPermissionAux, []byte("owner"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 6, tx, func() {
		_, _, _ = statedb.GetAccountKV(kvOwner, kvdomains.AccountPermissionAux, []byte("owner"))
	})

	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 7, tx, func() {
		dynProps.Set("shadow_counter", 1)
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 8, tx, func() {
		_, _ = dynProps.Get("shadow_counter")
	})

	// Prefix/range reads need range versions before they can be published.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 9, tx, func() {
		if err := statedb.IterateAccountKV(kvOwner, kvdomains.AccountPermissionAux, nil, func(_, _ []byte) (bool, error) {
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	got := shadow.Finish(statedb, dynProps)
	if got.transactions != 10 || got.firstPassValid != 5 || got.conflicts != 4 || got.unsupported != 1 {
		t.Fatalf("versioned shadow summary = %+v", got)
	}
	if got.accountConflicts != 1 || got.storageConflicts != 1 || got.accountKVConflicts != 1 || got.dynamicConflicts != 1 || got.otherConflicts != 0 {
		t.Fatalf("versioned shadow conflict families = %+v", got)
	}
	if got.maxDependencyDistance != 2 {
		t.Fatalf("max dependency distance = %d, want 2", got.maxDependencyDistance)
	}
	if got.otherTransactions != 10 || got.otherFirstPassValid != 5 {
		t.Fatalf("versioned shadow class stats = %+v", got)
	}
}

func TestVersionedAccessShadowBlindWriteOverlapDoesNotInvalidate(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	var shadow versionedAccessShadow
	shadow.Prepare(2)
	tx := types.NewTransactionFromPB(&corepb.Transaction{})

	// PutWitness journals a replacement without going through GetWitness, so it
	// is a genuine blind write from the logical access recorder's perspective.
	// Erigon publishes blind writes in original order; write/write overlap is
	// useful diagnostics but is not a read-version failure.
	for i := 0; i < 2; i++ {
		recordVersionedShadowTx(t, &shadow, statedb, dynProps, i, tx, func() {
			statedb.PutWitness(testProcessorAddr(9), "https://witness")
		})
	}
	got := shadow.Finish(statedb, dynProps)
	if got.firstPassValid != 2 || got.conflicts != 0 || got.writeConflicts != 1 {
		t.Fatalf("blind write stats = %+v", got)
	}
}

func recordVersionedShadowTx(t *testing.T, shadow *versionedAccessShadow, statedb *state.StateDB, dynProps *state.DynamicProperties, txIndex int, tx *types.Transaction, execute func()) {
	t.Helper()
	mark := statedb.DomainChangeJournalMark()
	journalEndBefore := mark
	shadow.BeginTransaction(statedb, dynProps)
	execute()
	journalEnd := statedb.DomainChangeJournalMark()
	shadow.ObserveTransaction(txIndex, tx, statedb, dynProps, mark)
	if got := statedb.DomainChangeJournalMark(); got != journalEnd {
		t.Fatalf("shadow observer changed journal: got %d, want %d (before %d)", got, journalEnd, journalEndBefore)
	}
}

func BenchmarkVersionedAccessShadowOverhead(b *testing.B) {
	const transfers = 64
	tx := types.NewTransactionFromPB(&corepb.Transaction{})
	for _, observe := range []bool{false, true} {
		name := "serial_only"
		if observe {
			name = "with_versioned_shadow"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				statedb := newTestState(b)
				dynProps := statedb.DynamicProperties()
				for suffix := 1; suffix <= transfers*2; suffix++ {
					address := testProcessorAddr(byte(suffix))
					statedb.CreateAccount(address, corepb.AccountType_Normal)
					statedb.AddBalance(address, 1_000)
				}
				var shadow versionedAccessShadow
				if observe {
					shadow.Prepare(transfers)
				}
				b.StartTimer()
				for i := 0; i < transfers; i++ {
					mark := statedb.DomainChangeJournalMark()
					if observe {
						shadow.BeginTransaction(statedb, dynProps)
					}
					if err := statedb.SubBalance(testProcessorAddr(byte(i+1)), 1); err != nil {
						b.Fatal(err)
					}
					statedb.AddBalance(testProcessorAddr(byte(i+1+transfers)), 1)
					if observe {
						shadow.ObserveTransaction(i, tx, statedb, dynProps, mark)
					}
				}
				if observe {
					stats := shadow.Finish(statedb, dynProps)
					if stats.firstPassValid != transfers {
						b.Fatalf("versioned stats = %+v", stats)
					}
				}
			}
		})
	}
}
