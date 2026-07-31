package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
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

func TestVersionedAccessShadowModelsOrderedSettlementDeltas(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	blackhole := testProcessorAddr(0x71)
	statedb.CreateAccount(blackhole, corepb.AccountType_Normal)
	statedb.AddBalance(blackhole, 100)

	var shadow versionedAccessShadow
	shadow.Prepare(4)
	tx := types.NewTransactionFromPB(&corepb.Transaction{})

	for i := 0; i < 2; i++ {
		recordVersionedShadowTx(t, &shadow, statedb, dynProps, i, tx, func() {
			statedb.AddSettlementBalance(blackhole, 1)
			dynProps.AddBurnTrx(1)
		})
	}
	// An ordinary read of either accumulator preserves the dependency even if
	// this transaction also contributes a settlement delta to the same cell.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 2, tx, func() {
		_ = statedb.GetBalance(blackhole)
		statedb.AddSettlementBalance(blackhole, 1)
		_ = dynProps.BurnTrxAmount()
		dynProps.AddBurnTrx(1)
	})
	// A normal balance addition is not implicitly commutative merely because it
	// happens to target the blackhole address.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 3, tx, func() {
		statedb.AddBalance(blackhole, 1)
	})

	got := shadow.Finish(statedb, dynProps)
	if got.transactions != 4 || got.firstPassValid != 1 || got.normalizedFirstPassValid != 2 {
		t.Fatalf("raw/normalized validity = %+v", got)
	}
	if got.conflicts != 3 || got.normalizedConflicts != 2 || got.settlementResolvedFirstPass != 1 {
		t.Fatalf("raw/normalized conflicts = %+v", got)
	}
	if got.settlementTaggedTransactions != 3 {
		t.Fatalf("settlement tagged transactions = %d, want 3", got.settlementTaggedTransactions)
	}
	if got.settlementBlackholeConflicts != 2 || got.settlementBurnConflicts != 2 {
		t.Fatalf("settlement conflict families = %+v", got)
	}
	if got.otherFirstPassValid != 1 || got.otherNormalizedFirstPass != 2 {
		t.Fatalf("normalized class stats = %+v", got)
	}
	if got := statedb.GetBalance(blackhole); got != 104 {
		t.Fatalf("canonical blackhole balance = %d, want 104", got)
	}
	if got := dynProps.BurnTrxAmount(); got != 3 {
		t.Fatalf("canonical burn amount = %d, want 3", got)
	}
}

func TestVersionedAccessShadowTypedAccountHierarchy(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	// Production sync enables temporal history, which represents scalar undo as
	// a full accountChange. Inline field coverage must keep that physical journal
	// shape from becoming a false typed full-account write.
	statedb.SetDomainChangeSetWriter(ethrawdb.NewMemoryDatabase(), 1, tcommon.Hash{0x80})
	for _, suffix := range []byte{0x81, 0x82, 0x83, 0x84} {
		statedb.CreateAccount(testProcessorAddr(suffix), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(suffix), 100)
	}

	var shadow versionedAccessShadow
	shadow.Prepare(9)
	tx := types.NewTransactionFromPB(&corepb.Transaction{})
	a := testProcessorAddr(0x81)
	b := testProcessorAddr(0x82)
	c := testProcessorAddr(0x83)
	d := testProcessorAddr(0x84)

	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 0, tx, func() {
		statedb.SetNetUsage(a, 1)
	})
	// A balance read is stale under the old whole-account model, but independent
	// from the preceding bandwidth-field write under the typed model.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 1, tx, func() {
		_ = statedb.GetBalance(a)
	})
	// A full-account view consumes the aggregate account version and must still
	// conflict with any preceding typed field write.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 2, tx, func() {
		_ = statedb.AccountReference(a)
	})

	// Full-account mutations invalidate every typed field.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 3, tx, func() {
		statedb.SetAccountName(b, "typed-shadow")
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 4, tx, func() {
		_ = statedb.GetBalance(b)
	})

	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 5, tx, func() {
		statedb.AddBalance(c, 1)
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 6, tx, func() {
		_ = statedb.GetNetUsage(c)
	})

	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 7, tx, func() {
		statedb.AddBalance(d, 1)
	})
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 8, tx, func() {
		_, _, _ = statedb.GetAccountKV(d, kvdomains.AccountPermissionAux, []byte("owner"))
	})

	got := shadow.Finish(statedb, dynProps)
	if got.transactions != 9 || got.firstPassValid != 4 || got.normalizedFirstPassValid != 4 || got.typedFirstPassValid != 7 {
		t.Fatalf("typed hierarchy validity = %+v", got)
	}
	if got.conflicts != 5 || got.normalizedConflicts != 5 || got.typedConflicts != 2 || got.typedResolvedFirstPass != 3 {
		t.Fatalf("typed hierarchy conflicts = %+v", got)
	}
	if got.typedAccountConflicts != 2 || got.typedAccountCoarseConflicts != 1 || got.typedAccountBalanceConflicts != 1 {
		t.Fatalf("typed account conflict paths = %+v", got)
	}
	if got.otherFirstPassValid != 4 || got.otherNormalizedFirstPass != 4 || got.otherTypedFirstPass != 7 {
		t.Fatalf("typed class stats = %+v", got)
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

func BenchmarkVersionedAccessShadowSettlementNormalization(b *testing.B) {
	const transactions = 64
	tx := types.NewTransactionFromPB(&corepb.Transaction{})
	for _, observe := range []bool{false, true} {
		name := "serial_only"
		if observe {
			name = "with_normalized_shadow"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				statedb := newTestState(b)
				dynProps := statedb.DynamicProperties()
				blackhole := testProcessorAddr(0x72)
				statedb.CreateAccount(blackhole, corepb.AccountType_Normal)
				var shadow versionedAccessShadow
				if observe {
					shadow.Prepare(transactions)
				}
				b.StartTimer()
				for i := 0; i < transactions; i++ {
					mark := statedb.DomainChangeJournalMark()
					if observe {
						shadow.BeginTransaction(statedb, dynProps)
					}
					statedb.AddSettlementBalance(blackhole, 1)
					dynProps.AddBurnTrx(1)
					if observe {
						shadow.ObserveTransaction(i, tx, statedb, dynProps, mark)
					}
				}
				if observe {
					stats := shadow.Finish(statedb, dynProps)
					if stats.firstPassValid != 1 || stats.normalizedFirstPassValid != transactions {
						b.Fatalf("settlement stats = %+v", stats)
					}
				}
			}
		})
	}
}
