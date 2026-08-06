package core

import (
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestVersionedAccessShadowPrepareReusesAndClearsBlockState(t *testing.T) {
	var shadow versionedAccessShadow
	shadow.Prepare(8)

	key := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "stale"}
	shadow.versions[key] = 7
	shadow.rawAccountVersions[testProcessorAddr(1)] = 6
	shadow.accountFullVersions[testProcessorAddr(2)] = 5
	shadow.accountAnyVersions[testProcessorAddr(3)] = 4
	shadow.accountFieldVersions[state.TransactionAccountFieldKey{
		Address: testProcessorAddr(4),
		Field:   state.TransactionAccountFieldBalance,
	}] = 3
	shadow.transactionOwners[0] = testProcessorAddr(1)
	shadow.transactionHasOwner[0] = true
	shadow.senderChainDepths[0] = 2
	shadow.lastSenderTx[testProcessorAddr(1)] = 0
	shadow.dependencyWaves[0] = 2
	shadow.dependencyWaveWidths = append(shadow.dependencyWaveWidths, 1)
	shadow.dependencyHeads[0] = 0
	shadow.dependencyEdges = append(shadow.dependencyEdges, transactionDependencyEdge{predecessor: 0, dependent: 1})
	shadow.transactionSupported[0] = true
	shadow.transactionDurations[0] = 99
	shadow.EnableWriteSetCaptureFiltered(8, func(state.TransactionAccessKey) bool { return true }, []bool{true}, true)
	shadow.transactionWriteSets[0] = state.TransactionWriteSet{key: {}}
	shadow.transactionWritesOK[0] = true
	shadow.EnableSharedVersionValues(8)
	shadow.transactionStarted = time.Now()
	shadow.lastBarrierTx = 3
	shadow.dependencyMinWave = 4
	shadow.dependencyMaxWave = 5
	shadow.stats.transactions = 8

	owners := &shadow.transactionOwners[0]
	shadow.Prepare(4)

	if got := &shadow.transactionOwners[0]; got != owners {
		t.Fatal("transaction owner backing array was not reused")
	}
	if len(shadow.versions) != 0 || len(shadow.rawAccountVersions) != 0 || len(shadow.accountFullVersions) != 0 ||
		len(shadow.accountAnyVersions) != 0 || len(shadow.accountFieldVersions) != 0 || len(shadow.lastSenderTx) != 0 {
		t.Fatal("version maps retained entries from the previous block")
	}
	for i := range shadow.transactionOwners {
		if shadow.transactionOwners[i] != (tcommon.Address{}) || shadow.transactionHasOwner[i] || shadow.senderChainDepths[i] != 0 ||
			shadow.dependencyWaves[i] != 0 || shadow.dependencyHeads[i] != -1 || shadow.transactionSupported[i] || shadow.transactionDurations[i] != 0 {
			t.Fatalf("transaction metadata %d retained previous block state", i)
		}
	}
	if len(shadow.dependencyWaveWidths) != 0 || len(shadow.dependencyEdges) != 0 || len(shadow.transactionWriteSets) != 0 ||
		len(shadow.transactionWritesOK) != 0 || shadow.writeCaptureInclude != nil || shadow.writeCaptureFull != nil ||
		shadow.writeCaptureRecorderOnly || shadow.sharedValues != nil || !shadow.transactionStarted.IsZero() {
		t.Fatal("optional block state was not released")
	}
	if shadow.lastBarrierTx != -1 || shadow.dependencyMinWave != 0 || shadow.dependencyMaxWave != -1 || shadow.stats != (versionedAccessShadowStats{}) {
		t.Fatal("scalar block state was not reset")
	}

	if allocs := testing.AllocsPerRun(100, func() { shadow.Prepare(4) }); allocs != 0 {
		t.Fatalf("warm Prepare allocated %.1f objects per block", allocs)
	}
}

func BenchmarkVersionedAccessShadowPrepareReuse(b *testing.B) {
	var shadow versionedAccessShadow
	shadow.Prepare(256)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		shadow.Prepare(256)
	}
}

func TestVersionedAccessShadowValidatesReadVersionsAcrossStateFamilies(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	for _, suffix := range []byte{1, 2, 3, 4} {
		statedb.CreateAccount(testProcessorAddr(suffix), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(suffix), 1_000)
	}
	contract := testProcessorAddr(3)

	var shadow versionedAccessShadow
	shadow.Prepare(12)
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

	// Direct Context.DB accesses participate in the same exact-key version map.
	rawMark := statedb.DomainChangeJournalMark()
	shadow.BeginTransaction(10, statedb, dynProps)
	shadow.recorder.RecordRawKVPut([]byte("raw-governance"), []byte("v1"))
	shadow.ObserveTransaction(10, tx, statedb, dynProps, rawMark)
	rawMark = statedb.DomainChangeJournalMark()
	shadow.BeginTransaction(11, statedb, dynProps)
	shadow.recorder.RecordRawKVRead([]byte("raw-governance"))
	shadow.ObserveTransaction(11, tx, statedb, dynProps, rawMark)

	got := shadow.Finish(statedb, dynProps)
	if got.transactions != 12 || got.firstPassValid != 6 || got.conflicts != 5 || got.rawKVConflicts != 1 || got.rawKVReadCells != 1 || got.rawKVWriteCells != 1 || got.unsupported != 1 {
		t.Fatalf("versioned shadow summary = %+v", got)
	}
	if got.accountConflicts != 1 || got.storageConflicts != 1 || got.accountKVConflicts != 1 || got.dynamicConflicts != 1 || got.otherConflicts != 0 {
		t.Fatalf("versioned shadow conflict families = %+v", got)
	}
	if got.maxDependencyDistance != 2 {
		t.Fatalf("max dependency distance = %d, want 2", got.maxDependencyDistance)
	}
	if got.otherTransactions != 12 || got.otherFirstPassValid != 6 {
		t.Fatalf("versioned shadow class stats = %+v", got)
	}
	if head := shadow.dependencyHeads[11]; head < 0 || shadow.dependencyEdges[head].predecessor != 10 {
		t.Fatalf("raw KV dependency head = %d, edges=%+v", head, shadow.dependencyEdges)
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

func TestVersionedAccessShadowModelsErigonSenderDependency(t *testing.T) {
	statedb := newTestState(t)
	dynProps := statedb.DynamicProperties()
	owner := testProcessorAddr(0x91)
	other := testProcessorAddr(0x92)
	for _, addr := range []tcommon.Address{owner, other} {
		statedb.CreateAccount(addr, corepb.AccountType_Normal)
		statedb.AddBalance(addr, 100)
	}

	var shadow versionedAccessShadow
	shadow.Prepare(3)
	ownerTx := makeTestTransferTx(0x91, 0x93, 1)
	otherTx := makeTestTransferTx(0x92, 0x94, 1)
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 0, ownerTx, func() {
		statedb.AddBalance(owner, 1)
	})
	// Erigon inserts a prevSenderTx edge, so this read executes only after tx 0
	// has published and is valid without an optimistic retry.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 1, ownerTx, func() {
		_ = statedb.GetBalance(owner)
	})
	// A different sender receives no such edge; its stale read remains a real
	// optimistic conflict.
	recordVersionedShadowTx(t, &shadow, statedb, dynProps, 2, otherTx, func() {
		_ = statedb.GetBalance(owner)
	})

	got := shadow.Finish(statedb, dynProps)
	if got.transactions != 3 || got.typedFirstPassValid != 1 || got.senderFirstPassValid != 2 {
		t.Fatalf("sender dependency validity = %+v", got)
	}
	if got.typedConflicts != 2 || got.senderConflicts != 1 || got.senderDependencyResolvedFirstPass != 1 {
		t.Fatalf("sender dependency conflicts = %+v", got)
	}
	if got.senderDependencyTaggedTransactions != 1 || got.maxSenderChainDepth != 2 {
		t.Fatalf("sender dependency shape = %+v", got)
	}
	if got.senderAccountConflicts != 1 || got.senderBalanceConflicts != 1 || got.senderStorageConflicts != 0 || got.senderDynamicConflicts != 0 {
		t.Fatalf("sender dependency conflict families = %+v", got)
	}
	if got.transferTypedFirstPass != 1 || got.transferSenderFirstPass != 2 {
		t.Fatalf("sender dependency class = %+v", got)
	}
	if got.dependencyDAGWaves != 2 || got.dependencyDAGMaxWidth != 2 || got.dependencyDAGParallelTransactions != 2 {
		t.Fatalf("dependency DAG shape = %+v", got)
	}
}

func TestEstimateDependencyDAGTiming(t *testing.T) {
	// Wave 0 has five tasks, so the unlimited-worker cost is its largest task
	// while the four-worker schedule must place a second task on one lane.
	// Wave 1 cannot begin until the wave-0 barrier completes.
	timing := estimateDependencyDAGTiming(
		[]int{0, 0, 0, 0, 0, 1},
		[]int64{10, 20, 30, 40, 50, 7},
		2,
	)
	if timing.serialNanos != 157 {
		t.Fatalf("serial duration = %d, want 157", timing.serialNanos)
	}
	if timing.waveNanos != 57 {
		t.Fatalf("unlimited-worker wave duration = %d, want 57", timing.waveNanos)
	}
	if timing.fourWorkerNanos != 67 {
		t.Fatalf("four-worker wave duration = %d, want 67", timing.fourWorkerNanos)
	}
}

func TestEstimateDependencyReadyQueueTiming(t *testing.T) {
	var shadow versionedAccessShadow
	shadow.Prepare(6)
	shadow.addDependency(5, 4)
	timing := estimateDependencyReadyQueueTiming(
		[]int64{10, 20, 30, 40, 50, 7},
		shadow.dependencyHeads,
		shadow.dependencyEdges,
	)
	if timing.criticalPathNanos != 57 {
		t.Fatalf("critical path duration = %d, want 57", timing.criticalPathNanos)
	}
	if timing.fourWorkerNanos != 67 {
		t.Fatalf("ready-queue four-worker duration = %d, want 67", timing.fourWorkerNanos)
	}
}

func recordVersionedShadowTx(t *testing.T, shadow *versionedAccessShadow, statedb *state.StateDB, dynProps *state.DynamicProperties, txIndex int, tx *types.Transaction, execute func()) {
	t.Helper()
	mark := statedb.DomainChangeJournalMark()
	journalEndBefore := mark
	shadow.BeginTransaction(txIndex, statedb, dynProps)
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
						shadow.BeginTransaction(i, statedb, dynProps)
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
						shadow.BeginTransaction(i, statedb, dynProps)
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
