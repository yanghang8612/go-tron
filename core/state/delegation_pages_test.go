package state

import (
	"bytes"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// writeDrAccountIndexLegacy is a test fixture/reference full replacement.
// The production edge edit path never repacks a complete list.
func (s *StateDB) writeDrAccountIndexLegacy(account []byte, record *corepb.DelegatedResourceAccountIndex) error {
	old, exists, err := s.loadLegacyDelegation(account)
	if err != nil {
		return err
	}
	s.detachLegacyDelegation(old)
	entry := &legacyDelegationEntry{key: string(rawdb.DrAccountIndexLegacyStateKey(account)), record: record, pages: splitLegacyDelegationPages(record)}
	for _, dir := range []struct {
		incoming        bool
		pages, oldPages [][][]byte
	}{{true, entry.pages.from, old.pages.from}, {false, entry.pages.to, old.pages.to}} {
		for i := range dir.pages {
			data, err := statecodec.MarshalDelegationPage(dir.pages[i])
			if err != nil {
				return err
			}
			if err := s.writeSystemDelegation(rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, uint64(i)), data); err != nil {
				return err
			}
		}
		if exists {
			for i := len(dir.pages); i < len(dir.oldPages); i++ {
				if err := s.deleteSystemDelegation(rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, uint64(i))); err != nil {
					return err
				}
			}
		}
	}
	return s.writeLegacyDelegationDescriptor(entry)
}

func TestDelegationPagesOrderDuplicatesRevertAndConvert(t *testing.T) {
	s := newTestStateDB(t)
	account, peer := legacyBenchmarkAddress(1), legacyBenchmarkAddress(2)
	list := make([][]byte, 3*statecodec.DelegationPageEntries+3)
	for i := range list {
		list[i] = legacyBenchmarkAddress(uint64(i + 10))
	}
	list[1], list[130], list[300], list[200] = peer, peer, peer, nil
	record := &corepb.DelegatedResourceAccountIndex{Account: []byte("preserved account field"), FromAccounts: [][]byte{[]byte("incoming")}, ToAccounts: list, Timestamp: -123}
	record.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
	delegationCachePut(t, s, account, record)
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	before := delegationCacheRead(t, s, account)
	snapshot := s.Snapshot()
	if err := s.WriteDrAccountIndexLegacyUnDelegate(account, peer); err != nil {
		t.Fatal(err)
	}
	want := proto.Clone(before).(*corepb.DelegatedResourceAccountIndex)
	want.ToAccounts = removeDelegationAccount(want.ToAccounts, peer)
	delegationCacheAssertRow(t, s, account, want)
	if err := s.WriteDrAccountIndexLegacyDelegate(account, peer); err != nil {
		t.Fatal(err)
	}
	want.ToAccounts = append(want.ToAccounts, peer)
	delegationCacheAssertRow(t, s, account, want)
	entry, _, err := s.loadLegacyDelegation(account)
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.pages.to) != 4 || len(entry.pages.to[0]) != 127 || len(entry.pages.to[1]) != 127 || len(entry.pages.to[2]) != 127 || len(entry.pages.to[3]) != 4 {
		t.Fatal("removal moved entries between pages")
	}
	s.RevertToSnapshot(snapshot)
	delegationCacheAssertRow(t, s, account, before)
	// Conversion still uses the Java logical list positions, including duplicates
	// and the empty peer; error on the latter must remain explicit and revertible.
	snapshot = s.Snapshot()
	if err := s.ConvertDrAccountIndexLegacy(account); err == nil {
		t.Fatal("conversion accepted empty peer")
	}
	s.RevertToSnapshot(snapshot)
	// Replace with valid lists, convert, and ensure all physical pages disappear.
	valid := proto.Clone(before).(*corepb.DelegatedResourceAccountIndex)
	valid.ToAccounts[200] = legacyBenchmarkAddress(9999)
	delegationCachePut(t, s, account, valid)
	if err := s.ConvertDrAccountIndexLegacy(account); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := s.ReadDrAccountIndexLegacyStrict(account); err != nil || exists {
		t.Fatalf("converted descriptor remains: %v %v", exists, err)
	}
	for _, dir := range []struct {
		incoming bool
		count    int
	}{{true, 1}, {false, 4}} {
		for i := 0; i < dir.count; i++ {
			if _, exists, err := s.readSystemDelegationWithError(rawdb.DrAccountIndexLegacyPageStateKey(account, dir.incoming, uint64(i))); err != nil || exists {
				t.Fatalf("converted page remains: %v %v", exists, err)
			}
		}
	}
	directional, exists, err := s.ReadDrAccountIndexEntryStrict(rawdb.DrAccIdxV1From, account, peer)
	if err != nil || !exists || directional.Timestamp != 301 {
		t.Fatalf("duplicate conversion position: %v %v %v", directional, exists, err)
	}
}

func delegationChangeBytes(changes []*rawdb.StateDomainChange) (prev, next, rows int) {
	for _, change := range changes {
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Domain == kvdomains.SystemDelegation {
			prev += len(change.Prev)
			next += len(change.Next)
			rows++
		}
	}
	return
}

type delegationMeasuringPublisher struct {
	delegate StateDomainChangePublisher
	changes  []*rawdb.StateDomainChange
}

func (p *delegationMeasuringPublisher) PublishStateDomainChanges(changes []*rawdb.StateDomainChange) error {
	p.changes = append(p.changes, changes...)
	return p.delegate.PublishStateDomainChanges(changes)
}

func TestDelegationPagesHistoryBytesBoundedAndAsOf(t *testing.T) {
	var firstPrev, firstNext, firstRows int
	for _, degree := range []int{128, 1024, 100000} {
		t.Run(fmt.Sprint(degree), func(t *testing.T) {
			f := seedLegacyBenchmark(t, degree)
			s := f.open(t)
			original := delegationCacheRead(t, s, f.from)
			begin, end, err := rawdb.NextStateTxRange(0, 2)
			if err != nil {
				t.Fatal(err)
			}
			s.BeginDomainChangeJournalCapture(f.db.DiskDB(), 2, tcommon.Hash{2}, begin, end)
			expected := make([]*corepb.DelegatedResourceAccountIndex, 0, 2)
			publisher := &delegationMeasuringPublisher{delegate: defaultStateDomainChangeRunner(f.db.DiskDB())}
			for step := 0; step < 2; step++ {
				mark := s.DomainChangeJournalMark()
				if step == 0 {
					err = s.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing)
				} else {
					err = s.WriteDrAccountIndexLegacyDelegate(f.from, f.existing)
				}
				if err != nil {
					t.Fatal(err)
				}
				expected = append(expected, delegationCacheRead(t, s, f.from))
				s.FinalizeTransaction()
				if err := s.publishDomainChangesSince(publisher, mark, begin+uint64(step)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.FlushPendingDomainChanges(end); err != nil {
				t.Fatal(err)
			}
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := New(root, f.db)
			if err != nil {
				t.Fatal(err)
			}
			delegationCacheAssertRow(t, reopened, f.from, expected[1])
			for step, want := range expected {
				got, exists, err := ReadPagedDelegationIndex(f.from, func(key []byte) ([]byte, bool, error) {
					return rawdb.ReadStateAccountKVAsOfTxNum(f.db.DiskDB(), tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, begin+uint64(step), end)
				})
				if err != nil || !exists || !proto.Equal(got, want) {
					t.Fatalf("as-of tx %d: exists=%v err=%v got=%v", step, exists, err, got)
				}
			}
			before, exists, err := ReadPagedDelegationIndex(f.from, func(key []byte) ([]byte, bool, error) {
				return rawdb.ReadStateAccountKVAsOfTxNum(f.db.DiskDB(), tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, begin-1, end)
			})
			if err != nil || !exists || !proto.Equal(before, original) {
				t.Fatalf("as-of before edits: %v %v", exists, err)
			}
			changes := publisher.changes
			persisted := collectStateDomainChanges(t, f.db.DiskDB(), 2)
			capturedPrev, _, capturedRows := delegationChangeBytes(changes)
			persistedPrev, _, persistedRows := delegationChangeBytes(persisted)
			if capturedPrev != persistedPrev || capturedRows != persistedRows {
				t.Fatal("persistent history differs from measured before-images")
			}
			// Inspect only the removal transaction: appending can cross a page boundary
			// and legitimately changes its tiny descriptor as well.
			var removal []*rawdb.StateDomainChange
			for _, change := range changes {
				if change.TxNum == begin {
					removal = append(removal, change)
				}
			}
			for step := 0; step < 2; step++ {
				var transaction []*rawdb.StateDomainChange
				for _, change := range changes {
					if change.TxNum == begin+uint64(step) {
						transaction = append(transaction, change)
					}
				}
				prev, next, rows := delegationChangeBytes(transaction)
				if prev > 4096 || next > 4096 || rows > 4 {
					t.Fatalf("transaction %d exceeds bounded pages: prev=%d next=%d rows=%d", step, prev, next, rows)
				}
			}
			prev, next, rows := delegationChangeBytes(removal)
			if prev > 4096 || next > 4096 || rows > 3 {
				t.Fatalf("unbounded mutation: prev=%d next=%d rows=%d", prev, next, rows)
			}
			if degree == 128 {
				firstPrev, firstNext, firstRows = prev, next, rows
			} else if prev != firstPrev || next != firstNext || rows != firstRows {
				t.Fatalf("history bytes scale with list: prev=%d next=%d rows=%d, first %d/%d/%d", prev, next, rows, firstPrev, firstNext, firstRows)
			}
			t.Logf("degree=%d removal Prev=%d Next=%d rows=%d", degree, prev, next, rows)
		})
	}
}

func TestDelegationPagesGenericPageInvalidationAndDependencies(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	key := rawdb.DrAccountIndexLegacyPageStateKey(from, false, 0)
	recorder := new(TransactionAccessRecorder)
	s.SetTransactionAccessRecorder(recorder)
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, read := range recorder.CaptureReadSet().Reads {
		if read.Key.Kind == TransactionAccessAccountKV && read.Key.LogicalKey == string(key) && read.Mode&TransactionAccessRead != 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("cache hit omitted page dependency")
	}
	page, err := statecodec.MarshalDelegationPage([][]byte{[]byte("replacement")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.writeSystemDelegation(key, page); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	record := delegationCacheRead(t, s, from)
	if len(record.ToAccounts) != 2 || !bytes.Equal(record.ToAccounts[0], []byte("replacement")) || !bytes.Equal(record.ToAccounts[1], to) {
		t.Fatal("generic page mutation reused stale cache")
	}
}

func TestDelegationPagesMissingOrCorruptPageFailsClosed(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			s, from, to := delegationCacheFixture(t)
			key := rawdb.DrAccountIndexLegacyPageStateKey(from, false, 0)
			var err error
			if missing {
				err = s.deleteSystemDelegation(key)
			} else {
				err = s.writeSystemDelegation(key, []byte("DPP1\x01\x80"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, exists, err := s.ReadDrAccountIndexLegacyStrict(from); err == nil || !exists {
				t.Fatalf("corrupt page accepted: %v %v", exists, err)
			}
			before := s.DomainChangeJournalMark()
			if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err == nil {
				t.Fatal("mutation accepted corrupt page")
			}
			if s.DomainChangeJournalMark() != before {
				t.Fatal("corrupt read wrote state")
			}
		})
	}
}

func BenchmarkDelegationPagesMutationHistoryBytes(b *testing.B) {
	for _, degree := range []int{128, 1024, 100000} {
		b.Run(fmt.Sprint(degree), func(b *testing.B) {
			f := seedLegacyBenchmark(b, degree)
			s := f.open(b)
			if _, _, err := s.loadLegacyDelegation(f.from); err != nil {
				b.Fatal(err)
			}
			if _, _, err := s.loadLegacyDelegation(f.existing); err != nil {
				b.Fatal(err)
			}
			publisher := new(capturingDomainChangePublisher)
			s.BeginDomainChangeJournalCapture(f.db.DiskDB(), 2, tcommon.Hash{2}, 2, 3)
			var prev, next, rows int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := s.Snapshot()
				mark := s.DomainChangeJournalMark()
				if err := s.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing); err != nil {
					b.Fatal(err)
				}
				publisher.changes = nil
				if err := s.publishDomainChangesSince(publisher, mark, 2); err != nil {
					b.Fatal(err)
				}
				prev, next, rows = delegationChangeBytes(publisher.changes)
				s.RevertToSnapshot(snapshot)
			}
			b.ReportMetric(float64(prev), "Prev-B/op")
			b.ReportMetric(float64(next), "Next-B/op")
			b.ReportMetric(float64(rows), "KV-rows/op")
		})
	}
}
