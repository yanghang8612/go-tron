package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These helpers deliberately freeze the pre-optimization generic-codec path.
// Do not replace them with the optimized reader, writer, or list helpers: they
// are the independent native-byte/history/root oracle and performance control.
func legacyReferenceRead(s *StateDB, account []byte) (*corepb.DelegatedResourceAccountIndex, error) {
	data, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyStateKey(account))
	if err != nil || !ok {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("decode dr account index legacy %x: empty value", account)
	}
	rec := new(corepb.DelegatedResourceAccountIndex)
	if err := statecodec.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("decode dr account index legacy %x: %w", account, err)
	}
	return rec, nil
}

func legacyReferenceWrite(s *StateDB, account []byte, rec *corepb.DelegatedResourceAccountIndex) error {
	data, err := statecodec.Marshal(rec)
	if err != nil {
		return err
	}
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyStateKey(account), data)
}

func legacyReferenceMutation(s *StateDB, from, to []byte, remove bool) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address")
	}
	fromRec, err := legacyReferenceRead(s, from)
	if err != nil {
		return err
	}
	toRec, err := legacyReferenceRead(s, to)
	if err != nil {
		return err
	}
	modify := func(list [][]byte, address []byte) [][]byte {
		if remove {
			out := list[:0]
			for _, item := range list {
				if !bytes.Equal(item, address) {
					out = append(out, item)
				}
			}
			return out
		}
		for _, item := range list {
			if bytes.Equal(item, address) {
				return list
			}
		}
		return append(list, bytes.Clone(address))
	}
	if fromRec == nil && !remove {
		fromRec = &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(from)}
	}
	if toRec == nil && !remove {
		toRec = &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(to)}
	}
	if fromRec != nil {
		fromRec.ToAccounts = modify(fromRec.ToAccounts, to)
		if err := legacyReferenceWrite(s, from, fromRec); err != nil {
			return err
		}
	}
	if toRec != nil {
		toRec.FromAccounts = modify(toRec.FromAccounts, from)
		return legacyReferenceWrite(s, to, toRec)
	}
	return nil
}

func legacyBenchmarkAddress(n uint64) []byte {
	addr := make([]byte, tcommon.AddressLength)
	addr[0] = 0x41
	binary.BigEndian.PutUint64(addr[len(addr)-8:], n)
	return addr
}

type legacyBenchmarkFixture struct {
	db                    *Database
	root                  tcommon.Hash
	from, existing, fresh []byte
}

func seedLegacyBenchmark(tb testing.TB, degree int) legacyBenchmarkFixture {
	tb.Helper()
	disk := ethrawdb.NewMemoryDatabase()
	tb.Cleanup(func() { _ = disk.Close() })
	db := NewDatabase(disk)
	s, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		tb.Fatal(err)
	}
	from := legacyBenchmarkAddress(1)
	peers := make([][]byte, degree)
	for i := range peers {
		peers[i] = legacyBenchmarkAddress(uint64(i + 2))
	}
	// Asymmetric star: owner has D outgoing entries, selected receiver has
	// one incoming entry. Seed native rows directly, outside timed work.
	existing := peers[len(peers)/2]
	if err := legacyReferenceWrite(s, from, &corepb.DelegatedResourceAccountIndex{Account: from, ToAccounts: peers}); err != nil {
		tb.Fatal(err)
	}
	if err := legacyReferenceWrite(s, existing, &corepb.DelegatedResourceAccountIndex{Account: existing, FromAccounts: [][]byte{from}}); err != nil {
		tb.Fatal(err)
	}
	root, err := s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	return legacyBenchmarkFixture{db: db, root: root, from: from, existing: existing, fresh: legacyBenchmarkAddress(uint64(degree + 100))}
}

func (f legacyBenchmarkFixture) open(tb testing.TB) *StateDB {
	tb.Helper()
	s, err := New(f.root, f.db)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// BenchmarkLegacyDelegationStateDB measures the rooted StateDB path, not the
// unrelated rawdb protobuf codec. GenericReference remains an old-path control
// when the canonical API changes. Each mutation iteration reverts its journal,
// keeping cardinality and retained undo bounded independently of b.N.
func BenchmarkLegacyDelegationStateDB(b *testing.B) {
	for _, degree := range []int{32, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("degree=%d", degree), func(b *testing.B) {
			f := seedLegacyBenchmark(b, degree)
			for _, implementation := range []string{"Canonical", "GenericReference"} {
				b.Run(implementation, func(b *testing.B) {
					mutate := func(s *StateDB, to []byte, remove bool) error {
						if implementation == "GenericReference" {
							return legacyReferenceMutation(s, f.from, to, remove)
						}
						if remove {
							return s.WriteDrAccountIndexLegacyUnDelegate(f.from, to)
						}
						return s.WriteDrAccountIndexLegacyDelegate(f.from, to)
					}
					for _, workload := range []string{"ResidentDuplicate", "ColdStateDuplicate", "AppendRevert", "RemoveReaddRevert"} {
						b.Run(workload, func(b *testing.B) {
							s := f.open(b)
							if err := mutate(s, f.existing, false); err != nil {
								b.Fatal(err)
							}
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								switch workload {
								case "ResidentDuplicate":
									if err := mutate(s, f.existing, false); err != nil {
										b.Fatal(err)
									}
								case "ColdStateDuplicate":
									// Cold execution object, warm in-memory database. This
									// includes New and load; it does not model physical I/O.
									s = f.open(b)
									if err := mutate(s, f.existing, false); err != nil {
										b.Fatal(err)
									}
								case "AppendRevert":
									snapshot := s.Snapshot()
									if err := mutate(s, f.fresh, false); err != nil {
										b.Fatal(err)
									}
									s.RevertToSnapshot(snapshot)
								case "RemoveReaddRevert":
									snapshot := s.Snapshot()
									if err := mutate(s, f.existing, true); err != nil {
										b.Fatal(err)
									}
									if err := mutate(s, f.existing, false); err != nil {
										b.Fatal(err)
									}
									s.RevertToSnapshot(snapshot)
								}
							}
						})
					}
				})
			}
		})
	}
}

// BenchmarkLegacyDelegationHistoryFlush includes native loading, a real
// transaction journal mark, removal, transaction finalization and production
// history row serialization/publication. The fixed txNum intentionally replaces
// the same rows in the in-memory sink so history storage does not grow with b.N.
// Commitment and disk I/O are excluded and must be measured in replay separately.
func BenchmarkLegacyDelegationHistoryFlush(b *testing.B) {
	for _, degree := range []int{32, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("degree=%d", degree), func(b *testing.B) {
			f := seedLegacyBenchmark(b, degree)
			for _, reference := range []bool{false, true} {
				name := "Canonical"
				if reference {
					name = "GenericReference"
				}
				b.Run(name, func(b *testing.B) {
					sink := ethrawdb.NewMemoryDatabase()
					defer func() {
						if err := sink.Close(); err != nil {
							b.Errorf("close history sink: %v", err)
						}
					}()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s := f.open(b)
						s.BeginDomainChangeJournalCapture(sink, 2, tcommon.Hash{2}, 2, 3)
						mark := s.DomainChangeJournalMark()
						var err error
						if reference {
							err = legacyReferenceMutation(s, f.from, f.existing, true)
						} else {
							err = s.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing)
						}
						if err != nil {
							b.Fatal(err)
						}
						s.FinalizeTransaction()
						if err := s.FlushDomainChangesSince(mark, 2); err != nil {
							b.Fatal(err)
						}
						if err := s.FlushPendingDomainChanges(3); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func TestLegacyDelegationReferenceBytesHistoryRootAndRollback(t *testing.T) {
	from, to, spare := legacyBenchmarkAddress(501), legacyBenchmarkAddress(502), legacyBenchmarkAddress(503)
	for _, scenario := range []string{"missing-both", "from-only", "to-only", "asymmetric-lists", "duplicate-entries", "self"} {
		t.Run(scenario, func(t *testing.T) {
			anchor, peer := from, to
			if scenario == "self" {
				peer = anchor
			}
			type lane struct {
				s  *StateDB
				db *Database
			}
			lanes := make([]lane, 2)
			for i := range lanes {
				disk := ethrawdb.NewMemoryDatabase()
				t.Cleanup(func() { _ = disk.Close() })
				db := NewDatabase(disk)
				s, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
				if err != nil {
					t.Fatal(err)
				}
				seed := func(key []byte, rec *corepb.DelegatedResourceAccountIndex) {
					t.Helper()
					if err := legacyReferenceWrite(s, key, rec); err != nil {
						t.Fatal(err)
					}
				}
				switch scenario {
				case "from-only":
					seed(anchor, &corepb.DelegatedResourceAccountIndex{Account: anchor, ToAccounts: [][]byte{peer}, Timestamp: 17})
				case "to-only":
					seed(peer, &corepb.DelegatedResourceAccountIndex{Account: peer, FromAccounts: [][]byte{anchor}, Timestamp: -17})
				case "asymmetric-lists":
					seed(anchor, &corepb.DelegatedResourceAccountIndex{Account: []byte("preserve-account"), FromAccounts: [][]byte{spare}, ToAccounts: [][]byte{peer, spare}, Timestamp: 99})
					seed(peer, &corepb.DelegatedResourceAccountIndex{Account: peer, FromAccounts: [][]byte{spare}, ToAccounts: [][]byte{spare}})
				case "duplicate-entries":
					seed(anchor, &corepb.DelegatedResourceAccountIndex{Account: anchor, ToAccounts: [][]byte{peer, spare, peer, nil}})
					seed(peer, &corepb.DelegatedResourceAccountIndex{Account: peer, FromAccounts: [][]byte{anchor, spare, anchor}})
				case "self":
					seed(anchor, &corepb.DelegatedResourceAccountIndex{Account: anchor, FromAccounts: [][]byte{spare}, ToAccounts: [][]byte{spare}})
				}
				root, err := s.Commit()
				if err != nil {
					t.Fatal(err)
				}
				s, err = New(root, db)
				if err != nil {
					t.Fatal(err)
				}
				lanes[i] = lane{s: s, db: db}
			}
			type row struct {
				value  []byte
				exists bool
			}
			readRows := func(s *StateDB) []row {
				t.Helper()
				rows := make([]row, 3)
				for i, address := range [][]byte{anchor, peer, spare} {
					value, exists, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyStateKey(address))
					if err != nil {
						t.Fatal(err)
					}
					rows[i] = row{value: bytes.Clone(value), exists: exists}
				}
				return rows
			}
			mutate := func(laneID int, peer []byte, remove bool) {
				t.Helper()
				var err error
				if laneID == 0 {
					err = legacyReferenceMutation(lanes[laneID].s, anchor, peer, remove)
				} else if remove {
					err = lanes[laneID].s.WriteDrAccountIndexLegacyUnDelegate(anchor, peer)
				} else {
					err = lanes[laneID].s.WriteDrAccountIndexLegacyDelegate(anchor, peer)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			steps := []string{"delegate", "duplicate", "nested-rollback", "remove", "readd", "remove-absent", "append-spare"}
			begin, end, err := rawdb.NextStateTxRange(0, uint64(len(steps)))
			if err != nil {
				t.Fatal(err)
			}
			for _, lane := range lanes {
				lane.s.BeginDomainChangeJournalCapture(lane.db.DiskDB(), 2, tcommon.Hash{2}, begin, end)
			}
			expected := make([][]row, len(steps))
			for ordinal, step := range steps {
				for laneID, lane := range lanes {
					mark := lane.s.DomainChangeJournalMark()
					switch step {
					case "delegate", "duplicate", "readd":
						mutate(laneID, peer, false)
					case "remove":
						mutate(laneID, peer, true)
					case "remove-absent":
						mutate(laneID, legacyBenchmarkAddress(9999), true)
					case "append-spare":
						mutate(laneID, spare, false)
					case "nested-rollback":
						before := readRows(lane.s)
						outer := lane.s.Snapshot()
						mutate(laneID, peer, true)
						inner := lane.s.Snapshot()
						mutate(laneID, spare, false)
						lane.s.RevertToSnapshot(inner)
						lane.s.RevertToSnapshot(outer)
						if got := readRows(lane.s); !reflect.DeepEqual(got, before) {
							t.Fatalf("lane %d rollback bytes changed", laneID)
						}
					}
					lane.s.FinalizeTransaction()
					if err := lane.s.FlushDomainChangesSince(mark, begin+uint64(ordinal)); err != nil {
						t.Fatal(err)
					}
				}
				expected[ordinal] = readRows(lanes[0].s)
				if got := readRows(lanes[1].s); !reflect.DeepEqual(got, expected[ordinal]) {
					t.Fatalf("%s native bytes differ: canonical=%+v reference=%+v", step, got, expected[ordinal])
				}
			}
			var referenceRoot tcommon.Hash
			for laneID, lane := range lanes {
				if err := lane.s.FlushPendingDomainChanges(end); err != nil {
					t.Fatal(err)
				}
				root, err := lane.s.Commit()
				if err != nil {
					t.Fatal(err)
				}
				if laneID == 0 {
					referenceRoot = root
				} else if root != referenceRoot {
					t.Fatalf("root %s, want %s", root.Hex(), referenceRoot.Hex())
				}
				for ordinal, rows := range expected {
					for i, address := range [][]byte{anchor, peer, spare} {
						value, exists, err := rawdb.ReadStateAccountKVAsOfTxNum(lane.db.DiskDB(), tcommon.SystemAccountAddress, kvdomains.SystemDelegation, rawdb.DrAccountIndexLegacyStateKey(address), begin+uint64(ordinal), end)
						if err != nil {
							t.Fatal(err)
						}
						if exists != rows[i].exists || !bytes.Equal(value, rows[i].value) {
							t.Fatalf("lane %d as-of %s row %d differs: exists=%v expected=%v", laneID, steps[ordinal], i, exists, rows[i].exists)
						}
					}
				}
			}
			wantHistory := collectStateDomainChanges(t, lanes[0].db.DiskDB(), 2)
			gotHistory := collectStateDomainChanges(t, lanes[1].db.DiskDB(), 2)
			if !reflect.DeepEqual(gotHistory, wantHistory) {
				t.Fatalf("history differs:\ncanonical=%+v\nreference=%+v", gotHistory, wantHistory)
			}
		})
	}
}
