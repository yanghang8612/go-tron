package state

import (
	"bytes"
	"fmt"
	"maps"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	delegationMembershipBenchmarkSet   map[string]struct{}
	delegationMembershipBenchmarkEntry *legacyDelegationEntry
)

func delegationMembershipBenchmarkRecord(degree int, mixed bool) *corepb.DelegatedResourceAccountIndex {
	record := &corepb.DelegatedResourceAccountIndex{Account: legacyBenchmarkAddress(1), Timestamp: -12345}
	record.ToAccounts = make([][]byte, degree)
	for i := range record.ToAccounts {
		address := legacyBenchmarkAddress(uint64(i + 2))
		if mixed {
			switch i % 5 {
			case 0:
				address = nil
			case 1:
				address = append(address, bytes.Repeat([]byte{0, 0xff}, i%32)...)
			case 2:
				address = address[:i%21]
			}
		}
		record.ToAccounts[i] = address
	}
	// Both builders also see an independently indexed second direction.
	for i := 0; i < legacyDelegationSetMinimum; i++ {
		record.FromAccounts = append(record.FromAccounts, legacyBenchmarkAddress(uint64(degree+i+2)))
	}
	return record
}

// BenchmarkLegacyDelegationMembershipArena freezes both builders in one test
// binary with identical inputs. Membership isolates map/key construction;
// NewEntry includes complete-row accounting and both indexes but starts from
// an already-owned record; DecodeEntry also owns and decodes every native byte.
// Neither includes StateDB reads, mutation, commitment, history or physical I/O.
// Only the latest output remains retained; B/op is not resident memory or RSS.
func BenchmarkLegacyDelegationMembershipArena(b *testing.B) {
	for _, fixture := range []struct {
		degree int
		mixed  bool
	}{{32, false}, {10_000, false}, {100_000, false}, {100_000, true}} {
		b.Run(fmt.Sprintf("degree=%d/mixed=%t", fixture.degree, fixture.mixed), func(b *testing.B) {
			record := delegationMembershipBenchmarkRecord(fixture.degree, fixture.mixed)
			raw, err := statecodec.Marshal(record) // Independent native-byte oracle.
			if err != nil {
				b.Fatal(err)
			}
			decoded, err := statecodec.UnmarshalDelegationIndex(raw)
			if err != nil {
				b.Fatal(err)
			}
			key := []byte("fixed-legacy-delegation-key")
			old, current := frozenLegacyDelegationEntry(key, decoded), newLegacyDelegationEntry(key, decoded)
			if old.charge != current.charge || !maps.Equal(old.fromSet, current.fromSet) || !maps.Equal(old.toSet, current.toSet) {
				b.Fatal("fixed-input complete entry differs from frozen control")
			}
			encoded, err := statecodec.MarshalDelegationIndex(current.record)
			if err != nil || !bytes.Equal(encoded, raw) {
				b.Fatal("fixed native bytes differ from generic codec")
			}
			for _, workload := range []string{"Membership", "NewEntry", "DecodeEntry"} {
				for _, mode := range []string{"Legacy", "Arena"} {
					b.Run(workload+"/"+mode, func(b *testing.B) {
						builder, entryBuilder := frozenLegacyDelegationMembership, frozenLegacyDelegationEntry
						if mode == "Arena" {
							builder, entryBuilder = legacyDelegationMembership, newLegacyDelegationEntry
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							switch workload {
							case "Membership":
								delegationMembershipBenchmarkSet = builder(decoded.ToAccounts)
							case "NewEntry":
								delegationMembershipBenchmarkEntry = entryBuilder(key, decoded)
							case "DecodeEntry":
								owned, err := statecodec.UnmarshalDelegationIndex(raw)
								if err != nil {
									b.Fatal(err)
								}
								delegationMembershipBenchmarkEntry = entryBuilder(key, owned)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(len(raw)), "native-B")
						b.ReportMetric(float64(current.charge), "entry-charge-B")
						delegationMembershipBenchmarkSet, delegationMembershipBenchmarkEntry = nil, nil
					})
				}
			}
		})
	}
}
