package state

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"syscall"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// APIs in this file exist in upstream too; the SAME fixture is compiled into
// both production arms. The entire lifecycle, including seed writes/commit and
// close, is timed. This is deterministic synthetic state, not mainnet replay.
func frozenDemandLifecycle(tb testing.TB, accounts, queries int) string {
	tb.Helper()
	disk := rawdb.NewMemoryDatabase()
	db := NewDatabase(rawdb.WrapKeyValueStore(disk))
	defer db.Close()
	if err := rawdb.InitializeStateCommitment(disk); err != nil {
		tb.Fatal(err)
	}
	s, err := New(common.Hash{}, db)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < accounts; i++ {
		addr := pruneTestOwner(i + 1)
		s.CreateAccount(addr, corepb.AccountType_Normal)
		s.FreezeV1Bandwidth(addr, int64(i+11), 100)
		s.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 1000000)
	}
	root, err := s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	s, err = New(root, db)
	if err != nil {
		tb.Fatal(err)
	}
	h := sha256.New()
	var buf [8]byte
	read := func(view *StateDB, addr common.Address) {
		for q := 0; q < queries; q++ {
			binary.BigEndian.PutUint64(buf[:], uint64(view.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH)))
			h.Write(buf[:])
			binary.BigEndian.PutUint64(buf[:], uint64(view.FrozenV1BandwidthCount(addr)))
			h.Write(buf[:])
			if view.HasExpiredFrozenV1Bandwidth(addr, 200) {
				h.Write([]byte{1})
			}
		}
	}
	for i := 0; i < accounts; i++ {
		addr := pruneTestOwner(i + 1)
		read(s, addr)
		if i%8 == 0 {
			mark := s.Snapshot()
			s.FreezeV1Bandwidth(addr, 99, 200)
			read(s, addr)
			s.RevertToSnapshot(mark)
			read(s, addr)
		}
		s.AddBalance(addr, 1)
	}
	// Include private-copy construction, cached reads and mutations in timing.
	cp, err := s.Copy()
	if err != nil {
		tb.Fatal(err)
	}
	read(cp, pruneTestOwner(1))
	mark := cp.Snapshot()
	cp.FreezeV1Bandwidth(pruneTestOwner(1), 123, 200)
	read(cp, pruneTestOwner(1))
	cp.RevertToSnapshot(mark)
	read(cp, pruneTestOwner(1))
	root, err = s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	h.Write(root[:])
	s, err = New(root, db)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < accounts; i++ {
		read(s, pruneTestOwner(i+1))
	}
	// Namespace reset followed by explicit replacement exercises invalidation;
	// generic setters, not protobuf aliases, remain the mutation authority.
	addr := pruneTestOwner(1)
	if err := s.ResetAccountKV(addr); err != nil {
		tb.Fatal(err)
	}
	value, err := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 77})
	if err != nil {
		tb.Fatal(err)
	}
	if err := s.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0), value); err != nil {
		tb.Fatal(err)
	}
	read(s, addr)
	s.DeleteAccount(addr)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	s.FreezeV1Bandwidth(addr, 88, 100)
	read(s, addr)
	root, err = s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	h.Write(root[:])
	if err := s.Error(); err != nil {
		tb.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
func TestFrozenDemandLifecycleTrace(t *testing.T) {
	for _, shape := range []struct {
		name string
		n, q int
	}{{"high_churn", 64, 1}, {"mixed", 32, 8}, {"minimal", 8, 1}, {"write_only", 64, 0}} {
		t.Run(shape.name, func(t *testing.T) { t.Logf("TRACE %s %s", shape.name, frozenDemandLifecycle(t, shape.n, shape.q)) })
	}
}
func BenchmarkFrozenDemandLifecycle(b *testing.B) {
	for _, shape := range []struct {
		name string
		n, q int
	}{{"high_churn", 64, 1}, {"mixed", 32, 8}, {"minimal", 8, 1}, {"write_only", 64, 0}} {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			cpuStart := frozenDemandCPU()
			for i := 0; i < b.N; i++ {
				frozenDemandLifecycle(b, shape.n, shape.q)
			}
			b.ReportMetric(float64(frozenDemandCPU()-cpuStart)/float64(b.N), "cpu-ns/op")
		})
	}
}

func frozenDemandCPU() int64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return (r.Utime.Sec+r.Stime.Sec)*1_000_000_000 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}
