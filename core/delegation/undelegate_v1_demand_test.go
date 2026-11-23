package delegation

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type bandwidthPrefixCounter struct {
	ethdb.KeyValueStore
	scans int
}

func (d *bandwidthPrefixCounter) NewIterator(prefix, start []byte) ethdb.Iterator {
	if _, _, domain, key, ok := rawdb.DecodeStateKVLatestKey(prefix); ok && domain == kvdomains.AccountFrozenBandwidthAux && len(key) == 0 {
		d.scans++
	}
	return d.KeyValueStore.NewIterator(prefix, start)
}

func undelegateV1Fixture(tb testing.TB, usage, acquired int64, corrupt bool) (*bandwidthPrefixCounter, *state.Database, tcommon.Hash, tcommon.Address, *state.DynamicProperties) {
	tb.Helper()
	const bandwidth = corepb.ResourceCode_BANDWIDTH
	disk := &bandwidthPrefixCounter{KeyValueStore: rawdb.NewMemoryDatabase()}
	db := state.NewDatabase(rawdb.WrapKeyValueStore(disk))
	tb.Cleanup(func() { db.Close() })
	if err := rawdb.InitializeStateCommitment(disk); err != nil {
		tb.Fatal(err)
	}
	s, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		tb.Fatal(err)
	}
	addr := demandAddress(0)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	s.FreezeV1Bandwidth(addr, 11_000_000, 10000)
	zeroKey := []byte{0, 0, 0, 0}
	value, exists, err := s.GetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, zeroKey)
	if err != nil || !exists {
		tb.Fatalf("fixture V1 row: exists=%t err=%v", exists, err)
	}
	if err := s.DeleteAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, zeroKey); err != nil {
		tb.Fatal(err)
	}
	if corrupt {
		value = []byte{0xff}
	}
	if err := s.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, []byte{0, 0, 0, 17}, value); err != nil {
		tb.Fatal(err)
	}
	s.AddFreezeV2(addr, bandwidth, 2_000_000)
	s.AddAcquiredDelegatedFrozenV2(addr, bandwidth, acquired)
	s.SetNetUsage(addr, usage)
	s.SetNetWindow(addr, 28_800_000, true)
	s.SetLatestConsumeTime(addr, 100)
	root, err := s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetTotalNetWeight(100_000)
	dp.SetAllowCancelAllUnfreezeV2(true)
	return disk, db, root, addr, dp
}

func TestUndelegateBandwidthV1Demand(t *testing.T) {
	for _, test := range []struct {
		name                 string
		usage, acquired, now int64
		wantScans            int
		wantTransfer         bool
	}{
		{"zero_usage", 0, 3_000_000, 200, 0, false},
		{"expired_usage", 4800, 3_000_000, 30_000, 0, false},
		{"insufficient_acquired", 4800, 500_000, 200, 0, false},
		{"complete_sparse_v1", 4800, 3_000_000, 200, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const bandwidth = corepb.ResourceCode_BANDWIDTH
			const undelegate = 1_000_000
			run := func(legacy bool) (string, tcommon.Hash, *corepb.Account) {
				disk, db, root, addr, dp := undelegateV1Fixture(t, test.usage, test.acquired, false)
				s, err := state.New(root, db)
				if err != nil {
					t.Fatal(err)
				}
				before := disk.scans
				read := TransferUsageFromReceiver
				if legacy {
					read = legacyDemandTransferUsageFromReceiver
				}
				transfer, window, optimized := read(s, dp, addr, bandwidth, undelegate, test.now, false)
				if !legacy && disk.scans-before != test.wantScans {
					t.Fatalf("V1 scans=%d want %d", disk.scans-before, test.wantScans)
				}
				if (transfer > 0) != test.wantTransfer || s.GetLatestConsumeTime(addr) != test.now {
					t.Fatalf("unexpected transfer=%d, consume time=%d", transfer, s.GetLatestConsumeTime(addr))
				}
				s.SubAcquiredDelegatedFrozenV2(addr, bandwidth, undelegate)
				copied, err := s.Copy()
				if err != nil {
					t.Fatal(err)
				}
				for _, view := range []*state.StateDB{s, copied} {
					if got, _ := view.GetFreezeV1BalanceAndExpire(addr, int64(bandwidth)); got != 11_000_000 {
						t.Fatalf("following fast getter lost sparse row: %d", got)
					}
				}
				snapshot := s.Snapshot()
				s.DeleteAccount(addr)
				s.CreateAccount(addr, corepb.AccountType_Normal)
				if got, _ := s.GetFreezeV1BalanceAndExpire(addr, int64(bandwidth)); got != 0 {
					t.Fatalf("recreated account inherited sparse row: %d", got)
				}
				s.RevertToSnapshot(snapshot)
				if restored := s.GetAccount(addr); restored == nil || restored.TotalFrozenBandwidth() != 11_000_000 {
					t.Fatalf("rollback lost sparse row: %v", s.Error())
				}
				post, err := s.Commit()
				if err != nil {
					t.Fatal(err)
				}
				reopened, err := state.New(post, db)
				if err != nil {
					t.Fatal(err)
				}
				acct := reopened.GetAccount(addr)
				if acct == nil || acct.TotalFrozenBandwidth() != 11_000_000 {
					t.Fatalf("missing persisted sparse V1 row: %v", reopened.Error())
				}
				return fmt.Sprintf("%d/%d/%t", transfer, window, optimized), post, acct.Proto()
			}
			result, post, account := run(false)
			oldResult, oldPost, oldAccount := run(true)
			if result != oldResult || post != oldPost || !proto.Equal(account, oldAccount) {
				t.Fatal("transfer or persisted state differs from legacy")
			}
		})
	}
}

type emptyDelegationVersionReader struct{}

func (emptyDelegationVersionReader) ReadTransactionVersionedValue(state.TransactionAccessKey, int) (state.TransactionWriteValue, int, bool) {
	return state.TransactionWriteValue{}, 0, false
}

func TestUndelegateBandwidthV1DemandCorruption(t *testing.T) {
	for _, test := range []struct {
		name                                   string
		usage                                  int64
		capture, versioned, tvmForm, wantError bool
		wantScans                              int
	}{
		{"irrelevant_serial_v1", 0, false, false, false, false, 0},
		{"required_serial_v1", 4800, false, false, false, true, 1},
		{"captured_v1", 0, true, false, false, true, 1},
		{"versioned_v1", 0, false, true, false, true, 1},
		{"tvm_v1", 0, false, false, true, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			disk, db, root, addr, dp := undelegateV1Fixture(t, test.usage, 3_000_000, true)
			s, err := state.New(root, db)
			if err != nil {
				t.Fatal(err)
			}
			var recorder state.TransactionAccessRecorder
			if test.capture {
				recorder.Reset(1)
				s.SetTransactionAccessRecorder(&recorder)
			}
			if test.versioned {
				s.SetTransactionVersionedValueReader(emptyDelegationVersionReader{}, 1)
			}
			before := disk.scans
			TransferUsageFromReceiver(s, dp, addr, corepb.ResourceCode_BANDWIDTH, 1_000_000, 200, test.tvmForm)
			if disk.scans-before != test.wantScans || (s.Error() != nil) != test.wantError {
				t.Fatalf("V1 scans=%d err=%v", disk.scans-before, s.Error())
			}
			if test.capture && !recorder.Unsupported() {
				t.Fatal("capture lost the full-prefix dependency")
			}
			wantTime := int64(200)
			if test.wantError {
				wantTime = 100
			}
			if got := s.GetLatestConsumeTime(addr); got != wantTime {
				t.Fatalf("consume time=%d want %d", got, wantTime)
			}
		})
	}
}

func BenchmarkUndelegateBandwidthV1Demand(b *testing.B) {
	for _, shape := range []struct {
		name  string
		usage int64
	}{
		{"zero_usage", 0},
		{"positive_usage", 4800},
	} {
		b.Run(shape.name, func(b *testing.B) {
			disk, db, root, addr, dp := undelegateV1Fixture(b, shape.usage, 3_000_000, false)
			b.ReportAllocs()
			before := disk.scans
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := state.New(root, db)
				if err != nil {
					b.Fatal(err)
				}
				TransferUsageFromReceiver(s, dp, addr, corepb.ResourceCode_BANDWIDTH, 1_000_000, 200, false)
				if err := s.Error(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(disk.scans-before)/float64(b.N), "V1-scans/op")
		})
	}
}
