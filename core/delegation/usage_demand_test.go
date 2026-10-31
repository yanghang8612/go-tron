// Legacy oracle: exact upstream 52aea62ef567 core/delegation/usage.go,
// with function identifiers mechanically prefixed. Do not simplify its reads.
// Package delegation holds V2 delegation usage-transfer helpers shared
// between the actuator path (DelegateResource / UnDelegateResource) and
// the VM path (opcodes 0xDE / 0xDF). No actuator/vm imports — only state
// + types + params.
package delegation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"syscall"
	"testing"
)

// legacyDemandResState reads the per-account usage, last-consume-time and the stored
// recovery window (raw + optimized) for a resource.
func legacyDemandResState(statedb *state.StateDB, acct *types.Account, addr tcommon.Address, resource corepb.ResourceCode) (usage, lastTime, rawWindow int64, optimized bool) {
	if resource == corepb.ResourceCode_BANDWIDTH {
		return statedb.GetNetUsage(addr), statedb.GetLatestConsumeTime(addr), acct.RawNetWindowSize(), acct.NetWindowOptimized()
	}
	return statedb.GetEnergyUsage(addr), statedb.GetLatestConsumeTimeForEnergy(addr), acct.RawEnergyWindowSize(), acct.EnergyWindowOptimized()
}

// legacyDemandWriteResState persists usage, the recovery window and the consume time.
func legacyDemandWriteResState(statedb *state.StateDB, addr tcommon.Address, resource corepb.ResourceCode, usage, rawWindow int64, optimized bool, now int64) {
	if resource == corepb.ResourceCode_BANDWIDTH {
		statedb.SetNetUsage(addr, usage)
		statedb.SetNetWindow(addr, rawWindow, optimized)
		statedb.SetLatestConsumeTime(addr, now)
	} else {
		statedb.SetEnergyUsage(addr, usage)
		statedb.SetEnergyWindow(addr, rawWindow, optimized)
		statedb.SetLatestConsumeTimeForEnergy(addr, now)
	}
}

// legacyDemandTransferUsageFromReceiver mirrors java-tron UnDelegateResourceActuator.execute's
// receiver side: BandwidthProcessor.updateUsageForDelegated(receiver) recovers the
// receiver's usage against its PER-ACCOUNT window (renormalizing + writing the
// window), then the actuator peels off the portion proportional to the undelegated
// balance (capped at unDelegateBalance/TRX × totalLimit/totalWeight) and writes the
// remainder back. Returns the transferable amount AND the receiver's post-recovery
// window so the owner-side combine (legacyDemandFoldUsageIntoOwner) can blend it in — exactly
// java's unDelegateIncrease reading receiver.getWindowSize().
// tvmForm selects the unDelegateMaxUsage float evaluation order: java's
// UnDelegateResourceProcessor (the 0xDF opcode, tvmForm=true) computes it
// left-to-right `(ub/TRX) * limit / weight`, while UnDelegateResourceActuator
// (tvmForm=false) groups the ratio `(ub/TRX) * (limit/weight)`. The two IEEE-754
// orders differ by 1 at rare ULP boundaries, and when that cap binds it flips the
// committed transferUsage — so the shared helper must use the form matching its
// caller's java path.
func legacyDemandTransferUsageFromReceiver(statedb *state.StateDB, dp *state.DynamicProperties, receiver tcommon.Address, resource corepb.ResourceCode, unDelegateBalance, now int64, tvmForm bool) (transfer, recvRawWindow int64, recvOptimized bool) {
	acct := statedb.GetAccount(receiver)
	if acct == nil {
		return 0, 0, false
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, rawWindow, optimized := legacyDemandResState(statedb, acct, receiver, resource)
	var totalLimit, totalWeight, totalFrozen, acquiredV2 int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
		totalFrozen = legacyDemandTotalBandwidthFrozen(acct)
		acquiredV2 = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	} else {
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
		totalFrozen = legacyDemandTotalEnergyFrozen(acct)
		acquiredV2 = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}

	// Per-account window recovery (usage arg = 0 → pure recovery + window renorm).
	recovered, newRaw, newOpt := computeResourceIncrease(rawWindow, optimized, usage, 0, lastTime, now, harden, cancelAllV2)

	// java UnDelegateResourceActuator/Processor: when the receiver's acquired V2
	// delegated balance is below the undelegated amount (a TVM contract suicide then
	// re-create clears acquired while a stale delegation record survives), it sets
	// acquired=0 and SKIPS the proportional usage transfer — transferUsage stays 0
	// and the receiver keeps its full recovered usage. The caller's
	// SubAcquiredDelegatedFrozenV2 already clamps the balance to 0 (== setAcquired(0)).
	if acquiredV2 >= unDelegateBalance && totalFrozen > 0 && recovered > 0 {
		var maxTransfer int64
		if tvmForm {
			// java UnDelegateResourceProcessor: (ub/TRX) * limit / weight (left-to-right).
			maxTransfer = tcommon.JavaDoubleToInt64(float64(unDelegateBalance) / float64(params.TRXPrecision) * float64(totalLimit) / float64(totalWeight))
		} else {
			// java UnDelegateResourceActuator: (ub/TRX) * (limit/weight) (grouped ratio).
			maxTransfer = tcommon.JavaDoubleToInt64(float64(unDelegateBalance) / float64(params.TRXPrecision) * (float64(totalLimit) / float64(totalWeight)))
		}
		transfer = tcommon.JavaDoubleToInt64(float64(recovered) * (float64(unDelegateBalance) / float64(totalFrozen)))
		if transfer > maxTransfer {
			transfer = maxTransfer
		}
	}

	newUsage := recovered - transfer
	legacyDemandWriteResState(statedb, receiver, resource, newUsage, newRaw, newOpt, now)
	return transfer, newRaw, newOpt
}

// legacyDemandFoldUsageIntoOwner mirrors java-tron ResourceProcessor.unDelegateIncrease /
// unDelegateIncreaseV2: recover the owner's usage against its PER-ACCOUNT window,
// add transferUsage, and set the new owner window to the usage-weighted blend of
// the owner's (post-recovery) window and the receiver's window. java gates this on
// `Objects.nonNull(receiverCapsule) && transferUsage > 0`, so the undelegate
// callers must only invoke it when transferUsage > 0 (the selfdestruct-inheritor
// merge calls it with the owner's full recovered usage, also > 0).
func legacyDemandFoldUsageIntoOwner(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, transferUsage, recvRawWindow int64, recvOptimized bool, now int64) {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, ownerRaw, ownerOpt := legacyDemandResState(statedb, acct, owner, resource)
	ownerRecovered, ownerNewRaw, ownerNewOpt := computeResourceIncrease(ownerRaw, ownerOpt, usage, 0, lastTime, now, harden, cancelAllV2)

	newOwnerUsage := ownerRecovered + transferUsage
	var finalRaw int64
	var finalOpt bool
	if newOwnerUsage == 0 {
		finalRaw, finalOpt = zeroOwnerWindow(ownerNewOpt, cancelAllV2)
	} else {
		finalRaw, finalOpt = combineOwnerWindow(ownerRecovered, ownerNewRaw, ownerNewOpt, transferUsage, recvRawWindow, recvOptimized, newOwnerUsage, harden, cancelAllV2)
	}
	legacyDemandWriteResState(statedb, owner, resource, newOwnerUsage, finalRaw, finalOpt, now)
}

// legacyDemandMergeUsageToInheritor mirrors java-tron Program.transferFrozenV2BalanceToInheritor's
// per-resource usage merge for a self-destructing contract: recover the owner's
// usage to `now` (BandwidthProcessor.updateUsageForDelegated / EnergyProcessor
// .updateUsage — pure recovery that also renormalizes + persists the window and
// stamps the owner's consume time = now), then, when that recovered usage is
// positive, fold ALL of it into the inheritor's window
// (unDelegateIncrease(inheritor, owner, owner.usage, resource, now)). java guards
// the fold on `owner.getUsage() > 0` after the recovery, so callers that later
// clearOwnerFreezeV2 still see the owner's consume time advanced to now.
func legacyDemandMergeUsageToInheritor(statedb *state.StateDB, dp *state.DynamicProperties, owner, inheritor tcommon.Address, resource corepb.ResourceCode, now int64) {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, rawWindow, optimized := legacyDemandResState(statedb, acct, owner, resource)
	recovered, newRaw, newOpt := computeResourceIncrease(rawWindow, optimized, usage, 0, lastTime, now, harden, cancelAllV2)
	// java updateUsageForDelegated/updateUsage persists the recovered usage+window
	// and the suicide body sets the owner's consume time to now.
	legacyDemandWriteResState(statedb, owner, resource, recovered, newRaw, newOpt, now)
	if recovered > 0 {
		legacyDemandFoldUsageIntoOwner(statedb, dp, inheritor, resource, recovered, newRaw, newOpt, now)
	}
}

// legacyDemandAvailableFrozenV2ForDelegation returns the owner's self-frozen V2 balance
// that can still be delegated after accounting for already-consumed resource
// usage. This mirrors java-tron's DelegateResourceActuator.validate:
//
//	available = selfFrozenV2 - max(0, usageAsFrozenBalance
//	    - ownV1Frozen - acquiredV1Delegation - acquiredV2Delegation)
//
// The recovery uses the owner's PER-ACCOUNT window (java validate calls
// updateUsageForDelegated/updateUsage on the owner first); validate is
// non-persisting, so the recovered window is discarded.
func legacyDemandAvailableFrozenV2ForDelegation(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, now int64) int64 {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return 0
	}

	selfFrozen := acct.GetFrozenV2Amount(resource)
	if selfFrozen <= 0 {
		return 0
	}

	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()
	usageRaw, lastTime, rawWindow, optimized := legacyDemandResState(statedb, acct, owner, resource)
	usage, _, _ := computeResourceIncrease(rawWindow, optimized, usageRaw, 0, lastTime, now, harden, cancelAllV2)

	var v1OwnFrozen, v1AcquiredFrozen, v2AcquiredFrozen, totalLimit, totalWeight int64
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		v1OwnFrozen = acct.TotalFrozenBandwidth()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenBandwidth()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
	case corepb.ResourceCode_ENERGY:
		v1OwnFrozen = acct.FrozenEnergyAmount()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenEnergy()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
	default:
		return 0
	}

	usageAsFrozen := legacyDemandResourceUsageToFrozenBalance(usage, totalLimit, totalWeight)
	v2Usage := usageAsFrozen - v1OwnFrozen - v1AcquiredFrozen - v2AcquiredFrozen
	if v2Usage < 0 {
		v2Usage = 0
	}
	available := selfFrozen - v2Usage
	if available < 0 {
		return 0
	}
	return available
}

// legacyDemandResourceUsageToFrozenBalance converts recovered usage back into the frozen
// balance it represents. Mirrors java DelegateResourceActuator.validate and
// DelegateResourceProcessor: (long)(usage * TRX_PRECISION * ((double)totalWeight
// / totalLimit)) — a plain double cast on BOTH the actuator and VM paths,
// UNGATED. AllowHardenResourceCalculation only switches the recovery averaging in
// ResourceProcessor.increase (mirrored by computeResourceIncrease); java never
// applies big.Int / harden to this product, so neither must go.
func legacyDemandResourceUsageToFrozenBalance(usage, totalLimit, totalWeight int64) int64 {
	return tcommon.JavaDoubleToInt64(float64(usage) * float64(params.TRXPrecision) * (float64(totalWeight) / float64(totalLimit)))
}

func legacyDemandTotalBandwidthFrozen(acct *types.Account) int64 {
	frozen := acct.TotalFrozenBandwidth()
	frozen += acct.AcquiredDelegatedFrozenBandwidth()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	return frozen
}

func legacyDemandTotalEnergyFrozen(acct *types.Account) int64 {
	frozen := acct.FrozenEnergyAmount()
	frozen += acct.AcquiredDelegatedFrozenEnergy()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	return frozen
}

func demandAddress(i int) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	binary.BigEndian.PutUint32(a[16:], uint32(i+100))
	return a
}

// The fixture counts construction, writes, reads, cache churn, mutations,
// snapshot/revert, commit/reopen, and closing all databases in the timed body.
// Every owner is first-used after reopen; repetition is an explicit workload
// parameter rather than a pre-warmed benchmark setup.
func demandLifecycle(tb testing.TB, legacy bool, accounts, repeats, auxiliaries int) (string, string) {
	tb.Helper()
	disk := rawdb.NewMemoryDatabase()
	db := state.NewDatabase(rawdb.WrapKeyValueStore(disk))
	defer db.Close()
	if err := rawdb.InitializeStateCommitment(disk); err != nil {
		tb.Fatal(err)
	}
	s, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < accounts; i++ {
		addr := demandAddress(i)
		s.CreateAccount(addr, corepb.AccountType_Normal)
		s.AddBalance(addr, 100_000_000_000)
		s.FreezeV1Bandwidth(addr, 11_000_000, 10000)
		s.FreezeV1Energy(addr, 12_000_000, 10000)
		s.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 200_000_000)
		s.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, 300_000_000)
		s.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_BANDWIDTH, 30_000_000)
		s.AddAcquiredDelegatedFrozenV2(addr, corepb.ResourceCode_ENERGY, 40_000_000)
		s.SetNetUsage(addr, 12345)
		s.SetEnergyUsage(addr, 23456)
		s.SetNetWindow(addr, 28800, false)
		s.SetEnergyWindow(addr, 28800, false)
		s.SetLatestConsumeTime(addr, 100)
		s.SetLatestConsumeTimeForEnergy(addr, 100)
		if auxiliaries > 0 {
			s.SetPermissions(addr, types.MakeDefaultOwnerPermission(addr), nil, nil)
			s.SetVotes(addr, []*corepb.Vote{{VoteAddress: demandAddress(accounts + 1).Bytes(), VoteCount: 7}})
			for j := 0; j < auxiliaries; j++ {
				s.SetTRC10Balance(addr, int64(1_000_000+j), int64(j+1))
				s.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, int64(j+1), int64(j+10000))
			}
		}
	}
	root, err := s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	s, err = state.New(root, db)
	if err != nil {
		tb.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetTotalNetWeight(100000)
	dp.SetTotalEnergyWeight(100000)
	var results bytes.Buffer
	apply := func(addr, owner tcommon.Address, resource corepb.ResourceCode, now int64) {
		var available, transfer, window int64
		var optimized bool
		if legacy {
			available = legacyDemandAvailableFrozenV2ForDelegation(s, dp, addr, resource, now)
			transfer, window, optimized = legacyDemandTransferUsageFromReceiver(s, dp, addr, resource, 1_000_000, now, now%2 == 0)
			if transfer > 0 {
				legacyDemandFoldUsageIntoOwner(s, dp, owner, resource, transfer, window, optimized, now)
			}
		} else {
			available = AvailableFrozenV2ForDelegation(s, dp, addr, resource, now)
			transfer, window, optimized = TransferUsageFromReceiver(s, dp, addr, resource, 1_000_000, now, now%2 == 0)
			if transfer > 0 {
				FoldUsageIntoOwner(s, dp, owner, resource, transfer, window, optimized, now)
			}
		}
		fmt.Fprintf(&results, "%d/%d/%d/%t;", available, transfer, window, optimized)
	}
	s.BeginDomainChangeJournalCapture(disk, 1, tcommon.Hash{31: 1}, 1, 1)
	for repeat := 0; repeat < repeats; repeat++ {
		for i := 0; i < accounts; i++ {
			addr, owner := demandAddress(i), demandAddress((i+1)%accounts)
			resource := corepb.ResourceCode(i % 2)
			if repeat == 0 && i%8 == 0 {
				mark := s.Snapshot()
				apply(addr, owner, resource, int64(1000+repeat))
				s.RevertToSnapshot(mark)
			}
			apply(addr, owner, resource, int64(1000+repeat))
		}
	}
	if repeats > 0 {
		if legacy {
			legacyDemandMergeUsageToInheritor(s, dp, demandAddress(0), demandAddress(1), corepb.ResourceCode_ENERGY, 2000)
		} else {
			MergeUsageToInheritor(s, dp, demandAddress(0), demandAddress(1), corepb.ResourceCode_ENERGY, 2000)
		}
	}
	cp, err := s.Copy()
	if err != nil {
		tb.Fatal(err)
	}
	cp.SetNetUsage(demandAddress(0), 77)
	if cp.GetNetUsage(demandAddress(0)) != 77 {
		tb.Fatal("copy mutation failed")
	}
	if err := s.FlushPendingDomainChanges(1); err != nil {
		tb.Fatal(err)
	}
	root, err = s.Commit()
	if err != nil {
		tb.Fatal(err)
	}
	s, err = state.New(root, db)
	if err != nil {
		tb.Fatal(err)
	}
	h := sha256.New()
	h.Write(root[:])
	for i := 0; i < accounts; i++ {
		a := s.GetAccount(demandAddress(i))
		if a == nil {
			tb.Fatal("missing final account")
		}
		v, err := proto.MarshalOptions{Deterministic: true}.Marshal(a.Proto())
		if err != nil {
			tb.Fatal(err)
		}
		h.Write(v)
	}
	// Compare every durable key/value too, including temporal capture bytes.
	it := disk.NewIterator(nil, nil)
	for it.Next() {
		for _, v := range [][]byte{it.Key(), it.Value()} {
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(len(v)))
			h.Write(n[:])
			h.Write(v)
		}
	}
	if err := it.Error(); err != nil {
		tb.Fatal(err)
	}
	it.Release()
	if err := s.Error(); err != nil {
		tb.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(results.Bytes())), fmt.Sprintf("%x", h.Sum(nil))
}

func TestDemandDelegationLegacyLifecycle(t *testing.T) {
	for _, shape := range []struct {
		name    string
		n, r, a int
	}{{"high_churn", 64, 1, 16}, {"mixed", 32, 4, 16}, {"minimal", 8, 1, 0}, {"dense_aux", 8, 2, 128}} {
		t.Run(shape.name, func(t *testing.T) {
			oldOut, oldState := demandLifecycle(t, true, shape.n, shape.r, shape.a)
			out, post := demandLifecycle(t, false, shape.n, shape.r, shape.a)
			t.Logf("TRACE %s %s %s", shape.name, out, post)
			if oldOut != out || oldState != post {
				t.Fatalf("legacy outcome mismatch: %s/%s vs %s/%s", oldOut, oldState, out, post)
			}
		})
	}
}

func BenchmarkDemandDelegationLifecycle(b *testing.B) {
	for _, shape := range []struct {
		name    string
		n, r, a int
	}{{"high_churn", 64, 1, 16}, {"mixed", 32, 4, 16}, {"minimal", 8, 1, 0}, {"dense_aux", 8, 2, 128}, {"write_only", 64, 0, 16}} {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			cpuStart := demandProcessCPU()
			for i := 0; i < b.N; i++ {
				demandLifecycle(b, false, shape.n, shape.r, shape.a)
			}
			b.ReportMetric(float64(demandProcessCPU()-cpuStart)/float64(b.N), "cpu-ns/op")
		})
	}
}

func TestDemandDelegationProjectionIgnoresUnrelatedCorruption(t *testing.T) {
	for _, resource := range []corepb.ResourceCode{corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY} {
		disk := rawdb.NewMemoryDatabase()
		db := state.NewDatabase(rawdb.WrapKeyValueStore(disk))
		s, err := state.New(tcommon.Hash{}, db)
		if err != nil {
			t.Fatal(err)
		}
		addr := demandAddress(0)
		s.CreateAccount(addr, corepb.AccountType_Normal)
		s.AddFreezeV2(addr, resource, 100_000_000)
		if err := s.SetAccountKV(addr, kvdomains.AccountVotesAux, []byte{0, 0, 0, 0}, []byte{0xff}); err != nil {
			t.Fatal(err)
		}
		root, err := s.Commit()
		if err != nil {
			t.Fatal(err)
		}
		s, err = state.New(root, db)
		if err != nil {
			t.Fatal(err)
		}
		dp := state.NewDynamicProperties()
		if got := AvailableFrozenV2ForDelegation(s, dp, addr, resource, 100); got != 100_000_000 || s.Error() != nil {
			t.Fatalf("irrelevant corruption touched: got=%d err=%v", got, s.Error())
		}
		old, err := state.New(root, db)
		if err != nil {
			t.Fatal(err)
		}
		if old.GetAccount(addr) != nil || old.Error() == nil {
			t.Fatal("fixture did not corrupt full account materialization")
		}
		db.Close()
	}
}

func demandProcessCPU() int64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return (r.Utime.Sec+r.Stime.Sec)*1_000_000_000 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}
