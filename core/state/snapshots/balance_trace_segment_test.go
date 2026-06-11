package snapshots

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestBalanceTraceSegmentBuildVerifyLookup(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	ownerA := balanceTraceTestAddress(0xa1)
	ownerB := balanceTraceTestAddress(0xb2)

	if err := rawdb.WriteBlockBalanceTrace(source, 10, balanceTraceTestBlockTrace(10, 1000)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 10: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 12: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerA.Bytes(), 9, 900); err != nil {
		t.Fatalf("WriteAccountTrace outside range: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerA.Bytes(), 10, 1000); err != nil {
		t.Fatalf("WriteAccountTrace ownerA 10: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerA.Bytes(), 15, 1500); err != nil {
		t.Fatalf("WriteAccountTrace ownerA 15: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerB.Bytes(), 12, 1200); err != nil {
		t.Fatalf("WriteAccountTrace ownerB 12: %v", err)
	}

	result, err := NewAggregator(snapshotDir).BuildBalanceTraces(source, 10, 20)
	if err != nil {
		t.Fatalf("BuildBalanceTraces: %v", err)
	}
	if len(result.Segments) != 1 {
		t.Fatalf("BuildBalanceTraces segments = %d, want 1", len(result.Segments))
	}
	ref := result.Segments[0]
	if ref.Dataset != SegmentDatasetBalanceTrace || ref.Kind != SegmentBalanceTrace {
		t.Fatalf("ref family = %s/%s, want %s/%s", ref.Dataset, ref.Kind, SegmentDatasetBalanceTrace, SegmentBalanceTrace)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref metadata missing: size=%d checksum=%q", ref.Size, ref.Checksum)
	}
	if err := CheckBalanceTraceSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckBalanceTraceSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyManifestFiles(snapshotDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}

	seg, err := OpenBalanceTraceSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenBalanceTraceSegment: %v", err)
	}
	defer seg.Close()
	trace, ok, err := seg.BlockBalanceTrace(12)
	if err != nil || !ok || trace.GetTimestamp() != 1200 {
		t.Fatalf("BlockBalanceTrace 12 = %+v/%v/%v, want timestamp 1200", trace, ok, err)
	}
	if trace, ok, err := seg.BlockBalanceTrace(11); err != nil || ok || trace != nil {
		t.Fatalf("missing BlockBalanceTrace = %+v/%v/%v, want nil/false/nil", trace, ok, err)
	}
	block, balance, ok, err := seg.AccountTraceAtOrBefore(ownerA.Bytes(), 16)
	if err != nil || !ok || block != 15 || balance != 1500 {
		t.Fatalf("AccountTraceAtOrBefore ownerA 16 = %d/%d/%v/%v, want 15/1500/true/nil", block, balance, ok, err)
	}
	block, balance, ok, err = seg.AccountTraceAtOrBefore(ownerA.Bytes(), 14)
	if err != nil || !ok || block != 10 || balance != 1000 {
		t.Fatalf("AccountTraceAtOrBefore ownerA 14 = %d/%d/%v/%v, want 10/1000/true/nil", block, balance, ok, err)
	}
	if _, _, ok, err := seg.AccountTraceAtOrBefore(ownerA.Bytes(), 9); err != nil || ok {
		t.Fatalf("AccountTraceAtOrBefore before segment ok=%v err=%v, want false/nil", ok, err)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err = mgr.BlockBalanceTrace(10)
	if err != nil || !ok || trace.GetTimestamp() != 1000 {
		t.Fatalf("manager BlockBalanceTrace = %+v/%v/%v, want timestamp 1000", trace, ok, err)
	}
	block, balance, ok, err = mgr.AccountTraceAtOrBefore(ownerB.Bytes(), 20)
	if err != nil || !ok || block != 12 || balance != 1200 {
		t.Fatalf("manager AccountTraceAtOrBefore = %d/%d/%v/%v, want 12/1200/true/nil", block, balance, ok, err)
	}

	coldOnly := rawdb.NewMemoryChainDB()
	coldOnly.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(coldOnly, 12); got == nil || got.GetTimestamp() != 1200 {
		t.Fatalf("rawdb cold BlockBalanceTrace = %+v, want timestamp 1200", got)
	}
	block, balance, ok, err = rawdb.ReadAccountTraceAtOrBefore(coldOnly, ownerA.Bytes(), 16)
	if err != nil || !ok || block != 15 || balance != 1500 {
		t.Fatalf("rawdb cold AccountTraceAtOrBefore = %d/%d/%v/%v, want 15/1500/true/nil", block, balance, ok, err)
	}
}

func TestBalanceTraceManagerSearchesOlderSegments(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xc3)
	if err := rawdb.WriteAccountTrace(source, owner.Bytes(), 5, 500); err != nil {
		t.Fatalf("WriteAccountTrace old: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 15, balanceTraceTestBlockTrace(15, 1500)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace new segment: %v", err)
	}
	oldRef, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 0, 10)
	if err != nil {
		t.Fatalf("Build old segment: %v", err)
	}
	newRef, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 11, 20)
	if err != nil {
		t.Fatalf("Build new segment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{newRef, oldRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	block, balance, ok, err := mgr.AccountTraceAtOrBefore(owner.Bytes(), 15)
	if err != nil || !ok || block != 5 || balance != 500 {
		t.Fatalf("AccountTraceAtOrBefore across segments = %d/%d/%v/%v, want 5/500/true/nil", block, balance, ok, err)
	}
}

func balanceTraceTestAddress(id byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[common.AddressLength-1] = id
	return addr
}

func balanceTraceTestBlockTrace(blockNum int64, timestamp int64) *contractpb.BlockBalanceTrace {
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{byte(blockNum)}, common.HashLength),
			Number: blockNum,
		},
		Timestamp: timestamp,
	}
}
