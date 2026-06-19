package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
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

func TestBuildBalanceTraceSegmentWithOptionsUsesETLScratch(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	ownerA := balanceTraceTestAddress(0xa2)
	ownerB := balanceTraceTestAddress(0xb3)

	if err := rawdb.WriteAccountTrace(source, ownerB.Bytes(), 3, 300); err != nil {
		t.Fatalf("WriteAccountTrace ownerB 3: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerA.Bytes(), 8, 800); err != nil {
		t.Fatalf("WriteAccountTrace ownerA 8: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerA.Bytes(), 5, 500); err != nil {
		t.Fatalf("WriteAccountTrace ownerA 5: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, ownerB.Bytes(), 7, 700); err != nil {
		t.Fatalf("WriteAccountTrace ownerB 7: %v", err)
	}

	etlTemp := filepath.Join(root, "etl-scratch")
	ref, err := BuildBalanceTraceSegmentFromDBWithOptions(source, snapshotDir, "trace/balance-trace-1-10.seg", 1, 10, RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDBWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if err := CheckBalanceTraceSegment(snapshotDir, ref); err != nil {
		t.Fatalf("CheckBalanceTraceSegment: %v", err)
	}

	seg, err := OpenBalanceTraceSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenBalanceTraceSegment: %v", err)
	}
	defer seg.Close()
	block, balance, ok, err := seg.AccountTraceAtOrBefore(ownerA.Bytes(), 6)
	if err != nil || !ok || block != 5 || balance != 500 {
		t.Fatalf("AccountTraceAtOrBefore ownerA 6 = %d/%d/%v/%v, want 5/500/true/nil", block, balance, ok, err)
	}
	block, balance, ok, err = seg.AccountTraceAtOrBefore(ownerA.Bytes(), 9)
	if err != nil || !ok || block != 8 || balance != 800 {
		t.Fatalf("AccountTraceAtOrBefore ownerA 9 = %d/%d/%v/%v, want 8/800/true/nil", block, balance, ok, err)
	}
	block, balance, ok, err = seg.AccountTraceAtOrBefore(ownerB.Bytes(), 6)
	if err != nil || !ok || block != 3 || balance != 300 {
		t.Fatalf("AccountTraceAtOrBefore ownerB 6 = %d/%d/%v/%v, want 3/300/true/nil", block, balance, ok, err)
	}
}

func TestCheckBalanceTraceSegmentRejectsMissingAccountIndexForOperation(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xd4)
	trace := balanceTraceTestBlockTrace(12, 1200)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: owner.Bytes(),
			Amount:  25,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(source, 12, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 12, 12)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	if err := CheckBalanceTraceSegment(snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "missing account index") {
		t.Fatalf("CheckBalanceTraceSegment missing account index = %v, want missing-account-index error", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if _, err := VerifyManifestFiles(snapshotDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err == nil || !strings.Contains(err.Error(), "missing account index") {
		t.Fatalf("VerifyManifestFiles missing account index = %v, want missing-account-index error", err)
	}
}

func TestCheckBalanceTraceSegmentRejectsMalformedOperationAddress(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	trace := balanceTraceTestBlockTrace(13, 1300)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: []byte{0x41},
			Amount:  25,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(source, 13, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 13, 13)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	if err := CheckBalanceTraceSegment(snapshotDir, ref); err == nil || !strings.Contains(err.Error(), "operation address length") {
		t.Fatalf("CheckBalanceTraceSegment malformed operation address = %v, want address-length error", err)
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

func TestBalanceTraceManagerSkipsOutOfRangeMissingSegmentForBlockLookup(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	if err := rawdb.WriteBlockBalanceTrace(source, 10, balanceTraceTestBlockTrace(10, 1000)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace old: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 20, balanceTraceTestBlockTrace(20, 2000)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace new: %v", err)
	}
	oldRef, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 10, 10)
	if err != nil {
		t.Fatalf("Build old segment: %v", err)
	}
	newRef, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 20, 20)
	if err != nil {
		t.Fatalf("Build new segment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{newRef, oldRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.Remove(filepath.Join(snapshotDir, newRef.Path)); err != nil {
		t.Fatalf("remove newer segment: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(10)
	if err != nil || !ok || trace.GetTimestamp() != 1000 {
		t.Fatalf("BlockBalanceTrace old with missing newer segment = %+v/%v/%v, want timestamp 1000", trace, ok, err)
	}
	if trace, ok, err := mgr.BlockBalanceTrace(20); err == nil || ok || trace != nil {
		t.Fatalf("BlockBalanceTrace missing in-range segment = %+v/%v/%v, want nil/false/error", trace, ok, err)
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
