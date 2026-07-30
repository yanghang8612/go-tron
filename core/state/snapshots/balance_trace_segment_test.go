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
	"google.golang.org/protobuf/proto"
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

func TestBuildBalanceTraceSegmentFromReaderMaterializesColdRows(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	targetDir := filepath.Join(root, "target")
	source := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xc3)
	trace := balanceTraceTestBlockTrace(10, 1000)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: owner.Bytes(),
			Amount:  25,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(source, 10, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 10: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, owner.Bytes(), 10, 1000); err != nil {
		t.Fatalf("WriteAccountTrace owner 10: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 12: %v", err)
	}
	sourceRef, err := BuildBalanceTraceSegmentFromDB(source, sourceDir, "", 10, 12)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	if err := PublishManifest(sourceDir, NewManifest(0, 0, []SegmentRef{sourceRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(sourceDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	result, err := NewAggregator(targetDir).BuildBalanceTracesFromReaderWithOptions(mgr, 10, 12, RestoreETLOptions{BufferLimit: 1})
	if err != nil {
		t.Fatalf("BuildBalanceTracesFromReaderWithOptions: %v", err)
	}
	if result.Manifest == nil || len(result.Manifest.Segments) != 1 || len(result.Segments) != 1 {
		t.Fatalf("BuildBalanceTracesFromReaderWithOptions result = %+v, want one manifest segment", result)
	}
	ref := result.Segments[0]
	if err := CheckBalanceTraceSegment(targetDir, ref); err != nil {
		t.Fatalf("CheckBalanceTraceSegment: %v", err)
	}
	if _, err := VerifyManifestFiles(targetDir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	seg, err := OpenBalanceTraceSegment(targetDir, ref)
	if err != nil {
		t.Fatalf("OpenBalanceTraceSegment: %v", err)
	}
	defer seg.Close()
	gotTrace, ok, err := seg.BlockBalanceTrace(10)
	if err != nil || !ok || gotTrace.GetTimestamp() != 1000 {
		t.Fatalf("BlockBalanceTrace 10 = %+v/%v/%v, want timestamp 1000", gotTrace, ok, err)
	}
	block, balance, ok, err := seg.AccountTraceAtOrBefore(owner.Bytes(), 10)
	if err != nil || !ok || block != 10 || balance != 1000 {
		t.Fatalf("AccountTraceAtOrBefore = %d/%d/%v/%v, want 10/1000/true/nil", block, balance, ok, err)
	}
}

func TestBuildBalanceTraceSegmentFromReaderRequiresExactAccountRows(t *testing.T) {
	owner := balanceTraceTestAddress(0xc4)
	trace := balanceTraceTestBlockTrace(10, 1000)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: owner.Bytes(),
			Amount:  25,
		}},
	}}
	_, err := BuildBalanceTraceSegmentFromReader(balanceTraceMissingAccountReader{trace: trace}, t.TempDir(), "", 10, 10)
	if err == nil || !strings.Contains(err.Error(), "missing exact AccountTrace") {
		t.Fatalf("BuildBalanceTraceSegmentFromReader missing account err = %v, want missing exact AccountTrace", err)
	}
}

func TestBalanceTracePayloadReadRejectsOutOfBoundsBeforeAlloc(t *testing.T) {
	path := filepath.Join(t.TempDir(), "balance-trace-payload.bin")
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03, 0x04}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	if _, err := readBalanceTracePayloadAt(file, 0, ^uint64(0), 4); err == nil || !strings.Contains(err.Error(), "exceeds segment bound") {
		t.Fatalf("readBalanceTracePayloadAt error = %v, want bounded rejection", err)
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

func TestBuildBalanceTraceSegmentRejectsMissingAccountIndexForOperation(t *testing.T) {
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
	if _, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 12, 12); err == nil || !strings.Contains(err.Error(), "missing account index") {
		t.Fatalf("BuildBalanceTraceSegmentFromDB missing account index = %v, want missing-account-index error", err)
	}
	if matches, err := filepath.Glob(filepath.Join(snapshotDir, "trace", "balance-trace-12-12-*.seg")); err != nil || len(matches) != 0 {
		t.Fatalf("invalid balance trace files = %v/%v, want none", matches, err)
	}
}

func TestBuildBalanceTraceSegmentRejectsMalformedOperationAddress(t *testing.T) {
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
	if _, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 13, 13); err == nil || !strings.Contains(err.Error(), "operation address length") {
		t.Fatalf("BuildBalanceTraceSegmentFromDB malformed operation address = %v, want address-length error", err)
	}
	if matches, err := filepath.Glob(filepath.Join(snapshotDir, "trace", "balance-trace-13-13-*.seg")); err != nil || len(matches) != 0 {
		t.Fatalf("invalid balance trace files = %v/%v, want none", matches, err)
	}
}

func TestAggregatorBuildBalanceTracesRejectsInvalidSegmentBeforeManifestPublish(t *testing.T) {
	dir := t.TempDir()
	source := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xd5)
	trace := balanceTraceTestBlockTrace(14, 1400)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: owner.Bytes(),
			Amount:  25,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(source, 14, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	if _, err := NewAggregator(dir).BuildBalanceTraces(source, 14, 14); err == nil || !strings.Contains(err.Error(), "missing account index") {
		t.Fatalf("BuildBalanceTraces error = %v, want missing-account-index error", err)
	}
	if manifest, err := LoadProductionManifest(dir); err == nil || !os.IsNotExist(err) || manifest != nil {
		t.Fatalf("LoadProductionManifest = %+v/%v, want no manifest", manifest, err)
	}
}

func TestBalanceTraceSegmentBlockBalanceTraceRejectsPayloadNumberMismatch(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	if err := rawdb.WriteBlockBalanceTrace(source, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 12, 12)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	rewriteBalanceTracePayloadNumber(t, snapshotDir, ref, 12, 13)

	seg, err := OpenBalanceTraceSegment(snapshotDir, ref)
	if err != nil {
		t.Fatalf("OpenBalanceTraceSegment: %v", err)
	}
	trace, ok, err := seg.BlockBalanceTrace(12)
	closeErr := seg.Close()
	if err == nil || !ok || trace == nil || !strings.Contains(err.Error(), "payload number 13") {
		t.Fatalf("BlockBalanceTrace mismatched payload = trace %+v ok %v err %v, want payload-number error", trace, ok, err)
	}
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err = mgr.BlockBalanceTrace(12)
	if err == nil || ok || trace != nil || !strings.Contains(err.Error(), "payload number 13") {
		t.Fatalf("manager BlockBalanceTrace mismatched payload = trace %+v ok %v err %v, want payload-number error", trace, ok, err)
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

type balanceTraceMissingAccountReader struct {
	trace *contractpb.BlockBalanceTrace
}

func (r balanceTraceMissingAccountReader) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	if r.trace != nil && r.trace.GetBlockIdentifier().GetNumber() == blockNum {
		return r.trace, true, nil
	}
	return nil, false, nil
}

func (balanceTraceMissingAccountReader) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	return 0, 0, false, nil
}

func rewriteBalanceTracePayloadNumber(t *testing.T, dir string, ref SegmentRef, blockNum, payloadNumber int64) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(dir, ref.Path), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()
	header, err := readBalanceTraceHeader(file)
	if err != nil {
		t.Fatalf("readBalanceTraceHeader: %v", err)
	}
	for i := uint64(0); i < header.blockCount; i++ {
		entry, err := readBalanceTraceBlockIndexEntryAt(file, header.blockIndexOffset+i*balanceTraceBlockIndexEntrySize)
		if err != nil {
			t.Fatalf("readBalanceTraceBlockIndexEntryAt: %v", err)
		}
		if entry.blockNum != uint64(blockNum) {
			continue
		}
		raw, err := readBalanceTracePayloadAt(file, entry.offset, entry.length, header.accountIndexOffset)
		if err != nil {
			t.Fatalf("readBalanceTracePayloadAt: %v", err)
		}
		var trace contractpb.BlockBalanceTrace
		if err := proto.Unmarshal(raw, &trace); err != nil {
			t.Fatalf("unmarshal balance trace payload: %v", err)
		}
		if trace.BlockIdentifier == nil {
			trace.BlockIdentifier = &contractpb.BlockBalanceTrace_BlockIdentifier{}
		}
		trace.BlockIdentifier.Number = payloadNumber
		encoded, err := proto.Marshal(&trace)
		if err != nil {
			t.Fatalf("marshal balance trace payload: %v", err)
		}
		if uint64(len(encoded)) != entry.length {
			t.Fatalf("rewritten payload length = %d, want %d", len(encoded), entry.length)
		}
		if _, err := file.WriteAt(encoded, int64(entry.offset)); err != nil {
			t.Fatalf("rewrite balance trace payload: %v", err)
		}
		return
	}
	t.Fatalf("balance trace block %d not found", blockNum)
}
