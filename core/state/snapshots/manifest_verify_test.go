package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestVerifyRemoteManifestFiles(t *testing.T) {
	dir, identity := writeVerifiableBranchManifest(t)

	report, err := VerifyRemoteManifestFiles(dir, identity)
	if err != nil {
		t.Fatalf("VerifyRemoteManifestFiles: %v", err)
	}
	if report.ActiveSegments != 1 || report.RetiredSegments != 0 {
		t.Fatalf("report = %+v, want 1 active / 0 retired", report)
	}
}

func TestVerifyRemoteManifestFilesRejectsWrongChain(t *testing.T) {
	dir, identity := writeVerifiableBranchManifest(t)
	identity.NetworkID++

	if _, err := VerifyRemoteManifestFiles(dir, identity); err == nil {
		t.Fatal("wrong chain identity accepted")
	}
}

func TestVerifyRemoteManifestFilesRejectsCorruptSegment(t *testing.T) {
	dir, identity := writeVerifiableBranchManifest(t)
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if len(manifest.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(manifest.Segments))
	}
	if err := os.WriteFile(filepath.Join(dir, manifest.Segments[0].Path), []byte(`{"corrupt":true}`), 0o644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}

	if _, err := VerifyRemoteManifestFiles(dir, identity); err == nil {
		t.Fatal("corrupt segment accepted")
	}
}

func TestVerifyRemoteManifestFilesRequiresChecksums(t *testing.T) {
	dir, identity := writeVerifiableBranchManifest(t)
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	manifest.Segments[0].Checksum = ""
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	_, err = VerifyRemoteManifestFiles(dir, identity)
	if err == nil {
		t.Fatal("segment without checksum accepted")
	}
	if !strings.Contains(err.Error(), "missing required checksum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreLatestFromVerifiedManifestRestoresCommitmentBranches(t *testing.T) {
	dir, identity := writeVerifiableBranchManifest(t)
	restored := rawdb.NewMemoryDatabase()

	result, err := RestoreLatestFromVerifiedManifest(restored, dir, identity)
	if err != nil {
		t.Fatalf("RestoreLatestFromVerifiedManifest: %v", err)
	}
	if result.RestoredTxNum != 10 || result.Verification.ActiveSegments != 1 {
		t.Fatalf("result = %+v, want txNum=10 active=1", result)
	}
	rows := 0
	if err := rawdb.IterateCommitmentBranches(restored, func(_, _ []byte) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate restored branches: %v", err)
	}
	if rows == 0 {
		t.Fatal("no commitment branches restored")
	}
}

func TestRestoreHistoryFromVerifiedManifestRestoresHotRowsAndIndexes(t *testing.T) {
	dir, identity, owner := writeVerifiableHistoryManifest(t)
	restored := rawdb.NewMemoryDatabase()

	result, err := RestoreHistoryFromVerifiedManifest(restored, dir, identity)
	if err != nil {
		t.Fatalf("RestoreHistoryFromVerifiedManifest: %v", err)
	}
	if result.FromTxNum != 10 || result.ToTxNum != 12 || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
		t.Fatalf("result = %+v, want range [10,12], 2 changes, 2 tx ranges", result)
	}
	got, ok, err := rawdb.ReadStateDomainChange(restored, 1, 1)
	if err != nil || !ok {
		t.Fatalf("ReadStateDomainChange: ok=%v err=%v", ok, err)
	}
	if got.TxNum != 10 || got.Owner != owner || string(got.Prev) != "old-a" {
		t.Fatalf("restored change = %+v", got)
	}
	gotRange, ok, err := rawdb.ReadStateTxRange(restored, 1)
	if err != nil || !ok {
		t.Fatalf("ReadStateTxRange: ok=%v err=%v", ok, err)
	}
	if gotRange.BeginTxNum != 10 || gotRange.EndTxNum != 11 {
		t.Fatalf("restored tx range = %+v, want [10,11]", gotRange)
	}
	noChangeRange, ok, err := rawdb.ReadStateTxRange(restored, 2)
	if err != nil || !ok {
		t.Fatalf("ReadStateTxRange no-change block: ok=%v err=%v", ok, err)
	}
	if noChangeRange.BeginTxNum != 12 || noChangeRange.EndTxNum != 12 {
		t.Fatalf("restored no-change tx range = %+v, want [12,12]", noChangeRange)
	}

	var keyed []*rawdb.StateDomainChange
	if err := rawdb.IterateStateDomainChangesByKey(restored, 9, 11, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.ContractStorage, []byte("slot/a"), func(change *rawdb.StateDomainChange) (bool, error) {
		keyed = append(keyed, change)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateStateDomainChangesByKey: %v", err)
	}
	if len(keyed) != 1 || keyed[0].TxNum != 10 || string(keyed[0].Next) != "new-a" {
		t.Fatalf("keyed restored changes = %+v", keyed)
	}
}

func TestVerifyLoadedManifestFilesRejectsStaleStateDomainAccessor(t *testing.T) {
	dir := t.TempDir()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 10,
		ToTxNum:   11,
		Path:      "history/state-domain-change-10-11.seg",
	}, []*rawdb.StateDomainChange{
		binaryStateDomainChange(1, 10, 1, "slot/a"),
		binaryStateDomainChange(2, 11, 1, "slot/b"),
	})
	if err != nil {
		t.Fatalf("write state-domain history: %v", err)
	}
	manifest := NewManifest(10, 11, []SegmentRef{segRef, idxRef, accessorRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if binary.BigEndian.Uint32(data[8:12]) >= stateDomainChangeBinaryVersionV3 {
		firstRecordIndex := stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra + stateDomainChangeBinaryAccessorV3HashSize + 8
		secondRecordIndex := firstRecordIndex + stateDomainChangeBinaryAccessorV3ExactEntrySize
		if secondRecordIndex+4 > len(data) {
			t.Fatalf("accessor data length = %d, want two v3 exact entries", len(data))
		}
		first := binary.BigEndian.Uint32(data[firstRecordIndex : firstRecordIndex+4])
		second := binary.BigEndian.Uint32(data[secondRecordIndex : secondRecordIndex+4])
		binary.BigEndian.PutUint32(data[firstRecordIndex:firstRecordIndex+4], second)
		binary.BigEndian.PutUint32(data[secondRecordIndex:secondRecordIndex+4], first)
	} else {
		if len(data) < stateDomainChangeBinaryHeaderSize+16 {
			t.Fatalf("accessor data length = %d, want second offset", len(data))
		}
		secondEntryOffset := binary.BigEndian.Uint64(data[stateDomainChangeBinaryHeaderSize+8 : stateDomainChangeBinaryHeaderSize+16])
		if secondEntryOffset+4 > uint64(len(data)) {
			t.Fatalf("second accessor offset %d outside data length %d", secondEntryOffset, len(data))
		}
		keyLen := binary.BigEndian.Uint32(data[secondEntryOffset : secondEntryOffset+4])
		keyEnd := secondEntryOffset + 4 + uint64(keyLen)
		if keyEnd == secondEntryOffset+4 || keyEnd > uint64(len(data)) {
			t.Fatalf("second accessor key range [%d,%d) outside data length %d", secondEntryOffset+4, keyEnd, len(data))
		}
		if data[keyEnd-1] != 'b' {
			t.Fatalf("second accessor key suffix = %q, want b", data[keyEnd-1])
		}
		data[keyEnd-1] = 'c'
	}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		t.Fatalf("write stale accessor: %v", err)
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("single-file accessor check should still pass: %v", err)
	}

	manifest = NewManifest(10, 11, []SegmentRef{segRef, idxRef, accessorRef})
	_, err = VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || (!strings.Contains(err.Error(), "key mismatch") && !strings.Contains(err.Error(), "exact entry")) {
		t.Fatalf("VerifyLoadedManifestFiles stale accessor err = %v, want companion mismatch", err)
	}
}

func TestVerifyLoadedManifestFilesRejectsStaleStateDomainIndex(t *testing.T) {
	dir := t.TempDir()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 20,
		ToTxNum:   21,
		Path:      "history/state-domain-change-20-21.seg",
	}, []*rawdb.StateDomainChange{
		binaryStateDomainChange(1, 20, 1, "slot/a"),
		binaryStateDomainChange(2, 21, 1, "slot/b"),
	})
	if err != nil {
		t.Fatalf("write state-domain history: %v", err)
	}
	manifest := NewManifest(20, 21, []SegmentRef{segRef, idxRef, accessorRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	data := mustReadFile(t, filepath.Join(dir, idxRef.Path))
	if len(data) < stateDomainChangeBinaryHeaderSize+2*stateDomainChangeBinaryIndexEntrySize {
		t.Fatalf("index data length = %d, want two entries", len(data))
	}
	firstOffset := binary.BigEndian.Uint64(data[stateDomainChangeBinaryHeaderSize+8 : stateDomainChangeBinaryHeaderSize+16])
	secondOffsetField := stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryIndexEntrySize + 8
	binary.BigEndian.PutUint64(data[secondOffsetField:secondOffsetField+8], firstOffset)
	setStateDomainChangeBinaryRefMetadata(&idxRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, idxRef.Path), data); err != nil {
		t.Fatalf("write stale index: %v", err)
	}
	if err := CheckStateDomainChangeIndexSegment(dir, idxRef); err != nil {
		t.Fatalf("single-file index check should still pass: %v", err)
	}

	manifest = NewManifest(20, 21, []SegmentRef{segRef, idxRef, accessorRef})
	_, err = VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("VerifyLoadedManifestFiles stale index err = %v, want offset mismatch", err)
	}
}

func TestVerifyStateDomainChangeBinaryCompanionsRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 30,
		ToTxNum:   31,
		Path:      "history/state-domain-change-30-31.seg",
	}, []*rawdb.StateDomainChange{
		binaryStateDomainChange(1, 30, 1, "slot/a"),
		binaryStateDomainChange(2, 31, 1, "slot/b"),
	})
	if err != nil {
		t.Fatalf("write state-domain history: %v", err)
	}
	idxRef.Checksum = badSnapshotChecksum()

	err = verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, segRef, idxRef, accessorRef)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("verifyStateDomainChangeBinaryCompanionsAgainstSegment err = %v, want checksum mismatch", err)
	}
}

func TestVerifyLoadedManifestFilesRejectsStaleLatestBinaryAccessor(t *testing.T) {
	dir := t.TempDir()
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, dir)
	manifest := NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	corruptLatestBinaryCompanionSegmentChecksum(t, dir, &accessorRef)
	if err := CheckLatestAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("single-file accessor check should still pass: %v", err)
	}

	manifest = NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	_, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || !strings.Contains(err.Error(), "segment checksum mismatch") {
		t.Fatalf("VerifyLoadedManifestFiles stale latest accessor err = %v, want segment checksum mismatch", err)
	}
}

func TestVerifyLoadedManifestFilesRejectsLatestBinaryAccessorOffsetMismatch(t *testing.T) {
	dir := t.TempDir()
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, dir)
	manifest := NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	data := mustReadFile(t, filepath.Join(dir, accessorRef.Path))
	if len(data) < latestBinaryAccessorHeaderSize+3*8 {
		t.Fatalf("accessor data length = %d, want three offsets", len(data))
	}
	second := binary.BigEndian.Uint64(data[latestBinaryAccessorHeaderSize+8 : latestBinaryAccessorHeaderSize+16])
	third := binary.BigEndian.Uint64(data[latestBinaryAccessorHeaderSize+16 : latestBinaryAccessorHeaderSize+24])
	if second+1 >= third {
		t.Fatalf("test accessor offsets too close: second=%d third=%d", second, third)
	}
	binary.BigEndian.PutUint64(data[latestBinaryAccessorHeaderSize+8:latestBinaryAccessorHeaderSize+16], second+1)
	updateSnapshotRefFileForTest(t, dir, &accessorRef, data)
	if err := CheckLatestAccessorSegment(dir, accessorRef); err != nil {
		t.Fatalf("single-file accessor check should still pass: %v", err)
	}

	manifest = NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	_, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || !strings.Contains(err.Error(), "want segment record offset") {
		t.Fatalf("VerifyLoadedManifestFiles stale latest accessor offset err = %v, want record offset mismatch", err)
	}
}

func TestVerifyLoadedManifestFilesRejectsStaleLatestBinaryBTree(t *testing.T) {
	dir := t.TempDir()
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, dir)
	manifest := NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	corruptLatestBinaryCompanionSegmentChecksum(t, dir, &btreeRef)
	if err := CheckLatestBTreeSegment(dir, btreeRef); err != nil {
		t.Fatalf("single-file btree check should still pass: %v", err)
	}

	manifest = NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	_, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || !strings.Contains(err.Error(), "segment checksum mismatch") {
		t.Fatalf("VerifyLoadedManifestFiles stale latest btree err = %v, want segment checksum mismatch", err)
	}
}

func TestVerifyLoadedManifestFilesRejectsLatestBinaryBTreeOrdinalMismatch(t *testing.T) {
	dir := t.TempDir()
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, dir)
	manifest := NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyLoadedManifestFiles before corruption: %v", err)
	}

	data := mustReadFile(t, filepath.Join(dir, btreeRef.Path))
	if len(data) < latestBinaryBTreeHeaderSize+8 {
		t.Fatalf("btree data length = %d, want first entry offset", len(data))
	}
	entryOffset := binary.BigEndian.Uint64(data[latestBinaryBTreeHeaderSize : latestBinaryBTreeHeaderSize+8])
	if entryOffset+12 > uint64(len(data)) {
		t.Fatalf("btree first entry offset %d outside data length %d", entryOffset, len(data))
	}
	binary.BigEndian.PutUint64(data[entryOffset+4:entryOffset+12], 1)
	updateSnapshotRefFileForTest(t, dir, &btreeRef, data)
	if err := CheckLatestBTreeSegment(dir, btreeRef); err == nil || !strings.Contains(err.Error(), "ordinal=1, want 0") {
		t.Fatalf("single-file btree check err = %v, want ordinal mismatch", err)
	}

	manifest = NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})
	_, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true})
	if err == nil || !strings.Contains(err.Error(), "ordinal=1, want 0") {
		t.Fatalf("VerifyLoadedManifestFiles stale latest btree ordinal err = %v, want ordinal mismatch", err)
	}
}

func TestVerifyManifestLatestBinarySidecarsRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, dir)
	accessorRef.Checksum = badSnapshotChecksum()
	manifest := NewManifest(1, 10, []SegmentRef{segRef, accessorRef, btreeRef})

	err := verifyManifestLatestBinarySidecars(dir, manifest)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("verifyManifestLatestBinarySidecars err = %v, want checksum mismatch", err)
	}
}

func TestRestoreSnapshotFromVerifiedManifestWritesInstallProgress(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	eventRef := writeVerifiableEventLogSegment(t, dir, 1, 2)
	eventIndexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	manifest.Segments = append(manifest.Segments, eventRef, eventIndexRef)
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest with event log: %v", err)
	}
	restored := rawdb.NewMemoryDatabase()

	result, err := RestoreSnapshotFromVerifiedManifestWithOptions(restored, dir, identity, strictBoundaryOptions(t, restored))
	if err != nil {
		t.Fatalf("RestoreSnapshotFromVerifiedManifest: %v", err)
	}
	if result.RestoredTxNum != 12 || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
		t.Fatalf("result = %+v, want txNum=12, 2 changes, 2 tx ranges", result)
	}
	if result.Progress == nil || result.Progress.HistoryBuildTxNum != 12 || result.Progress.AccessorBuildTxNum != 12 {
		t.Fatalf("progress = %+v, want history/accessor progress at 12", result.Progress)
	}
	for _, tc := range []struct {
		stage rawdb.StageID
		want  uint64
	}{
		{stage: rawdb.StageSnapshotInstall, want: 12},
		{stage: rawdb.StageSnapshotHistory, want: 12},
		{stage: rawdb.StageSnapshotAccessor, want: 12},
		{stage: rawdb.StageSnapshotEventLogBuild, want: 2},
	} {
		got, ok, err := rawdb.ReadStageProgress(restored, tc.stage)
		if err != nil || !ok || got != tc.want {
			t.Fatalf("%s progress = %d ok=%v err=%v, want %d", tc.stage, got, ok, err, tc.want)
		}
	}
	if _, ok, err := rawdb.ReadStageProgress(restored, rawdb.StageHeaders); err != nil || ok {
		t.Fatalf("canonical Headers stage should not be advanced by snapshot install: ok=%v err=%v", ok, err)
	}
}

func badSnapshotChecksum() string {
	return "sha256:" + strings.Repeat("00", sha256.Size)
}

func writeLatestBinaryCompanionManifestForTest(t *testing.T, dir string) (SegmentRef, SegmentRef, SegmentRef) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x64}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemDynamicProperty, []byte(key), []byte("value-"+key)); err != nil {
			t.Fatalf("write kv latest %q: %v", key, err)
		}
	}
	segRef, accessorRef, btreeRef, err := BuildLatestDomainSegmentFilesFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 10, "latest/system-dp.seg")
	if err != nil {
		t.Fatalf("BuildLatestDomainSegmentFilesFromDB: %v", err)
	}
	return segRef, accessorRef, btreeRef
}

func corruptLatestBinaryCompanionSegmentChecksum(t *testing.T, dir string, ref *SegmentRef) {
	t.Helper()
	data := mustReadFile(t, filepath.Join(dir, ref.Path))
	checksumStart := latestBinaryAccessorHeaderSize - sha256.Size
	if len(data) < checksumStart+sha256.Size {
		t.Fatalf("latest binary companion %q length = %d, want checksum header", ref.Path, len(data))
	}
	for i := 0; i < sha256.Size; i++ {
		data[checksumStart+i] ^= 0xff
	}
	size, checksum := latestBinaryMetadata(data)
	ref.Size = size
	ref.Checksum = checksum
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0o644); err != nil {
		t.Fatalf("write stale latest binary companion %q: %v", ref.Path, err)
	}
}

func updateSnapshotRefFileForTest(t *testing.T, dir string, ref *SegmentRef, data []byte) {
	t.Helper()
	size, checksum := latestBinaryMetadata(data)
	ref.Size = size
	ref.Checksum = checksum
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0o644); err != nil {
		t.Fatalf("write snapshot ref %q: %v", ref.Path, err)
	}
}

func TestWriteSnapshotInstallProgressCapsEventLogBuildAtContinuousCoverage(t *testing.T) {
	dir := t.TempDir()
	ref1 := writeVerifiableEventLogSegment(t, dir, 1, 2)
	index1, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	ref4 := writeVerifiableEventLogSegment(t, dir, 4, 4)
	manifest := NewManifest(0, 0, []SegmentRef{ref1, index1, ref4})
	db := rawdb.NewMemoryDatabase()
	block2Hash := common.Hash{0x02}
	if err := rawdb.WriteStateTxRange(db, 2, block2Hash, 2, 2); err != nil {
		t.Fatalf("WriteStateTxRange block2: %v", err)
	}

	if _, err := WriteSnapshotInstallProgress(db, manifest); err != nil {
		t.Fatalf("WriteSnapshotInstallProgress: %v", err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != 2 {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want 2", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block2Hash {
		t.Fatalf("StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, block2Hash)
	}
}

func TestWriteSnapshotInstallProgressCombinesAdjacentEventLogIndexes(t *testing.T) {
	dir := t.TempDir()
	ref1 := writeVerifiableEventLogSegment(t, dir, 1, 2)
	index1, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments 1: %v", err)
	}
	ref2 := writeVerifiableEventLogSegment(t, dir, 3, 4)
	index2, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref2}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments 2: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref1, index1, ref2, index2})
	db := rawdb.NewMemoryDatabase()
	block4Hash := common.Hash{0x04}
	if err := rawdb.WriteStateTxRange(db, 4, block4Hash, 4, 4); err != nil {
		t.Fatalf("WriteStateTxRange block4: %v", err)
	}

	if _, err := WriteSnapshotInstallProgress(db, manifest); err != nil {
		t.Fatalf("WriteSnapshotInstallProgress: %v", err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != 4 {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want 4", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block4Hash {
		t.Fatalf("StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, block4Hash)
	}
}

func TestWriteSnapshotInstallProgressRequiresEventLogCoverageFromBlockOne(t *testing.T) {
	dir := t.TempDir()
	ref := writeVerifiableEventLogSegment(t, dir, 10, 12)
	index, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref, index})
	db := rawdb.NewMemoryDatabase()

	if _, err := WriteSnapshotInstallProgress(db, manifest); err != nil {
		t.Fatalf("WriteSnapshotInstallProgress: %v", err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || ok {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want absent for event-log coverage starting after block 1", got, ok, err)
	}
}

func TestWriteSnapshotInstallProgressRequiresEventLogIndexCoverage(t *testing.T) {
	dir := t.TempDir()
	ref := writeVerifiableEventLogSegment(t, dir, 1, 2)
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	db := rawdb.NewMemoryDatabase()

	if _, err := WriteSnapshotInstallProgress(db, manifest); err != nil {
		t.Fatalf("WriteSnapshotInstallProgress: %v", err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || ok {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want absent without event-log-index", got, ok, err)
	}
}

func TestWriteSnapshotInstallProgressPreflightsEventLogBoundaryBeforeWritingStages(t *testing.T) {
	dir := t.TempDir()
	eventRef := writeVerifiableEventLogSegment(t, dir, 1, 2)
	eventIndexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	manifest := NewManifest(0, 12, []SegmentRef{
		{
			Dataset:   SegmentDatasetStateDomainChange,
			Kind:      SegmentHistory,
			FromTxNum: 1,
			ToTxNum:   12,
			Path:      "history/state-domain-change-1-12.seg",
		},
		eventRef,
		eventIndexRef,
	})
	db := rawdb.NewMemoryDatabase()

	_, err = WriteSnapshotInstallProgress(db, manifest)
	if err == nil || !strings.Contains(err.Error(), string(rawdb.StageSnapshotEventLogBuild)) {
		t.Fatalf("WriteSnapshotInstallProgress err = %v, want event-log boundary error", err)
	}
	for _, stage := range []rawdb.StageID{
		rawdb.StageSnapshotInstall,
		rawdb.StageSnapshotHistory,
		rawdb.StageSnapshotEventLogBuild,
	} {
		if got, ok, readErr := rawdb.ReadStageProgress(db, stage); readErr != nil || ok {
			t.Fatalf("%s progress = %d ok=%v err=%v, want absent after failed preflight", stage, got, ok, readErr)
		}
	}
}

func writeVerifiableEventLogSegment(t *testing.T, dir string, fromBlock, toBlock uint64) SegmentRef {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		block, infos := eventLogTestBlock(t, blockNum, nil)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	ref, err := BuildEventLogSegmentFromChain(db, dir, "", fromBlock, toBlock)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain %d..%d: %v", fromBlock, toBlock, err)
	}
	return ref
}

func TestRestoreSnapshotFromVerifiedManifestVerifiesCommitmentBoundary(t *testing.T) {
	dir, identity, root, _ := writeVerifiableCommitmentManifest(t)
	restored := rawdb.NewMemoryDatabase()

	result, err := RestoreSnapshotFromVerifiedManifest(restored, dir, identity)
	if err != nil {
		t.Fatalf("RestoreSnapshotFromVerifiedManifest: %v", err)
	}
	if result.RestoredTxNum != 10 || result.Progress == nil || result.Progress.CommitmentFlushTxNum != 10 {
		t.Fatalf("result = %+v, want commitment flush progress at 10", result)
	}
	gotRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(restored)
	if err != nil || !ok || gotRoot != root {
		t.Fatalf("restored commitment root = %x ok=%v err=%v, want %x", gotRoot, ok, err, root)
	}
	checkpoint, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(restored)
	if err != nil || !ok || checkpoint.Root != root || checkpoint.BlockNum != 7 {
		t.Fatalf("restored latest checkpoint = %+v ok=%v err=%v", checkpoint, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(restored, rawdb.StageSnapshotCommitmentFlush); err != nil || !ok || got != 10 {
		t.Fatalf("SnapshotCommitmentFlush progress = %d ok=%v err=%v, want 10", got, ok, err)
	}
}

func TestVerifyRestoredSnapshotBoundaryRejectsMismatchedCommitmentMetadata(t *testing.T) {
	dir, _, root, owner := writeVerifiableCommitmentManifest(t)
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	manifest := mgr.Manifest()
	restored := rawdb.NewMemoryDatabase()
	if err := mgr.RestoreLatest(restored, manifest.VisibleTxEnd); err != nil {
		t.Fatalf("RestoreLatest: %v", err)
	}

	if err := rawdb.WriteLatestDomainCommitmentRoot(restored, common.Hash{0xee}); err != nil {
		t.Fatalf("overwrite root: %v", err)
	}
	if err := VerifyRestoredSnapshotBoundary(restored, mgr, manifest); err == nil || !strings.Contains(err.Error(), "commitment root") {
		t.Fatalf("root mismatch error = %v, want commitment root mismatch", err)
	}

	if err := rawdb.WriteLatestDomainCommitmentRoot(restored, root); err != nil {
		t.Fatalf("restore root: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(restored, owner, []byte("tampered-account")); err != nil {
		t.Fatalf("tamper latest account: %v", err)
	}
	if err := VerifyRestoredSnapshotBoundaryWithOptions(restored, mgr, manifest, strictBoundaryOptions(t, restored).Boundary); err == nil || !strings.Contains(err.Error(), "rebuilt latest commitment root") {
		t.Fatalf("latest-row tamper error = %v, want rebuilt commitment root mismatch", err)
	}
	if err := mgr.RestoreLatest(restored, manifest.VisibleTxEnd); err != nil {
		t.Fatalf("RestoreLatest after tamper: %v", err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(restored, root); err != nil {
		t.Fatalf("restore root after tamper: %v", err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(restored, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  7,
		BlockHash: common.Hash{0x07},
		Root:      common.Hash{0xdd},
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("overwrite checkpoint: %v", err)
	}
	if err := VerifyRestoredSnapshotBoundary(restored, mgr, manifest); err == nil || !strings.Contains(err.Error(), "commitment checkpoint") {
		t.Fatalf("checkpoint mismatch error = %v, want checkpoint mismatch", err)
	}
}

func writeVerifiableBranchManifest(t *testing.T) (string, ChainIdentity) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	seedStagedBranchRows(t, db)

	dir := t.TempDir()
	ref, err := BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branches-10-10.json", 10, 10)
	if err != nil {
		t.Fatalf("build branch segment: %v", err)
	}
	identity := ChainIdentity{
		ChainID:        1,
		NetworkID:      11111,
		GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if err := PublishManifest(dir, NewManifestForChain(10, 10, []SegmentRef{ref}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	return dir, identity
}

func writeVerifiableHistoryManifest(t *testing.T) (string, ChainIdentity, common.Address) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x77}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 11); err != nil {
		t.Fatalf("write state tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 12, 12); err != nil {
		t.Fatalf("write no-change state tx range: %v", err)
	}
	for _, change := range []*rawdb.StateDomainChange{
		{
			BlockNum:   1,
			BlockHash:  common.Hash{0x01},
			TxNum:      10,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot/a"),
			PrevExists: true,
			Prev:       []byte("old-a"),
			NextExists: true,
			Next:       []byte("new-a"),
		},
		{
			BlockNum:   1,
			BlockHash:  common.Hash{0x01},
			TxNum:      11,
			Seq:        2,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot/b"),
			NextExists: true,
			Next:       []byte("new-b"),
		},
	} {
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write state change: %v", err)
		}
	}

	dir := t.TempDir()
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatalf("build binary history: %v", err)
	}
	identity := ChainIdentity{
		ChainID:        1,
		NetworkID:      11111,
		GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if err := PublishManifest(dir, NewManifestForChain(10, 12, refs, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	return dir, identity, owner
}

func writeVerifiableCommitmentManifest(t *testing.T) (string, ChainIdentity, common.Hash, common.Address) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x88}, common.AccountIDLength)...))
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-latest")); err != nil {
		t.Fatalf("write account latest: %v", err)
	}
	root, err := statedomains.NewStagedCommitmentStore(db).Rebuild()
	if err != nil {
		t.Fatalf("rebuild commitment root: %v", err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  7,
		BlockHash: common.Hash{0x07},
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	dir := t.TempDir()
	accountRef, accountAccessorRef, accountBTreeRef, err := BuildAccountLatestSegmentFilesFromDB(db, dir, 10, 10, "latest/accounts-10-10.seg")
	if err != nil {
		t.Fatalf("build account latest segment: %v", err)
	}
	rootRef, rootAccessorRef, rootBTreeRef, err := BuildCommitmentRootSegmentFilesFromDB(db, dir, 10, 10, "commitment/root-10-10.seg")
	if err != nil {
		t.Fatalf("build root segment: %v", err)
	}
	checkpointRef, checkpointAccessorRef, checkpointBTreeRef, err := BuildCommitmentCheckpointSegmentFilesFromDB(db, dir, 10, 10, "commitment/checkpoints-10-10.seg")
	if err != nil {
		t.Fatalf("build checkpoint segment: %v", err)
	}
	identity := ChainIdentity{
		ChainID:        1,
		NetworkID:      11111,
		GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	refs := []SegmentRef{accountRef, accountAccessorRef, accountBTreeRef, rootRef, rootAccessorRef, rootBTreeRef, checkpointRef, checkpointAccessorRef, checkpointBTreeRef}
	if err := PublishManifest(dir, NewManifestForChain(10, 10, refs, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	return dir, identity, root, owner
}

func strictBoundaryOptions(t *testing.T, db statedomains.CommitmentDB) RestoreVerifiedSnapshotOptions {
	t.Helper()
	return RestoreVerifiedSnapshotOptions{
		Boundary: VerifyRestoredSnapshotBoundaryOptions{
			RequireIndependentCommitmentRoot: true,
			RebuildCommitmentRoot: func() (common.Hash, error) {
				return statedomains.NewStagedCommitmentStore(db).Rebuild()
			},
		},
	}
}
