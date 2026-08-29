package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func commitmentPointTestPrefix(ordinal uint64) []byte {
	prefix := make([]byte, 16)
	for i := len(prefix) - 1; i >= 0; i-- {
		prefix[i] = byte(ordinal & 0x0f)
		ordinal >>= 4
	}
	return prefix
}

func commitmentPointTestValue(ordinal int) []byte {
	value := make([]byte, 530)
	if ordinal%2 == 0 {
		// Exercise the compressed frame path.
		for i := range value {
			value[i] = byte(ordinal)
		}
		return value
	}
	// Exercise the uncompressed/owned-copy path.
	binary.BigEndian.PutUint64(value[:8], uint64(ordinal))
	x := uint64(ordinal)*0x9e3779b97f4a7c15 + 1
	for i := 8; i < len(value); i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		value[i] = byte(x)
	}
	return value
}

func openCommitmentPointTestView(t *testing.T, rows int) (pointread.CommitmentBranchSnapshotView, [][]byte) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	want := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		want[i] = commitmentPointTestValue(i)
		if err := rawdb.WriteCommitmentBranch(db, commitmentPointTestPrefix(uint64(i)), want[i]); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	segment, accessor, btree, err := BuildCommitmentBranchSegmentFilesFromDB(db, dir, "commitment/point-view.seg", 1, 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishManifest(dir, NewManifest(1, 9, []SegmentRef{segment, accessor, btree})); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	view, ok, err := mgr.OpenCommitmentBranchSnapshot(9)
	if err != nil || !ok || view == nil {
		t.Fatalf("OpenCommitmentBranchSnapshot = view=%v ok=%v err=%v", view, ok, err)
	}
	return view, want
}

func TestCommitmentBranchPointViewResidentIndexMultiBlock(t *testing.T) {
	const rows = 3*int(latestBinaryBTreeBlockSize) + 17
	view, want := openCommitmentPointTestView(t, rows)
	defer view.Close()
	resident := view.(*CommitmentBranchPointView)
	wantIndex := (rows + int(latestBinaryBTreeBlockSize) - 1) / int(latestBinaryBTreeBlockSize)
	if len(resident.index) != wantIndex {
		t.Fatalf("resident entries = %d, want %d", len(resident.index), wantIndex)
	}
	if resident.maxBlockBytes <= 0 || resident.maxBlockBytes > commitmentBranchPointMaxBlockBytes {
		t.Fatalf("resident max block = %d", resident.maxBlockBytes)
	}
	wantScratchCapacity := commitmentBranchPointScratchPoolCapacity(resident.maxBlockBytes)
	if cap(resident.scratchPool) != wantScratchCapacity {
		t.Fatalf("scratch pool capacity = %d, want %d", cap(resident.scratchPool), wantScratchCapacity)
	}
	for i := 0; i < rows; i++ {
		got, ok, err := view.Get(commitmentPointTestPrefix(uint64(i)))
		if err != nil || !ok || !bytes.Equal(got, want[i]) {
			t.Fatalf("Get(%d) = %x ok=%v err=%v, want %x", i, got, ok, err, want[i])
		}
	}
	for _, missing := range [][]byte{
		nil,
		append(commitmentPointTestPrefix(5), 0),
		commitmentPointTestPrefix(uint64(rows + 100)),
	} {
		if got, ok, err := view.Get(missing); err != nil || ok || got != nil {
			t.Fatalf("Get(missing %x) = %x ok=%v err=%v", missing, got, ok, err)
		}
	}

	// Returned bytes must not alias the next pooled block read.
	retained, ok, err := view.Get(commitmentPointTestPrefix(1))
	if err != nil || !ok {
		t.Fatal(err)
	}
	retainedCopy := append([]byte(nil), retained...)
	for i := 2; i < rows; i++ {
		if _, _, err := view.Get(commitmentPointTestPrefix(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(retained, retainedCopy) {
		t.Fatal("returned uncompressed value aliased pooled scratch")
	}

	entryBytes := uintptr(len(resident.index)) * unsafe.Sizeof(latestBinaryBTreeEntry{})
	residentBytes := entryBytes + uintptr(cap(resident.keyArena))
	retainedScratchBytes := uintptr(cap(resident.scratchPool) * resident.maxBlockBytes)
	t.Logf("resident index bytes=%d retained scratch upper bound=%d max block=%d", residentBytes, retainedScratchBytes, resident.maxBlockBytes)
}

func TestCommitmentBranchPointScratchPoolCapacityBoundsRetainedBytes(t *testing.T) {
	for _, tc := range []struct {
		blockBytes int
		want       int
	}{
		{blockBytes: 0, want: 0},
		{blockBytes: 64 << 10, want: 32},
		{blockBytes: 2 << 20, want: 16},
		{blockBytes: commitmentBranchPointMaxBlockBytes, want: 4},
	} {
		got := commitmentBranchPointScratchPoolCapacity(tc.blockBytes)
		if got != tc.want {
			t.Fatalf("capacity(%d) = %d, want %d", tc.blockBytes, got, tc.want)
		}
		if got > 0 && got*tc.blockBytes > commitmentBranchPointMaxRetainedScratchBytes {
			t.Fatalf("capacity(%d) retains %d bytes", tc.blockBytes, got*tc.blockBytes)
		}
	}
}

func TestCommitmentBranchPointViewConcurrentGetAndClose(t *testing.T) {
	const rows = 2*int(latestBinaryBTreeBlockSize) + 11
	view, want := openCommitmentPointTestView(t, rows)
	resident := view.(*CommitmentBranchPointView)

	const workers = 64
	start := make(chan struct{})
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ordinal := worker % rows
			got, ok, err := view.Get(commitmentPointTestPrefix(uint64(ordinal)))
			if err != nil && !errors.Is(err, errCommitmentBranchPointViewClosed) {
				errCh <- err
				return
			}
			if err == nil && (!ok || !bytes.Equal(got, want[ordinal])) {
				errCh <- &commitmentPointTestError{ordinal: ordinal, found: ok}
			}
		}()
	}
	close(start)
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
	if _, _, err := view.Get(commitmentPointTestPrefix(0)); err == nil {
		t.Fatal("Get after Close succeeded")
	}
	if resident.segment != nil || resident.index != nil || resident.keyArena != nil || resident.scratchPool != nil {
		t.Fatal("Close retained resident descriptors or buffers")
	}
}

type commitmentPointTestError struct {
	ordinal int
	found   bool
}

func (e *commitmentPointTestError) Error() string {
	return "commitment point concurrent read returned an incorrect result"
}
