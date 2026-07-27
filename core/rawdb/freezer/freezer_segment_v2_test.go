package freezer

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestV2SegmentRoundTripAndChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bodies", v2SegmentName(100, 150))
	records := make(map[uint64][]byte)
	for number := uint64(100); number < 250; number++ {
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], number)
		records[number] = append(bytes.Repeat([]byte("repeated-v2-record"), int(number%11)), suffix[:]...)
	}
	read := func(number uint64) ([]byte, error) { return records[number], nil }
	if err := writeV2Segment(path, 100, 150, 16, read); err != nil {
		t.Fatalf("writeV2Segment: %v", err)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatalf("openV2Segment: %v", err)
	}
	store := newTestV2Store(t, "bodies", reader)
	for number := uint64(100); number < 250; number++ {
		got, err := store.read("bodies", number)
		if err != nil {
			t.Fatalf("read %d: %v", number, err)
		}
		if !bytes.Equal(got, records[number]) {
			t.Fatalf("record %d mismatch", number)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err = openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	first := reader.frames[0]
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	if _, err := file.ReadAt(one, int64(first.compressedStart)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one, int64(first.compressedStart)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err = openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	store = newTestV2Store(t, "bodies", reader)
	if _, err := store.read("bodies", 100); err == nil {
		t.Fatal("corrupt frame passed checksum validation")
	}
	_ = store.Close()
}

func TestV2LoadFrameReusesCompressedBuffer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bodies", v2SegmentName(0, 64))
	read := func(number uint64) ([]byte, error) {
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], number)
		return append(bytes.Repeat([]byte{byte(number), byte(number >> 8), 0xa5}, 1024), suffix[:]...), nil
	}
	if err := writeV2Segment(path, 0, 64, 64, read); err != nil {
		t.Fatal(err)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	store := newTestV2Store(t, "bodies", reader)
	defer store.Close()

	if _, err := store.loadFrame(reader, 0); err != nil {
		t.Fatal(err)
	}
	first := <-store.compressedBuffers
	if cap(first.data) == 0 {
		t.Fatal("pooled compressed buffer has zero capacity")
	}
	firstByte := &first.data[:cap(first.data)][0]
	store.compressedBuffers <- first

	if _, err := store.loadFrame(reader, 0); err != nil {
		t.Fatal(err)
	}
	second := <-store.compressedBuffers
	secondByte := &second.data[:cap(second.data)][0]
	store.compressedBuffers <- second
	if secondByte != firstByte {
		t.Fatal("second frame load did not reuse compressed buffer backing")
	}
}

func TestV2CompressedBufferPoolIsStrictlyBounded(t *testing.T) {
	store := &v2Store{compressedBuffers: make(chan *v2CompressedBuffer, v2DecoderConcurrency)}
	store.releaseCompressedBuffer(&v2CompressedBuffer{data: make([]byte, v2MaxPooledCompressed+1)})
	if got := len(store.compressedBuffers); got != 0 {
		t.Fatalf("oversized retained buffers = %d, want 0", got)
	}
	for i := 0; i < v2DecoderConcurrency+2; i++ {
		store.releaseCompressedBuffer(&v2CompressedBuffer{data: make([]byte, 1)})
	}
	if got := len(store.compressedBuffers); got != v2DecoderConcurrency {
		t.Fatalf("retained buffers = %d, want %d", got, v2DecoderConcurrency)
	}
}

func TestFreezerMigrateV2RoundTripResumeAndAppend(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true},
	}
	want := make(map[string][][]byte)
	for kind := range tables {
		want[kind] = make([][]byte, 300)
	}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 300; number++ {
			var suffix [8]byte
			binary.BigEndian.PutUint64(suffix[:], number)
			for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
				value := append(bytes.Repeat([]byte(kind+"-repeated-"), 20+int(number%7)), suffix[:]...)
				want[kind][number] = append([]byte(nil), value...)
				if err := op.AppendRaw(kind, number, value); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatal(err)
	}
	result, err := freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   2,
	})
	if err != nil {
		t.Fatalf("first migration: %v", err)
	}
	if result.End != 128 || result.Segments != 2 || freezer.v2.coverage != 128 {
		t.Fatalf("first result = %+v, coverage=%d", result, freezer.v2.coverage)
	}
	rangeBodies, err := freezer.AncientRange("bodies", 120, 16, 0)
	if err != nil || len(rangeBodies) != 16 {
		t.Fatalf("cross-tier range len=%d err=%v", len(rangeBodies), err)
	}
	for i, body := range rangeBodies {
		if !bytes.Equal(body, want["bodies"][120+i]) {
			t.Fatalf("cross-tier range item %d mismatch", i)
		}
	}
	maxBytes := uint64(len(want["bodies"][120]) + len(want["bodies"][121]) - 1)
	limitedBodies, err := freezer.AncientRange("bodies", 120, 16, maxBytes)
	if err != nil || len(limitedBodies) != 1 || !bytes.Equal(limitedBodies[0], want["bodies"][120]) {
		t.Fatalf("limited V2 range len=%d err=%v", len(limitedBodies), err)
	}
	rangeRoots, err := freezer.AncientRange("state_roots", 0, 16, 0)
	if err != nil || len(rangeRoots) != 16 {
		t.Fatalf("V1-only range len=%d err=%v", len(rangeRoots), err)
	}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		for number := uint64(0); number < 300; number++ {
			got, err := freezer.Ancient(kind, number)
			if err != nil {
				t.Fatalf("read %s[%d]: %v", kind, number, err)
			}
			if !bytes.Equal(got, want[kind][number]) {
				t.Fatalf("read %s[%d] mismatch", kind, number)
			}
		}
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}

	freezer, err = NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("reopen mixed freezer: %v", err)
	}
	result, err = freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
	})
	if err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if result.End != 256 || result.Segments != 2 || freezer.v2.coverage != 256 {
		t.Fatalf("resume result = %+v, coverage=%d", result, freezer.v2.coverage)
	}
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for kind := range tables {
			if err := op.AppendRaw(kind, 300, []byte("appended-"+kind)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	for _, kind := range []string{"bodies", "tx_infos"} {
		got, err := freezer.Ancient(kind, 300)
		if err != nil || string(got) != "appended-"+kind {
			t.Fatalf("appended %s = %q, err=%v", kind, got, err)
		}
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRewriteV2TransactionInfosRoundTripAndResume(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":   {Prunable: true},
		"tx_infos": {Prunable: true},
	}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	const rows = uint64(32)
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < rows; number++ {
			if err := op.AppendRaw("bodies", number, []byte(fmt.Sprintf("body-%d", number))); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, []byte(fmt.Sprintf("receipt-%d-DUPLICATE", number))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := freezer.MigrateV2(V2MigrationOptions{
		Tables: []string{"bodies", "tx_infos"}, SegmentBlocks: 16, FrameBlocks: 4,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = freezer.RewriteV2TransactionInfos(V2TxInfoRewriteOptions{
		MaxSegments: 1,
		Transform: func(_ uint64, txInfo, _ []byte) ([]byte, uint64, error) {
			compact := bytes.TrimSuffix(txInfo, []byte("-DUPLICATE"))
			return compact, uint64(len(txInfo) - len(compact)), nil
		},
		BeforePublish: func() error { return errors.New("injected prerequisite failure") },
	})
	if err == nil {
		t.Fatal("rewrite published despite prerequisite failure")
	}
	if got, readErr := freezer.Ancient("tx_infos", 0); readErr != nil || string(got) != "receipt-0-DUPLICATE" {
		t.Fatalf("failed rewrite changed active data: %q err=%v", got, readErr)
	}
	result, err := freezer.RewriteV2TransactionInfos(V2TxInfoRewriteOptions{
		MaxSegments: 1,
		Transform: func(_ uint64, txInfo, _ []byte) ([]byte, uint64, error) {
			compact := bytes.TrimSuffix(txInfo, []byte("-DUPLICATE"))
			return compact, uint64(len(txInfo) - len(compact)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments != 1 || result.Rows != 16 || result.RemovedBytes != 16*10 {
		t.Fatalf("first rewrite = %+v", result)
	}
	for number := uint64(0); number < rows; number++ {
		got, err := freezer.Ancient("tx_infos", number)
		if err != nil {
			t.Fatal(err)
		}
		wantSuffix := "-DUPLICATE"
		if number < 16 {
			wantSuffix = ""
		}
		want := fmt.Sprintf("receipt-%d%s", number, wantSuffix)
		if string(got) != want {
			t.Fatalf("tx_infos[%d]=%q, want %q", number, got, want)
		}
	}
	result, err = freezer.RewriteV2TransactionInfos(V2TxInfoRewriteOptions{
		Transform: func(_ uint64, txInfo, _ []byte) ([]byte, uint64, error) {
			compact := bytes.TrimSuffix(txInfo, []byte("-DUPLICATE"))
			return compact, uint64(len(txInfo) - len(compact)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments != 1 || result.Rows != 16 || result.RemovedBytes != 16*10 {
		t.Fatalf("resume rewrite = %+v", result)
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}
	freezer, err = NewFreezer(dir, "", true, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer freezer.Close()
	for number := uint64(0); number < rows; number++ {
		got, err := freezer.Ancient("tx_infos", number)
		if err != nil || string(got) != fmt.Sprintf("receipt-%d", number) {
			t.Fatalf("reopen tx_infos[%d]=%q err=%v", number, got, err)
		}
	}
}

func TestFreezerMigrateV2OnlineKeepsConcurrentReadsAvailable(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":   {Prunable: true},
		"tx_infos": {Prunable: true},
	}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer freezer.Close()
	const rows = 512
	want := make(map[string][][]byte, len(tables))
	for kind := range tables {
		want[kind] = make([][]byte, rows)
	}
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < rows; number++ {
			for kind := range tables {
				value := []byte(fmt.Sprintf("%s-%06d-%s", kind, number, bytes.Repeat([]byte("x"), int(number%31))))
				want[kind][number] = append([]byte(nil), value...)
				if err := op.AppendRaw(kind, number, value); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	readErr := make(chan error, 1)
	var readers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		readers.Add(1)
		go func() {
			defer readers.Done()
			for iteration := uint64(0); ; iteration++ {
				select {
				case <-stop:
					return
				default:
				}
				number := (iteration*37 + uint64(worker)) % rows
				for kind := range tables {
					got, err := freezer.Ancient(kind, number)
					if err != nil || !bytes.Equal(got, want[kind][number]) {
						select {
						case readErr <- fmt.Errorf("read %s[%d]: match=%v err=%v", kind, number, bytes.Equal(got, want[kind][number]), err):
						default:
						}
						return
					}
				}
			}
		}()
	}
	result, migrateErr := freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   8,
		Online:        true,
	})
	close(stop)
	readers.Wait()
	if migrateErr != nil {
		t.Fatalf("online migration: %v", migrateErr)
	}
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
	if result.End != rows || freezer.V2Coverage() != rows {
		t.Fatalf("result=%+v coverage=%d", result, freezer.V2Coverage())
	}
}

func TestFreezerMigrateV2CancellationDoesNotPublish(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{"bodies": {Prunable: true}}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer freezer.Close()
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 64; number++ {
			if err := op.AppendRaw("bodies", number, bytes.Repeat([]byte("cancel"), 20)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		Online:        true,
		Context:       ctx,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if coverage := freezer.V2Coverage(); coverage != 0 {
		t.Fatalf("coverage=%d after cancellation", coverage)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "v2", "manifests"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("published manifests after cancellation: %d", len(entries))
	}
}

func newTestV2Store(t *testing.T, kind string, reader *v2SegmentReader) *v2Store {
	t.Helper()
	decoder, err := newV2Decoder()
	if err != nil {
		t.Fatal(err)
	}
	return &v2Store{
		segments:          map[string][]*v2SegmentReader{kind: {reader}},
		decoder:           decoder,
		cacheList:         list.New(),
		cacheItems:        make(map[v2FrameKey]*list.Element),
		cacheLoads:        make(map[v2FrameKey]*v2FrameLoad),
		cacheLimit:        v2DefaultCacheBytes,
		compressedBuffers: make(chan *v2CompressedBuffer, v2DecoderConcurrency),
	}
}
