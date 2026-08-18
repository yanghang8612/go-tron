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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
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

func TestV2BodiesDictionaryRoundTripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bodies", v2SegmentName(0, 32))
	records := make([][]byte, 32)
	for number := range records {
		prefix := bytes.Repeat([]byte("tron-block-body-contract-transfer-"), 600)
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], uint64(number))
		records[number] = append(prefix, suffix[:]...)
	}
	read := func(number uint64) ([]byte, error) {
		return records[number], nil
	}
	if err := writeV2TableSegment(path, "bodies", 0, uint64(len(records)), 8, read, read); err != nil {
		t.Fatal(err)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	if reader.codec != v2CodecBodiesTrainedDict || len(reader.dictionary) < v2BodiesDictBytes || len(reader.dictionary) > v2BodiesDictMaxBytes {
		_ = reader.Close()
		t.Fatalf("body codec=%d dictionary=%d, want codec=%d dictionary=[%d,%d]", reader.codec, len(reader.dictionary), v2CodecBodiesTrainedDict, v2BodiesDictBytes, v2BodiesDictMaxBytes)
	}
	inspected, err := zstd.InspectDictionary(reader.dictionary)
	if err != nil || inspected.ID() != v2DictionaryID(0) {
		_ = reader.Close()
		t.Fatalf("trained dictionary inspection id=%v err=%v", inspected, err)
	}
	store := newTestV2Store(t, "bodies", reader)
	decoder, err := newV2DecoderForSegments(store.segments)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	store.decoder.Close()
	store.decoder = decoder
	for number, want := range records {
		got, err := store.read("bodies", uint64(number))
		if err != nil || !bytes.Equal(got, want) {
			_ = store.Close()
			t.Fatalf("body %d round trip err=%v equal=%v", number, err, bytes.Equal(got, want))
		}
	}
	frameCount := len(reader.frames)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	dictionaryOffset := int64(v2HeaderSize + frameCount*v2FrameEntrySize)
	one := []byte{0}
	if _, err := file.ReadAt(one, dictionaryOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one, dictionaryOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openV2Segment(path, "bodies"); err == nil || !strings.Contains(err.Error(), "dictionary checksum mismatch") {
		t.Fatalf("corrupt body dictionary error=%v, want checksum mismatch", err)
	}
}

func TestV2BodiesCodecIsNotAppliedToOtherTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tx_infos", v2SegmentName(0, 8))
	if err := writeV2TableSegment(path, "tx_infos", 0, 8, 4, func(number uint64) ([]byte, error) {
		return []byte(fmt.Sprintf("receipt-%d", number)), nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	reader, err := openV2Segment(path, "tx_infos")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.codec != v2CodecDefault || len(reader.dictionary) != 0 {
		t.Fatalf("tx_infos codec=%d dictionary=%d, want default without dictionary", reader.codec, len(reader.dictionary))
	}
}

func TestV2BodiesTrainingUsesSideEffectFreeReader(t *testing.T) {
	const records = uint64(512)
	path := filepath.Join(t.TempDir(), "bodies", v2SegmentName(9_000, records))
	var writeReads, trainingReads uint64
	readRecord := func(number uint64) []byte {
		var suffix [16]byte
		binary.BigEndian.PutUint64(suffix[:8], number)
		binary.BigEndian.PutUint64(suffix[8:], number%17)
		return append(bytes.Repeat([]byte("transfer-contract-owner-recipient-amount"), 64), suffix[:]...)
	}
	err := writeV2TableSegment(path, "bodies", 9_000, records, 64,
		func(number uint64) ([]byte, error) {
			writeReads++
			return readRecord(number), nil
		},
		func(number uint64) ([]byte, error) {
			trainingReads++
			return readRecord(number), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if writeReads != records {
		t.Fatalf("ordered writer reads = %d, want %d", writeReads, records)
	}
	if trainingReads == 0 || trainingReads > v2BodiesTrainSamples {
		t.Fatalf("training reads = %d, want (0,%d]", trainingReads, v2BodiesTrainSamples)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.codec != v2CodecBodiesTrainedDict {
		t.Fatalf("codec = %d, want trained dictionary", reader.codec)
	}
}

func TestV2BodiesTrainingFallsBackForDegenerateCorpus(t *testing.T) {
	const records = uint64(32)
	path := filepath.Join(t.TempDir(), "bodies", v2SegmentName(0, records))
	read := func(number uint64) ([]byte, error) {
		return []byte{byte(number)}, nil
	}
	if err := writeV2TableSegment(path, "bodies", 0, records, 8, read, read); err != nil {
		t.Fatal(err)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	store := newTestV2Store(t, "bodies", reader)
	defer store.Close()
	if reader.codec != v2CodecBodiesRawDict {
		t.Fatalf("codec = %d, want raw dictionary fallback", reader.codec)
	}
	for number := uint64(0); number < records; number++ {
		got, err := store.read("bodies", number)
		if err != nil || !bytes.Equal(got, []byte{byte(number)}) {
			t.Fatalf("body %d = %x err=%v", number, got, err)
		}
	}
}

func TestV2BodiesTrainingPropagatesSourceError(t *testing.T) {
	want := errors.New("sample read failed")
	path := filepath.Join(t.TempDir(), "bodies", v2SegmentName(0, 512))
	err := writeV2TableSegment(path, "bodies", 0, 512, 64,
		func(number uint64) ([]byte, error) {
			return bytes.Repeat([]byte("canonical-body"), 64), nil
		},
		func(number uint64) ([]byte, error) {
			if number > 400 {
				return nil, want
			}
			return bytes.Repeat([]byte("canonical-body"), 64), nil
		},
	)
	if !errors.Is(err, want) {
		t.Fatalf("write error = %v, want %v", err, want)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("failed trained segment was published: %v", statErr)
	}
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

func TestFreezerDirectV2PersistsLogicalHeadWithoutV1(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	want := make(map[string][][]byte, len(tables))
	for kind := range tables {
		want[kind] = make([][]byte, 64)
		for number := uint64(0); number < 64; number++ {
			want[kind][number] = []byte(fmt.Sprintf("%s-direct-%d", kind, number))
		}
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	if !f.CanAppendV2Direct(0) {
		t.Fatal("fresh freezer did not accept direct V2 at genesis")
	}
	result, err := f.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   1,
		Online:        true,
		SourceHead:    64,
		Source: func(kind string, number uint64) ([]byte, error) {
			return want[kind][number], nil
		},
	})
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if result.End != 64 || result.Segments != 1 || f.V2Coverage() != 64 {
		_ = f.Close()
		t.Fatalf("direct result=%+v coverage=%d", result, f.V2Coverage())
	}
	for kind, table := range f.tables {
		if table.items.Load() != 0 || table.itemHidden.Load() != 0 {
			_ = f.Close()
			t.Fatalf("direct V2 materialized V1 %s head=%d tail=%d", kind, table.items.Load(), table.itemHidden.Load())
		}
	}
	if _, err := f.ModifyAncients(func(AncientWriteOp) error { return nil }); err == nil {
		_ = f.Close()
		t.Fatal("V1 append remained enabled after direct V2 advanced the logical head")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if count, err := reopened.AncientCount("bodies"); err != nil || count != 64 || reopened.head.Load() != 64 {
		t.Fatalf("reopened direct head count=%d logical=%d err=%v", count, reopened.head.Load(), err)
	}
	if !reopened.CanAppendV2Direct(64) {
		t.Fatal("reopened freezer cannot continue direct V2")
	}
	if tail := reopened.V1Tail(); tail != 64 {
		t.Fatalf("reopened direct V1 tail=%d, want logical head 64", tail)
	}
	for kind := range tables {
		for _, number := range []uint64{0, 31, 63} {
			got, err := reopened.Ancient(kind, number)
			if err != nil || !bytes.Equal(got, want[kind][number]) {
				t.Fatalf("reopened %s[%d]=%q err=%v, want %q", kind, number, got, err, want[kind][number])
			}
		}
	}
}

func TestFreezerDirectV2RecoversManifestPublishedBeforeLiveInstall(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop after V2 manifest")
	options := V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   1,
		Online:        true,
		SourceHead:    64,
		Source: func(kind string, number uint64) ([]byte, error) {
			return []byte(fmt.Sprintf("%s-%d", kind, number)), nil
		},
		TransactionIndexPrefixBits: 8,
		TransactionIndexEntries: func(number uint64, _ []byte) ([]TransactionIndexEntry, error) {
			var hash [32]byte
			binary.BigEndian.PutUint64(hash[24:], number+1)
			return []TransactionIndexEntry{{Hash: hash, Location: number}}, nil
		},
		BeforeTransactionIndexPublish: func() error { return stop },
	}
	_, err = f.MigrateV2(options)
	if !errors.Is(err, stop) {
		_ = f.Close()
		t.Fatalf("direct interrupted err=%v, want %v", err, stop)
	}
	if f.V2Coverage() != 0 || f.head.Load() != 0 {
		_ = f.Close()
		t.Fatalf("failed live install advanced coverage/head to %d/%d", f.V2Coverage(), f.head.Load())
	}
	// Retry without restarting the process. The durable manifest is the commit
	// marker, so recovery must adopt its files without invoking the source or
	// deleting/replacing any manifest-referenced segment.
	options.Source = func(kind string, number uint64) ([]byte, error) {
		return nil, fmt.Errorf("source unexpectedly reread during recovery: %s[%d]", kind, number)
	}
	options.BeforeTransactionIndexPublish = func() error {
		return errors.New("transaction-index publication unexpectedly retried")
	}
	result, err := f.MigrateV2(options)
	if err != nil {
		_ = f.Close()
		t.Fatalf("same-process recovery: %v", err)
	}
	if result.Start != 0 || result.End != 64 || result.Segments != 1 || f.V2Coverage() != 64 || f.head.Load() != 64 {
		_ = f.Close()
		t.Fatalf("same-process recovery result=%+v coverage=%d head=%d", result, f.V2Coverage(), f.head.Load())
	}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		if got, err := f.Ancient(kind, 63); err != nil || string(got) != kind+"-63" {
			_ = f.Close()
			t.Fatalf("same-process recovered %s[63]=%q err=%v", kind, got, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.V2Coverage() != 64 || reopened.head.Load() != 64 || !reopened.CanAppendV2Direct(64) {
		t.Fatalf("recovered coverage=%d head=%d direct=%t, want 64/64/true", reopened.V2Coverage(), reopened.head.Load(), reopened.CanAppendV2Direct(64))
	}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		if got, err := reopened.Ancient(kind, 63); err != nil || string(got) != kind+"-63" {
			t.Fatalf("recovered %s[63]=%q err=%v", kind, got, err)
		}
	}
}

func TestFreezerDirectV2SourceFailureLeavesNoPublishedSegmentAndRetries(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sourceErr := errors.New("injected direct source failure")
	fail := true
	options := V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   1,
		Online:        true,
		SourceHead:    64,
		Source: func(kind string, number uint64) ([]byte, error) {
			if fail && kind == "tx_infos" && number == 17 {
				return nil, sourceErr
			}
			return []byte(fmt.Sprintf("%s-retry-%d", kind, number)), nil
		},
	}
	if _, err := f.MigrateV2(options); !errors.Is(err, sourceErr) {
		t.Fatalf("direct source failure err=%v, want %v", err, sourceErr)
	}
	if coverage, head := f.V2Coverage(), f.head.Load(); coverage != 0 || head != 0 {
		t.Fatalf("failed source advanced coverage/head to %d/%d", coverage, head)
	}
	manifests, err := filepath.Glob(filepath.Join(dir, "v2", "manifests", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 0 {
		t.Fatalf("failed source published manifests: %v", manifests)
	}
	for kind := range tables {
		segments, err := filepath.Glob(filepath.Join(dir, "v2", kind, "*.gtv2"))
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 0 {
			t.Fatalf("failed source left published %s segments: %v", kind, segments)
		}
	}

	fail = false
	result, err := f.MigrateV2(options)
	if err != nil {
		t.Fatal(err)
	}
	if result.End != 64 || result.Segments != 1 || f.V2Coverage() != 64 || f.head.Load() != 64 {
		t.Fatalf("retry result=%+v coverage=%d head=%d", result, f.V2Coverage(), f.head.Load())
	}
	for kind := range tables {
		got, err := f.Ancient(kind, 17)
		if err != nil || string(got) != kind+"-retry-17" {
			t.Fatalf("retried %s[17]=%q err=%v", kind, got, err)
		}
	}
}

func TestFreezerMigrateV2RoundTripResumeAndAppend(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
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
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
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
	borrowed, err := freezer.AncientNoCopy("bodies", 3)
	if err != nil {
		t.Fatalf("borrow V2 body: %v", err)
	}
	segment, ok := freezer.v2.find("bodies", 3)
	if !ok {
		t.Fatal("borrowed V2 segment missing")
	}
	frame, err := freezer.v2.readFrame(segment, 0)
	if err != nil {
		t.Fatal(err)
	}
	bounds := frame.records[3]
	if len(borrowed) == 0 || &borrowed[0] != &frame.data[bounds.start] {
		t.Fatal("AncientNoCopy did not return the decoded-frame record view")
	}
	if cap(borrowed) != len(borrowed) || !bytes.Equal(borrowed, want["bodies"][3]) {
		t.Fatalf("borrowed body len/cap/value = %d/%d/%x", len(borrowed), cap(borrowed), borrowed)
	}
	// A retained immutable view keeps its decoded frame allocation alive even
	// after the cache evicts that frame. Stored replay relies on this while the
	// decoded block borrows calldata-like fields across execution.
	retainedWant := bytes.Clone(borrowed)
	freezer.v2.cacheLimit = 1
	if _, err := freezer.AncientNoCopy("bodies", 11); err != nil {
		t.Fatalf("load eviction frame: %v", err)
	}
	if !bytes.Equal(borrowed, retainedWant) {
		t.Fatalf("retained borrowed body changed after frame eviction: got %x want %x", borrowed, retainedWant)
	}
	freezer.v2.cacheLimit = v2DefaultCacheBytes
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
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
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

func TestFreezerMigrateV2BuildsAndPublishesTransactionIndex(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 64; number++ {
			for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
				if err := op.AppendRaw(kind, number, []byte{byte(number), byte(number >> 8)}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var calls int
	result, err := f.MigrateV2(V2MigrationOptions{
		Tables:                         []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks:                  64,
		FrameBlocks:                    8,
		Online:                         true,
		TransactionIndexPrefixBits:     8,
		TransactionIndexETLBufferBytes: 1024,
		TransactionIndexEntries: func(number uint64, _ []byte) ([]TransactionIndexEntry, error) {
			calls++
			var hash [32]byte
			binary.BigEndian.PutUint64(hash[24:], number+1)
			return []TransactionIndexEntry{{Hash: hash, Location: transactionLocationMarker | number<<transactionLocationOrdinalBits}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 64 {
		t.Fatalf("body callback calls=%d, want 64", calls)
	}
	if result.TransactionIndexRuns != 1 || result.TransactionIndexRows != 64 || result.TransactionIndexSpilledRuns == 0 || f.TransactionIndexCoverage() != 64 {
		t.Fatalf("fused index result=%+v coverage=%d", result, f.TransactionIndexCoverage())
	}
	var hash [32]byte
	binary.BigEndian.PutUint64(hash[24:], 43)
	candidates, err := f.TransactionIndexCandidates(hash)
	found := false
	for _, candidate := range candidates {
		if transactionIndexLocationBlock(candidate) == 42 {
			found = true
			break
		}
	}
	if err != nil || !found {
		t.Fatalf("candidates=%v err=%v", candidates, err)
	}
}

func TestFreezerMigrateV2IndexPublishFailureKeepsV1(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 64; number++ {
			for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
				if err := op.AppendRaw(kind, number, []byte{byte(number)}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("index publication blocked")
	_, err = f.MigrateV2(V2MigrationOptions{
		Tables:                     []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks:              64,
		FrameBlocks:                8,
		Online:                     true,
		TransactionIndexPrefixBits: 8,
		TransactionIndexEntries: func(number uint64, _ []byte) ([]TransactionIndexEntry, error) {
			var hash [32]byte
			binary.BigEndian.PutUint64(hash[24:], number+1)
			return []TransactionIndexEntry{{Hash: hash, Location: transactionLocationMarker | number<<transactionLocationOrdinalBits}}, nil
		},
		BeforeTransactionIndexPublish: func() error { return injected },
	})
	if !errors.Is(err, injected) {
		t.Fatalf("migration error=%v, want injected failure", err)
	}
	if tail := f.V1Tail(); tail != 0 {
		t.Fatalf("V1 tail=%d after failed index publish, want 0", tail)
	}
	if got, err := f.tables["bodies"].Retrieve(7); err != nil || len(got) == 0 {
		t.Fatalf("V1 body unavailable after failed publication: %x %v", got, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.V2Coverage() != 64 || f.TransactionIndexCoverage() != 0 || f.V1Tail() != 0 {
		t.Fatalf("reopen coverage v2=%d index=%d tail=%d", f.V2Coverage(), f.TransactionIndexCoverage(), f.V1Tail())
	}
	if got, err := f.Ancient("bodies", 7); err != nil || len(got) == 0 {
		t.Fatalf("published V2 body unavailable after recovery: %x %v", got, err)
	}
	collector, err := etl.NewCollector(etl.Options{TempDir: t.TempDir(), BufferLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	for number := uint64(0); number < 64; number++ {
		var hash [32]byte
		binary.BigEndian.PutUint64(hash[24:], number+1)
		location := transactionLocationMarker | number<<transactionLocationOrdinalBits
		if err := collector.PutEncoded(40, 0, func(key, _ []byte) {
			copy(key[:32], hash[:])
			binary.BigEndian.PutUint64(key[32:], location)
		}); err != nil {
			t.Fatal(err)
		}
	}
	runPath := TransactionIndexRunPath(dir, 0, 64)
	recoveredResult, recovered, err := buildOrRecoverTransactionIndexRun(context.Background(), runPath, 0, 64, 8, collector)
	if err != nil || !recovered {
		t.Fatalf("recover unpublished fused run: recovered=%v err=%v", recovered, err)
	}
	if err := PublishTransactionIndexRun(dir, recoveredResult); err != nil {
		t.Fatalf("publish recovered fused run: %v", err)
	}
	index, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	if index.Coverage() != 64 {
		t.Fatalf("recovered index coverage=%d, want 64", index.Coverage())
	}
}

func TestFusedTransactionIndexExternalSortLargeSample(t *testing.T) {
	const rows = 200_000
	collector, err := etl.NewCollector(etl.Options{TempDir: t.TempDir(), BufferLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	for i := uint64(0); i < rows; i++ {
		var hash [32]byte
		hash[0] = byte(i)
		binary.BigEndian.PutUint64(hash[24:], i+1)
		location := transactionLocationMarker | (i%64)<<transactionLocationOrdinalBits
		if err := collector.PutEncoded(40, 0, func(key, _ []byte) {
			copy(key[:32], hash[:])
			binary.BigEndian.PutUint64(key[32:], location)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if collector.Stats().SpilledRuns == 0 {
		t.Fatal("large fused-index sample did not use external-sort spill runs")
	}
	path := TransactionIndexRunPath(t.TempDir(), 0, 64)
	result, recovered, err := buildOrRecoverTransactionIndexRun(context.Background(), path, 0, 64, 8, collector)
	if err != nil || recovered {
		t.Fatalf("large fused-index build: recovered=%v err=%v", recovered, err)
	}
	if result.Rows != rows {
		t.Fatalf("large fused-index rows=%d, want %d", result.Rows, rows)
	}
	run, err := OpenTransactionIndexRun(path)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	if err := run.Verify(); err != nil {
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

func TestRewriteV2TransactionInfosMarksUnchangedSegmentWithoutRewrite(t *testing.T) {
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
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 16; number++ {
			if err := op.AppendRaw("bodies", number, []byte(fmt.Sprintf("body-%d", number))); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, []byte(fmt.Sprintf("receipt-%d", number))); err != nil {
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
	base := filepath.Join(dir, "v2")
	manifests, err := readV2Manifests(base)
	if err != nil || len(manifests) != 1 {
		t.Fatalf("manifests=%+v err=%v", manifests, err)
	}
	originalName := manifests[0].Tables["tx_infos"]
	originalPath := filepath.Join(base, "tx_infos", originalName)
	originalInfo, err := os.Stat(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := freezer.RewriteV2TransactionInfos(V2TxInfoRewriteOptions{
		Transform: func(_ uint64, txInfo, _ []byte) ([]byte, uint64, error) {
			return txInfo, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments != 1 || result.RewrittenSegments != 0 || result.Rows != 16 || result.RemovedBytes != 0 {
		t.Fatalf("unchanged audit = %+v", result)
	}
	manifests, err = readV2Manifests(base)
	if err != nil || len(manifests) != 1 {
		t.Fatalf("updated manifests=%+v err=%v", manifests, err)
	}
	if !manifests[0].TxInfoIDsCompacted || manifests[0].Tables["tx_infos"] != originalName {
		t.Fatalf("updated manifest=%+v", manifests[0])
	}
	updatedInfo, err := os.Stat(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(originalInfo, updatedInfo) {
		t.Fatal("unchanged tx_infos segment was replaced")
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

func TestFreezerMigrateV2OnlineInstallsManyManifestsIncrementally(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	const (
		segmentBlocks = uint64(8)
		segmentCount  = uint64(64)
		rows          = segmentBlocks * segmentCount
	)
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < rows; number++ {
			for kind := range tables {
				value := []byte(fmt.Sprintf("%s-%06d", kind, number))
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

	var (
		firstStore   *v2Store
		firstReaders = make(map[string]*v2SegmentReader)
	)
	result, err := freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks: segmentBlocks,
		FrameBlocks:   4,
		Online:        true,
		Progress: func(progress V2MigrationProgress) {
			if progress.Stage != "complete" || progress.Segment != 1 {
				return
			}
			freezer.v2Mu.RLock()
			defer freezer.v2Mu.RUnlock()
			firstStore = freezer.v2
			for kind := range tables {
				firstReaders[kind] = freezer.v2.segments[kind][0]
			}
		},
	})
	if err != nil {
		t.Fatalf("online migration: %v", err)
	}
	if result.Segments != segmentCount || result.End != rows {
		t.Fatalf("migration result=%+v", result)
	}

	freezer.v2Mu.RLock()
	if freezer.v2 != firstStore {
		freezer.v2Mu.RUnlock()
		t.Fatal("online migration replaced the V2 store instead of extending it")
	}
	var installed []*v2SegmentReader
	for kind := range tables {
		segments := freezer.v2.segments[kind]
		if len(segments) != int(segmentCount) {
			freezer.v2Mu.RUnlock()
			t.Fatalf("%s segments=%d, want %d", kind, len(segments), segmentCount)
		}
		if segments[0] != firstReaders[kind] || segments[0].file == nil {
			freezer.v2Mu.RUnlock()
			t.Fatalf("%s first reader was reopened or closed", kind)
		}
		installed = append(installed, segments...)
	}
	freezer.v2Mu.RUnlock()

	for number := uint64(0); number < rows; number += segmentBlocks {
		for kind := range tables {
			got, err := freezer.Ancient(kind, number)
			want := []byte(fmt.Sprintf("%s-%06d", kind, number))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %s[%d]=%q want=%q err=%v", kind, number, got, want, err)
			}
		}
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, reader := range installed {
		if reader.file != nil {
			t.Fatalf("segment reader %s remained open after freezer close", reader.path)
		}
	}
}

func BenchmarkV2StoreInstallAtHighManifestCount(b *testing.B) {
	const manifestCount = uint64(256)
	base := filepath.Join(b.TempDir(), "v2")
	tables := []string{"bodies", "tx_infos", "state_roots"}
	writeManifestFiles := func(manifest v2Manifest) {
		b.Helper()
		for _, kind := range tables {
			name := v2SegmentName(manifest.Start, manifest.Count)
			manifest.Tables[kind] = name
			path := filepath.Join(base, kind, name)
			if err := writeV2Segment(path, manifest.Start, manifest.Count, manifest.FrameBlocks, func(number uint64) ([]byte, error) {
				return []byte(fmt.Sprintf("%s-%d", kind, number)), nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for start := uint64(0); start < manifestCount; start++ {
		manifest := v2Manifest{Version: v2Version, Start: start, Count: 1, FrameBlocks: 1, Tables: make(map[string]string, len(tables))}
		writeManifestFiles(manifest)
		if err := publishV2Manifest(base, manifest); err != nil {
			b.Fatal(err)
		}
	}
	next := v2Manifest{Version: v2Version, Start: manifestCount, Count: 1, FrameBlocks: 1, Tables: make(map[string]string, len(tables))}
	writeManifestFiles(next)

	b.Run("incremental", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			readers, err := openV2ManifestReaders(base, next)
			if err != nil {
				b.Fatal(err)
			}
			closeV2SegmentReaders(readers)
		}
	})
	b.Run("full-reload-256", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store, err := openV2Store(filepath.Dir(base))
			if err != nil {
				b.Fatal(err)
			}
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestFreezerMigrateV2SerializesExternalTailPrune(t *testing.T) {
	dir := t.TempDir()
	freezer, err := NewFreezer(dir, "", false, 2049, map[string]TableConfig{"bodies": {Prunable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer freezer.Close()
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 64; number++ {
			if err := op.AppendRaw("bodies", number, bytes.Repeat([]byte{byte(number)}, 128)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	migrated := make(chan error, 1)
	go func() {
		_, err := freezer.MigrateV2(V2MigrationOptions{
			Tables:        []string{"bodies"},
			SegmentBlocks: 64,
			FrameBlocks:   8,
			Online:        true,
			Transform: func(_ string, _ uint64, data, _ []byte) ([]byte, error) {
				once.Do(func() {
					close(started)
					<-release
				})
				return data, nil
			},
		})
		migrated <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("V2 migration did not begin")
	}

	pruned := make(chan error, 1)
	go func() {
		_, err := freezer.TruncateTail(32)
		pruned <- err
	}()
	select {
	case err := <-pruned:
		t.Fatalf("tail prune completed during V2 source read: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-migrated; err != nil {
		t.Fatalf("MigrateV2: %v", err)
	}
	if err := <-pruned; err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	if coverage, tail := freezer.V2Coverage(), freezer.V1Tail(); coverage != 64 || tail != 64 {
		t.Fatalf("coverage/tail = %d/%d, want 64/64", coverage, tail)
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
	segments := map[string][]*v2SegmentReader{kind: {reader}}
	decoder, err := newV2DecoderForSegments(segments)
	if err != nil {
		t.Fatal(err)
	}
	return &v2Store{
		segments:          segments,
		decoder:           decoder,
		cacheList:         list.New(),
		cacheItems:        make(map[v2FrameKey]*list.Element),
		cacheLoads:        make(map[v2FrameKey]*v2FrameLoad),
		cacheLimit:        v2DefaultCacheBytes,
		compressedBuffers: make(chan *v2CompressedBuffer, v2DecoderConcurrency),
	}
}
