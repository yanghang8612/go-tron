package snapshots

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEventLogV3CompactLookupSpaceAndFilterSemantics(t *testing.T) {
	const rowsCount = 4096
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x61))
	rows := make([]EventLog, 0, rowsCount)
	var legacyTopicPostingBytes uint64
	for i := uint64(0); i < rowsCount; i++ {
		var topic common.Hash
		binary.BigEndian.PutUint64(topic[common.HashLength-8:], i+1)
		var txHash common.Hash
		binary.BigEndian.PutUint64(txHash[common.HashLength-8:], i+1)
		rows = append(rows, EventLog{
			BlockNum: 1, TxIndex: i, LogIndex: i, TxHash: txHash, BlockHash: common.Hash{0x71}, Address: address,
			Log: &corepb.TransactionInfo_Log{Address: append([]byte(nil), address[:]...), Topics: [][]byte{topic[:]}, Data: []byte{byte(i)}},
		})
		legacyTopicPostingBytes += uvarintBytes(i)
	}
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogV3SegmentFromReader: %v", err)
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	for _, section := range []uint64{seg.v3.header.addressIndexOffset, seg.v3.header.topicIndexOffset} {
		compact, err := isEventLogV3LookupV2(seg.file, section, seg.size-section, seg.size)
		if err != nil || !compact {
			t.Fatalf("lookup section at %d compact=%v err=%v", section, compact, err)
		}
	}
	legacyTopicBytes := uint64(eventLogV3LookupHeaderSize) + rowsCount*uint64(eventLogTopicLookupKeySize+eventLogV3LookupKeyTail) + rowsCount*eventLogV3LookupFrameEntry + legacyTopicPostingBytes
	compactTopicBytes := seg.v3.header.topicIndexLength
	if compactTopicBytes*100 >= legacyTopicBytes*65 {
		t.Fatalf("compact topic lookup = %d bytes, legacy model = %d bytes, want at least 35%% saving", compactTopicBytes, legacyTopicBytes)
	}

	var wantTopic common.Hash
	binary.BigEndian.PutUint64(wantTopic[common.HashLength-8:], 3073)
	var got []EventLog
	if err := seg.IterateLogs(1, 1, EventLogFilter{Addresses: []common.Address{address}, Topics: [][]common.Hash{{wantTopic}}}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	}); err != nil {
		t.Fatalf("filtered IterateLogs: %v", err)
	}
	if len(got) != 1 || got[0].TxIndex != 3072 || got[0].Address != address {
		t.Fatalf("filtered rows = %+v, want txIndex 3072", got)
	}
	t.Logf("topic lookup bytes: compact=%d legacy=%d saving=%.1f%%", compactTopicBytes, legacyTopicBytes, 100*(1-float64(compactTopicBytes)/float64(legacyTopicBytes)))
}

func TestEventLogV3ReaderAcceptsLegacyLookupSection(t *testing.T) {
	postings := map[string][]uint64{
		string(makeLookupTestKey(eventLogTopicLookupKeySize, 1)): {1, 4, 9},
		string(makeLookupTestKey(eventLogTopicLookupKeySize, 2)): {2, 7},
	}
	file, length := writeLegacyEventLogV3LookupForTest(t, eventLogTopicLookupKeySize, postings)
	defer file.Close()
	keys, frames, err := readEventLogV3LookupCounts(file, 0, length, length, eventLogTopicLookupKeySize)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 2 || frames != 2 {
		t.Fatalf("legacy lookup counts = %d/%d, want 2/2", keys, frames)
	}
	wantKey := makeLookupTestKey(eventLogTopicLookupKeySize, 1)
	rows, err := readEventLogV3LookupRows(file, 0, length, length, eventLogTopicLookupKeySize, wantKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0] != 1 || rows[1] != 4 || rows[2] != 9 {
		t.Fatalf("legacy lookup rows = %v", rows)
	}
}

func TestEventLogV3CompactLookupRejectsCorruptKeyBlock(t *testing.T) {
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x62))
	row := eventLogV3TestRow(1, 0, 0, address, common.Hash{1}, common.Hash{2}, common.Hash{3}, []byte("payload"))
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, ref.Path), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	header, err := readEventLogHeader(file)
	if err != nil {
		t.Fatal(err)
	}
	lookupHeader, err := readEventLogV3LookupV2Header(file, header.v3.topicIndexOffset, header.v3.topicIndexLength, ref.Size, eventLogTopicLookupKeySize)
	if err != nil {
		t.Fatal(err)
	}
	block, err := readEventLogV3LookupV2Block(file, header.v3.topicIndexOffset, lookupHeader, 0)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := file.ReadAt(one[:], int64(block.dataOff)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one[:], int64(block.dataOff)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	ref.Checksum = ""
	if err := CheckEventLogSegment(dir, ref); err == nil {
		t.Fatal("CheckEventLogSegment accepted corrupt compact lookup key block")
	}
}

func BenchmarkEventLogV3CompactLookupSpace(b *testing.B) {
	const keysCount = 100000
	postings := make(map[string][]uint64, keysCount)
	var legacy uint64 = eventLogV3LookupHeaderSize
	for i := uint64(0); i < keysCount; i++ {
		key := makeRandomTopicLookupTestKey(i)
		values := []uint64{i * 3}
		if i < 16 {
			values = make([]uint64, 4096)
			for j := range values {
				values[j] = uint64(j) * 17
			}
		}
		postings[string(key)] = values
		legacy += uint64(eventLogTopicLookupKeySize+eventLogV3LookupKeyTail) + uint64(ceilDiv(uint64(len(values)), eventLogV3LookupFrameRows))*eventLogV3LookupFrameEntry
		for j, value := range values {
			if j > 0 {
				value -= values[j-1]
			}
			legacy += uvarintBytes(value)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		lookup, err := buildEventLogV3Lookup(dir, "benchmark-topic", eventLogTopicLookupKeySize, postings)
		if err != nil {
			b.Fatal(err)
		}
		compact := lookup.length()
		lookup.close()
		b.ReportMetric(float64(compact), "compact-bytes")
		b.ReportMetric(float64(legacy), "legacy-bytes")
		b.ReportMetric(100*(1-float64(compact)/float64(legacy)), "saving-percent")
	}
}

func makeRandomTopicLookupTestKey(value uint64) []byte {
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], value)
	hash := sha256.Sum256(seed[:])
	key := make([]byte, eventLogTopicLookupKeySize)
	binary.BigEndian.PutUint64(key[:8], value%4)
	copy(key[8:], hash[:])
	return key
}

func makeLookupTestKey(size int, value uint64) []byte {
	key := make([]byte, size)
	binary.BigEndian.PutUint64(key[size-8:], value)
	return key
}

func writeLegacyEventLogV3LookupForTest(t *testing.T, keySize int, postings map[string][]uint64) (*os.File, uint64) {
	t.Helper()
	dir := t.TempDir()
	data, err := os.CreateTemp(dir, "legacy-postings")
	if err != nil {
		t.Fatal(err)
	}
	build := &eventLogV3LookupBuild{keySize: uint64(keySize), data: data, name: data.Name()}
	for _, value := range []byte{1, 2} {
		key := makeLookupTestKey(keySize, uint64(value))
		values := postings[string(key)]
		raw := make([]byte, 0, len(values)*2)
		for i, row := range values {
			if i > 0 {
				row -= values[i-1]
			}
			raw = binary.AppendUvarint(raw, row)
		}
		off, err := data.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := data.Write(raw); err != nil {
			t.Fatal(err)
		}
		build.keys = append(build.keys, eventLogV3LookupKey{key: key, firstFrame: uint64(len(build.frames)), frameCount: 1, postingCount: uint64(len(values))})
		build.frames = append(build.frames, eventLogV3LookupFrame{dataOff: uint64(off), dataLen: uint32(len(raw)), count: uint32(len(values)), first: values[0], checksum: crc32.ChecksumIEEE(raw)})
	}
	stat, err := data.Stat()
	if err != nil {
		t.Fatal(err)
	}
	build.dataLen = uint64(stat.Size())
	outPath := filepath.Join(dir, "legacy.idx")
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeEventLogV3Lookup(out, 0, build); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	build.close()
	read, err := os.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := read.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return read, uint64(info.Size())
}
