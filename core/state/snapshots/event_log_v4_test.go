package snapshots

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestEventLogV4StripsAndExactlyRestoresOrderedTopics(t *testing.T) {
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x81))
	topicA, topicB := common.Hash{0xa1}, common.Hash{0xb2}
	source := EventLog{
		BlockNum: 1, TxIndex: 2, LogIndex: 3, TxHash: common.Hash{0x11}, BlockHash: common.Hash{0x22}, Address: address,
		Log: &corepb.TransactionInfo_Log{
			Address: eventLogV3PayloadAddress(address),
			Topics:  [][]byte{topicB[:], topicA[:], topicB[:]},
			Data:    bytes.Repeat([]byte{0x5a}, 257),
		},
	}
	ref, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: []EventLog{source}}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.header.version != EventLogSegmentV4Version {
		t.Fatalf("version = %d, want V4", seg.header.version)
	}

	row, err := seg.readEventLogV3Row(0)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := readEventLogV3FrameAt(seg.file, seg.v3.header.payloadDirOffset, row.payloadFrame, seg.v3.payloadFrames)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := readEventLogPayloadAt(seg.file, frame.dataOff, uint64(frame.dataLen), seg.v3.header.payloadDataOffset+seg.v3.header.payloadDataLength)
	if err != nil {
		t.Fatal(err)
	}
	_, dec, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dec.DecodeAll(compressed, make([]byte, 0, int(frame.rawLen)))
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.TransactionInfo_Log
	if err := proto.Unmarshal(decoded[row.payloadOffset:row.payloadOffset+row.payloadLength], &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.GetAddress()) != 0 || len(stored.GetTopics()) != 0 {
		t.Fatalf("stored payload retained address/topics: address=%x topics=%x", stored.GetAddress(), stored.GetTopics())
	}

	var got EventLog
	if err := seg.IterateLogs(1, 1, EventLogFilter{}, func(row EventLog) (bool, error) {
		got = row
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Log, source.Log) {
		t.Fatalf("restored protobuf = %v, want %v", got.Log, source.Log)
	}
	filter := EventLogFilter{Topics: [][]common.Hash{{topicB}, {topicA}, {topicB}}}
	matched := 0
	if err := seg.IterateLogs(1, 1, filter, func(EventLog) (bool, error) { matched++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if matched != 1 {
		t.Fatalf("ordered duplicate topic filter matched %d rows, want 1", matched)
	}
}

func TestEventLogV4ReaderRejectsV3Magic(t *testing.T) {
	raw := make([]byte, eventLogV3HeaderSize)
	copy(raw, eventLogMagicV3[:])
	if _, err := readEventLogHeader(bytes.NewReader(raw)); err == nil || !strings.Contains(err.Error(), "V3 is unsupported") {
		t.Fatalf("V3 header error = %v", err)
	}
}

func TestEventLogV4RejectsCorruptTopicSequence(t *testing.T) {
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x82))
	topicA, topicB := common.Hash{0xa1}, common.Hash{0xb2}
	row := EventLog{BlockNum: 1, TxHash: common.Hash{1}, BlockHash: common.Hash{2}, Address: address, Log: &corepb.TransactionInfo_Log{
		Address: eventLogV3PayloadAddress(address), Topics: [][]byte{topicA[:], topicB[:]},
	}}
	ref, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
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
	frame, err := readEventLogV3FrameAt(file, header.v3.rowDirOffset, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, frame.dataLen)
	if _, err := file.ReadAt(raw, int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	off := 0
	for i := 0; i < 8; i++ {
		_, n := binary.Uvarint(raw[off:])
		if n <= 0 {
			t.Fatal("invalid generated row")
		}
		off += n
	}
	count, n := binary.Uvarint(raw[off:])
	if n <= 0 || count != 2 {
		t.Fatalf("topic count = %d n=%d", count, n)
	}
	off += n
	_, n = binary.Uvarint(raw[off:])
	off += n
	// Replace the position-1 dictionary ID with the position-0 ID. The row
	// frame checksum is updated so semantic topic-position validation is what
	// rejects the file.
	if raw[off] != 1 {
		t.Fatalf("second topic id encoding = %x, want 1", raw[off])
	}
	raw[off] = 0
	if _, err := file.WriteAt(raw, int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(raw))
	if _, err := file.WriteAt(checksum[:], int64(header.v3.rowDirOffset+8+24)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	ref.Checksum = ""
	if err := CheckEventLogSegment(dir, ref); err == nil || !strings.Contains(err.Error(), "topic dictionary position mismatch") {
		t.Fatalf("corrupt topic sequence error = %v", err)
	}
}

func TestEventLogIndexV2CompactAndCorruptionSafe(t *testing.T) {
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x83))
	topic := common.Hash{0xc3}
	row := eventLogV3TestRow(1, 0, 0, address, common.Hash{1}, common.Hash{2}, topic, []byte("payload"))
	eventRef, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatal(err)
	}
	index, err := OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range []struct {
		offset, length uint64
		keySize        int
	}{{index.header.addressIndexOffset, index.header.addressIndexLength, eventLogAddressLookupKeySize}, {index.header.topicIndexOffset, index.header.topicIndexLength, eventLogTopicLookupKeySize}} {
		if _, err := readEventLogV3LookupV2Header(index.file, section.offset, section.length, index.size, section.keySize); err != nil {
			t.Fatalf("compact sidecar lookup: %v", err)
		}
	}
	starts, used, err := index.CandidateSegmentStarts(EventLogFilter{Addresses: []common.Address{address}, Topics: [][]common.Hash{{topic}}})
	if err != nil || !used || len(starts) != 1 || starts[0] != 1 {
		t.Fatalf("candidate starts=%v used=%v err=%v", starts, used, err)
	}
	header := index.header
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(filepath.Join(dir, indexRef.Path), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	lookupHeader, err := readEventLogV3LookupV2Header(file, header.topicIndexOffset, header.topicIndexLength, indexRef.Size, eventLogTopicLookupKeySize)
	if err != nil {
		t.Fatal(err)
	}
	block, err := readEventLogV3LookupV2Block(file, header.topicIndexOffset, lookupHeader, 0)
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
	indexRef.Checksum = ""
	if err := CheckEventLogIndexSegment(dir, indexRef); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupt sidecar error = %v", err)
	}
}

func TestEventLogV4SpaceBreakdown(t *testing.T) {
	const rowCount = 4096
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x84))
	rows := make([]EventLog, 0, rowCount)
	var fullPayloadRaw, strippedPayloadRaw uint64
	for i := 0; i < rowCount; i++ {
		topics := make([][]byte, 4)
		for position := range topics {
			var topic common.Hash
			binary.BigEndian.PutUint64(topic[24:], uint64(position*64+i%64))
			topics[position] = append([]byte(nil), topic[:]...)
		}
		log := &corepb.TransactionInfo_Log{Address: eventLogV3PayloadAddress(address), Topics: topics, Data: bytes.Repeat([]byte{byte(i)}, 128)}
		full, err := proto.Marshal(log)
		if err != nil {
			t.Fatal(err)
		}
		copy := proto.Clone(log).(*corepb.TransactionInfo_Log)
		copy.Address, copy.Topics = nil, nil
		stripped, err := proto.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		fullPayloadRaw += uint64(len(full))
		strippedPayloadRaw += uint64(len(stripped))
		var txHash common.Hash
		binary.BigEndian.PutUint64(txHash[24:], uint64(i+1))
		rows = append(rows, EventLog{BlockNum: 1, TxIndex: uint64(i), LogIndex: uint64(i), TxHash: txHash, BlockHash: common.Hash{2}, Address: address, Log: log})
	}
	eventRef, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatal(err)
	}
	seg, err := OpenEventLogSegment(dir, eventRef)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	index, err := OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := index.Stats()
	if err != nil {
		t.Fatal(err)
	}
	_ = index.Close()
	legacySidecar := uint64(eventLogIndexHeaderSize) +
		eventLogLookupHeaderSize + stats.Address.Keys*uint64(eventLogAddressLookupKeySize+16) + stats.Address.Postings*8 +
		eventLogLookupHeaderSize + stats.Topic.Keys*uint64(eventLogTopicLookupKeySize+16) + stats.Topic.Postings*8
	t.Logf("V4 bytes main=%d sidecar=%d payloadCompressed=%d topicLookup=%d payloadRawFull=%d payloadRawStripped=%d sidecarV1Projected=%d",
		eventRef.Size, indexRef.Size, seg.v3.header.payloadDataLength, seg.v3.header.topicIndexLength, fullPayloadRaw, strippedPayloadRaw, legacySidecar)
	if indexRef.Size >= legacySidecar {
		t.Fatalf("compact sidecar=%d is not smaller than V1 projection=%d", indexRef.Size, legacySidecar)
	}
	if strippedPayloadRaw*2 >= fullPayloadRaw {
		t.Fatalf("topic stripping saved less than 50%% raw payload: %d -> %d", fullPayloadRaw, strippedPayloadRaw)
	}
}
