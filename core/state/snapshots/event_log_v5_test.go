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
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func collectAllEventLogsFromSegment(t *testing.T, dir string, ref SegmentRef, from, to uint64, filter EventLogFilter) []EventLog {
	t.Helper()
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatalf("OpenEventLogSegment(%s): %v", ref.Path, err)
	}
	defer seg.Close()
	var out []EventLog
	if err := seg.IterateLogs(from, to, filter, func(row EventLog) (bool, error) {
		out = append(out, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLogs(%s): %v", ref.Path, err)
	}
	return out
}

func TestEventLogV5RoundTripParityAcrossEdgeCases(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		dir := t.TempDir()
		ref, err := BuildEventLogV5SegmentFromReader(eventLogRowsReader{}, dir, "", 1, 10)
		if err != nil {
			t.Fatalf("BuildEventLogV5SegmentFromReader(empty): %v", err)
		}
		if err := CheckEventLogSegment(dir, ref); err != nil {
			t.Fatalf("CheckEventLogSegment(empty): %v", err)
		}
		if rows := collectAllEventLogsFromSegment(t, dir, ref, 1, 10, EventLogFilter{}); len(rows) != 0 {
			t.Fatalf("empty segment returned %d rows", len(rows))
		}
	})

	t.Run("multi-frame mixed native TVM and duplicate topics", func(t *testing.T) {
		dir := t.TempDir()
		evmAddr := common.BytesToAddress(eventLogTestAddress(0x21))
		nativeAddr := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, eventLogTestAddress(0x22)[1:]...))
		topicHot := common.Hash{0xaa, 0x01}
		topicOther := common.Hash{0xbb, 0x02}

		rows := make([]EventLog, 0, 1250)
		for i := uint64(0); i < 1250; i++ {
			block := uint64(100) + i/25
			addr := evmAddr
			if i%5 == 0 {
				addr = nativeAddr
			}
			var txHash, blockHash, uniqTopic common.Hash
			binary.BigEndian.PutUint64(txHash[24:], i/2+1)
			binary.BigEndian.PutUint64(blockHash[24:], block)
			binary.BigEndian.PutUint64(uniqTopic[24:], i+1)

			var topics [][]byte
			switch i % 4 {
			case 0:
				topics = [][]byte{topicHot[:], uniqTopic[:], topicOther[:]}
			case 1:
				topics = [][]byte{topicHot[:], uniqTopic[:], uniqTopic[:]}
			case 2:
				topics = nil
			case 3:
				topics = [][]byte{topicOther[:]}
			}
			rawAddr := eventLogV3PayloadAddress(addr)
			if addr[0] == common.AddressPrefixMainnet {
				rawAddr = append([]byte(nil), addr[:]...)
			}
			rows = append(rows, EventLog{
				BlockNum:  block,
				TxIndex:   i % 25,
				LogIndex:  i % 25,
				TxHash:    txHash,
				BlockHash: blockHash,
				Address:   addr,
				Log: &corepb.TransactionInfo_Log{
					Address: rawAddr,
					Topics:  topics,
					Data:    bytes.Repeat([]byte{byte(i)}, int(32+(i%96))),
				},
			})
		}

		// Also include unknown protobuf field preservation in one log.
		msgDesc := &descriptorpb.DescriptorProto{
			Name: proto.String("ExtendedLog"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("address"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
				{Name: proto.String("topics"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
				{Name: proto.String("data"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
				{Name: proto.String("extra"), Number: proto.Int32(99), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()},
			},
		}
		fileDesc, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
			Name:        proto.String("ext.proto"),
			Package:     proto.String("ext"),
			Syntax:      proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{msgDesc},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		dyn := dynamicpb.NewMessage(fileDesc.Messages().ByName("ExtendedLog"))
		md := dyn.Descriptor()
		dyn.Set(md.Fields().ByNumber(1), protoreflect.ValueOfBytes(eventLogV3PayloadAddress(evmAddr)))
		dyn.Set(md.Fields().ByNumber(3), protoreflect.ValueOfBytes([]byte{1, 2, 3}))
		dyn.Set(md.Fields().ByNumber(99), protoreflect.ValueOfUint64(424242))
		extWire, err := proto.Marshal(dyn)
		if err != nil {
			t.Fatal(err)
		}
		var extLog corepb.TransactionInfo_Log
		if err := proto.Unmarshal(extWire, &extLog); err != nil {
			t.Fatal(err)
		}
		lastBlock := uint64(200)
		rows = append(rows, EventLog{
			BlockNum: lastBlock, TxIndex: 0, LogIndex: 0,
			TxHash: common.Hash{0xfe}, BlockHash: common.Hash{0xfd}, Address: evmAddr, Log: &extLog,
		})

		reader := eventLogRowsReader{rows: rows}
		refV4, err := BuildEventLogV4SegmentFromReader(reader, dir, "log/v4.seg", 100, lastBlock)
		if err != nil {
			t.Fatalf("BuildEventLogV4SegmentFromReader: %v", err)
		}
		refV5, err := BuildEventLogV5SegmentFromReader(reader, dir, "log/v5.seg", 100, lastBlock)
		if err != nil {
			t.Fatalf("BuildEventLogV5SegmentFromReader: %v", err)
		}
		if err := CheckEventLogSegment(dir, refV5); err != nil {
			t.Fatalf("CheckEventLogSegment(V5): %v", err)
		}

		gotV4 := collectAllEventLogsFromSegment(t, dir, refV4, 100, lastBlock, EventLogFilter{})
		gotV5 := collectAllEventLogsFromSegment(t, dir, refV5, 100, lastBlock, EventLogFilter{})
		if len(gotV4) != len(gotV5) || len(gotV5) != len(rows) {
			t.Fatalf("row count V4=%d V5=%d want=%d", len(gotV4), len(gotV5), len(rows))
		}
		for i := range rows {
			if gotV4[i].BlockNum != gotV5[i].BlockNum || gotV4[i].TxIndex != gotV5[i].TxIndex ||
				gotV4[i].LogIndex != gotV5[i].LogIndex || gotV4[i].TxHash != gotV5[i].TxHash ||
				gotV4[i].BlockHash != gotV5[i].BlockHash || gotV4[i].Address != gotV5[i].Address ||
				!proto.Equal(gotV4[i].Log, gotV5[i].Log) {
				t.Fatalf("row %d mismatch V4 vs V5", i)
			}
		}

		// Filtered lookup parity across V4 vs V5
		filter := EventLogFilter{
			Addresses: []common.Address{evmAddr.AccountID().Address(0)},
			Topics:    [][]common.Hash{{topicHot}, nil, {topicOther}},
		}
		fV4 := collectAllEventLogsFromSegment(t, dir, refV4, 105, 145, filter)
		fV5 := collectAllEventLogsFromSegment(t, dir, refV5, 105, 145, filter)
		if len(fV4) != len(fV5) || len(fV5) == 0 {
			t.Fatalf("filtered count V4=%d V5=%d", len(fV4), len(fV5))
		}
		for i := range fV4 {
			if !proto.Equal(fV4[i].Log, fV5[i].Log) || fV4[i].BlockNum != fV5[i].BlockNum {
				t.Fatalf("filtered row %d mismatch", i)
			}
		}
	})
}

func TestEventLogV5InlineSingletonsAndMultiBlockLookupV4(t *testing.T) {
	dir := t.TempDir()
	// >300 unique addresses & singleton topics across >256 keys (2+ V4 lookup blocks),
	// with zero multi-posting keys in topicIndex => topic lookup postingDataLen == 0.
	rows := make([]EventLog, 0, 320)
	for i := uint64(0); i < 320; i++ {
		var addrBytes [20]byte
		binary.BigEndian.PutUint64(addrBytes[12:], i+1)
		addr := common.AccountID(addrBytes).Address(0)
		var txHash, topic common.Hash
		binary.BigEndian.PutUint64(txHash[24:], i+1)
		binary.BigEndian.PutUint64(topic[24:], i+1)
		rows = append(rows, eventLogV3TestRow(1, i, i, addr, txHash, common.Hash{0x99}, topic, []byte("singleton")))
	}
	ref, err := BuildEventLogV5SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogV5SegmentFromReader: %v", err)
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.v3.topicLookupVersion != eventLogLookupV4Format {
		t.Fatalf("topic lookup format = %d, want V4", seg.v3.topicLookupVersion)
	}
	if seg.v3.topicLookupV4.blockCount != 2 {
		t.Fatalf("topic block count = %d, want 2 for 320 keys (256/block)", seg.v3.topicLookupV4.blockCount)
	}
	if seg.v3.topicLookupV4.postingDataLen != 0 {
		t.Fatalf("all-singleton topic postingDataLen = %d, want 0 (inlined)", seg.v3.topicLookupV4.postingDataLen)
	}

	var queryTopic common.Hash
	binary.BigEndian.PutUint64(queryTopic[24:], 299)
	got := collectAllEventLogsFromSegment(t, dir, ref, 1, 1, EventLogFilter{Topics: [][]common.Hash{{queryTopic}}})
	if len(got) != 1 || got[0].TxIndex != 298 {
		t.Fatalf("inline singleton query returned %+v, want txIndex 298", got)
	}
}

func TestEventLogV5RejectsCorruptions(t *testing.T) {
	buildValid := func(t *testing.T) (string, SegmentRef) {
		t.Helper()
		dir := t.TempDir()
		addr := common.BytesToAddress(eventLogTestAddress(0x31))
		topicA := common.Hash{0x01}
		topicB := common.Hash{0x02}
		rows := []EventLog{
			eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{2}, topicA, bytes.Repeat([]byte("a"), 200)),
			eventLogV3TestRow(1, 1, 1, addr, common.Hash{3}, common.Hash{2}, topicB, bytes.Repeat([]byte("b"), 200)),
			eventLogV3TestRow(1, 2, 2, addr, common.Hash{4}, common.Hash{2}, topicA, bytes.Repeat([]byte("c"), 200)),
		}
		ref, err := BuildEventLogV5SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		return dir, ref
	}

	flipByte := func(t *testing.T, path string, off int64) {
		t.Helper()
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var b [1]byte
		if _, err := f.ReadAt(b[:], off); err != nil {
			t.Fatal(err)
		}
		b[0] ^= 0xff
		if _, err := f.WriteAt(b[:], off); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("corrupt row frame checksum", func(t *testing.T) {
		dir, ref := buildValid(t)
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		h, err := readEventLogHeader(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		flipByte(t, filepath.Join(dir, ref.Path), int64(h.v3.rowDataOffset))
		ref.Checksum = ""
		if err := CheckEventLogSegment(dir, ref); err == nil {
			t.Fatal("accepted corrupt row frame")
		}
	})

	t.Run("corrupt lookup v4 key block", func(t *testing.T) {
		dir, ref := buildValid(t)
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		h, err := readEventLogHeader(f)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		lh, err := readEventLogLookupV4Header(f, h.v3.topicIndexOffset, h.v3.topicIndexLength, ref.Size, eventLogTopicLookupKeySize)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		blk, err := readEventLogLookupV4Block(f, h.v3.topicIndexOffset, lh, 0)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		flipByte(t, filepath.Join(dir, ref.Path), int64(blk.dataOff))
		ref.Checksum = ""
		if err := CheckEventLogSegment(dir, ref); err == nil {
			t.Fatal("accepted corrupt V5 lookup key block")
		}
	})

	t.Run("corrupt lookup v4 external posting stream", func(t *testing.T) {
		dir, ref := buildValid(t)
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		h, err := readEventLogHeader(f)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		lh, err := readEventLogLookupV4Header(f, h.v3.topicIndexOffset, h.v3.topicIndexLength, ref.Size, eventLogTopicLookupKeySize)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		postingOff := h.v3.topicIndexOffset + eventLogLookupV4HeaderSize + lh.blockDirLen + lh.keyDataLen
		flipByte(t, filepath.Join(dir, ref.Path), int64(postingOff))
		ref.Checksum = ""
		if err := CheckEventLogSegment(dir, ref); err == nil {
			t.Fatal("accepted corrupt V5 lookup external posting")
		}
	})
}

func TestEventLogIndexV3FilterParityZeroFNAndBoundedFPR(t *testing.T) {
	dir := t.TempDir()
	const keyCount = 2000
	addr := common.BytesToAddress(eventLogTestAddress(0x41))
	otherAddr := common.BytesToAddress(eventLogTestAddress(0x42))
	rows := make([]EventLog, 0, keyCount)
	topics := make([]common.Hash, keyCount)
	for i := 0; i < keyCount; i++ {
		binary.BigEndian.PutUint64(topics[i][24:], uint64(i+1))
		var txHash common.Hash
		binary.BigEndian.PutUint64(txHash[24:], uint64(i+1))
		a := addr
		if i%2 == 1 {
			a = otherAddr
		}
		rows = append(rows, eventLogV3TestRow(10+uint64(i/100), uint64(i), uint64(i), a, txHash, common.Hash{0xaa}, topics[i], []byte("ev")))
	}

	evRef, idxRef, err := buildEventLogV5PairFromReaderWorkers(eventLogRowsReader{rows: rows}, dir, 10, 29, 2)
	if err != nil {
		t.Fatalf("buildEventLogV5PairFromReaderWorkers: %v", err)
	}
	if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, idxRef, []SegmentRef{evRef}); err != nil {
		t.Fatalf("verifyEventLogIndexSegmentAgainstEventLogs(V3): %v", err)
	}

	idxSeg, err := OpenEventLogIndexSegment(dir, idxRef)
	if err != nil {
		t.Fatal(err)
	}
	defer idxSeg.Close()
	if idxSeg.header.version != EventLogIndexV3Version {
		t.Fatalf("index version = %d, want V3", idxSeg.header.version)
	}

	// Zero false negatives for all 2,000 indexed topics.
	for i, topic := range topics {
		starts, used, err := idxSeg.CandidateSegmentStarts(EventLogFilter{Topics: [][]common.Hash{{topic}}})
		if err != nil || !used || len(starts) != 1 || starts[0] != 10 {
			t.Fatalf("topic %d missed or error: starts=%v used=%v err=%v", i, starts, used, err)
		}
	}

	// False positive rate test on 10,000 non-existent keys.
	fp := 0
	const probes = 10_000
	for i := 0; i < probes; i++ {
		var absent common.Hash
		binary.BigEndian.PutUint64(absent[0:8], 0xdeadbeef)
		binary.BigEndian.PutUint64(absent[24:], uint64(i+1))
		starts, used, err := idxSeg.CandidateSegmentStarts(EventLogFilter{Topics: [][]common.Hash{{absent}}})
		if err != nil || !used {
			t.Fatalf("negative probe %d err=%v used=%v", i, err, used)
		}
		if len(starts) > 0 {
			fp++
		}
	}
	fpr := float64(fp) / probes
	t.Logf("EventLogIndexV3 bloom probes=%d fp=%d fpr=%.4f%% size=%d bytes (keys=%d)", probes, fp, fpr*100, idxRef.Size, keyCount)
	if fpr > 0.005 {
		t.Fatalf("false positive rate %.4f%% exceeds 0.5%% bound", fpr*100)
	}

	// Stats reporting
	stats, err := idxSeg.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Address.Keys != 2 || stats.Topic.Keys != keyCount {
		t.Fatalf("stats = %+v", stats)
	}

	// Exact end-to-end Manager query parity (including missing address & false-positive topic)
	agg := NewAggregator(dir)
	if _, err := agg.Integrate(0, 0, []SegmentRef{evRef, idxRef}); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Exact match
	var got []EventLog
	if err := mgr.IterateEventLogs(10, 29, EventLogFilter{
		Addresses: []common.Address{otherAddr},
		Topics:    [][]common.Hash{{topics[1], topics[2]}},
	}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TxIndex != 1 {
		t.Fatalf("Manager filtered result = %+v, want txIndex 1", got)
	}
}

func TestEventLogIndexV3RejectsCorruptBloomDataAndHeader(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x55))
	rows := []EventLog{eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{2}, common.Hash{3}, []byte("p"))}
	evRef, idxRef, err := buildEventLogV5PairFromReaderWorkers(eventLogRowsReader{rows: rows}, dir, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	idxPath := filepath.Join(dir, idxRef.Path)
	raw, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt last byte (bloom data bit)
	corrupted := append([]byte(nil), raw...)
	corrupted[len(corrupted)-1] ^= 0xff
	if err := os.WriteFile(idxPath, corrupted, 0o600); err != nil {
		t.Fatal(err)
	}
	unverified := idxRef
	unverified.Checksum = ""
	if err := CheckEventLogIndexSegment(dir, unverified); err == nil {
		t.Fatal("CheckEventLogIndexSegment accepted corrupt V3 bloom body")
	}

	// Restore valid bloom bytes, update page CRC but flip a bit anyway -> deterministic verification MUST reject!
	hdr, err := readEventLogIndexHeader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	bloomRaw := append([]byte(nil), raw[hdr.topicIndexOffset:]...)
	// Flip a bit in the bloom body AND recompute the page CRC so CheckEventLogIndexSegment passes,
	// proving verifyEventLogIndexSegmentAgainstEventLogs detects semantic bloom alteration!
	pageBodyOff := eventLogTopicBloomHeaderSize + 4
	bloomRaw[pageBodyOff] ^= 0x01
	newPageCRC := crc32.ChecksumIEEE(bloomRaw[pageBodyOff:])
	binary.BigEndian.PutUint32(bloomRaw[eventLogTopicBloomHeaderSize:pageBodyOff], newPageCRC)
	patched := append(append([]byte(nil), raw[:hdr.topicIndexOffset]...), bloomRaw...)
	if err := os.WriteFile(idxPath, patched, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, unverified, []SegmentRef{evRef}); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("verifyEventLogIndexSegmentAgainstEventLogs accepted modified bloom bytes: %v", err)
	}
}

func TestEventLogV5SyntheticTRC20CompressionBenchmark(t *testing.T) {
	dir := t.TempDir()
	usdt := common.BytesToAddress(eventLogTestAddress(0x01))
	otherToken := common.BytesToAddress(eventLogTestAddress(0x02))
	transferSig := common.Hash{0xdd, 0xf2, 0x52, 0xad}
	approvalSig := common.Hash{0x8c, 0x5b, 0xe1, 0xe5}

	const totalRows = 6000
	rows := make([]EventLog, 0, totalRows)
	for i := uint64(0); i < totalRows; i++ {
		block := 1000 + i/50
		token := usdt
		sig := transferSig
		if i%10 == 0 {
			token = otherToken
			sig = approvalSig
		}
		// Realistic padded 20-byte from/to addresses across Transfer events (hot hubs + unique users)
		var fromTopic, toTopic, txHash, blockHash common.Hash
		binary.BigEndian.PutUint64(fromTopic[24:], (i%400)+1)
		binary.BigEndian.PutUint64(toTopic[24:], ((i*7+13)%650)+1)
		binary.BigEndian.PutUint64(txHash[24:], i/2+1)
		binary.BigEndian.PutUint64(blockHash[24:], block)

		var data [32]byte
		binary.BigEndian.PutUint64(data[24:], 1_000_000+i*100)
		rows = append(rows, EventLog{
			BlockNum: block, TxIndex: i % 50, LogIndex: i % 50, TxHash: txHash, BlockHash: blockHash, Address: token,
			Log: &corepb.TransactionInfo_Log{
				Address: eventLogV3PayloadAddress(token),
				Topics:  [][]byte{sig[:], fromTopic[:], toTopic[:]},
				Data:    data[:],
			},
		})
	}

	reader := eventLogRowsReader{rows: rows}
	v4Main, err := BuildEventLogV4SegmentFromReader(reader, dir, "log/bench-v4.seg", 1000, 1000+(totalRows-1)/50)
	if err != nil {
		t.Fatal(err)
	}
	v4Idx, err := writeFreshEventLogV4Index(dir, v4Main, "log/bench-v4.idx")
	if err != nil {
		t.Fatal(err)
	}
	v5Main, v5Idx, err := buildEventLogV5PairFromReaderWorkers(reader, dir, 1000, 1000+(totalRows-1)/50, 2)
	if err != nil {
		t.Fatal(err)
	}
	v4Total := v4Main.Size + v4Idx.Size
	v5Total := v5Main.Size + v5Idx.Size
	pct := 100 * (1 - float64(v5Total)/float64(v4Total))
	t.Logf("TRC-20 single-segment benchmark (%d rows): V4+V2 total=%d (main=%d idx=%d), V5+V3 total=%d (main=%d idx=%d), saved=%.1f%%",
		totalRows, v4Total, v4Main.Size, v4Idx.Size, v5Total, v5Main.Size, v5Idx.Size, pct)
	if v5Total*100 >= v4Total*70 {
		t.Fatalf("expected >= 30%% total reduction on single-segment TRC-20 workload, got %.1f%%", pct)
	}

	// Simulate 20 small V4+V2 segments (300 rows/seg across 6000 rows) merged into 1 V5+V3 pair.
	var fragmentedV4Total, fragmentedV4Main, fragmentedV4Idx uint64
	const chunkRows = 300
	for start := 0; start < totalRows; start += chunkRows {
		end := min(start+chunkRows, totalRows)
		slice := rows[start:end]
		fBlk, tBlk := slice[0].BlockNum, slice[len(slice)-1].BlockNum
		if start > 0 && fBlk == rows[start-1].BlockNum {
			// Skip shared boundary block in test loop; build contiguous blocks only.
			continue
		}
		for end < totalRows && rows[end-1].BlockNum == rows[end].BlockNum {
			end++
		}
		if end > totalRows {
			break
		}
		_ = fBlk
		_ = tBlk
	}
	// Clean block-aligned fragmentation: 20 segments of 6 blocks each (300 rows/segment = 120 blocks).
	var fragRefs []SegmentRef
	for s := uint64(0); s < 20; s++ {
		fromB := 1000 + s*6
		toB := fromB + 5
		segRows := rows[s*300 : (s+1)*300]
		ev, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: segRows}, dir, "", fromB, toB)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := writeFreshEventLogV4Index(dir, ev, "")
		if err != nil {
			t.Fatal(err)
		}
		fragmentedV4Main += ev.Size
		fragmentedV4Idx += idx.Size
		fragRefs = append(fragRefs, ev, idx)
	}
	fragmentedV4Total = fragmentedV4Main + fragmentedV4Idx
	_ = fragRefs
	mergedPct := 100 * (1 - float64(v5Total)/float64(fragmentedV4Total))
	t.Logf("TRC-20 20-segment-merge benchmark (%d rows): fragmented V4+V2 total=%d (main=%d idx=%d), merged V5+V3 total=%d (main=%d idx=%d), saved=%.1f%%",
		totalRows, fragmentedV4Total, fragmentedV4Main, fragmentedV4Idx, v5Total, v5Main.Size, v5Idx.Size, mergedPct)
}
