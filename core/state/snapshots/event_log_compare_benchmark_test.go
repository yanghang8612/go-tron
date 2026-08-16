package snapshots

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEventLogComparisonFixture(t *testing.T) {
	const rowCount = 4096
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x84))
	rows := make([]EventLog, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		topics := make([][]byte, 4)
		for position := range topics {
			var topic common.Hash
			binary.BigEndian.PutUint64(topic[24:], uint64(position*rowCount+i))
			topics[position] = append([]byte(nil), topic[:]...)
		}
		var txHash common.Hash
		binary.BigEndian.PutUint64(txHash[24:], uint64(i+1))
		rows = append(rows, EventLog{BlockNum: 1, TxIndex: uint64(i), LogIndex: uint64(i), TxHash: txHash, BlockHash: common.Hash{2}, Address: address, Log: &corepb.TransactionInfo_Log{
			Address: eventLogV3PayloadAddress(address), Topics: topics, Data: bytes.Repeat([]byte{byte(i)}, 128),
		}})
	}
	started := time.Now()
	mainRef, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{mainRef}, "")
	if err != nil {
		t.Fatal(err)
	}
	build := time.Since(started)
	physical, err := InspectEventLogV4Physical(dir, mainRef)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := OpenEventLogSegment(dir, mainRef)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	var wanted common.Hash
	binary.BigEndian.PutUint64(wanted[24:], 3073)
	filtered := EventLogFilter{Topics: [][]common.Hash{{wanted}}}
	bench := func(filter EventLogFilter) testing.BenchmarkResult {
		return testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if err := seg.IterateLogs(1, 1, filter, func(EventLog) (bool, error) { return true, nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	full, filteredResult := bench(EventLogFilter{}), bench(filtered)
	t.Logf("FORMAT=V4/V2 main=%d sidecar=%d total=%d build_ns=%d payload_compressed=%d topic_lookup=%d full_ns=%d full_B=%d full_allocs=%d filtered_ns=%d filtered_B=%d filtered_allocs=%d",
		mainRef.Size, indexRef.Size, mainRef.Size+indexRef.Size, build.Nanoseconds(), physical.PayloadCompressedBytes, physical.TopicLookupBytes,
		full.NsPerOp(), full.AllocedBytesPerOp(), full.AllocsPerOp(), filteredResult.NsPerOp(), filteredResult.AllocedBytesPerOp(), filteredResult.AllocsPerOp())
}
