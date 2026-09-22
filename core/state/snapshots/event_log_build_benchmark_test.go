package snapshots

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// BenchmarkEventLogV4CompleteBuild includes both source passes, lookup and
// payload encoding, file publication, hashing, and the full post-write check.
func BenchmarkEventLogV4CompleteBuild(b *testing.B) {
	for _, tc := range []struct {
		name                     string
		n, dataBytes, topicCount int
		uniqueKeys               bool
	}{
		{name: "payload_multi_log", n: 4096, dataBytes: 512, topicCount: 4},
		{name: "high_cardinality", n: 4096, dataBytes: 128, topicCount: 4, uniqueKeys: true},
		{name: "small", n: 8, dataBytes: 32, topicCount: 2},
		{name: "empty"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rows := eventLogV4BuildBenchRows(tc.n, tc.dataBytes, tc.topicCount, tc.uniqueKeys)
			reader := eventLogRowsReader{rows: rows}
			dir := b.TempDir()
			toBlock := uint64(1)
			if tc.n > 0 {
				toBlock = uint64((tc.n-1)/32 + 1)
			}
			b.ReportAllocs()
			b.SetBytes(int64(tc.n * tc.dataBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := buildEventLogV4SegmentFromReaderWorkers(reader, dir, "", 1, toBlock, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func eventLogV4BuildBenchRows(n, dataBytes, topicCount int, uniqueKeys bool) []EventLog {
	random := rand.New(rand.NewSource(20260922))
	rows := make([]EventLog, n)
	for i := range rows {
		var txHash, blockHash common.Hash
		blockNum := uint64(i/32 + 1)
		txIndex := uint64(i%32) / 4 // four logs per transaction
		binary.BigEndian.PutUint64(txHash[24:], blockNum*8+txIndex)
		binary.BigEndian.PutUint64(blockHash[24:], blockNum)
		address := common.BytesToAddress(eventLogTestAddress(byte(i%16 + 1)))
		if uniqueKeys {
			binary.BigEndian.PutUint64(address[len(address)-8:], uint64(i+1))
		}
		topics := make([][]byte, topicCount)
		for position := range topics {
			var topic common.Hash
			topicValue := uint64((position*97 + i) % 256)
			if uniqueKeys {
				topicValue = uint64(position*n + i)
			}
			binary.BigEndian.PutUint64(topic[24:], topicValue)
			topics[position] = append([]byte(nil), topic[:]...)
		}
		data := make([]byte, dataBytes)
		_, _ = random.Read(data)
		rows[i] = EventLog{
			BlockNum: blockNum, TxIndex: txIndex, LogIndex: uint64(i % 32),
			TxHash: txHash, BlockHash: blockHash, Address: address,
			Log: &corepb.TransactionInfo_Log{Address: eventLogV3PayloadAddress(address), Topics: topics, Data: data},
		}
	}
	return rows
}
