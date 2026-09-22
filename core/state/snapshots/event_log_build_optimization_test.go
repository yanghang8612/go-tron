package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// The fixture crosses a 256-row frame boundary and alternates zero, short,
// repeated and >8-topic rows so topic storage can grow within a frame.
func eventLogV4BuildRegressionRows() []EventLog {
	rows := make([]EventLog, 270)
	for i := range rows {
		blockNum := uint64(i/30 + 1)
		txIndex := uint64(i%30) / 3
		var txHash, blockHash common.Hash
		binary.BigEndian.PutUint64(txHash[24:], blockNum*10+txIndex)
		binary.BigEndian.PutUint64(blockHash[24:], blockNum)
		address := common.BytesToAddress(eventLogTestAddress(byte(i%7 + 1)))
		var topics [][]byte
		for position := range []int{0, 1, 4, 9, 2}[i%5] {
			var topic common.Hash
			binary.BigEndian.PutUint64(topic[24:], uint64((i+position)%13))
			if position == 2 {
				copy(topic[:], topics[0])
			}
			topics = append(topics, append([]byte(nil), topic[:]...))
		}
		data := bytes.Repeat([]byte{byte(i)}, 13+i%17)
		if i%17 == 0 {
			data = bytes.Repeat([]byte{byte(i)}, 512)
		}
		log := &corepb.TransactionInfo_Log{Address: eventLogV3PayloadAddress(address), Topics: topics, Data: data}
		log.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, byte(i % 128)})
		rows[i] = EventLog{
			BlockNum: blockNum, TxIndex: txIndex, LogIndex: uint64(i % 30),
			TxHash: txHash, BlockHash: blockHash, Address: address, Log: log,
		}
	}
	return rows
}

func TestEventLogV4BuildPostingAndTopicStorageMatchesBaseline(t *testing.T) {
	rows := eventLogV4BuildRegressionRows()
	for _, workers := range []int{1, 4} {
		dir := t.TempDir()
		ref, err := buildEventLogV4SegmentFromReaderWorkers(eventLogRowsReader{rows: rows}, dir, "", 1, 9, workers)
		if err != nil {
			t.Fatal(err)
		}
		index, err := writeFreshEventLogV4Index(dir, ref, "")
		if err != nil {
			t.Fatal(err)
		}
		// Frozen from the unoptimized V4 writer at HEAD on Go 1.25.5.
		if ref.Size != 11284 || ref.Checksum != "sha256:2d965be82d67c946b76561eb98d01be9c722f402cb3011fd4b135887ebc9cdc9" {
			t.Fatalf("workers=%d main output changed: %+v", workers, ref)
		}
		if index.Size != 1970 || index.Checksum != "sha256:733239873e1c5e8e142be1b2d575152f5c76b755552823e7edcfb4d2b49c50d7" {
			t.Fatalf("workers=%d index output changed: %+v", workers, index)
		}
		seg, err := OpenEventLogSegment(dir, ref)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		if err := seg.IterateLogs(1, 9, EventLogFilter{}, func(got EventLog) (bool, error) {
			want := rows[seen]
			if got.BlockNum != want.BlockNum || got.TxIndex != want.TxIndex || got.LogIndex != want.LogIndex || got.TxHash != want.TxHash || got.BlockHash != want.BlockHash || got.Address != want.Address || !proto.Equal(got.Log, want.Log) {
				t.Fatalf("row %d differs", seen)
			}
			seen++
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		if seen != len(rows) {
			t.Fatalf("got %d rows, want %d", seen, len(rows))
		}
		filter := EventLogFilter{Addresses: []common.Address{rows[17].Address}, Topics: [][]common.Hash{{common.BytesToHash(rows[17].Log.Topics[0])}}}
		var wantFiltered, gotFiltered int
		for _, row := range rows {
			if row.Address == filter.Addresses[0] && len(row.Log.Topics) > 0 && common.BytesToHash(row.Log.Topics[0]) == filter.Topics[0][0] {
				wantFiltered++
			}
		}
		if wantFiltered == 0 {
			t.Fatal("filter fixture has no matching rows")
		}
		if err := seg.IterateLogs(1, 9, filter, func(EventLog) (bool, error) { gotFiltered++; return true, nil }); err != nil {
			t.Fatal(err)
		}
		if gotFiltered != wantFiltered {
			t.Fatalf("filtered got %d, want %d", gotFiltered, wantFiltered)
		}
		if err := seg.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type eventLogV4SecondPassReader struct {
	rows   []EventLog
	passes int
	change func([]EventLog) []EventLog
	fail   error
}

func (r *eventLogV4SecondPassReader) EventLogRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func (r *eventLogV4SecondPassReader) IterateEventLogs(_ uint64, _ uint64, _ EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.passes++
	rows := r.rows
	if r.passes == 2 {
		if r.fail != nil {
			return r.fail
		}
		rows = r.change(rows)
	}
	for _, row := range rows {
		cont, err := fn(row)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func TestEventLogV4BuildRetainsSecondPassSourceChecks(t *testing.T) {
	rows := eventLogV4BuildRegressionRows()[:2]
	for _, tc := range []struct {
		name, want string
		change     func([]EventLog) []EventLog
	}{
		{name: "fewer rows", want: "source changed between passes", change: func(rows []EventLog) []EventLog { return rows[:1] }},
		{name: "missing dictionary", want: "missing V4 topic dictionary entry", change: func(rows []EventLog) []EventLog {
			changed := append([]EventLog(nil), rows...)
			copyLog := proto.Clone(rows[1].Log).(*corepb.TransactionInfo_Log)
			copyLog.Topics[0] = bytes.Repeat([]byte{0xee}, common.HashLength)
			changed[1].Log = copyLog
			return changed
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &eventLogV4SecondPassReader{rows: rows, change: tc.change}
			_, err := buildEventLogV4SegmentFromReaderWorkers(reader, t.TempDir(), "", 1, 9, 1)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
	boom := errors.New("injected second-pass source error")
	reader := &eventLogV4SecondPassReader{rows: rows, fail: boom}
	_, err := buildEventLogV4SegmentFromReaderWorkers(reader, t.TempDir(), "", 1, 9, 1)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want source error", err)
	}
}

func TestEventLogV4BuildSecondPassKeyOmissionMatchesBaseline(t *testing.T) {
	rows := eventLogV4BuildRegressionRows()[:2]
	reader := &eventLogV4SecondPassReader{rows: rows, change: func(rows []EventLog) []EventLog {
		changed := append([]EventLog(nil), rows...)
		changed[1].Address = rows[0].Address
		copyLog := proto.Clone(rows[1].Log).(*corepb.TransactionInfo_Log)
		copyLog.Address = eventLogV3PayloadAddress(rows[0].Address)
		copyLog.Topics = nil
		changed[1].Log = copyLog
		return changed
	}}
	ref, err := buildEventLogV4SegmentFromReaderWorkers(reader, t.TempDir(), "", 1, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	// HEAD writer omitted the address/topic keys that disappeared in pass two.
	if ref.Size != 609 || ref.Checksum != "sha256:48a6d0ca785c5d28dfaaac585b953e6526494bb5a64e87241c8926d95200aee9" {
		t.Fatalf("second-pass key omission changed output: %+v", ref)
	}
}
