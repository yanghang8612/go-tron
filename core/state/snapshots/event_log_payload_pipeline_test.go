package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func eventPayloadPipelineFixture(count, dataBytes int) []EventLog {
	random := rand.New(rand.NewSource(908))
	rows := make([]EventLog, count)
	for i := range rows {
		address := common.BytesToAddress(eventLogTestAddress(byte(i%8 + 1)))
		var hash, topic common.Hash
		binary.BigEndian.PutUint64(hash[24:], uint64(i+1))
		binary.BigEndian.PutUint64(topic[24:], uint64(i%64))
		data := make([]byte, dataBytes)
		random.Read(data[:len(data)/2])
		copy(data[len(data)/2:], data[:len(data)/2])
		log := &corepb.TransactionInfo_Log{Address: eventLogV3PayloadAddress(address), Topics: [][]byte{topic[:]}, Data: data}
		// Unknown protobuf fields must survive the payload projection and query.
		log.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, byte(i % 128)})
		rows[i] = EventLog{BlockNum: uint64(i/32 + 1), TxIndex: uint64(i % 32), LogIndex: uint64(i % 32), TxHash: hash, BlockHash: common.Hash{byte(i/32 + 1)}, Address: address, Log: log}
	}
	return rows
}

func TestEventPayloadProjectionMatchesCloneAndPreservesSource(t *testing.T) {
	for _, row := range eventPayloadPipelineFixture(16, 1024) {
		before := proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		want := proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		want.Address, want.Topics = nil, nil
		got := eventLogPayloadProjection(row.Log)
		gotRaw, err := proto.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		wantRaw, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotRaw, wantRaw) || !proto.Equal(before, row.Log) {
			t.Fatal("payload projection changed bytes or mutated source")
		}
	}
}

func TestEventPayloadWorkersPreserveCompleteSegmentAndQueries(t *testing.T) {
	rows := eventPayloadPipelineFixture(1024, 4096)
	var want, wantIndex SegmentRef
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			dir := t.TempDir()
			ref, err := buildEventLogV4SegmentFromReaderWorkers(eventLogRowsReader{rows: rows}, dir, "", 1, 32, workers)
			if err != nil {
				t.Fatal(err)
			}
			index, err := writeFreshEventLogV4Index(dir, ref, "")
			if err != nil {
				t.Fatal(err)
			}
			if workers == 1 {
				want, wantIndex = ref, index
			} else if ref.Checksum != want.Checksum || ref.Size != want.Size || index.Checksum != wantIndex.Checksum || index.Size != wantIndex.Size {
				t.Fatalf("workers change content addressed outputs: main=%+v index=%+v", ref, index)
			}
			seg, err := OpenEventLogSegment(dir, ref)
			if err != nil {
				t.Fatal(err)
			}
			defer seg.Close()
			seen := 0
			if err := seg.IterateLogs(1, 32, EventLogFilter{}, func(got EventLog) (bool, error) {
				want := rows[seen]
				if got.BlockNum != want.BlockNum || got.TxIndex != want.TxIndex || got.LogIndex != want.LogIndex || got.TxHash != want.TxHash || got.BlockHash != want.BlockHash || got.Address != want.Address || !proto.Equal(got.Log, want.Log) {
					t.Fatalf("row %d semantics differ", seen)
				}
				seen++
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if seen != len(rows) {
				t.Fatalf("rows=%d want=%d", seen, len(rows))
			}
			filter := EventLogFilter{Addresses: []common.Address{rows[7].Address}, Topics: [][]common.Hash{{common.BytesToHash(rows[7].Log.Topics[0])}}}
			seen = 0
			if err := seg.IterateLogs(1, 32, filter, func(got EventLog) (bool, error) {
				if got.Address != rows[7].Address || !bytes.Equal(got.Log.Topics[0], rows[7].Log.Topics[0]) {
					t.Fatal("filtered query false match")
				}
				seen++
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if seen != 16 {
				t.Fatalf("filtered rows=%d want=16", seen)
			}
		})
	}
}

func TestEventPayloadPipelineOrderedBoundedAndOversized(t *testing.T) {
	random := rand.New(rand.NewSource(908))
	inputs := make([][]byte, 90)
	for i := range inputs {
		n := eventLogV3PayloadTarget
		if i == 42 {
			n = 256 << 10
		} // larger than the queue's accepted frame
		inputs[i] = make([]byte, n)
		random.Read(inputs[i])
	}
	var want []byte
	var wantFrames []eventLogV3Frame
	for _, workers := range []int{1, 8} {
		file, err := os.Create(filepath.Join(t.TempDir(), "payload"))
		if err != nil {
			t.Fatal(err)
		}
		w := newEventLogV3PayloadWriter(file, workers)
		for i, raw := range inputs {
			frame, offset, err := w.add(uint64(i), raw)
			if err != nil {
				t.Fatal(err)
			}
			if frame != uint64(i) || offset != 0 {
				t.Fatalf("frame=%d offset=%d row=%d", frame, offset, i)
			}
		}
		if err := w.flush(uint64(len(inputs)) - uint64(w.rows)); err != nil {
			t.Fatal(err)
		}
		if err := w.drain(); err != nil {
			t.Fatal(err)
		}
		if w.pipeline != nil {
			stats := w.pipeline.stats
			if stats.Workers != workers || stats.PeakPending <= 1 || stats.PeakPending > 2*workers || stats.PeakReservedBytes > cdcMaxInflightBytes || w.pipeline.pending != 0 {
				t.Fatalf("unbounded or unused pipeline: %+v pending=%d", stats, w.pipeline.pending)
			}
		}
		w.close()
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		if workers == 1 {
			want, wantFrames = got, w.frames
		} else if !bytes.Equal(got, want) || !reflect.DeepEqual(w.frames, wantFrames) {
			t.Fatal("parallel completion changes frames or compressed bytes")
		}
	}
}

func TestEventPayloadPipelineFailureClosesWorkers(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "payload"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	w := newEventLogV3PayloadWriter(file, 4)
	boom := errors.New("injected event encoder error")
	w.encode = func(context.Context, []byte, []byte) ([]byte, error) { return nil, boom }
	defer w.close()
	raw := bytes.Repeat([]byte{7}, eventLogV3PayloadTarget)
	for i := uint64(0); i < 6; i++ {
		if _, _, err := w.add(i, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(5); err != nil {
		t.Fatal(err)
	}
	if err := w.drain(); !errors.Is(err, boom) {
		t.Fatalf("got %v want encoder failure", err)
	}
	w.close() // idempotent, workers cannot wait for a result consumer
}

func TestEventPayloadPipelineCommitsOutOfOrderWorkersInOrder(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "payload"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	w := newEventLogV3PayloadWriter(file, 2)
	defer w.close()
	encoder, decoder, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan struct{})
	w.encode = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
		if raw[0] == 3 {
			select {
			case <-secondDone:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		encoded := encoder.EncodeAll(raw, dst)
		if raw[0] == 4 {
			close(secondDone)
		}
		return encoded, nil
	}
	for i := uint64(0); i < 6; i++ {
		if _, _, err := w.add(i, bytes.Repeat([]byte{byte(i)}, eventLogV3PayloadTarget)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(5); err != nil {
		t.Fatal(err)
	}
	if err := w.drain(); err != nil {
		t.Fatal(err)
	}
	for i, frame := range w.frames {
		encoded := make([]byte, frame.dataLen)
		if _, err := file.ReadAt(encoded, int64(frame.dataOff)); err != nil {
			t.Fatal(err)
		}
		got, err := decoder.DecodeAll(encoded, make([]byte, 0, eventLogV3PayloadTarget))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, eventLogV3PayloadTarget)) {
			t.Fatalf("frame %d order/decode: %v", i, err)
		}
	}
}

func TestEventPayloadPipelineWriteFailureClosesWorkers(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "payload"))
	if err != nil {
		t.Fatal(err)
	}
	w := newEventLogV3PayloadWriter(file, 4)
	defer w.close()
	for i := uint64(0); i < 6; i++ {
		if _, _, err := w.add(i, bytes.Repeat([]byte{byte(i)}, eventLogV3PayloadTarget)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(5); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.drain(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("got %v want closed output failure", err)
	}
	w.close()
}

// The same source rows, mandatory main validation and fresh sidecar are built
// in each case. This is a CPU/format fixture with warm local files, not a claim
// about mainnet IO or complete import/cold duty-cycle throughput.
func BenchmarkEventPayloadCompleteBuild(b *testing.B) {
	rows := eventPayloadPipelineFixture(4096, 4096)
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			dir := b.TempDir()
			b.ReportAllocs()
			b.SetBytes(4096 * 4096)
			for i := 0; i < b.N; i++ {
				ref, err := buildEventLogV4SegmentFromReaderWorkers(eventLogRowsReader{rows: rows}, dir, "", 1, 128, workers)
				if err != nil {
					b.Fatal(err)
				}
				index, err := writeFreshEventLogV4Index(dir, ref, "")
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(ref.Size+index.Size), "output_bytes")
			}
		})
	}
}
