package freezer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type v2BarrierPreparationInput struct {
	number     uint64
	data, body []byte
	result     []byte
	err        error
}

// Frozen pre-pipeline reader: fill a complete batch, prepare it, join all
// workers, then deliver it. This is a benchmark oracle, not a production mode.
type v2BarrierPreparedReader struct {
	ctx       context.Context
	next, end uint64
	workers   int
	load      func(uint64) ([]byte, []byte, error)
	prepare   func(uint64, []byte, []byte) ([]byte, error)
	batch     []v2BarrierPreparationInput
	index     int
	carry     *v2BarrierPreparationInput
}

func (r *v2BarrierPreparedReader) Close() {}

func (r *v2BarrierPreparedReader) Read(number uint64) ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if number != r.next || number >= r.end {
		return nil, fmt.Errorf("barrier read %d, expected %d", number, r.next)
	}
	if r.index == len(r.batch) {
		r.batch, r.index = r.batch[:0], 0
		var owned uint64
		for n := r.next; n < r.end && len(r.batch) < v2PreparationBatchRecords; n++ {
			if err := r.ctx.Err(); err != nil {
				return nil, err
			}
			var item v2BarrierPreparationInput
			if r.carry != nil {
				item, r.carry = *r.carry, nil
			} else {
				data, body, err := r.load(n)
				if err != nil {
					return nil, err
				}
				item = v2BarrierPreparationInput{number: n, data: data, body: body}
			}
			footprint := uint64(cap(item.data)) + uint64(cap(item.body))
			if len(r.batch) != 0 && owned+footprint > v2PreparationBatchBytes {
				r.carry = &item
				break
			}
			r.batch = append(r.batch, item)
			owned += footprint
			if owned >= v2PreparationBatchBytes {
				break
			}
		}
		var next atomic.Uint32
		run := func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(r.batch) {
					return
				}
				item := &r.batch[i]
				if err := r.ctx.Err(); err != nil {
					item.err = err
				} else {
					item.result, item.err = r.prepare(item.number, item.data, item.body)
				}
				item.data, item.body = nil, nil
			}
		}
		var wg sync.WaitGroup
		for i := 1; i < min(r.workers, len(r.batch)); i++ {
			wg.Add(1)
			go func() { defer wg.Done(); run() }()
		}
		run()
		wg.Wait()
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
	}
	item := &r.batch[r.index]
	data, err := item.result, item.err
	*item = v2BarrierPreparationInput{}
	r.index++
	r.next++
	return data, err
}

type v2PreparationBenchmarkFixture struct {
	rows map[string][][]byte
	size int64
}

func newV2PreparationBenchmarkFixture(b testing.TB, count, transactions int) v2PreparationBenchmarkFixture {
	b.Helper()
	fixture := v2PreparationBenchmarkFixture{rows: make(map[string][][]byte)}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		fixture.rows[kind] = make([][]byte, count)
	}
	rng := rand.New(rand.NewSource(20260909))
	for n := range count {
		block := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n) * 3000}}}
		ret := &corepb.TransactionRet{BlockNumber: int64(n)}
		for ordinal := range transactions {
			payload := make([]byte, 384)
			_, _ = rng.Read(payload)
			tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(n)*3000 + int64(ordinal), Data: payload}}
			block.Transactions = append(block.Transactions, tx)
			raw, err := proto.Marshal(tx.RawData)
			if err != nil {
				b.Fatal(err)
			}
			hash := sha256.Sum256(raw)
			ret.Transactioninfo = append(ret.Transactioninfo, &corepb.TransactionInfo{
				Id: hash[:], BlockNumber: int64(n),
				Log: []*corepb.TransactionInfo_Log{{Address: bytes.Repeat([]byte{byte(ordinal)}, 20), Topics: [][]byte{bytes.Repeat([]byte{0x13}, 32)}, Data: payload[:128]}},
			})
		}
		for kind, message := range map[string]proto.Message{"bodies": block, "tx_infos": ret} {
			data, err := proto.Marshal(message)
			if err != nil {
				b.Fatal(err)
			}
			fixture.rows[kind][n] = data
			fixture.size += int64(len(data))
		}
		root := sha256.Sum256(fixture.rows["bodies"][n])
		fixture.rows["state_roots"][n] = root[:]
		fixture.size += int64(len(root))
	}
	return fixture
}

// This identical CPU preparation is deliberately shared by both readers. It
// validates a deterministic protobuf corpus and preserves every input byte;
// it does not emulate Pebble or the production receipt compaction profile.
func (f v2PreparationBenchmarkFixture) prepare(kind string, number uint64, data, body []byte) ([]byte, error) {
	if kind == "state_roots" {
		return data, nil
	}
	var block corepb.Block
	blockRaw := data
	if kind == "tx_infos" {
		blockRaw = body
	}
	if err := proto.Unmarshal(blockRaw, &block); err != nil {
		return nil, err
	}
	if block.GetBlockHeader().GetRawData().GetNumber() != int64(number) {
		return nil, fmt.Errorf("wrong block number %d", number)
	}
	var ret corepb.TransactionRet
	if kind == "tx_infos" {
		if err := proto.Unmarshal(data, &ret); err != nil {
			return nil, err
		}
		if ret.BlockNumber != int64(number) || len(ret.Transactioninfo) != len(block.Transactions) {
			return nil, fmt.Errorf("wrong receipt cardinality %d", number)
		}
	}
	for ordinal, tx := range block.Transactions {
		raw, err := proto.Marshal(tx.RawData)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(raw)
		if kind == "tx_infos" && !bytes.Equal(ret.Transactioninfo[ordinal].Id, hash[:]) {
			return nil, fmt.Errorf("wrong receipt ID %d/%d", number, ordinal)
		}
	}
	return data, nil
}

func runV2PreparationBenchmark(dir, mode string, fixture v2PreparationBenchmarkFixture) (*v2Store, error) {
	const workers = 4
	ctx := context.Background()
	count := uint64(len(fixture.rows["bodies"]))
	manifest := v2Manifest{Version: v2Version, Start: 0, Count: count, FrameBlocks: 64, Tables: make(map[string]string)}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		load := func(number uint64) ([]byte, []byte, error) {
			data := bytes.Clone(fixture.rows[kind][number])
			var body []byte
			if kind == "tx_infos" {
				body = bytes.Clone(fixture.rows["bodies"][number])
			}
			return data, body, nil
		}
		prepare := func(number uint64, data, body []byte) ([]byte, error) {
			return fixture.prepare(kind, number, data, body)
		}
		readExpected := func(number uint64) ([]byte, error) {
			data, body, err := load(number)
			if err != nil {
				return nil, err
			}
			return prepare(number, data, body)
		}
		read, closeReader := readExpected, func() {}
		if kind != "state_roots" {
			if mode == "barrier_4" {
				reader := &v2BarrierPreparedReader{ctx: ctx, end: count, workers: workers, load: load, prepare: prepare}
				read, closeReader = reader.Read, reader.Close
			} else {
				reader := newV2PreparedReader(ctx, 0, count, workers, load, prepare)
				read, closeReader = reader.Read, reader.Close
			}
		}
		name := v2SegmentName(0, count)
		path := filepath.Join(dir, "v2", kind, name)
		err := func() error {
			defer closeReader()
			return writeV2TableSegmentWithWorkers(path, kind, 0, count, 64, read, readExpected, 8)
		}()
		if err != nil {
			return nil, err
		}
		if err := verifyV2Segment(ctx, path, kind, 0, count, readExpected); err != nil {
			return nil, err
		}
		manifest.Tables[kind] = name
	}
	if err := publishV2Manifest(filepath.Join(dir, "v2"), manifest); err != nil {
		return nil, err
	}
	return openV2Store(dir)
}

// Each operation writes all three tables, verifies every frame, fsyncs and
// publishes the complete manifest, then reopens it. All rows are compared outside
// the timer. Both modes use the same 1,024-block/48-tx corpus and eight encoders.
// Source is memory backed: this measures pipeline overhead/opportunity, not
// production Pebble I/O, 20M density, or importer interference.
func BenchmarkV2PreparationThreeTablePipeline(b *testing.B) {
	fixture := newV2PreparationBenchmarkFixture(b, 1024, 48)
	for _, mode := range []string{"barrier_4", "pipeline_4"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(fixture.size)
			b.ReportMetric(float64(fixture.size), "input_B")
			for range b.N {
				b.StopTimer()
				dir := b.TempDir()
				b.StartTimer()
				store, err := runV2PreparationBenchmark(dir, mode, fixture)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				var size uint64
				hash := sha256.New()
				for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
					size += store.size(kind)
					for n, want := range fixture.rows[kind] {
						got, err := store.read(kind, uint64(n))
						if err != nil || !bytes.Equal(got, want) {
							_ = store.Close()
							b.Fatalf("query %s[%d]: %v", kind, n, err)
						}
						_, _ = hash.Write(got)
					}
				}
				b.ReportMetric(float64(size), "output_B")
				b.Logf("complete 3,072-row query SHA-256: %x", hash.Sum(nil))
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
