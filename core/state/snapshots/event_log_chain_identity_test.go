package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Use canonical rawdb block and receipt storage, including contract-bearing
// transaction raw data. Payload-only readers omit precisely the protobuf
// allocations and canonical transaction hashing that this cache avoids.
func eventIdentityChainFixture(t testing.TB, blocks, txsPerBlock, contractsPerTx, contractBytes int) *rawdb.ChainDB {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	t.Cleanup(func() { _ = db.Close() })
	for number := uint64(1); number <= uint64(blocks); number++ {
		txs := make([]*corepb.Transaction, txsPerBlock)
		infos := make([]*corepb.TransactionInfo, txsPerBlock)
		for i := range txs {
			ordinal := (number-1)*uint64(txsPerBlock) + uint64(i)
			contracts := make([]*corepb.Transaction_Contract, contractsPerTx)
			for j := range contracts {
				data := bytes.Repeat([]byte{byte(ordinal + uint64(j))}, contractBytes)
				binary.BigEndian.PutUint64(data[:8], ordinal)
				parameter, err := anypb.New(&contractpb.TriggerSmartContract{OwnerAddress: append([]byte{common.AddressPrefixMainnet}, eventLogTestAddress(byte(i))...), ContractAddress: append([]byte{common.AddressPrefixMainnet}, eventLogTestAddress(byte(j))...), Data: data})
				if err != nil {
					t.Fatal(err)
				}
				contracts[j] = &corepb.Transaction_Contract{Type: corepb.Transaction_Contract_TriggerSmartContract, Parameter: parameter}
			}
			txs[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(ordinal + 10_000), Expiration: int64(ordinal + 100_000), RefBlockBytes: []byte{byte(number >> 8), byte(number)}, Contract: contracts}, Signature: [][]byte{bytes.Repeat([]byte{byte(ordinal)}, 65)}}
			hash := coretypes.NewTransactionFromPB(txs[i]).Hash()
			var topic, second common.Hash
			binary.BigEndian.PutUint64(topic[24:], ordinal%8)
			binary.BigEndian.PutUint64(second[24:], ordinal%3)
			log := &corepb.TransactionInfo_Log{Address: eventLogTestAddress(byte(ordinal % 16)), Topics: [][]byte{topic[:], second[:]}, Data: bytes.Repeat([]byte{byte(ordinal)}, 128)}
			log.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, byte(ordinal % 128)})
			infos[i] = &corepb.TransactionInfo{Id: append([]byte(nil), hash[:]...), BlockNumber: int64(number), BlockTimeStamp: int64(number + 20_000)}
			if i%7 != 0 {
				infos[i].Log = []*corepb.TransactionInfo_Log{log}
			}
		}
		block := coretypes.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number + 20_000), ParentHash: bytes.Repeat([]byte{byte(number - 1)}, 32)}}, Transactions: txs})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, number, infos); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func collectEventIdentityRows(t testing.TB, reader rawdb.EventLogReader, from, to uint64, filter EventLogFilter) []EventLog {
	t.Helper()
	var rows []EventLog
	if err := reader.IterateEventLogs(from, to, filter, func(row EventLog) (bool, error) {
		row.Log = proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertEventIdentityRows(t testing.TB, want, got []EventLog) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("rows=%d want=%d", len(got), len(want))
	}
	for i, row := range got {
		expected := want[i]
		if row.BlockNum != expected.BlockNum || row.TxIndex != expected.TxIndex || row.LogIndex != expected.LogIndex || row.BlockHash != expected.BlockHash || row.TxHash != expected.TxHash || row.Address != expected.Address || !proto.Equal(row.Log, expected.Log) {
			t.Fatalf("row %d changed canonical identity or complete log payload", i)
		}
	}
}

func TestEventChainIdentityCachePreservesCompleteBuildAndQueries(t *testing.T) {
	const blocks = 8
	db := eventIdentityChainFixture(t, blocks, 16, 3, 256)
	baselineReader := eventLogV3ChainReader{chain: db}
	wantRows := collectEventIdentityRows(t, baselineReader, 1, blocks, EventLogFilter{})
	var wantSegment, wantIndex []byte
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", enabled), func(t *testing.T) {
			dir := t.TempDir()
			reader := eventLogV3ChainReader{chain: db}
			if enabled {
				reader.identities = newEventLogChainIdentityCache()
			}
			ref, err := buildEventLogV4SegmentFromReaderWorkers(reader, dir, "", 1, blocks, 1)
			if err != nil {
				t.Fatal(err)
			}
			index, err := writeFreshEventLogV4Index(dir, ref, "")
			if err != nil {
				t.Fatal(err)
			}
			segmentBytes, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			indexBytes, err := os.ReadFile(filepath.Join(dir, index.Path))
			if err != nil {
				t.Fatal(err)
			}
			if !enabled {
				wantSegment, wantIndex = segmentBytes, indexBytes
			} else if !bytes.Equal(wantSegment, segmentBytes) || !bytes.Equal(wantIndex, indexBytes) || len(reader.identities.blocks) != blocks {
				t.Fatal("cache changed complete segment/index or failed to cover fixture blocks")
			}
			if err := CheckEventLogSegment(dir, ref); err != nil {
				t.Fatal(err)
			}
			if err := CheckEventLogIndexSegment(dir, index); err != nil {
				t.Fatal(err)
			}
			if err := PublishManifest(dir, NewManifest(1, blocks, []SegmentRef{ref, index})); err != nil {
				t.Fatal(err)
			}
			manager, err := OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			assertEventIdentityRows(t, wantRows, collectEventIdentityRows(t, manager, 1, blocks, EventLogFilter{}))
			filters := []EventLogFilter{{}, {Addresses: []common.Address{{0xff}}}}
			for i := 0; i < 16; i++ {
				filters = append(filters, EventLogFilter{Addresses: []common.Address{eventLogAddress(eventLogTestAddress(byte(i)))}})
			}
			for i := uint64(0); i < 8; i++ {
				var topic common.Hash
				binary.BigEndian.PutUint64(topic[24:], i)
				filters = append(filters, EventLogFilter{Topics: [][]common.Hash{{topic}}})
			}
			filters = append(filters, EventLogFilter{Addresses: []common.Address{wantRows[0].Address, wantRows[1].Address}, Topics: [][]common.Hash{nil, {common.BytesToHash(wantRows[0].Log.Topics[1])}}})
			for _, filter := range filters {
				want := collectEventIdentityRows(t, baselineReader, 2, blocks-1, filter)
				assertEventIdentityRows(t, want, collectEventIdentityRows(t, manager, 2, blocks-1, filter))
			}
			seen := 0
			if err := manager.IterateEventLogs(1, blocks, EventLogFilter{}, func(EventLog) (bool, error) { seen++; return false, nil }); err != nil || seen != 1 {
				t.Fatalf("short-circuit seen=%d err=%v", seen, err)
			}
		})
	}
}

func TestEventChainIdentityCacheBuildsFromAncientWithoutHotRows(t *testing.T) {
	for _, version := range []string{"v1", "v2_compact"} {
		t.Run(version, func(t *testing.T) {
			const blocks = 7 // genesis plus seven logged blocks forms one V2 segment
			hot := eventIdentityChainFixture(t, blocks, 16, 2, 128)
			baseline := eventLogV3ChainReader{chain: hot}
			filters := []EventLogFilter{{}, {Addresses: []common.Address{eventLogAddress(eventLogTestAddress(1))}}, {Topics: [][]common.Hash{{common.Hash{}}}}}
			wanted := make([][]EventLog, len(filters))
			for i, filter := range filters {
				wanted[i] = collectEventIdentityRows(t, baseline, 1, blocks, filter)
			}
			baselineDir := t.TempDir()
			wantRef, err := buildEventLogV4SegmentFromReaderWorkers(baseline, baselineDir, "", 1, blocks, 1)
			if err != nil {
				t.Fatal(err)
			}
			wantIndex, err := writeFreshEventLogV4Index(baselineDir, wantRef, "")
			if err != nil {
				t.Fatal(err)
			}

			store := openChainFreezerTestStore(t, filepath.Join(t.TempDir(), "ancient"))
			defer store.Close()
			genesis, _ := eventLogTestBlock(t, 0, nil)
			rows := []chainFreezerRawTestRow{{block: genesis}}
			for number := uint64(1); number <= blocks; number++ {
				block, ok, err := rawdb.ReadBlockStrict(hot, number)
				if err != nil || !ok {
					t.Fatalf("hot block %d: found=%t err=%v", number, ok, err)
				}
				infos, ok, err := rawdb.ReadTransactionInfosRawStrict(hot, number)
				if err != nil || !ok {
					t.Fatalf("hot receipts %d: found=%t err=%v", number, ok, err)
				}
				rows = append(rows, chainFreezerRawTestRow{block: block, txInfosRaw: infos})
			}
			appendChainFreezerRawRows(t, store, rows)
			if version == "v2_compact" {
				result, err := store.MigrateV2(rawdbfreezer.V2MigrationOptions{
					Tables:        []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable},
					SegmentBlocks: blocks + 1, FrameBlocks: 2, MaxSegments: 1, CompressionWorkers: 1,
					Transform: rawdb.CompactAncientV2Record,
				})
				if err != nil || result.End != blocks+1 || store.V2Coverage() != blocks+1 || store.V1Tail() != blocks+1 {
					t.Fatalf("real V2 migration/reclamation: result=%+v coverage=%d tail=%d err=%v", result, store.V2Coverage(), store.V1Tail(), err)
				}
			}
			if err := rawdb.DeleteFrozenBlockRange(hot, 1, blocks); err != nil {
				t.Fatal(err)
			}
			for number := uint64(1); number <= blocks; number++ {
				body, infos, err := rawdb.HasHotFrozenBlockRows(hot, number)
				if err != nil || body || infos {
					t.Fatalf("hot rows remain at %d: body=%t infos=%t err=%v", number, body, infos, err)
				}
			}
			chain := rawdb.NewChainDB(hot.KeyValueStore, rawdb.NewFreezerReader(store))
			reader := &eventIdentityMutatingReader{eventLogV3ChainReader: eventLogV3ChainReader{chain: chain, identities: newEventLogChainIdentityCache()}, mutate: func() {}}
			dir := t.TempDir()
			ref, err := buildEventLogV4SegmentFromReaderWorkers(reader, dir, "", 1, blocks, 4)
			if err != nil {
				t.Fatal(err)
			}
			index, err := writeFreshEventLogV4Index(dir, ref, "")
			if err != nil {
				t.Fatal(err)
			}
			if reader.passes != 2 || len(reader.identities.blocks) != blocks {
				t.Fatalf("ancient two-pass cache not exercised: passes=%d cached=%d", reader.passes, len(reader.identities.blocks))
			}
			if ref.Checksum != wantRef.Checksum || ref.Size != wantRef.Size || index.Checksum != wantIndex.Checksum || index.Size != wantIndex.Size {
				t.Fatal("ancient-only build changed complete main/index bytes")
			}
			if err := PublishManifest(dir, NewManifest(1, blocks, []SegmentRef{ref, index})); err != nil {
				t.Fatal(err)
			}
			manager, err := OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			for i, filter := range filters {
				assertEventIdentityRows(t, wanted[i], collectEventIdentityRows(t, manager, 1, blocks, filter))
			}
		})
	}
}

type eventIdentityMutatingReader struct {
	eventLogV3ChainReader
	passes int
	mutate func()
}

func (r *eventIdentityMutatingReader) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.passes++
	if r.passes == 2 {
		r.mutate()
	}
	return r.eventLogV3ChainReader.IterateEventLogs(from, to, filter, fn)
}

// Capture the key actually written by rawdb rather than duplicating its schema
// prefixes in corruption fixtures.
func eventIdentityStoredValueKey(t *testing.T, db *rawdb.ChainDB, value []byte) []byte {
	t.Helper()
	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		if bytes.Equal(it.Value(), value) {
			return append([]byte(nil), it.Key()...)
		}
	}
	t.Fatalf("source value not found: iterator err=%v", it.Error())
	return nil
}

func TestEventChainIdentityCacheRejectsSecondPassSourceDamage(t *testing.T) {
	for _, damage := range []string{"body_signature_changed", "body_missing", "body_corrupt", "receipt_missing", "receipt_corrupt", "receipt_id", "receipt_count", "receipt_block_number", "receipt_id_length"} {
		t.Run(damage, func(t *testing.T) {
			db := eventIdentityChainFixture(t, 2, 4, 2, 64)
			body := rawdb.ReadBlockRaw(db, 1)
			bodyKey := eventIdentityStoredValueKey(t, db, body)
			receiptKey := eventIdentityStoredValueKey(t, db, rawdb.ReadTransactionInfosRaw(db, 1))
			reader := &eventIdentityMutatingReader{eventLogV3ChainReader: eventLogV3ChainReader{chain: db, identities: newEventLogChainIdentityCache()}}
			reader.mutate = func() {
				var err error
				switch damage {
				case "body_signature_changed":
					pb := new(corepb.Block)
					if err = proto.Unmarshal(body, pb); err != nil {
						t.Fatal(err)
					}
					pb.Transactions[0].Signature[0][0] ^= 1
					var changed []byte
					changed, err = proto.Marshal(pb)
					if err == nil {
						err = db.Put(bodyKey, changed)
					}
				case "body_missing":
					err = db.Delete(bodyKey)
				case "body_corrupt":
					err = db.Put(bodyKey, []byte{0xff})
				case "receipt_missing":
					err = rawdb.DeleteTransactionInfosByBlock(db, 1)
				case "receipt_corrupt":
					err = db.Put(receiptKey, []byte{0xff})
				default:
					infos, _, readErr := rawdb.ReadTransactionInfosByBlockStrict(db, 1)
					if readErr != nil {
						t.Fatal(readErr)
					}
					switch damage {
					case "receipt_id":
						infos[0].Id = bytes.Repeat([]byte{0xee}, common.HashLength)
					case "receipt_count":
						infos = infos[:len(infos)-1]
					case "receipt_block_number":
						infos[0].BlockNumber = 99
					case "receipt_id_length":
						infos[0].Id = []byte{1}
					}
					var encoded []byte
					encoded, err = proto.Marshal(&corepb.TransactionRet{BlockNumber: 1, Transactioninfo: infos})
					if err == nil {
						err = db.Put(receiptKey, encoded)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			if _, err := buildEventLogV4SegmentFromReaderWorkers(reader, dir, "", 1, 2, 1); err == nil {
				t.Fatal("damaged second-pass source was published")
			}
			if reader.passes != 2 {
				t.Fatalf("passes=%d, want mutation during second pass", reader.passes)
			}
			if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					t.Errorf("failed build retained file %s", path)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEventChainIdentityCacheGenesisAndCompactReceiptSemantics(t *testing.T) {
	db := eventIdentityChainFixture(t, 1, 4, 2, 64)
	genesis, _ := eventLogTestBlock(t, 0, nil)
	if err := rawdb.WriteBlock(db, genesis); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, []*corepb.TransactionInfo{nil}); err == nil || !strings.Contains(err.Error(), "nil transaction info") {
		t.Fatalf("nil receipt writer error=%v", err)
	}
	infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCompactTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatal(err)
	}
	baseline := eventLogV3ChainReader{chain: db}
	cached := eventLogV3ChainReader{chain: db, identities: newEventLogChainIdentityCache()}
	want := collectEventIdentityRows(t, baseline, 0, 1, EventLogFilter{})
	for pass := 0; pass < 2; pass++ {
		assertEventIdentityRows(t, want, collectEventIdentityRows(t, cached, 0, 1, EventLogFilter{}))
	}
	if len(cached.identities.blocks) != 2 || len(cached.identities.blocks[0].transactions) != 1 {
		t.Fatal("genesis allocation transaction identity was dropped")
	}
}

func TestEventChainIdentityCacheResourceBoundsAndFallback(t *testing.T) {
	for _, limit := range []string{"bytes", "blocks"} {
		t.Run(limit, func(t *testing.T) {
			cache := newEventLogChainIdentityCache()
			if limit == "bytes" {
				identity := eventLogChainIdentity{transactions: make([]common.Hash, 1024)}
				for n := uint64(1); n <= 2048; n++ {
					cache.put(n, identity)
				}
			} else {
				for n := uint64(1); n <= eventLogIdentityMaxBlocks+1; n++ {
					cache.put(n, eventLogChainIdentity{})
				}
			}
			if cache.bytes > eventLogIdentityMaxBytes || len(cache.blocks) > eventLogIdentityMaxBlocks {
				t.Fatal("identity cache exceeded fixed resource bounds")
			}
			if limit == "blocks" && len(cache.blocks) != eventLogIdentityMaxBlocks {
				t.Fatal("block limit not exercised")
			}
			if limit == "bytes" && cache.bytes+(1024*common.HashLength+eventLogIdentityBlockCharge) <= eventLogIdentityMaxBytes {
				t.Fatal("byte limit not exercised")
			}
		})
	}
	db := eventIdentityChainFixture(t, 2, 4, 2, 64)
	for _, configure := range []func(*eventLogChainIdentityCache){
		func(cache *eventLogChainIdentityCache) { cache.maxBlocks = 1 },
		func(cache *eventLogChainIdentityCache) {
			cache.maxBytes = eventLogIdentityBlockCharge + 4*common.HashLength
		},
	} {
		cache := newEventLogChainIdentityCache()
		configure(cache)
		reader := eventLogV3ChainReader{chain: db, identities: cache}
		want := collectEventIdentityRows(t, eventLogV3ChainReader{chain: db}, 1, 2, EventLogFilter{})
		for pass := 0; pass < 2; pass++ {
			assertEventIdentityRows(t, want, collectEventIdentityRows(t, reader, 1, 2, EventLogFilter{}))
		}
		if len(cache.blocks) != 1 {
			t.Fatalf("cached blocks=%d want one plus fallback", len(cache.blocks))
		}
		infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(db, 2)
		if err != nil {
			t.Fatal(err)
		}
		infos[0].Id[0] ^= 1
		if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos); err != nil {
			t.Fatal(err)
		}
		if err := reader.IterateEventLogs(2, 2, EventLogFilter{}, func(EventLog) (bool, error) { return true, nil }); err == nil {
			t.Fatal("uncached fallback skipped canonical identity validation")
		}
		infos[0].Id[0] ^= 1
		if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkEventChainIdentityCacheFullBuild(b *testing.B) {
	const blocks, txs, contracts = 96, 32, 4
	db := eventIdentityChainFixture(b, blocks, txs, contracts, 256)
	var want, wantIndex SegmentRef
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("cache=%t", enabled), func(b *testing.B) {
			root := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := eventLogV3ChainReader{chain: db}
				if enabled {
					reader.identities = newEventLogChainIdentityCache()
				}
				dir := filepath.Join(root, fmt.Sprintf("run-%d", i))
				ref, err := buildEventLogV4SegmentFromReaderWorkers(reader, dir, "", 1, blocks, 1)
				if err != nil {
					b.Fatal(err)
				}
				index, err := writeFreshEventLogV4Index(dir, ref, "")
				if err != nil {
					b.Fatal(err)
				}
				if want.Checksum == "" {
					want, wantIndex = ref, index
				}
				if ref.Checksum != want.Checksum || ref.Size != want.Size || index.Checksum != wantIndex.Checksum || index.Size != wantIndex.Size {
					b.Fatal("identity cache changed full benchmark outputs")
				}
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(want.Size+wantIndex.Size), "complete-output-B/op")
			b.ReportMetric(blocks*txs, "canonical-txs/op")
		})
	}
}
