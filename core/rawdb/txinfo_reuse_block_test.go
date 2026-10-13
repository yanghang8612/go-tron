package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type receiptPinnedTestStore struct {
	ethdb.KeyValueStore
	gets                  int
	pinned                bool
	bodyReads, failBodyAt int
	fail                  error
}

func (s *receiptPinnedTestStore) Get(key []byte) ([]byte, error) {
	s.gets++
	if bytes.Equal(key, blockKey(7)) {
		s.bodyReads++
		if s.failBodyAt > 0 && s.bodyReads == s.failBodyAt {
			return nil, s.fail
		}
	}
	return s.KeyValueStore.Get(key)
}
func (s *receiptPinnedTestStore) IsPinnedKeyValueView() bool { return s.pinned }

func TestStrictReceiptPinnedBlockReuse(t *testing.T) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 1)
	log := &corepb.TransactionInfo_Log{Address: []byte{0x31}, Data: bytes.Repeat([]byte{0xaa}, 512)}
	encoded, err := marshalTransactionInfosByBlock(7, []*corepb.TransactionInfo{{Id: hashes[0][:], BlockNumber: 7, Log: []*corepb.TransactionInfo_Log{log}}}, block.Timestamp())
	if err != nil {
		t.Fatal(err)
	}
	external, _, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil {
		t.Fatal(err)
	}
	store := &receiptPinnedTestStore{KeyValueStore: NewMemoryDatabase(), pinned: true}
	defer store.Close()
	if err := WriteBlock(store, block); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosRaw(store, 7, external); err != nil {
		t.Fatal(err)
	}
	chain := NewChainDB(store, NoopAncient{})
	row := EventLog{BlockNum: 7, TxIndex: 0, LogIndex: 0, TxHash: hashes[0], BlockHash: block.Hash(), Address: testChainDBEventLogAddress(0x31), Log: log}
	reader := &recordingCoveredEventLogReader{covered: true, rows: []EventLog{row}}
	chain.SetEventLogReader(reader)
	store.gets = 0
	old, ok, err := ReadTransactionInfosByBlockStrict(chain, 7)
	if err != nil || !ok {
		t.Fatal(err)
	}
	oldGets := store.gets
	store.gets = 0
	_, _, got, ok, err := ReadBlockAndTransactionInfosStrict(chain, 7)
	if err != nil || !ok || !proto.Equal(old[0], got[0]) {
		t.Fatalf("reuse differs: %v", err)
	}
	if store.gets != oldGets {
		t.Fatalf("combined pinned read repeated body reads: %d want %d", store.gets, oldGets)
	}
	store.pinned = false
	store.gets = 0
	_, _, _, _, err = ReadBlockAndTransactionInfosStrict(chain, 7)
	if err != nil || store.gets != oldGets+1 {
		t.Fatalf("live path changed: reads %d want %d err %v", store.gets, oldGets, err)
	}
	injected := errors.New("second live body read failed")
	store.bodyReads, store.failBodyAt, store.fail = 0, 2, injected
	if _, _, _, _, err := ReadBlockAndTransactionInfosStrict(chain, 7); !errors.Is(err, injected) {
		t.Fatalf("live hydration error lost: %v", err)
	}
	store.failBodyAt = 0

	store.pinned = true
	for name, mutate := range map[string]func(*EventLog){"body-hash": func(r *EventLog) { r.BlockHash[0] ^= 1 }, "tx-hash": func(r *EventLog) { r.TxHash[0] ^= 1 }, "ordinal": func(r *EventLog) { r.LogIndex++ }, "tx-index": func(r *EventLog) { r.TxIndex++ }} {
		t.Run(name, func(t *testing.T) {
			bad := row
			mutate(&bad)
			reader.rows = []EventLog{bad}
			if _, _, _, _, err := ReadBlockAndTransactionInfosStrict(chain, 7); err == nil {
				t.Fatal("invalid external identity accepted")
			}
		})
	}
	reader.rows = nil
	if _, _, _, _, err := ReadBlockAndTransactionInfosStrict(chain, 7); err == nil {
		t.Fatal("missing external rows accepted")
	}
}

func TestStrictBlockReceiptCombinationErrors(t *testing.T) {
	if _, _, _, ok, err := ReadBlockAndTransactionInfosStrict(nil, 7); err == nil || ok {
		t.Fatal("nil source accepted")
	}
	store := &receiptPinnedTestStore{KeyValueStore: NewMemoryDatabase(), pinned: true}
	defer store.Close()
	chain := NewChainDB(store, NoopAncient{})
	if _, _, _, ok, err := ReadBlockAndTransactionInfosStrict(chain, 7); err != nil || ok {
		t.Fatal("missing body changed")
	}
	if err := store.Put(blockKey(7), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosRaw(store, 7, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := ReadBlockAndTransactionInfosStrict(chain, 7); err == nil || !ok || !strings.Contains(err.Error(), "block 7 decode") {
		t.Fatalf("body error must precede receipt error: %v", err)
	}
	block, _ := testChainDBEventLogBlockWithTransactions(7, 1)
	if err := WriteBlock(store, block); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosRaw(store, 7, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := ReadBlockAndTransactionInfosStrict(chain, 7); err == nil || !ok {
		t.Fatal("malformed receipts accepted")
	}
}
