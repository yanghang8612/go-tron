package rawdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateDomainReadersSurfaceStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x90)

	if err := WriteStateAccountLatest(db, owner, []byte("account-envelope")); err != nil {
		t.Fatalf("write account latest: %v", err)
	}
	if err := WriteStateKVLatest(db, owner, 3, kvdomains.ContractStorage, []byte("slot"), []byte("value")); err != nil {
		t.Fatalf("write kv latest: %v", err)
	}
	if err := WriteStateKVGeneration(db, owner, 3); err != nil {
		t.Fatalf("write kv generation: %v", err)
	}
	if err := WriteStateTxRange(db, 7, common.Hash{0x07}, 70, 72); err != nil {
		t.Fatalf("write tx range: %v", err)
	}
	if err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum:   7,
		BlockHash:  common.Hash{0x07},
		TxNum:      70,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("value"),
	}); err != nil {
		t.Fatalf("write domain change: %v", err)
	}

	readers := []struct {
		name string
		read func(ethdb.KeyValueReader) (bool, error)
	}{
		{
			name: "account latest",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadStateAccountLatest(r, owner)
				return ok, err
			},
		},
		{
			name: "kv latest",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadStateKVLatest(r, owner, 3, kvdomains.ContractStorage, []byte("slot"))
				return ok, err
			},
		},
		{
			name: "kv generation",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadStateKVGeneration(r, owner)
				return ok, err
			},
		},
		{
			name: "tx range",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadStateTxRange(r, 7)
				return ok, err
			},
		},
		{
			name: "domain change",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadStateDomainChange(r, 7, 1)
				return ok, err
			},
		},
	}

	for _, tc := range readers {
		t.Run(tc.name+"/has", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "presence") {
				t.Fatalf("has error: ok=%v err=%v", ok, err)
			}
		})
		t.Run(tc.name+"/get", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, getErr: errors.New("get boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "get boom") {
				t.Fatalf("get error: ok=%v err=%v", ok, err)
			}
		})
	}
}

type failingStateDomainReader struct {
	reader ethdb.KeyValueReader
	hasErr error
	getErr error
}

func (r failingStateDomainReader) Has(key []byte) (bool, error) {
	if r.hasErr != nil {
		return false, r.hasErr
	}
	return r.reader.Has(key)
}

func (r failingStateDomainReader) Get(key []byte) ([]byte, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.reader.Get(key)
}
