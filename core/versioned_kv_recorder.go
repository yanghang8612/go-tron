package core

import (
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// transactionRecordingKVStore closes the gap between StateDB's typed access
// recorder and the narrow Context.DB surface still used directly by a few
// actuators and TVM opcodes. It preserves the optional cached/block-hash
// capabilities of the block view so canonical execution does not lose its
// existing fast paths while the versioned shadow is enabled.
type transactionRecordingKVStore struct {
	parent   actuator.BufferedKVStore
	recorder *state.TransactionAccessRecorder
}

func (db *transactionRecordingKVStore) Has(key []byte) (bool, error) {
	db.recorder.RecordRawKVRead(key)
	return db.parent.Has(key)
}

func (db *transactionRecordingKVStore) Get(key []byte) ([]byte, error) {
	db.recorder.RecordRawKVRead(key)
	return db.parent.Get(key)
}

func (db *transactionRecordingKVStore) Put(key, value []byte) error {
	if err := db.parent.Put(key, value); err != nil {
		return err
	}
	db.recorder.RecordRawKVPut(key, value)
	return nil
}

func (db *transactionRecordingKVStore) Delete(key []byte) error {
	if err := db.parent.Delete(key); err != nil {
		return err
	}
	db.recorder.RecordRawKVDelete(key)
	return nil
}

func (db *transactionRecordingKVStore) GetNoCopyCached(key []byte) ([]byte, error) {
	db.recorder.RecordRawKVRead(key)
	if reader, ok := db.parent.(cachedNoCopyKeyReader); ok {
		return reader.GetNoCopyCached(key)
	}
	return db.parent.Get(key)
}

func (db *transactionRecordingKVStore) GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error) {
	var local [64]byte
	length := len(first) + len(second)
	key := local[:0]
	if length > cap(local) {
		key = make([]byte, 0, length)
	}
	key = append(key, first...)
	key = append(key, second...)
	db.recorder.RecordRawKVRead(key)
	if reader, ok := db.parent.(cachedNoCopyKeyPartsReader); ok {
		return reader.GetNoCopyCachedKeyParts(first, second)
	}
	return db.parent.Get(key)
}

func (db *transactionRecordingKVStore) IsKeyNotFound(err error) bool {
	classifier, ok := db.parent.(keyNotFoundClassifier)
	return ok && classifier.IsKeyNotFound(err)
}

func (db *transactionRecordingKVStore) BlockHashByNumber(number uint64) (tcommon.Hash, bool) {
	if reader, ok := db.parent.(rawdb.BlockHashReader); ok {
		return reader.BlockHashByNumber(number)
	}
	return rawdb.ReadBlockHashKV(db.parent, number)
}

func (db *transactionRecordingKVStore) BlockHashByNumberStrict(number uint64) (tcommon.Hash, bool, error) {
	if reader, ok := db.parent.(rawdb.BlockHashReaderStrict); ok {
		return reader.BlockHashByNumberStrict(number)
	}
	if reader, ok := db.parent.(rawdb.BlockHashReader); ok {
		hash, found := reader.BlockHashByNumber(number)
		return hash, found, nil
	}
	return tcommon.Hash{}, false, nil
}
