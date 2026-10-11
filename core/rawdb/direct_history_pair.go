package rawdb

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// ReadDirectHistoryPair reads the constant-size identity that binds one hot
// database to its history database. It does not authenticate their contents.
func ReadDirectHistoryPair(db ethdb.KeyValueReader) ([]byte, bool, error) {
	value, present, err := readPresentValue(db, directHistoryPairKey, "direct history pair")
	if err != nil || !present {
		return nil, false, err
	}
	if len(value) != 16 {
		return nil, false, errors.New("rawdb: invalid direct history pair")
	}
	return bytes.Clone(value), true, nil
}

// EnsureDirectHistoryPair validates the two physical database identities before
// runtime access or any offline source deletion. Creation requires two unpaired,
// unmarked databases and persists history's identity before hot's identity.
// Callers must own both databases exclusively while creating their pair.
func EnsureDirectHistoryPair(hot, history ethdb.KeyValueStore, allowCreate bool) error {
	if hot == nil || history == nil {
		return errors.New("rawdb: direct history pair requires both databases")
	}
	hotPair, hotPresent, err := ReadDirectHistoryPair(hot)
	if err != nil {
		return err
	}
	historyPair, historyPresent, err := ReadDirectHistoryPair(history)
	if err != nil {
		return err
	}
	if hotPresent {
		if !historyPresent || !bytes.Equal(hotPair, historyPair) {
			return errors.New("rawdb: direct history database pair does not match")
		}
		return nil
	}
	if historyPresent {
		return errors.New("rawdb: history database is already paired; explicit offline recovery is required")
	}
	if err := requireUnmarkedDirectHistoryPair(hot, history); err != nil {
		return err
	}
	if !allowCreate {
		return errors.New("rawdb: direct history database pair is missing")
	}
	if err := requireEmptyDirectHistoryTarget(history, false); err != nil {
		return err
	}
	pair := make([]byte, 16)
	if _, err := rand.Read(pair); err != nil {
		return fmt.Errorf("rawdb: create direct history pair: %w", err)
	}
	if err := history.Put(directHistoryPairKey, pair); err != nil {
		return fmt.Errorf("rawdb: write history pair: %w", err)
	}
	if err := history.SyncKeyValue(); err != nil {
		return fmt.Errorf("rawdb: sync history pair: %w", err)
	}
	return persistDirectHistoryHotPair(hot, pair)
}

func requireUnmarkedDirectHistoryPair(hot, history ethdb.KeyValueReader) error {
	for _, db := range []ethdb.KeyValueReader{hot, history} {
		marked, err := DirectHistoryLayoutInitialized(db)
		if err != nil {
			return err
		}
		if marked {
			return errors.New("rawdb: initialized direct history layout has no database pair")
		}
	}
	return nil
}
func persistDirectHistoryHotPair(hot ethdb.KeyValueStore, pair []byte) error {
	if err := hot.Put(directHistoryPairKey, pair); err != nil {
		return fmt.Errorf("rawdb: write hot history pair: %w", err)
	}
	if err := hot.SyncKeyValue(); err != nil {
		return fmt.Errorf("rawdb: sync hot history pair: %w", err)
	}
	return nil
}

// A new target must be empty. Recovery between the two identity writes may
// contain the history pair alone: no source page can have been deleted yet.
// There is at most one allowed row, so this checks at most two iterator rows.
func requireEmptyDirectHistoryTarget(history ethdb.Iteratee, allowPair bool) error {
	it := history.NewIterator(nil, nil)
	defer it.Release()
	if !it.Next() {
		return it.Error()
	}
	if allowPair && bytes.Equal(it.Key(), directHistoryPairKey) && !it.Next() {
		return it.Error()
	}
	return errors.New("rawdb: unpaired history target contains existing data")
}

// AdoptUnmarkedDirectHistoryPair is explicit offline recovery for interruption
// between persisting history's pair and persisting hot's pair. The caller must
// deliberately select the original history database; normal startup must never
// adopt an existing history identity. Both layouts must still be unmarked.
func AdoptUnmarkedDirectHistoryPair(hot, history ethdb.KeyValueStore) error {
	if hot == nil || history == nil {
		return errors.New("rawdb: direct history pair requires both databases")
	}
	_, hotPresent, err := ReadDirectHistoryPair(hot)
	if err != nil {
		return err
	}
	if hotPresent {
		return EnsureDirectHistoryPair(hot, history, false)
	}
	if err := requireUnmarkedDirectHistoryPair(hot, history); err != nil {
		return err
	}
	pair, present, err := ReadDirectHistoryPair(history)
	if err != nil {
		return err
	}
	if !present {
		return errors.New("rawdb: history database has no pair to recover")
	}
	if err := requireEmptyDirectHistoryTarget(history, true); err != nil {
		return err
	}
	if err := history.SyncKeyValue(); err != nil {
		return fmt.Errorf("rawdb: sync recovered history pair: %w", err)
	}
	return persistDirectHistoryHotPair(hot, pair)
}
