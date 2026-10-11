package rawdb

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type directPairDB struct {
	ethdb.KeyValueStore
	name            string
	events          *[]string
	putErr, syncErr error
}

func (d *directPairDB) Put(key, value []byte) error {
	*d.events = append(*d.events, d.name+".Put")
	if d.putErr != nil {
		return d.putErr
	}
	return d.KeyValueStore.Put(key, value)
}
func (d *directPairDB) SyncKeyValue() error {
	*d.events = append(*d.events, d.name+".Sync")
	return d.syncErr
}
func TestDirectHistoryPairPersistenceOrder(t *testing.T) {
	var events []string
	hot := &directPairDB{KeyValueStore: NewMemoryDatabase(), name: "hot", events: &events}
	defer hot.Close()
	history := &directPairDB{KeyValueStore: NewMemoryDatabase(), name: "history", events: &events}
	defer history.Close()
	if err := EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	if want := []string{"history.Put", "history.Sync", "hot.Put", "hot.Sync"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
	hp, has, err := ReadDirectHistoryPair(hot)
	if err != nil || !has || len(hp) != 16 {
		t.Fatalf("hot pair=%x,%v,%v", hp, has, err)
	}
	ip, has, err := ReadDirectHistoryPair(history)
	if err != nil || !has || !bytes.Equal(hp, ip) {
		t.Fatalf("history pair=%x,%v,%v", ip, has, err)
	}
	events = nil
	if err := EnsureDirectHistoryPair(hot, history, false); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("validation mutated databases: %v", events)
	}
}
func TestDirectHistoryPairRejectsTargetReplacementAfterSourceDeletion(t *testing.T) {
	hot, history, other := NewMemoryDatabase(), NewMemoryDatabase(), NewMemoryDatabase()
	defer hot.Close()
	defer history.Close()
	defer other.Close()
	key := stateTxRangeKey(9)
	directTestPut(t, hot, key, []byte("history"))
	if err := EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	// Represent one durable page already moved before the offline run stops.
	directTestPut(t, history, key, []byte("history"))
	if err := hot.Delete(key); err != nil {
		t.Fatal(err)
	}
	for _, allowCreate := range []bool{false, true} {
		if err := EnsureDirectHistoryPair(hot, other, allowCreate); err == nil {
			t.Fatal("accepted replacement history target")
		}
	}
	if err := MarkDirectHistoryLayout(other); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirectHistoryPair(hot, other, true); err == nil {
		t.Fatal("accepted unrelated marked target")
	}
	if err := EnsureDirectHistoryPair(hot, history, false); err != nil {
		t.Fatal(err)
	}
	directTestRead(t, history, key, []byte("history"))
	directTestAbsent(t, hot, key)
}
func TestDirectHistoryPairInterruptedCreationRecovery(t *testing.T) {
	sentinel := errors.New("interrupted")
	for _, failure := range []string{"history-put", "history-sync", "hot-put", "hot-sync"} {
		t.Run(failure, func(t *testing.T) {
			var events []string
			hot := &directPairDB{KeyValueStore: NewMemoryDatabase(), name: "hot", events: &events}
			defer hot.Close()
			history := &directPairDB{KeyValueStore: NewMemoryDatabase(), name: "history", events: &events}
			defer history.Close()
			switch failure {
			case "history-put":
				history.putErr = sentinel
			case "history-sync":
				history.syncErr = sentinel
			case "hot-put":
				hot.putErr = sentinel
			case "hot-sync":
				hot.syncErr = sentinel
			}
			if err := EnsureDirectHistoryPair(hot, history, true); !errors.Is(err, sentinel) {
				t.Fatalf("error=%v", err)
			}
			hot.putErr, hot.syncErr, history.putErr, history.syncErr = nil, nil, nil, nil
			_, hotPresent, err := ReadDirectHistoryPair(hot)
			if err != nil {
				t.Fatal(err)
			}
			_, historyPresent, err := ReadDirectHistoryPair(history)
			if err != nil {
				t.Fatal(err)
			}
			if historyPresent && !hotPresent {
				if err := EnsureDirectHistoryPair(hot, history, true); err == nil {
					t.Fatal("runtime creation adopted existing target")
				}
				if err := AdoptUnmarkedDirectHistoryPair(hot, history); err != nil {
					t.Fatal(err)
				}
			} else if err := EnsureDirectHistoryPair(hot, history, true); err != nil {
				t.Fatal(err)
			}
			if err := EnsureDirectHistoryPair(hot, history, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDirectHistoryPairRejectsUnpairedInitializedAndMalformedStores(t *testing.T) {
	for _, badSide := range []string{"hot-layout", "history-layout", "hot-pair", "history-pair", "missing"} {
		t.Run(badSide, func(t *testing.T) {
			hot, history := NewMemoryDatabase(), NewMemoryDatabase()
			defer hot.Close()
			defer history.Close()
			switch badSide {
			case "hot-layout":
				if err := MarkDirectHistoryLayout(hot); err != nil {
					t.Fatal(err)
				}
			case "history-layout":
				if err := MarkDirectHistoryLayout(history); err != nil {
					t.Fatal(err)
				}
			case "hot-pair":
				directTestPut(t, hot, directHistoryPairKey, []byte("short"))
			case "history-pair":
				directTestPut(t, history, directHistoryPairKey, []byte("short"))
			}
			if err := EnsureDirectHistoryPair(hot, history, badSide != "missing"); err == nil {
				t.Fatal("accepted invalid pair state")
			}
		})
	}
	hot, history := NewMemoryDatabase(), NewMemoryDatabase()
	defer hot.Close()
	defer history.Close()
	directTestPut(t, history, directHistoryPairKey, bytes.Repeat([]byte{9}, 16))
	if err := MarkDirectHistoryLayout(history); err != nil {
		t.Fatal(err)
	}
	if err := AdoptUnmarkedDirectHistoryPair(hot, history); err == nil {
		t.Fatal("adopted completed target into unrelated hot database")
	}
}

func TestDirectHistoryPairRejectsForeignUnmarkedHistory(t *testing.T) {
	for _, paired := range []bool{false, true} {
		name := "no-pair"
		if paired {
			name = "orphan-pair"
		}
		t.Run(name, func(t *testing.T) {
			hot, history := NewMemoryDatabase(), NewMemoryDatabase()
			defer hot.Close()
			defer history.Close()
			if paired {
				directTestPut(t, history, directHistoryPairKey, bytes.Repeat([]byte{7}, 16))
			}
			foreign := stateChangeSetKey(1024, 1)
			directTestPut(t, history, foreign, []byte("another source's history"))
			if err := EnsureDirectHistoryPair(hot, history, true); err == nil {
				t.Fatal("paired foreign unmarked history")
			}
			if paired {
				if err := AdoptUnmarkedDirectHistoryPair(hot, history); err == nil {
					t.Fatal("adopted foreign partial migration")
				}
			}
			if _, present, err := ReadDirectHistoryPair(hot); err != nil || present {
				t.Fatalf("source was paired on rejection: present=%v err=%v", present, err)
			}
			directTestRead(t, history, foreign, []byte("another source's history"))
		})
	}
}
