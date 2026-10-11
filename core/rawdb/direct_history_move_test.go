package rawdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestDirectHistoryMoveResumesAndSelectsAuthoritativeLegacyRows(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	history := directTestPebble(t, t.TempDir())
	defer history.Close()
	ctx := context.Background()
	for bucket, owner := range map[uint64]HistoryStagingOwner{1: HistoryStagingOwnerSource, 2: HistoryStagingOwnerTarget, 3: HistoryStagingOwnerCold} {
		route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: 1, WriteVersion: 1, Owner: owner, ReceiptDigest: [32]byte{9}}
		if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), route); err != nil {
			t.Fatal(err)
		}
	}
	keys := make(map[uint64][]byte)
	for bucket := uint64(1); bucket <= 4; bucket++ {
		first, _, _ := StateHistoryChunkBucketBounds(bucket)
		key := stateChangeSetKey(first, 1)
		keys[bucket] = key
		if bucket != 4 {
			directTestPut(t, m.hot, key, []byte("source"))
		}
		directTestPut(t, m.stage, historyStagingPayloadKey(1, key), []byte("target"))
		// Tx ranges remain authoritative in hot even for TARGET/COLD buckets.
		if err := WriteStateTxRange(m.hot, first, common.Hash{byte(bucket)}, first, first+1); err != nil {
			t.Fatal(err)
		}
		directTestPut(t, m.stage, historyStagingPayloadKey(1, stateTxRangeKey(first)), []byte("stale tx range"))
	}
	directTestPut(t, m.hot, stageProgressKey(StageStateHistoryIndex), []byte("index"))
	directTestPut(t, m.hot, stageProgressKey(StageFinish), []byte("finish"))
	interrupted := errors.New("stop after first durable page")
	pages := 0
	err := MoveDirectHistory(ctx, m.hot, m.stage, history, 1, func(DirectHistoryMoveProgress) error { pages++; return interrupted })
	if !errors.Is(err, interrupted) || pages != 1 {
		t.Fatalf("interrupted move pages=%d err=%v", pages, err)
	}
	directTestAbsent(t, m.hot, keys[1])
	directTestRead(t, history, keys[1], []byte("source"))
	if marked, err := DirectHistoryLayoutInitialized(m.hot); err != nil || marked {
		t.Fatalf("partial source marked=%v err=%v", marked, err)
	}
	// Replacing the destination after a page was deleted must fail immediately.
	wrongTarget := NewMemoryDatabase()
	defer wrongTarget.Close()
	if err := MoveDirectHistory(ctx, m.hot, m.stage, wrongTarget, 1, nil); err == nil {
		t.Fatal("resumed interrupted move into a replacement target")
	}
	if err := MoveDirectHistory(ctx, m.hot, m.stage, history, 1, nil); err != nil {
		t.Fatal(err)
	}
	directTestRead(t, history, keys[1], []byte("source"))
	directTestRead(t, history, keys[2], []byte("target"))
	directTestAbsent(t, history, keys[3])
	directTestAbsent(t, history, keys[4])
	for bucket, key := range keys {
		directTestAbsent(t, m.hot, key)
		directTestAbsent(t, m.stage, historyStagingPayloadKey(1, key))
		first, _, _ := StateHistoryChunkBucketBounds(bucket)
		rangeRow, ok, err := ReadStateTxRange(history, first)
		if err != nil || !ok || rangeRow.BlockHash != (common.Hash{byte(bucket)}) {
			t.Fatalf("canonical tx range bucket %d: %+v,%v,%v", bucket, rangeRow, ok, err)
		}
	}
	directTestRead(t, history, stageProgressKey(StageStateHistoryIndex), []byte("index"))
	directTestAbsent(t, m.hot, stageProgressKey(StageStateHistoryIndex))
	directTestRead(t, m.hot, stageProgressKey(StageFinish), []byte("finish"))
	for _, db := range []interface {
		Has([]byte) (bool, error)
		Get([]byte) ([]byte, error)
	}{m.hot, history} {
		if marked, err := DirectHistoryLayoutInitialized(db); err != nil || !marked {
			t.Fatalf("completed marked=%v err=%v", marked, err)
		}
	}
	if err := MoveDirectHistory(ctx, m.hot, m.stage, history, 1, nil); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
}

func TestDirectHistoryMoveIdentityFailureKeepsSource(t *testing.T) {
	m, closeStores, _, _, identity := stagingProtocolStores(t)
	defer closeStores()
	history := NewMemoryDatabase()
	defer history.Close()
	key := stateChangeSetKey(1024, 1)
	directTestPut(t, m.hot, key, []byte("source"))
	identity.TargetID[0] ^= 0xff
	if err := writeHistoryStagingValue(m.stage, historyStagingIdentityKey, identity); err != nil {
		t.Fatal(err)
	}
	if err := MoveDirectHistory(context.Background(), m.hot, m.stage, history, 1, nil); err == nil {
		t.Fatal("accepted wrong legacy identity")
	}
	directTestRead(t, m.hot, key, []byte("source"))
	directTestAbsent(t, history, key)
}

func TestDirectHistoryMoveDestinationSyncFailureKeepsSource(t *testing.T) {
	hot := directTestPebble(t, t.TempDir())
	defer hot.Close()
	var events []string
	history := &directPairDB{KeyValueStore: directTestPebble(t, t.TempDir()), name: "history", events: &events}
	defer history.Close()
	if err := EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	key := stateChangeSetKey(1024, 1)
	directTestPut(t, hot, key, []byte("source"))
	sentinel := errors.New("destination sync failure")
	history.syncErr = sentinel
	if err := MoveDirectHistory(context.Background(), hot, nil, history, 1, nil); !errors.Is(err, sentinel) {
		t.Fatalf("error=%v", err)
	}
	directTestRead(t, hot, key, []byte("source"))
	if marked, err := DirectHistoryLayoutInitialized(hot); err != nil || marked {
		t.Fatalf("failed move marked=%v err=%v", marked, err)
	}
	history.syncErr = nil
	if err := MoveDirectHistory(context.Background(), hot, nil, history, 1, nil); err != nil {
		t.Fatal(err)
	}
	directTestAbsent(t, hot, key)
	directTestRead(t, history, key, []byte("source"))
}

func TestDirectHistoryMoveRejectsForeignUnmarkedTargetBeforeSourceDeletion(t *testing.T) {
	for _, paired := range []bool{false, true} {
		name := "no-pair"
		if paired {
			name = "orphan-pair"
		}
		t.Run(name, func(t *testing.T) {
			hot, history := NewMemoryDatabase(), NewMemoryDatabase()
			defer hot.Close()
			defer history.Close()
			key := stateChangeSetKey(1024, 1)
			directTestPut(t, hot, key, []byte("own source"))
			directTestPut(t, history, key, []byte("foreign source"))
			if paired {
				directTestPut(t, history, directHistoryPairKey, bytes.Repeat([]byte{8}, 16))
			}
			if err := MoveDirectHistory(context.Background(), hot, nil, history, 1, nil); err == nil {
				t.Fatal("moved into foreign unmarked history")
			}
			directTestRead(t, hot, key, []byte("own source"))
			directTestRead(t, history, key, []byte("foreign source"))
			if _, present, err := ReadDirectHistoryPair(hot); err != nil || present {
				t.Fatalf("source mutated: pair present=%v err=%v", present, err)
			}
		})
	}
}

func TestDirectHistoryMovePageRangeKeepsUnexaminedAndSingletonNeighbors(t *testing.T) {
	hot, history := NewMemoryDatabase(), NewMemoryDatabase()
	defer hot.Close()
	defer history.Close()
	first := stateChangeSetKey(1, 1)
	second := append(bytes.Clone(first), 0)
	third := stateChangeSetKey(2, 1)
	for _, key := range [][]byte{first, second, third} {
		directTestPut(t, hot, key, []byte("value"))
	}
	singleton := stageProgressKey(StageStateHistoryIndex)
	neighbor := append(bytes.Clone(singleton), 0)
	directTestPut(t, hot, singleton, []byte("index"))
	directTestPut(t, hot, neighbor, []byte("keep neighbor"))
	stop := errors.New("page stop")
	err := MoveDirectHistory(context.Background(), hot, nil, history, 1, func(DirectHistoryMoveProgress) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	directTestAbsent(t, hot, first)
	directTestRead(t, hot, second, []byte("value"))
	directTestRead(t, hot, third, []byte("value"))
	if err := MoveDirectHistory(context.Background(), hot, nil, history, 1, nil); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{first, second, third} {
		directTestAbsent(t, hot, key)
		directTestRead(t, history, key, []byte("value"))
	}
	directTestRead(t, history, singleton, []byte("index"))
	directTestRead(t, hot, neighbor, []byte("keep neighbor"))
}
