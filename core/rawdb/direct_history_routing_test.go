package rawdb

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var directHistoryRouteSink bool

func TestDirectHistoryByteRoutingMatchesString(t *testing.T) {
	var keys [][]byte
	for _, r := range DirectHistoryRanges() {
		for n := 0; n <= len(r.Start); n++ {
			keys = append(keys, bytes.Clone(r.Start[:n]))
		}
		keys = append(keys, r.Start, r.End, append(bytes.Clone(r.Start), 0), append(bytes.Clone(r.Start), 0xff), append(bytes.Clone(r.Start), 'x'))
	}
	keys = append(keys, stateAccountLatestPrefix, stateKVLatestPrefix, []byte(CommitmentBranchKeyPrefix), []byte{0xff}, nil)
	rng := rand.New(rand.NewSource(14))
	for i := 0; i < 1000; i++ {
		key := make([]byte, rng.Intn(160))
		rng.Read(key)
		keys = append(keys, key)
	}
	for _, key := range keys {
		if got, want := IsDirectHistoryKey(key), isDirectHistoryKeyString(string(key)); got != want {
			t.Fatalf("key %x routed history=%v, want %v", key, got, want)
		}
	}
	long := StateKVLatestCommitmentKey(common.Address{}, 0, kvdomains.ContractStorage, make([]byte, 32))
	if n := testing.AllocsPerRun(1000, func() { directHistoryRouteSink = IsDirectHistoryKey(long) }); n != 0 {
		t.Fatalf("byte routing allocs=%v", n)
	}
}

func BenchmarkDirectHistoryByteRouting(b *testing.B) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"storage", StateKVLatestCommitmentKey(common.Address{}, 0, kvdomains.ContractStorage, make([]byte, 32))},
		{"branch", append(bytes.Clone([]byte(CommitmentBranchKeyPrefix)), make([]byte, 64)...)},
		{"history", append(bytes.Clone(stateChangeKeyDirectoryPrefix), make([]byte, 64)...)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				directHistoryRouteSink = IsDirectHistoryKey(tc.key)
			}
		})
	}
}

type directRoutingSnapshot struct {
	opens, closes       int
	cursorErr, closeErr error
}

func (s *directRoutingSnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	s.opens++
	if s.cursorErr != nil {
		return nil, s.cursorErr
	}
	return &directRoutingCursor{prefix: bytes.Clone(prefix), snapshot: s}, nil
}
func (s *directRoutingSnapshot) Close() error { s.closes++; return s.closeErr }

type directRoutingCursor struct {
	prefix   []byte
	snapshot *directRoutingSnapshot
}

func (c *directRoutingCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if !bytes.HasPrefix(key, c.prefix) {
		return false, nil
	}
	return true, fn(key)
}
func (c *directRoutingCursor) Close() error { return c.snapshot.closeErr }

func TestDirectHistoryCursorPrefixPartition(t *testing.T) {
	singleton := stageProgressKey(StageStateHistoryIndex)
	tests := []struct {
		name         string
		prefix       []byte
		hot, history int
	}{
		{"empty", nil, 1, 1}, {"partial", []byte("state-"), 1, 1}, {"stage", stageProgressPrefix, 1, 1},
		{"singleton", singleton, 1, 1}, {"singleton_extension", append(bytes.Clone(singleton), 0), 1, 0},
		{"storage", stateKVLatestPrefix, 1, 0}, {"branch", []byte(CommitmentBranchKeyPrefix), 1, 0}, {"ff", []byte{0xff}, 1, 0},
	}
	for _, r := range DirectHistoryRanges()[:len(DirectHistoryRanges())-1] {
		tests = append(tests, struct {
			name         string
			prefix       []byte
			hot, history int
		}{r.Name, r.Start, 0, 1})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hot, history := new(directRoutingSnapshot), new(directRoutingSnapshot)
			s := &directHistoryPointSnapshot{hot: hot, history: history}
			cursor, err := s.NewCursor(tc.prefix)
			if err != nil {
				t.Fatal(err)
			}
			if hot.opens != tc.hot || history.opens != tc.history {
				t.Fatalf("opened hot/history %d/%d want %d/%d", hot.opens, history.opens, tc.hot, tc.history)
			}
			key := append(bytes.Clone(tc.prefix), 'k')
			called := false
			if ok, err := cursor.View(key, func(v []byte) error { called = true; return nil }); !ok || err != nil || !called {
				t.Fatalf("bounded read %v/%v callback=%v", ok, err, called)
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if hot.closes != 1 || history.closes != 1 {
				t.Fatalf("snapshot closes %d/%d", hot.closes, history.closes)
			}
		})
	}
}

func TestDirectHistoryCursorCreationErrors(t *testing.T) {
	openErr, closeErr := errors.New("open history"), errors.New("close hot")
	hot, history := &directRoutingSnapshot{closeErr: closeErr}, &directRoutingSnapshot{cursorErr: openErr}
	s := &directHistoryPointSnapshot{hot: hot, history: history}
	if _, err := s.NewCursor(nil); !errors.Is(err, openErr) || !errors.Is(err, closeErr) {
		t.Fatalf("dual cleanup error %v", err)
	}
	if _, err := s.NewCursor(stateChangeSetPrefix); !errors.Is(err, openErr) {
		t.Fatalf("single cursor error %v", err)
	}
}

func TestDirectHistorySingleCursorPinnedRouting(t *testing.T) {
	dir := t.TempDir()
	hot, history := directTestPebble(t, filepath.Join(dir, "hot")), directTestPebble(t, filepath.Join(dir, "history"))
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	hotKey := StateKVLatestCommitmentKey(common.Address{}, 0, kvdomains.ContractStorage, []byte("row"))
	historyKey := append(bytes.Clone(stateChangeSetPrefix), 'a')
	directTestPut(t, db, hotKey, []byte("hot-old"))
	directTestPut(t, db, historyKey, []byte("history-old"))
	directTestPut(t, history, hotKey, []byte("wrong-history"))
	directTestPut(t, hot, historyKey, []byte("wrong-hot"))
	snapshot, err := db.NewPointReadSnapshotWithCapacity(2)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, tc := range []struct{ prefix, key, want []byte }{
		{stateKVLatestPrefix, hotKey, []byte("hot-old")},
		{stateChangeSetPrefix, historyKey, []byte("history-old")},
	} {
		cursor, err := snapshot.NewCursor(tc.prefix)
		if err != nil {
			t.Fatal(err)
		}
		directTestPut(t, db, tc.key, []byte("replacement"))
		found, err := cursor.View(tc.key, func(value []byte) error {
			if !bytes.Equal(value, tc.want) {
				t.Errorf("pinned row %q, want %q", value, tc.want)
			}
			return nil
		})
		if err != nil || !found {
			t.Fatalf("pinned read=%v/%v", found, err)
		}
		outsideFound, outsideErr := cursor.View([]byte("outside-prefix"), func([]byte) error { t.Error("out-of-prefix callback"); return nil })
		if outsideFound {
			t.Fatal("out-of-prefix key found")
		} else if os.Getenv("GTRON_PEBBLE_BOUNDED_POINT_READ") == "get" {
			if outsideErr != nil {
				t.Fatalf("get outside prefix: %v", outsideErr)
			}
		} else if outsideErr == nil || !strings.Contains(outsideErr.Error(), "SeekPrefixGE supplied") {
			// The existing iterator backend rejects this lower-bound violation;
			// routing must preserve that error rather than normalizing it to a miss.
			t.Fatalf("iterator outside prefix error=%v", outsideErr)
		}
		if err := cursor.Close(); !errors.Is(err, outsideErr) {
			// Iterator.Close preserves the same bounds error after a rejected seek.
			t.Fatalf("cursor close=%v, want preserved bounds error %v", err, outsideErr)
		}
	}
}

func TestDirectHistorySingletonCursorPinnedRouting(t *testing.T) {
	dir := t.TempDir()
	hot, history := directTestPebble(t, filepath.Join(dir, "hot")), directTestPebble(t, filepath.Join(dir, "history"))
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	singleton := stageProgressKey(StageStateHistoryIndex)
	keys := [][]byte{singleton, append(bytes.Clone(singleton), 0), append(bytes.Clone(singleton), []byte("-suffix")...)}
	wants := [][]byte{[]byte("history-singleton"), []byte("hot-zero"), []byte("hot-suffix")}
	for i, key := range keys {
		directTestPut(t, db, key, wants[i])
		shadow := history
		if IsDirectHistoryKey(key) {
			shadow = hot
		}
		directTestPut(t, shadow, key, []byte("wrong-engine-shadow"))
	}
	snapshot, err := db.NewPointReadSnapshotWithCapacity(len(singleton) + 3)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, key := range keys {
		directTestPut(t, db, key, []byte("after-snapshot"))
	}
	// Every ancestor, including the exact singleton prefix, must keep dual
	// routing: its keyspace contains both the history singleton and hot suffixes.
	for n := 0; n <= len(singleton); n++ {
		cursor, err := snapshot.NewCursor(singleton[:n])
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			found, err := cursor.View(key, func(value []byte) error {
				if !bytes.Equal(value, wants[i]) {
					t.Errorf("prefix %q key %q value=%q want=%q", singleton[:n], key, value, wants[i])
				}
				return nil
			})
			if err != nil || !found {
				t.Fatalf("prefix %q key %q found/error=%v/%v", singleton[:n], key, found, err)
			}
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
