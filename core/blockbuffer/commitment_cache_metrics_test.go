package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type cacheMetricsCursor struct {
	value  []byte
	before func()
	err    error
}

func (c cacheMetricsCursor) View(_ []byte, fn func([]byte) error) (bool, error) {
	if c.before != nil {
		c.before()
	}
	if c.err != nil || c.value == nil {
		return false, c.err
	}
	return true, fn(c.value)
}

func (cacheMetricsCursor) Close() error { return nil }

func cacheDiagnosticCounts() [3]int64 {
	return [3]int64{
		commitmentParentCacheNoResidentCounter.Snapshot().Count(),
		commitmentParentCacheResidentNewerCounter.Snapshot().Count(),
		commitmentParentCacheFillVersionChangedCounter.Snapshot().Count(),
	}
}

func closeAndCheckCacheDiagnostics(t *testing.T, session *commitmentParentReadSession, before, want [3]int64) {
	t.Helper()
	if got := cacheDiagnosticCounts(); got != before {
		t.Fatalf("metrics published before session close: got %v, want %v", got, before)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	after := cacheDiagnosticCounts()
	for i := range want {
		if got := after[i] - before[i]; got != want[i] {
			t.Fatalf("diagnostic %d delta=%d, want %d (all=%v)", i, got, want[i], after)
		}
	}
	if err := session.Close(); err != nil || cacheDiagnosticCounts() != after {
		t.Fatalf("second close repeated diagnostics: %v", err)
	}
}

func TestCommitmentParentCacheDiagnosticReasons(t *testing.T) {
	readErr := errors.New("durable read failed")
	for _, prefetch := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			for _, kind := range []string{"disabled", "cache_hit", "no_resident", "resident_newer", "version_changed", "read_error"} {
				t.Run(fmt.Sprintf("%s/prefetch=%v/present=%v", kind, prefetch, present), func(t *testing.T) {
					cache := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
					if kind == "disabled" {
						cache = nil
					}
					prefix, path := []byte(rawdb.CommitmentBranchKeyPrefix), []byte{1, 2, 3}
					key := append(append([]byte(nil), prefix...), path...)
					value := []byte(nil)
					if present {
						value = []byte("snapshot-value")
					}
					cursor := cacheMetricsCursor{value: value}
					session := &commitmentParentReadSession{
						cache: cache, snapshot: benchmarkCommitmentSnapshot{},
						cursors: make([]pointread.Cursor, 1), keyScratch: borrowCommitmentParentKeyScratch(1),
					}
					session.readContexts = borrowCommitmentParentReadContexts(session, 1)
					t.Cleanup(func() {
						if err := session.Close(); err != nil {
							t.Error(err)
						}
					})
					var want [3]int64
					switch kind {
					case "cache_hit":
						if present {
							testBaseReadCacheSet(cache, key, value)
						} else {
							_, _, epoch := cache.getWithEpoch(key)
							cache.setMissingIfEpoch(key, epoch)
							cache.setMissingIfEpoch(key, epoch)
						}
						cursor.before = func() { t.Fatal("resident read reached durable cursor") }
					case "no_resident":
						want[0] = 1
					case "resident_newer":
						cache.advanceVersion()
						testBaseReadCacheSet(cache, key, []byte("newer-value"))
						want[1] = 1
					case "version_changed":
						// Advance an unrelated flush generation after the miss was
						// observed, before the cursor callback (or missing result).
						cursor.before = func() { cache.advanceVersion() }
						want = [3]int64{1, 0, 1}
					case "read_error":
						cursor.before = func() { cache.advanceVersion() }
						cursor.err = readErr
						want[0] = 1 // Error paths must not count a fill attempt.
					}
					session.cursors[0] = cursor
					before := cacheDiagnosticCounts()
					var found bool
					var err error
					if prefetch {
						found, err = session.PrefetchKeyParts(0, prefix, path)
					} else {
						found, err = session.ViewKeyParts(0, prefix, path, func(got []byte, _ bool) error {
							if !bytes.Equal(got, value) {
								t.Fatalf("snapshot value changed: got %q, want %q", got, value)
							}
							return nil
						})
					}
					if kind == "read_error" {
						if found || !errors.Is(err, readErr) {
							t.Fatalf("durable error changed: found=%v err=%v", found, err)
						}
					} else if found != present || err != nil {
						t.Fatalf("result changed: found=%v err=%v, want found=%v", found, err, present)
					}
					switch kind {
					case "resident_newer":
						if got, ok, _ := cache.getWithEpoch(key); !ok || !bytes.Equal(got, []byte("newer-value")) {
							t.Fatalf("snapshot overwrote newer resident: %q, %v", got, ok)
						}
					case "version_changed":
						if _, ok, _ := cache.getWithEpoch(key); ok {
							t.Fatal("version-changed fill entered cache")
						}
					}
					closeAndCheckCacheDiagnostics(t, session, before, want)
				})
			}
		}
	}
}

func TestCommitmentParentCacheDiagnosticsSharedRecheck(t *testing.T) {
	for _, leaderPrefetch := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			t.Run(fmt.Sprintf("leader_prefetch=%v/present=%v", leaderPrefetch, present), func(t *testing.T) {
				state := &blockingCommitmentCursorState{
					started: make(chan struct{}), release: make(chan struct{}),
					present: present, value: []byte("snapshot-value"),
				}
				session := newBlockingCommitmentParentSession(t, 2, state)
				t.Cleanup(func() {
					if err := session.Close(); err != nil {
						t.Error(err)
					}
				})
				prefix, path := []byte(rawdb.CommitmentBranchKeyPrefix), []byte{1, 2, 3}
				key := append(append([]byte(nil), prefix...), path...)
				before := cacheDiagnosticCounts()
				results := make(chan error, 2)
				read := func(reader int, prefetch bool) {
					var found bool
					var err error
					if prefetch {
						found, err = session.PrefetchKeyParts(reader, prefix, path)
					} else {
						found, err = session.ViewKeyParts(reader, prefix, path, func(got []byte, _ bool) error {
							if !bytes.Equal(got, state.value) {
								return fmt.Errorf("shared value %q, want %q", got, state.value)
							}
							return nil
						})
					}
					if err == nil && found != present {
						err = fmt.Errorf("found=%v, want %v", found, present)
					}
					results <- err
				}
				go read(0, leaderPrefetch)
				<-state.started
				go read(1, !leaderPrefetch)
				waitForCommitmentParentFlightFollowers(t, session, key, 1)
				session.cache.advanceVersion()
				close(state.release)
				for range 2 {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				if state.calls.Load() != 1 {
					t.Fatalf("singleflight performed %d durable reads, want 1", state.calls.Load())
				}
				// Two initial probes plus the follower's existing recheck; both
				// leader and follower reject filling the now-older snapshot result.
				closeAndCheckCacheDiagnostics(t, session, before, [3]int64{3, 0, 2})
			})
		}
	}
}

func TestReturnCommitmentParentReadContextsClearsCacheDiagnostics(t *testing.T) {
	ctx := newCommitmentParentReadContext().(*commitmentParentReadContext)
	ctx.cacheNoResident, ctx.cacheResidentNewer, ctx.cacheFillVersionChanged = 1, 2, 3
	returnCommitmentParentReadContexts([]*commitmentParentReadContext{ctx})
	if ctx.cacheNoResident != 0 || ctx.cacheResidentNewer != 0 || ctx.cacheFillVersionChanged != 0 {
		t.Fatal("pooled context retained cache diagnostics")
	}
}
