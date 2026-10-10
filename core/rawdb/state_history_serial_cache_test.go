package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
)

func TestSerialHistoryChunkCacheIntegrityAndCapabilities(t *testing.T) {
	for _, mode := range []string{"shared", "snappy-shared", "missing", "bad-chunk", "chunk-hash", "bad-digest", "bad-header"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := pipelineFixture(t, mode)
			base, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			// Hide the owned/concurrent capabilities, as routed overlays do.
			serial := struct{ StateHistoryReadView }{base}
			pack, err := db.Get(stateChangeSetKey(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := decodeStateHistorySharedPack(serial, pack, 1)
			view, cache, err := AcquireSerialStateHistoryChunkCacheView(context.Background(), serial)
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			if _, ok := view.(pointread.ConcurrentOwnedKeyValueView); ok {
				t.Fatal("serial cache granted concurrent ownership")
			}
			got, gotErr := decodeStateHistorySharedPack(view, pack, 1)
			if !bytes.Equal(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("changed integrity result: %v / %v", gotErr, wantErr)
			}
			if mode == "shared" || mode == "snappy-shared" {
				got[0] ^= 0xff
				again, err := decodeStateHistorySharedPack(view, pack, 1)
				if err != nil || !bytes.Equal(again, want) || cache.Stats().Hits == 0 {
					t.Fatal("private authenticated copies not reused", err)
				}
				if err := IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), view, 1, 1, 0, 99, borrowedStateDomainChangeNoop); !errors.Is(err, ErrStateHistoryPipelineView) {
					t.Fatal("pipeline accepted serial source", err)
				}
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := view.Get(nil); !errors.Is(err, ErrStateHistoryChunkCacheClosed) {
				t.Fatal("closed cache remained readable", err)
			}
			if !base.IsPinnedKeyValueView() {
				t.Fatal("borrowed source closed prematurely")
			}
		})
	}
}

type serialPresenceTestView struct {
	StateHistoryReadView
	calls int
	err   error
}

func (v *serialPresenceTestView) GetWithPresence(k []byte) ([]byte, bool, error) {
	v.calls++
	if v.err != nil {
		return nil, false, v.err
	}
	return readPresentValue(v.StateHistoryReadView, k, "test presence")
}

func TestSerialHistoryChunkCachePreservesPresenceAndCancellation(t *testing.T) {
	db, _ := pipelineFixture(t, "shared")
	base, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pack, _ := db.Get(stateChangeSetKey(1, 0))
	sentinel := errors.New("presence read failed")
	presence := &serialPresenceTestView{StateHistoryReadView: base, err: sentinel}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	view, cache, err := AcquireSerialStateHistoryChunkCacheView(ctx, presence)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if _, err := decodeStateHistorySharedPack(view, pack, 1); !errors.Is(err, sentinel) || presence.calls != 1 || cache.Stats().Entries != 0 {
		t.Fatal("presence error lost or cached", err)
	}
	presence.err = nil
	want, err := decodeStateHistorySharedPack(view, pack, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeStateHistorySharedPack(view, pack, 1)
	if err != nil || !reflect.DeepEqual(got, want) || presence.calls != 2 || cache.Stats().Hits != 1 {
		t.Fatal("presence cache reuse failed", err)
	}
	cancel()
	if _, err := decodeStateHistorySharedPack(view, pack, 1); !errors.Is(err, context.Canceled) {
		t.Fatal("warm hit ignored cancellation", err)
	}
}
