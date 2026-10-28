package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestHistoryRoutingReadScopePinsOnlyOneRequestImage(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	m = addSmallRoutingFixture(t, dir, m, 3)
	for _, ref := range []SegmentRef{mustRoutingRef(t, m), mustSmallRoutingRef(t, m)} {
		ctx := WithHistoryRoutingReadScope(context.Background())
		first, digest, err := readSystemHistoryRoutingBytes(ctx, dir, ref)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, ref.Path)
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		changed := bytes.Clone(original)
		changed[len(changed)-1] ^= 1
		if err := os.WriteFile(path, changed, 0600); err != nil {
			t.Fatal(err)
		}
		again, digest2, err := readSystemHistoryRoutingBytes(ctx, dir, ref)
		if err != nil || digest2 != digest || !bytes.Equal(first, again) || &first[0] != &again[0] {
			t.Fatalf("request image not reused: %v", err)
		}
		// Independent requests and unscoped reads must authenticate current
		// bytes, even for the same pinned manager and exact old reference.
		for _, fresh := range []context.Context{WithHistoryRoutingReadScope(ctx), context.Background()} {
			if _, _, err := readSystemHistoryRoutingBytes(fresh, dir, ref); err == nil {
				t.Fatal("cross-request stale proof accepted")
			}
		}
		next := ref
		next.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(changed))
		if data, _, err := readSystemHistoryRoutingBytes(ctx, dir, next); err != nil || !bytes.Equal(data, changed) {
			t.Fatalf("replacement ref reused old image: %v", err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, _, err := readSystemHistoryRoutingBytes(cancelled, dir, ref); err != context.Canceled {
			t.Fatal(err)
		}
		if _, _, err := readSystemHistoryRoutingBytes(ctx, t.TempDir(), ref); err == nil {
			t.Fatal("different directory reused image")
		}
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryRoutingReadScopeBudgetAndConcurrentReads(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	ref := mustRoutingRef(t, m)
	ctx := WithHistoryRoutingReadScope(context.Background())
	scope := routingReadScope(ctx)
	scope.bytes = historyRoutingRequestBytes - 1
	if _, _, err := readSystemHistoryRoutingBytes(ctx, dir, ref); err != nil {
		t.Fatal(err)
	}
	if len(scope.images) != 0 {
		t.Fatal("byte limit exceeded")
	}
	scope.bytes = 0
	for i := range historyRoutingRequestEntries {
		scope.put(historyRoutingReadKey{dir: fmt.Sprint(i)}, historyRoutingReadImage{data: []byte{1}})
	}
	if _, _, err := readSystemHistoryRoutingBytes(ctx, dir, ref); err != nil {
		t.Fatal(err)
	}
	if len(scope.images) != historyRoutingRequestEntries {
		t.Fatal("entry limit exceeded")
	}
	ctx = WithHistoryRoutingReadScope(context.Background())
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := readSystemHistoryRoutingBytes(ctx, dir, ref); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
