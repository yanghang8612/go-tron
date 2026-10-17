package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type pooledMetricsPresenceReader struct {
	ethdb.KeyValueReader
	value   []byte
	present bool
	err     error
	onRead  func([]byte)
}

func (r *pooledMetricsPresenceReader) GetWithPresence(k []byte) ([]byte, bool, error) {
	if r.onRead != nil {
		r.onRead(k)
	}
	return r.value, r.present, r.err
}

type pooledMetricsViewReader struct {
	ethdb.KeyValueReader
	value  []byte
	err    error
	onRead func([]byte)
}

func (r *pooledMetricsViewReader) View(k []byte, fn func([]byte) error) error {
	if r.onRead != nil {
		r.onRead(k)
	}
	if r.err != nil {
		return r.err
	}
	return fn(r.value)
}

type pooledMetricsSnapshot [4]int64

func pooledMetricsCounts(m *pooledForegroundBaseMetrics) pooledMetricsSnapshot {
	return pooledMetricsSnapshot{m.calls.Snapshot().Count(), m.present.Snapshot().Count(), m.notFound.Snapshot().Count(), m.errors.Snapshot().Count()}
}
func pooledMetricsDelta(m *pooledForegroundBaseMetrics, before pooledMetricsSnapshot) pooledMetricsSnapshot {
	after := pooledMetricsCounts(m)
	for i := range after {
		after[i] -= before[i]
	}
	return after
}
func pooledMetricsRequireDelta(t *testing.T, m *pooledForegroundBaseMetrics, before, want pooledMetricsSnapshot) {
	t.Helper()
	if got := pooledMetricsDelta(m, before); got != want {
		t.Fatalf("counter delta %v, want %v", got, want)
	}
}
func pooledMetricsKey(domain kvdomains.KVDomain, logical []byte) []byte {
	return rawdb.StateKVLatestCommitmentKey(common.Address{0: 0x41, 20: 7}, 17, domain, logical)
}

func TestPooledForegroundBaseMetricsHelperResults(t *testing.T) {
	key := pooledMetricsKey(kvdomains.ContractStorage, bytes.Repeat([]byte{3}, 32))
	m := pooledForegroundStorageReads
	fault := errors.New("read fault")
	wrapped := fmt.Errorf("wrapped: %w", ErrNotFound)
	for _, cached := range []bool{false, true} {
		for _, tt := range []struct {
			name    string
			value   []byte
			present bool
			err     error
			want    pooledMetricsSnapshot
		}{
			{"present", []byte("owned-value"), true, nil, pooledMetricsSnapshot{1, 1, 0, 0}},
			{"empty-present", []byte{}, true, nil, pooledMetricsSnapshot{1, 1, 0, 0}},
			{"nil-present", nil, true, nil, pooledMetricsSnapshot{1, 1, 0, 0}},
			{"missing", nil, false, nil, pooledMetricsSnapshot{1, 0, 1, 0}},
			{"exact-sentinel", nil, false, ErrNotFound, pooledMetricsSnapshot{1, 0, 1, 0}},
			{"wrapped-sentinel-returned", nil, false, wrapped, pooledMetricsSnapshot{1, 0, 0, 1}},
			{"fault", nil, false, fault, pooledMetricsSnapshot{1, 0, 0, 1}},
		} {
			t.Run(fmt.Sprintf("%v/%s", cached, tt.name), func(t *testing.T) {
				r := &pooledMetricsPresenceReader{value: tt.value, present: tt.present, err: tt.err}
				var c *baseReadCache
				var epoch baseReadCacheEpoch
				if cached {
					c = newBaseReadCache(1 << 20)
					_, _, epoch = c.getWithEpoch(key)
				}
				before := pooledMetricsCounts(m)
				got, err := readBaseIntoCachePooledKey(r, c, key, epoch)
				wantErr := tt.err
				if wantErr == nil && !tt.present {
					wantErr = ErrNotFound
				}
				if err != wantErr || !bytes.Equal(got, tt.value) {
					t.Fatalf("result=(%q,%v), want(%q,%v)", got, err, tt.value, wantErr)
				}
				pooledMetricsRequireDelta(t, m, before, tt.want)
				for i := 0; i < 20; i++ {
					b := borrowSplitReadKey()
					for j := range b {
						b[j] = 0xcc
					}
					returnSplitReadKey(b)
				}
				if !bytes.Equal(got, tt.value) {
					t.Fatal("pool reuse changed result")
				}
			})
		}
	}
}

func TestPooledForegroundBaseMetricsViewEpochAndPanic(t *testing.T) {
	key := pooledMetricsKey(kvdomains.AccountAssetV2, []byte("1000001"))
	m := pooledForegroundAssetReads
	cache := newBaseReadCache(1 << 20)
	_, _, epoch := cache.getWithEpoch(key)
	source := []byte("transient-value")
	reader := &pooledMetricsViewReader{value: source, onRead: func([]byte) { cache.clear() }}
	before := pooledMetricsCounts(m)
	got, err := readBaseIntoCachePooledKey(reader, cache, key, epoch)
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("read=%q,%v", got, err)
	}
	source[0] ^= 0xff
	if string(got) != "transient-value" {
		t.Fatal("epoch rejection leaked transient value")
	}
	pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{1, 1, 0, 0})
	before = pooledMetricsCounts(m)
	marker := errors.New("panic marker")
	func() {
		defer func() {
			if recover() != marker {
				t.Fatal("panic changed")
			}
		}()
		reader.onRead = func([]byte) { panic(marker) }
		readBaseIntoCachePooledKey(reader, cache, key, epoch)
	}()
	pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{})
	// The original View branch normalizes wrapped misses; measurement classifies
	// that returned sentinel, whereas the presence interface above returns it raw.
	reader.onRead = nil
	reader.err = fmt.Errorf("wrapped: %w", ErrNotFound)
	before = pooledMetricsCounts(m)
	_, err = readBaseIntoCachePooledKey(reader, cache, key, epoch)
	if err != ErrNotFound {
		t.Fatalf("normalized err=%v", err)
	}
	pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{1, 0, 1, 0})
}

func TestPooledForegroundBaseMetricsStructuredRoutes(t *testing.T) {
	for _, layer := range []bool{false, true} {
		t.Run(fmt.Sprint(layer), func(t *testing.T) {
			base := rawdb.NewMemoryDatabase()
			defer base.Close()
			b := New(base)
			b.SetBaseReadCacheSize(1 << 20)
			b.BeginBlock(bufHash(1), 1)
			var reader ethdb.KeyValueReader = b
			if layer {
				h, _ := b.NewestInflight()
				reader = b.ViewLayer(h)
			}
			owner := common.Address{0: 0x41, 20: 9}
			logical := bytes.Repeat([]byte{0x91}, 32)
			m := pooledForegroundStorageReads
			if err := rawdb.WriteStateKVLatest(base, owner, 17, kvdomains.ContractStorage, logical, []byte("payload")); err != nil {
				t.Fatal(err)
			}
			before := pooledMetricsCounts(m)
			for i := 0; i < 3; i++ {
				got, ok, err := rawdb.ReadStateKVLatestNoCopy(reader, owner, 17, kvdomains.ContractStorage, logical)
				if err != nil || !ok || string(got) != "payload" {
					t.Fatalf("read=%q,%v,%v", got, ok, err)
				}
			}
			pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{2, 2, 0, 0}) // Two-hit admission; the third is a cache hit.
			logical[0]++
			before = pooledMetricsCounts(m)
			for i := 0; i < 3; i++ {
				_, ok, err := rawdb.ReadStateKVLatestNoCopy(reader, owner, 17, kvdomains.ContractStorage, logical)
				if err != nil || ok {
					t.Fatalf("missing=%v,%v", ok, err)
				}
			}
			pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{2, 0, 2, 0})
			logical[0]++
			physical := rawdb.StateKVLatestCommitmentKey(owner, 17, kvdomains.ContractStorage, logical)
			if err := b.Put(physical, []byte("overlay")); err != nil {
				t.Fatal(err)
			}
			prefixLen := len(physical) - common.AccountIDLength - 10 - len(logical)
			typed := reader.(interface {
				GetNoCopyCachedStateKVLatest([]byte, common.AccountID, uint64, uint16, []byte) ([]byte, error)
			})
			before = pooledMetricsCounts(m)
			got, err := typed.GetNoCopyCachedStateKVLatest(physical[:prefixLen], owner.AccountID(), 17, uint16(kvdomains.ContractStorage), logical)
			if err != nil || string(got) != "overlay" {
				t.Fatalf("overlay=%q,%v", got, err)
			}
			if err = b.Delete(physical); err != nil {
				t.Fatal(err)
			}
			_, err = typed.GetNoCopyCachedStateKVLatest(physical[:prefixLen], owner.AccountID(), 17, uint16(kvdomains.ContractStorage), logical)
			if err != ErrNotFound {
				t.Fatalf("tombstone=%v", err)
			}
			pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{})
			logical[0]++
			physical = rawdb.StateKVLatestCommitmentKey(owner, 17, kvdomains.ContractStorage, logical)
			before = pooledMetricsCounts(m)
			b.Prefetch(physical)
			pooledMetricsRequireDelta(t, m, before, pooledMetricsSnapshot{})
		})
	}
}

func TestPooledForegroundBaseMetricsExclusionsAndConcurrentCompletion(t *testing.T) {
	r := &pooledMetricsPresenceReader{present: true, value: []byte("v")}
	storage := pooledForegroundStorageReads
	asset := pooledForegroundAssetReads
	sb, ab := pooledMetricsCounts(storage), pooledMetricsCounts(asset)
	for _, key := range [][]byte{[]byte("generic"), pooledMetricsKey(kvdomains.ContractStorage, make([]byte, 31)), pooledMetricsKey(kvdomains.AccountAssetV2, []byte("01")), pooledMetricsKey(kvdomains.ContractMetadata, make([]byte, 32))} {
		if _, err := readBaseIntoCachePooledKey(r, nil, key, baseReadCacheEpoch{}); err != nil {
			t.Fatal(err)
		}
	}
	pooledMetricsRequireDelta(t, storage, sb, pooledMetricsSnapshot{})
	pooledMetricsRequireDelta(t, asset, ab, pooledMetricsSnapshot{})
	key := pooledMetricsKey(kvdomains.AccountAssetV2, []byte("1"))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 64; j++ {
				if _, err := readBaseIntoCachePooledKey(r, nil, key, baseReadCacheEpoch{}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	pooledMetricsRequireDelta(t, asset, ab, pooledMetricsSnapshot{512, 512, 0, 0})
}
