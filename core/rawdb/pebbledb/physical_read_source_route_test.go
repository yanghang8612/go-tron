package pebbledb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type sourceRouteBase struct {
	ethdb.KeyValueReader
	db      *pebble.DB
	lastKey []byte
}

func (b *sourceRouteBase) Put(k, v []byte) error {
	b.lastKey = bytes.Clone(k)
	return b.db.Set(k, v, pebble.NoSync)
}
func (b *sourceRouteBase) Delete(k []byte) error { return b.db.Delete(k, pebble.NoSync) }
func (b *sourceRouteBase) Get(k []byte) ([]byte, error) {
	v, c, e := b.db.Get(k)
	if e != nil {
		return nil, e
	}
	out := bytes.Clone(v)
	return out, c.Close()
}
func (b *sourceRouteBase) Has(k []byte) (bool, error) {
	_, c, e := b.db.Get(k)
	if errors.Is(e, pebble.ErrNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, c.Close()
}
func (b *sourceRouteBase) IsKeyNotFound(e error) bool { return errors.Is(e, pebble.ErrNotFound) }
func (b *sourceRouteBase) View(k []byte, fn func([]byte) error) (err error) {
	v, c, e := b.db.Get(k)
	if e != nil {
		return e
	}
	defer func() {
		if ce := c.Close(); err == nil {
			err = ce
		}
	}()
	return fn(v)
}
func TestPhysicalReadCallerRealSSTRoutes(t *testing.T) {
	mem := vfs.NewMem()
	db, err := pebble.Open("db", &pebble.Options{FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	owner := common.Address{0: 0x41, 20: 3}
	base := &sourceRouteBase{db: db}
	for i := 0; i < 1024; i++ {
		var key [32]byte
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		if err := rawdb.WriteStateKVLatest(base, owner, 1, kvdomains.ContractStorage, key[:], bytes.Repeat([]byte{byte(i + 1)}, 32)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	observed := pebbledb.SourceAttributionTestFS(mem, t.Name()+"/")
	db, err = pebble.Open("db", &pebble.Options{FS: observed})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base.db = db
	// Ordinary Get explicitly provides an unlabelled execution stack. Open may
	// issue no ReadAt outside the later pooled call, so it is not an oracle for
	// unknown coverage. This last key is far from the near-key Buffer reads.
	before := pebbledb.SourceAttributionTestCounts(observed)
	v, closer, err := db.Get(base.lastKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, rawdb.EncodeStateKVLatestValue(bytes.Repeat([]byte{0}, 32))) {
		closer.Close()
		t.Fatalf("ordinary far-key value=%x", v)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	afterOrdinary := pebbledb.SourceAttributionTestCounts(observed)
	if afterOrdinary["unknown"] <= before["unknown"] {
		t.Fatalf("ordinary Get produced no unlabelled read: before=%v after=%v", before, afterOrdinary)
	}
	for _, cache := range []int{0, 1 << 20} {
		b := blockbuffer.New(base)
		b.SetBaseReadCacheSize(cache)
		for i := 0; i < 32; i++ {
			var k [32]byte
			k[0] = byte(i >> 8)
			k[1] = byte(i)
			v, ok, e := rawdb.ReadStateKVLatestNoCopy(b, owner, 1, kvdomains.ContractStorage, k[:])
			if e != nil || !ok || !bytes.Equal(v, bytes.Repeat([]byte{byte(i + 1)}, 32)) {
				t.Fatalf("row %d=%x,%v,%v", i, v, ok, e)
			}
		}
	}
	counts := pebbledb.SourceAttributionTestCounts(observed)
	if counts["pooled_flat_stack"] <= afterOrdinary["pooled_flat_stack"] {
		t.Fatalf("realSST route missing: %v", counts)
	}
}
