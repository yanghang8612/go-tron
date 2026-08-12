package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestStateCodeReadWrite(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	hash := common.Keccak256(code)

	if got := ReadStateCode(db, hash); got != nil {
		t.Fatalf("pre-write code = %x", got)
	}
	if err := WriteStateCode(db, hash, code); err != nil {
		t.Fatalf("write state code: %v", err)
	}
	got := ReadStateCode(db, hash)
	if !bytes.Equal(got, code) {
		t.Fatalf("code = %x, want %x", got, code)
	}
	got[0] = 0xff
	if reread := ReadStateCode(db, hash); bytes.Equal(reread, got) {
		t.Fatal("ReadStateCode returned aliased bytes")
	}
	strict, ok, err := ReadStateCodeStrict(db, hash)
	if err != nil || !ok || !bytes.Equal(strict, code) {
		t.Fatalf("strict code = %x ok=%v err=%v, want %x", strict, ok, err, code)
	}
	strict[0] = 0xff
	if reread, ok, err := ReadStateCodeStrict(db, hash); err != nil || !ok || bytes.Equal(reread, strict) {
		t.Fatalf("ReadStateCodeStrict returned aliased bytes or failed reread=%x ok=%v err=%v", reread, ok, err)
	}
}

func TestIterateStateCodeHashes(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	want := make(map[common.Hash]struct{})
	for _, code := range [][]byte{{0x60, 0x01}, {0x60, 0x02, 0x00}} {
		hash := common.Keccak256(code)
		if err := WriteStateCode(db, hash, code); err != nil {
			t.Fatal(err)
		}
		want[hash] = struct{}{}
	}
	got := make(map[common.Hash]struct{})
	if err := IterateStateCodeHashes(db, func(hash common.Hash) (bool, error) {
		got[hash] = struct{}{}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("code hashes = %x, want %x", got, want)
	}
	for hash := range want {
		if _, ok := got[hash]; !ok {
			t.Fatalf("missing code hash %x in %x", hash, got)
		}
	}
}

var benchmarkStateCodeRows int

func BenchmarkIterateStateCodeKeys(b *testing.B) {
	const (
		rows     = 256
		codeSize = 4096
	)
	db := ethrawdb.NewMemoryDatabase()
	for i := 0; i < rows; i++ {
		code := bytes.Repeat([]byte{byte(i), byte(i >> 8), 0x60, 0x00}, codeSize/4)
		hash := common.Keccak256(code)
		if err := WriteStateCode(db, hash, code); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("owning-rows", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			seen := 0
			if err := IterateStateCode(db, func(StateCodeRow) (bool, error) {
				seen++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			benchmarkStateCodeRows = seen
		}
	})
	b.Run("hashes-only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			seen := 0
			if err := IterateStateCodeHashes(db, func(common.Hash) (bool, error) {
				seen++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			benchmarkStateCodeRows = seen
		}
	})
}

func TestStateCodeRejectsMismatchedHash(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateCode(db, common.Hash{0x01}, []byte{0x60, 0x00}); err == nil {
		t.Fatal("expected hash mismatch error")
	}
}

func TestStateCodeDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	code := []byte{0x60, 0x01}
	hash := common.Keccak256(code)
	if err := WriteStateCode(db, hash, code); err != nil {
		t.Fatalf("write state code: %v", err)
	}
	if err := DeleteStateCode(db, hash); err != nil {
		t.Fatalf("delete state code: %v", err)
	}
	if got := ReadStateCode(db, hash); got != nil {
		t.Fatalf("deleted code = %x", got)
	}
}

func TestStateCodeStrictReadDistinguishesMissAndErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	code := []byte{0x60, 0x42}
	hash := common.Keccak256(code)

	if got, ok, err := ReadStateCodeStrict(db, common.Hash{}); err != nil || ok || got != nil {
		t.Fatalf("zero hash read = %x ok=%v err=%v, want miss", got, ok, err)
	}
	if got, ok, err := ReadStateCodeStrict(db, hash); err != nil || ok || got != nil {
		t.Fatalf("missing code = %x ok=%v err=%v, want miss", got, ok, err)
	}
	if got, ok, err := ReadStateCodeStrict(failingStateCodeReader{
		KeyValueStore: db,
		getKey:        stateCodeKey(hash),
		getErr:        errors.New("get boom"),
	}, hash); err != nil || ok || got != nil {
		t.Fatalf("missing code with get error = %x ok=%v err=%v, want verified miss", got, ok, err)
	}

	if err := WriteStateCode(db, hash, code); err != nil {
		t.Fatalf("write state code: %v", err)
	}
	if got, ok, err := ReadStateCodeStrict(failingStateCodeReader{
		KeyValueStore: db,
		getKey:        stateCodeKey(hash),
		getErr:        errors.New("get boom"),
	}, hash); err == nil || ok || got != nil || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("present code get error = %x ok=%v err=%v, want get error", got, ok, err)
	}
	if got, ok, err := ReadStateCodeStrict(failingStateCodeReader{
		KeyValueStore: db,
		getKey:        stateCodeKey(hash),
		getErr:        errors.New("get boom"),
		hasErr:        errors.New("has boom"),
	}, hash); err == nil || ok || got != nil || !strings.Contains(err.Error(), "presence after get error") {
		t.Fatalf("present code has error = %x ok=%v err=%v, want presence error", got, ok, err)
	}
	if got := ReadStateCode(failingStateCodeReader{
		KeyValueStore: db,
		getKey:        stateCodeKey(hash),
		getErr:        errors.New("get boom"),
	}, hash); got != nil {
		t.Fatalf("compat reader get error = %x, want nil", got)
	}
}

type failingStateCodeReader struct {
	ethdb.KeyValueStore
	getKey []byte
	getErr error
	hasErr error
}

func (r failingStateCodeReader) Has(key []byte) (bool, error) {
	if bytes.Equal(key, r.getKey) && r.hasErr != nil {
		return false, r.hasErr
	}
	return r.KeyValueStore.Has(key)
}

func (r failingStateCodeReader) Get(key []byte) ([]byte, error) {
	if bytes.Equal(key, r.getKey) && r.getErr != nil {
		return nil, r.getErr
	}
	return r.KeyValueStore.Get(key)
}
