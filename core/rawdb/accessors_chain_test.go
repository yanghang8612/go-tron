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

func TestTotalTransactionCount(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Initial read returns 0.
	if n := ReadTotalTransactionCount(db); n != 0 {
		t.Fatalf("initial count: want 0, got %d", n)
	}

	WriteTotalTransactionCount(db, 42)
	if n := ReadTotalTransactionCount(db); n != 42 {
		t.Fatalf("after write 42: want 42, got %d", n)
	}

	// Overwrite with a larger value.
	WriteTotalTransactionCount(db, 1_000_000)
	if n := ReadTotalTransactionCount(db); n != 1_000_000 {
		t.Fatalf("after write 1000000: want 1000000, got %d", n)
	}

	// Increment simulation.
	prev := ReadTotalTransactionCount(db)
	WriteTotalTransactionCount(db, prev+5)
	if n := ReadTotalTransactionCount(db); n != 1_000_005 {
		t.Fatalf("after +5: want 1000005, got %d", n)
	}
}

func TestChainSingletonStrictRoundTripAndAbsent(t *testing.T) {
	db := NewMemoryDatabase()
	head := common.HexToHash("0xaaaa")
	solid := common.HexToHash("0xbbbb")
	genesisRoot := common.HexToHash("0xcccc")
	propValue := []byte("dynamic")
	witnesses := []GenesisWitness{
		{Address: common.BytesToAddress([]byte{0x41, 0x01}), VoteCount: 7},
		{Address: common.BytesToAddress([]byte{0x41, 0x02}), VoteCount: 11},
	}

	if got, ok, err := ReadHeadBlockHashStrict(db); err != nil || ok || got != (common.Hash{}) {
		t.Fatalf("head absent = %x/%v/%v, want zero/false/nil", got, ok, err)
	}
	if got, ok, err := ReadHeadSolidBlockHashStrict(db); err != nil || ok || got != (common.Hash{}) {
		t.Fatalf("solid absent = %x/%v/%v, want zero/false/nil", got, ok, err)
	}
	if got, ok, err := ReadDynamicPropertyStrict(db, "k"); err != nil || ok || got != nil {
		t.Fatalf("dynamic property absent = %x/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got, ok, err := ReadGenesisWitnessesStrict(db); err != nil || ok || got != nil {
		t.Fatalf("genesis witnesses absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got, ok, err := ReadTotalTransactionCountStrict(db); err != nil || ok || got != 0 {
		t.Fatalf("tx count absent = %d/%v/%v, want 0/false/nil", got, ok, err)
	}
	if got, ok, err := ReadGenesisStateRootStrict(db); err != nil || ok || got != (common.Hash{}) {
		t.Fatalf("genesis root absent = %x/%v/%v, want zero/false/nil", got, ok, err)
	}

	WriteHeadBlockHash(db, head)
	WriteHeadSolidBlockHash(db, solid)
	WriteDynamicProperty(db, "k", propValue)
	WriteGenesisWitnesses(db, witnesses)
	WriteTotalTransactionCount(db, 42)
	WriteGenesisStateRoot(db, genesisRoot)

	if got, ok, err := ReadHeadBlockHashStrict(db); err != nil || !ok || got != head {
		t.Fatalf("head strict = %x/%v/%v, want %x/true/nil", got, ok, err, head)
	}
	if got, ok, err := ReadHeadSolidBlockHashStrict(db); err != nil || !ok || got != solid {
		t.Fatalf("solid strict = %x/%v/%v, want %x/true/nil", got, ok, err, solid)
	}
	if got, ok, err := ReadDynamicPropertyStrict(db, "k"); err != nil || !ok || !bytes.Equal(got, propValue) {
		t.Fatalf("dynamic property strict = %x/%v/%v, want %x/true/nil", got, ok, err, propValue)
	}
	if got, ok, err := ReadGenesisWitnessesStrict(db); err != nil || !ok || len(got) != len(witnesses) || got[0] != witnesses[0] || got[1] != witnesses[1] {
		t.Fatalf("genesis witnesses strict = %v/%v/%v, want %v/true/nil", got, ok, err, witnesses)
	}
	if got, ok, err := ReadTotalTransactionCountStrict(db); err != nil || !ok || got != 42 {
		t.Fatalf("tx count strict = %d/%v/%v, want 42/true/nil", got, ok, err)
	}
	if got, ok, err := ReadGenesisStateRootStrict(db); err != nil || !ok || got != genesisRoot {
		t.Fatalf("genesis root strict = %x/%v/%v, want %x/true/nil", got, ok, err, genesisRoot)
	}
}

func TestChainSingletonStrictSurfacesStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	WriteHeadBlockHash(db, common.HexToHash("0xaaaa"))
	WriteHeadSolidBlockHash(db, common.HexToHash("0xbbbb"))
	WriteDynamicProperty(db, "k", []byte("v"))
	WriteGenesisWitnesses(db, []GenesisWitness{{Address: common.BytesToAddress([]byte{0x41, 0x01}), VoteCount: 7}})
	WriteTotalTransactionCount(db, 42)
	WriteGenesisStateRoot(db, common.HexToHash("0xcccc"))

	readers := []struct {
		name string
		read func(ethdb.KeyValueReader) (bool, error)
	}{
		{
			name: "head",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadHeadBlockHashStrict(r)
				return ok, err
			},
		},
		{
			name: "solid",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadHeadSolidBlockHashStrict(r)
				return ok, err
			},
		},
		{
			name: "dynamic",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadDynamicPropertyStrict(r, "k")
				return ok, err
			},
		},
		{
			name: "genesis witnesses",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadGenesisWitnessesStrict(r)
				return ok, err
			},
		},
		{
			name: "tx count",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadTotalTransactionCountStrict(r)
				return ok, err
			},
		},
		{
			name: "genesis root",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadGenesisStateRootStrict(r)
				return ok, err
			},
		},
	}

	for _, tc := range readers {
		t.Run(tc.name+"/has", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "presence") {
				t.Fatalf("has error: ok=%v err=%v", ok, err)
			}
		})
		t.Run(tc.name+"/get", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, getErr: errors.New("get boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "get boom") {
				t.Fatalf("get error: ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestChainSingletonStrictSurfacesMalformedRows(t *testing.T) {
	db := NewMemoryDatabase()
	if err := db.Put(headBlockKey, []byte{0xaa}); err != nil {
		t.Fatal(err)
	}
	if got := ReadHeadBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("compat malformed head = %x, want zero", got)
	}
	if got, ok, err := ReadHeadBlockHashStrict(db); err == nil || ok || got != (common.Hash{}) || !strings.Contains(err.Error(), "length 1, want 32") {
		t.Fatalf("strict malformed head = %x/%v/%v, want length error", got, ok, err)
	}

	if err := db.Put(headSolidBlockKey, []byte{0xbb}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadHeadSolidBlockHashStrict(db); err == nil || ok || got != (common.Hash{}) || !strings.Contains(err.Error(), "length 1, want 32") {
		t.Fatalf("strict malformed solid = %x/%v/%v, want length error", got, ok, err)
	}

	if err := db.Put(genesisStateRootKey, []byte{0xcc}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadGenesisStateRootStrict(db); err == nil || ok || got != (common.Hash{}) || !strings.Contains(err.Error(), "length 1, want 32") {
		t.Fatalf("strict malformed genesis root = %x/%v/%v, want length error", got, ok, err)
	}

	if err := db.Put(totalTransactionCountKey, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := ReadTotalTransactionCount(db); got != 0 {
		t.Fatalf("compat malformed tx count = %d, want 0", got)
	}
	if got, ok, err := ReadTotalTransactionCountStrict(db); err == nil || ok || got != 0 || !strings.Contains(err.Error(), "length 1, want 8") {
		t.Fatalf("strict malformed tx count = %d/%v/%v, want length error", got, ok, err)
	}

	if err := db.Put(genesisWitnessesKey, []byte{0x00, 0x00, 0x00, 0x01, 0xaa}); err != nil {
		t.Fatal(err)
	}
	if got := ReadGenesisWitnesses(db); got != nil {
		t.Fatalf("compat short genesis witnesses = %v, want nil", got)
	}
	if got, ok, err := ReadGenesisWitnessesStrict(db); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "genesis witnesses") {
		t.Fatalf("strict short genesis witnesses = %v/%v/%v, want decode error", got, ok, err)
	}

	witness := GenesisWitness{Address: common.BytesToAddress([]byte{0x41, 0x02}), VoteCount: 9}
	WriteGenesisWitnesses(db, []GenesisWitness{witness})
	if err := db.Put(genesisWitnessesKey, append(readGenesisWitnessesRawForTest(db), 0xff)); err != nil {
		t.Fatal(err)
	}
	if got := ReadGenesisWitnesses(db); len(got) != 1 || got[0] != witness {
		t.Fatalf("compat trailing genesis witnesses = %v, want original witness", got)
	}
	if got, ok, err := ReadGenesisWitnessesStrict(db); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "genesis witnesses") {
		t.Fatalf("strict trailing genesis witnesses = %v/%v/%v, want decode error", got, ok, err)
	}
}

func readGenesisWitnessesRawForTest(db ethdb.KeyValueReader) []byte {
	data, err := db.Get(genesisWitnessesKey)
	if err != nil {
		return nil
	}
	return append([]byte(nil), data...)
}
