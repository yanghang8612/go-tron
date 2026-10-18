package rawdb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func sealGenerationFixture(t *testing.T, db ethdb.KeyValueStore) {
	t.Helper()
	acc := muhash3072.NewAccumulator()
	if err := IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
		e, err := EncodeStateCommitmentElement(k, v)
		if err == nil {
			err = acc.Insert(e)
		}
		return err == nil, err
	}); err != nil {
		t.Fatal(err)
	}
	f, err := acc.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	h := NewStateCommitmentHead()
	h.State = f.State
	h.Root = common.Hash(f.Digest)
	h.BlockNum = 123
	h.BlockHash = common.Hash{1}
	h.TxNum = 456
	if err = WriteStateCommitmentHead(db, h); err != nil {
		t.Fatal(err)
	}
	if err = WriteLatestDomainCommitmentRoot(db, h.Root); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationCensusIndependentOracle(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	rnd := rand.New(rand.NewSource(781))
	expected := map[string]GenerationBytes{"current": {}, "obsolete": {}, "future": {}}
	var wantGroups uint64
	var wantOwners, obsoleteOwners, futureOwners, noGen uint64
	for i := 0; i < 90; i++ {
		owner := common.Address{0x41, byte(i + 1)}
		explicit := i%3 != 0
		current := uint64(i % 4)
		if explicit {
			if err := WriteStateKVGeneration(db, owner, current); err != nil {
				t.Fatal(err)
			}
		} else {
			current = 0
		}
		if i%2 == 0 {
			if err := WriteStateAccountLatest(db, owner, []byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
		}
		wantOwners++
		if !explicit {
			noGen++
		}
		old, future := false, false
		for gen := uint64(0); gen < 5; gen++ {
			for _, domain := range []kvdomains.KVDomain{kvdomains.ContractStorage, kvdomains.AccountAsset} {
				class := "current"
				if gen < current {
					class = "obsolete"
					old = true
				} else if gen > current {
					class = "future"
					future = true
				}
				if class != "current" {
					wantGroups++
				}
				for j := 0; j < 3; j++ {
					key := []byte(fmt.Sprint(j))
					value := make([]byte, rnd.Intn(1500))
					rnd.Read(value)
					if err := WriteStateKVLatest(db, owner, gen, domain, key, value); err != nil {
						t.Fatal(err)
					}
					x := expected[class]
					x.Rows++
					k := StateKVLatestCommitmentKey(owner, gen, domain, key)
					x.KeyBytes += uint64(len(k))
					x.ValueBytes += uint64(len(value) + 1)
					x.PayloadBytes += uint64(len(value))
					x.LogicalBytes += uint64(len(k) + len(value) + 1)
					x.MaxPayload = max(x.MaxPayload, uint64(len(value)))
					expected[class] = x
				}
			}
		}
		if old {
			obsoleteOwners++
		}
		if future {
			futureOwners++
		}
	}
	// Generation-only owners before and after KV owners exercise both drain edges.
	for _, owner := range []common.Address{{0x41}, {0x41, 255}} {
		if err := WriteStateKVGeneration(db, owner, 9); err != nil {
			t.Fatal(err)
		}
	}
	sealGenerationFixture(t, db)
	before := generationFixtureRows(t, db)
	var groups, groupRows uint64
	r, err := InspectStateKVGenerations(context.Background(), db, true, func(g GenerationGroup) error {
		groups++
		groupRows += g.Rows
		if g.Class == "current" {
			t.Fatal("current group emitted")
		}
		return nil
	}, nil)
	if err != nil || !r.Complete || !r.MuHashVerified {
		t.Fatalf("report=%+v error=%v", r, err)
	}
	for k, want := range expected {
		if *r.Classes[k] != want {
			t.Fatalf("%s got=%+v want=%+v", k, *r.Classes[k], want)
		}
	}
	if r.Owners != wantOwners || r.OwnersWithObsolete != obsoleteOwners || r.OwnersWithFuture != futureOwners || r.OwnersWithoutGeneration != noGen || groups != wantGroups || groupRows != expected["obsolete"].Rows+expected["future"].Rows {
		t.Fatalf("owner/group counts differ: %+v groups=%d", r, groups)
	}
	if r.SourceRows != [3]uint64{45, 62, 2700} {
		t.Fatalf("source row counts %+v", r.SourceRows)
	}
	if len(r.TopObsoleteOwners) != 32 {
		t.Fatalf("top bounded owners=%d", len(r.TopObsoleteOwners))
	}
	var domainRows, histRows, generationRows uint64
	for _, m := range r.Domains {
		for _, s := range m {
			domainRows += s.Rows
		}
	}
	for _, m := range r.PayloadSizes {
		for _, s := range m {
			histRows += s.Rows
		}
	}
	for _, m := range r.StoredGenerations {
		for _, s := range m {
			generationRows += s.Rows
		}
	}
	if domainRows != r.Total.Rows || histRows != r.Total.Rows || generationRows != r.Total.Rows {
		t.Fatal("dimension conservation failed")
	}
	if !reflect.DeepEqual(before, generationFixtureRows(t, db)) {
		t.Fatal("census modified source")
	}
}

func generationFixtureRows(t *testing.T, db ethdb.Iteratee) map[string]string {
	t.Helper()
	m := make(map[string]string)
	it := db.NewIterator(nil, nil)
	for it.Next() {
		m[string(it.Key())] = string(it.Value())
	}
	err := it.Error()
	it.Release()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestGenerationCensusFailsClosed(t *testing.T) {
	for _, scenario := range []string{"root", "source", "generation", "key", "value", "sink", "cancel", "restore", "unwind"} {
		t.Run(scenario, func(t *testing.T) {
			db := NewMemoryDatabase()
			defer db.Close()
			owner := common.Address{0x41, 1}
			_ = WriteStateKVGeneration(db, owner, 1)
			_ = WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte("slot"), nil)
			sealGenerationFixture(t, db)
			ctx := context.Background()
			var sink func(GenerationGroup) error
			switch scenario {
			case "root":
				_ = WriteLatestDomainCommitmentRoot(db, common.Hash{2})
			case "source":
				_ = WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte("slot"), []byte("changed"))
			case "generation":
				_ = db.Put(stateKVGenerationKey(owner), []byte{1})
			case "key":
				_ = db.Put(append(append([]byte{}, stateKVLatestPrefix...), 1), []byte{1})
			case "value":
				_ = db.Put(stateKVLatestKey(owner, 0, kvdomains.ContractStorage, []byte("slot")), nil)
			case "sink":
				sink = func(GenerationGroup) error { return errors.New("sink failed") }
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "restore":
				_ = WriteStateCommitmentRestoreMarker(db)
			case "unwind":
				_ = WriteStateCommitmentUnwindCleanup(db, 2, 1)
			}
			r, err := InspectStateKVGenerations(ctx, db, true, sink, nil)
			if err == nil || r.Complete || r.Error == "" {
				t.Fatalf("accepted %s: %+v %v", scenario, r, err)
			}
		})
	}
}
