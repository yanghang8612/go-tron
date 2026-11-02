package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSparseResourceAgainstProductionWriterAndMuHash(t *testing.T) {
	var owner common.Address
	owner[0] = 0x41
	owner[20] = 0x90
	var roots [2]common.Hash
	for arm := 0; arm < 2; arm++ {
		disk := ethrawdb.NewMemoryDatabase()
		defer disk.Close()
		s, err := state.New(common.Hash{}, state.NewDatabase(disk))
		if err != nil {
			t.Fatal(err)
		}
		s.CreateAccount(owner, corepb.AccountType_Normal)
		s.SetEnergyUsage(owner, 17)
		s.SetLatestConsumeTimeForEnergy(owner, 17000000)
		s.SetEnergyWindow(owner, 28800000, true)
		s.FreezeV1Energy(owner, 100000000, 17208000)
		s.AddDelegatedFrozenV2(owner, corepb.ResourceCode_ENERGY, 23)
		s.AddAcquiredDelegatedFrozenV2(owner, corepb.ResourceCode_ENERGY, 31)
		old, present, err := s.GetAccountKV(owner, kvdomains.AccountResourceAux, []byte{0})
		if err != nil || !present {
			t.Fatal(present, err)
		}
		r, err := parseResourceV1(old)
		if err != nil {
			t.Fatal(err)
		}
		zig := func(v uint64) uint64 { return v << 1 }
		if r.words != [10]uint64{zig(17), zig(17000000), 0, 0, 0, 0, 0, zig(28800000), zig(23), zig(31)} || !r.frozen || r.frozenBalance != zig(100000000) || r.frozenExpiry != zig(17208000) || !r.optimized {
			t.Fatalf("experiment parser disagrees with production writer: %+v", r)
		}
		if arm == 1 {
			compact, err := compactResource(old)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := expandResource(compact)
			if err != nil || !bytes.Equal(restored, old) {
				t.Fatal(err)
			}
			if err = s.SetAccountKV(owner, kvdomains.AccountResourceAux, []byte{0}, restored); err != nil {
				t.Fatal(err)
			}
			if s.GetEnergyUsage(owner) != 17 {
				t.Fatal("production materialization disagrees")
			}
		}
		roots[arm], err = s.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
	if roots[0] != roots[1] {
		t.Fatal("restored canonical bytes changed MuHash", roots)
	}
}

func TestFixedSyntheticExportSSTPipeline(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "synthetic-resource.bin")
	f, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for i := 1; i <= 3000; i++ {
		var owner common.Address
		owner[0] = 0x41
		binary.BigEndian.PutUint64(owner[13:], uint64(i))
		k := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountResourceAux, []byte{0})
		r := resourceFields{}
		if i%3 != 0 {
			r.words[0] = uint64(i) * 9127
			r.words[1] = uint64(i) * 704
			r.words[7] = 57600000
		}
		if i%5 == 0 {
			r.frozen = true
			r.frozenBalance = 20000000
			r.frozenExpiry = 1793589902000
		}
		v := rawdb.EncodeStateKVLatestValue(encodeResourceV1(r))
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(k)))
		w.Write(b[:])
		w.Write(k)
		binary.BigEndian.PutUint32(b[:], uint32(len(v)))
		w.Write(b[:])
		w.Write(v)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = run(input, filepath.Join(dir, "results"), 5000, 1000, 2); err != nil {
		t.Fatal(err)
	}
}
