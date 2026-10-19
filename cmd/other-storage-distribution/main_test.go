package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestIntegerModelRestoresExactEightBytes(t *testing.T) {
	for _, n := range []uint64{0, 1, 255, 256, 65536, 1 << 32, 1<<56 - 1, 1 << 56, 1 << 63, ^uint64(0)} {
		v := binary.BigEndian.AppendUint64(nil, n)
		encoded := intModel(v)
		var restored []byte
		switch encoded[0] {
		case 1:
			restored = encoded[1:]
		case 3:
			restored = append(make([]byte, 8-len(encoded[1:])), encoded[1:]...)
		default:
			t.Fatal("tag")
		}
		if !bytes.Equal(restored, v) || len(encoded) > 9 {
			t.Fatalf("%d: %x", n, encoded)
		}
		if n == 0 && len(encoded) != 1 {
			t.Fatal("zero is a present one-byte tagged word")
		}
	}
}
func TestCandidateKeyRetainsEntireLogicalSuffix(t *testing.T) {
	old := append([]byte("state-kv-latest-v2-"), []byte{0, 1, 2, 3, 255}...)
	if got := postV2Key(old, len("state-kv-latest-v2-")); !bytes.Equal(got, append([]byte("kv3-"), []byte{0, 1, 2, 3, 255}...)) {
		t.Fatalf("%x", got)
	}
}

func TestFullCensusConservesSourceAndSSTRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 7}
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(123)
	core, e := acc.MarshalStorageCoreV4()
	if e != nil {
		t.Fatal(e)
	}
	env := &state.StateAccountV3{Version: 5, AccountProto: core, AccountKVRoot: state.EmptyKVRoot}
	v, e := env.Encode()
	if e != nil {
		t.Fatal(e)
	}
	if e = rawdb.WriteStateAccountLatest(db, owner, v); e != nil {
		t.Fatal(e)
	}
	for i, n := range []uint64{0, 1, 255, 256, 1 << 40, 1 << 63} {
		if e = rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.AccountAssetV2, []byte(strconv.Itoa(1000000+i)), binary.BigEndian.AppendUint64(nil, n)); e != nil {
			t.Fatal(e)
		}
	}
	mu := muhash3072.NewAccumulator()
	if e = rawdb.IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
		b, e := rawdb.EncodeStateCommitmentElement(k, v)
		if e == nil {
			e = mu.Insert(b)
		}
		return e == nil, e
	}); e != nil {
		t.Fatal(e)
	}
	sealed, e := mu.Finalize()
	if e != nil {
		t.Fatal(e)
	}
	head := rawdb.NewStateCommitmentHead()
	head.Root = common.Hash(sealed.Digest)
	head.State = sealed.State
	head.BlockNum = 7
	head.TxNum = 12
	if e = rawdb.WriteStateCommitmentHead(db, head); e != nil {
		t.Fatal(e)
	}
	if e = rawdb.WriteLatestDomainCommitmentRoot(db, head.Root); e != nil {
		t.Fatal(e)
	}
	prior, e := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e := inspect(db, prior, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if !r.Complete || !r.Authenticated || r.SourceRows != prior.SourceRows || r.Accounts.AddressMatchesOwner != 1 || r.Accounts.AddressElisionBytes < 22 {
		t.Fatalf("bad census: %+v", r)
	}
	n := r.Numbers["AccountAssetV2/current"]
	if n.Rows != 6 || n.Zero != 1 || n.Negative != 1 || n.Width[0] != 1 || n.Width[1] != 2 || r.Assets.V2Only != 6 {
		t.Fatalf("bad numbers: %+v", n)
	}
	for _, suffix := range []string{"snappy", "none"} {
		for _, name := range []string{"int64-post-v2", "int64-trim"} {
			s := r.Experiments[name+"-"+suffix]
			if s.Rows != 6 || !s.ChecksumsVerified {
				t.Fatalf("bad SST: %+v", s)
			}
		}
	}
	got, _, e := rawdb.ReadStateAccountLatest(db, owner)
	if e != nil || !bytes.Equal(got, v) {
		t.Fatal("source mutated")
	}
	code := bytes.Repeat([]byte{0x60, 0x80, 0x60, 0x40, 0x52}, 100)
	if e = rawdb.WriteStateCode(db, common.Keccak256(code), code); e != nil {
		t.Fatal(e)
	}
	codeOut := t.TempDir()
	if e = inspectCode(db, prior, codeOut); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(codeOut, "code-distribution.json"))
	if e != nil {
		t.Fatal(e)
	}
	var cr codeReport
	if e = json.Unmarshal(b, &cr); e != nil || !cr.Complete || cr.Rows != 1 || cr.CodeBytes != uint64(len(code)) {
		t.Fatalf("code census: %+v %v", cr, e)
	}
	for _, series := range cr.Experiments {
		if series.Rows != 1 || !series.ChecksumsVerified {
			t.Fatalf("code SST: %+v", series)
		}
	}
}
