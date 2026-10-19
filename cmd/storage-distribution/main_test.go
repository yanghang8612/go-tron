package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func sealed(t *testing.T, db ethdb.KeyValueStore) rawdb.GenerationCensus {
	t.Helper()
	acc := muhash3072.NewAccumulator()
	if err := rawdb.IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
		e, err := rawdb.EncodeStateCommitmentElement(k, v)
		if err == nil {
			err = acc.Insert(e)
		}
		return err == nil, err
	}); err != nil {
		t.Fatal(err)
	}
	v, err := acc.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	h := rawdb.NewStateCommitmentHead()
	h.State = v.State
	h.Root = common.Hash(v.Digest)
	h.BlockNum = 7
	h.TxNum = 12
	if err = rawdb.WriteStateCommitmentHead(db, h); err != nil {
		t.Fatal(err)
	}
	if err = rawdb.WriteLatestDomainCommitmentRoot(db, h.Root); err != nil {
		t.Fatal(err)
	}
	r, err := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func fixture(t *testing.T, db ethdb.KeyValueStore) rawdb.GenerationCensus {
	t.Helper()
	for i := byte(1); i <= 2; i++ {
		owner := common.Address{0x41, i}
		if i == 2 {
			if err := rawdb.WriteStateKVGeneration(db, owner, 1); err != nil {
				t.Fatal(err)
			}
		}
		prefix := common.Keccak256(owner[:])
		for gen := uint64(0); gen < 2; gen++ {
			for j := byte(0); j < 5; j++ {
				key := make([]byte, 32)
				copy(key, prefix[:16])
				key[31] = j
				v := make([]byte, 32)
				switch j {
				case 1:
					v[31] = 1
				case 2:
					v[30] = 1
				case 3:
					v[10] = 1
				case 4:
					for k := range v {
						v[k] = 255
					}
				}
				if err := rawdb.WriteStateKVLatest(db, owner, gen, kvdomains.ContractStorage, key, v); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, height := range []int64{1, 2, 4, 5, 6, 7} {
		v := make([]byte, 32)
		if height >= 6 {
			v[0] = 1
		}
		if err := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.MerkleTreeIndexStateKey(height), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.IncrMerkleLastTreeStateKey(), nil); err != nil {
		t.Fatal(err)
	}
	return sealed(t, db)
}

func TestDistributionExactAndExperiments(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	prior := fixture(t, db)
	out := t.TempDir()
	r, err := inspect(context.Background(), db, prior, out, true)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Complete || !r.Authenticated || r.SourceRows != prior.SourceRows || r.StorageOwners != 2 || r.Incarnations != 4 || r.PrefixVariants != 0 {
		t.Fatalf("bad census: %+v", r)
	}
	if r.Storage["current"].Rows != 10 || r.Storage["obsolete"].Rows != 5 || r.Storage["future"].Rows != 5 || r.TrimmedBytes != 4*(0+1+2+22+32) {
		t.Fatalf("bad metrics: %+v", r)
	}
	if r.RootRows != 6 || r.Runs != 3 || r.DistinctRoots != 2 || r.Gaps != 1 || r.MinHeight != 1 || r.MaxHeight != 7 || r.LargestRun != 2 {
		t.Fatalf("bad roots: %+v", r)
	}
	if r.Shielded["last-tree/current"].Rows != 1 || r.ValueKinds["zero"].Rows != 4 || r.ValueKinds["one"].Rows != 4 {
		t.Fatalf("bad shape: %+v", r)
	}
	lines, _ := os.ReadFile(filepath.Join(out, "storage-owners.jsonl"))
	if bytes.Count(lines, []byte{'\n'}) != 4 {
		t.Fatal("lost owners")
	}
	for _, line := range bytes.Split(bytes.TrimSpace(lines), []byte{'\n'}) {
		var o ownerMetric
		if err = json.Unmarshal(line, &o); err != nil {
			t.Fatal(err)
		}
		if !o.PrefixMatchesMetadata || o.Prefixes != 1 || o.SmallTailRows != 5 {
			t.Fatalf("owner %+v", o)
		}
	}
	data, _ := os.ReadFile(filepath.Join(out, "shielded-runs.jsonl"))
	var count uint64
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var run rootRun
		if err = json.Unmarshal(line, &run); err != nil {
			t.Fatal(err)
		}
		count += run.End - run.Start + 1
	}
	if count != 6 {
		t.Fatal("run coverage")
	}
	values, _ := os.ReadFile(filepath.Join(out, "storage-values.hex"))
	if len(values) != 20*65 || bytes.Count(values, []byte{'\n'}) != 20 {
		t.Fatal("exact values lost")
	}
	for _, compression := range []string{"snappy", "none"} {
		for _, model := range []string{"storage-baseline", "storage-compact-key", "storage-compact-key-trim"} {
			s := r.Experiments[model+"-"+compression]
			if s.Rows != 20 || !s.ChecksumsVerified || s.SSTBytes == 0 {
				t.Fatal("SST output", s)
			}
		}
		if r.Experiments["storage-prefix-metadata-"+compression].Rows != 4 || r.Experiments["shielded-runs-"+compression].Rows != 3 {
			t.Fatal("metadata or runs dropped")
		}
	}
	after, err := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
	if err != nil || after.SourceSHA256 != prior.SourceSHA256 {
		t.Fatal("source mutated", err)
	}
}

func TestDistributionRejectsChangedSourceAndAmbiguousPrefix(t *testing.T) {
	for _, kind := range []string{"changed", "prefix", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			prior := fixture(t, db)
			owner := common.Address{0x41, 1}
			if kind == "changed" {
				rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.AccountAsset, []byte("x"), []byte("changed"))
			}
			if kind == "prefix" {
				key := bytes.Repeat([]byte{255}, 32)
				rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, make([]byte, 32))
				prior = sealed(t, db)
			}
			ctx := context.Background()
			if kind == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			r, err := inspect(ctx, db, prior, t.TempDir(), kind == "prefix")
			if err == nil || r.Complete {
				t.Fatalf("accepted invalid scan %+v %v", r, err)
			}
		})
	}
}

func TestDistributionCLIOutputGuards(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	db, err := rawdb.NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	prior := fixture(t, db)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(prior)
	baseline := filepath.Join(root, "baseline.json")
	os.WriteFile(baseline, b, 0600)
	if err = run([]string{"--db-dir", dir, "--baseline", baseline, "--out-dir", filepath.Join(dir, "unsafe")}); err == nil {
		t.Fatal("source output allowed")
	}
	out := filepath.Join(root, "out")
	args := []string{"--db-dir", dir, "--baseline", baseline, "--out-dir", out, "--sst-experiments"}
	if err = run(args); err != nil {
		t.Fatal(err)
	}
	if err = run(args); err == nil {
		t.Fatal("overwrote report")
	}
	data, _ := os.ReadFile(filepath.Join(out, "distribution.json"))
	var r report
	if err = json.Unmarshal(data, &r); err != nil || !r.Complete {
		t.Fatalf("receipt %s %v", data, err)
	}
	prior.MuHashVerified = false
	b, _ = json.Marshal(prior)
	os.WriteFile(baseline, b, 0600)
	bad := filepath.Join(root, "bad")
	if err = run([]string{"--db-dir", dir, "--baseline", baseline, "--out-dir", bad}); err == nil {
		t.Fatal("unverified baseline accepted")
	}
	if _, err = os.Stat(bad); !os.IsNotExist(err) {
		t.Fatal("created output before baseline validation")
	}
}

func TestShieldedSubkeys(t *testing.T) {
	rows := []struct {
		k    []byte
		kind string
	}{{rawdb.MerkleTreeIndexStateKey(8), "block-root-index"}, {rawdb.IncrMerkleLastTreeStateKey(), "last-tree"}, {rawdb.IncrMerkleCurrentTreeStateKey(), "current-tree"}, {rawdb.IncrMerkleTreeStateKey(make([]byte, 32)), "root-tree"}, {rawdb.NoteCommitmentCountStateKey(), "note-count"}, {rawdb.NoteCommitmentStateKey(2), "note-commitment"}, {rawdb.NullifierStateKey(make([]byte, 32)), "nullifier"}, {rawdb.ZKProofStateKey(make([]byte, 32)), "proof-cache"}}
	for _, row := range rows {
		if got := shieldedKind(row.k); got != row.kind {
			t.Fatalf("%x = %s", row.k, got)
		}
	}
	if got := shieldedKind(binary.BigEndian.AppendUint64(nil, 1)); got == "block-root-index" {
		t.Fatal("guessed prefix")
	}
}
