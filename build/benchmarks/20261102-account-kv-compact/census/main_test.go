package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func address(i byte) common.Address { var a common.Address; a[0] = 0x41; a[20] = i; return a }
func accountValue(t *testing.T, generation uint64) []byte {
	t.Helper()
	v, e := (&state.StateAccountV3{Version: state.StateAccountVersion, AccountProto: []byte{1, 2, 3}, AccountKVRoot: state.EmptyKVRoot, AccountKVGeneration: generation}).Encode()
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func TestCensusSortedGenerationAccountJoinAndExports(t *testing.T) {
	r := filepath.Join(t.TempDir(), "maintenance")
	source := filepath.Join(r, "chaindata")
	if e := os.MkdirAll(source, 0755); e != nil {
		t.Fatal(e)
	}
	db, e := pebble.Open(source, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	put := func(k, v []byte) {
		if e := db.Set(k, v, pebble.NoSync); e != nil {
			t.Fatal(e)
		}
	}
	// Owner 1: real account generation zero, no generation watermark (normal).
	put(rawdb.StateAccountLatestCommitmentKey(address(1)), accountValue(t, 0))
	// Owner 2: current=2, table=2; keep one obsolete gen=1 row.
	put(rawdb.StateAccountLatestCommitmentKey(address(2)), accountValue(t, 2))
	put(rawdb.StateKVGenerationCommitmentKey(address(2)), rawdb.EncodeStateKVGenerationValue(2))
	// Owner 3: absent account, watermark=1, row is not active even at gen=1.
	put(rawdb.StateKVGenerationCommitmentKey(address(3)), rawdb.EncodeStateKVGenerationValue(1))
	// Owner 4: envelope/table mismatch, table must not determine live generation.
	put(rawdb.StateAccountLatestCommitmentKey(address(4)), accountValue(t, 3))
	put(rawdb.StateKVGenerationCommitmentKey(address(4)), rawdb.EncodeStateKVGenerationValue(4))
	// Owner 5: no KV, nonzero account envelope without table must still be checked.
	put(rawdb.StateAccountLatestCommitmentKey(address(5)), accountValue(t, 7))
	// Owner 6: watermark with neither account nor KV must still be exported.
	put(rawdb.StateKVGenerationCommitmentKey(address(6)), rawdb.EncodeStateKVGenerationValue(5))
	add := func(owner byte, gen uint64, domain kvdomains.KVDomain, key, value []byte) {
		put(rawdb.StateKVLatestCommitmentKey(address(owner), gen, domain, key), rawdb.EncodeStateKVLatestValue(value))
	}
	add(1, 0, kvdomains.AccountResourceAux, []byte{0}, []byte("resource"))
	add(1, 0, kvdomains.AccountAssetV2, []byte("1000001"), make([]byte, 8))
	add(2, 1, kvdomains.ContractStorage, []byte("old"), []byte("old value"))
	add(2, 2, kvdomains.ContractStorage, []byte("now"), make([]byte, 32))
	add(2, 3, kvdomains.ContractStorage, []byte("future"), nil)
	add(3, 1, kvdomains.SystemReward, []byte("reward"), []byte("inaccessible"))
	add(4, 3, kvdomains.ContractStorage, []byte("now"), []byte("current"))
	add(4, 4, kvdomains.ContractStorage, []byte("table generation"), []byte("future"))
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	out := filepath.Join(r, "census")
	if e = run(source, out, 123, 0); e != nil {
		t.Fatal(e)
	}
	var result struct {
		Complete        bool          `json:"complete"`
		KV              uint64        `json:"kv_rows"`
		Accounts        uint64        `json:"account_rows"`
		Gen             uint64        `json:"generation_rows"`
		Mismatch        uint64        `json:"account_generation_table_mismatches"`
		NonzeroMissing  uint64        `json:"nonzero_account_generation_without_table_row"`
		Implicit        uint64        `json:"zero_account_generation_without_table_row"`
		MissingOwner    uint64        `json:"kv_owners_without_account"`
		GenWithoutOwner uint64        `json:"generation_rows_without_account"`
		Domains         []domainStats `json:"domains"`
	}
	b, e := os.ReadFile(filepath.Join(out, "summary.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &result); e != nil {
		t.Fatal(e)
	}
	if !result.Complete || result.KV != 8 || result.Accounts != 4 || result.Gen != 4 || result.Mismatch != 1 || result.NonzeroMissing != 1 || result.Implicit != 1 || result.MissingOwner != 1 || result.GenWithoutOwner != 2 {
		t.Fatalf("bad summary %+v", result)
	}
	var all, current, obsolete, future, missing uint64
	for _, d := range result.Domains {
		all += d.Counts.All.Rows
		current += d.Counts.Current.Rows
		obsolete += d.Counts.Obsolete.Rows
		future += d.Counts.Future.Rows
		missing += d.Counts.Missing.Rows
		if d.Name == "ContractStorage" && (d.Counts.All.Rows != 5 || d.Counts.Current.Rows != 2 || d.Counts.Obsolete.Rows != 1 || d.Counts.Future.Rows != 2 || d.Owners != 2) {
			t.Fatalf("bad contract stats %+v", d)
		}
	}
	if all != 8 || current != 4 || obsolete != 1 || future != 2 || missing != 1 {
		t.Fatal("classification did not close", all, current, obsolete, future, missing)
	}
	for filename, want := range map[string]int{"kv_generation.bin": 4, "ContractStorage.bin": 5, "SystemReward.bin": 1, "AccountResourceAux.bin": 1, "AccountAssetV2.bin": 1} {
		f, e := os.Open(filepath.Join(out, filename))
		if e != nil {
			t.Fatal(e)
		}
		reader := bufio.NewReader(f)
		n := 0
		var last []byte
		for {
			var size [4]byte
			_, e = io.ReadFull(reader, size[:])
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			key := make([]byte, binary.BigEndian.Uint32(size[:]))
			if _, e = io.ReadFull(reader, key); e != nil {
				t.Fatal(e)
			}
			if _, e = io.ReadFull(reader, size[:]); e != nil {
				t.Fatal(e)
			}
			value := make([]byte, binary.BigEndian.Uint32(size[:]))
			if _, e = io.ReadFull(reader, value); e != nil {
				t.Fatal(e)
			}
			if string(last) >= string(key) && n > 0 {
				t.Fatal("unordered export")
			}
			last = key
			n++
			if filename == "kv_generation.bin" {
				if _, e = rawdb.DecodeStateKVGenerationValue(value); e != nil {
					t.Fatal(e)
				}
			} else {
				if e = rawdb.ValidateStateKVLatestValue(value); e != nil {
					t.Fatal(e)
				}
			}
		}
		f.Close()
		if n != want {
			t.Fatal(filename, n, want)
		}
	}
}
func TestProductionPathDenied(t *testing.T) {
	for _, p := range []string{"/data/gtron/main/datadir/gtron/chaindata", "/tmp/chaindata"} {
		if _, e := allowedSource(p); e == nil {
			t.Fatal("production/unapproved source allowed", p)
		}
	}
}

func TestPriorityRewardUsesPublicSystemOwnerAndAccountGeneration(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "db")
	db, err := pebble.Open(source, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	owner := common.SystemAccountAddress
	put := func(k, v []byte) {
		if err := db.Set(k, v, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	put(rawdb.StateAccountLatestCommitmentKey(owner), accountValue(t, 3))
	// A conflicting watermark must not override the account's active namespace.
	put(rawdb.StateKVGenerationCommitmentKey(owner), rawdb.EncodeStateKVGenerationValue(4))
	for _, gen := range []uint64{2, 3, 4} {
		for _, key := range []string{"reward-1", "reward-2"} {
			put(rawdb.StateKVLatestCommitmentKey(owner, gen, kvdomains.SystemReward, []byte(key)), rawdb.EncodeStateKVLatestValue([]byte(key)))
		}
	}
	put(rawdb.StateKVLatestCommitmentKey(address(1), 3, kvdomains.SystemReward, []byte("other-owner")), rawdb.EncodeStateKVLatestValue([]byte("other")))
	out := filepath.Join(dir, "out")
	if err = os.Mkdir(out, 0755); err != nil {
		t.Fatal(err)
	}
	meta, err := priorityReward(db, out, 123, 0)
	if err != nil {
		t.Fatal(err)
	}
	if meta["generation"].(uint64) != 3 || meta["export"].(map[string]any)["rows"].(uint64) != 2 {
		t.Fatalf("bad priority metadata %+v", meta)
	}
}
