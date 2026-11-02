package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func fixture(t *testing.T, mode string) (string, string, []sourceRow) {
	t.Helper()
	dir := t.TempDir()
	ssts := filepath.Join(dir, "original-ssts")
	if e := os.Mkdir(ssts, 0755); e != nil {
		t.Fatal(e)
	}
	var rows []sourceRow
	for i := 0; i < 500; i++ {
		var owner common.Address
		owner[0] = 0x41
		h := sha256.Sum256([]byte(fmt.Sprint(i)))
		copy(owner[1:], h[:20])
		if mode == "resource" {
			old := encodeResourceV1(resourceFields{words: [10]uint64{uint64(i * 2), uint64(i) * 982}, optimized: i%2 == 0})
			v := rawdb.EncodeStateKVLatestValue(old)
			rows = append(rows, sourceRow{key: rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountResourceAux, []byte{0}), wrapped: v, logical: old, owner: owner})
		} else {
			for j := 0; j < 8; j++ {
				var n [8]byte
				binary.BigEndian.PutUint64(n[:], uint64(i*8+j))
				v := rawdb.EncodeStateKVLatestValue(n[:])
				rows = append(rows, sourceRow{key: rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountAssetV2, []byte(strconv.Itoa(1000000+j))), wrapped: v, logical: append([]byte(nil), n[:]...), owner: owner})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	input := filepath.Join(dir, "source.bin")
	f, e := os.Create(input)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.New()
	w := io.MultiWriter(f, hash)
	for _, r := range rows {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(r.key)))
		w.Write(n[:])
		w.Write(r.key)
		binary.BigEndian.PutUint32(n[:], uint32(len(r.wrapped)))
		w.Write(n[:])
		w.Write(r.wrapped)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	candidate := make([]sourceRow, 0, len(rows))
	if mode == "resource" {
		for _, r := range rows {
			v, e := compactResource(r.logical)
			if e != nil {
				t.Fatal(e)
			}
			r.wrapped = rawdb.EncodeStateKVLatestValue(v)
			candidate = append(candidate, r)
		}
	} else {
		for i := 0; i < len(rows); i += 8 {
			group := rows[i : i+8]
			payload := append([]byte(nil), bundleMagic...)
			payload = binary.AppendUvarint(payload, 8)
			var prev []byte
			for j, r := range group {
				_, _, _, key, _ := rawdb.DecodeStateKVLatestKey(r.key)
				if j > 0 {
					n := 0
					for n < len(prev) && n < len(key) && prev[n] == key[n] {
						n++
					}
					payload = binary.AppendUvarint(payload, uint64(n))
					payload = appendBlob(payload, key[n:])
				}
				payload = appendBlob(payload, r.wrapped)
				prev = key
			}
			candidate = append(candidate, sourceRow{key: group[0].key, wrapped: payload})
		}
	}
	caseMeta := map[string]any{}
	for arm, data := range map[string][]sourceRow{"old": rows, "candidate": candidate} {
		path := filepath.Join(ssts, "snappy-"+arm+".sst")
		v, e := vfs.Default.Create(path)
		if e != nil {
			t.Fatal(e)
		}
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(v), sstable.WriterOptions{Compression: sstable.SnappyCompression, FilterPolicy: bloom.FilterPolicy(10), BlockSize: 4096, IndexBlockSize: 4096, TableFormat: sstable.TableFormatPebblev4})
		for _, r := range data {
			if e = writer.Set(r.key, r.wrapped); e != nil {
				t.Fatal(e)
			}
		}
		if e = writer.Close(); e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		caseMeta[arm] = map[string]any{"bytes": len(b), "sha256": fmt.Sprintf("%x", sha256.Sum256(b))}
	}
	meta := map[string]any{"input_subset_sha256": hex.EncodeToString(hash.Sum(nil)), "rows": len(rows), "source_rows": len(rows), "sst_cases": map[string]any{"snappy": caseMeta}}
	if e = writeJSON(filepath.Join(ssts, "results.json"), meta); e != nil {
		t.Fatal(e)
	}
	return input, ssts, rows
}
func TestExactControlProductionBloomAndBundleRange(t *testing.T) {
	for _, mode := range []string{"resource", "bundle"} {
		t.Run(mode, func(t *testing.T) {
			input, ssts, rows := fixture(t, mode)
			out := filepath.Join(t.TempDir(), "control")
			if e := run(input, ssts, out, mode, len(rows), 200, 1, []string{"snappy"}); e != nil {
				t.Fatal(e)
			}
			b, e := os.ReadFile(filepath.Join(out, "results.json"))
			if e != nil {
				t.Fatal(e)
			}
			var result struct {
				Cases map[string]struct {
					Measurements []struct {
						Arm      string `json:"arm"`
						Workload string `json:"workload"`
						Cold     struct {
							Rejects  int64 `json:"bloom_rejections"`
							Verified bool  `json:"all_results_verified"`
						} `json:"empty_app_cache"`
						Warm struct {
							Rejects  int64 `json:"bloom_rejections"`
							Verified bool  `json:"all_results_verified"`
						} `json:"warm_app_cache"`
					}
				} `json:"cases"`
			}
			if e = json.Unmarshal(b, &result); e != nil {
				t.Fatal(e)
			}
			checked := 0
			for _, m := range result.Cases["snappy"].Measurements {
				if !m.Cold.Verified || !m.Warm.Verified {
					t.Fatal("unverified results")
				}
				if m.Workload == "negative_same_owner" {
					if m.Arm == "old" || (mode == "resource" && m.Arm == "candidate") {
						if m.Cold.Rejects < 180 || m.Warm.Rejects < 180 {
							t.Fatalf("exact cursor did not reject >=90%% of misses with real L6 Bloom: %+v", m)
						}
						checked++
					} else if m.Cold.Rejects != 0 || m.Warm.Rejects != 0 {
						t.Fatal("ordinary bundle range seek unexpectedly used exact filter")
					}
				}
			}
			if checked == 0 {
				t.Fatal("no negative controls")
			}
			for _, arm := range []string{"old", "candidate"} {
				if _, e = os.Stat(filepath.Join(ssts, "snappy-"+arm+".sst")); e != nil {
					t.Fatal("original SST consumed")
				}
			}
			if mode == "bundle" {
				cache := pebble.NewCache(32 << 20)
				defer cache.Unref()
				db, e := pebble.Open(filepath.Join(out, "snappy", "candidate", "db"), options(vfs.Default, cache, true))
				if e != nil {
					t.Fatal(e)
				}
				defer db.Close()
				snapshot := db.NewSnapshot()
				defer snapshot.Close()
				cursor, e := snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16, UseL6Filters: true})
				if e != nil {
					t.Fatal(e)
				}
				defer cursor.Close()
				if _, e = cursor.Get(rows[1].key); e != pebble.ErrNotFound {
					t.Fatal("bundle unexpectedly supports unchanged exact physical Get", e)
				}
			}
		})
	}
}
