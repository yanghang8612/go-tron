package state

// Run explicitly with GTRON_PERMISSION_COHORT=/.../permission-cohort.bin and
// GTRON_PERMISSION_OUT=/.../new-output go test ./core/state
// -run '^TestAccountPermissionV2FixedCohort$' -count=1 -timeout=15m.
// This test never opens the source database or operates on production SSTs.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const permissionV2FixedCohortSHA = "77f5db2d25223ea7d5bb52a1fcca72b87471993a37db947b7386b4088aae8b3b"

type permissionCorpusRow struct {
	key, old, candidate []byte
	owner               common.Address
}

var permissionCorpusComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(k []byte) int { return len(k) }
	return &c
}()

type permissionCorpusFS struct {
	vfs.FS
	calls, bytes atomic.Int64
}

type permissionCorpusFile struct {
	vfs.File
	owner *permissionCorpusFS
}

func (f *permissionCorpusFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err == nil && strings.HasSuffix(name, ".sst") {
		return &permissionCorpusFile{file, f}, nil
	}
	return file, err
}

func (f *permissionCorpusFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.owner.calls.Add(1)
	f.owner.bytes.Add(int64(n))
	return n, err
}

func permissionCorpusOptions(fs vfs.FS, cache *pebble.Cache, readOnly bool) *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	return &pebble.Options{FS: fs, Cache: cache, Comparer: permissionCorpusComparer,
		Levels: levels, ReadOnly: readOnly, FormatMajorVersion: pebble.FormatNewest,
		DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 1 }, MaxOpenFiles: 128, LBaseMaxBytes: 1}
}

func permissionCorpusRead(path string) ([]permissionCorpusRow, map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	rows := make([]permissionCorpusRow, 0, 500000)
	modes := map[string]int{}
	var keyBytes, oldBytes, newBytes uint64
	readBlob := func(max uint32, eofAllowed bool) ([]byte, error) {
		var n [4]byte
		_, err := io.ReadFull(r, n[:])
		if err != nil {
			if eofAllowed && err == io.EOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read framed length: %w", err)
		}
		length := binary.BigEndian.Uint32(n[:])
		if length > max {
			return nil, fmt.Errorf("row field %d exceeds bounded cohort limit %d", length, max)
		}
		v := make([]byte, length)
		_, err = io.ReadFull(r, v)
		return v, err
	}
	for {
		key, err := readBlob(1024, true)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if len(rows) == 500000 {
			return nil, nil, fmt.Errorf("input exceeds fixed 500k row bound")
		}
		value, err := readBlob(1<<20, false)
		if err != nil {
			return nil, nil, err
		}
		owner, _, domain, logical, ok := rawdb.DecodeStateKVLatestKey(key)
		if !ok || domain != kvdomains.AccountPermissionAux || len(rows) > 0 && bytes.Compare(rows[len(rows)-1].key, key) >= 0 {
			return nil, nil, fmt.Errorf("cohort key is not sorted unique PermissionAux")
		}
		native, err := rawdb.DecodeStateKVLatestValueNoCopy(value)
		if err != nil {
			return nil, nil, err
		}
		p, _, err := decodeAccountPermissionRow(owner, logical, native)
		if err != nil {
			return nil, nil, err
		}
		candidate, err := appendAccountPermissionForOwner(nil, owner, logical, p)
		if err != nil {
			return nil, nil, err
		}
		got, _, err := decodeAccountPermissionRow(owner, logical, candidate)
		if err != nil {
			return nil, nil, err
		}
		canonical, err := encodeAccountPermission(got)
		if err != nil || !bytes.Equal(canonical, native) {
			return nil, nil, fmt.Errorf("row %d does not restore exact old canonical bytes: %v", len(rows), err)
		}
		mode := "v1_raw_fallback"
		if len(candidate) >= 7 && candidate[4] == accountPermissionRowVersion {
			mode = fmt.Sprintf("v2_mode_%d", candidate[6])
		}
		wrapped := rawdb.EncodeStateKVLatestValue(candidate)
		rows = append(rows, permissionCorpusRow{key, value, wrapped, owner})
		modes[mode]++
		keyBytes += uint64(len(key))
		oldBytes += uint64(len(value))
		newBytes += uint64(len(wrapped))
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("empty permission cohort")
	}
	return rows, map[string]any{"rows": len(rows), "source_cohort_sha256": hex.EncodeToString(h.Sum(nil)),
		"key_bytes": keyBytes, "old_wrapped_value_bytes": oldBytes, "new_wrapped_value_bytes": newBytes,
		"modes": modes, "all_rows_restore_exact_old_canonical_native": true, "physical_keys_unchanged": true}, nil
}

func permissionCorpusSST(path string, rows []permissionCorpusRow, candidate bool) (map[string]any, error) {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return nil, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: permissionCorpusComparer, Compression: sstable.SnappyCompression,
		BlockSize: 4096, IndexBlockSize: 4096, FilterPolicy: bloom.FilterPolicy(10), TableFormat: sstable.TableFormatPebblev4})
	for _, row := range rows {
		v := row.old
		if candidate {
			v = row.candidate
		}
		if err := w.Set(row.key, v); err != nil {
			w.Close()
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	// Ingest a private copy, retaining both comparison SSTs for reproducibility.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	input := path + ".ingest-copy"
	if err := os.WriteFile(input, data, 0644); err != nil {
		return nil, err
	}
	copyInfo, err := os.Stat(input)
	if err != nil {
		return nil, err
	}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	dbPath := path + ".db"
	db, err := pebble.Open(dbPath, permissionCorpusOptions(vfs.Default, cache, false))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := db.Ingest([]string{input}); err != nil {
		return nil, err
	}
	tables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		return nil, err
	}
	count, filter := 0, uint64(0)
	for level, files := range tables {
		for _, file := range files {
			count++
			if level != 6 || file.Properties == nil || file.Properties.FilterSize == 0 {
				return nil, fmt.Errorf("expected exact full-key Bloom L6 table, got L%d", level)
			}
			filter += file.Properties.FilterSize
			info, err := os.Stat(filepath.Join(dbPath, fmt.Sprintf("%06d.sst", file.FileNum)))
			if err != nil || !os.SameFile(copyInfo, info) || info.Size() != int64(len(data)) {
				return nil, fmt.Errorf("ingest rewrote SST: %v", err)
			}
		}
	}
	if count != 1 {
		return nil, fmt.Errorf("expected one immutable L6 SST, got %d", count)
	}
	return map[string]any{"sst_path": path, "sst_bytes": len(data), "sst_sha256": hex.EncodeToString(hash[:]),
		"db_path": dbPath, "l6_filter_bytes": filter, "l6_tables": count, "ingested_private_copy_unchanged": true}, nil
}

type permissionCorpusQuery struct {
	key, logical []byte
	owner        common.Address
	want         *corepb.Permission
}

func permissionCorpusQueries(rows []permissionCorpusRow, negative bool) ([]permissionCorpusQuery, error) {
	indices := rand.New(rand.NewSource(113177)).Perm(len(rows))[:min(10000, len(rows))]
	q := make([]permissionCorpusQuery, 0, len(indices))
	for _, index := range indices {
		r := rows[index]
		owner, gen, domain, logical, _ := rawdb.DecodeStateKVLatestKey(r.key)
		if negative {
			var key []byte
			for id := int32(mathMaxPermissionID); ; id-- {
				logical = accountActivePermissionKey(id)
				key = rawdb.StateKVLatestCommitmentKey(owner, gen, domain, logical)
				at := sort.Search(len(rows), func(i int) bool { return bytes.Compare(rows[i].key, key) >= 0 })
				if at == len(rows) || !bytes.Equal(rows[at].key, key) {
					break
				}
			}
			q = append(q, permissionCorpusQuery{key: key, logical: logical, owner: owner})
			continue
		}
		native, err := rawdb.DecodeStateKVLatestValueNoCopy(r.old)
		if err != nil {
			return nil, err
		}
		want, _, err := decodeAccountPermissionRow(owner, logical, native)
		if err != nil {
			return nil, err
		}
		q = append(q, permissionCorpusQuery{r.key, logical, owner, want})
	}
	return q, nil
}

const mathMaxPermissionID = 1<<31 - 1

func permissionCorpusPoint(path string, q []permissionCorpusQuery, group int) (map[string]any, error) {
	fs := &permissionCorpusFS{FS: vfs.Default}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	db, err := pebble.Open(path, permissionCorpusOptions(fs, cache, true))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	out := map[string]any{"operations_per_pass": len(q), "scope_group_rows": group, "cache_bytes": 32 << 20,
		"cursor": "Snapshot.NewGetCursorWithOptions(MaxRetainedTables=16,UseL6Filters=true)",
		"scope":  "matched L6, native-to-API decode and every result checked; app cache empty then warm; OS cache uncontrolled; not HTTP"}
	for pass := 0; pass < 2; pass++ {
		calls, n := fs.calls.Load(), fs.bytes.Load()
		filters := db.Metrics().Filter
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		for at := 0; at < len(q); at += group {
			snapshot := db.NewSnapshot()
			cursor, err := snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16, UseL6Filters: true})
			if err != nil {
				snapshot.Close()
				return nil, err
			}
			for _, r := range q[at:min(at+group, len(q))] {
				v, err := cursor.Get(r.key)
				found := err == nil
				if err != nil && err != pebble.ErrNotFound || found != (r.want != nil) {
					cursor.Close()
					snapshot.Close()
					return nil, fmt.Errorf("point presence mismatch: %v", err)
				}
				if found {
					native, err := rawdb.DecodeStateKVLatestValueNoCopy(v)
					var got *corepb.Permission
					if err == nil {
						got, _, err = decodeAccountPermissionRow(r.owner, r.logical, native)
					}
					if err != nil || !proto.Equal(got, r.want) {
						cursor.Close()
						snapshot.Close()
						return nil, fmt.Errorf("point API permission mismatch: %v", err)
					}
					if err := cursor.ReleaseValue(); err != nil {
						cursor.Close()
						snapshot.Close()
						return nil, err
					}
				}
			}
			if err := cursor.Close(); err != nil {
				snapshot.Close()
				return nil, err
			}
			if err := snapshot.Close(); err != nil {
				return nil, err
			}
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		filter := db.Metrics().Filter
		label := "empty_app_cache"
		if pass == 1 {
			label = "warm_app_cache"
		}
		out[label] = map[string]any{"wall_ns": elapsed.Nanoseconds(), "sst_read_calls": fs.calls.Load() - calls,
			"sst_read_bytes": fs.bytes.Load() - n, "bloom_rejections": filter.Hits - filters.Hits,
			"bloom_may_contain": filter.Misses - filters.Misses, "mallocs": after.Mallocs - before.Mallocs,
			"allocated_bytes": after.TotalAlloc - before.TotalAlloc, "every_result_verified": true}
	}
	return out, nil
}

func TestAccountPermissionV2FixedCohort(t *testing.T) {
	source, out := os.Getenv("GTRON_PERMISSION_COHORT"), os.Getenv("GTRON_PERMISSION_OUT")
	if source == "" && out == "" {
		t.Skip("set GTRON_PERMISSION_COHORT and GTRON_PERMISSION_OUT for offline fixed-cohort measurement")
	}
	if source == "" || out == "" {
		t.Fatal("both explicit source cohort and new output directory are required")
	}
	for _, path := range []string{source, out} {
		absolute, err := filepath.Abs(path)
		if err != nil || strings.Contains(filepath.ToSlash(absolute), "/main/datadir/") {
			t.Fatal("production datadir paths forbidden", path, err)
		}
	}
	if err := os.Mkdir(out, 0755); err != nil {
		t.Fatal("output must be a new independent directory", err)
	}
	started := time.Now()
	rows, stats, err := permissionCorpusRead(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 500000 || stats["source_cohort_sha256"] != permissionV2FixedCohortSHA {
		t.Fatal("input does not match original fixed 500k permission cohort", stats)
	}
	arms := map[string]map[string]any{}
	for _, arm := range []string{"baseline", "candidate"} {
		arms[arm], err = permissionCorpusSST(filepath.Join(out, arm+".sst"), rows, arm == "candidate")
		if err != nil {
			t.Fatal(err)
		}
	}
	if arms["baseline"]["l6_filter_bytes"] != arms["candidate"]["l6_filter_bytes"] {
		t.Fatal("unchanged full-key set produced different Bloom filter sizes")
	}
	var measurements []map[string]any
	for round := 0; round < 2; round++ {
		order := []string{"baseline", "candidate"}
		if round == 1 {
			order = []string{"candidate", "baseline"}
		}
		for _, negative := range []bool{false, true} {
			q, err := permissionCorpusQueries(rows, negative)
			if err != nil {
				t.Fatal(err)
			}
			for _, group := range []int{1, 32} {
				for _, arm := range order {
					measurement, err := permissionCorpusPoint(arms[arm]["db_path"].(string), q, group)
					if err != nil {
						t.Fatal(err)
					}
					measurement["arm"], measurement["negative"], measurement["round"] = arm, negative, round
					measurements = append(measurements, measurement)
				}
			}
		}
	}
	result := map[string]any{"source": source, "cohort": stats, "arms": arms, "point_measurements": measurements,
		"started_utc": started.UTC().Format(time.RFC3339Nano), "finished_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"scope": "actual production codec, original fixed 500k owner-prefix cohort, matched Snappy SSTs and L6 Bloom; not HTTP, mixed LSM, OS-cold, or whole-DB saving"}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "results.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("PERMISSION_V2_FIXED_COHORT_COMPLETE rows=%d result=%s", len(rows), filepath.Join(out, "results.json"))
}

func TestAccountPermissionV2CorpusControlFixture(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 256; i++ {
		var owner common.Address
		owner[0] = 0x41
		binary.BigEndian.PutUint32(owner[17:], i)
		for _, row := range []struct {
			key []byte
			p   *corepb.Permission
		}{{accountOwnerPermissionKey, types.MakeDefaultOwnerPermission(owner)},
			{accountActivePermissionKey(2), types.MakeDefaultActivePermission(owner, bytes.Repeat([]byte{byte(i)}, 32))}} {
			key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountPermissionAux, row.key)
			native, err := encodeAccountPermission(row.p)
			if err != nil {
				t.Fatal(err)
			}
			for _, blob := range [][]byte{key, rawdb.EncodeStateKVLatestValue(native)} {
				var n [4]byte
				binary.BigEndian.PutUint32(n[:], uint32(len(blob)))
				if _, err := f.Write(n[:]); err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write(blob); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	rows, stats, err := permissionCorpusRead(path)
	if err != nil || len(rows) != 512 || stats["all_rows_restore_exact_old_canonical_native"] != true {
		t.Fatal("fixture corpus", len(rows), stats, err)
	}
	arms := map[string]map[string]any{}
	for _, arm := range []string{"baseline", "candidate"} {
		arms[arm], err = permissionCorpusSST(filepath.Join(dir, arm+".sst"), rows, arm == "candidate")
		if err != nil {
			t.Fatal(err)
		}
		for _, negative := range []bool{false, true} {
			q, err := permissionCorpusQueries(rows, negative)
			if err != nil {
				t.Fatal(err)
			}
			for _, group := range []int{1, 32} {
				if _, err := permissionCorpusPoint(arms[arm]["db_path"].(string), q, group); err != nil {
					t.Fatal("fixture point", arm, negative, group, err)
				}
			}
		}
	}
	if arms["baseline"]["l6_filter_bytes"] != arms["candidate"]["l6_filter_bytes"] || arms["baseline"]["sst_bytes"].(int) <= arms["candidate"]["sst_bytes"].(int) {
		t.Fatal("fixture keys/filter changed or no Snappy gain", arms)
	}
}
