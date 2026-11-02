package main

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
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var bundleMagic = []byte{'G', 'T', 'P', 'G', 1}

const bundleMaxRows = 16
const bundleMaxBytes = 16 << 10

func bundleEligible(k []byte) bool {
	_, _, domain, _, ok := rawdb.DecodeStateKVLatestKey(k)
	return ok && domain >= kvdomains.AccountAsset && domain <= kvdomains.AccountAssetOperationTimeV2
}
func sameOwnerDomain(a, b []byte) bool {
	oa, ga, da, _, oka := rawdb.DecodeStateKVLatestKey(a)
	ob, gb, db, _, okb := rawdb.DecodeStateKVLatestKey(b)
	return oka && okb && oa == ob && ga == gb && da == db
}
func sharedPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
func encodeBundle(rows []row) ([]byte, error) {
	if len(rows) < 2 || len(rows) > bundleMaxRows {
		return nil, fmt.Errorf("bundle row count")
	}
	dst := append([]byte(nil), bundleMagic...)
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	_, _, _, firstKey, ok := rawdb.DecodeStateKVLatestKey(rows[0].key)
	if !ok || !bundleEligible(rows[0].key) {
		return nil, fmt.Errorf("ineligible bundle")
	}
	prev := firstKey
	for i, r := range rows {
		if !sameOwnerDomain(rows[0].key, r.key) {
			return nil, fmt.Errorf("mixed bundle owner/domain")
		}
		_, _, _, key, _ := rawdb.DecodeStateKVLatestKey(r.key)
		if i > 0 {
			if bytes.Compare(rows[i-1].key, r.key) >= 0 {
				return nil, fmt.Errorf("unordered bundle")
			}
			n := sharedPrefix(prev, key)
			dst = binary.AppendUvarint(dst, uint64(n))
			dst = appendBlob(dst, key[n:])
		}
		dst = appendBlob(dst, r.old)
		prev = key
	}
	if len(dst) > bundleMaxBytes {
		return nil, fmt.Errorf("bundle payload bound")
	}
	return dst, nil
}
func decodeBundle(firstKey, payload []byte) ([]row, error) {
	if len(payload) > bundleMaxBytes || !bytes.HasPrefix(payload, bundleMagic) {
		return nil, fmt.Errorf("not bounded bundle")
	}
	d := decoder{data: payload, off: len(bundleMagic)}
	count, err := d.word()
	if err != nil {
		return nil, err
	}
	if count < 2 || count > bundleMaxRows {
		return nil, fmt.Errorf("invalid bundle row count")
	}
	owner, generation, domain, logical, ok := rawdb.DecodeStateKVLatestKey(firstKey)
	if !ok || !bundleEligible(firstKey) {
		return nil, fmt.Errorf("bad bundle key")
	}
	prev := logical
	rows := make([]row, 0, int(count))
	for i := uint64(0); i < count; i++ {
		key := firstKey
		if i > 0 {
			shared, err := d.word()
			if err != nil || shared > uint64(len(prev)) {
				return nil, fmt.Errorf("invalid shared key prefix")
			}
			suffix, err := d.blob()
			if err != nil {
				return nil, err
			}
			logical = append(append([]byte(nil), prev[:shared]...), suffix...)
			key = rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logical)
			if bytes.Compare(rows[len(rows)-1].key, key) >= 0 {
				return nil, fmt.Errorf("unordered decoded bundle")
			}
		}
		v, err := d.blob()
		if err != nil {
			return nil, err
		}
		if err = rawdb.ValidateStateKVLatestValue(v); err != nil {
			return nil, err
		}
		rows = append(rows, row{key: key, old: v, candidate: v})
		prev = logical
	}
	if err = d.done(); err != nil {
		return nil, err
	}
	return rows, nil
}
func packOwnerBundles(input []row, limit int) ([]row, int, error) {
	if limit < 2 || limit > bundleMaxRows {
		return nil, 0, fmt.Errorf("bundle entries must be 2..16")
	}
	output := make([]row, 0, len(input))
	groups := 0
	for i := 0; i < len(input); {
		end := i + 1
		if bundleEligible(input[i].key) {
			for end < len(input) && end-i < limit && sameOwnerDomain(input[i].key, input[end].key) {
				end++
			}
		}
		var payload []byte
		var err error
		for end-i > 1 {
			payload, err = encodeBundle(input[i:end])
			if err == nil {
				break
			}
			end--
		}
		if end-i <= 1 {
			output = append(output, input[i])
			i++
			continue
		}
		restored, err := decodeBundle(input[i].key, payload)
		if err != nil {
			return nil, 0, err
		}
		for j, r := range restored {
			if !bytes.Equal(r.key, input[i+j].key) || !bytes.Equal(r.old, input[i+j].old) {
				return nil, 0, fmt.Errorf("bundle changed canonical physical row")
			}
		}
		output = append(output, row{key: input[i].key, old: payload, candidate: payload})
		groups++
		i = end
	}
	return output, groups, nil
}
func bundlePointBench(path string, source []row, packed, negative bool, ops, cacheMiB int) (map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rc := &readCounter{File: f}
	rd, err := sstable.NewSimpleReadable(rc)
	if err != nil {
		f.Close()
		return nil, err
	}
	cache := pebble.NewCache(int64(cacheMiB) << 20)
	defer cache.Unref()
	reader, err := sstable.NewReader(rd, sstable.ReaderOptions{Cache: cache, Filters: map[string]sstable.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if ops > len(source) {
		ops = len(source)
	}
	indices := rand.New(rand.NewSource(113177)).Perm(len(source))[:ops]
	result := map[string]any{"operations_per_pass": ops, "cache_bytes": int64(cacheMiB) << 20, "negative": negative, "scope": "random standalone SST seeks; packed arm can seek predecessor and decode <=16 original rows; iterator path does not use Bloom filters, so miss measurements do not establish real point-Get equivalence; LSM write amplification not simulated; application-cold only, OS cache uncontrolled"}
	for pass := 0; pass < 2; pass++ {
		start := time.Now()
		calls, n := rc.calls, rc.bytes
		var scanned, predecessors int
		for _, index := range indices {
			want := source[index]
			target := want.key
			if negative {
				target = append(append([]byte(nil), target...), 0)
				for {
					i := sort.Search(len(source), func(i int) bool { return bytes.Compare(source[i].key, target) >= 0 })
					if i == len(source) || !bytes.Equal(source[i].key, target) {
						break
					}
					target = append(target, 0)
				}
			}
			k, lv := it.SeekGE(target, 0)
			if packed && (k == nil || !bytes.Equal(k.UserKey, target)) {
				k, lv = it.SeekLT(target, 0)
				predecessors++
			}
			if k == nil {
				if negative {
					continue
				}
				return nil, fmt.Errorf("lookup has no containing bundle")
			}
			value, _, err := lv.Value(nil)
			if err != nil {
				return nil, err
			}
			found := false
			if packed && bytes.HasPrefix(value, bundleMagic) {
				rows, err := decodeBundle(k.UserKey, value)
				if err != nil {
					return nil, err
				}
				scanned += len(rows)
				for _, r := range rows {
					if bytes.Equal(r.key, target) {
						if !bytes.Equal(r.old, want.old) {
							return nil, fmt.Errorf("bundle point value mismatch")
						}
						found = true
						break
					}
				}
			} else {
				scanned++
				found = bytes.Equal(k.UserKey, target) && bytes.Equal(value, want.old)
			}
			if found == negative {
				return nil, fmt.Errorf("bundle result presence mismatch: found=%t negative=%t", found, negative)
			}
		}
		label := "empty_app_cache"
		if pass == 1 {
			label = "warm_app_cache"
		}
		result[label] = map[string]any{"wall_ns": time.Since(start).Nanoseconds(), "read_calls": rc.calls - calls, "read_bytes": rc.bytes - n, "decoded_rows": scanned, "predecessor_seeks": predecessors}
	}
	return result, nil
}
func runBundle(input, out string, maxRows, ops, cache, limit int) error {
	if err := os.Mkdir(out, 0755); err != nil {
		return err
	}
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	source := make([]row, 0, min(maxRows, 100000))
	var oldKeyBytes, oldValueBytes int64
	domains := map[string]int{}
	subsetHash := sha256.New()
	for len(source) < maxRows {
		k, v, err := nextRecord(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(source) > 0 && bytes.Compare(source[len(source)-1].key, k) >= 0 {
			return fmt.Errorf("unordered input")
		}
		_, _, domain, _, ok := rawdb.DecodeStateKVLatestKey(k)
		if !ok {
			return fmt.Errorf("unrecognized input key")
		}
		if err = rawdb.ValidateStateKVLatestValue(v); err != nil {
			return err
		}
		domains[kvdomains.Name(domain)]++
		source = append(source, row{key: k, old: v, candidate: v})
		digestRecord(subsetHash, k, v)
		oldKeyBytes += int64(len(k))
		oldValueBytes += int64(len(v))
	}
	if len(source) == 0 {
		return fmt.Errorf("empty input")
	}
	packed, groups, err := packOwnerBundles(source, limit)
	if err != nil {
		return err
	}
	var newKeyBytes, newValueBytes int64
	for _, r := range packed {
		newKeyBytes += int64(len(r.key))
		newValueBytes += int64(len(r.old))
	}
	result := map[string]any{"input": input, "input_subset_sha256": hex.EncodeToString(subsetHash.Sum(nil)), "input_subset_bytes": oldKeyBytes + oldValueBytes + int64(len(source))*8, "input_hash_scope": "exact consumed record prefix; full source identity belongs to census export receipt", "source_rows": len(source), "stored_candidate_rows": len(packed), "bundles": groups, "max_bundle_rows": limit, "max_bundle_payload_bytes": bundleMaxBytes, "domains": domains, "old_key_bytes": oldKeyBytes, "old_value_bytes": oldValueBytes, "candidate_key_bytes": newKeyBytes, "candidate_value_bytes": newValueBytes, "every_original_physical_key_and_wrapped_value_restored_exactly": true, "scope": "experimental bounded same-owner/generation/domain scalar-map bundles; no production read/write/root/snapshot integration; not net whole-DB or HTTP gain"}
	cases := map[string]any{}
	for _, c := range []struct {
		name  string
		codec sstable.Compression
	}{{"none", sstable.NoCompression}, {"snappy", sstable.SnappyCompression}, {"zstd", sstable.ZstdCompression}} {
		ab := map[string]any{}
		for _, candidate := range []bool{false, true} {
			name := "old"
			rows := source
			if candidate {
				name = "candidate"
				rows = packed
			}
			path := filepath.Join(out, c.name+"-"+name+".sst")
			meta, err := writeSST(path, rows, false, c.codec)
			if err != nil {
				return err
			}
			bench, err := bundlePointBench(path, source, candidate, false, ops, cache)
			if err != nil {
				return err
			}
			misses, err := bundlePointBench(path, source, candidate, true, ops, cache)
			if err != nil {
				return err
			}
			meta["point_reads"] = map[string]any{"positive": bench, "negative": misses}
			ab[name] = meta
		}
		cases[c.name] = ab
	}
	result["sst_cases"] = cases
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "results.json"), append(encoded, '\n'), 0644); err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}
