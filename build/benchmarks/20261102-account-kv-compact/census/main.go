// Read-only, merged-live-key census of a stopped-service maintenance copy.
// This opens no production path and never compacts, migrates or deletes keys.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type counts struct {
	Rows              uint64 `json:"rows"`
	PhysicalKeyBytes  uint64 `json:"physical_key_bytes"`
	WrappedValueBytes uint64 `json:"wrapped_value_bytes"`
	LogicalKeyBytes   uint64 `json:"logical_key_bytes"`
	LogicalValueBytes uint64 `json:"logical_value_bytes"`
}

func (c *counts) add(k, v, lk, lv []byte) {
	c.Rows++
	c.PhysicalKeyBytes += uint64(len(k))
	c.WrappedValueBytes += uint64(len(v))
	c.LogicalKeyBytes += uint64(len(lk))
	c.LogicalValueBytes += uint64(len(lv))
}

type classified struct {
	All      counts `json:"all"`
	Current  counts `json:"active_current"`
	Obsolete counts `json:"obsolete_generation"`
	Future   counts `json:"future_generation"`
	Missing  counts `json:"owner_missing"`
}

func (c *classified) add(status string, k, v, lk, lv []byte) {
	c.All.add(k, v, lk, lv)
	switch status {
	case "active_current":
		c.Current.add(k, v, lk, lv)
	case "obsolete_generation":
		c.Obsolete.add(k, v, lk, lv)
	case "future_generation":
		c.Future.add(k, v, lk, lv)
	case "owner_missing":
		c.Missing.add(k, v, lk, lv)
	default:
		panic("unknown classification")
	}
}

type domainStats struct {
	Domain            uint16            `json:"domain"`
	Name              string            `json:"name"`
	Counts            classified        `json:"counts"`
	Owners            uint64            `json:"owners_with_any_stored_rows"`
	CurrentOwners     uint64            `json:"owners_with_current_rows"`
	Fanout            map[string]uint64 `json:"rows_per_owner_histogram"`
	CurrentFanout     map[string]uint64 `json:"current_rows_per_owner_histogram"`
	WrappedLengths    map[string]uint64 `json:"wrapped_value_length_histogram"`
	LogicalLengths    map[string]uint64 `json:"logical_value_length_histogram"`
	LogicalKeyLengths map[string]uint64 `json:"logical_key_length_histogram"`
	PresentEmpty      uint64            `json:"present_empty_values"`
	Zero8             uint64            `json:"present_zero_8byte_words"`
	Zero32            uint64            `json:"present_zero_32byte_words"`
}

func histogram(n uint64) string {
	if n <= 64 {
		return fmt.Sprint(n)
	}
	p := bits.Len64(n - 1)
	return fmt.Sprintf("%d..%d", uint64(1)<<(p-1)+1, uint64(1)<<p)
}
func fanoutBucket(n uint64) string {
	if n <= 2 {
		return fmt.Sprint(n)
	}
	p := bits.Len64(n - 1)
	return fmt.Sprintf("%d..%d", uint64(1)<<(p-1)+1, uint64(1)<<p)
}

type exportFile struct {
	path        string
	f           *os.File
	buf         *bufio.Writer
	hash        hash.Hash
	Rows, Bytes uint64
	Min, Max    string
	Closed      bool
}

func newExport(path string) (*exportFile, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	return &exportFile{path: path, f: f, hash: h, buf: bufio.NewWriterSize(io.MultiWriter(f, h), 1<<20)}, nil
}
func (e *exportFile) write(k, v []byte) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(k)))
	if _, err := e.buf.Write(b[:]); err != nil {
		return err
	}
	if _, err := e.buf.Write(k); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(b[:], uint32(len(v)))
	if _, err := e.buf.Write(b[:]); err != nil {
		return err
	}
	if _, err := e.buf.Write(v); err != nil {
		return err
	}
	if e.Rows == 0 {
		e.Min = hex.EncodeToString(k)
	}
	e.Max = hex.EncodeToString(k)
	e.Rows++
	e.Bytes += uint64(8 + len(k) + len(v))
	return nil
}
func (e *exportFile) close() error {
	if e.Closed {
		return nil
	}
	e.Closed = true
	if err := e.buf.Flush(); err != nil {
		e.f.Close()
		return err
	}
	if err := e.f.Sync(); err != nil {
		e.f.Close()
		return err
	}
	return e.f.Close()
}
func (e *exportFile) metadata() map[string]any {
	status := "in_progress"
	digest := ""
	if e.Closed {
		status = "complete"
		digest = hex.EncodeToString(e.hash.Sum(nil))
	}
	return map[string]any{"path": e.path, "rows": e.Rows, "bytes": e.Bytes, "min_key_hex": e.Min, "max_key_hex": e.Max, "sha256": digest, "status": status, "row_format": "u32be key length; full physical key; u32be wrapped value length; full original wrapped value"}
}

type ownerContract struct {
	Owner          string     `json:"owner"`
	AccountPresent bool       `json:"account_present"`
	Generation     uint64     `json:"account_generation"`
	Counts         classified `json:"counts"`
}
type topOwners []ownerContract

func (t topOwners) Len() int { return len(t) }
func (t topOwners) Less(i, j int) bool {
	return t[i].Counts.All.LogicalValueBytes+t[i].Counts.All.LogicalKeyBytes < t[j].Counts.All.LogicalValueBytes+t[j].Counts.All.LogicalKeyBytes
}
func (t topOwners) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t *topOwners) Push(v any)   { *t = append(*t, v.(ownerContract)) }
func (t *topOwners) Pop() any     { s := *t; v := s[len(s)-1]; *t = s[:len(s)-1]; return v }

type cursor struct {
	it                  *pebble.Iterator
	kind                string
	bound               rawdb.PhysicalSpaceRange
	valid               bool
	owner               common.Address
	generation          uint64
	domain              kvdomains.KVDomain
	logical             []byte
	key, value          []byte
	rows, physicalBytes uint64
}

func newCursor(db *pebble.DB, name string) (*cursor, error) {
	var bound rawdb.PhysicalSpaceRange
	for _, r := range rawdb.DiskSpaceObservationRanges() {
		if r.Name == name {
			bound = r
			break
		}
	}
	if len(bound.Start) == 0 {
		return nil, fmt.Errorf("missing schema range %s", name)
	}
	it, e := db.NewIter(&pebble.IterOptions{LowerBound: bound.Start, UpperBound: bound.End})
	if e != nil {
		return nil, e
	}
	c := &cursor{it: it, kind: name, bound: bound}
	c.valid = it.First()
	return c, c.load()
}
func (c *cursor) load() error {
	for c.valid {
		c.key, c.value = c.it.Key(), c.it.Value()
		switch c.kind {
		case "account_kv_latest":
			if len(c.key) == common.HashLength {
				c.valid = c.it.Next()
				continue
			}
			owner, generation, domain, logical, ok := rawdb.DecodeStateKVLatestKey(c.key)
			if !ok {
				return fmt.Errorf("bad KV key %x", c.key)
			}
			c.owner, c.generation, c.domain, c.logical = owner, generation, domain, logical
		case "kv_generation":
			owner, ok := rawdb.DecodeStateKVGenerationKey(c.key)
			if !ok {
				return fmt.Errorf("bad generation key %x", c.key)
			}
			generation, e := rawdb.DecodeStateKVGenerationValue(c.value)
			if e != nil {
				return e
			}
			c.owner, c.generation = owner, generation
		case "account_latest":
			suffix := c.key[len(c.bound.Start):]
			if len(suffix) != common.AccountIDLength {
				return fmt.Errorf("bad account key %x", c.key)
			}
			var id common.AccountID
			copy(id[:], suffix)
			c.owner = id.Address(common.AddressPrefixMainnet)
		default:
			return fmt.Errorf("unknown cursor")
		}
		return nil
	}
	return c.it.Error()
}
func (c *cursor) next() error {
	c.rows++
	c.physicalBytes += uint64(len(c.key) + len(c.value))
	c.valid = c.it.Next()
	return c.load()
}
func (c *cursor) at(owner common.Address) bool { return c.valid && c.owner == owner }
func minimumOwner(cs ...*cursor) common.Address {
	var owner common.Address
	found := false
	for _, c := range cs {
		if c.valid && (!found || bytes.Compare(c.owner[:], owner[:]) < 0) {
			owner = c.owner
			found = true
		}
	}
	return owner
}
func atomicJSON(path string, value any) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	p := path + ".tmp"
	if e = os.WriteFile(p, append(b, '\n'), 0644); e != nil {
		return e
	}
	return os.Rename(p, path)
}
func allowedSource(path string) (string, error) {
	p, e := filepath.EvalSymlinks(path)
	if e != nil {
		return "", e
	}
	p, e = filepath.Abs(p)
	if e != nil {
		return "", e
	}
	if !strings.Contains(filepath.ToSlash(p), "/maintenance/") || filepath.Base(p) != "chaindata" || strings.Contains(filepath.ToSlash(p), "/main/datadir/") {
		return "", fmt.Errorf("only an explicit maintenance copy ending /chaindata is allowed: %s", p)
	}
	return p, nil
}
func zero(v []byte) bool {
	for _, b := range v {
		if b != 0 {
			return false
		}
	}
	return true
}

func priorityReward(db *pebble.DB, out string, height uint64, rateMiB int) (map[string]any, error) {
	started := time.Now()
	owner := common.SystemAccountAddress
	file, err := newExport(filepath.Join(out, "SystemReward-system-owner-current.bin"))
	if err != nil {
		return nil, err
	}
	defer file.close()
	value, closer, err := db.Get(rawdb.StateAccountLatestCommitmentKey(owner))
	accountPresent := err == nil
	var generation uint64
	if err != nil && err != pebble.ErrNotFound {
		return nil, err
	}
	if accountPresent {
		envelope, e := state.DecodeStateAccountV3(value)
		closer.Close()
		if e != nil {
			return nil, e
		}
		generation = envelope.AccountKVGeneration
	}
	metadata := map[string]any{"scope": "priority current SystemReward rows for canonical system account only; full-domain coverage must be compared against later complete census", "owner": owner.Hex(), "account_present": accountPresent, "generation": generation, "height_label": height, "started_utc": started.UTC().Format(time.RFC3339Nano)}
	if accountPresent {
		prefix := rawdb.StateKVLatestCommitmentKey(owner, generation, kvdomains.SystemReward, nil)
		var upper []byte
		for _, r := range rawdb.DiskSpaceObservationRanges() {
			if r.Name == "account_kv_latest" {
				upper = r.End
			}
		}
		it, e := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
		if e != nil {
			return nil, e
		}
		defer it.Close()
		lastProgress := time.Time{}
		for ok := it.First(); ok && bytes.HasPrefix(it.Key(), prefix); ok = it.Next() {
			k, v := it.Key(), it.Value()
			o, g, domain, _, valid := rawdb.DecodeStateKVLatestKey(k)
			if !valid || o != owner || g != generation || domain != kvdomains.SystemReward {
				return nil, fmt.Errorf("priority reward boundary mismatch")
			}
			if e = rawdb.ValidateStateKVLatestValue(v); e != nil {
				return nil, e
			}
			if e = file.write(k, v); e != nil {
				return nil, e
			}
			if file.Rows%4096 == 0 {
				if rateMiB > 0 {
					target := time.Duration(float64(file.Bytes) / float64(uint64(rateMiB)<<20) * float64(time.Second))
					if delay := target - time.Since(started); delay > 0 {
						time.Sleep(delay)
					}
				}
				now := time.Now()
				if now.Sub(lastProgress) >= 5*time.Second {
					lastProgress = now
					if e = atomicJSON(filepath.Join(out, "progress.json"), map[string]any{"phase": "priority_system_reward_export", "at_utc": now.UTC().Format(time.RFC3339Nano), "rows": file.Rows, "bytes": file.Bytes, "owner": owner.Hex(), "generation": generation}); e != nil {
						return nil, e
					}
				}
			}
		}
		if e = it.Error(); e != nil {
			return nil, e
		}
	}
	if err = file.close(); err != nil {
		return nil, err
	}
	metadata["export"] = file.metadata()
	metadata["complete"] = true
	metadata["finished_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	if err = atomicJSON(filepath.Join(out, "priority-system-reward-complete.json"), metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func run(source, out string, height uint64, rateMiB int) error {
	return runMode(source, out, height, rateMiB, false)
}
func runMode(source, out string, height uint64, rateMiB int, priorityOnly bool) (err error) {
	resolved, err := allowedSource(source)
	if err != nil {
		return err
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}
	if out == resolved || strings.HasPrefix(out, resolved+string(os.PathSeparator)) {
		return fmt.Errorf("output must be outside source")
	}
	if err = os.Mkdir(out, 0755); err != nil {
		return err
	}
	started := time.Now()
	defer func() {
		if err != nil {
			_ = atomicJSON(filepath.Join(out, "error.json"), map[string]any{"error": err.Error(), "at_utc": time.Now().UTC().Format(time.RFC3339Nano)})
		}
	}()
	cache := pebble.NewCache(128 << 20)
	defer cache.Unref()
	db, err := pebble.Open(resolved, &pebble.Options{ReadOnly: true, Cache: cache, MaxOpenFiles: 256, DisableAutomaticCompactions: true})
	if err != nil {
		return err
	}
	defer db.Close()
	priority, err := priorityReward(db, out, height, rateMiB)
	if err != nil {
		return err
	}
	if priorityOnly {
		return atomicJSON(filepath.Join(out, "complete.json"), map[string]any{"complete": true, "full_census_completed": false, "scope": "priority system owner/gen reward export only", "priority_system_reward": priority})
	}
	kv, err := newCursor(db, "account_kv_latest")
	if err != nil {
		return err
	}
	defer kv.it.Close()
	accounts, err := newCursor(db, "account_latest")
	if err != nil {
		return err
	}
	defer accounts.it.Close()
	generations, err := newCursor(db, "kv_generation")
	if err != nil {
		return err
	}
	defer generations.it.Close()
	exports := map[kvdomains.KVDomain]*exportFile{}
	exportMetadata := func() map[string]any {
		m := map[string]any{}
		for domain, e := range exports {
			m[kvdomains.Name(domain)] = e.metadata()
		}
		return m
	}
	for _, domain := range []kvdomains.KVDomain{kvdomains.ContractStorage, kvdomains.SystemReward, kvdomains.AccountResourceAux, kvdomains.AccountAssetV2} {
		e, err := newExport(filepath.Join(out, kvdomains.Name(domain)+".bin"))
		if err != nil {
			return err
		}
		exports[domain] = e
		defer e.close()
	}
	genExport, err := newExport(filepath.Join(out, "kv_generation.bin"))
	if err != nil {
		return err
	}
	defer genExport.close()
	contracts, err := os.OpenFile(filepath.Join(out, "contract-owners.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer contracts.Close()
	contractBuffer := bufio.NewWriterSize(contracts, 1<<20)
	contractJSON := json.NewEncoder(contractBuffer)
	fanoutFile, err := os.OpenFile(filepath.Join(out, "owner-domain-fanout.jsonl.gz"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fanoutFile.Close()
	fanoutGzip, err := gzip.NewWriterLevel(fanoutFile, gzip.BestSpeed)
	if err != nil {
		return err
	}
	defer fanoutGzip.Close()
	fanoutBuffer := bufio.NewWriterSize(fanoutGzip, 1<<20)
	fanoutJSON := json.NewEncoder(fanoutBuffer)
	domains := map[kvdomains.KVDomain]*domainStats{}
	ownerCounts := map[kvdomains.KVDomain]*classified{}
	pool := make([]classified, 128)
	top := &topOwners{}
	heap.Init(top)
	var ownersWithKV, allOwners, genMismatch, nonzeroAccountMissingGen, zeroAccountMissingGen, genWithoutAccount, kvWithoutAccount uint64
	var mismatchExamples []map[string]any
	lastProgress := time.Time{}
	processedBytes := uint64(0)
	processedRows := uint64(0)
	progress := func(force bool) error {
		now := time.Now()
		if !force && now.Sub(lastProgress) < 5*time.Second {
			return nil
		}
		lastProgress = now
		return atomicJSON(filepath.Join(out, "progress.json"), map[string]any{"started_utc": started.UTC().Format(time.RFC3339Nano), "at_utc": now.UTC().Format(time.RFC3339Nano), "elapsed_seconds": now.Sub(started).Seconds(), "phase": "scanning_merged_live_keys", "height_label": height, "kv_rows": kv.rows, "account_rows": accounts.rows, "generation_rows": generations.rows, "owner_groups": allOwners, "physical_key_value_bytes_processed": processedBytes, "logical_rate_limit_mib_sec": rateMiB, "exports": exportMetadata()})
	}
	throttle := func() error {
		if processedRows%4096 != 0 {
			return nil
		}
		if rateMiB > 0 {
			target := time.Duration(float64(processedBytes) / float64(uint64(rateMiB)<<20) * float64(time.Second))
			if delay := target - time.Since(started); delay > 0 {
				time.Sleep(delay)
			}
		}
		return progress(false)
	}
	if err = progress(true); err != nil {
		return err
	}
	for kv.valid || accounts.valid || generations.valid {
		owner := minimumOwner(kv, accounts, generations)
		allOwners++
		accountPresent := accounts.at(owner)
		generationPresent := generations.at(owner)
		var activeGeneration, watermark uint64
		if accountPresent {
			envelope, e := state.DecodeStateAccountV3(accounts.value)
			if e != nil {
				return fmt.Errorf("account %s: %w", owner.Hex(), e)
			}
			activeGeneration = envelope.AccountKVGeneration
			processedBytes += uint64(len(accounts.key) + len(accounts.value))
			processedRows++
			if err = accounts.next(); err != nil {
				return err
			}
		}
		if generationPresent {
			watermark = generations.generation
			if err = genExport.write(generations.key, generations.value); err != nil {
				return err
			}
			processedBytes += uint64(len(generations.key) + len(generations.value))
			processedRows++
			if err = generations.next(); err != nil {
				return err
			}
		}
		if accountPresent && generationPresent && activeGeneration != watermark {
			genMismatch++
			if len(mismatchExamples) < 100 {
				mismatchExamples = append(mismatchExamples, map[string]any{"owner": owner.Hex(), "account_generation": activeGeneration, "generation_table": watermark})
			}
		}
		if accountPresent && !generationPresent {
			if activeGeneration == 0 {
				zeroAccountMissingGen++
			} else {
				nonzeroAccountMissingGen++
				if len(mismatchExamples) < 100 {
					mismatchExamples = append(mismatchExamples, map[string]any{"owner": owner.Hex(), "account_generation": activeGeneration, "generation_table": "missing"})
				}
			}
		}
		if !accountPresent && generationPresent {
			genWithoutAccount++
		}
		for domain := range ownerCounts {
			delete(ownerCounts, domain)
		}
		used := 0
		hadKV := false
		for kv.at(owner) {
			hadKV = true
			logicalValue, e := rawdb.DecodeStateKVLatestValueNoCopy(kv.value)
			if e != nil {
				return fmt.Errorf("KV %x: %w", kv.key, e)
			}
			status := "active_current"
			if !accountPresent {
				status = "owner_missing"
			} else if kv.generation < activeGeneration {
				status = "obsolete_generation"
			} else if kv.generation > activeGeneration {
				status = "future_generation"
			}
			d := domains[kv.domain]
			if d == nil {
				d = &domainStats{Domain: uint16(kv.domain), Name: kvdomains.Name(kv.domain), Fanout: map[string]uint64{}, CurrentFanout: map[string]uint64{}, WrappedLengths: map[string]uint64{}, LogicalLengths: map[string]uint64{}, LogicalKeyLengths: map[string]uint64{}}
				domains[kv.domain] = d
			}
			d.Counts.add(status, kv.key, kv.value, kv.logical, logicalValue)
			d.WrappedLengths[histogram(uint64(len(kv.value)))]++
			d.LogicalLengths[histogram(uint64(len(logicalValue)))]++
			d.LogicalKeyLengths[histogram(uint64(len(kv.logical)))]++
			if len(logicalValue) == 0 {
				d.PresentEmpty++
			} else if len(logicalValue) == 8 && zero(logicalValue) {
				d.Zero8++
			} else if len(logicalValue) == 32 && zero(logicalValue) {
				d.Zero32++
			}
			oc := ownerCounts[kv.domain]
			if oc == nil {
				if used == len(pool) {
					return fmt.Errorf("owner has more registered domains than bounded pool")
				}
				oc = &pool[used]
				*oc = classified{}
				used++
				ownerCounts[kv.domain] = oc
			}
			oc.add(status, kv.key, kv.value, kv.logical, logicalValue)
			if export := exports[kv.domain]; export != nil {
				if err = export.write(kv.key, kv.value); err != nil {
					return err
				}
			}
			processedBytes += uint64(len(kv.key) + len(kv.value))
			processedRows++
			if err = kv.next(); err != nil {
				return err
			}
			if err = throttle(); err != nil {
				return err
			}
		}
		if hadKV {
			ownersWithKV++
			if !accountPresent {
				kvWithoutAccount++
			}
			ownerHex := owner.Hex()
			for domain, oc := range ownerCounts {
				d := domains[domain]
				d.Owners++
				d.Fanout[fanoutBucket(oc.All.Rows)]++
				if oc.Current.Rows > 0 {
					d.CurrentOwners++
					d.CurrentFanout[fanoutBucket(oc.Current.Rows)]++
				}
				if err = fanoutJSON.Encode([]any{ownerHex, uint16(domain), oc.All.Rows, oc.Current.Rows, oc.Obsolete.Rows, oc.Future.Rows, oc.Missing.Rows}); err != nil {
					return err
				}
			}
			if oc := ownerCounts[kvdomains.ContractStorage]; oc != nil {
				v := ownerContract{Owner: ownerHex, AccountPresent: accountPresent, Generation: activeGeneration, Counts: *oc}
				if err = contractJSON.Encode(v); err != nil {
					return err
				}
				heap.Push(top, v)
				if top.Len() > 100 {
					heap.Pop(top)
				}
			}
		}
		if err = throttle(); err != nil {
			return err
		}
	}
	if err = progress(true); err != nil {
		return err
	}
	for _, e := range exports {
		if err = e.close(); err != nil {
			return err
		}
	}
	if err = genExport.close(); err != nil {
		return err
	}
	if err = contractBuffer.Flush(); err != nil {
		return err
	}
	if err = contracts.Sync(); err != nil {
		return err
	}
	if err = fanoutBuffer.Flush(); err != nil {
		return err
	}
	if err = fanoutGzip.Close(); err != nil {
		return err
	}
	if err = fanoutFile.Sync(); err != nil {
		return err
	}
	ordered := make([]*domainStats, 0, len(domains))
	for _, d := range domains {
		ordered = append(ordered, d)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Domain < ordered[j].Domain })
	sort.Sort(sort.Reverse(top))
	result := map[string]any{"complete": true, "source": resolved, "height_label": height, "started_utc": started.UTC().Format(time.RFC3339Nano), "finished_utc": time.Now().UTC().Format(time.RFC3339Nano), "scope": "all Pebble merged visible KV keys in immutable copy, including obsolete semantic generations; no SST old versions/tombstones; physical key/value and restored logical byte counts are not compressed SST allocation", "generation_authority": "present account_latest envelope AccountKVGeneration is authoritative for StateDB.GetAccountKV; kv_generation is a recreation/deletion watermark checked separately. Missing generation table with account generation zero is the implicit-zero case; absent accounts have no active KV state and are never classified current.", "domains": ordered, "kv_rows": kv.rows, "account_rows": accounts.rows, "generation_rows": generations.rows, "owner_groups": allOwners, "owners_with_kv": ownersWithKV, "kv_owners_without_account": kvWithoutAccount, "generation_rows_without_account": genWithoutAccount, "account_generation_table_mismatches": genMismatch, "nonzero_account_generation_without_table_row": nonzeroAccountMissingGen, "zero_account_generation_without_table_row": zeroAccountMissingGen, "generation_mismatch_examples": mismatchExamples, "exports": exportMetadata(), "generation_export": genExport.metadata(), "top_contract_owners_by_logical_key_plus_value_bytes": top, "owner_fanout_file": "owner-domain-fanout.jsonl.gz", "owner_fanout_array_fields": []string{"owner", "domain", "all_rows", "active_current_rows", "obsolete_generation_rows", "future_generation_rows", "owner_missing_rows"}, "all_contract_owner_file": "contract-owners.jsonl", "logical_rate_limit_mib_sec": rateMiB, "gomaxprocs": runtime.GOMAXPROCS(0), "pebble_cache_bytes": 128 << 20}
	result["priority_system_reward"] = priority
	result["full_census_completed"] = true
	if err = atomicJSON(filepath.Join(out, "summary.json"), result); err != nil {
		return err
	}
	if err = atomicJSON(filepath.Join(out, "complete.json"), map[string]any{"complete": true, "finished_utc": result["finished_utc"], "kv_rows": kv.rows, "account_rows": accounts.rows, "generation_rows": generations.rows, "exports": exportMetadata(), "generation_export": genExport.metadata()}); err != nil {
		return err
	}
	return atomicJSON(filepath.Join(out, "progress.json"), map[string]any{"phase": "complete", "complete": true, "finished_utc": result["finished_utc"], "kv_rows": kv.rows, "account_rows": accounts.rows, "generation_rows": generations.rows, "exports": exportMetadata()})
}
func main() {
	priorityOnly := flag.Bool("priority-only", false, "export only current canonical system-owner rewards and exit; full census is not complete")
	source := flag.String("source", "", "immutable maintenance copy ending /chaindata")
	out := flag.String("out", "", "new output directory, outside database")
	height := flag.Uint64("height", 0, "copy height label supplied by collector")
	rate := flag.Int("logical-mib-sec", 128, "maximum average input key/value MiB per second (zero disables)")
	procs := flag.Int("gomaxprocs", 2, "CPU concurrency bound")
	flag.Parse()
	runtime.GOMAXPROCS(*procs)
	if *source == "" || *out == "" || *rate < 0 || *procs < 1 {
		panic("source/out and valid limits required")
	}
	if err := runMode(*source, *out, *height, *rate, *priorityOnly); err != nil {
		panic(err)
	}
	fmt.Println("KV_CENSUS_COMPLETE")
}
