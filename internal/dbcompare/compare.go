// Package dbcompare compares a stopped gtron Pebble database with a stopped
// java-tron LevelDB lite database at one exact block height.
package dbcompare

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ethereum/go-ethereum/ethdb"
	ethleveldb "github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/google/go-cmp/cmp"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

const maxDetailLen = 4096

const defaultProgressInterval = 5 * time.Second

// ProgressEvent reports comparison lifecycle and long-running scan progress.
// Callbacks are synchronous and must return quickly. Snapshot is an immutable
// point-in-time report suitable for refreshing a live JSON output file.
type ProgressEvent struct {
	Phase    string
	Store    string
	Rows     uint64
	Elapsed  time.Duration
	Detail   string
	Result   StoreResult
	Snapshot *Report
}

// Options controls comparison scope and retained diagnostic output.
type Options struct {
	Height           uint64
	MaxDifferences   int
	ReverseAccounts  bool
	Workers          int
	Progress         func(ProgressEvent)
	ProgressInterval time.Duration
}

// Difference is one retained mismatch. Counts in StoreResult are authoritative;
// the list is capped by Options.MaxDifferences to keep large audits usable.
type Difference struct {
	Store  string `json:"store"`
	Key    string `json:"key"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type StoreResult struct {
	Name         string `json:"name"`
	Scope        string `json:"scope"`
	Compared     uint64 `json:"compared"`
	Equal        uint64 `json:"equal"`
	Different    uint64 `json:"different"`
	MissingGtron uint64 `json:"missing_gtron"`
	MissingJava  uint64 `json:"missing_java"`
	Invalid      uint64 `json:"invalid"`
	Skipped      uint64 `json:"skipped"`
	Present      bool   `json:"present"`
}

func (s StoreResult) Mismatches() uint64 {
	return s.Different + s.MissingGtron + s.MissingJava + s.Invalid
}

type Report struct {
	Height                 uint64          `json:"height"`
	GtronHead              uint64          `json:"gtron_head"`
	JavaHead               uint64          `json:"java_head"`
	StateCoverageComplete  bool            `json:"state_coverage_complete"`
	UnsupportedStateStores []string        `json:"unsupported_state_stores,omitempty"`
	UnclassifiedStores     []string        `json:"unclassified_stores,omitempty"`
	ExcludedStores         []string        `json:"excluded_non_state_stores,omitempty"`
	Stores                 []StoreResult   `json:"stores"`
	Differences            []Difference    `json:"differences,omitempty"`
	Progress               *ReportProgress `json:"progress,omitempty"`
}

// ReportProgress describes the point-in-time state of an in-progress JSON
// snapshot. It is omitted from the final report.
type ReportProgress struct {
	Phase         string       `json:"phase"`
	Store         string       `json:"store,omitempty"`
	Stage         string       `json:"stage,omitempty"`
	Rows          uint64       `json:"rows"`
	ElapsedMillis int64        `json:"elapsed_ms"`
	CurrentResult *StoreResult `json:"current_result,omitempty"`
	Mismatches    uint64       `json:"mismatches"`
}

func (r *Report) Mismatches() uint64 {
	var total uint64
	for _, store := range r.Stores {
		total += store.Mismatches()
	}
	return total
}

type JavaStores struct {
	Root       string
	Account    ethdb.KeyValueStore
	Witness    ethdb.KeyValueStore
	Contract   ethdb.KeyValueStore
	ABI        ethdb.KeyValueStore
	Code       ethdb.KeyValueStore
	Properties ethdb.KeyValueStore
	BlockIndex ethdb.KeyValueStore
	Block      ethdb.KeyValueStore
	stores     map[string]ethdb.KeyValueStore
	discovered []string
	all        []ethdb.KeyValueStore
}

func (j *JavaStores) Store(name string) ethdb.KeyValueStore {
	return j.stores[name]
}

func (j *JavaStores) Discovered() []string {
	return append([]string(nil), j.discovered...)
}

// ResolveJavaDatabaseDir accepts either output-directory or its database child.
func ResolveJavaDatabaseDir(path string) string {
	if info, err := os.Stat(filepath.Join(path, "database")); err == nil && info.IsDir() {
		return filepath.Join(path, "database")
	}
	return path
}

// ResolveGtronChainDataDir accepts either --datadir or the chaindata directory.
func ResolveGtronChainDataDir(path string) string {
	if info, err := os.Stat(filepath.Join(path, "gtron", "chaindata")); err == nil && info.IsDir() {
		return filepath.Join(path, "gtron", "chaindata")
	}
	return path
}

// OpenJavaStores opens java-tron's standard LevelDB stores read-only. Nodes
// must be stopped first because both LevelDB and Pebble hold exclusive locks.
func OpenJavaStores(path string) (*JavaStores, error) {
	root := ResolveJavaDatabaseDir(path)
	j := &JavaStores{Root: root, stores: make(map[string]ethdb.KeyValueStore)}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read java-tron database directory %s: %w", root, err)
	}
	actualNames := make(map[string]string)
	for _, entry := range entries {
		if !entry.IsDir() || !looksLikeLevelDB(filepath.Join(root, entry.Name())) {
			continue
		}
		actualNames[strings.ToLower(entry.Name())] = entry.Name()
		j.discovered = append(j.discovered, entry.Name())
	}
	sort.Strings(j.discovered)
	open := func(name string, required bool) (ethdb.KeyValueStore, error) {
		actual, present := actualNames[strings.ToLower(name)]
		if !present {
			if required {
				return nil, fmt.Errorf("required java-tron store %q not found under %s", name, root)
			}
			return nil, nil
		}
		storePath := filepath.Join(root, actual)
		db, err := ethleveldb.New(storePath, 16, 16, "db-compare-"+name, true)
		if err != nil {
			return nil, fmt.Errorf("open java-tron store %q (only LevelDB is supported): %w", name, err)
		}
		j.all = append(j.all, db)
		j.stores[name] = db
		return db, nil
	}
	for _, spec := range javaStoreSpecs {
		if _, err = open(spec.Name, spec.Required); err != nil {
			j.Close()
			return nil, err
		}
	}
	// Keep the named handles used by the core comparison path.
	j.Account = j.Store("account")
	j.Witness = j.Store("witness")
	j.Contract = j.Store("contract")
	j.Properties = j.Store("properties")
	j.BlockIndex = j.Store("block-index")
	j.Block = j.Store("block")
	j.ABI = j.Store("abi")
	j.Code = j.Store("code")
	return j, nil
}

func looksLikeLevelDB(path string) bool {
	if info, err := os.Stat(filepath.Join(path, "CURRENT")); err == nil && !info.IsDir() {
		return true
	}
	matches, _ := filepath.Glob(filepath.Join(path, "MANIFEST-*"))
	return len(matches) != 0
}

func (j *JavaStores) Close() error {
	var errs []error
	for i := len(j.all) - 1; i >= 0; i-- {
		if err := j.all[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}
	j.all = nil
	return errors.Join(errs...)
}

type comparer struct {
	opts   Options
	report *Report
}

type progressCounter struct {
	c       *comparer
	result  *StoreResult
	store   string
	detail  string
	started time.Time
	last    time.Time
	rows    uint64
}

func (c *comparer) emitProgress(event ProgressEvent) {
	if c.opts.Progress != nil {
		event.Snapshot = c.progressSnapshot(event)
		c.opts.Progress(event)
	}
}

func (c *comparer) progressSnapshot(event ProgressEvent) *Report {
	snapshot := *c.report
	snapshot.UnsupportedStateStores = append([]string(nil), c.report.UnsupportedStateStores...)
	snapshot.UnclassifiedStores = append([]string(nil), c.report.UnclassifiedStores...)
	snapshot.ExcludedStores = append([]string(nil), c.report.ExcludedStores...)
	snapshot.Stores = append([]StoreResult(nil), c.report.Stores...)
	snapshot.Differences = append([]Difference(nil), c.report.Differences...)
	if event.Result.Name != "" && event.Phase != "done" {
		snapshot.Stores = append(snapshot.Stores, event.Result)
	}
	snapshot.Progress = &ReportProgress{
		Phase: event.Phase, Store: event.Store, Stage: event.Detail, Rows: event.Rows,
		ElapsedMillis: event.Elapsed.Milliseconds(),
	}
	if event.Result.Name != "" {
		current := event.Result
		snapshot.Progress.CurrentResult = &current
	}
	snapshot.Progress.Mismatches = snapshot.Mismatches()
	snapshot.Sort()
	return &snapshot
}

func (c *comparer) beginStore(result StoreResult) time.Time {
	started := time.Now()
	if !result.Present {
		c.emitProgress(ProgressEvent{Phase: "skip", Store: result.Name, Detail: "java store absent", Result: result})
		return started
	}
	c.emitProgress(ProgressEvent{Phase: "start", Store: result.Name, Result: result})
	return started
}

func (c *comparer) finishStore(result StoreResult, started time.Time) {
	if !result.Present {
		return
	}
	rows := result.Compared + result.MissingGtron + result.Invalid + result.Skipped
	c.emitProgress(ProgressEvent{
		Phase: "done", Store: result.Name, Rows: rows, Elapsed: time.Since(started), Result: result,
	})
}

func (c *comparer) trackStore(result *StoreResult) func() {
	started := c.beginStore(*result)
	return func() {
		c.report.Stores = append(c.report.Stores, *result)
		c.finishStore(*result, started)
	}
}

func (c *comparer) newProgressCounter(result *StoreResult, detail string) *progressCounter {
	now := time.Now()
	return &progressCounter{c: c, result: result, store: result.Name, detail: detail, started: now, last: now}
}

func (p *progressCounter) Add(delta uint64) {
	if p.c.opts.Progress == nil {
		p.rows += delta
		return
	}
	interval := p.c.opts.ProgressInterval
	if interval <= 0 {
		interval = defaultProgressInterval
	}
	now := time.Now()
	if p.rows == 0 || now.Sub(p.last) < interval {
		p.rows += delta
		return
	}
	p.last = now
	p.c.emitProgress(ProgressEvent{
		Phase: "progress", Store: p.store, Rows: p.rows, Elapsed: now.Sub(p.started), Detail: p.detail, Result: *p.result,
	})
	p.rows += delta
}

func Compare(gtron ethdb.KeyValueStore, java *JavaStores, opts Options) (*Report, error) {
	if opts.MaxDifferences <= 0 {
		opts.MaxDifferences = 100
	}
	c := &comparer{opts: opts, report: &Report{Height: opts.Height}}
	c.auditJavaStoreCoverage(java)

	gtronHead, err := gtronHead(gtron)
	if err != nil {
		return nil, err
	}
	javaHead, err := javaHead(java.Properties)
	if err != nil {
		return nil, err
	}
	c.report.GtronHead, c.report.JavaHead = gtronHead, javaHead
	c.emitProgress(ProgressEvent{
		Phase: "info", Detail: fmt.Sprintf("height guard requested=%d gtron=%d java=%d", opts.Height, gtronHead, javaHead),
	})
	if gtronHead != opts.Height || javaHead != opts.Height {
		return nil, fmt.Errorf("height guard failed: requested=%d gtron_head=%d java_head=%d", opts.Height, gtronHead, javaHead)
	}

	disk := rawdb.WrapKeyValueStore(gtron)
	stateDB := state.NewDatabase(disk)
	statedb, err := state.New(tcommon.Hash{}, stateDB)
	if err != nil {
		return nil, fmt.Errorf("open gtron head state: %w", err)
	}
	dp := state.LoadDynamicProperties(gtron, statedb)

	if err := c.compareBlock(gtron, java); err != nil {
		return nil, err
	}
	if err := c.compareProperties(statedb, dp, java.Properties); err != nil {
		return nil, err
	}
	if err := c.compareAccounts(gtron, statedb, java.Account); err != nil {
		return nil, err
	}
	if err := c.compareWitnesses(statedb, java.Witness); err != nil {
		return nil, err
	}
	if err := c.compareContracts(gtron, java.Contract); err != nil {
		return nil, err
	}
	if err := c.compareABIs(statedb, java.ABI); err != nil {
		return nil, err
	}
	if err := c.compareCode(statedb, java.Code); err != nil {
		return nil, err
	}
	if err := c.compareAdditionalStateStores(gtron, statedb, java); err != nil {
		return nil, err
	}
	if err := c.reportUnsupportedStateStores(java); err != nil {
		return nil, err
	}
	c.finalizeStateCoverage(java)
	return c.report, nil
}

func (c *comparer) reportUnsupportedStateStores(java *JavaStores) error {
	for _, spec := range javaStoreSpecs {
		if !spec.State || spec.Compare {
			continue
		}
		db := java.Store(spec.Name)
		result := StoreResult{Name: spec.Name, Scope: spec.Scope, Present: db != nil}
		started := c.beginStore(result)
		if db != nil {
			progress := c.newProgressCounter(&result, "counting unsupported java rows")
			it := db.NewIterator(nil, nil)
			for it.Next() {
				progress.Add(1)
				result.Skipped++
			}
			err := it.Error()
			it.Release()
			if err != nil {
				return fmt.Errorf("enumerate unsupported java-tron state store %q: %w", spec.Name, err)
			}
		}
		c.report.Stores = append(c.report.Stores, result)
		c.finishStore(result, started)
	}
	return nil
}

func (c *comparer) auditJavaStoreCoverage(java *JavaStores) {
	for _, discovered := range java.Discovered() {
		spec, ok := javaStoreSpecByName(discovered)
		if !ok {
			c.report.UnclassifiedStores = append(c.report.UnclassifiedStores, discovered)
			continue
		}
		if spec.State && !spec.Compare {
			c.report.UnsupportedStateStores = append(c.report.UnsupportedStateStores, spec.Name)
		}
		if !spec.State {
			c.report.ExcludedStores = append(c.report.ExcludedStores, spec.Name+" ("+spec.Scope+")")
		}
	}
}

func (c *comparer) finalizeStateCoverage(java *JavaStores) {
	reported := make(map[string]bool)
	for _, result := range c.report.Stores {
		reported[result.Name] = true
	}
	for _, spec := range javaStoreSpecs {
		if !spec.State || !spec.Compare || java.Store(spec.Name) == nil {
			continue
		}
		if !reported[spec.Name] {
			c.report.UnsupportedStateStores = append(c.report.UnsupportedStateStores, spec.Name+" (adapter not run)")
		}
	}
	sort.Strings(c.report.UnsupportedStateStores)
	sort.Strings(c.report.UnclassifiedStores)
	sort.Strings(c.report.ExcludedStores)
	c.report.StateCoverageComplete = len(c.report.UnsupportedStateStores) == 0 && len(c.report.UnclassifiedStores) == 0
}

func gtronHead(db ethdb.KeyValueStore) (uint64, error) {
	hash := rawdb.ReadHeadBlockHash(db)
	if hash == (tcommon.Hash{}) {
		return 0, errors.New("gtron head hash is missing")
	}
	num := rawdb.ReadBlockNumber(rawdb.NewChainDB(db, rawdb.NoopAncient{}), hash)
	if num == nil {
		return 0, fmt.Errorf("gtron head number missing for hash %x", hash)
	}
	return *num, nil
}

func javaHead(db ethdb.KeyValueStore) (uint64, error) {
	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		if normalizePropertyKey(it.Key()) != "latest_block_header_number" {
			continue
		}
		if len(it.Value()) != 8 {
			return 0, fmt.Errorf("java latest_block_header_number has %d bytes, want 8", len(it.Value()))
		}
		return binary.BigEndian.Uint64(it.Value()), nil
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return 0, errors.New("java properties/latest_block_header_number is missing")
}

func (c *comparer) compareBlock(gtron ethdb.KeyValueStore, java *JavaStores) error {
	r := StoreResult{Name: "block", Scope: "chain", Present: true}
	defer c.trackStore(&r)()
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], c.opts.Height)
	javaID, err := java.BlockIndex.Get(num[:])
	if err != nil {
		r.MissingJava++
		c.addDiff("block", fmt.Sprint(c.opts.Height), "missing_java", "block-index row missing")
		return nil
	}
	javaRaw, err := java.Block.Get(javaID)
	if err != nil {
		r.MissingJava++
		c.addDiff("block", fmt.Sprint(c.opts.Height), "missing_java", "block row missing for block-index id "+hex.EncodeToString(javaID))
		return nil
	}
	gblock := rawdb.ReadBlock(rawdb.NewChainDB(gtron, rawdb.NoopAncient{}), c.opts.Height)
	if gblock == nil {
		r.MissingGtron++
		c.addDiff("block", fmt.Sprint(c.opts.Height), "missing_gtron", "canonical block row missing")
		return nil
	}
	var jblock corepb.Block
	if err := proto.Unmarshal(javaRaw, &jblock); err != nil {
		r.Invalid++
		c.addDiff("block", fmt.Sprint(c.opts.Height), "invalid_java", err.Error())
		return nil
	}
	r.Compared++
	if proto.Equal(gblock.Proto(), &jblock) && bytes.Equal(gblock.Hash().Bytes(), javaID) {
		r.Equal++
		return nil
	}
	r.Different++
	c.addProtoDiff("block", fmt.Sprint(c.opts.Height), &jblock, gblock.Proto())
	return nil
}

func (c *comparer) compareProperties(sdb *state.StateDB, dp *state.DynamicProperties, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "properties", Scope: "state", Present: true}
	defer c.trackStore(&r)()
	javaKeys := make(map[string][]byte)
	forkController := forks.NewForkControllerFromState(sdb)
	progress := c.newProgressCounter(&r, "comparing java properties")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		name := normalizePropertyKey(it.Key())
		value := append([]byte(nil), it.Value()...)
		javaKeys[name] = value
		var want []byte
		switch {
		case name == "latest_block_header_hash":
			want = dp.LatestBlockHeaderHash().Bytes()
		case strings.HasPrefix(name, "fork_version_"):
			version, err := strconv.ParseInt(strings.TrimPrefix(name, "fork_version_"), 10, 32)
			if err != nil {
				r.Invalid++
				c.addDiff("properties", printableKey(it.Key()), "invalid_java", "invalid fork version property name")
				continue
			}
			want = sdb.ReadForkStats(int32(version))
			if want == nil {
				r.MissingGtron++
				c.addDiff("properties", printableKey(it.Key()), "missing_gtron", "rooted fork-version bitmap not found")
				continue
			}
		case strings.HasPrefix(name, "fork_controller"):
			version, err := strconv.ParseInt(strings.TrimPrefix(name, "fork_controller"), 10, 32)
			if err != nil {
				r.Invalid++
				c.addDiff("properties", printableKey(it.Key()), "invalid_java", "invalid fork-controller property name")
				continue
			}
			passed := forkController.Pass(int32(version), dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
			want = []byte(strconv.FormatBool(passed))
		default:
			if v, ok := dp.Get(name); ok {
				switch len(value) {
				case 4:
					want = make([]byte, 4)
					binary.BigEndian.PutUint32(want, uint32(v))
				case 8:
					want = make([]byte, 8)
					binary.BigEndian.PutUint64(want, uint64(v))
				default:
					r.Invalid++
					c.addDiff("properties", printableKey(it.Key()), "invalid_java", fmt.Sprintf("numeric property value has %d bytes, want 4 or 8", len(value)))
					continue
				}
			} else if v, ok := dp.GetString(name); ok {
				want = []byte(v)
			} else {
				r.MissingGtron++
				c.addDiff("properties", printableKey(it.Key()), "missing_gtron", "java dynamic property is not implemented by gtron")
				continue
			}
		}
		r.Compared++
		if bytes.Equal(value, want) {
			r.Equal++
		} else {
			r.Different++
			c.addByteDiff("properties", printableKey(it.Key()), value, want)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	for _, name := range dp.Keys() {
		if _, ok := javaKeys[name]; !ok {
			r.MissingJava++
			c.addDiff("properties", name, "missing_java", "known gtron property has no normalized java key")
		}
	}
	for _, name := range dp.StringKeys() {
		if _, ok := javaKeys[name]; !ok {
			r.MissingJava++
			c.addDiff("properties", name, "missing_java", "known gtron string property has no normalized java key")
		}
	}
	if _, ok := javaKeys["latest_block_header_hash"]; !ok {
		r.MissingJava++
		c.addDiff("properties", "latest_block_header_hash", "missing_java", "head hash property missing")
	}
	return nil
}

func (c *comparer) compareAccounts(gtron ethdb.KeyValueStore, sdb *state.StateDB, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "account", Scope: "state", Present: true}
	defer c.trackStore(&r)()
	progress := c.newProgressCounter(&r, "comparing java accounts")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		var want corepb.Account
		if err := proto.Unmarshal(it.Value(), &want); err != nil {
			r.Invalid++
			c.addDiff("account", printableKey(it.Key()), "invalid_java", err.Error())
			continue
		}
		addr := tcommon.BytesToAddress(want.Address)
		got := sdb.GetAccount(addr)
		if got == nil {
			r.MissingGtron++
			c.addDiff("account", hex.EncodeToString(addr.Bytes()), "missing_gtron", "account not found")
			continue
		}
		javaAccount := normalizeAccountForStoreComparison(&want)
		gtronAccount := normalizeAccountForStoreComparison(got.Proto())
		r.Compared++
		if proto.Equal(javaAccount, gtronAccount) {
			r.Equal++
		} else {
			r.Different++
			c.addProtoDiff("account", hex.EncodeToString(addr.Bytes()), javaAccount, gtronAccount)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if !c.opts.ReverseAccounts {
		return nil
	}
	reverseProgress := c.newProgressCounter(&r, "checking gtron-only accounts")
	return rawdb.IterateStateAccountLatest(gtron, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		reverseProgress.Add(1)
		if tcommon.IsSystemAccount(row.Owner) {
			return true, nil
		}
		has, err := java.Has(row.Owner.Bytes())
		if err != nil {
			return false, err
		}
		if !has {
			r.MissingJava++
			c.addDiff("account", hex.EncodeToString(row.Owner.Bytes()), "missing_java", "gtron latest account has no java account row")
		}
		return true, nil
	})
}

// java-tron's account-asset optimization moves both asset maps out of the
// Account protobuf and sets AssetOptimized. go-tron keeps the maps inline;
// account-asset is compared separately, so these physical-layout fields must
// not make the logical account store look divergent.
func normalizeAccountForStoreComparison(account *corepb.Account) *corepb.Account {
	cloned := proto.Clone(account).(*corepb.Account)
	cloned.Asset = nil
	cloned.AssetV2 = nil
	cloned.AssetOptimized = false
	return cloned
}

func (c *comparer) compareWitnesses(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "witness", Scope: "state", Present: true}
	defer c.trackStore(&r)()
	progress := c.newProgressCounter(&r, "comparing java witnesses")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		var want corepb.Witness
		if err := proto.Unmarshal(it.Value(), &want); err != nil {
			r.Invalid++
			c.addDiff("witness", printableKey(it.Key()), "invalid_java", err.Error())
			continue
		}
		addr := tcommon.BytesToAddress(want.Address)
		got := sdb.GetWitness(addr)
		if got == nil {
			r.MissingGtron++
			c.addDiff("witness", hex.EncodeToString(addr.Bytes()), "missing_gtron", "witness not found")
			continue
		}
		r.Compared++
		if proto.Equal(&want, got.Proto()) {
			r.Equal++
		} else {
			r.Different++
			c.addProtoDiff("witness", hex.EncodeToString(addr.Bytes()), &want, got.Proto())
		}
	}
	return it.Error()
}

type contractComparisonKind uint8

const (
	contractEqual contractComparisonKind = iota
	contractDifferent
	contractMissingGtron
	contractInvalidJavaKey
	contractInvalidJava
	contractInvalidGtron
)

type contractComparisonJob struct {
	slot  int
	key   []byte
	value []byte
}

type contractComparisonResult struct {
	slot   int
	kind   contractComparisonKind
	key    string
	detail string
	want   *contractpb.SmartContract
	got    *contractpb.SmartContract
}

func (c *comparer) contractWorkerCount() int {
	workers := c.opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
		if workers > 8 {
			workers = 8
		}
	}
	if workers < 1 {
		return 1
	}
	if workers > 64 {
		return 64
	}
	return workers
}

func compareContractJob(gtron ethdb.KeyValueStore, job contractComparisonJob) contractComparisonResult {
	result := contractComparisonResult{slot: job.slot, key: hex.EncodeToString(job.key)}
	if len(job.key) != tcommon.AddressLength {
		result.kind = contractInvalidJavaKey
		result.key = printableKey(job.key)
		result.detail = fmt.Sprintf("contract address is %d bytes, want %d", len(job.key), tcommon.AddressLength)
		return result
	}
	addr := tcommon.BytesToAddress(job.key)
	gotRaw, ok, err := state.ReadCommittedContractMetadataBytes(gtron, addr)
	if err != nil {
		result.kind = contractInvalidGtron
		result.detail = err.Error()
		return result
	}
	if ok && bytes.Equal(job.value, gotRaw) {
		result.kind = contractEqual
		return result
	}

	want := new(contractpb.SmartContract)
	if err := proto.Unmarshal(job.value, want); err != nil {
		result.kind = contractInvalidJava
		result.detail = err.Error()
		return result
	}
	if !ok {
		result.kind = contractMissingGtron
		result.detail = "contract metadata not found"
		return result
	}
	got := new(contractpb.SmartContract)
	if err := proto.Unmarshal(gotRaw, got); err != nil {
		result.kind = contractInvalidGtron
		result.detail = err.Error()
		return result
	}
	if proto.Equal(want, got) {
		result.kind = contractEqual
		return result
	}
	result.kind = contractDifferent
	result.want, result.got = want, got
	return result
}

func (c *comparer) applyContractResult(r *StoreResult, result contractComparisonResult) {
	switch result.kind {
	case contractEqual:
		r.Compared++
		r.Equal++
	case contractDifferent:
		r.Compared++
		r.Different++
		c.addProtoDiff("contract", result.key, result.want, result.got)
	case contractMissingGtron:
		r.MissingGtron++
		c.addDiff("contract", result.key, "missing_gtron", result.detail)
	case contractInvalidJavaKey:
		r.Invalid++
		c.addDiff("contract", result.key, "invalid_java_key", result.detail)
	case contractInvalidJava:
		r.Invalid++
		c.addDiff("contract", result.key, "invalid_java", result.detail)
	case contractInvalidGtron:
		r.Invalid++
		c.addDiff("contract", result.key, "invalid_gtron", result.detail)
	}
}

func (c *comparer) compareContracts(gtron ethdb.KeyValueStore, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "contract", Scope: "state", Present: true}
	defer c.trackStore(&r)()
	workers := c.contractWorkerCount()
	batchSize := workers * 32
	stage := fmt.Sprintf("comparing java contracts (raw-byte fast path, workers=%d)", workers)
	c.emitProgress(ProgressEvent{Phase: "info", Store: r.Name, Detail: fmt.Sprintf("contract parallel workers=%d batch_size=%d", workers, batchSize)})
	progress := c.newProgressCounter(&r, stage)

	jobs := make(chan contractComparisonJob, batchSize)
	results := make(chan contractComparisonResult, batchSize)
	var workersWG sync.WaitGroup
	workersWG.Add(workers)
	for range workers {
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				results <- compareContractJob(gtron, job)
			}
		}()
	}
	defer func() {
		close(jobs)
		workersWG.Wait()
	}()

	processBatch := func(batch []contractComparisonJob) {
		for _, job := range batch {
			jobs <- job
		}
		ordered := make([]contractComparisonResult, len(batch))
		for range batch {
			result := <-results
			ordered[result.slot] = result
		}
		for _, result := range ordered {
			progress.Add(1)
			c.applyContractResult(&r, result)
		}
	}

	it := java.NewIterator(nil, nil)
	defer it.Release()
	batch := make([]contractComparisonJob, 0, batchSize)
	for it.Next() {
		batch = append(batch, contractComparisonJob{
			slot: len(batch), key: append([]byte(nil), it.Key()...), value: append([]byte(nil), it.Value()...),
		})
		if len(batch) == batchSize {
			processBatch(batch)
			batch = batch[:0]
		}
	}
	processBatch(batch)
	return it.Error()
}

func (c *comparer) compareABIs(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "abi", Scope: "state", Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	progress := c.newProgressCounter(&r, "comparing java ABIs")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		var want contractpb.SmartContract_ABI
		if err := proto.Unmarshal(it.Value(), &want); err != nil {
			r.Invalid++
			c.addDiff("abi", printableKey(it.Key()), "invalid_java", err.Error())
			continue
		}
		addr := tcommon.BytesToAddress(it.Key())
		got := sdb.ReadContractABI(addr)
		if got == nil {
			r.MissingGtron++
			c.addDiff("abi", hex.EncodeToString(addr.Bytes()), "missing_gtron", "contract ABI not found")
			continue
		}
		r.Compared++
		if proto.Equal(&want, got) {
			r.Equal++
		} else {
			r.Different++
			c.addProtoDiff("abi", hex.EncodeToString(addr.Bytes()), &want, got)
		}
	}
	return it.Error()
}

func (c *comparer) compareCode(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "code", Scope: "state", Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	progress := c.newProgressCounter(&r, "comparing java code rows")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		addr := tcommon.BytesToAddress(it.Key())
		got := sdb.GetCode(addr)
		r.Compared++
		if bytes.Equal(it.Value(), got) {
			r.Equal++
		} else if len(got) == 0 {
			r.MissingGtron++
			c.addDiff("code", hex.EncodeToString(addr.Bytes()), "missing_gtron", "contract code not found")
		} else {
			r.Different++
			c.addByteDiff("code", hex.EncodeToString(addr.Bytes()), it.Value(), got)
		}
	}
	return it.Error()
}

func normalizePropertyKey(key []byte) string {
	name := strings.ToLower(strings.TrimSpace(string(key)))
	if name == "total_create_witness_fee" {
		return "total_create_witness_cost"
	}
	return name
}

func (c *comparer) addProtoDiff(store, key string, java, gtron proto.Message) {
	detail := cmp.Diff(java, gtron, protocmp.Transform())
	c.addDiff(store, key, "different", truncate(detail, maxDetailLen))
}

func (c *comparer) addByteDiff(store, key string, java, gtron []byte) {
	c.addDiff(store, key, "different", fmt.Sprintf("java(len=%d sha256=%x value=%s) gtron(len=%d sha256=%x value=%s)",
		len(java), sha256.Sum256(java), shortHex(java), len(gtron), sha256.Sum256(gtron), shortHex(gtron)))
}

func (c *comparer) addDiff(store, key, kind, detail string) {
	if len(c.report.Differences) >= c.opts.MaxDifferences {
		return
	}
	c.report.Differences = append(c.report.Differences, Difference{Store: store, Key: key, Kind: kind, Detail: detail})
}

func shortHex(b []byte) string {
	const max = 64
	if len(b) <= max {
		return hex.EncodeToString(b)
	}
	return hex.EncodeToString(b[:max]) + "..."
}

func printableKey(key []byte) string {
	if utf8.Valid(key) {
		printable := true
		for _, r := range string(key) {
			if r < 0x20 || r > 0x7e {
				printable = false
				break
			}
		}
		if printable {
			return string(key)
		}
	}
	return hex.EncodeToString(key)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n... truncated ..."
}

// Sort stores and differences for deterministic JSON and tests.
func (r *Report) Sort() {
	sort.Slice(r.Stores, func(i, j int) bool { return r.Stores[i].Name < r.Stores[j].Name })
	sort.Slice(r.Differences, func(i, j int) bool {
		if r.Differences[i].Store != r.Differences[j].Store {
			return r.Differences[i].Store < r.Differences[j].Store
		}
		return r.Differences[i].Key < r.Differences[j].Key
	})
}

// Keep this compile-time assertion close to the block comparison: a gtron
// block must remain the native wrapper around the same protobuf type.
var _ interface{ Proto() *corepb.Block } = (*types.Block)(nil)
