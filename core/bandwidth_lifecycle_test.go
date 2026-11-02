package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

type bandwidthLifecycleMetadata struct {
	Root      tcommon.Hash
	Addresses []tcommon.Address
	Filtered  bool
}
type bandwidthLifecycleMeasure struct {
	Wall           time.Time
	CPU            int64
	Alloc, Mallocs uint64
}

func bandwidthLifecycleCPU() int64 {
	var r syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
		panic(e)
	}
	return r.Utime.Nano() + r.Stime.Nano()
}
func bandwidthLifecycleStart() bandwidthLifecycleMeasure {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bandwidthLifecycleMeasure{time.Now(), bandwidthLifecycleCPU(), m.TotalAlloc, m.Mallocs}
}
func (a bandwidthLifecycleMeasure) finish() map[string]int64 {
	wall, cpu := time.Since(a.Wall).Nanoseconds(), bandwidthLifecycleCPU()-a.CPU
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return map[string]int64{"wall_ns": wall, "cpu_ns": cpu, "alloc_bytes": int64(m.TotalAlloc - a.Alloc), "mallocs": int64(m.Mallocs - a.Mallocs)}
}
func bandwidthLifecycleDelta(a, b map[string]int64) map[string]int64 {
	o := map[string]int64{}
	for k, v := range a {
		o[k] = v - b[k]
	}
	return o
}
func bandwidthLifecycleAdd(a, b map[string]int64) {
	for k, v := range b {
		a[k] += v
	}
}

type bandwidthLifecycleStore struct {
	*pebbledb.Database
	live, get, has, presence int64
	// Rows are durable foreground scope calls only, not logical object/cache hits.
	rows                   [4]int64 // V1row0,V1row1,V2bandwidth,other
	row1Found, pointErrors int64
	ownerID                map[tcommon.Address]int
	row1MissingOwners      [16]uint64
}

func (s *bandwidthLifecycleStore) Get(k []byte) ([]byte, error) { s.get++; return s.Database.Get(k) }
func (s *bandwidthLifecycleStore) Has(k []byte) (bool, error)   { s.has++; return s.Database.Has(k) }
func (s *bandwidthLifecycleStore) GetWithPresence(k []byte) ([]byte, bool, error) {
	s.presence++
	return s.Database.GetWithPresence(k)
}
func (s *bandwidthLifecycleStore) View(k []byte, fn func([]byte) error) error {
	s.live++
	return s.Database.View(k, fn)
}
func (s *bandwidthLifecycleStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	v, e := s.Database.NewKeyValueSnapshot()
	if e != nil {
		return nil, e
	}
	return &bandwidthLifecycleSnapshot{KeyValueSnapshot: v, store: s}, nil
}

type bandwidthLifecycleSnapshot struct {
	pointread.KeyValueSnapshot
	store *bandwidthLifecycleStore
}

func (s *bandwidthLifecycleSnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	c, e := s.KeyValueSnapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if e != nil {
		return nil, e
	}
	return &bandwidthLifecycleCursor{Cursor: c, store: s.store}, nil
}

type bandwidthLifecycleCursor struct {
	pointread.Cursor
	store *bandwidthLifecycleStore
}

func (c *bandwidthLifecycleCursor) View(k []byte, fn func([]byte) error) (bool, error) {
	owner, _, d, key, ok := rawdb.DecodeStateKVLatestKey(k)
	kind := 3
	if ok && len(key) == 4 {
		n := binary.BigEndian.Uint32(key)
		if d == kvdomains.AccountFrozenBandwidthAux && n < 2 {
			kind = int(n)
		} else if d == kvdomains.AccountFrozenV2Aux && n == uint32(corepb.ResourceCode_BANDWIDTH) {
			kind = 2
		}
	}
	c.store.rows[kind]++
	found, e := c.Cursor.View(k, fn)
	if e != nil {
		c.store.pointErrors++
	}
	if kind == 1 {
		if found {
			c.store.row1Found++
		} else if e == nil {
			if id, ok := c.store.ownerID[owner]; ok {
				c.store.row1MissingOwners[id/64] |= 1 << uint(id%64)
			}
		}
	}
	return found, e
}
func (s *bandwidthLifecycleStore) logical() map[string]int64 {
	distinct := 0
	for _, mask := range s.row1MissingOwners {
		distinct += bits.OnesCount64(mask)
	}
	return map[string]int64{"hot_live_get": s.get, "hot_live_has": s.has, "hot_live_presence": s.presence, "hot_live_view": s.live, "scope_v1_row0": s.rows[0], "scope_v1_row1": s.rows[1], "scope_v2_bandwidth": s.rows[2], "scope_other": s.rows[3], "scope_row1_found": s.row1Found, "scope_point_errors": s.pointErrors, "scope_distinct_row1_missing_owners": int64(distinct)}
}
func bandwidthLifecycleBlock(t testing.TB, addresses []tcommon.Address, number uint64) *types.Block {
	t.Helper()
	txs := make([]*corepb.Transaction, len(addresses))
	for i, a := range addresses {
		parameter, e := anypb.New(&contractpb.TransferContract{OwnerAddress: a.Bytes(), ToAddress: addresses[(i+1)%len(addresses)].Bytes(), Amount: 1})
		if e != nil {
			t.Fatal(e)
		}
		txs[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: parameter}}}}
	}
	return types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), WitnessAddress: addresses[0].Bytes()}}, Transactions: txs})
}

// Manual same-source paired test. It calls the actual core charge dispatcher,
// so original production sources compile this exact test without a new API shim.
func TestBandwidthChargeLifecycleProcess(t *testing.T) {
	result := os.Getenv("GTRON_BANDWIDTH_LIFECYCLE_RESULT")
	if result == "" {
		t.Skip("manual paired consensus bandwidth lifecycle")
	}
	source, work, role, arm := os.Getenv("GTRON_BANDWIDTH_LIFECYCLE_FIXTURE"), os.Getenv("GTRON_BANDWIDTH_LIFECYCLE_WORK"), os.Getenv("GTRON_BANDWIDTH_LIFECYCLE_ROLE"), os.Getenv("GTRON_BANDWIDTH_LIFECYCLE_ARM")
	if source == "" || work == "" || (arm != "baseline" && arm != "candidate") || (role != "pre70" && role != "post70" && role != "positive-warm") {
		t.Fatal("invalid fixed lifecycle input")
	}
	whole := bandwidthLifecycleStart()
	setup := bandwidthLifecycleStart()
	encoded, e := os.ReadFile(filepath.Join(source, "metadata.json"))
	if e != nil {
		t.Fatal(e)
	}
	var m bandwidthLifecycleMetadata
	if e = json.Unmarshal(encoded, &m); e != nil {
		t.Fatal(e)
	}
	if len(m.Addresses) != 1024 || !m.Filtered {
		t.Fatal("expected sealed1024 filtered fixture")
	}
	if e = os.CopyFS(filepath.Join(work, "hot"), os.DirFS(filepath.Join(source, "image"))); e != nil {
		t.Fatal(e)
	}
	namespace := "bench/bandwidth-consensus/lifecycle/"
	opts := pebbledb.DefaultOptions()
	opts.EnableL6Filter = true
	opts.DisableAutomaticCompactions = true
	disk, e := pebbledb.New(filepath.Join(work, "hot"), 16, 16, namespace, false, opts)
	if e != nil {
		t.Fatal(e)
	}
	history, e := pebbledb.New(filepath.Join(work, "history"), 16, 16, namespace+"history/", false, opts)
	if e != nil {
		t.Fatal(e)
	}
	observed := &bandwidthLifecycleStore{Database: disk, ownerID: make(map[tcommon.Address]int, 1024)}
	for i, a := range m.Addresses {
		observed.ownerID[a] = i
	}
	sink := rawdb.NewDirectHistoryStore(observed, history)
	database := state.NewDatabase(rawdb.WrapKeyValueStore(sink))
	s, e := state.New(m.Root, database)
	if e != nil {
		t.Fatal(e)
	}
	buffer := blockbuffer.New(sink)
	buffer.SetBaseReadCacheSize(8 << 20)
	s.SetAccountKVIndexStore(buffer)
	commits := s.NewCommitScope()
	prefetch := state.NewStateReadAhead(buffer, state.StateReadAheadConfig{Workers: 1})
	prefetch.Start()
	dp := state.NewDynamicProperties()
	dp.SetTotalNetWeight(1024)
	dp.Set("total_net_limit", 1_000_000_000)
	if role != "pre70" {
		dp.Set("unfreeze_delay_days", 14)
	}
	const blocks, batch = 32, 64
	sets := make([][]tcommon.Address, blocks)
	planning := make([]*types.Block, blocks)
	for b := 0; b < blocks; b++ {
		start := (b * batch) % 1024
		sets[b] = m.Addresses[start : start+batch]
		if role == "positive-warm" {
			sets[b] = make([]tcommon.Address, batch)
			for j := range sets[b] {
				sets[b][j] = m.Addresses[j*4]
			}
		}
		planning[b] = bandwidthLifecycleBlock(t, sets[b], uint64(b+1))
	}
	counter := func(ns, suffix string) int64 {
		return metrics.GetOrRegisterCounter(ns+"disk/physical/read/sst/"+suffix, nil).Snapshot().Count()
	}
	io := func() map[string]int64 {
		o := map[string]int64{}
		for _, ns := range []string{"", "history/"} {
			for _, suffix := range []string{"calls", "bytes", "nanos", "errors", "short_reads"} {
				o[ns+suffix] = counter(namespace+ns, suffix)
			}
		}
		return o
	}
	cursorStats := func() map[string]int64 {
		o := map[string]int64{}
		for _, k := range []string{"table_requests", "table_iterator_opens", "table_reuses", "table_evictions", "table_fallbacks", "retained_peak_sum"} {
			o[k] = metrics.GetOrRegisterCounter("blockbuffer/storage_read/pebble/get_cursor/"+k, nil).Snapshot().Count()
		}
		return o
	}
	setupCost := setup.finish()
	beforeIO, beforeLogical, beforeCursor := io(), observed.logical(), cursorStats()
	beforeScopes := metrics.GetOrRegisterCounter("blockbuffer/storage_read/scopes", nil).Snapshot().Count()
	active := bandwidthLifecycleStart()
	phases := map[string]map[string]int64{}
	for _, name := range []string{"prefetch", "demand", "commit"} {
		phases[name] = map[string]int64{}
	}
	phase := func(name string, fn func()) {
		before, cpu, start := io(), bandwidthLifecycleCPU(), time.Now()
		fn()
		cost := phases[name]
		cost["wall_ns"] += time.Since(start).Nanoseconds()
		cost["cpu_ns"] += bandwidthLifecycleCPU() - cpu
		for k, v := range bandwidthLifecycleDelta(io(), before) {
			cost["sst/"+k] += v
		}
	}
	roots := make([]string, 0, blocks)
	charged, declined := 0, 0
	for b := 0; b < blocks; b++ {
		number := uint64(b + 1)
		hash := tcommon.Hash{30: 1, 31: byte(number)}
		buffer.BeginBlock(hash, number)
		phase("prefetch", func() {
			if !prefetch.EnqueueBlock(planning[b], 0) {
				t.Fatal("prefetch dropped")
			}
			prefetch.Wait()
		})
		prefetch.MarkApplying(number)
		closeScope, e := s.BeginCanonicalStorageReadScope(number)
		if e != nil {
			t.Fatal(e)
		}
		s.BeginDomainChangeJournalCapture(buffer, number, hash, number, number)
		mark := s.DomainChangeJournalMark()
		phase("demand", func() {
			for _, a := range sets[b] {
				if _, e = s.AccountPermissionByID(a, 0); e != nil {
					t.Fatal(e)
				}
				if chargeStakedNet(s, dp, a, 100, int64(number)) {
					charged++
				} else {
					declined++
				}
				s.AddBalance(a, 1)
			}
		})
		if e = s.Error(); e != nil {
			t.Fatal(e)
		}
		phase("commit", func() {
			if e = s.FlushDomainChangesSince(mark, number); e != nil {
				t.Fatal(e)
			}
			if e = closeScope(); e != nil {
				t.Fatal(e)
			}
			root, _, e := s.CommitWithStatsOptionsInScope(commits, state.CommitOptions{BlockNumber: number})
			if e != nil {
				t.Fatal(e)
			}
			roots = append(roots, root.Hex())
			if e = commits.FlushLatest(); e != nil {
				t.Fatal(e)
			}
			buffer.CommitBlock()
			if e = buffer.Flush(sink); e != nil {
				t.Fatal(e)
			}
		})
	}
	activeCost := active.finish()
	activeIO, activeLogical, activeCursor := bandwidthLifecycleDelta(io(), beforeIO), bandwidthLifecycleDelta(observed.logical(), beforeLogical), bandwidthLifecycleDelta(cursorStats(), beforeCursor)
	// Detailed checks and hashing deliberately follow active timing. All remain
	// in the whole-process observation, including the separate prefetch worker.
	verify := bandwidthLifecycleStart()
	stats := prefetch.Stats()
	scopeCount := metrics.GetOrRegisterCounter("blockbuffer/storage_read/scopes", nil).Snapshot().Count() - beforeScopes
	if scopeCount != blocks {
		t.Fatalf("scope count%d want32", scopeCount)
	}
	if stats.ProcessedBlocks != blocks || stats.Errors != 0 || stats.StaleBlocks != 0 || stats.DroppedBlocks != 0 {
		t.Fatalf("prefetch qualification %+v", stats)
	}
	wantCharged := 1024
	if role == "pre70" {
		wantCharged = 512
	} else if role == "positive-warm" {
		wantCharged = 2048
	}
	if charged != wantCharged || declined != blocks*batch-wantCharged {
		t.Fatalf("charged%d declined%d want%d", charged, declined, wantCharged)
	}
	if activeLogical["scope_point_errors"] != 0 || activeLogical["scope_row1_found"] != 0 {
		t.Fatal("invalid source shape/cursor error")
	}
	if arm == "candidate" || role == "pre70" {
		if activeLogical["scope_v1_row1"] != 0 {
			t.Fatal("unexpected compatibility probe")
		}
	} else {
		wantOwners := int64(1024)
		if role == "positive-warm" {
			wantOwners = 64
		}
		if activeLogical["scope_distinct_row1_missing_owners"] != wantOwners {
			t.Fatalf("row1 source coverage%d want%d", activeLogical["scope_distinct_row1_missing_owners"], wantOwners)
		}
	}
	historyHash, outcomeHash := sha256.New(), sha256.New()
	historyChanges := 0
	for number := uint64(1); number <= blocks; number++ {
		if e = rawdb.IterateStateDomainChanges(sink, number, func(change *rawdb.StateDomainChange) (bool, error) {
			raw, e := json.Marshal(change)
			if e != nil {
				return false, e
			}
			historyHash.Write(raw)
			historyChanges++
			return true, nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	for _, a := range m.Addresses {
		rawWindow, opt := s.GetNetWindow(a)
		values := [5]int64{s.GetNetUsage(a), s.GetLatestConsumeTime(a), rawWindow, s.GetBalance(a), 0}
		if opt {
			values[4] = 1
		}
		for _, v := range values {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], uint64(v))
			outcomeHash.Write(raw[:])
		}
	}
	verifyCost := verify.finish()
	closeCost := bandwidthLifecycleStart()
	prefetch.Close()
	commits.Discard()
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	if e = sink.Close(); e != nil {
		t.Fatal(e)
	}
	closed := closeCost.finish()
	report := map[string]any{"schema": "bandwidth-consensus-lifecycle-v1", "role": role, "arm": arm, "blocks": blocks, "owners_per_block": batch, "charged": charged, "declined": declined, "setup": setupCost, "active": activeCost, "verification": verifyCost, "close": closed, "whole": whole.finish(), "phases": phases, "active_sst": activeIO, "whole_sst": io(), "active_logical": activeLogical, "active_cursor": activeCursor, "scope_count": scopeCount, "read_ahead": stats, "roots": roots, "history_changes": historyChanges, "history_sha256": fmt.Sprintf("%x", historyHash.Sum(nil)), "outcome_sha256": fmt.Sprintf("%x", outcomeHash.Sum(nil)), "env": map[string]string{"GOMAXPROCS": os.Getenv("GOMAXPROCS"), "GOGC": os.Getenv("GOGC"), "GOMEMLIMIT": os.Getenv("GOMEMLIMIT"), "GTRON_PEBBLE_BOUNDED_POINT_READ": os.Getenv("GTRON_PEBBLE_BOUNDED_POINT_READ")}}
	encoded, e = json.MarshalIndent(report, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(result, encoded, 0600); e != nil {
		t.Fatal(e)
	}
}
