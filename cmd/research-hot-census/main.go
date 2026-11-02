// Read-only logical census of an explicitly captured maintenance copy.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"google.golang.org/protobuf/encoding/protowire"
)

type reader struct{ db *pebble.DB }

func (r reader) Get(k []byte) ([]byte, error) {
	v, c, e := r.db.Get(k)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	return bytes.Clone(v), nil
}
func (r reader) Has(k []byte) (bool, error) {
	_, c, e := r.db.Get(k)
	if e == pebble.ErrNotFound {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return true, c.Close()
}

type count struct {
	Rows         uint64 `json:"rows"`
	KeyBytes     uint64 `json:"key_bytes"`
	ValueBytes   uint64 `json:"value_bytes"`
	Transactions uint64 `json:"transactions_or_receipts"`
	HasHeight    bool   `json:"has_height"`
	Min          uint64 `json:"min_height"`
	Max          uint64 `json:"max_height"`
}

func (c *count) add(k, v []byte, h uint64, has bool, n uint64) {
	c.Rows++
	c.KeyBytes += uint64(len(k))
	c.ValueBytes += uint64(len(v))
	c.Transactions += n
	if has {
		if !c.HasHeight || h < c.Min {
			c.Min = h
		}
		if !c.HasHeight || h > c.Max {
			c.Max = h
		}
		c.HasHeight = true
	}
}

type report struct {
	Started                   string                         `json:"started_utc"`
	Finished                  string                         `json:"finished_utc,omitempty"`
	Database                  string                         `json:"database"`
	Complete                  bool                           `json:"complete"`
	Head                      uint64                         `json:"head"`
	Solid                     uint64                         `json:"solid"`
	ColdExclusive             uint64                         `json:"chain_freezer_exclusive"`
	TxPrunedExclusive         uint64                         `json:"tx_pruned_exclusive"`
	EligibleExclusive         uint64                         `json:"eligible_exclusive"`
	EligibleCompleteExclusive uint64                         `json:"eligible_complete_segment_exclusive"`
	Margin                    uint64                         `json:"freezer_margin_blocks"`
	Stages                    map[string]rawdb.StageProgress `json:"stages"`
	Families                  map[string]*count              `json:"families"`
	Windows                   map[string]*count              `json:"windows"`
	Heights                   map[string]*count              `json:"height_10000_buckets"`
	SSTEstimates              map[string]uint64              `json:"sst_range_estimates_including_shared_boundary_blocks"`
	Errors                    []string                       `json:"errors"`
	Skipped                   map[string]bool                `json:"skipped_state_families"`
}

func add(m map[string]*count, name string, k, v []byte, h uint64, has bool, n uint64) {
	c := m[name]
	if c == nil {
		c = new(count)
		m[name] = c
	}
	c.add(k, v, h, has, n)
}
func wireCount(data []byte, field protowire.Number) (uint64, error) {
	var n uint64
	for len(data) > 0 {
		num, typ, l := protowire.ConsumeTag(data)
		if l < 0 {
			return n, fmt.Errorf("tag %d", l)
		}
		data = data[l:]
		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return n, fmt.Errorf("field %d", size)
		}
		if num == field && typ == protowire.BytesType {
			n++
		}
		data = data[size:]
	}
	return n, nil
}
func wireHeight(data []byte) (uint64, bool, error) {
	var h uint64
	var found bool
	for len(data) > 0 {
		num, typ, l := protowire.ConsumeTag(data)
		if l < 0 {
			return 0, false, fmt.Errorf("tag %d", l)
		}
		data = data[l:]
		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return 0, false, fmt.Errorf("field %d", size)
		}
		if num == 3 && typ == protowire.VarintType {
			h, _ = protowire.ConsumeVarint(data)
			found = true
		}
		data = data[size:]
	}
	return h, found, nil
}
func unknownName(k []byte) string {
	for i, b := range k {
		if i >= 48 || b < 32 || b > 126 {
			return "other/binary_or_long"
		}
		if b == '-' {
			return "other/" + string(k[:i+1])
		}
	}
	if len(k) > 48 {
		return "other/" + string(k[:48])
	}
	return "other/" + string(k)
}
func save(path string, r *report) {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		panic(e)
	}
	if e = os.WriteFile(path+".tmp", b, 0644); e != nil {
		panic(e)
	}
	if e = os.Rename(path+".tmp", path); e != nil {
		panic(e)
	}
}

func main() {
	dbPath := flag.String("db", "", "captured maintenance chaindata directory")
	out := flag.String("out", "", "new output directory")
	rate := flag.Int64("logical-mib-sec", 128, "bounded decoded input throughput")
	margin := flag.Uint64("margin", 128, "production freezer.margin; default from core/freezer.Default")
	flag.Parse()
	abs, e := filepath.Abs(*dbPath)
	if e != nil {
		panic(e)
	}
	if !strings.Contains(abs, "/maintenance/") || filepath.Base(abs) != "chaindata" {
		panic("only an explicit maintenance chaindata copy is allowed")
	}
	if e = os.Mkdir(*out, 0755); e != nil {
		panic(e)
	}
	cache := pebble.NewCache(128 << 20)
	defer cache.Unref()
	db, e := pebble.Open(abs, &pebble.Options{ReadOnly: true, Cache: cache, MaxOpenFiles: 256})
	if e != nil {
		panic(e)
	}
	defer db.Close()
	r := report{Started: time.Now().UTC().Format(time.RFC3339Nano), Database: abs, Stages: make(map[string]rawdb.StageProgress), Families: make(map[string]*count), Windows: make(map[string]*count), Heights: make(map[string]*count), SSTEstimates: make(map[string]uint64), Skipped: make(map[string]bool)}
	getter := reader{db}
	stages := []rawdb.StageID{rawdb.StageFinish, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageChainFreezer, rawdb.StageFreezerTxIndexPrune, rawdb.StageChainFreezerStateRootPrune, rawdb.StageSnapshotBuild, rawdb.StageSnapshotPrune, rawdb.StageSnapshotHotPrune, rawdb.StageSnapshotEventLogBuild, rawdb.StageSnapshotChainLookupPrune, rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady, rawdb.StageSyncFinish}
	for _, id := range stages {
		s, ok, e := rawdb.ReadStageProgressRow(getter, id)
		if e != nil {
			panic(e)
		}
		if ok {
			r.Stages[string(id)] = s
		}
	}
	r.Head = r.Stages[string(rawdb.StageFinish)].BlockNum
	// Current-format solidification is a derived dynamic property. The legacy
	// HeadSolidBlock hash is intentionally absent from fresh databases.
	execution, e := rawdb.ReadStateExecutionProperties(getter)
	if e != nil {
		panic(e)
	}
	if uint64(execution.BlockNum) != r.Head {
		panic("execution properties and Finish are not the same captured cut")
	}
	r.Solid = uint64(execution.SolidifiedBlockNum)
	if s, ok := r.Stages[string(rawdb.StageChainFreezer)]; ok {
		r.ColdExclusive = s.BlockNum + 1
	}
	r.TxPrunedExclusive = r.Stages[string(rawdb.StageFreezerTxIndexPrune)].BlockNum
	r.Margin = *margin
	if r.Solid >= *margin {
		r.EligibleExclusive = r.Solid - *margin + 1
	}
	r.EligibleCompleteExclusive = r.EligibleExclusive / 65536 * 65536
	ranges := rawdb.DiskSpaceObservationRanges()
	var families []rawdb.PhysicalSpaceRange
	for _, rr := range ranges {
		if rr.Name != "transaction_receipts" {
			families = append(families, rr)
		}
		n, e := db.EstimateDiskUsage(rr.Start, rr.End)
		if e != nil {
			panic(e)
		}
		r.SSTEstimates[rr.Name] = n
	}
	for _, rr := range rawdb.HotMaintenanceRanges() {
		if rr.Name == "transaction_info" || rr.Name == "transaction_info_block" {
			families = append(families, rr)
		}
	}
	skip := map[string]bool{"account_kv_latest": true, "account_latest": true, "kv_generation": true, "state_code": true}
	it, e := db.NewIter(nil)
	if e != nil {
		panic(e)
	}
	defer it.Close()
	started, last := time.Now(), time.Now()
	var total, rows uint64
	for valid := it.First(); valid; {
		k, v := it.Key(), it.Value()
		name := unknownName(k)
		var family *rawdb.PhysicalSpaceRange
		for i := range families {
			rr := &families[i]
			if bytes.Compare(k, rr.Start) >= 0 && bytes.Compare(k, rr.End) < 0 {
				name = rr.Name
				family = rr
				break
			}
		}
		if skip[name] {
			r.Skipped[name] = true
			valid = it.SeekGE(family.End)
			continue
		}
		var h, n uint64
		var has bool
		var err error
		switch name {
		case "block_body", "staged_body", "transaction_info_block":
			if len(k) != len(family.Start)+8 {
				err = fmt.Errorf("bad number key %x", k)
			} else {
				h = binary.BigEndian.Uint64(k[len(family.Start):])
				has = true
			}
			if name == "block_body" || name == "staged_body" {
				n, err = wireCount(v, 1)
			} else {
				n, err = wireCount(v, 3)
			}
		case "transaction_index":
			if len(v) != 8 {
				err = fmt.Errorf("bad tx location %x", k)
			} else {
				h = rawdb.TransactionIndexLocationBlock(binary.BigEndian.Uint64(v))
				has = true
				n = 1
			}
		case "transaction_info":
			h, has, err = wireHeight(v)
			n = 1
		}
		if err != nil {
			if len(r.Errors) < 64 {
				r.Errors = append(r.Errors, name+"/"+hex.EncodeToString(k)+": "+err.Error())
			}
			panic(err)
		}
		add(r.Families, name, k, v, h, has, n)
		if has {
			boundary := r.ColdExclusive
			if name == "transaction_index" {
				boundary = r.TxPrunedExclusive
			}
			window := "recent_retention"
			if h < boundary {
				window = "below_published_or_pruned_boundary"
			} else if h < r.EligibleCompleteExclusive {
				window = "eligible_complete_unarchived"
			} else if h < r.EligibleExclusive {
				window = "eligible_incomplete_segment"
			} else if h > r.Head {
				window = "ahead_of_head"
			}
			if name == "staged_body" && h <= r.Head {
				window = "staged_not_ahead_of_head"
			}
			add(r.Windows, name+"/"+window, k, v, h, true, n)
			add(r.Heights, fmt.Sprintf("%s/%d", name, h/10000*10000), k, v, h, true, n)
		}
		total += uint64(len(k) + len(v))
		rows++
		if rows%16384 == 0 {
			if *rate > 0 {
				target := time.Duration(float64(total) / float64(*rate<<20) * float64(time.Second))
				if d := target - time.Since(started); d > 0 {
					time.Sleep(d)
				}
			}
			if time.Since(last) > 5*time.Second {
				save(filepath.Join(*out, "progress.json"), &r)
				last = time.Now()
			}
		}
		valid = it.Next()
	}
	if e = it.Error(); e != nil {
		panic(e)
	}
	r.Complete = true
	r.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	save(filepath.Join(*out, "hot-census.json"), &r)
	fmt.Printf("HOT_CENSUS_COMPLETE rows=%d logical_bytes=%d elapsed=%s head=%d cold_exclusive=%d\n", rows, total, time.Since(started), r.Head, r.ColdExclusive)
}
