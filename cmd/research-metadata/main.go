package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"os"
	"path/filepath"
	"strings"
	"time"
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
func main() {
	p := flag.String("db", "", "closed maintenance copy ending chaindata")
	o := flag.String("out", "", "new JSON path")
	flag.Parse()
	if !strings.Contains(*p, "/maintenance/") || filepath.Base(*p) != "chaindata" {
		panic("maintenance copy only")
	}
	c := pebble.NewCache(32 << 20)
	defer c.Unref()
	d, e := pebble.Open(*p, &pebble.Options{ReadOnly: true, Cache: c})
	if e != nil {
		panic(e)
	}
	defer d.Close()
	r := reader{d}
	ex, e := rawdb.ReadStateExecutionProperties(r)
	if e != nil {
		panic(e)
	}
	ss := map[string]rawdb.StageProgress{}
	for _, id := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageTxLookup, rawdb.StageChainFreezer, rawdb.StageFreezerTxIndexPrune, rawdb.StageSnapshotBuild, rawdb.StageSnapshotEventLogBuild, rawdb.StageSnapshotChainLookupPrune} {
		s, ok, e := rawdb.ReadStageProgressRow(r, id)
		if e != nil {
			panic(e)
		}
		if ok {
			ss[string(id)] = s
		}
	}
	f := ss[string(rawdb.StageFinish)]
	if f.BlockNum != uint64(ex.BlockNum) || !f.HasBlockHash || f.BlockHash != ex.BlockHash {
		panic("execution cut and Finish mismatch")
	}
	h, ok, e := rawdb.ReadStateCommitmentHead(r)
	if e != nil || !ok {
		panic(fmt.Sprintf("commitment head %v %v", ok, e))
	}
	if h.BlockNum != f.BlockNum || h.BlockHash != f.BlockHash {
		panic("MuHash HEAD and Finish mismatch")
	}
	v := map[string]any{"observed_utc": time.Now().UTC().Format(time.RFC3339Nano), "database": *p, "execution": ex, "stages": ss, "execution_finish_hash_equal": true, "muhash_head_finish_equal": true, "muhash_stored_state_digest_valid": true, "muhash_scheme": h.Scheme, "muhash_head_root": h.Root.Hex(), "scope": "point metadata checks only; not a full source MuHash recomputation"}
	b, _ := json.MarshalIndent(v, "", "  ")
	file, e := os.OpenFile(*o, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		panic(e)
	}
	defer file.Close()
	if _, e = file.Write(b); e != nil {
		panic(e)
	}
	fmt.Println(string(b))
}
