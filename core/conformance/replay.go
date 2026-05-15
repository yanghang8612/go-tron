package conformance

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
)

// ReplayConfig configures a single ReplayRange call.
type ReplayConfig struct {
	RangeName       string
	SeedPath        string
	BlocksPath      string
	OraclePath      string
	AllowlistPath   string
	GenesisTime     int64 // ms; forwarded to ProcessBlock
	ActiveWitnesses []tcommon.Address
}

// ReplayRange drives ProcessBlock over every block in BlocksPath, compares
// DigestB at each block to the corresponding OracleEntry, consults the
// allowlist for whitelisted divergences, and returns a Report.
//
// On a hard divergence the function returns with rep.FirstFailure populated
// and rep.Passed = false; subsequent blocks are not replayed. A harness-level
// error (unreadable files, proto parse failure) is returned as the error.
func ReplayRange(ctx context.Context, cfg ReplayConfig) (*Report, error) {
	loaded, err := LoadSeed(cfg.SeedPath)
	if err != nil {
		return nil, err
	}
	sdb, dp, closure, db := loaded.StateDB, loaded.DynProps, loaded.Closure, loaded.DiskDB

	blocks, err := openBlocksReader(cfg.BlocksPath)
	if err != nil {
		return nil, fmt.Errorf("open blocks: %w", err)
	}
	defer blocks.Close()

	oracle, err := openOracleReader(cfg.OraclePath)
	if err != nil {
		return nil, fmt.Errorf("open oracle: %w", err)
	}
	defer oracle.Close()

	allowlist, err := LoadAllowlist(cfg.AllowlistPath)
	if err != nil {
		return nil, err
	}

	rep := &Report{RangeName: cfg.RangeName, Passed: true}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		blk, err := blocks.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read block: %w", err)
		}

		ent, err := oracle.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("oracle ran out at block %d", blk.Number())
		}
		if err != nil {
			return nil, fmt.Errorf("read oracle: %w", err)
		}
		if ent.BlockNum != blk.Number() {
			return nil, fmt.Errorf("oracle/block height mismatch: oracle=%d block=%d", ent.BlockNum, blk.Number())
		}

		// validateEnvelope=false: conformance replay consumes pre-validated
		// java-tron blocks; signatures have already been verified upstream.
		if _, procErr := core.ProcessBlock(sdb, dp, blk, db, cfg.ActiveWitnesses, cfg.GenesisTime, false); procErr != nil {
			div := &Divergence{
				BlockNum:   blk.Number(),
				FieldDiffs: []FieldDiff{{Field: "_processBlockError", Got: procErr.Error(), Want: "success"}},
			}
			rep.Passed = false
			rep.FirstFailure = div
			rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: false, Divergence: div})
			return rep, nil
		}

		gotB := DigestB(sdb, db, closure, dp)
		wantBytes, err := hex.DecodeString(ent.DigestB)
		if err != nil {
			return nil, fmt.Errorf("oracle digestB at block %d: %w", blk.Number(), err)
		}
		if len(wantBytes) != 32 {
			return nil, fmt.Errorf("oracle digestB at block %d: want 32 bytes, got %d", blk.Number(), len(wantBytes))
		}

		if digestMatches(gotB[:], wantBytes) {
			rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: true})
			continue
		}

		gotC := DigestC(sdb, db, closure, dp)
		diffs := diffC(gotC, ent.DiagC)

		// DigestB mismatch with no structural diffs means DigestC can't see
		// what DigestB flagged — e.g. oracle DigestB was hand-edited or
		// some canonical encoding detail the JSON view loses. Surface it as
		// a synthetic field so it can't be silently whitelisted.
		if len(diffs) == 0 {
			diffs = []FieldDiff{{
				Field: "_digestB",
				Got:   hex.EncodeToString(gotB[:]),
				Want:  ent.DigestB,
			}}
		}

		unhandled := make([]FieldDiff, 0, len(diffs))
		for _, d := range diffs {
			if !allowlist.IsWhitelisted(blk.Number(), d.Field) {
				unhandled = append(unhandled, d)
			}
		}

		if len(unhandled) == 0 {
			rep.AllowlistHits += len(diffs)
			rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: true})
			continue
		}

		div := &Divergence{
			BlockNum:   blk.Number(),
			FieldDiffs: unhandled,
			GotJSON:    gotC,
			WantJSON:   ent.DiagC,
		}
		rep.Passed = false
		rep.FirstFailure = div
		rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: false, Divergence: div})
		return rep, nil
	}

	rep.StaleAllowlistEntries = allowlist.Stale()
	return rep, nil
}

func digestMatches(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- blocks.bin reader ----

type blockReader struct {
	f   *os.File
	buf *bufio.Reader
}

func openBlocksReader(path string) (*blockReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &blockReader{f: f, buf: bufio.NewReader(f)}, nil
}

func (r *blockReader) Next() (*types.Block, error) {
	n, err := binary.ReadUvarint(r.buf)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.buf, buf); err != nil {
		return nil, err
	}
	return types.UnmarshalBlock(buf)
}

func (r *blockReader) Close() error { return r.f.Close() }

// WriteBlocks appends the given blocks to path in the blocks.bin length-prefix
// format. Used by the smoke-corpus generator and by tests.
func WriteBlocks(path string, blocks []*types.Block) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var buf [binary.MaxVarintLen64]byte
	for _, blk := range blocks {
		data, err := blk.Marshal()
		if err != nil {
			return err
		}
		n := binary.PutUvarint(buf[:], uint64(len(data)))
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// ---- oracle.ndjson reader ----

type oracleReader struct {
	f       *os.File
	scanner *bufio.Scanner
}

func openOracleReader(path string) (*oracleReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	return &oracleReader{f: f, scanner: scanner}, nil
}

func (r *oracleReader) Next() (OracleEntry, error) {
	for r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ent OracleEntry
		if err := json.Unmarshal(line, &ent); err != nil {
			return OracleEntry{}, err
		}
		return ent, nil
	}
	if err := r.scanner.Err(); err != nil {
		return OracleEntry{}, err
	}
	return OracleEntry{}, io.EOF
}

func (r *oracleReader) Close() error { return r.f.Close() }

// WriteOracle writes entries as ndjson. Used by tests and by capture tooling.
func WriteOracle(path string, entries []OracleEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return w.Flush()
}

// ---- JSON diff ----

// diffC compares two DigestC JSON payloads and returns the flat list of
// field-level disagreements. Missing side → reported as "" for that side.
// Payload shape: { "accounts": {<hex>: {<field>: value}}, "dp": {<key>: v} }
func diffC(got, want json.RawMessage) []FieldDiff {
	var gotM, wantM map[string]json.RawMessage
	_ = json.Unmarshal(got, &gotM)
	_ = json.Unmarshal(want, &wantM)
	if gotM == nil {
		gotM = map[string]json.RawMessage{}
	}
	if wantM == nil {
		wantM = map[string]json.RawMessage{}
	}

	var out []FieldDiff
	out = append(out, diffSubtree("account", gotM["accounts"], wantM["accounts"])...)
	out = append(out, diffMap("dp", gotM["dp"], wantM["dp"])...)
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	return out
}

// diffSubtree compares a "accounts" subtree: { <hex>: { <field>: v } }.
func diffSubtree(prefix string, got, want json.RawMessage) []FieldDiff {
	var gotAccs, wantAccs map[string]json.RawMessage
	_ = json.Unmarshal(got, &gotAccs)
	_ = json.Unmarshal(want, &wantAccs)
	if gotAccs == nil {
		gotAccs = map[string]json.RawMessage{}
	}
	if wantAccs == nil {
		wantAccs = map[string]json.RawMessage{}
	}
	keys := unionKeys(gotAccs, wantAccs)
	var out []FieldDiff
	for _, k := range keys {
		out = append(out, diffMap(prefix+":"+k, gotAccs[k], wantAccs[k])...)
	}
	return out
}

// diffMap compares two flat {k: v} JSON maps under a field prefix.
func diffMap(prefix string, got, want json.RawMessage) []FieldDiff {
	var gotM, wantM map[string]json.RawMessage
	_ = json.Unmarshal(got, &gotM)
	_ = json.Unmarshal(want, &wantM)
	if gotM == nil {
		gotM = map[string]json.RawMessage{}
	}
	if wantM == nil {
		wantM = map[string]json.RawMessage{}
	}
	keys := unionKeys(gotM, wantM)
	var out []FieldDiff
	for _, k := range keys {
		g := string(gotM[k])
		w := string(wantM[k])
		if g != w {
			out = append(out, FieldDiff{Field: prefix + ":" + k, Got: g, Want: w})
		}
	}
	return out
}

func unionKeys(a, b map[string]json.RawMessage) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
