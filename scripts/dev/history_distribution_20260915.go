//go:build ignore

// Standalone fixed-input diagnostic. No node, source DB, output files or writes.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
)

const (
	capturePath       = "/data/gtron/releases/20260915-history-range/result/capture"
	manifestSHA       = "897936be40658c1b8a770970ca15f9a16037e6f8eea822aad26d390c8a826678"
	expectedRows      = uint64(9163)
	expectedPrevBytes = uint64(101515932)
	largePrev         = uint64(128 << 10)
)

type captureManifest struct {
	Version         int                                 `json:"version"`
	SourceChaindata string                              `json:"source_chaindata"`
	CreatedUTC      string                              `json:"created_utc"`
	FinishBlock     uint64                              `json:"finish_block"`
	CoveredBlock    uint64                              `json:"covered_block"`
	Export          rawdb.StateHistoryRangeExportReport `json:"export"`
	Error           string                              `json:"error,omitempty"`
}

type rowStats struct {
	Rows                  uint64 `json:"rows"`
	PrevBytes             uint64 `json:"prev_bytes"`
	MaxPrevBytes          uint64 `json:"max_prev_bytes"`
	LargePrevRows         uint64 `json:"large_prev_rows_ge_128kib"`
	LargePrevBytes        uint64 `json:"large_prev_bytes_ge_128kib"`
	LargePrevDistinctKeys uint64 `json:"large_prev_distinct_logical_keys_ge_128kib"`
	DistinctKeys          uint64 `json:"distinct_logical_keys"`
	keys                  map[[32]byte]struct{}
	largeKeys             map[[32]byte]struct{}
}

type listStats struct {
	DecodedRows    uint64 `json:"decoded_prev_rows"`
	FromReferences uint64 `json:"summed_from_references"`
	ToReferences   uint64 `json:"summed_to_references"`
	MaxFrom        uint64 `json:"max_from_list_count"`
	MaxTo          uint64 `json:"max_to_list_count"`
	MaxCombined    uint64 `json:"max_combined_list_count"`
}

type distribution struct {
	Complete                    bool                 `json:"complete"`
	Error                       string               `json:"error,omitempty"`
	CapturePath                 string               `json:"capture_path"`
	ManifestSHA                 string               `json:"manifest_sha256"`
	PhysicalVerified            bool                 `json:"physical_verified"`
	ClosureVerified             bool                 `json:"closure_verified"`
	FromBlock                   uint64               `json:"from_block"`
	ToBlock                     uint64               `json:"to_block"`
	All                         rowStats             `json:"all"`
	SystemDelegation            rowStats             `json:"system_delegation"`
	Families                    map[string]*rowStats `json:"families"`
	LegacyLists                 listStats            `json:"legacy_lists_all_existing_prev"`
	LargeLegacyLists            listStats            `json:"legacy_lists_prev_ge_128kib"`
	SystemDelegationPrevPercent float64              `json:"system_delegation_prev_percent_of_all"`
	LegacyPrevPercent           float64              `json:"legacy_aggregate_prev_percent_of_all"`
	ListCountScope              string               `json:"list_count_scope"`
	ElapsedNanos                int64                `json:"elapsed_ns"`
}

var legacyPrefix = rawdb.DrAccountIndexLegacyStateKey(nil)
var directionalPrefixes = []struct {
	name   string
	prefix []byte
}{
	{"delegation_directional_v1_from", rawdb.DrAccountIndexAnchorStatePrefix(rawdb.DrAccIdxV1From, nil)},
	{"delegation_directional_v1_to", rawdb.DrAccountIndexAnchorStatePrefix(rawdb.DrAccIdxV1To, nil)},
	{"delegation_directional_v2_from", rawdb.DrAccountIndexAnchorStatePrefix(rawdb.DrAccIdxV2From, nil)},
	{"delegation_directional_v2_to", rawdb.DrAccountIndexAnchorStatePrefix(rawdb.DrAccIdxV2To, nil)},
}

func classify(row *rawdb.StateDomainChange) string {
	if row.FlatDomain != rawdb.StateFlatDomainKVLatest || row.Domain != kvdomains.SystemDelegation {
		return fmt.Sprintf("other_%s_domain_%d", row.FlatDomain.String(), row.Domain)
	}
	if bytes.HasPrefix(row.Key, legacyPrefix) {
		return "delegation_legacy_aggregate"
	}
	for _, item := range directionalPrefixes {
		if bytes.HasPrefix(row.Key, item.prefix) {
			return item.name
		}
	}
	return "delegation_other"
}

// Identity includes the complete logical namespace, not block/tx/version.
// The hashes are retained only for counting and never emitted.
func logicalKey(row *rawdb.StateDomainChange) [32]byte {
	h := sha256.New()
	var scalar [8]byte
	for _, n := range []uint64{uint64(row.FlatDomain), row.Generation, uint64(row.Domain)} {
		binary.BigEndian.PutUint64(scalar[:], n)
		_, _ = h.Write(scalar[:])
	}
	_, _ = h.Write(row.Owner[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(len(row.Key)))
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(row.Key)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (s *rowStats) add(row *rawdb.StateDomainChange, key [32]byte) {
	if s.keys == nil {
		s.keys = make(map[[32]byte]struct{})
	}
	s.keys[key] = struct{}{}
	s.DistinctKeys = uint64(len(s.keys))
	s.Rows++
	n := uint64(len(row.Prev))
	s.PrevBytes += n
	s.MaxPrevBytes = max(s.MaxPrevBytes, n)
	if n >= largePrev {
		if s.largeKeys == nil {
			s.largeKeys = make(map[[32]byte]struct{})
		}
		s.largeKeys[key] = struct{}{}
		s.LargePrevDistinctKeys = uint64(len(s.largeKeys))
		s.LargePrevRows++
		s.LargePrevBytes += n
	}
}

func (s *listStats) add(from, to uint64) {
	s.DecodedRows++
	s.FromReferences += from
	s.ToReferences += to
	s.MaxFrom = max(s.MaxFrom, from)
	s.MaxTo = max(s.MaxTo, to)
	s.MaxCombined = max(s.MaxCombined, from+to)
}

func (d *distribution) add(ctx context.Context, row *rawdb.StateDomainChange) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.New("nil history row")
	}
	if d.All.Rows >= expectedRows || uint64(len(row.Prev)) > expectedPrevBytes-d.All.PrevBytes {
		return false, errors.New("logical rows/Prev bytes exceed pinned capture totals")
	}
	family, key := classify(row), logicalKey(row)
	d.All.add(row, key)
	if d.Families == nil {
		d.Families = make(map[string]*rowStats)
	}
	if d.Families[family] == nil {
		d.Families[family] = new(rowStats)
	}
	d.Families[family].add(row, key)
	if row.FlatDomain == rawdb.StateFlatDomainKVLatest && row.Domain == kvdomains.SystemDelegation {
		d.SystemDelegation.add(row, key)
	}
	if family == "delegation_legacy_aggregate" && row.PrevExists {
		index, err := statecodec.UnmarshalDelegationIndex(row.Prev)
		if err != nil {
			return false, fmt.Errorf("decode legacy Prev at block %d tx %d seq %d: %w", row.BlockNum, row.TxNum, row.Seq, err)
		}
		from, to := uint64(len(index.FromAccounts)), uint64(len(index.ToAccounts))
		d.LegacyLists.add(from, to)
		if uint64(len(row.Prev)) >= largePrev {
			d.LargeLegacyLists.add(from, to)
		}
	}
	return ctx.Err() == nil, ctx.Err()
}

type discardWriter struct{}

func (discardWriter) Put(_, _ []byte) error { return nil }
func (discardWriter) Delete([]byte) error   { return errors.New("unexpected diagnostic delete") }

func readManifest(ctx context.Context) (out captureManifest, err error) {
	for _, path := range []string{capturePath, filepath.Join(capturePath, "pebble")} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return out, err
		}
		if resolved != path {
			return out, errors.New("private capture path resolves elsewhere")
		}
	}
	var files uint64
	if err := filepath.WalkDir(capturePath, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		files++
		if files > 65536 || entry.Type()&os.ModeSymlink != 0 || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return errors.New("private capture has unexpected links, special files or excessive file count")
		}
		return nil
	}); err != nil {
		return out, err
	}
	file, err := os.Open(filepath.Join(capturePath, "manifest.json"))
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	data, err := io.ReadAll(io.LimitReader(file, (64<<20)+1))
	if err != nil {
		return out, err
	}
	if len(data) > 64<<20 {
		return out, errors.New("capture manifest too large")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != manifestSHA {
		return out, errors.New("pinned manifest SHA mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return out, errors.New("manifest has trailing JSON")
	}
	e := out.Export
	if out.Version != 1 || out.Error != "" || !e.Complete || e.ContentVerified || e.Error != "" || e.StopReason != "complete" ||
		e.Blocks != 16 || e.FromBlock > e.ToBlock || e.ToBlock-e.FromBlock != 15 || e.FromTxNum > e.ToTxNum ||
		e.FromBlock <= out.CoveredBlock || e.ToBlock > out.FinishBlock ||
		e.PhysicalRows == 0 || e.PhysicalRows != uint64(len(e.Entries)) || e.PhysicalRows > rawdb.StateHistoryRangeExportMaxRows ||
		e.PhysicalBytes == 0 || e.PhysicalBytes > rawdb.StateHistoryRangeExportMaxBytes || e.DeclaredDecodedBytes > rawdb.StateHistoryRangeExportMaxDecodedBytes {
		return out, errors.New("invalid pinned capture manifest")
	}
	return out, ctx.Err()
}

func verifyPhysical(ctx context.Context, view rawdb.StateHistoryReadView, expected rawdb.StateHistoryRangeExportReport) error {
	it := view.NewIterator(nil, nil)
	defer it.Release()
	var rows, physical uint64
	var previous []byte
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rows >= uint64(len(expected.Entries)) {
			return errors.New("extra physical row")
		}
		e := expected.Entries[rows]
		key, err := hex.DecodeString(e.KeyHex)
		if err != nil || len(key) == 0 || (previous != nil && bytes.Compare(previous, key) >= 0) || !bytes.Equal(key, it.Key()) {
			return fmt.Errorf("physical key mismatch at ordinal %d", rows)
		}
		want, err := hex.DecodeString(e.ValueSHA256)
		sum := sha256.Sum256(it.Value())
		if err != nil || len(want) != sha256.Size || uint64(len(it.Value())) != e.ValueBytes || !bytes.Equal(want, sum[:]) {
			return fmt.Errorf("physical value mismatch at ordinal %d", rows)
		}
		physical += uint64(len(key)) + e.ValueBytes
		if physical > rawdb.StateHistoryRangeExportMaxBytes {
			return errors.New("physical input budget exceeded")
		}
		previous, rows = key, rows+1
	}
	if err := it.Error(); err != nil {
		return err
	}
	if rows != expected.PhysicalRows || physical != expected.PhysicalBytes {
		return errors.New("physical totals mismatch")
	}
	replayed, err := rawdb.ExportStateHistoryRange(ctx, view, discardWriter{}, rawdb.StateHistoryRangeExportOptions{
		FromBlock: expected.FromBlock, ToBlock: expected.ToBlock, MaxBytes: rawdb.StateHistoryRangeExportMaxBytes,
		MaxRows: rawdb.StateHistoryRangeExportMaxRows, MaxDecodedBytes: rawdb.StateHistoryRangeExportMaxDecodedBytes})
	if err != nil {
		return err
	}
	if !replayed.Complete || !reflect.DeepEqual(replayed, expected) {
		return errors.New("full physical re-export report mismatch")
	}
	return ctx.Err()
}

func probe(ctx context.Context) (out distribution, err error) {
	start := time.Now()
	out.CapturePath, out.ManifestSHA = capturePath, manifestSHA
	out.ListCountScope = "List references are summed per historical Prev, including repeated versions; distinct keys include full logical namespace. No addresses or key hashes are emitted."
	defer func() { out.ElapsedNanos = time.Since(start).Nanoseconds() }()
	manifest, err := readManifest(ctx)
	if err != nil {
		return out, err
	}
	db, err := rawdb.NewPebbleDBReadOnly(filepath.Join(capturePath, "pebble"), 64, 128)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, release()) }()
	if !view.IsPinnedKeyValueView() {
		return out, rawdb.ErrStateHistoryReadViewUnpinned
	}
	e := manifest.Export
	if err := verifyPhysical(ctx, view, e); err != nil {
		return out, err
	}
	out.PhysicalVerified, out.ClosureVerified = true, true
	out.FromBlock, out.ToBlock = e.FromBlock, e.ToBlock
	err = rawdb.IterateStateDomainChangesByBlockTxRangeBorrowed(view, e.FromBlock, e.ToBlock, e.FromTxNum, e.ToTxNum,
		func(row *rawdb.StateDomainChange) (bool, error) { return out.add(ctx, row) })
	if err != nil {
		return out, err
	}
	if out.All.Rows != expectedRows || out.All.PrevBytes != expectedPrevBytes {
		return out, errors.New("decoded totals differ from authenticated prior source digest")
	}
	out.SystemDelegationPrevPercent = 100 * float64(out.SystemDelegation.PrevBytes) / float64(out.All.PrevBytes)
	if legacy := out.Families["delegation_legacy_aggregate"]; legacy != nil {
		out.LegacyPrevPercent = 100 * float64(legacy.PrevBytes) / float64(out.All.PrevBytes)
	}
	return out, ctx.Err()
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := probe(ctx)
	out.Complete = err == nil
	if err != nil {
		out.Error = err.Error()
	}
	if encodeErr := json.NewEncoder(os.Stdout).Encode(out); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	if err != nil {
		os.Exit(1)
	}
}
