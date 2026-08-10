package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

const (
	stateChangeIndexCurrentPrefix = "state-change-index-v2-"
	stateChangeIndexHashPrefix    = "state-change-index-v3-"
	stateChangePostingPrefix      = "state-change-posting-v1-"
	benchmarkDataBlockSize        = 4096
	benchmarkRestartInterval      = 16
)

var (
	dbBenchmarkStateChangeFamilyFlag = &cli.StringFlag{
		Name:  "family",
		Usage: "Index family to scan: kv-latest, account-latest, kv-generation, or all",
		Value: string(rawdb.StateChangeIndexKVLatest),
	}
	dbBenchmarkStateChangeMaxRowsFlag = &cli.Uint64Flag{
		Name:  "max-rows",
		Usage: "Stop after this many rows (0 scans the selected family completely; limited results are not full projections)",
	}
	dbBenchmarkStateChangePostingRowsFlag = &cli.StringFlag{
		Name:  "posting-rows",
		Usage: "Comma-separated maximum block postings per immutable segment",
		Value: "32,64,128,256,512,1024",
	}
)

type stateChangeIndexBenchmarkOutput struct {
	ChaindataPath              string                               `json:"chaindata_path"`
	Family                     rawdb.StateChangeIndexFamily         `json:"family"`
	Complete                   bool                                 `json:"complete"`
	MaxRows                    uint64                               `json:"max_rows,omitempty"`
	Rows                       uint64                               `json:"rows"`
	UniqueLatestKeys           uint64                               `json:"unique_latest_keys"`
	AverageBlocksPerKey        float64                              `json:"average_blocks_per_key"`
	MaxBlocksPerKey            uint64                               `json:"max_blocks_per_key"`
	CurrentLogicalBytes        uint64                               `json:"current_logical_bytes"`
	CurrentAverageRowBytes     float64                              `json:"current_average_row_bytes"`
	DataBlockSize              uint64                               `json:"simulated_data_block_size"`
	RestartInterval            uint64                               `json:"simulated_restart_interval"`
	ExpectedHashCollisionPairs float64                              `json:"expected_hash256_collision_pairs"`
	GroupDistribution          []stateChangeIndexGroupDistribution  `json:"group_distribution"`
	Candidates                 []stateChangeIndexBenchmarkCandidate `json:"candidates"`
	ElapsedSeconds             float64                              `json:"elapsed_seconds"`
}

type stateChangeIndexGroupDistribution struct {
	Name string `json:"name"`
	Keys uint64 `json:"keys"`
}

type stateChangeIndexBenchmarkCandidate struct {
	Name                            string  `json:"name"`
	PostingRows                     uint64  `json:"posting_rows,omitempty"`
	PhysicalRows                    uint64  `json:"physical_rows"`
	KeyBytes                        uint64  `json:"key_bytes"`
	ValueBytes                      uint64  `json:"value_bytes"`
	LogicalBytes                    uint64  `json:"logical_bytes"`
	EstimatedDataBlockBytes         uint64  `json:"estimated_data_block_bytes_no_compression"`
	LogicalSavingsPercent           float64 `json:"logical_savings_percent"`
	EstimatedDataBlockSavings       float64 `json:"estimated_data_block_savings_percent"`
	SupportsExactKeyHistory         bool    `json:"supports_exact_key_history"`
	SupportsLogicalPrefixHistory    bool    `json:"supports_logical_prefix_history"`
	RequiresChangesetCollisionCheck bool    `json:"requires_changeset_collision_check"`
}

func dbBenchmarkStateChangeIndexCommand() *cli.Command {
	return &cli.Command{
		Name:        "benchmark-state-change-index",
		Usage:       "Measure hashed-row and posting-list layouts against hot state history",
		Description: "The node using this datadir must be stopped. The command opens chaindata read-only and sequentially scans the selected state-change-index family without modifying it. A complete scan gives exact logical and streaming prefix-encoded data-block estimates.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbBenchmarkStateChangeFamilyFlag,
			dbBenchmarkStateChangeMaxRowsFlag,
			dbBenchmarkStateChangePostingRowsFlag,
			dbInspectProgressFlag,
			dbBenchmarkJSONFlag,
		},
		Action: dbBenchmarkStateChangeIndexCmd,
	}
}

func dbBenchmarkStateChangeIndexCmd(ctx *cli.Context) error {
	family := rawdb.StateChangeIndexFamily(strings.TrimSpace(ctx.String("family")))
	if !validStateChangeIndexFamily(family) {
		return fmt.Errorf("--family must be one of kv-latest, account-latest, kv-generation, all")
	}
	postingRows, err := parseStateChangePostingRows(ctx.String("posting-rows"))
	if err != nil {
		return err
	}
	if ctx.Duration("progress") < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	cache := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value)
	handles := intFlagOrDefault(ctx, "db.handles", dbHandlesFlag.Value)
	if cache <= 0 || handles <= 0 {
		return fmt.Errorf("--db.cache and --db.handles must be positive")
	}

	path := chainDataDir(ctx.String("datadir"))
	db, err := rawdb.NewPebbleDBReadOnly(path, cache, handles)
	if err != nil {
		return fmt.Errorf("open chaindata read-only %q (stop gtron before benchmarking): %w", path, err)
	}
	defer db.Close()
	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	started := time.Now()
	progressAt := started.Add(ctx.Duration("progress"))
	acc := newStateChangeIndexBenchmarkAccumulator(postingRows)
	result, err := rawdb.IterateStateChangeIndexRows(db, rawdb.StateChangeIndexScanOptions{
		Family:  family,
		MaxRows: ctx.Uint64("max-rows"),
	}, func(row rawdb.StateChangeIndexRow) (bool, error) {
		acc.Add(row)
		if interval := ctx.Duration("progress"); interval > 0 && !time.Now().Before(progressAt) {
			elapsed := time.Since(started)
			rate := float64(acc.rows) / max(elapsed.Seconds(), 0.001)
			fmt.Fprintf(errWriter, "benchmarked state change index family=%s rows=%d keys=%d logical=%s rate=%.0f rows/s elapsed=%s\n",
				family, acc.rows, acc.uniqueKeys, formatIEC(acc.currentLogicalBytes), rate, elapsed.Round(time.Second))
			progressAt = time.Now().Add(interval)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if result.Rows == 0 {
		return fmt.Errorf("chaindata %q contains no state change index rows for family %q", path, family)
	}
	acc.Finish()
	output := acc.Output(path, family, result.Complete, ctx.Uint64("max-rows"), time.Since(started))

	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	writeStateChangeIndexBenchmarkText(writer, output)
	return nil
}

func validStateChangeIndexFamily(family rawdb.StateChangeIndexFamily) bool {
	switch family {
	case rawdb.StateChangeIndexKVLatest, rawdb.StateChangeIndexAccountLatest, rawdb.StateChangeIndexKVGeneration, rawdb.StateChangeIndexAll:
		return true
	default:
		return false
	}
}

func parseStateChangePostingRows(value string) ([]uint64, error) {
	seen := make(map[uint64]struct{})
	var out []uint64
	for _, field := range strings.Split(value, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		rows, err := strconv.ParseUint(field, 10, 64)
		if err != nil || rows == 0 || rows > 1<<20 {
			return nil, fmt.Errorf("invalid --posting-rows value %q (want integers in [1,1048576])", field)
		}
		if _, ok := seen[rows]; ok {
			continue
		}
		seen[rows] = struct{}{}
		out = append(out, rows)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--posting-rows must contain at least one positive value")
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

type stateChangeIndexBenchmarkAccumulator struct {
	rows                uint64
	uniqueKeys          uint64
	currentLogicalBytes uint64
	currentKeyBytes     uint64
	currentValueBytes   uint64
	maxBlocksPerKey     uint64
	currentLatestKey    []byte
	currentGroupRows    uint64
	previousBlock       uint64
	distribution        [7]uint64
	currentBlocks       prefixEncodedDataBlockEstimator
	hashRows            stateChangeIndexCandidateAccumulator
	postings            []*stateChangePostingAccumulator
}

func newStateChangeIndexBenchmarkAccumulator(postingRows []uint64) *stateChangeIndexBenchmarkAccumulator {
	acc := &stateChangeIndexBenchmarkAccumulator{
		currentBlocks: newPrefixEncodedDataBlockEstimator(),
		hashRows: stateChangeIndexCandidateAccumulator{
			name:           "hash256-row-v3",
			exact:          true,
			collisionCheck: true,
			estimator:      newPrefixEncodedDataBlockEstimator(),
		},
	}
	for _, rows := range postingRows {
		acc.postings = append(acc.postings, &stateChangePostingAccumulator{
			frameRows: rows,
			candidate: stateChangeIndexCandidateAccumulator{
				name:           fmt.Sprintf("hash256-posting-%d", rows),
				exact:          true,
				collisionCheck: true,
				estimator:      newPrefixEncodedDataBlockEstimator(),
			},
		})
	}
	return acc
}

func (a *stateChangeIndexBenchmarkAccumulator) Add(row rawdb.StateChangeIndexRow) {
	newGroup := len(a.currentLatestKey) == 0 || !bytes.Equal(a.currentLatestKey, row.LatestKey)
	var currentShared int
	if newGroup {
		if len(a.currentLatestKey) != 0 {
			a.finishGroup()
			currentShared = len(stateChangeIndexCurrentPrefix) + commonPrefixLen(a.currentLatestKey, row.LatestKey)
		}
		a.currentLatestKey = append(a.currentLatestKey[:0], row.LatestKey...)
		a.currentGroupRows = 0
		a.uniqueKeys++
	} else {
		currentShared = len(stateChangeIndexCurrentPrefix) + len(row.LatestKey) + commonUint64Prefix(a.previousBlock, row.BlockNum)
	}
	keyBytes := uint64(len(row.PhysicalKey))
	valueBytes := uint64(max(row.ValueBytes, 0))
	a.rows++
	a.currentGroupRows++
	a.currentKeyBytes += keyBytes
	a.currentValueBytes += valueBytes
	a.currentLogicalBytes += keyBytes + valueBytes
	a.currentBlocks.Add(len(row.PhysicalKey), row.ValueBytes, currentShared)

	hashShared := len(stateChangeIndexHashPrefix)
	if !newGroup {
		hashShared += 32 + commonUint64Prefix(a.previousBlock, row.BlockNum)
	}
	a.hashRows.Add(len(stateChangeIndexHashPrefix)+32+8, 0, hashShared)
	for _, posting := range a.postings {
		posting.Add(row.BlockNum, newGroup)
	}
	a.previousBlock = row.BlockNum
}

func (a *stateChangeIndexBenchmarkAccumulator) finishGroup() {
	if a.currentGroupRows == 0 {
		return
	}
	a.distribution[stateChangeIndexGroupBucket(a.currentGroupRows)]++
	a.maxBlocksPerKey = max(a.maxBlocksPerKey, a.currentGroupRows)
	for _, posting := range a.postings {
		posting.FinishGroup()
	}
}

func (a *stateChangeIndexBenchmarkAccumulator) Finish() {
	a.finishGroup()
	a.currentBlocks.Finish()
	a.hashRows.Finish()
	for _, posting := range a.postings {
		posting.Finish()
	}
}

func (a *stateChangeIndexBenchmarkAccumulator) Output(path string, family rawdb.StateChangeIndexFamily, complete bool, maxRows uint64, elapsed time.Duration) stateChangeIndexBenchmarkOutput {
	out := stateChangeIndexBenchmarkOutput{
		ChaindataPath:       path,
		Family:              family,
		Complete:            complete,
		MaxRows:             maxRows,
		Rows:                a.rows,
		UniqueLatestKeys:    a.uniqueKeys,
		MaxBlocksPerKey:     a.maxBlocksPerKey,
		CurrentLogicalBytes: a.currentLogicalBytes,
		DataBlockSize:       benchmarkDataBlockSize,
		RestartInterval:     benchmarkRestartInterval,
		ElapsedSeconds:      elapsed.Seconds(),
		GroupDistribution:   stateChangeIndexGroupDistributionRows(a.distribution),
	}
	if a.rows != 0 {
		out.CurrentAverageRowBytes = float64(a.currentLogicalBytes) / float64(a.rows)
	}
	if a.uniqueKeys != 0 {
		out.AverageBlocksPerKey = float64(a.rows) / float64(a.uniqueKeys)
		out.ExpectedHashCollisionPairs = expectedHash256CollisionPairs(a.uniqueKeys)
	}
	current := stateChangeIndexBenchmarkCandidate{
		Name:                         "current-row-v2",
		PhysicalRows:                 a.rows,
		KeyBytes:                     a.currentKeyBytes,
		ValueBytes:                   a.currentValueBytes,
		LogicalBytes:                 a.currentLogicalBytes,
		EstimatedDataBlockBytes:      a.currentBlocks.Total(),
		SupportsExactKeyHistory:      true,
		SupportsLogicalPrefixHistory: true,
	}
	out.Candidates = append(out.Candidates, current)
	out.Candidates = append(out.Candidates, a.hashRows.Output(current, 0))
	for _, posting := range a.postings {
		out.Candidates = append(out.Candidates, posting.candidate.Output(current, posting.frameRows))
	}
	return out
}

type stateChangeIndexCandidateAccumulator struct {
	name           string
	rows           uint64
	keyBytes       uint64
	valueBytes     uint64
	exact          bool
	prefix         bool
	collisionCheck bool
	estimator      prefixEncodedDataBlockEstimator
}

func (c *stateChangeIndexCandidateAccumulator) Add(keyBytes, valueBytes, shared int) {
	c.rows++
	c.keyBytes += uint64(keyBytes)
	c.valueBytes += uint64(valueBytes)
	c.estimator.Add(keyBytes, valueBytes, shared)
}

func (c *stateChangeIndexCandidateAccumulator) Finish() {
	c.estimator.Finish()
}

func (c *stateChangeIndexCandidateAccumulator) Output(current stateChangeIndexBenchmarkCandidate, postingRows uint64) stateChangeIndexBenchmarkCandidate {
	logical := c.keyBytes + c.valueBytes
	estimated := c.estimator.Total()
	return stateChangeIndexBenchmarkCandidate{
		Name:                            c.name,
		PostingRows:                     postingRows,
		PhysicalRows:                    c.rows,
		KeyBytes:                        c.keyBytes,
		ValueBytes:                      c.valueBytes,
		LogicalBytes:                    logical,
		EstimatedDataBlockBytes:         estimated,
		LogicalSavingsPercent:           savingsPercent(current.LogicalBytes, logical),
		EstimatedDataBlockSavings:       savingsPercent(current.EstimatedDataBlockBytes, estimated),
		SupportsExactKeyHistory:         c.exact,
		SupportsLogicalPrefixHistory:    c.prefix,
		RequiresChangesetCollisionCheck: c.collisionCheck,
	}
}

type stateChangePostingAccumulator struct {
	frameRows       uint64
	count           uint64
	startBlock      uint64
	previousBlock   uint64
	deltaBytes      uint64
	segmentsInGroup uint64
	previousStart   uint64
	candidate       stateChangeIndexCandidateAccumulator
}

func (p *stateChangePostingAccumulator) Add(blockNum uint64, newGroup bool) {
	if newGroup {
		p.FinishGroup()
	}
	if p.count == 0 {
		p.startBlock = blockNum
		p.previousBlock = blockNum
		p.count = 1
		return
	}
	p.deltaBytes += uint64(uvarintSize(blockNum - p.previousBlock))
	p.previousBlock = blockNum
	p.count++
	if p.count >= p.frameRows {
		p.emit()
	}
}

func (p *stateChangePostingAccumulator) FinishGroup() {
	if p.count != 0 {
		p.emit()
	}
	p.segmentsInGroup = 0
	p.previousStart = 0
}

func (p *stateChangePostingAccumulator) Finish() {
	p.FinishGroup()
	p.candidate.Finish()
}

func (p *stateChangePostingAccumulator) emit() {
	if p.count == 0 {
		return
	}
	valueBytes := uvarintSize(p.count) + int(p.deltaBytes)
	shared := len(stateChangePostingPrefix)
	if p.segmentsInGroup != 0 {
		shared += 32 + commonUint64Prefix(p.previousStart, p.startBlock)
	}
	p.candidate.Add(len(stateChangePostingPrefix)+32+8, valueBytes, shared)
	p.previousStart = p.startBlock
	p.segmentsInGroup++
	p.count = 0
	p.deltaBytes = 0
}

type prefixEncodedDataBlockEstimator struct {
	blockBytes int
	entries    int
	restarts   int
	total      uint64
}

func newPrefixEncodedDataBlockEstimator() prefixEncodedDataBlockEstimator {
	return prefixEncodedDataBlockEstimator{}
}

func (e *prefixEncodedDataBlockEstimator) Add(keyBytes, valueBytes, possibleShared int) {
	shared := possibleShared
	if e.entries%benchmarkRestartInterval == 0 {
		shared = 0
	}
	shared = min(max(shared, 0), keyBytes)
	entryBytes := prefixEncodedEntrySize(shared, keyBytes-shared, valueBytes)
	threshold := benchmarkDataBlockSize * 90 / 100
	if e.entries != 0 && e.blockBytes >= threshold && e.blockBytes+entryBytes > benchmarkDataBlockSize {
		e.flush()
		shared = 0
		entryBytes = prefixEncodedEntrySize(0, keyBytes, valueBytes)
	}
	if e.entries%benchmarkRestartInterval == 0 {
		e.restarts++
	}
	e.blockBytes += entryBytes
	e.entries++
}

func (e *prefixEncodedDataBlockEstimator) Finish() {
	e.flush()
}

func (e *prefixEncodedDataBlockEstimator) Total() uint64 {
	return e.total
}

func (e *prefixEncodedDataBlockEstimator) flush() {
	if e.entries == 0 {
		return
	}
	e.total += uint64(e.blockBytes + e.restarts*4 + 4)
	e.blockBytes = 0
	e.entries = 0
	e.restarts = 0
}

func prefixEncodedEntrySize(shared, unshared, value int) int {
	return uvarintSize(uint64(shared)) + uvarintSize(uint64(unshared)) + uvarintSize(uint64(max(value, 0))) + unshared + max(value, 0)
}

func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func commonUint64Prefix(a, b uint64) int {
	var aa, bb [8]byte
	binary.BigEndian.PutUint64(aa[:], a)
	binary.BigEndian.PutUint64(bb[:], b)
	return commonPrefixLen(aa[:], bb[:])
}

func uvarintSize(value uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], value)
}

func stateChangeIndexGroupBucket(rows uint64) int {
	switch {
	case rows == 1:
		return 0
	case rows <= 3:
		return 1
	case rows <= 15:
		return 2
	case rows <= 63:
		return 3
	case rows <= 255:
		return 4
	case rows <= 1023:
		return 5
	default:
		return 6
	}
}

func stateChangeIndexGroupDistributionRows(counts [7]uint64) []stateChangeIndexGroupDistribution {
	names := [...]string{"1", "2-3", "4-15", "16-63", "64-255", "256-1023", "1024+"}
	rows := make([]stateChangeIndexGroupDistribution, len(names))
	for i, name := range names {
		rows[i] = stateChangeIndexGroupDistribution{Name: name, Keys: counts[i]}
	}
	return rows
}

func expectedHash256CollisionPairs(keys uint64) float64 {
	if keys < 2 {
		return 0
	}
	n := float64(keys)
	return math.Ldexp(n*(n-1), -257)
}

func savingsPercent(before, after uint64) float64 {
	if before == 0 {
		return 0
	}
	return (1 - float64(after)/float64(before)) * 100
}

func writeStateChangeIndexBenchmarkText(writer io.Writer, output stateChangeIndexBenchmarkOutput) {
	fmt.Fprintf(writer, "Chaindata: %s\n", output.ChaindataPath)
	fmt.Fprintf(writer, "Family: %s complete=%t rows=%d unique_keys=%d avg_blocks_per_key=%.2f max_blocks_per_key=%d elapsed=%.3fs\n",
		output.Family, output.Complete, output.Rows, output.UniqueLatestKeys, output.AverageBlocksPerKey, output.MaxBlocksPerKey, output.ElapsedSeconds)
	tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "CANDIDATE\tROWS\tLOGICAL\tDATA_BLOCK_EST\tLOGICAL_SAVE\tBLOCK_SAVE\tPREFIX_HISTORY")
	for _, candidate := range output.Candidates {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%.2f%%\t%.2f%%\t%t\n",
			candidate.Name, candidate.PhysicalRows, formatIEC(candidate.LogicalBytes), formatIEC(candidate.EstimatedDataBlockBytes),
			candidate.LogicalSavingsPercent, candidate.EstimatedDataBlockSavings, candidate.SupportsLogicalPrefixHistory)
	}
	_ = tw.Flush()
}
