package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

const (
	defaultTxIndexBenchmarkSamples    = uint64(1_000_000)
	defaultTxIndexBenchmarkWindows    = uint64(256)
	defaultTxIndexBenchmarkPrefixBits = uint64(20)
	defaultTxIndexBenchmarkLookups    = uint64(1_000_000)
	currentTxIndexRowBytes            = uint64(3 + 32 + 8)
)

var (
	dbBenchmarkTxIndexSamplesFlag = &cli.Uint64Flag{
		Name:  "sample-transactions",
		Usage: "Maximum tx-* rows sampled across the transaction-hash space",
		Value: defaultTxIndexBenchmarkSamples,
	}
	dbBenchmarkTxIndexWindowsFlag = &cli.Uint64Flag{
		Name:  "windows",
		Usage: "Number of evenly distributed transaction-hash windows",
		Value: defaultTxIndexBenchmarkWindows,
	}
	dbBenchmarkTxIndexPrefixBitsFlag = &cli.Uint64Flag{
		Name:  "prefix-bits",
		Usage: "High transaction-hash bits used by the immutable bucket directory (8-24)",
		Value: defaultTxIndexBenchmarkPrefixBits,
	}
	dbBenchmarkTxIndexLookupsFlag = &cli.Uint64Flag{
		Name:  "lookups",
		Usage: "Successful and unsuccessful in-memory lookups to benchmark (0 disables)",
		Value: defaultTxIndexBenchmarkLookups,
	}
)

type txIndexBenchmarkOutput struct {
	ChaindataPath          string                      `json:"chaindata_path"`
	TotalTransactionCount  uint64                      `json:"total_transaction_count"`
	ProjectedRows          uint64                      `json:"projected_rows"`
	ProjectionUsesCounter  bool                        `json:"projection_uses_total_transaction_count"`
	SampleTransactions     uint64                      `json:"sample_transactions"`
	Windows                uint64                      `json:"windows"`
	PrefixBits             uint64                      `json:"prefix_bits"`
	DirectoryBytes         uint64                      `json:"directory_bytes"`
	SampleMaxBucketRows    uint64                      `json:"sample_max_bucket_rows"`
	ProjectedAvgBucketRows float64                     `json:"projected_average_bucket_rows"`
	ElapsedSeconds         float64                     `json:"elapsed_seconds"`
	Candidates             []txIndexBenchmarkCandidate `json:"candidates"`
	Lookup                 txIndexLookupBenchmark      `json:"lookup"`
}

type txIndexBenchmarkCandidate struct {
	Name                     string  `json:"name"`
	FingerprintBits          uint64  `json:"fingerprint_bits,omitempty"`
	EntryBytes               uint64  `json:"entry_bytes"`
	DirectoryBytes           uint64  `json:"directory_bytes"`
	BucketHeaderBytes        uint64  `json:"bucket_header_bytes,omitempty"`
	ProjectedBytes           uint64  `json:"projected_bytes"`
	SavingsPercent           float64 `json:"savings_percent"`
	ObservedSampleCollisions uint64  `json:"observed_sample_collision_pairs,omitempty"`
	ExpectedCollisionPairs   float64 `json:"expected_collision_pairs,omitempty"`
	FalseCandidateRate       float64 `json:"false_candidate_probability_per_lookup,omitempty"`
}

type txIndexLookupBenchmark struct {
	Lookups                    uint64  `json:"lookups"`
	PositiveNanosecondsPerOp   float64 `json:"positive_nanoseconds_per_op"`
	NegativeNanosecondsPerOp   float64 `json:"negative_nanoseconds_per_op"`
	NegativeFingerprintMatches uint64  `json:"negative_fingerprint_matches"`
}

func dbBenchmarkTxIndexCommand() *cli.Command {
	return &cli.Command{
		Name:        "benchmark-tx-index",
		Usage:       "Measure compact immutable layouts against offline tx-* data",
		Description: "The node using this datadir must be stopped. The command opens chaindata read-only, seeks into evenly distributed transaction-hash windows, and does not modify database or ancient files.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbBenchmarkTxIndexSamplesFlag,
			dbBenchmarkTxIndexWindowsFlag,
			dbBenchmarkTxIndexPrefixBitsFlag,
			dbBenchmarkTxIndexLookupsFlag,
			dbInspectProgressFlag,
			dbBenchmarkJSONFlag,
		},
		Action: dbBenchmarkTxIndexCmd,
	}
}

func dbBenchmarkTxIndexCmd(ctx *cli.Context) error {
	if ctx.Uint64("sample-transactions") == 0 {
		return fmt.Errorf("--sample-transactions must be positive")
	}
	if ctx.Uint64("sample-transactions") > math.MaxUint32 {
		return fmt.Errorf("--sample-transactions must not exceed %d", uint64(math.MaxUint32))
	}
	if ctx.Uint64("windows") == 0 || ctx.Uint64("windows") > 1<<16 {
		return fmt.Errorf("--windows must be in [1,65536]")
	}
	prefixBits := ctx.Uint64("prefix-bits")
	if prefixBits < 8 || prefixBits > 24 {
		return fmt.Errorf("--prefix-bits must be in [8,24]")
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
	samples, err := rawdb.SampleTransactionIndexes(db, rawdb.TransactionIndexSampleOptions{
		Rows:             ctx.Uint64("sample-transactions"),
		Windows:          ctx.Uint64("windows"),
		ProgressInterval: ctx.Duration("progress"),
		Progress: func(progress rawdb.TransactionIndexSampleProgress) {
			fmt.Fprintf(errWriter, "sampled tx indexes rows=%d windows=%d elapsed=%s\n",
				progress.Rows, progress.Windows, progress.Elapsed.Round(time.Second))
		},
	})
	if err != nil {
		return err
	}
	if len(samples) == 0 {
		return fmt.Errorf("chaindata %q contains no tx-* rows", path)
	}

	totalCount := rawdb.ReadTotalTransactionCount(db)
	projectedRows := uint64(len(samples))
	usesCounter := false
	if totalCount > 0 {
		projectedRows = uint64(totalCount)
		usesCounter = true
	}
	table := buildTxIndexBenchmarkTable(samples, uint(prefixBits))
	directoryBytes := (uint64(1)<<prefixBits + 1) * 8
	output := txIndexBenchmarkOutput{
		ChaindataPath:          path,
		TotalTransactionCount:  uint64(max(totalCount, 0)),
		ProjectedRows:          projectedRows,
		ProjectionUsesCounter:  usesCounter,
		SampleTransactions:     uint64(len(samples)),
		Windows:                ctx.Uint64("windows"),
		PrefixBits:             prefixBits,
		DirectoryBytes:         directoryBytes,
		SampleMaxBucketRows:    table.maxBucketRows,
		ProjectedAvgBucketRows: float64(projectedRows) / float64(uint64(1)<<prefixBits),
	}
	output.Candidates = projectTxIndexCandidates(projectedRows, prefixBits, directoryBytes, table.collisionPairs)
	output.Lookup = benchmarkTxIndexLookups(table, samples, ctx.Uint64("lookups"))
	output.ElapsedSeconds = time.Since(started).Seconds()

	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	writeTxIndexBenchmarkText(writer, output)
	return nil
}

type txIndexBenchmarkTable struct {
	prefixBits     uint
	offsets        []uint32
	fingerprints   []uint64
	locations      []uint64
	maxBucketRows  uint64
	collisionPairs uint64
}

func buildTxIndexBenchmarkTable(samples []rawdb.TransactionIndexSample, prefixBits uint) txIndexBenchmarkTable {
	buckets := 1 << prefixBits
	table := txIndexBenchmarkTable{
		prefixBits:   prefixBits,
		offsets:      make([]uint32, buckets+1),
		fingerprints: make([]uint64, len(samples)),
		locations:    make([]uint64, len(samples)),
	}
	for _, sample := range samples {
		prefix, _ := txIndexPrefixFingerprint(sample.Hash, prefixBits)
		table.offsets[prefix+1]++
	}
	for i := 1; i < len(table.offsets); i++ {
		table.offsets[i] += table.offsets[i-1]
		rows := uint64(table.offsets[i] - table.offsets[i-1])
		if rows > table.maxBucketRows {
			table.maxBucketRows = rows
		}
	}
	next := append([]uint32(nil), table.offsets[:len(table.offsets)-1]...)
	for _, sample := range samples {
		prefix, fingerprint := txIndexPrefixFingerprint(sample.Hash, prefixBits)
		index := next[prefix]
		table.fingerprints[index] = fingerprint
		table.locations[index] = sample.Location
		next[prefix]++
	}
	for bucket := 0; bucket < buckets; bucket++ {
		start, end := table.offsets[bucket], table.offsets[bucket+1]
		for i := start + 1; i < end; i++ {
			if table.fingerprints[i] == table.fingerprints[i-1] {
				table.collisionPairs++
			}
		}
	}
	return table
}

func txIndexPrefixFingerprint(hash [32]byte, prefixBits uint) (uint32, uint64) {
	high := binary.BigEndian.Uint64(hash[:8])
	next := binary.BigEndian.Uint64(hash[8:16])
	prefix := uint32(high >> (64 - prefixBits))
	fingerprint := high<<prefixBits | next>>(64-prefixBits)
	return prefix, fingerprint
}

func (table txIndexBenchmarkTable) lookup(hash [32]byte) (start, end uint32) {
	prefix, fingerprint := txIndexPrefixFingerprint(hash, table.prefixBits)
	lo, hi := table.offsets[prefix], table.offsets[prefix+1]
	length := int(hi - lo)
	first := sort.Search(length, func(i int) bool { return table.fingerprints[int(lo)+i] >= fingerprint })
	start = lo + uint32(first)
	end = start
	for end < hi && table.fingerprints[end] == fingerprint {
		end++
	}
	return start, end
}

func benchmarkTxIndexLookups(table txIndexBenchmarkTable, samples []rawdb.TransactionIndexSample, lookups uint64) txIndexLookupBenchmark {
	report := txIndexLookupBenchmark{Lookups: lookups}
	if lookups == 0 || len(samples) == 0 {
		return report
	}
	started := time.Now()
	var positive uint64
	for i := uint64(0); i < lookups; i++ {
		sample := samples[(i*2_654_435_761)%uint64(len(samples))]
		start, end := table.lookup(sample.Hash)
		for candidate := start; candidate < end; candidate++ {
			if table.locations[candidate] == sample.Location {
				positive++
				break
			}
		}
	}
	report.PositiveNanosecondsPerOp = float64(time.Since(started).Nanoseconds()) / float64(lookups)

	started = time.Now()
	var negativeMatches uint64
	for i := uint64(0); i < lookups; i++ {
		hash := samples[(i*2_654_435_761)%uint64(len(samples))].Hash
		// Byte 9 is wholly inside the 64-bit fingerprint for every supported
		// prefix width. Changing it preserves the bucket but produces a stable
		// absent-key workload without paying for a hash inside the timed loop.
		hash[9] ^= 0x80
		start, end := table.lookup(hash)
		negativeMatches += uint64(end - start)
	}
	report.NegativeNanosecondsPerOp = float64(time.Since(started).Nanoseconds()) / float64(lookups)
	report.NegativeFingerprintMatches = negativeMatches
	runtime.KeepAlive(positive)
	return report
}

func projectTxIndexCandidates(rows, prefixBits, directoryBytes, observedCollisions uint64) []txIndexBenchmarkCandidate {
	currentBytes := rows * currentTxIndexRowBytes
	buckets := uint64(1) << prefixBits
	expectedNonEmpty := uint64(math.Ceil(float64(buckets) * -math.Expm1(-float64(rows)/float64(buckets))))
	bucketHeaderBytes := expectedNonEmpty * 16
	const fileHeaderBytes = uint64(128)
	makeCandidate := func(name string, fingerprintBits, entryBytes, directory, bucketHeaders uint64) txIndexBenchmarkCandidate {
		projected := rows*entryBytes + directory + bucketHeaders
		if directory > 0 {
			projected += fileHeaderBytes
		}
		candidate := txIndexBenchmarkCandidate{
			Name:              name,
			FingerprintBits:   fingerprintBits,
			EntryBytes:        entryBytes,
			DirectoryBytes:    directory,
			BucketHeaderBytes: bucketHeaders,
			ProjectedBytes:    projected,
		}
		if currentBytes != 0 {
			candidate.SavingsPercent = (1 - float64(projected)/float64(currentBytes)) * 100
		}
		if fingerprintBits != 0 {
			identifierBits := float64(prefixBits + fingerprintBits)
			candidate.ExpectedCollisionPairs = float64(rows) * float64(rows-1) / (2 * math.Exp2(identifierBits))
			candidate.FalseCandidateRate = float64(rows) / math.Exp2(identifierBits)
		}
		return candidate
	}
	current := makeCandidate("pebble-current-logical", 0, currentTxIndexRowBytes, 0, 0)
	// An exact sharded representation can omit complete prefix bytes. A partial
	// byte remains in the first stored suffix byte.
	exactHashBytes := uint64((256 - prefixBits + 7) / 8)
	exact := makeCandidate("exact-hash-sharded", 0, exactHashBytes+8, directoryBytes, bucketHeaderBytes)
	compact64 := makeCandidate("fingerprint64-sharded", 64, 16, directoryBytes, bucketHeaderBytes)
	compact64.ObservedSampleCollisions = observedCollisions
	compact96 := makeCandidate("fingerprint96-sharded", 96, 20, directoryBytes, bucketHeaderBytes)
	return []txIndexBenchmarkCandidate{current, exact, compact64, compact96}
}

func writeTxIndexBenchmarkText(writer io.Writer, output txIndexBenchmarkOutput) {
	fmt.Fprintf(writer, "Chaindata: %s\n", output.ChaindataPath)
	fmt.Fprintf(writer, "Sample: %d tx indexes across %d hash windows; elapsed %s\n",
		output.SampleTransactions, output.Windows, time.Duration(output.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond))
	fmt.Fprintf(writer, "Projection: %d rows (total-tx-count=%d, counter used=%t)\n",
		output.ProjectedRows, output.TotalTransactionCount, output.ProjectionUsesCounter)
	fmt.Fprintf(writer, "Directory: %d bits, %s; projected average %.1f rows/bucket; sample max %d\n\n",
		output.PrefixBits, formatIEC(output.DirectoryBytes), output.ProjectedAvgBucketRows, output.SampleMaxBucketRows)
	tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "LAYOUT\tENTRY\tDIRECTORY\tBUCKET HEADERS\tPROJECTED\tSAVING\tEXPECTED COLLISIONS")
	for _, candidate := range output.Candidates {
		fmt.Fprintf(tw, "%s\t%d B\t%s\t%s\t%s\t%.2f%%\t%.6g\n",
			candidate.Name, candidate.EntryBytes, formatIEC(candidate.DirectoryBytes),
			formatIEC(candidate.BucketHeaderBytes), formatIEC(candidate.ProjectedBytes), candidate.SavingsPercent, candidate.ExpectedCollisionPairs)
	}
	_ = tw.Flush()
	if output.Lookup.Lookups > 0 {
		fmt.Fprintf(writer, "\n64-bit sample-table lookup: %.1f ns positive, %.1f ns negative (%d operations each, %d negative fingerprint candidates).\n",
			output.Lookup.PositiveNanosecondsPerOp, output.Lookup.NegativeNanosecondsPerOp,
			output.Lookup.Lookups, output.Lookup.NegativeFingerprintMatches)
	}
	fmt.Fprintln(writer, "Full-hash verification against the canonical block body makes fingerprint collisions safe; projected bytes are immutable file payload, not Pebble physical size.")
}
