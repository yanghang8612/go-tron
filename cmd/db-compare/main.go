package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/internal/dbcompare"
)

func main() {
	var (
		gtronPath  = flag.String("gtron", "", "gtron --datadir or gtron/chaindata path (node must be stopped)")
		javaPath   = flag.String("java", "", "java-tron output-directory or database path (node must be stopped)")
		height     = flag.Uint64("height", 0, "exact height expected at both database heads")
		maxDiffs   = flag.Int("max-diffs", 10000, "maximum detailed differences retained in output")
		storeDiffs = flag.Int("max-diffs-per-store", 100, "maximum detailed differences retained for each store (0 disables per-store cap)")
		liveDiffs  = flag.Int("live-max-diffs", 1000, "maximum detailed differences written to each in-progress JSON snapshot")
		jsonOut    = flag.Bool("json", false, "write the full report as JSON")
		oneWay     = flag.Bool("java-only-accounts", false, "skip reverse detection of accounts present only in gtron")
		workers    = flag.Int("workers", 0, "parallel comparison workers for large stores (0=auto, maximum 64)")
		quiet      = flag.Bool("quiet", false, "suppress progress logs written to stderr")
	)
	flag.Parse()
	if *maxDiffs < 0 || *storeDiffs < 0 || *liveDiffs < 0 {
		fmt.Fprintln(os.Stderr, "--max-diffs, --max-diffs-per-store and --live-max-diffs must be non-negative")
		os.Exit(2)
	}
	if *gtronPath == "" || *javaPath == "" {
		fmt.Fprintln(os.Stderr, "--gtron and --java are required")
		flag.Usage()
		os.Exit(2)
	}
	logger := log.New(os.Stderr, "db-compare ", log.LstdFlags)
	logf := func(format string, args ...any) {
		if !*quiet {
			logger.Printf(format, args...)
		}
	}
	liveJSON, err := newLiveJSONOutput(os.Stdout, *jsonOut)
	if err != nil {
		fatal("prepare live JSON output", err)
	}
	writeInitialJSON := func(stage string) {
		if liveJSON == nil {
			return
		}
		snapshot := &dbcompare.Report{
			Height: *height, Stores: []dbcompare.StoreResult{},
			Progress: &dbcompare.ReportProgress{Phase: "opening", Stage: stage},
		}
		if err := liveJSON.Write(snapshot); err != nil {
			fatal("write live JSON report", err)
		}
	}

	gtronDir := dbcompare.ResolveGtronChainDataDir(*gtronPath)
	writeInitialJSON("opening gtron database")
	logf("opening gtron database path=%s", gtronDir)
	gtron, err := rawdb.NewPebbleDB(gtronDir, 64, 64)
	if err != nil {
		fatal("open gtron database (stop gtron first)", err)
	}
	defer gtron.Close()
	logf("opened gtron database")
	javaDir := dbcompare.ResolveJavaDatabaseDir(*javaPath)
	writeInitialJSON("opening java-tron stores")
	logf("opening java-tron stores path=%s", javaDir)
	java, err := dbcompare.OpenJavaStores(*javaPath)
	if err != nil {
		fatal("open java-tron database (stop java-tron first)", err)
	}
	defer java.Close()
	logf("opened java-tron stores discovered=%d", len(java.Discovered()))
	writeInitialJSON("starting comparison")
	progressJSON := newLiveProgressWriter(liveJSON)
	progressDifferenceLimit := *liveDiffs

	report, err := dbcompare.Compare(gtron, java, dbcompare.Options{
		Height: *height, MaxDifferences: *maxDiffs, MaxDifferencesPerStore: *storeDiffs,
		ReverseAccounts: !*oneWay, Workers: *workers,
		ProgressMaxDifferences: &progressDifferenceLimit,
		Progress: func(event dbcompare.ProgressEvent) {
			if event.Snapshot != nil {
				progressJSON.Submit(liveReportSnapshot(event.Snapshot, *liveDiffs))
			}
			if *quiet {
				return
			}
			elapsed := event.Elapsed.Round(time.Millisecond)
			switch event.Phase {
			case "start":
				logger.Printf("[%s] start", event.Store)
			case "skip":
				logger.Printf("[%s] skip detail=%s", event.Store, event.Detail)
			case "progress":
				r := event.Result
				logger.Printf("[%s] progress rows=%d elapsed=%s stage=%s compared=%d equal=%d different=%d missing_gtron=%d missing_java=%d invalid=%d skipped=%d mismatches=%d",
					event.Store, event.Rows, elapsed, event.Detail, r.Compared, r.Equal, r.Different, r.MissingGtron, r.MissingJava, r.Invalid, r.Skipped, r.Mismatches())
			case "done":
				r := event.Result
				logger.Printf("[%s] done elapsed=%s compared=%d equal=%d different=%d missing_gtron=%d missing_java=%d invalid=%d skipped=%d mismatches=%d",
					event.Store, elapsed, r.Compared, r.Equal, r.Different, r.MissingGtron, r.MissingJava, r.Invalid, r.Skipped, r.Mismatches())
			case "info":
				logger.Printf("%s", event.Detail)
			}
		},
	})
	if err != nil {
		_ = progressJSON.Close()
		fatal("compare databases", err)
	}
	if err := progressJSON.Close(); err != nil {
		fatal("write live JSON report", err)
	}
	report.Sort()
	logf("comparison complete stores=%d mismatches=%d state_coverage_complete=%t", len(report.Stores), report.Mismatches(), report.StateCoverageComplete)
	if *jsonOut {
		if liveJSON != nil {
			if err := liveJSON.Write(report); err != nil {
				fatal("write final JSON report", err)
			}
		} else {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(report); err != nil {
				fatal("encode report", err)
			}
		}
	} else {
		fmt.Printf("height=%d gtron_head=%d java_head=%d state_coverage_complete=%t mismatches=%d\n",
			report.Height, report.GtronHead, report.JavaHead, report.StateCoverageComplete, report.Mismatches())
		for _, s := range report.Stores {
			fmt.Printf("%-34s scope=%-11s present=%-5t compared=%d equal=%d different=%d missing_gtron=%d missing_java=%d invalid=%d skipped=%d\n",
				s.Name, s.Scope, s.Present, s.Compared, s.Equal, s.Different, s.MissingGtron, s.MissingJava, s.Invalid, s.Skipped)
		}
		if len(report.UnsupportedStateStores) != 0 {
			fmt.Printf("unsupported_state_stores=%v\n", report.UnsupportedStateStores)
		}
		if len(report.UnclassifiedStores) != 0 {
			fmt.Printf("unclassified_stores=%v\n", report.UnclassifiedStores)
		}
		if len(report.ExcludedStores) != 0 {
			fmt.Printf("excluded_non_state_stores=%v\n", report.ExcludedStores)
		}
		for _, d := range report.Differences {
			fmt.Printf("\n[%s] %s %s\n%s\n", d.Store, d.Kind, d.Key, d.Detail)
		}
	}
	if !report.StateCoverageComplete {
		fmt.Fprintln(os.Stderr, "state-store coverage is incomplete; see unsupported_state_stores/unclassified_stores")
		os.Exit(2)
	}
	if report.Mismatches() != 0 {
		os.Exit(1)
	}
}

// liveJSONOutput periodically replaces a redirected regular file with the
// latest complete JSON snapshot. Pipes and terminals keep the traditional
// single final JSON document behavior.
type liveJSONOutput struct {
	file *os.File
}

// liveProgressWriter keeps JSON serialization and file replacement off the
// comparison hot path. Its single-slot queue coalesces stale snapshots: a slow
// disk should delay visibility of progress, never database comparison itself.
type liveProgressWriter struct {
	output *liveJSONOutput
	queue  chan *dbcompare.Report
	done   chan struct{}
	err    error
}

func newLiveProgressWriter(output *liveJSONOutput) *liveProgressWriter {
	w := &liveProgressWriter{output: output}
	if output == nil {
		return w
	}
	w.queue = make(chan *dbcompare.Report, 1)
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		for snapshot := range w.queue {
			if w.err == nil {
				w.err = w.output.Write(snapshot)
			}
		}
	}()
	return w
}

func (w *liveProgressWriter) Submit(snapshot *dbcompare.Report) {
	if w == nil || w.output == nil || snapshot == nil {
		return
	}
	select {
	case w.queue <- snapshot:
		return
	default:
	}
	// Replace an obsolete queued snapshot without waiting for the writer.
	select {
	case <-w.queue:
	default:
	}
	select {
	case w.queue <- snapshot:
	default:
	}
}

func (w *liveProgressWriter) Close() error {
	if w == nil || w.output == nil {
		return nil
	}
	close(w.queue)
	<-w.done
	return w.err
}

func newLiveJSONOutput(file *os.File, enabled bool) (*liveJSONOutput, error) {
	if !enabled {
		return nil, nil
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil
	}
	return &liveJSONOutput{file: file}, nil
}

func (o *liveJSONOutput) Write(value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := o.file.Truncate(0); err != nil {
		return err
	}
	if _, err := o.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	n, err := o.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// liveReportSnapshot bounds the expensive, repeatedly-marshaled diagnostics in
// an in-progress file. Store mismatch counters remain authoritative and the
// final write still contains up to --max-diffs details.
func liveReportSnapshot(report *dbcompare.Report, maxDifferences int) *dbcompare.Report {
	if report == nil {
		return nil
	}
	snapshot := *report
	limit := len(report.Differences)
	if limit > maxDifferences {
		limit = maxDifferences
	}
	snapshot.Differences = append([]dbcompare.Difference(nil), report.Differences[:limit]...)
	return &snapshot
}

func fatal(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(2)
}
