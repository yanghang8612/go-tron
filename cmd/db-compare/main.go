package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/internal/dbcompare"
)

func main() {
	var (
		gtronPath = flag.String("gtron", "", "gtron --datadir or gtron/chaindata path (node must be stopped)")
		javaPath  = flag.String("java", "", "java-tron output-directory or database path (node must be stopped)")
		height    = flag.Uint64("height", 0, "exact height expected at both database heads")
		maxDiffs  = flag.Int("max-diffs", 100, "maximum detailed differences retained in output")
		jsonOut   = flag.Bool("json", false, "write the full report as JSON")
		oneWay    = flag.Bool("java-only-accounts", false, "skip reverse detection of accounts present only in gtron")
		quiet     = flag.Bool("quiet", false, "suppress progress logs written to stderr")
	)
	flag.Parse()
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

	gtronDir := dbcompare.ResolveGtronChainDataDir(*gtronPath)
	logf("opening gtron database path=%s", gtronDir)
	gtron, err := rawdb.NewPebbleDB(gtronDir, 64, 64)
	if err != nil {
		fatal("open gtron database (stop gtron first)", err)
	}
	defer gtron.Close()
	logf("opened gtron database")
	javaDir := dbcompare.ResolveJavaDatabaseDir(*javaPath)
	logf("opening java-tron stores path=%s", javaDir)
	java, err := dbcompare.OpenJavaStores(*javaPath)
	if err != nil {
		fatal("open java-tron database (stop java-tron first)", err)
	}
	defer java.Close()
	logf("opened java-tron stores discovered=%d", len(java.Discovered()))

	report, err := dbcompare.Compare(gtron, java, dbcompare.Options{
		Height: *height, MaxDifferences: *maxDiffs, ReverseAccounts: !*oneWay,
		Progress: func(event dbcompare.ProgressEvent) {
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
		fatal("compare databases", err)
	}
	report.Sort()
	logf("comparison complete stores=%d mismatches=%d state_coverage_complete=%t", len(report.Stores), report.Mismatches(), report.StateCoverageComplete)
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatal("encode report", err)
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

func fatal(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(2)
}
