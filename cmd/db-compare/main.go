package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

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
	)
	flag.Parse()
	if *gtronPath == "" || *javaPath == "" {
		fmt.Fprintln(os.Stderr, "--gtron and --java are required")
		flag.Usage()
		os.Exit(2)
	}

	gtronDir := dbcompare.ResolveGtronChainDataDir(*gtronPath)
	gtron, err := rawdb.NewPebbleDB(gtronDir, 64, 64)
	if err != nil {
		fatal("open gtron database (stop gtron first)", err)
	}
	defer gtron.Close()
	java, err := dbcompare.OpenJavaStores(*javaPath)
	if err != nil {
		fatal("open java-tron database (stop java-tron first)", err)
	}
	defer java.Close()

	report, err := dbcompare.Compare(gtron, java, dbcompare.Options{
		Height: *height, MaxDifferences: *maxDiffs, ReverseAccounts: !*oneWay,
	})
	if err != nil {
		fatal("compare databases", err)
	}
	report.Sort()
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
