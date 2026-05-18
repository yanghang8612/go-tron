// gtron-replay runs the M0" conformance replay harness against a
// pre-captured mainnet-blocks fixture directory.
//
// Exit codes:
//
//	0  — all blocks passed; allowlist clean (or --exit-gate not set)
//	1  — replay passed but --exit-gate saw allowlist hits / stale entries
//	2  — hard divergence (DigestB mismatch not covered by allowlist)
//	3  — harness error (fixture files missing / malformed / I/O failure)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/conformance"
)

func main() {
	rangeDir := flag.String("range", "", "path to test/fixtures/mainnet-blocks/<name>")
	exitGate := flag.Bool("exit-gate", false, "fail if allowlist has hits or stale entries")
	verbose := flag.Bool("verbose", false, "print C-digest diffs on failure")
	flag.Parse()

	if *rangeDir == "" {
		fmt.Fprintln(os.Stderr, "--range is required")
		os.Exit(3)
	}

	meta, err := conformance.LoadFixtureMeta(filepath.Join(*rangeDir, "fixture.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load fixture.json: %v\n", err)
		os.Exit(3)
	}
	witnesses, err := conformance.ParseAddresses(meta.ActiveWitnesses)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse active witnesses: %v\n", err)
		os.Exit(3)
	}

	cfg := conformance.ReplayConfig{
		RangeName:       filepath.Base(*rangeDir),
		SeedPath:        filepath.Join(*rangeDir, "seed.json"),
		BlocksPath:      filepath.Join(*rangeDir, "blocks.bin"),
		OraclePath:      filepath.Join(*rangeDir, "oracle.ndjson"),
		AllowlistPath:   filepath.Join(*rangeDir, "divergence-allowlist.json"),
		GenesisTime:     meta.GenesisTime,
		ActiveWitnesses: witnesses,
	}

	rep, err := conformance.ReplayRange(context.Background(), cfg)
	if err != nil {
		log.Error("Replay error", "err", err)
		os.Exit(3)
	}

	fmt.Println(rep.String())

	if *verbose && rep.FirstFailure != nil {
		fmt.Println("--- got (JSON) ---")
		fmt.Println(string(rep.FirstFailure.GotJSON))
		fmt.Println("--- want (JSON) ---")
		fmt.Println(string(rep.FirstFailure.WantJSON))
	}

	switch {
	case rep.FirstFailure != nil:
		os.Exit(2)
	case *exitGate && (rep.AllowlistHits > 0 || len(rep.StaleAllowlistEntries) > 0):
		os.Exit(1)
	default:
		os.Exit(0)
	}
}
