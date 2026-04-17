// gen-smoke regenerates the smoke conformance corpus at
// test/fixtures/mainnet-blocks/smoke/. It is hermetic (no java-tron
// dependency) — the oracle is the replay engine's own output. Re-run
// only after intentional changes to ProcessBlock semantics or to the
// conformance fixture format.
//
// Usage (from repo root):
//
//	go run ./scripts/fixtures/cmd/gen-smoke [dir]
//
// Default dir is test/fixtures/mainnet-blocks/smoke.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tronprotocol/go-tron/core/conformance"
)

func main() {
	log.SetFlags(0)
	flag.Parse()

	dir := filepath.Join("test", "fixtures", "mainnet-blocks", "smoke")
	if flag.NArg() > 0 {
		dir = flag.Arg(0)
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		log.Fatalf("abs %s: %v", dir, err)
	}

	params := conformance.SyntheticRangeParams{
		Dir:        absDir,
		Scenario:   "smoke",
		StartBlock: 1_000_000,
		BlockCount: 5,
		// Fixed witness addr so re-runs stay byte-identical.
		WitnessHex: "41" + strings.Repeat("a", 40),
		WitnessBal: 1_000_000,
		CapturedAt: "2026-04-17T00:00:00Z",
	}
	if err := conformance.BuildSyntheticRange(params); err != nil {
		log.Fatalf("build synthetic range: %v", err)
	}
	fmt.Printf("smoke corpus written: %s\n", absDir)
	// Sanity: files exist.
	for _, f := range []string{"seed.json", "blocks.bin", "oracle.ndjson", "divergence-allowlist.json", "fixture.json"} {
		if _, err := os.Stat(filepath.Join(absDir, f)); err != nil {
			log.Fatalf("missing: %v", err)
		}
	}
}
