// fixture-capture is the operator-side driver for M0″ Phase 2: live forward
// capture of a java-tron mainnet block range into the conformance fixture
// format. Talks to a remote java-tron HTTP endpoint (optionally through a
// SOCKS5 proxy), waits for chain head to advance, snapshots the
// touched-address closure per block, and writes blocks.bin + seed.json +
// oracle.ndjson + fixture.json into a fresh range directory.
//
// Usage (POC):
//
//	fixture-capture \
//	    --http=http://3.12.206.71:8088 \
//	    --socks5=127.0.0.1:1088 \
//	    --range=range-maintenance \
//	    --start-auto-buffer=10 \
//	    --count=5 \
//	    --closure-witnesses \
//	    --out=test/fixtures/mainnet-blocks
//
// See docs/dev/conformance-harness.md (Capture protocol).

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	httpURL := flag.String("http", "", "java-tron HTTP base URL, e.g. http://1.2.3.4:8088")
	socks5 := flag.String("socks5", "", "optional SOCKS5 proxy host:port (e.g. 127.0.0.1:1088)")
	rangeName := flag.String("range", "", "range name, e.g. range-maintenance")
	startAuto := flag.Int("start-auto-buffer", 10, "when --start unset: start = head + buffer")
	startExplicit := flag.Uint64("start", 0, "explicit start block (overrides --start-auto-buffer)")
	count := flag.Int("count", 0, "number of blocks [start..start+count-1] to capture")
	closureFile := flag.String("closure", "", "file with one 41-hex address per line")
	closureWitnesses := flag.Bool("closure-witnesses", false, "include all 437 candidate witnesses")
	closureActiveOnly := flag.Bool("closure-active-only", false, "with --closure-witnesses, only the 27 isJobs SRs (POC)")
	out := flag.String("out", "test/fixtures/mainnet-blocks", "output root; range data goes to <out>/<range>")
	parallel := flag.Int("parallel", 16, "concurrent getaccount calls per snapshot")
	flag.Parse()

	if *httpURL == "" || *rangeName == "" || *count <= 0 {
		fmt.Fprintln(os.Stderr, "usage: fixture-capture --http=URL --range=NAME --count=K [...]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	if err := run(captureConfig{
		httpURL:           *httpURL,
		socks5:            *socks5,
		rangeName:         *rangeName,
		startExplicit:     *startExplicit,
		startAutoBuffer:   *startAuto,
		count:             *count,
		closureFile:       *closureFile,
		closureWitnesses:  *closureWitnesses,
		closureActiveOnly: *closureActiveOnly,
		outRoot:           *out,
		parallel:          *parallel,
	}); err != nil {
		log.Fatalf("capture failed: %v", err)
	}
}

type captureConfig struct {
	httpURL           string
	socks5            string
	rangeName         string
	startExplicit     uint64
	startAutoBuffer   int
	count             int
	closureFile       string
	closureWitnesses  bool
	closureActiveOnly bool
	outRoot           string
	parallel          int
}

