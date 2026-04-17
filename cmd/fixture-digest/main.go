// fixture-digest consumes a capture snapshot JSON (java-tron's post-state
// view for one block) and emits a single OracleEntry JSON line: the B-
// digest and, optionally, the C-digest diagnostic.
//
// Wire format for the snapshot JSON: core/conformance.Snapshot.
//
// Usage:
//
//	fixture-digest --mode=B < snapshot.json >> oracle.ndjson
//	fixture-digest --mode=BC --input=snapshot.json
//
// Exit codes:
//
//	0  success
//	1  snapshot parse / digest computation error
//	2  CLI argument error
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/tronprotocol/go-tron/core/conformance"
)

func main() {
	log.SetFlags(0)
	input := flag.String("input", "", "snapshot JSON path; default stdin")
	mode := flag.String("mode", "B", "B = DigestB only; BC = DigestB + DigestC")
	flag.Parse()

	if *mode != "B" && *mode != "BC" {
		fmt.Fprintln(os.Stderr, "--mode must be B or BC")
		os.Exit(2)
	}

	var r io.Reader = os.Stdin
	if *input != "" {
		f, err := os.Open(*input)
		if err != nil {
			log.Fatalf("open %s: %v", *input, err)
		}
		defer f.Close()
		r = f
	}

	loaded, snap, err := conformance.LoadSnapshot(r)
	if err != nil {
		log.Fatalf("load snapshot: %v", err)
	}

	d := conformance.DigestB(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)
	entry := conformance.OracleEntry{
		BlockNum: snap.BlockNum,
		DigestB:  hex.EncodeToString(d[:]),
	}
	if *mode == "BC" {
		entry.DiagC = conformance.DigestC(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)
	}
	out, _ := json.Marshal(entry)
	fmt.Println(string(out))
}
