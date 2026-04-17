// fixture-closure reads a blocks.bin file and prints the sorted JSON
// list of addresses referenced by the contained blocks (witness + every
// contract-type-specific address field). Used during capture to build
// seed.json's ClosureAddresses set.
//
// Any ContractTypes we don't yet decode are printed to stderr as a
// warning so the operator knows the closure is a best-effort upper
// bound and may need hand-extension before replay.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/conformance"
	"github.com/tronprotocol/go-tron/core/types"
)

func main() {
	log.SetFlags(0)
	blocksPath := flag.String("blocks", "", "path to blocks.bin (varint-prefixed Block protos)")
	extrasFlag := flag.String("standby-witnesses", "", "comma-separated 41-hex addresses to merge into the closure; pass the top-127 witness set at StartBlock-1 when change_delegation is active on mainnet")
	flag.Parse()
	if *blocksPath == "" {
		log.Fatal("--blocks is required")
	}

	blocks, err := readAllBlocks(*blocksPath)
	if err != nil {
		log.Fatalf("read blocks: %v", err)
	}

	var extras []tcommon.Address
	if *extrasFlag != "" {
		for _, h := range strings.Split(*extrasFlag, ",") {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			a, err := conformance.ParseAddress(h)
			if err != nil {
				log.Fatalf("standby-witnesses %q: %v", h, err)
			}
			extras = append(extras, a)
		}
	}

	addrs, unhandled, err := conformance.ComputeClosure(blocks, extras)
	if err != nil {
		log.Fatalf("compute closure: %v", err)
	}

	hexes := make([]string, 0, len(addrs))
	for _, a := range addrs {
		hexes = append(hexes, hex.EncodeToString(a[:]))
	}
	out, _ := json.Marshal(hexes)
	fmt.Println(string(out))

	if len(unhandled) > 0 {
		fmt.Fprintln(os.Stderr, "warning: unhandled ContractTypes (closure may miss addresses):")
		for t, n := range unhandled {
			fmt.Fprintf(os.Stderr, "  %s × %d\n", t.String(), n)
		}
	}
}

// readAllBlocks duplicates core/conformance's internal reader rather than
// exporting it — the framing is the same (varint len + Block proto), and
// the main binary doesn't need the reader anywhere else.
func readAllBlocks(path string) ([]*types.Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	var out []*types.Block
	for {
		n, err := binary.ReadUvarint(br)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		b, err := types.UnmarshalBlock(buf)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
}
