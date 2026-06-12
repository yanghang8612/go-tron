// abi_hash prints the 4-byte function selector and 32-byte event topic for
// each canonical ABI signature given as an argument.
//
//	go run ./scripts/dev/abi_hash 'removeOrders(uint256[])' 'Sync(uint256)'
//
// Used in sync-stall debugging to map dispatch-table PUSH4 constants and
// canonical receipt log topics back to names (docs/dev/sync-stall-runbook.md).
package main

import (
	"fmt"
	"os"

	"golang.org/x/crypto/sha3"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./scripts/dev/abi_hash '<sig>' [...]")
		os.Exit(1)
	}
	for _, sig := range os.Args[1:] {
		h := sha3.NewLegacyKeccak256()
		h.Write([]byte(sig))
		sum := h.Sum(nil)
		fmt.Printf("%-48s selector %x  topic %x\n", sig, sum[:4], sum)
	}
}
