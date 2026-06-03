package core

import (
	"bufio"
	"os"
	"strconv"
	"sync"
)

// total_energy_weight / total_net_weight per-block tracer. Diagnostic-only,
// gated by the GTRON_TW_TRACE env var (a file path). Used to localize the
// Nile-8,825,873 total_energy_weight drift: re-sync with the var set, then diff
// the emitted (block,tw,net) curve against a canonical reference (a from-genesis
// java-tron with the same logging, or an offline floor-sum of the wire
// freeze/unfreeze txs). The first block where tw diverges brackets the bad
// freeze/unfreeze event. NOT committed — scratch instrumentation.
var (
	twTracePath  = os.Getenv("GTRON_TW_TRACE")
	twTraceMu    sync.Mutex
	twTraceW     *bufio.Writer
	twTraceFile  *os.File
	twTraceSetup bool
)

func traceTotalWeights(blockNum uint64, energyWeight, netWeight int64) {
	if twTracePath == "" {
		return
	}
	twTraceMu.Lock()
	defer twTraceMu.Unlock()
	if !twTraceSetup {
		twTraceSetup = true
		f, err := os.OpenFile(twTracePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			twTracePath = "" // disable on error
			return
		}
		twTraceFile = f
		twTraceW = bufio.NewWriterSize(f, 1<<20)
		twTraceW.WriteString("block,total_energy_weight,total_net_weight\n")
	}
	if twTraceW == nil {
		return
	}
	twTraceW.WriteString(strconv.FormatUint(blockNum, 10))
	twTraceW.WriteByte(',')
	twTraceW.WriteString(strconv.FormatInt(energyWeight, 10))
	twTraceW.WriteByte(',')
	twTraceW.WriteString(strconv.FormatInt(netWeight, 10))
	twTraceW.WriteByte('\n')
	// Flush periodically so a stall (e.g. at 8,825,873) leaves a usable tail.
	if blockNum%1024 == 0 {
		twTraceW.Flush()
		if twTraceFile != nil {
			twTraceFile.Sync()
		}
	}
}
