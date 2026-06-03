package actuator

import (
	"bufio"
	"encoding/hex"
	"os"
	"strconv"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// V1 freeze/unfreeze weight-event tracer. Diagnostic-only, gated by the
// GTRON_TW_EVENTS env var (a file path). Logs every total_{net,energy,
// tron_power}_weight mutation with full context so an offline v4.0.1-faithful
// floor simulator can recompute the expected per-block weight and pinpoint the
// Nile-8,825,873 energy-weight drift. signedAmount > 0 is a freeze (delta =
// floor(amount/1e6)); < 0 is an unfreeze (the |amount| is the stored frozen
// that gtron unfroze, delta = -floor(|amount|/1e6)). NOT committed.
var (
	weTracePath  = os.Getenv("GTRON_TW_EVENTS")
	weTraceMu    sync.Mutex
	weTraceW     *bufio.Writer
	weTraceF     *os.File
	weTraceSetup bool
)

func traceWeightEvent(block uint64, owner, receiver tcommon.Address, hasReceiver bool, res corepb.ResourceCode, signedAmount int64) {
	if weTracePath == "" {
		return
	}
	weTraceMu.Lock()
	defer weTraceMu.Unlock()
	if !weTraceSetup {
		weTraceSetup = true
		f, err := os.OpenFile(weTracePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			weTracePath = ""
			return
		}
		weTraceF = f
		weTraceW = bufio.NewWriterSize(f, 1<<20)
		weTraceW.WriteString("block,resource,owner,receiver,signed_amount\n")
	}
	if weTraceW == nil {
		return
	}
	recv := ""
	if hasReceiver {
		recv = hex.EncodeToString(receiver[:])
	}
	weTraceW.WriteString(strconv.FormatUint(block, 10))
	weTraceW.WriteByte(',')
	weTraceW.WriteString(res.String())
	weTraceW.WriteByte(',')
	weTraceW.WriteString(hex.EncodeToString(owner[:]))
	weTraceW.WriteByte(',')
	weTraceW.WriteString(recv)
	weTraceW.WriteByte(',')
	weTraceW.WriteString(strconv.FormatInt(signedAmount, 10))
	weTraceW.WriteByte('\n')
	if block%1024 == 0 {
		weTraceW.Flush()
		if weTraceF != nil {
			weTraceF.Sync()
		}
	}
}
