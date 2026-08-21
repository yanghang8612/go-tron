package net

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

func TestStalledFetchRecoveryLogsAreStatefulAndRateLimited(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	ss := new(SyncService)
	started := time.Now()
	ss.logStalledFetchRecovery(20, 20, true, started)
	for minute := 1; minute < 10; minute++ {
		ss.logStalledFetchRecovery(20, 20, true, started.Add(time.Duration(minute)*time.Minute))
	}
	ss.logStalledFetchRecovery(20, 20, true, started.Add(stalledFetchRecoverySummaryInterval))
	ss.logStalledFetchRecovery(20, 21, true, started.Add(stalledFetchRecoverySummaryInterval+time.Minute))

	out := buf.String()
	if got := strings.Count(out, `msg="Re-kicking stalled sync fetch"`); got != 1 {
		t.Fatalf("initial re-kick log count = %d, want 1:\n%s", got, out)
	}
	if got := strings.Count(out, `msg="Sync fetch remains stalled after recovery attempts"`); got != 1 {
		t.Fatalf("stalled summary log count = %d, want 1:\n%s", got, out)
	}
	if got := strings.Count(out, `msg="Stalled sync fetch recovered"`); got != 1 {
		t.Fatalf("recovery log count = %d, want 1:\n%s", got, out)
	}
	for _, field := range []string{
		"head=20", "attempts=11", "suppressedSinceLastLog=9", "stalledFor=",
		"fromHead=20", "toHead=21",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing stalled-fetch log field %q:\n%s", field, out)
		}
	}
}
