package net

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/p2p"
)

func TestPeerRateLimitWarningsAreAggregated(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelWarn)))

	ps := &peerState{peer: p2p.NewPeer(nil, "limited-peer", false, nil)}
	now := time.Now()
	var handler TronHandler
	handler.reportPeerRateLimited(ps, p2p.MsgTx, now)
	handler.reportPeerRateLimited(ps, p2p.MsgTx, now.Add(time.Second))
	handler.reportPeerRateLimited(ps, p2p.MsgTrxs, now.Add(peerRateLimitSummaryInterval))

	out := buf.String()
	if got := strings.Count(out, `msg="Peer message rate-limit drops"`); got != 2 {
		t.Fatalf("summary count = %d, want 2:\n%s", got, out)
	}
	for _, field := range []string{"droppedSinceLastReport=1", "droppedSinceLastReport=2", "sampleMsg=", "sampleCode="} {
		if !strings.Contains(out, field) {
			t.Errorf("missing aggregate field %q:\n%s", field, out)
		}
	}
}

func TestPeerRateLimitConcurrentAccounting(t *testing.T) {
	ps := &peerState{peer: p2p.NewPeer(nil, "limited-peer", false, nil)}
	ps.lastRateLimitSummaryNanos.Store(time.Now().UnixNano())
	var handler TronHandler
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.reportPeerRateLimited(ps, p2p.MsgTx, time.Now())
		}()
	}
	wg.Wait()
	if got := ps.rateLimitDrops.Load(); got != 64 {
		t.Fatalf("pending rate-limit drops = %d, want 64", got)
	}
}
