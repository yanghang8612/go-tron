package net

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gnet "net"

	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/p2p"
)

// TestSync_BatchSummaryReportedOnInterval drives a stream of blocks through
// HandleBlock with statsReportInterval temporarily shrunk to 50ms, then
// asserts the throttled "Imported chain segment" summary line is emitted at
// least once with the expected fields.
func TestSync_BatchSummaryReportedOnInterval(t *testing.T) {
	oldInterval := statsReportInterval
	statsReportInterval = 50 * time.Millisecond
	defer func() { statsReportInterval = oldInterval }()

	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Pipe + peer so HandleBlock's bookkeeping has a peer to record stats
	// against. We don't drive the writer end — the test never causes
	// fetchNextBatch to send, so the pipe stays quiescent.
	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "summary-peer", false, nil)

	now := time.Now()
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.stats.startTime = now
	ss.stats.totalStart = now
	ss.inflight = 1
	ss.armFetchTimer()
	ss.mu.Unlock()

	// Insert 3 blocks with a 60ms gap before the third so the rolling
	// window definitely exceeds statsReportInterval=50ms and triggers a
	// summary emit at least once. Each block must be pre-registered in
	// ss.pending so HandleBlock's request-dedup gate accepts it.
	parent := bc.CurrentBlock().Hash()
	for i := int64(1); i <= 3; i++ {
		if i == 3 {
			time.Sleep(60 * time.Millisecond)
		}
		blk := stubBlock(i, parent)
		ss.mu.Lock()
		ss.inflight = 1
		if ss.pending == nil {
			ss.pending = make(map[tcommon.Hash]uint64)
		}
		ss.pending[blk.Hash()] = uint64(i)
		ss.mu.Unlock()
		if !ss.HandleBlock(peer, blk) {
			t.Fatalf("HandleBlock(%d) returned false", i)
		}
		parent = blk.Hash()
	}

	out := buf.String()
	if !strings.Contains(out, "Imported chain segment") {
		t.Fatalf("expected 'Imported chain segment' summary line, got:\n%s", out)
	}
	for _, k := range []string{
		"blocks=",
		"txs=",
		"elapsed=",
		"execElapsed=",
		"bufferWaitElapsed=",
		"validate=",
		"execute=",
		"maintenance=",
		"stateCommit=",
		"dpUpdate=",
		"persist=",
		"hooks=",
		"blocks/s=",
		"head=",
		"blockBuffer=",
		"requested=",
		"retryList=",
		"peer=",
		"peerState=",
		"inflight=",
		"fetchList=",
	} {
		if !strings.Contains(out, k) {
			t.Errorf("missing key %q in summary line:\n%s", k, out)
		}
	}
}
