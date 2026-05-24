package net

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gnet "net"

	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/p2p"
)

// TestSync_BatchSummaryReportedOnInterval drives a stream of blocks through
// HandleBlock with StatsReportInterval temporarily shrunk to 50ms, then
// asserts the throttled "Imported chain segment" summary line is emitted at
// least once with the expected fields.
func TestSync_BatchSummaryReportedOnInterval(t *testing.T) {
	oldInterval := tsync.StatsReportInterval
	tsync.StatsReportInterval = 50 * time.Millisecond
	defer func() { tsync.StatsReportInterval = oldInterval }()

	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelDebug)
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
	ss.stats.InitSession(now)
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
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
		"applyElapsed=",
		"slowPhase=",
		"slowElapsed=",
		"slowStateCommitPhase=",
		"slowStateCommitElapsed=",
		"blocks/s=",
		"head=",
		"peer=",
	} {
		if !strings.Contains(out, k) {
			t.Errorf("missing key %q in summary line:\n%s", k, out)
		}
	}
	for _, k := range []string{
		"Imported chain segment details",
		"bufferWaitElapsed=",
		"validate=",
		"execute=",
		"maintenance=",
		"stateCommit=",
		"stateCommitMeasured=",
		"stateCommitPrepare=",
		"stateCommitFlatWrite=",
		"stateCommitFlatFlush=",
		"stateCommitKVCompute=",
		"stateCommitKVNodes=",
		"stateCommitAccountTrieUpdate=",
		"stateCommitAccountTrieMarshal=",
		"stateCommitAccountTrieGeneration=",
		"stateCommitAccountTrieWrite=",
		"stateCommitFinalize=",
		"stateCommitAccountTrieCommit=",
		"stateCommitTrieNodes=",
		"stateCommitTrieFlush=",
		"stateCommitReopen=",
		"stateCommitAccounts=",
		"stateCommitKVAccounts=",
		"stateCommitKVItems=",
		"dpUpdate=",
		"persist=",
		"hooks=",
		"blockBuffer=",
		"requested=",
		"retryList=",
		"peerState=",
		"inflight=",
		"fetchList=",
	} {
		if !strings.Contains(out, k) {
			t.Errorf("missing key %q in summary line:\n%s", k, out)
		}
	}
}

func TestSlowestStateCommitPhasePrefersAccountTrieLeafWhenAvailable(t *testing.T) {
	phase, elapsed := slowestStateCommitPhase(core.ApplyStats{
		StateCommitDetail: state.CommitStats{
			KVCompute:             2 * time.Second,
			AccountTrieUpdate:     10 * time.Second,
			AccountTrieMarshal:    time.Second,
			AccountTrieGeneration: 500 * time.Millisecond,
			AccountTrieWrite:      6 * time.Second,
		},
	})
	if phase != "accountTrieWrite" || elapsed != 6*time.Second {
		t.Fatalf("slowestStateCommitPhase = %s %v, want accountTrieWrite 6s", phase, elapsed)
	}
}

func TestSlowestStateCommitPhaseFallsBackToAccountTrieAggregate(t *testing.T) {
	phase, elapsed := slowestStateCommitPhase(core.ApplyStats{
		StateCommitDetail: state.CommitStats{
			KVCompute:         2 * time.Second,
			AccountTrieUpdate: 10 * time.Second,
		},
	})
	if phase != "accountTrieUpdate" || elapsed != 10*time.Second {
		t.Fatalf("slowestStateCommitPhase = %s %v, want accountTrieUpdate 10s", phase, elapsed)
	}
}
