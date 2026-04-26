package net

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type testCollector struct {
	mu    sync.Mutex
	codes []byte
}

func (tc *testCollector) OnPeerConnected(p *p2p.Peer)                        {}
func (tc *testCollector) OnPeerDisconnected(p *p2p.Peer)                     {}
func (tc *testCollector) OnMessage(p *p2p.Peer, code byte, data []byte) {}

func TestBroadcastBlockSendsInventory(t *testing.T) {
	// Set up two peers connected via pipe
	c1, c2 := net.Pipe()
	h := &testCollector{}
	peer := p2p.NewPeer(c1, "test:1", false, h)
	peer.Start()
	defer peer.Stop()

	// Read messages from the other end (post-handshake: CompressMessage-wrapped)
	go func() {
		for {
			body, err := p2p.ReadFrameBody(c2)
			if err != nil {
				return
			}
			code, _, err := p2p.UnwrapPostHandshake(body)
			if err != nil {
				return
			}
			h.mu.Lock()
			h.codes = append(h.codes, code)
			h.mu.Unlock()
		}
	}()

	bc := NewBroadcastService(func() []*p2p.Peer { return []*p2p.Peer{peer} })

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	bc.BroadcastBlock(block)
	time.Sleep(50 * time.Millisecond)

	h.mu.Lock()
	defer h.mu.Unlock()
	found := false
	for _, c := range h.codes {
		if c == p2p.MsgInventory {
			found = true
		}
	}
	if !found {
		t.Fatal("expected INVENTORY message to be sent")
	}
}

func TestBroadcastDeduplicates(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testCollector{}
	peer := p2p.NewPeer(c1, "test:1", false, h)
	peer.Start()
	defer peer.Stop()

	go func() {
		for {
			body, err := p2p.ReadFrameBody(c2)
			if err != nil {
				return
			}
			code, _, err := p2p.UnwrapPostHandshake(body)
			if err != nil {
				return
			}
			h.mu.Lock()
			h.codes = append(h.codes, code)
			h.mu.Unlock()
		}
	}()

	bc := NewBroadcastService(func() []*p2p.Peer { return []*p2p.Peer{peer} })

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	// Broadcast same block twice
	bc.BroadcastBlock(block)
	bc.BroadcastBlock(block)
	time.Sleep(50 * time.Millisecond)

	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, c := range h.codes {
		if c == p2p.MsgInventory {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 INVENTORY (deduplicated), got %d", count)
	}
}

// TestBroadcastBatchesTxs verifies that multiple transactions enqueued before
// the flush ticker fires are delivered in a single INV message to the peer.
func TestBroadcastBatchesTxs(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testCollector{}
	peer := p2p.NewPeer(c1, "test:1", false, h)
	peer.Start()
	defer peer.Stop()

	var invMessages sync.WaitGroup
	var invCount int
	var invMu sync.Mutex

	go func() {
		for {
			body, err := p2p.ReadFrameBody(c2)
			if err != nil {
				return
			}
			code, _, err := p2p.UnwrapPostHandshake(body)
			if err != nil {
				return
			}
			if code == p2p.MsgInventory {
				invMu.Lock()
				invCount++
				invMu.Unlock()
				invMessages.Done()
			}
		}
	}()

	bs := NewBroadcastService(func() []*p2p.Peer { return []*p2p.Peer{peer} })
	bs.Start()
	defer bs.Stop()

	// Enqueue 3 distinct transactions without waiting for the ticker.
	invMessages.Add(1) // expect exactly one flush containing all 3
	for i := 0; i < 3; i++ {
		tx := types.NewTransactionFromPB(&corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				RefBlockNum: int64(i + 1),
			},
		})
		bs.BroadcastTx(tx)
	}

	// Wait up to 200ms for the ticker to flush (ticker fires every 30ms).
	done := make(chan struct{})
	go func() { invMessages.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected batched INV flush within 200ms")
	}

	invMu.Lock()
	got := invCount
	invMu.Unlock()
	if got != 1 {
		t.Fatalf("expected 1 batched INV message, got %d", got)
	}
}

// TestBroadcastBlockFromExcludesOrigin verifies that the origin peer does NOT
// receive the INV when a block is relayed via BroadcastBlockFrom.
func TestBroadcastBlockFromExcludesOrigin(t *testing.T) {
	// origin peer (should NOT receive INV)
	o1, o2 := net.Pipe()
	originH := &testCollector{}
	originPeer := p2p.NewPeer(o1, "origin:1", false, originH)
	originPeer.Start()
	defer originPeer.Stop()

	go func() {
		for {
			body, err := p2p.ReadFrameBody(o2)
			if err != nil {
				return
			}
			code, _, err := p2p.UnwrapPostHandshake(body)
			if err != nil {
				return
			}
			originH.mu.Lock()
			originH.codes = append(originH.codes, code)
			originH.mu.Unlock()
		}
	}()

	// other peer (should receive INV)
	r1, r2 := net.Pipe()
	otherH := &testCollector{}
	otherPeer := p2p.NewPeer(r1, "other:1", false, otherH)
	otherPeer.Start()
	defer otherPeer.Stop()

	go func() {
		for {
			body, err := p2p.ReadFrameBody(r2)
			if err != nil {
				return
			}
			code, _, err := p2p.UnwrapPostHandshake(body)
			if err != nil {
				return
			}
			otherH.mu.Lock()
			otherH.codes = append(otherH.codes, code)
			otherH.mu.Unlock()
		}
	}()

	bs := NewBroadcastService(func() []*p2p.Peer { return []*p2p.Peer{originPeer, otherPeer} })

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 5, Timestamp: 9000},
		},
	})

	bs.BroadcastBlockFrom(block, originPeer)
	time.Sleep(60 * time.Millisecond)

	// origin must NOT have received an INV
	originH.mu.Lock()
	for _, c := range originH.codes {
		if c == p2p.MsgInventory {
			originH.mu.Unlock()
			t.Fatal("origin peer should not receive INV for block it sent us")
		}
	}
	originH.mu.Unlock()

	// other peer must have received an INV
	otherH.mu.Lock()
	defer otherH.mu.Unlock()
	found := false
	for _, c := range otherH.codes {
		if c == p2p.MsgInventory {
			found = true
		}
	}
	if !found {
		t.Fatal("other peer should have received INV")
	}
}
