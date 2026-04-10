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

	// Read messages from the other end
	go func() {
		for {
			code, _, err := p2p.ReadMsg(c2)
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
			code, _, err := p2p.ReadMsg(c2)
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
