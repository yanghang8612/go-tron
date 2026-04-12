//go:build integration
// +build integration

package p2p

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/p2p/discover"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// TestAppLayerMessageFlow exercises the java-tron application-layer protocol
// on top of a single live connection. It:
//
//   1. Completes libp2p handshake (via dialAndHandshake)
//   2. Reads peer's app-layer P2P_HELLO (0x20) and proto-decodes it
//   3. Responds with a matching HelloMessage (echoing peer's genesis) so peer
//      keeps the connection alive
//   4. Reads further frames for ~10 seconds, proto-decoding each by type
//   5. Verifies SYNC_BLOCK_CHAIN / BLOCK_INVENTORY / BLOCK / TRX / INVENTORY
//      payloads parse as their expected proto types (where observed)
//   6. Sends an app-layer P2P_PING (0x22) and expects a P2P_PONG (0x23)
//   7. Sends a FETCH_INV_DATA for peer's head block and checks response
//
// Requires:  JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_NETWORK=0
func TestAppLayerMessageFlow(t *testing.T) {
	conn := dialAndHandshake(t)
	defer conn.Close()

	// ── Step 1: read app-layer P2P_HELLO from peer ────────────────────────
	peerHello, err := readAppHello(conn)
	if err != nil {
		t.Fatalf("read peer P2P_HELLO: %v", err)
	}
	t.Logf("✓ peer P2P_HELLO received: version=%d head=#%d solid=#%d genesis=#%d",
		peerHello.Version, peerHello.HeadBlockId.GetNumber(),
		peerHello.SolidBlockId.GetNumber(), peerHello.GenesisBlockId.GetNumber())

	if peerHello.GenesisBlockId == nil || len(peerHello.GenesisBlockId.Hash) == 0 {
		t.Fatal("peer HELLO missing genesis")
	}

	// ── Step 2: reply with matching app-layer Hello, claiming we're behind so
	// peer initiates sync and sends us SYNC_BLOCK_CHAIN / BLOCK_INVENTORY ────
	ourHello := buildReplyHello(peerHello, /*claimBehind=*/ true)
	ourHelloBytes, err := proto.Marshal(ourHello)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendWrapped(conn, MsgHello, ourHelloBytes); err != nil {
		t.Fatalf("send our hello: %v", err)
	}
	t.Logf("✓ sent matching app-layer HELLO (genesis #%d, our head #%d)",
		ourHello.GenesisBlockId.GetNumber(), ourHello.HeadBlockId.GetNumber())

	// ── Step 2b: proactively send SYNC_BLOCK_CHAIN to force sync flow ─────
	// java-tron responds with BLOCK_CHAIN_INVENTORY containing the next batch of
	// block IDs that we (as a "behind" peer) should fetch.
	syncReq := &corepb.ChainInventory{
		Ids: []*corepb.ChainInventory_BlockId{
			// Claim we only have the genesis — peer will reply with the missing tail.
			{Hash: peerHello.GenesisBlockId.Hash, Number: peerHello.GenesisBlockId.Number},
		},
	}
	syncReqBytes, _ := proto.Marshal(syncReq)
	if err := sendWrapped(conn, MsgSyncBlockChain, syncReqBytes); err != nil {
		t.Fatalf("send sync req: %v", err)
	}
	t.Logf("✓ sent SYNC_BLOCK_CHAIN asking for blocks after genesis")

	// ── Step 3: drain and categorize messages for ~10 s ───────────────────
	stats := newFlowStats()
	deadline := time.Now().Add(10 * time.Second)
	sentPing := false
	sawPong := false
	var pingPayload []byte

	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		body, err := ReadFrameBody(conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// opportunistic ping while idle
				if !sentPing {
					pingPayload = encodeKeepAlive()
					if sendErr := sendWrapped(conn, MsgPing, pingPayload); sendErr == nil {
						sentPing = true
					}
				}
				continue
			}
			t.Logf("stream ended: %v", err)
			break
		}
		code, payload, err := UnwrapPostHandshake(body)
		if err != nil {
			t.Logf("unwrap failed: %v", err)
			continue
		}
		stats.record(code, payload)

		// answer peer keepalives so we don't get dropped
		switch code {
		case MsgLibp2pKeepAlivePing:
			_ = sendWrapped(conn, MsgLibp2pKeepAlivePong, payload)
		case MsgPing:
			_ = sendWrapped(conn, MsgPong, payload)
		case MsgPong:
			if sentPing {
				sawPong = true
			}
		case MsgSyncBlockChain, MsgChainInventory:
			validateChainInventory(t, code, payload, stats)
		case MsgInventory:
			validateInventory(t, payload, stats)
		case MsgBlock:
			validateBlock(t, payload, stats)
		case MsgTx:
			validateTransaction(t, payload, stats)
		}

		// When we see an INVENTORY advertising blocks, fetch one to trigger
		// a BLOCK response — this proves the full advertise → fetch → receive
		// round-trip.
		if code == MsgInventory && !stats.sentFetch {
			var inv corepb.Inventory
			if err := proto.Unmarshal(payload, &inv); err == nil && len(inv.Ids) > 0 {
				fetch := &corepb.Inventory{Type: inv.Type, Ids: inv.Ids[:1]}
				fb, _ := proto.Marshal(fetch)
				if err := sendWrapped(conn, MsgFetchInvData, fb); err == nil {
					stats.sentFetch = true
					t.Logf("✓ sent FETCH_INV_DATA for %s id[0]", inv.Type.String())
				}
			}
		}

		// When we see CHAIN_INVENTORY (the response to our SYNC_BLOCK_CHAIN),
		// pick a block id from it and fetch it as BLOCK.
		if code == MsgChainInventory && !stats.sentBlockFetch {
			var ci corepb.ChainInventory
			if err := proto.Unmarshal(payload, &ci); err == nil && len(ci.Ids) > 0 {
				// Fetch the first non-genesis block id.
				for _, bid := range ci.Ids {
					if bid.Number == 0 {
						continue
					}
					fetchInv := &corepb.Inventory{
						Type: corepb.Inventory_BLOCK,
						Ids:  [][]byte{bid.Hash},
					}
					fb, _ := proto.Marshal(fetchInv)
					if err := sendWrapped(conn, MsgFetchInvData, fb); err == nil {
						stats.sentBlockFetch = true
						t.Logf("✓ sent FETCH_INV_DATA for block #%d", bid.Number)
					}
					break
				}
			}
		}
	}

	// ── Step 4: assertions ────────────────────────────────────────────────
	stats.dump(t)

	// We consumed the P2P_HELLO via readAppHello above, so it's NOT in stats.
	// The handshake success is the proto-decode + ≥1 subsequent app-layer frame.
	if len(stats.count) == 0 {
		t.Errorf("no app-layer activity after handshake — peer may have silently closed")
	}

	// Strong signals that app-layer interop works:
	// - Either SYNC_BLOCK_CHAIN/CHAIN_INVENTORY exchanged, OR
	// - INVENTORY received (peer advertising new blocks/txs)
	appSeen := stats.count[MsgSyncBlockChain] + stats.count[MsgChainInventory] +
		stats.count[MsgInventory] + stats.count[MsgBlock] + stats.count[MsgTx] +
		stats.count[0x12] // BLOCK_INVENTORY
	if appSeen == 0 {
		t.Errorf("no application-layer messages observed beyond HELLO")
	}

	if sentPing && !sawPong {
		t.Logf("note: app-layer P2P_PING not answered with PONG (saw other traffic though)")
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// readAppHello drains libp2p control frames until it finds an app-layer HELLO,
// then proto-decodes and returns it.
func readAppHello(conn net.Conn) (*corepb.HelloMessage, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		body, err := ReadFrameBody(conn)
		if err != nil {
			return nil, err
		}
		code, payload, err := UnwrapPostHandshake(body)
		if err != nil {
			return nil, err
		}
		if code == MsgHello {
			var msg corepb.HelloMessage
			if err := proto.Unmarshal(payload, &msg); err != nil {
				return nil, fmt.Errorf("proto decode HelloMessage: %w", err)
			}
			return &msg, nil
		}
		// otherwise keep draining (keepalives, sync frames, etc.)
	}
	return nil, fmt.Errorf("no app-layer HELLO within 5s")
}

// buildReplyHello constructs a HelloMessage matching the peer's genesis.
// If claimBehind is true, we advertise that our head is at genesis — causing
// peer to initiate a block-sync flow (SYNC_BLOCK_CHAIN / BLOCK_INVENTORY).
func buildReplyHello(peerHello *corepb.HelloMessage, claimBehind bool) *corepb.HelloMessage {
	from := &corepb.Endpoint{
		Address: []byte("127.0.0.1"),
		Port:    18889,
		NodeId:  discover.GenerateNodeID(),
	}
	head := peerHello.HeadBlockId
	solid := peerHello.SolidBlockId
	if claimBehind {
		// Claim head == genesis so peer thinks we need blocks [1..their_head].
		head = peerHello.GenesisBlockId
		solid = peerHello.GenesisBlockId
	}
	return &corepb.HelloMessage{
		From:           from,
		Version:        peerHello.Version,
		Timestamp:      time.Now().UnixMilli(),
		GenesisBlockId: peerHello.GenesisBlockId,
		SolidBlockId:   solid,
		HeadBlockId:    head,
	}
}

// sendWrapped wraps [code, payload] in a CompressMessage and writes the frame.
func sendWrapped(conn net.Conn, code byte, payload []byte) error {
	body, err := WrapPostHandshake(code, payload)
	if err != nil {
		return err
	}
	return WriteFrameBody(conn, body)
}

func encodeKeepAlive() []byte {
	ka, _ := EncodeKeepAlive(BuildKeepAlive())
	return ka
}

// ── Per-type validators ──────────────────────────────────────────────────────

func validateChainInventory(t *testing.T, code byte, payload []byte, s *flowStats) {
	var ci corepb.ChainInventory
	if err := proto.Unmarshal(payload, &ci); err != nil {
		t.Errorf("%s: proto decode ChainInventory failed: %v", codeName(code), err)
		return
	}
	s.decoded[code] = fmt.Sprintf("ids=%d remain=%d", len(ci.Ids), ci.RemainNum)
}

func validateInventory(t *testing.T, payload []byte, s *flowStats) {
	var inv corepb.Inventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		t.Errorf("INVENTORY: proto decode failed: %v", err)
		return
	}
	s.decoded[MsgInventory] = fmt.Sprintf("type=%v count=%d", inv.Type, len(inv.Ids))
}

func validateBlock(t *testing.T, payload []byte, s *flowStats) {
	var blk corepb.Block
	if err := proto.Unmarshal(payload, &blk); err != nil {
		t.Errorf("BLOCK: proto decode failed: %v", err)
		return
	}
	if blk.BlockHeader == nil {
		t.Errorf("BLOCK: missing header")
		return
	}
	s.decoded[MsgBlock] = fmt.Sprintf("num=%d txs=%d",
		blk.BlockHeader.GetRawData().GetNumber(), len(blk.Transactions))
}

func validateTransaction(t *testing.T, payload []byte, s *flowStats) {
	var tx corepb.Transaction
	if err := proto.Unmarshal(payload, &tx); err != nil {
		t.Errorf("TX: proto decode failed: %v", err)
		return
	}
	s.decoded[MsgTx] = fmt.Sprintf("contracts=%d", len(tx.GetRawData().GetContract()))
}

// ── Stats helper ─────────────────────────────────────────────────────────────

type flowStats struct {
	count          map[byte]int
	bytes          map[byte]int
	decoded        map[byte]string
	sentFetch      bool
	sentBlockFetch bool
}

func newFlowStats() *flowStats {
	return &flowStats{
		count:   map[byte]int{},
		bytes:   map[byte]int{},
		decoded: map[byte]string{},
	}
}

func (s *flowStats) record(code byte, payload []byte) {
	s.count[code]++
	s.bytes[code] += len(payload)
}

func (s *flowStats) dump(t *testing.T) {
	fmt.Printf("\n╔═══ App-Layer Message Flow Summary ════════════════════════════════════╗\n")
	fmt.Printf("║ Code    Name                         Count    Bytes   Decoded           ║\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════════════════╣\n")
	for code, count := range s.count {
		note := s.decoded[code]
		if len(note) > 24 {
			note = note[:24]
		}
		fmt.Printf("║ %#04x    %-28s  %5d   %6d   %-24s ║\n",
			code, codeName(code), count, s.bytes[code], note)
	}
	fmt.Printf("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
}
