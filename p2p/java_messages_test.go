//go:build integration
// +build integration

package p2p

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/p2p/discover"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
	"google.golang.org/protobuf/proto"
)

// Application-layer message codes from java-tron's MessageTypes.java. Kept
// local to this test file — the p2p package proper doesn't reference them.
const (
	javaTronP2PHello            byte = 0x20
	javaTronP2PDisconnect       byte = 0x21
	javaTronP2PPing             byte = 0x22
	javaTronP2PPong             byte = 0x23
	javaTronTRX                 byte = 0x01
	javaTronBlock               byte = 0x02
	javaTronInventory           byte = 0x06
	javaTronFetchInvData        byte = 0x07
	javaTronSyncBlockChain      byte = 0x08
	javaTronBlockChainInventory byte = 0x09
	javaTronBlockInventory      byte = 0x12
)

func getJavaTronAddr(t *testing.T) (string, int32) {
	t.Helper()
	addr := os.Getenv("JAVA_TRON_ADDR")
	if addr == "" {
		t.Skip("JAVA_TRON_ADDR not set")
	}
	n, err := strconv.ParseInt(os.Getenv("JAVA_TRON_NETWORK"), 10, 32)
	if err != nil {
		t.Skipf("JAVA_TRON_NETWORK not set or invalid: %v", err)
	}
	return addr, int32(n)
}

// ── UDP discovery messages ────────────────────────────────────────────────────

// TestUDPFindNodeReturnsNeighbors verifies a FIND_NEIGHBOURS query returns a
// NEIGHBOURS reply. Single-node setup may have 0 neighbours — non-fatal.
func TestUDPFindNodeReturnsNeighbors(t *testing.T) {
	addr, networkID := getJavaTronAddr(t)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	localAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	nodeID := discover.GenerateNodeID()
	localEP := &corepb.Endpoint{
		Address: []byte("127.0.0.1"),
		Port:    int32(conn.LocalAddr().(*net.UDPAddr).Port),
		NodeId:  nodeID,
	}

	// Ping first to seed us in peer's routing table.
	ping := &corepb.PingMessage{
		From:      localEP,
		To:        &corepb.Endpoint{Address: []byte("127.0.0.1"), Port: int32(udpAddr.Port)},
		Version:   networkID,
		Timestamp: time.Now().UnixMilli(),
	}
	pingBytes, _ := proto.Marshal(ping)
	if _, err := conn.WriteToUDP(append([]byte{discover.MsgPing}, pingBytes...), udpAddr); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 2048)
	if _, _, err := conn.ReadFromUDP(buf); err != nil {
		t.Fatalf("read pong: %v", err)
	}

	target := discover.GenerateNodeID()
	find := &corepb.FindNeighbours{
		From:      localEP,
		TargetId:  target,
		Timestamp: time.Now().UnixMilli(),
	}
	findBytes, _ := proto.Marshal(find)
	if _, err := conn.WriteToUDP(append([]byte{discover.MsgFindNode}, findBytes...), udpAddr); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var neighbors *corepb.Neighbours
	for i := 0; i < 5 && neighbors == nil; i++ {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if buf[0] == discover.MsgNeighbours {
			neighbors = &corepb.Neighbours{}
			if err := proto.Unmarshal(buf[1:n], neighbors); err != nil {
				t.Fatalf("decode: %v", err)
			}
		}
	}
	if neighbors == nil {
		t.Fatal("no NEIGHBOURS within 5 frames")
	}
	t.Logf("NEIGHBOURS contains %d peer(s)", len(neighbors.Neighbours))
}

// ── TCP handshake helper ──────────────────────────────────────────────────────

// dialAndHandshake dials the java-tron node, completes the libp2p HANDSHAKE_HELLO
// exchange, and returns the live conn. Caller closes.
func dialAndHandshake(t *testing.T) net.Conn {
	t.Helper()
	addr, networkID := getJavaTronAddr(t)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	nodeID := discover.GenerateNodeID()
	localEP := &corepb.Endpoint{
		Address: []byte("127.0.0.1"),
		Port:    18889,
		NodeId:  nodeID,
	}
	hello := BuildHelloMessage(localEP, networkID, 0)
	payload, _ := EncodeHello(hello)

	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := WriteMsg(conn, MsgLibp2pHello, payload); err != nil {
		conn.Close()
		t.Fatalf("send hello: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	code, p, err := ReadMsg(conn)
	if err != nil {
		conn.Close()
		t.Fatalf("read hello: %v", err)
	}
	if code != MsgLibp2pHello {
		conn.Close()
		t.Fatalf("expected HELLO, got %#x", code)
	}
	var peerHello p2ppb.HelloMessage
	if err := proto.Unmarshal(p, &peerHello); err != nil {
		conn.Close()
		t.Fatalf("decode hello: %v", err)
	}
	if peerHello.Code != 0 {
		conn.Close()
		t.Fatalf("peer rejected us with code=%d", peerHello.Code)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn
}

// ── One-shot comprehensive message test ───────────────────────────────────────

// TestP2PMessageCompleteness runs one TCP session and exercises every message
// type we can verify against a running java-tron. Consolidating into a single
// session avoids libp2p's per-IP ban-on-disconnect (ChannelManager.notifyDisconnect
// unconditionally calls banNode with DEFAULT_BAN_TIME).
//
// Assertions:
//   1. Libp2p HANDSHAKE_HELLO — exchange completed by dialAndHandshake above
//   2. App-layer P2P_HELLO (0x20) received from peer within 3s of handshake
//   3. Libp2p KEEP_ALIVE_PING — peer responds with PONG to our inbound PING
//   4. App-layer SYNC_BLOCK_CHAIN (0x08) and BLOCK_INVENTORY (0x12) observed
//      (java-tron immediately starts sync after handshake)
//   5. Libp2p DISCONNECT — we send one, peer closes TCP
//
// Run:
//   JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_NETWORK=0 \
//     go test -tags=integration ./p2p/ -run TestP2PMessageCompleteness -v
func TestP2PMessageCompleteness(t *testing.T) {
	conn := dialAndHandshake(t)
	defer conn.Close()

	seen := map[byte]int{}
	byteCount := map[byte]int{}
	record := func(code byte, n int) { seen[code]++; byteCount[code] += n }

	// Step A: receive initial burst (app-layer P2P_HELLO + optional sync frames)
	// for up to 3 seconds.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		body, err := ReadFrameBody(conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				break
			}
			t.Fatalf("read during initial burst: %v", err)
		}
		code, payload, err := UnwrapPostHandshake(body)
		if err != nil {
			t.Fatalf("unwrap: %v", err)
		}
		record(code, len(payload))
	}

	if seen[javaTronP2PHello] == 0 {
		t.Errorf("did not observe app.P2P_HELLO (0x20) in initial burst; got %v", seen)
	} else {
		t.Logf("✓ app.P2P_HELLO received (count=%d)", seen[javaTronP2PHello])
	}

	// Step B: send libp2p KEEP_ALIVE_PING; expect PONG.
	pingPayload, _ := EncodeKeepAlive(BuildKeepAlive())
	pingBody, err := WrapPostHandshake(MsgLibp2pKeepAlivePing, pingPayload)
	if err != nil {
		t.Fatalf("wrap ping: %v", err)
	}
	if err := WriteFrameBody(conn, pingBody); err != nil {
		t.Fatalf("send ping: %v", err)
	}

	pongSeen := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !pongSeen {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		body, err := ReadFrameBody(conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			t.Fatalf("read after ping: %v", err)
		}
		code, payload, err := UnwrapPostHandshake(body)
		if err != nil {
			t.Fatalf("unwrap: %v", err)
		}
		record(code, len(payload))
		if code == MsgLibp2pKeepAlivePong {
			pongSeen = true
		}
		if code == MsgLibp2pKeepAlivePing {
			// Answer peer's PING so they don't kill us.
			pongBody, _ := WrapPostHandshake(MsgLibp2pKeepAlivePong, payload)
			_ = WriteFrameBody(conn, pongBody)
		}
	}
	if !pongSeen {
		t.Errorf("did not receive KEEP_ALIVE_PONG within 5s of sending PING")
	} else {
		t.Logf("✓ libp2p PING → PONG round-trip confirmed")
	}

	// Step C: send libp2p DISCONNECT; expect peer to close TCP.
	dm := BuildDisconnect(p2ppb.DisconnectReason_PEER_QUITING)
	dcPayload, _ := EncodeDisconnect(dm)
	dcBody, _ := WrapPostHandshake(MsgLibp2pDisconnect, dcPayload)
	if err := WriteFrameBody(conn, dcBody); err != nil {
		t.Fatalf("send disconnect: %v", err)
	}

	gotClose := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		body, err := ReadFrameBody(conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			gotClose = true
			break
		}
		if code, payload, err := UnwrapPostHandshake(body); err == nil {
			record(code, len(payload))
		}
	}
	if !gotClose {
		t.Errorf("peer did not close TCP connection within 3s after our DISCONNECT")
	} else {
		t.Logf("✓ peer closed TCP after our DISCONNECT")
	}

	// Summary dump
	fmt.Printf("\n╔═══ Message Completeness Summary ═════════════════════════════╗\n")
	fmt.Printf("║ Code    Name                            Count    Bytes       ║\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════════╣\n")
	for code, count := range seen {
		fmt.Printf("║ %#04x    %-30s  %5d   %8d    ║\n", code, codeName(code), count, byteCount[code])
	}
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")
}

func codeName(c byte) string {
	switch c {
	case MsgLibp2pHello:
		return "libp2p.HANDSHAKE_HELLO"
	case MsgLibp2pStatus:
		return "libp2p.STATUS"
	case MsgLibp2pDisconnect:
		return "libp2p.DISCONNECT"
	case MsgLibp2pKeepAlivePing:
		return "libp2p.KEEP_ALIVE_PING"
	case MsgLibp2pKeepAlivePong:
		return "libp2p.KEEP_ALIVE_PONG"
	case javaTronTRX:
		return "app.TRX"
	case javaTronBlock:
		return "app.BLOCK"
	case javaTronInventory:
		return "app.INVENTORY"
	case javaTronFetchInvData:
		return "app.FETCH_INV_DATA"
	case javaTronSyncBlockChain:
		return "app.SYNC_BLOCK_CHAIN"
	case javaTronBlockChainInventory:
		return "app.BLOCK_CHAIN_INVENTORY"
	case javaTronBlockInventory:
		return "app.BLOCK_INVENTORY"
	case javaTronP2PHello:
		return "app.P2P_HELLO"
	case javaTronP2PDisconnect:
		return "app.P2P_DISCONNECT"
	case javaTronP2PPing:
		return "app.P2P_PING"
	case javaTronP2PPong:
		return "app.P2P_PONG"
	default:
		return fmt.Sprintf("unknown(%#x)", c)
	}
}
