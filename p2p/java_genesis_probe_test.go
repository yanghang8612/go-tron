//go:build integration
// +build integration

package p2p

import (
	"encoding/hex"
	"testing"
	"time"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// TestProbeJavaTronGenesis fetches block #1 from a live java-tron and prints
// the fields needed to assess genesis-structure parity with gtron's
// core.GenesisToBlock:
//
//   - peer's Hello.GenesisBlockId (= java-tron's genesis block id)
//   - block #1's parentHash       (must equal the same)
//   - block #1 header fields visible in the proto (txTrieRoot,
//     accountStateRoot, witness_signature, version)
//
// One-shot diagnostic. Asserts only that we received the data.
func TestProbeJavaTronGenesis(t *testing.T) {
	conn := dialAndHandshake(t)
	defer conn.Close()

	peerHello, err := readAppHello(conn)
	if err != nil {
		t.Fatalf("read peer Hello: %v", err)
	}
	t.Logf("peer Hello: head=#%d genesis=#%d",
		peerHello.GetHeadBlockId().GetNumber(),
		peerHello.GetGenesisBlockId().GetNumber())
	t.Logf("=== JAVA-TRON GENESIS BLOCK ID (from peer.Hello) ===")
	t.Logf("    %s", hex.EncodeToString(peerHello.GetGenesisBlockId().GetHash()))

	// Send our matching Hello, claiming we are at genesis so peer initiates sync.
	hello := buildReplyHello(peerHello, true)
	helloBytes, _ := proto.Marshal(hello)
	if err := sendWrapped(conn, MsgHello, helloBytes); err != nil {
		t.Fatalf("send Hello: %v", err)
	}

	// Ask for blocks after genesis.
	syncReq := &corepb.ChainInventory{
		Ids: []*corepb.ChainInventory_BlockId{
			{Hash: peerHello.GetGenesisBlockId().GetHash(), Number: 0},
		},
	}
	syncBytes, _ := proto.Marshal(syncReq)
	if err := sendWrapped(conn, MsgSyncBlockChain, syncBytes); err != nil {
		t.Fatalf("send SYNC_BLOCK_CHAIN: %v", err)
	}

	deadline := time.Now().Add(8 * time.Second)
	fetched := false
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		body, err := ReadFrameBody(conn)
		if err != nil {
			continue
		}
		code, payload, err := UnwrapPostHandshake(body)
		if err != nil {
			continue
		}
		switch code {
		case MsgChainInventory:
			var ci corepb.ChainInventory
			if err := proto.Unmarshal(payload, &ci); err != nil || len(ci.Ids) == 0 {
				continue
			}
			t.Logf("CHAIN_INVENTORY: %d ids, remain=%d", len(ci.Ids), ci.GetRemainNum())
			for i := 0; i < len(ci.Ids) && i < 3; i++ {
				t.Logf("    ids[%d] = #%d %s", i, ci.Ids[i].Number,
					hex.EncodeToString(ci.Ids[i].Hash))
			}
			if !fetched {
				for _, bid := range ci.Ids {
					if bid.Number == 1 {
						fetchInv := &corepb.Inventory{
							Type: corepb.Inventory_BLOCK,
							Ids:  [][]byte{bid.Hash},
						}
						fb, _ := proto.Marshal(fetchInv)
						if err := sendWrapped(conn, MsgFetchInvData, fb); err == nil {
							fetched = true
							t.Logf("→ FETCH_INV_DATA for block #1 id=%s",
								hex.EncodeToString(bid.Hash))
						}
						break
					}
				}
			}
		case MsgBlock:
			var blk corepb.Block
			if err := proto.Unmarshal(payload, &blk); err != nil {
				t.Errorf("decode BLOCK: %v", err)
				continue
			}
			h := blk.GetBlockHeader()
			r := h.GetRawData()
			t.Logf("=== BLOCK #%d (java-tron) ===", r.GetNumber())
			t.Logf("    parentHash       = %s", hex.EncodeToString(r.GetParentHash()))
			t.Logf("    timestamp        = %d", r.GetTimestamp())
			t.Logf("    txTrieRoot       = %s", hex.EncodeToString(r.GetTxTrieRoot()))
			t.Logf("    accountStateRoot = %s", hex.EncodeToString(r.GetAccountStateRoot()))
			t.Logf("    witnessAddress   = %s", hex.EncodeToString(r.GetWitnessAddress()))
			t.Logf("    witnessId        = %d", r.GetWitnessId())
			t.Logf("    version          = %d", r.GetVersion())
			t.Logf("    transactions     = %d", len(blk.GetTransactions()))
			t.Logf("    witnessSignature = %s", hex.EncodeToString(h.GetWitnessSignature()))
			t.Logf("    raw_data bytes   = %d", proto.Size(r))
			return
		}
	}
	if !fetched {
		t.Fatalf("never received CHAIN_INVENTORY response from peer")
	}
	t.Fatalf("CHAIN_INVENTORY received but no BLOCK #1 within 8s")
}
