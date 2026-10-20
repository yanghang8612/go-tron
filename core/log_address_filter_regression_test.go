package core

import (
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	snapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Exercise the public 20-byte RPC shape against both persisted protobuf widths,
// with the hot receipt source removed in the cold cases. Old tests constructed
// both sides from the same raw width and missed the RPC's 0x41 normalization.
func TestJSONRPCLogAddressFilterTVMAndNative(t *testing.T) {
	for _, width := range []int{20, 21} {
		for _, storage := range []string{"hot", "legacy-cold", "v4-cold"} {
			t.Run(fmt.Sprintf("%d/%s", width, storage), func(t *testing.T) {
				bc, cleanup := newTestBlockchain(t)
				defer cleanup()
				address := common.FromHex("a4997124a328a4615eab437cede31da47a31b375")
				raw := append([]byte(nil), address...)
				if width == 21 {
					raw = append([]byte{0x41}, raw...)
				}
				topic := common.Hash{0xa0, 0xd1}
				block, info := testBackendLogBlock(1, &corepb.TransactionInfo_Log{Address: raw, Topics: [][]byte{topic[:]}, Data: []byte{6}})
				if err := rawdb.WriteBlock(bc.db, block); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info}); err != nil {
					t.Fatal(err)
				}
				bc.currentBlock.Store(block)
				if storage != "hot" {
					dir := t.TempDir()
					var ref snapshots.SegmentRef
					var err error
					if storage == "legacy-cold" {
						ref, err = snapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "", 1, 1)
					} else {
						ref, err = snapshots.BuildEventLogV4SegmentFromChain(bc.ChainDB(), dir, "", 1, 1)
					}
					if err != nil {
						t.Fatal(err)
					}
					index, err := snapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []snapshots.SegmentRef{ref}, "")
					if err != nil {
						t.Fatal(err)
					}
					if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, []snapshots.SegmentRef{ref, index})); err != nil {
						t.Fatal(err)
					}
					mgr, err := snapshots.OpenManager(dir)
					if err != nil {
						t.Fatal(err)
					}
					bc.ChainDB().SetEventLogReader(mgr)
					if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
						t.Fatal(err)
					}
				}
				server := jsonrpc.NewServer(&TronBackend{chain: bc}, 0)
				defer server.Stop()
				http := httptest.NewServer(server.Handler())
				defer http.Close()
				query := func(address any, topics []any) any {
					f := map[string]any{"fromBlock": "0x1", "toBlock": "0x1", "topics": topics}
					if address != nil {
						f["address"] = address
					}
					r := postCoreJSONRPC(t, http.URL, "eth_getLogs", []any{f})
					if r["error"] != nil {
						t.Fatalf("RPC error: %v", r)
					}
					return r["result"]
				}
				want := query(nil, nil)
				if rows, ok := want.([]any); !ok || len(rows) != 1 {
					t.Fatalf("unfiltered: %v", want)
				}
				addr := fmt.Sprintf("0x%x", address)
				for _, filter := range []any{addr, []string{fmt.Sprintf("0x%x", bytes20(0x99)), addr}} {
					got := query(filter, []any{fmt.Sprintf("0x%x", topic[:])})
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("filtered=%v unfiltered=%v", got, want)
					}
				}
				for _, got := range []any{query(fmt.Sprintf("0x%x", bytes20(0x99)), nil), query(addr, []any{fmt.Sprintf("0x%x", common.Hash{0xff})})} {
					if rows, ok := got.([]any); !ok || len(rows) != 0 {
						t.Fatalf("false positive: %v", got)
					}
				}
			})
		}
	}
}
