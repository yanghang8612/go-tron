package tronapi_test

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	tronapi "github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// solidStubBackend wraps stubBackend with a custom solid/pbft block number.
type solidStubBackend struct {
	stubBackend
	solidNum uint64
	pbftNum  int64
}

func (s *solidStubBackend) SolidifiedBlockNum() uint64 { return s.solidNum }
func (s *solidStubBackend) LatestPbftBlockNum() int64  { return s.pbftNum }

func newSolidTestServer(t *testing.T, stub tronapi.Backend) *httptest.Server {
	t.Helper()
	api := tronapi.NewAPI(stub)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux) // already fans out to RegisterSolidityRoutes + PbftRoutes
	return httptest.NewServer(mux)
}

// TestSolidityGetNowBlock_routeExists verifies /walletsolidity/getnowblock is registered
// and returns 404 when the solid block (num=0) is not in the chain (stub returns nil).
func TestSolidityGetNowBlock_routeExists(t *testing.T) {
	stub := &solidStubBackend{solidNum: 0, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getnowblock")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	// stubBackend.GetBlockByNumber returns nil → 404
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// TestSolidityGetBlockByNum_rejectsAboveSolid checks that requesting block #5
// when solidNum=3 returns 404.
func TestSolidityGetBlockByNum_rejectsAboveSolid(t *testing.T) {
	stub := &solidStubBackend{solidNum: 3, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getblockbynum?num=5")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for block above solid, got %d", resp.StatusCode)
	}
}

// TestPbftGetNowBlock_fallsBackToSolid checks that /walletpbft/getnowblock falls back to
// the solid block when LatestPbftBlockNum returns -1.
func TestPbftGetNowBlock_fallsBackToSolid(t *testing.T) {
	stub := &solidStubBackend{solidNum: 2, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	// Both solid and pbft getnowblock call GetBlockByNumber(2) → stub returns nil → 404
	resp, err := http.Get(srv.URL + "/walletpbft/getnowblock")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 (fallback to solid, stub nil), got %d", resp.StatusCode)
	}
}

// TestPbftGetBlockByNum_rejectsAbovePbft checks that requesting block #10
// when pbftNum=7 returns 404.
func TestPbftGetBlockByNum_rejectsAbovePbft(t *testing.T) {
	stub := &solidStubBackend{solidNum: 5, pbftNum: 7}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletpbft/getblockbynum?num=10")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for block above pbft, got %d", resp.StatusCode)
	}
}

// TestSolidityAccount_routeExists verifies /walletsolidity/getaccount is registered.
func TestSolidityAccount_routeExists(t *testing.T) {
	stub := &solidStubBackend{solidNum: 0, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getaccount?address=411234567890")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	// stub returns nil account → empty {} with 200
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// isolationStubBackend lets a test detect which Backend method ran. Each
// path returns a sentinel address in the Account.proto so the test can
// inspect the JSON response and verify routing.
type isolationStubBackend struct {
	solidStubBackend
	liveAddr  common.Address
	solidAddr common.Address
	gotAt     uint64 // last blockNum passed to GetAccountAt
}

func (s *isolationStubBackend) GetAccount(addr common.Address) (*types.Account, error) {
	return types.NewAccount(s.liveAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	s.gotAt = blockNum
	return types.NewAccount(s.solidAddr, corepb.AccountType_Normal), nil
}

// TestSolidityAccount_isolation verifies the audit's P1 fix:
// /walletsolidity/getaccount must call Backend.GetAccountAt(_, solidBound)
// rather than the live Backend.GetAccount. Pre-fix the live handler ran
// directly on the solid route, so the response was current-head state and
// indistinguishable from /wallet/getaccount — the audit's "fall through
// to live wallet handler" finding.
func TestSolidityAccount_isolation(t *testing.T) {
	liveAddr := common.Address{0x41, 0x01}
	solidAddr := common.Address{0x41, 0x02}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
		liveAddr:         liveAddr,
		solidAddr:        solidAddr,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getaccount?address=411234567890")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	// Expect the solid sentinel address in the response (hex-encoded).
	addrField, _ := got["address"].(string)
	if !strings.EqualFold(addrField, hex.EncodeToString(solidAddr.Bytes())) {
		t.Fatalf("response address = %q; want solid sentinel %x (live would be %x)",
			addrField, solidAddr.Bytes(), liveAddr.Bytes())
	}
	if stub.gotAt != 42 {
		t.Fatalf("GetAccountAt called with blockNum=%d; want solidNum=42", stub.gotAt)
	}
}

// TestPbftAccount_isolation: same shape as TestSolidityAccount_isolation,
// but via the /walletpbft/ route. The pbft bound takes precedence over
// the solid one (and falls back to solid when pbftNum < 0).
func TestPbftAccount_isolation(t *testing.T) {
	liveAddr := common.Address{0x41, 0x01}
	solidAddr := common.Address{0x41, 0x02}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
		liveAddr:         liveAddr,
		solidAddr:        solidAddr,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletpbft/getaccount?address=411234567890")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	addrField, _ := got["address"].(string)
	if !strings.EqualFold(addrField, hex.EncodeToString(solidAddr.Bytes())) {
		t.Fatalf("response address = %q; want solid sentinel %x (live would be %x)",
			addrField, solidAddr.Bytes(), liveAddr.Bytes())
	}
	if stub.gotAt != 13 {
		t.Fatalf("GetAccountAt called with blockNum=%d; want pbftNum=13", stub.gotAt)
	}
}
