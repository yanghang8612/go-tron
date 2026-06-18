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
	apipb "github.com/tronprotocol/go-tron/proto/api"
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
	liveAddr                 common.Address
	solidAddr                common.Address
	gotAt                    uint64 // last blockNum passed to GetAccountAt
	liveAccountIDCalls       int
	accountIDAtBlock         uint64
	liveAccountNetCalls      int
	accountNetAtBlock        uint64
	liveMarketOrderCalls     int
	marketOrderAtBlock       uint64
	liveMarketOrdersCalls    int
	marketOrdersAtBlock      uint64
	liveMarketPriceCalls     int
	marketPriceAtBlock       uint64
	liveExchangeCalls        int
	exchangesAtBlock         uint64
	liveDelegatedCalls       int
	liveDelegationIndexCalls int
	delegatedAtBlock         uint64
	delegationIndexAtBlock   uint64
}

func (s *isolationStubBackend) GetAccount(addr common.Address) (*types.Account, error) {
	return types.NewAccount(s.liveAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	s.gotAt = blockNum
	return types.NewAccount(s.solidAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	s.liveAccountIDCalls++
	return types.NewAccount(s.liveAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountByIdAt(accountID []byte, blockNum uint64) (*types.Account, error) {
	s.accountIDAtBlock = blockNum
	return types.NewAccount(s.solidAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error) {
	s.liveAccountNetCalls++
	return &apipb.AccountNetMessage{FreeNetUsed: 1}, nil
}

func (s *isolationStubBackend) GetAccountNetAt(addr common.Address, blockNum uint64) (*apipb.AccountNetMessage, error) {
	s.accountNetAtBlock = blockNum
	return &apipb.AccountNetMessage{FreeNetUsed: 9, NetUsed: 99}, nil
}

func (s *isolationStubBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
	s.liveMarketOrderCalls++
	return &corepb.MarketOrder{OrderId: []byte("live-order"), SellTokenQuantity: 1, BuyTokenQuantity: 2}
}

func (s *isolationStubBackend) GetMarketOrderByIDAt(orderID []byte, blockNum uint64) (*corepb.MarketOrder, error) {
	s.marketOrderAtBlock = blockNum
	return &corepb.MarketOrder{
		OrderId:           []byte("bound-order"),
		OwnerAddress:      common.Address{0x41, 0x51}.Bytes(),
		SellTokenId:       []byte("sell"),
		SellTokenQuantity: 9,
		BuyTokenId:        []byte("buy"),
		BuyTokenQuantity:  99,
		State:             corepb.MarketOrder_ACTIVE,
	}, nil
}

func (s *isolationStubBackend) GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder {
	s.liveMarketOrdersCalls++
	return []*corepb.MarketOrder{{OrderId: []byte("live-account-order"), SellTokenQuantity: 3}}
}

func (s *isolationStubBackend) GetMarketOrdersByAccountAt(addr common.Address, blockNum uint64) ([]*corepb.MarketOrder, error) {
	s.marketOrdersAtBlock = blockNum
	return []*corepb.MarketOrder{{
		OrderId:           []byte("bound-account-order"),
		OwnerAddress:      addr.Bytes(),
		SellTokenId:       []byte("sell"),
		SellTokenQuantity: 11,
		BuyTokenId:        []byte("buy"),
		BuyTokenQuantity:  111,
		State:             corepb.MarketOrder_ACTIVE,
	}}, nil
}

func (s *isolationStubBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	s.liveMarketPriceCalls++
	return &corepb.MarketPriceList{
		SellTokenId: sellTokenID,
		BuyTokenId:  buyTokenID,
		Prices:      []*corepb.MarketPrice{{SellTokenQuantity: 4, BuyTokenQuantity: 5}},
	}
}

func (s *isolationStubBackend) GetMarketPriceByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	s.marketPriceAtBlock = blockNum
	return &corepb.MarketPriceList{
		SellTokenId: sellTokenID,
		BuyTokenId:  buyTokenID,
		Prices:      []*corepb.MarketPrice{{SellTokenQuantity: 12, BuyTokenQuantity: 120}},
	}, nil
}

func (s *isolationStubBackend) ListExchanges() ([]*corepb.Exchange, error) {
	s.liveExchangeCalls++
	return []*corepb.Exchange{{ExchangeId: 1, FirstTokenBalance: 1, SecondTokenBalance: 2}}, nil
}

func (s *isolationStubBackend) ListExchangesAt(blockNum uint64) ([]*corepb.Exchange, error) {
	s.exchangesAtBlock = blockNum
	return []*corepb.Exchange{{
		ExchangeId:         9,
		FirstTokenId:       []byte("solid"),
		FirstTokenBalance:  90,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 900,
		CreatorAddress:     common.Address{0x41, 0x61}.Bytes(),
	}}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceV2(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	s.liveDelegatedCalls++
	return []*tronapi.DelegatedResourceInfo{{
		FromAddress:               hex.EncodeToString(from.Bytes()),
		ToAddress:                 hex.EncodeToString(to.Bytes()),
		FrozenBalanceForBandwidth: 1,
	}}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceV2At(from, to common.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	s.delegatedAtBlock = blockNum
	return []*tronapi.DelegatedResourceInfo{{
		FromAddress:            hex.EncodeToString(from.Bytes()),
		ToAddress:              hex.EncodeToString(to.Bytes()),
		FrozenBalanceForEnergy: 9,
		ExpireTimeForEnergy:    99,
		ExpireTimeForBandwidth: 88,
	}}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	s.liveDelegationIndexCalls++
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr.Bytes()),
		ToAddresses: []string{hex.EncodeToString(common.Address{0x41, 0x7f}.Bytes())},
	}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceAccountIndexV2At(addr common.Address, blockNum uint64) (*tronapi.DelegationIndexInfo, error) {
	s.delegationIndexAtBlock = blockNum
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr.Bytes()),
		ToAddresses: []string{hex.EncodeToString(common.Address{0x41, 0x8f}.Bytes())},
	}, nil
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

func TestSolidityAccountByIdUsesSolidBoundArchivePath(t *testing.T) {
	liveAddr := common.Address{0x41, 0x01}
	solidAddr := common.Address{0x41, 0x02}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
		liveAddr:         liveAddr,
		solidAddr:        solidAddr,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getaccountbyid", "application/json", strings.NewReader(`{"account_id":"user1234"}`))
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
	if stub.accountIDAtBlock != 42 {
		t.Fatalf("GetAccountByIdAt block = %d; want solidNum=42", stub.accountIDAtBlock)
	}
	if stub.liveAccountIDCalls != 0 {
		t.Fatalf("live GetAccountById called %d times, want 0", stub.liveAccountIDCalls)
	}
}

func TestPbftAccountByIdUsesPbftBoundArchivePath(t *testing.T) {
	liveAddr := common.Address{0x41, 0x01}
	solidAddr := common.Address{0x41, 0x02}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
		liveAddr:         liveAddr,
		solidAddr:        solidAddr,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/getaccountbyid", "application/json", strings.NewReader(`{"account_id":"user1234"}`))
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
		t.Fatalf("response address = %q; want pbft sentinel %x (live would be %x)",
			addrField, solidAddr.Bytes(), liveAddr.Bytes())
	}
	if stub.accountIDAtBlock != 13 {
		t.Fatalf("GetAccountByIdAt block = %d; want pbftNum=13", stub.accountIDAtBlock)
	}
	if stub.liveAccountIDCalls != 0 {
		t.Fatalf("live GetAccountById called %d times, want 0", stub.liveAccountIDCalls)
	}
}

func TestSolidityAccountNetUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getaccountnet", "application/json", strings.NewReader(`{"address":"411234567890"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["freeNetUsed"] != "9" || got["NetUsed"] != "99" {
		t.Fatalf("getaccountnet = %+v, want bound sentinel freeNetUsed=9 NetUsed=99", got)
	}
	if stub.accountNetAtBlock != 42 {
		t.Fatalf("GetAccountNetAt block = %d; want solidNum=42", stub.accountNetAtBlock)
	}
	if stub.liveAccountNetCalls != 0 {
		t.Fatalf("live GetAccountNet called %d times, want 0", stub.liveAccountNetCalls)
	}
}

func TestPbftAccountNetUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/getaccountnet", "application/json", strings.NewReader(`{"address":"411234567890"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["freeNetUsed"] != "9" || got["NetUsed"] != "99" {
		t.Fatalf("getaccountnet = %+v, want bound sentinel freeNetUsed=9 NetUsed=99", got)
	}
	if stub.accountNetAtBlock != 13 {
		t.Fatalf("GetAccountNetAt block = %d; want pbftNum=13", stub.accountNetAtBlock)
	}
	if stub.liveAccountNetCalls != 0 {
		t.Fatalf("live GetAccountNet called %d times, want 0", stub.liveAccountNetCalls)
	}
}

func TestSolidityMarketRoutesUseSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertMarketRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftMarketRoutesUsePbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertMarketRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertMarketRoutesUseBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Post(prefix+"/getmarketorderbyid", "application/json", strings.NewReader(`{"value":"6f72646572"}`))
	if err != nil {
		t.Fatalf("getmarketorderbyid request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getmarketorderbyid status: %d", resp.StatusCode)
	}
	var order struct {
		SellTokenQuantity int64 `json:"sell_token_quantity"`
		BuyTokenQuantity  int64 `json:"buy_token_quantity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	if order.SellTokenQuantity != 9 || order.BuyTokenQuantity != 99 {
		t.Fatalf("market order = %+v, want bound sentinel", order)
	}
	if stub.marketOrderAtBlock != wantBlock {
		t.Fatalf("GetMarketOrderByIDAt block = %d, want %d", stub.marketOrderAtBlock, wantBlock)
	}
	if stub.liveMarketOrderCalls != 0 {
		t.Fatalf("live GetMarketOrderByID called %d times, want 0", stub.liveMarketOrderCalls)
	}

	resp, err = http.Post(prefix+"/getmarketordersfromaccount", "application/json", strings.NewReader(`{"address":"411234567890"}`))
	if err != nil {
		t.Fatalf("getmarketordersfromaccount request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getmarketordersfromaccount status: %d", resp.StatusCode)
	}
	var orders struct {
		Orders []struct {
			SellTokenQuantity int64 `json:"sell_token_quantity"`
			BuyTokenQuantity  int64 `json:"buy_token_quantity"`
		} `json:"orders"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&orders); err != nil {
		t.Fatal(err)
	}
	if len(orders.Orders) != 1 || orders.Orders[0].SellTokenQuantity != 11 || orders.Orders[0].BuyTokenQuantity != 111 {
		t.Fatalf("market orders = %+v, want bound sentinel", orders.Orders)
	}
	if stub.marketOrdersAtBlock != wantBlock {
		t.Fatalf("GetMarketOrdersByAccountAt block = %d, want %d", stub.marketOrdersAtBlock, wantBlock)
	}
	if stub.liveMarketOrdersCalls != 0 {
		t.Fatalf("live GetMarketOrdersByAccount called %d times, want 0", stub.liveMarketOrdersCalls)
	}

	resp, err = http.Post(prefix+"/getmarketpricebypair", "application/json", strings.NewReader(`{"sell_token_id":"73656c6c","buy_token_id":"627579"}`))
	if err != nil {
		t.Fatalf("getmarketpricebypair request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getmarketpricebypair status: %d", resp.StatusCode)
	}
	var prices struct {
		Prices []struct {
			SellTokenQuantity int64 `json:"sell_token_quantity"`
			BuyTokenQuantity  int64 `json:"buy_token_quantity"`
		} `json:"prices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&prices); err != nil {
		t.Fatal(err)
	}
	if len(prices.Prices) != 1 || prices.Prices[0].SellTokenQuantity != 12 || prices.Prices[0].BuyTokenQuantity != 120 {
		t.Fatalf("market prices = %+v, want bound sentinel", prices.Prices)
	}
	if stub.marketPriceAtBlock != wantBlock {
		t.Fatalf("GetMarketPriceByPairAt block = %d, want %d", stub.marketPriceAtBlock, wantBlock)
	}
	if stub.liveMarketPriceCalls != 0 {
		t.Fatalf("live GetMarketPriceByPair called %d times, want 0", stub.liveMarketPriceCalls)
	}
}

func TestSolidityListExchangesUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertListExchangesUsesBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftListExchangesUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertListExchangesUsesBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertListExchangesUsesBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Get(prefix + "/listexchanges")
	if err != nil {
		t.Fatalf("listexchanges request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listexchanges status: %d", resp.StatusCode)
	}
	var got struct {
		Exchanges []struct {
			ExchangeID         int64 `json:"exchange_id"`
			FirstTokenBalance  int64 `json:"first_token_balance"`
			SecondTokenBalance int64 `json:"second_token_balance"`
		} `json:"exchanges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Exchanges) != 1 || got.Exchanges[0].ExchangeID != 9 || got.Exchanges[0].FirstTokenBalance != 90 {
		t.Fatalf("exchanges = %+v, want bound sentinel", got.Exchanges)
	}
	if stub.exchangesAtBlock != wantBlock {
		t.Fatalf("ListExchangesAt block = %d, want %d", stub.exchangesAtBlock, wantBlock)
	}
	if stub.liveExchangeCalls != 0 {
		t.Fatalf("live ListExchanges called %d times, want 0", stub.liveExchangeCalls)
	}
}

func TestSolidityDelegationRoutesUseSolidBoundArchivePath(t *testing.T) {
	from := common.Address{0x41, 0x10}
	to := common.Address{0x41, 0x20}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	body := `{"fromAddress":"` + hex.EncodeToString(from.Bytes()) + `","toAddress":"` + hex.EncodeToString(to.Bytes()) + `"}`
	resp, err := http.Post(srv.URL+"/walletsolidity/getdelegatedresourcev2", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got struct {
		DelegatedResource []tronapi.DelegatedResourceInfo `json:"delegatedResource"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.DelegatedResource) != 1 || got.DelegatedResource[0].FrozenBalanceForEnergy != 9 {
		t.Fatalf("delegatedResource = %+v, want solid-bound sentinel", got.DelegatedResource)
	}
	if stub.delegatedAtBlock != 42 {
		t.Fatalf("GetDelegatedResourceV2At block = %d, want solidNum=42", stub.delegatedAtBlock)
	}
	if stub.liveDelegatedCalls != 0 {
		t.Fatalf("live GetDelegatedResourceV2 called %d times, want 0", stub.liveDelegatedCalls)
	}

	indexBody := `{"value":"` + hex.EncodeToString(from.Bytes()) + `"}`
	resp, err = http.Post(srv.URL+"/walletsolidity/getdelegatedresourceaccountindexv2", "application/json", strings.NewReader(indexBody))
	if err != nil {
		t.Fatalf("index request failed: %v", err)
	}
	defer resp.Body.Close()
	var idx tronapi.DelegationIndexInfo
	if err := json.NewDecoder(resp.Body).Decode(&idx); err != nil {
		t.Fatal(err)
	}
	if len(idx.ToAddresses) != 1 || !strings.EqualFold(idx.ToAddresses[0], hex.EncodeToString(common.Address{0x41, 0x8f}.Bytes())) {
		t.Fatalf("delegation index = %+v, want solid-bound sentinel", idx)
	}
	if stub.delegationIndexAtBlock != 42 {
		t.Fatalf("GetDelegatedResourceAccountIndexV2At block = %d, want solidNum=42", stub.delegationIndexAtBlock)
	}
	if stub.liveDelegationIndexCalls != 0 {
		t.Fatalf("live GetDelegatedResourceAccountIndexV2 called %d times, want 0", stub.liveDelegationIndexCalls)
	}
}

func TestPbftDelegationRoutesUsePbftBoundArchivePath(t *testing.T) {
	from := common.Address{0x41, 0x30}
	to := common.Address{0x41, 0x40}
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	body := `{"fromAddress":"` + hex.EncodeToString(from.Bytes()) + `","toAddress":"` + hex.EncodeToString(to.Bytes()) + `"}`
	resp, err := http.Post(srv.URL+"/walletpbft/getdelegatedresourcev2", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var got struct {
		DelegatedResource []tronapi.DelegatedResourceInfo `json:"delegatedResource"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.DelegatedResource) != 1 || got.DelegatedResource[0].FrozenBalanceForEnergy != 9 {
		t.Fatalf("delegatedResource = %+v, want pbft-bound sentinel", got.DelegatedResource)
	}
	if stub.delegatedAtBlock != 13 {
		t.Fatalf("GetDelegatedResourceV2At block = %d, want pbftNum=13", stub.delegatedAtBlock)
	}
	if stub.liveDelegatedCalls != 0 {
		t.Fatalf("live GetDelegatedResourceV2 called %d times, want 0", stub.liveDelegatedCalls)
	}
}
