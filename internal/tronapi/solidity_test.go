package tronapi_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	tronapi "github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// solidStubBackend wraps stubBackend with a custom solid/pbft block number.
type solidStubBackend struct {
	stubBackend
	solidNum uint64
	pbftNum  int64
}

func (s *solidStubBackend) SolidifiedBlockNum() uint64 { return s.solidNum }
func (s *solidStubBackend) LatestPbftBlockNum() int64  { return s.pbftNum }

type boundBlockStubBackend struct {
	solidStubBackend
	block          *types.Block
	hashCalls      int
	rangeErr       error
	rangeCalls     int
	lastRangeStart uint64
	lastRangeEnd   uint64
	txBlockNum     uint64
	txBlockOK      bool
	txBlockCalls   int
	txCalls        int
	txInfoCalls    int
	tx             *corepb.Transaction
	txInfo         *corepb.TransactionInfo
}

func (s *boundBlockStubBackend) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	s.hashCalls++
	if s.block != nil && s.block.Hash() == hash {
		return s.block, nil
	}
	return nil, nil
}

func (s *boundBlockStubBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	s.rangeCalls++
	s.lastRangeStart = start
	s.lastRangeEnd = end
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	if s.block == nil {
		return nil, nil
	}
	n := s.block.Number()
	if n >= start && n < end {
		return []*types.Block{s.block}, nil
	}
	return nil, nil
}

func (s *boundBlockStubBackend) GetTransactionBlockNumByID(hash common.Hash) (uint64, bool, error) {
	s.txBlockCalls++
	return s.txBlockNum, s.txBlockOK, nil
}

func (s *boundBlockStubBackend) GetTransactionByID(hash common.Hash) (*corepb.Transaction, error) {
	s.txCalls++
	return s.tx, nil
}

func (s *boundBlockStubBackend) GetTransactionInfoByID(hash common.Hash) (*corepb.TransactionInfo, error) {
	s.txInfoCalls++
	return s.txInfo, nil
}

func testBlockWithNumber(number int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: number},
		},
	})
}

func testTransactionHash(fill byte) common.Hash {
	var hash common.Hash
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

func newSolidTestServer(t *testing.T, stub tronapi.Backend) *httptest.Server {
	t.Helper()
	api := tronapi.NewAPI(stub)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux) // already fans out to RegisterSolidityRoutes + PbftRoutes
	return httptest.NewServer(mux)
}

func TestSolidityGetNowBlockNotFoundReturnsEmpty(t *testing.T) {
	stub := &solidStubBackend{solidNum: 0, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getnowblock")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
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

func TestSolidityGetBlockByNumBackendErrorReturnsEmpty(t *testing.T) {
	stub := &solidStubBackend{
		stubBackend: stubBackend{blockErr: errors.New("rawdb: block 2 decode: corrupt")},
		solidNum:    3,
		pbftNum:     -1,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getblockbynum?num=2")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
}

func TestSolidityGetAccountBackendErrorReturnsInternal(t *testing.T) {
	stub := &solidStubBackend{
		stubBackend: stubBackend{accountAtErr: errors.New("state history: cold account segment corrupt")},
		solidNum:    3,
		pbftNum:     -1,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getaccount", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccount status = %d, want 500", resp.StatusCode)
	}
}

func TestSolidityGetAccountByIdBackendErrorReturnsInternal(t *testing.T) {
	stub := &solidStubBackend{
		stubBackend: stubBackend{accountIDAtErr: errors.New("state history: cold account-id index corrupt")},
		solidNum:    3,
		pbftNum:     -1,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getaccountbyid", "application/json", strings.NewReader(`{"account_id":"user1234"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccountbyid status = %d, want 500", resp.StatusCode)
	}
}

func TestSolidityBalanceTraceRoutesUseSolidBoundGate(t *testing.T) {
	stub := &solidStubBackend{solidNum: 42, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertBalanceTraceRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftBalanceTraceRoutesUsePbftBoundGate(t *testing.T) {
	stub := &solidStubBackend{solidNum: 42, pbftNum: 13}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertBalanceTraceRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertBalanceTraceRoutesUseBound(t *testing.T, prefix string, stub *solidStubBackend, wantBlock uint64) {
	t.Helper()

	hashHex := strings.Repeat("11", common.HashLength)
	hashBytes, err := hex.DecodeString(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	stub.accountBalanceResp = &contractpb.AccountBalanceResponse{
		Balance: 123,
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Number: int64(wantBlock),
			Hash:   hashBytes,
		},
	}
	stub.blockBalanceTrace = &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Number: int64(wantBlock),
			Hash:   hashBytes,
		},
		Timestamp: 456,
	}

	accountBody := balanceTraceAccountBody(wantBlock, hashHex)
	resp, err := http.Post(prefix+"/getaccountbalance", "application/json", strings.NewReader(accountBody))
	if err != nil {
		t.Fatalf("getaccountbalance request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getaccountbalance status = %d, want 200", resp.StatusCode)
	}
	if stub.lastAccountBalanceReq == nil || stub.lastAccountBalanceReq.GetBlockIdentifier().GetNumber() != int64(wantBlock) {
		t.Fatalf("GetAccountBalanceTrace request = %+v, want block %d", stub.lastAccountBalanceReq, wantBlock)
	}

	stub.lastAccountBalanceReq = nil
	resp, err = http.Post(prefix+"/getaccountbalance", "application/json", strings.NewReader(balanceTraceAccountBody(wantBlock+1, hashHex)))
	if err != nil {
		t.Fatalf("future getaccountbalance request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("future getaccountbalance status = %d, want 404", resp.StatusCode)
	}
	if stub.lastAccountBalanceReq != nil {
		t.Fatalf("future getaccountbalance called backend with request %+v", stub.lastAccountBalanceReq)
	}

	blockBody := balanceTraceBlockBody(wantBlock, hashHex)
	resp, err = http.Post(prefix+"/getblockbalancetrace", "application/json", strings.NewReader(blockBody))
	if err != nil {
		t.Fatalf("getblockbalancetrace request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getblockbalancetrace status = %d, want 200", resp.StatusCode)
	}
	if stub.lastBlockBalanceTraceID == nil || stub.lastBlockBalanceTraceID.GetNumber() != int64(wantBlock) {
		t.Fatalf("GetBlockBalanceTrace id = %+v, want block %d", stub.lastBlockBalanceTraceID, wantBlock)
	}

	stub.lastBlockBalanceTraceID = nil
	resp, err = http.Post(prefix+"/getblockbalancetrace", "application/json", strings.NewReader(balanceTraceBlockBody(wantBlock+1, hashHex)))
	if err != nil {
		t.Fatalf("future getblockbalancetrace request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("future getblockbalancetrace status = %d, want 404", resp.StatusCode)
	}
	if stub.lastBlockBalanceTraceID != nil {
		t.Fatalf("future getblockbalancetrace called backend with id %+v", stub.lastBlockBalanceTraceID)
	}
}

func balanceTraceAccountBody(blockNum uint64, hash string) string {
	return `{"account_identifier":{"address":"4100000000000000000000000000000000000000aa"},"block_identifier":{"number":` +
		strconv.FormatUint(blockNum, 10) + `,"hash":"` + hash + `"}}`
}

func balanceTraceBlockBody(blockNum uint64, hash string) string {
	return `{"number":` + strconv.FormatUint(blockNum, 10) + `,"hash":"` + hash + `"}`
}

func TestSolidityGetContractBackendErrorReturnsInternal(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 3, pbftNum: -1},
		contractAtErr:    errors.New("state history: cold contract metadata corrupt"),
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getcontract", "application/json", strings.NewReader(`{"value":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getcontract status = %d, want 500", resp.StatusCode)
	}
	if stub.contractAtBlock != 3 {
		t.Fatalf("GetContractAt block = %d, want solid block 3", stub.contractAtBlock)
	}
}

func TestSolidityGetTransactionInfoByBlockNumSurfacesBackendError(t *testing.T) {
	stub := &solidStubBackend{
		stubBackend: stubBackend{txInfoByBlockErr: errors.New("rawdb: transaction info block 2 decode: corrupt")},
		solidNum:    3,
		pbftNum:     -1,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/gettransactioninfobyblocknum", "application/json", strings.NewReader(`{"num":2}`))
	if err != nil {
		t.Fatalf("POST walletsolidity/gettransactioninfobyblocknum: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("walletsolidity/gettransactioninfobyblocknum status = %d, want 500", resp.StatusCode)
	}
}

// TestPbftGetNowBlockNotFoundReturnsEmpty checks that /walletpbft/getnowblock
// falls back to the solid block number and returns java-compatible empty JSON
// when the backend has no block for that number.
func TestPbftGetNowBlockNotFoundReturnsEmpty(t *testing.T) {
	stub := &solidStubBackend{solidNum: 2, pbftNum: -1}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletpbft/getnowblock")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
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

func TestSolidityGetBlockByIDRejectsAboveSolidBeforeBackend(t *testing.T) {
	block := testBlockWithNumber(10)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: -1},
		block:            block,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getblockbyid", "application/json", strings.NewReader(`{"value":"`+block.Hash().Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for block id above solid, got %d", resp.StatusCode)
	}
	if stub.hashCalls != 0 {
		t.Fatalf("GetBlockByHash called %d times for unsolidified block id, want 0", stub.hashCalls)
	}
}

func TestPbftGetBlockByIDRejectsAbovePbftBeforeBackend(t *testing.T) {
	block := testBlockWithNumber(10)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 7},
		block:            block,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/getblockbyid", "application/json", strings.NewReader(`{"value":"`+block.Hash().Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for block id above pbft, got %d", resp.StatusCode)
	}
	if stub.hashCalls != 0 {
		t.Fatalf("GetBlockByHash called %d times for unconfirmed block id, want 0", stub.hashCalls)
	}
}

func TestSolidityGetBlockByLimitNextRejectsAboveSolidBeforeBackend(t *testing.T) {
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: -1},
		block:            testBlockWithNumber(5),
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getblockbylimitnext", "application/json", strings.NewReader(`{"startNum":4,"endNum":7}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for range above solid, got %d", resp.StatusCode)
	}
	if stub.rangeCalls != 0 {
		t.Fatalf("GetBlocksByRange called %d times for unsolidified range, want 0", stub.rangeCalls)
	}
}

func TestPbftGetBlockByLimitNextRejectsAbovePbftBeforeBackend(t *testing.T) {
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 7},
		block:            testBlockWithNumber(7),
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/getblockbylimitnext", "application/json", strings.NewReader(`{"startNum":6,"endNum":9}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for range above pbft, got %d", resp.StatusCode)
	}
	if stub.rangeCalls != 0 {
		t.Fatalf("GetBlocksByRange called %d times for unconfirmed range, want 0", stub.rangeCalls)
	}
}

func TestBoundGetBlockByLimitNextBackendErrorReturnsEmptyList(t *testing.T) {
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: -1},
		rangeErr:         errors.New("rawdb: block 4 decode: corrupt"),
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getblockbylimitnext", "application/json", strings.NewReader(`{"startNum":4,"endNum":6}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with empty block list, got %d", resp.StatusCode)
	}
	var body struct {
		Block []json.RawMessage `json:"block"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Block) != 0 {
		t.Fatalf("block list len = %d, want 0", len(body.Block))
	}
	if stub.rangeCalls != 1 || stub.lastRangeStart != 4 || stub.lastRangeEnd != 6 {
		t.Fatalf("range call = %d [%d,%d), want 1 [4,6)", stub.rangeCalls, stub.lastRangeStart, stub.lastRangeEnd)
	}
}

func TestBoundGetBlockByLimitNextRejectsInvalidRangeBeforeBackend(t *testing.T) {
	endpoints := []struct {
		name string
		path string
	}{
		{name: "solidity", path: "/walletsolidity/getblockbylimitnext"},
		{name: "pbft", path: "/walletpbft/getblockbylimitnext"},
	}
	ranges := []struct {
		name string
		body string
	}{
		{name: "empty", body: `{"startNum":5,"endNum":5}`},
		{name: "reversed", body: `{"startNum":6,"endNum":5}`},
	}
	for _, endpoint := range endpoints {
		for _, r := range ranges {
			t.Run(endpoint.name+"/"+r.name, func(t *testing.T) {
				stub := &boundBlockStubBackend{
					solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 20},
					block:            testBlockWithNumber(5),
				}
				srv := newSolidTestServer(t, stub)
				defer srv.Close()

				resp, err := http.Post(srv.URL+endpoint.path, "application/json", strings.NewReader(r.body))
				if err != nil {
					t.Fatalf("request failed: %v", err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
				}
				if stub.rangeCalls != 0 {
					t.Fatalf("GetBlocksByRange called %d times for invalid range, want 0", stub.rangeCalls)
				}
			})
		}
	}
}

func TestSolidityGetTransactionByIDRejectsAboveSolidBeforeBackend(t *testing.T) {
	hash := testTransactionHash(0x01)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: -1},
		txBlockNum:       10,
		txBlockOK:        true,
		tx:               &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1}},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/gettransactionbyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with empty JSON for tx above solid, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txCalls != 0 {
		t.Fatalf("GetTransactionByID called %d times for unsolidified tx, want 0", stub.txCalls)
	}
}

func TestSolidityGetTransactionByIDWithinSolidReadsBackend(t *testing.T) {
	hash := testTransactionHash(0x02)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 10, pbftNum: -1},
		txBlockNum:       10,
		txBlockOK:        true,
		tx:               &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 2}},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/gettransactionbyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for tx within solid, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txCalls != 1 {
		t.Fatalf("GetTransactionByID called %d times, want 1", stub.txCalls)
	}
}

func TestPbftGetTransactionInfoByIDRejectsAbovePbftBeforeBackend(t *testing.T) {
	hash := testTransactionHash(0x03)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 7},
		txBlockNum:       10,
		txBlockOK:        true,
		txInfo:           &corepb.TransactionInfo{Id: hash[:], BlockNumber: 10},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/gettransactioninfobyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with empty JSON for tx info above pbft, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txInfoCalls != 0 {
		t.Fatalf("GetTransactionInfoByID called %d times for unconfirmed tx info, want 0", stub.txInfoCalls)
	}
}

func TestPbftGetTransactionInfoByIDWithinPbftReadsBackend(t *testing.T) {
	hash := testTransactionHash(0x04)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 10},
		txBlockNum:       10,
		txBlockOK:        true,
		txInfo:           &corepb.TransactionInfo{Id: hash[:], BlockNumber: 10},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/gettransactioninfobyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for tx info within pbft, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txInfoCalls != 1 {
		t.Fatalf("GetTransactionInfoByID called %d times, want 1", stub.txInfoCalls)
	}
}

func TestSolidityGetTransactionReceiptByIDRejectsAboveSolidBeforeBackend(t *testing.T) {
	hash := testTransactionHash(0x05)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: -1},
		txBlockNum:       10,
		txBlockOK:        true,
		txInfo:           &corepb.TransactionInfo{Id: hash[:], BlockNumber: 10},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/gettransactionreceiptbyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with empty JSON for receipt above solid, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txInfoCalls != 0 {
		t.Fatalf("GetTransactionInfoByID called %d times for unsolidified receipt, want 0", stub.txInfoCalls)
	}
}

func TestPbftGetTransactionReceiptByIDWithinPbftReadsBackend(t *testing.T) {
	hash := testTransactionHash(0x06)
	stub := &boundBlockStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 20, pbftNum: 10},
		txBlockNum:       10,
		txBlockOK:        true,
		txInfo:           &corepb.TransactionInfo{Id: hash[:], BlockNumber: 10},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletpbft/gettransactionreceiptbyid", "application/json", strings.NewReader(`{"value":"`+hash.Hex()+`"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for receipt within pbft, got %d", resp.StatusCode)
	}
	if stub.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", stub.txBlockCalls)
	}
	if stub.txInfoCalls != 1 {
		t.Fatalf("GetTransactionInfoByID called %d times, want 1", stub.txInfoCalls)
	}
}

func TestSolidityRewardCamelAliasUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertRewardAliasUsesBound(t, srv.URL+"/walletsolidity/getReward", stub, 42)
}

func TestPbftRewardCamelAliasUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertRewardAliasUsesBound(t, srv.URL+"/walletpbft/getReward", stub, 13)
}

func assertRewardAliasUsesBound(t *testing.T, url string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Post(url, "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("getReward request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getReward status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Reward int64 `json:"reward"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Reward != 4242 {
		t.Fatalf("getReward reward = %d, want bound sentinel 4242", got.Reward)
	}
	if stub.rewardAtBlock != wantBlock {
		t.Fatalf("GetRewardAt block = %d, want %d", stub.rewardAtBlock, wantBlock)
	}
	if stub.liveRewardCalls != 0 {
		t.Fatalf("live GetReward called %d times, want 0", stub.liveRewardCalls)
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
	liveAddr                  common.Address
	solidAddr                 common.Address
	gotAt                     uint64 // last blockNum passed to GetAccountAt
	liveContractCalls         int
	contractAtBlock           uint64
	contractAtErr             error
	liveConstantCalls         int
	constantAtBlock           uint64
	liveEstimateCalls         int
	estimateAtBlock           uint64
	liveAccountIDCalls        int
	accountIDAtBlock          uint64
	liveAccountNetCalls       int
	accountNetAtBlock         uint64
	liveChainParameterCalls   int
	chainParametersAtBlock    uint64
	liveBurnCalls             int
	burnAtBlock               uint64
	liveBandwidthCalls        int
	bandwidthAtBlock          uint64
	liveEnergyCalls           int
	energyAtBlock             uint64
	liveBrokerageCalls        int
	brokerageAtBlock          uint64
	liveRewardCalls           int
	rewardAtBlock             uint64
	liveWitnessCalls          int
	witnessesAtBlock          uint64
	liveNextMaintenanceCalls  int
	nextMaintenanceAtBlock    uint64
	liveProposalCalls         int
	proposalsAtBlock          uint64
	liveProposalIDCalls       int
	proposalIDAtBlock         uint64
	proposalIDAtErr           error
	liveProposalPageCalls     int
	proposalPageAtBlock       uint64
	liveAssetIDCalls          int
	assetIDAtBlock            uint64
	liveAssetNameCalls        int
	assetNameAtBlock          uint64
	liveAssetListCalls        int
	assetListAtBlock          uint64
	liveAssetPageCalls        int
	assetPageAtBlock          uint64
	liveAssetAccountCalls     int
	assetAccountAtBlock       uint64
	liveMarketOrderCalls      int
	marketOrderAtBlock        uint64
	liveMarketOrdersCalls     int
	marketOrdersAtBlock       uint64
	liveMarketOrderPairCalls  int
	marketOrderPairAtBlock    uint64
	liveMarketPairListCalls   int
	marketPairListAtBlock     uint64
	liveMarketPriceCalls      int
	marketPriceAtBlock        uint64
	liveExchangeCalls         int
	exchangesAtBlock          uint64
	liveExchangePageCalls     int
	exchangePageAtBlock       uint64
	liveExchangeIDCalls       int
	exchangeIDAtBlock         uint64
	liveDelegatedCalls        int
	liveLegacyDelegatedCalls  int
	liveDelegationIndexCalls  int
	liveLegacyDelegIndexCalls int
	delegatedAtBlock          uint64
	legacyDelegatedAtBlock    uint64
	delegationIndexAtBlock    uint64
	legacyDelegIndexAtBlock   uint64
	liveCanDelegateCalls      int
	canDelegateAtBlock        uint64
	liveCanWithdrawCalls      int
	canWithdrawAtBlock        uint64
	liveAvailableCalls        int
	availableAtBlock          uint64
}

func (s *isolationStubBackend) GetAccount(addr common.Address) (*types.Account, error) {
	return types.NewAccount(s.liveAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	s.gotAt = blockNum
	return types.NewAccount(s.solidAddr, corepb.AccountType_Normal), nil
}

func (s *isolationStubBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	s.liveContractCalls++
	return &contractpb.SmartContract{
		ContractAddress: s.liveAddr.Bytes(),
		Name:            "live-contract",
		Bytecode:        []byte{0x01},
	}, nil
}

func (s *isolationStubBackend) GetContractAt(addr common.Address, blockNum uint64) (*contractpb.SmartContract, error) {
	s.contractAtBlock = blockNum
	if s.contractAtErr != nil {
		return nil, s.contractAtErr
	}
	return &contractpb.SmartContract{
		ContractAddress: s.solidAddr.Bytes(),
		Name:            "bound-contract",
		Bytecode:        []byte{0x09},
	}, nil
}

func (s *isolationStubBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	s.liveConstantCalls++
	return &tronapi.TriggerResult{Result: []byte("live"), EnergyUsed: 1}, nil
}

func (s *isolationStubBackend) TriggerConstantContractAt(owner, contract common.Address, data []byte, energyLimit int64, blockNum uint64) (*tronapi.TriggerResult, error) {
	s.constantAtBlock = blockNum
	return &tronapi.TriggerResult{Result: []byte("bound"), EnergyUsed: 99}, nil
}

func (s *isolationStubBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	s.liveEstimateCalls++
	return 1, nil
}

func (s *isolationStubBackend) EstimateEnergyAt(owner, contract common.Address, data []byte, blockNum uint64) (int64, error) {
	s.estimateAtBlock = blockNum
	return 88, nil
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

func (s *isolationStubBackend) GetChainParameters() []tronapi.ChainParameter {
	s.liveChainParameterCalls++
	return []tronapi.ChainParameter{{Key: "live_param", Value: 1}}
}

func (s *isolationStubBackend) GetChainParametersAt(blockNum uint64) ([]tronapi.ChainParameter, error) {
	s.chainParametersAtBlock = blockNum
	return []tronapi.ChainParameter{{Key: "bound_param", Value: 99}}, nil
}

func (s *isolationStubBackend) GetBrokerageInfo(addr common.Address) int64 {
	s.liveBrokerageCalls++
	return 1
}

func (s *isolationStubBackend) GetBrokerageInfoAt(addr common.Address, blockNum uint64) (int64, error) {
	s.brokerageAtBlock = blockNum
	return 88, nil
}

func (s *isolationStubBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) {
	s.liveRewardCalls++
	return &tronapi.RewardInfo{Reward: 1}, nil
}

func (s *isolationStubBackend) GetRewardAt(addr common.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	s.rewardAtBlock = blockNum
	return &tronapi.RewardInfo{Reward: 4242}, nil
}

func (s *isolationStubBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	s.liveWitnessCalls++
	return []*tronapi.WitnessInfo{{
		Address:   hex.EncodeToString(common.Address{0x41, 0x31}.Bytes()),
		VoteCount: 1,
		URL:       "live-witness",
	}}, nil
}

func (s *isolationStubBackend) ListWitnessesAt(blockNum uint64) ([]*tronapi.WitnessInfo, error) {
	s.witnessesAtBlock = blockNum
	return []*tronapi.WitnessInfo{{
		Address:   hex.EncodeToString(common.Address{0x41, 0x32}.Bytes()),
		VoteCount: 9,
		URL:       "bound-witness",
		IsJobs:    true,
	}}, nil
}

func (s *isolationStubBackend) NextMaintenanceTime() int64 {
	s.liveNextMaintenanceCalls++
	return 1
}

func (s *isolationStubBackend) NextMaintenanceTimeAt(blockNum uint64) (int64, error) {
	s.nextMaintenanceAtBlock = blockNum
	return 9900, nil
}

func (s *isolationStubBackend) GetBurnTrx() int64 {
	s.liveBurnCalls++
	return 1
}

func (s *isolationStubBackend) GetBurnTrxAt(blockNum uint64) (int64, error) {
	s.burnAtBlock = blockNum
	return 123456, nil
}

func (s *isolationStubBackend) GetBandwidthPrices() string {
	s.liveBandwidthCalls++
	return "live-bandwidth"
}

func (s *isolationStubBackend) GetBandwidthPricesAt(blockNum uint64) (string, error) {
	s.bandwidthAtBlock = blockNum
	return "0:10,42:20", nil
}

func (s *isolationStubBackend) GetEnergyPrices() string {
	s.liveEnergyCalls++
	return "live-energy"
}

func (s *isolationStubBackend) GetEnergyPricesAt(blockNum uint64) (string, error) {
	s.energyAtBlock = blockNum
	return "0:100,42:300", nil
}

func (s *isolationStubBackend) ListProposals() ([]*tronapi.ProposalInfo, error) {
	s.liveProposalCalls++
	return []*tronapi.ProposalInfo{{ProposalID: 1, State: "LIVE"}}, nil
}

func (s *isolationStubBackend) ListProposalsAt(blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	s.proposalsAtBlock = blockNum
	return []*tronapi.ProposalInfo{{ProposalID: 42, State: "APPROVED"}}, nil
}

func (s *isolationStubBackend) GetProposalByID(id int64) (*tronapi.ProposalInfo, error) {
	s.liveProposalIDCalls++
	return &tronapi.ProposalInfo{ProposalID: id, State: "LIVE"}, nil
}

func (s *isolationStubBackend) GetProposalByIDAt(id int64, blockNum uint64) (*tronapi.ProposalInfo, error) {
	s.proposalIDAtBlock = blockNum
	if s.proposalIDAtErr != nil {
		return nil, s.proposalIDAtErr
	}
	return &tronapi.ProposalInfo{ProposalID: id, State: "BOUND"}, nil
}

func (s *isolationStubBackend) ListProposalsPaginated(offset, limit int) ([]*tronapi.ProposalInfo, error) {
	s.liveProposalPageCalls++
	return []*tronapi.ProposalInfo{{ProposalID: 2, State: "LIVE"}}, nil
}

func (s *isolationStubBackend) ListProposalsPaginatedAt(offset, limit int, blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	s.proposalPageAtBlock = blockNum
	return []*tronapi.ProposalInfo{{ProposalID: 43, State: "PAGED"}}, nil
}

func assetSentinel(id string, supply int64) *contractpb.AssetIssueContract {
	return &contractpb.AssetIssueContract{
		Id:           id,
		OwnerAddress: common.Address{0x41, 0x71}.Bytes(),
		Name:         []byte(id),
		TotalSupply:  supply,
		TrxNum:       1,
		Num:          1,
	}
}

func (s *isolationStubBackend) GetAssetIssueByID(id int64) (*contractpb.AssetIssueContract, error) {
	s.liveAssetIDCalls++
	return assetSentinel("live-id", 1), nil
}

func (s *isolationStubBackend) GetAssetIssueByIDAt(id int64, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	s.assetIDAtBlock = blockNum
	return assetSentinel("bound-id", 9), nil
}

func (s *isolationStubBackend) GetAssetIssueByName(name []byte) (*contractpb.AssetIssueContract, error) {
	s.liveAssetNameCalls++
	return assetSentinel("live-name", 2), nil
}

func (s *isolationStubBackend) GetAssetIssueByNameAt(name []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	s.assetNameAtBlock = blockNum
	return assetSentinel("bound-name", 99), nil
}

func (s *isolationStubBackend) GetAssetIssueList() ([]*contractpb.AssetIssueContract, error) {
	s.liveAssetListCalls++
	return []*contractpb.AssetIssueContract{assetSentinel("live-list", 3)}, nil
}

func (s *isolationStubBackend) GetAssetIssueListAt(blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	s.assetListAtBlock = blockNum
	return []*contractpb.AssetIssueContract{assetSentinel("bound-list", 11)}, nil
}

func (s *isolationStubBackend) GetAssetIssueListPaginated(offset, limit int) ([]*contractpb.AssetIssueContract, error) {
	s.liveAssetPageCalls++
	return []*contractpb.AssetIssueContract{assetSentinel("live-page", 4)}, nil
}

func (s *isolationStubBackend) GetAssetIssueListPaginatedAt(offset, limit int, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	s.assetPageAtBlock = blockNum
	return []*contractpb.AssetIssueContract{assetSentinel("bound-page", 12)}, nil
}

func (s *isolationStubBackend) GetAssetIssueByAccount(addr common.Address) (*contractpb.AssetIssueContract, error) {
	s.liveAssetAccountCalls++
	return assetSentinel("live-account", 5), nil
}

func (s *isolationStubBackend) GetAssetIssueByAccountAt(addr common.Address, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	s.assetAccountAtBlock = blockNum
	return assetSentinel("bound-account", 77), nil
}

func (s *isolationStubBackend) GetMarketOrderByID(orderID []byte) (*corepb.MarketOrder, error) {
	s.liveMarketOrderCalls++
	return &corepb.MarketOrder{OrderId: []byte("live-order"), SellTokenQuantity: 1, BuyTokenQuantity: 2}, nil
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

func (s *isolationStubBackend) GetMarketOrdersByAccount(addr common.Address) ([]*corepb.MarketOrder, error) {
	s.liveMarketOrdersCalls++
	return []*corepb.MarketOrder{{OrderId: []byte("live-account-order"), SellTokenQuantity: 3}}, nil
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

func (s *isolationStubBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) (*corepb.MarketPriceList, error) {
	s.liveMarketPriceCalls++
	return &corepb.MarketPriceList{
		SellTokenId: sellTokenID,
		BuyTokenId:  buyTokenID,
		Prices:      []*corepb.MarketPrice{{SellTokenQuantity: 4, BuyTokenQuantity: 5}},
	}, nil
}

func (s *isolationStubBackend) GetMarketPriceByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	s.marketPriceAtBlock = blockNum
	return &corepb.MarketPriceList{
		SellTokenId: sellTokenID,
		BuyTokenId:  buyTokenID,
		Prices:      []*corepb.MarketPrice{{SellTokenQuantity: 12, BuyTokenQuantity: 120}},
	}, nil
}

func (s *isolationStubBackend) GetMarketOrderListByPair(sellTokenID, buyTokenID []byte) ([]*corepb.MarketOrder, error) {
	s.liveMarketOrderPairCalls++
	return []*corepb.MarketOrder{{OrderId: []byte("live-pair-order"), SellTokenQuantity: 6}}, nil
}

func (s *isolationStubBackend) GetMarketOrderListByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) ([]*corepb.MarketOrder, error) {
	s.marketOrderPairAtBlock = blockNum
	return []*corepb.MarketOrder{{
		OrderId:           []byte("bound-pair-order"),
		SellTokenId:       sellTokenID,
		SellTokenQuantity: 13,
		BuyTokenId:        buyTokenID,
		BuyTokenQuantity:  130,
		State:             corepb.MarketOrder_ACTIVE,
	}}, nil
}

func (s *isolationStubBackend) GetMarketPairList() (*corepb.MarketOrderPairList, error) {
	s.liveMarketPairListCalls++
	return &corepb.MarketOrderPairList{OrderPair: []*corepb.MarketOrderPair{{
		SellTokenId: []byte("live-sell"),
		BuyTokenId:  []byte("live-buy"),
	}}}, nil
}

func (s *isolationStubBackend) GetMarketPairListAt(blockNum uint64) (*corepb.MarketOrderPairList, error) {
	s.marketPairListAtBlock = blockNum
	return &corepb.MarketOrderPairList{OrderPair: []*corepb.MarketOrderPair{{
		SellTokenId: []byte("solid-sell"),
		BuyTokenId:  []byte("solid-buy"),
	}}}, nil
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

func (s *isolationStubBackend) ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error) {
	s.liveExchangePageCalls++
	return []*corepb.Exchange{{ExchangeId: 2, FirstTokenBalance: 2, SecondTokenBalance: 3}}, nil
}

func (s *isolationStubBackend) ListExchangesPaginatedAt(offset, limit int, blockNum uint64) ([]*corepb.Exchange, error) {
	s.exchangePageAtBlock = blockNum
	return []*corepb.Exchange{{
		ExchangeId:         10,
		FirstTokenId:       []byte("page"),
		FirstTokenBalance:  100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 1000,
		CreatorAddress:     common.Address{0x41, 0x62}.Bytes(),
	}}, nil
}

func (s *isolationStubBackend) GetExchangeByID(id int64) (*corepb.Exchange, error) {
	s.liveExchangeIDCalls++
	return &corepb.Exchange{ExchangeId: id, FirstTokenBalance: 3, SecondTokenBalance: 4}, nil
}

func (s *isolationStubBackend) GetExchangeByIDAt(id int64, blockNum uint64) (*corepb.Exchange, error) {
	s.exchangeIDAtBlock = blockNum
	return &corepb.Exchange{
		ExchangeId:         id,
		FirstTokenId:       []byte("one"),
		FirstTokenBalance:  110,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 1100,
		CreatorAddress:     common.Address{0x41, 0x63}.Bytes(),
	}, nil
}

func (s *isolationStubBackend) GetDelegatedResource(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	s.liveLegacyDelegatedCalls++
	return []*tronapi.DelegatedResourceInfo{{
		FromAddress:               hex.EncodeToString(from.Bytes()),
		ToAddress:                 hex.EncodeToString(to.Bytes()),
		FrozenBalanceForBandwidth: 2,
	}}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceAt(from, to common.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	s.legacyDelegatedAtBlock = blockNum
	return []*tronapi.DelegatedResourceInfo{{
		FromAddress:               hex.EncodeToString(from.Bytes()),
		ToAddress:                 hex.EncodeToString(to.Bytes()),
		FrozenBalanceForBandwidth: 7,
		ExpireTimeForBandwidth:    77,
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

func (s *isolationStubBackend) GetDelegatedResourceAccountIndex(addr common.Address) (*corepb.DelegatedResourceAccountIndex, error) {
	s.liveLegacyDelegIndexCalls++
	return &corepb.DelegatedResourceAccountIndex{
		Account:    addr.Bytes(),
		ToAccounts: [][]byte{common.Address{0x41, 0x6f}.Bytes()},
	}, nil
}

func (s *isolationStubBackend) GetDelegatedResourceAccountIndexAt(addr common.Address, blockNum uint64) (*corepb.DelegatedResourceAccountIndex, error) {
	s.legacyDelegIndexAtBlock = blockNum
	return &corepb.DelegatedResourceAccountIndex{
		Account:      addr.Bytes(),
		ToAccounts:   [][]byte{common.Address{0x41, 0x9f}.Bytes()},
		FromAccounts: [][]byte{common.Address{0x41, 0xaf}.Bytes()},
	}, nil
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

func (s *isolationStubBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	s.liveCanDelegateCalls++
	return &tronapi.CanDelegateInfo{MaxSize: 1, CanDelegateSize: 1, Balance: amount}, nil
}

func (s *isolationStubBackend) CanDelegateResourceAt(addr common.Address, amount int64, resource corepb.ResourceCode, blockNum uint64) (*tronapi.CanDelegateInfo, error) {
	s.canDelegateAtBlock = blockNum
	return &tronapi.CanDelegateInfo{MaxSize: 900, CanDelegateSize: 700, Balance: amount}, nil
}

func (s *isolationStubBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	s.liveCanWithdrawCalls++
	return &tronapi.CanWithdrawUnfreezeInfo{Amount: 1}, nil
}

func (s *isolationStubBackend) GetCanWithdrawUnfreezeAmountAt(addr common.Address, timestamp int64, blockNum uint64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	s.canWithdrawAtBlock = blockNum
	return &tronapi.CanWithdrawUnfreezeInfo{Amount: 5000}, nil
}

func (s *isolationStubBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	s.liveAvailableCalls++
	return &tronapi.AvailableUnfreezeCountInfo{Count: 1}, nil
}

func (s *isolationStubBackend) GetAvailableUnfreezeCountAt(addr common.Address, blockNum uint64) (*tronapi.AvailableUnfreezeCountInfo, error) {
	s.availableAtBlock = blockNum
	return &tronapi.AvailableUnfreezeCountInfo{Count: 29}, nil
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

func TestSolidityAccountNetSurfacesBackendError(t *testing.T) {
	stub := &solidStubBackend{
		stubBackend: stubBackend{accountNetAtErr: errors.New("state history: cold account net dynamic properties corrupt")},
		solidNum:    42,
		pbftNum:     -1,
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/walletsolidity/getaccountnet", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for backend account net error, got %d", resp.StatusCode)
	}
}

func TestSolidityGetContractUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
		solidAddr:        common.Address{0x41, 0x42},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertGetContractUsesBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftGetContractUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
		solidAddr:        common.Address{0x41, 0x13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertGetContractUsesBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertGetContractUsesBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Get(prefix + "/getcontract?value=411234567890")
	if err != nil {
		t.Fatalf("getcontract request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getcontract status: %d", resp.StatusCode)
	}
	var got struct {
		Name     string `json:"name"`
		Bytecode string `json:"bytecode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "bound-contract" {
		t.Fatalf("contract name = %q, want bound-contract", got.Name)
	}
	if stub.contractAtBlock != wantBlock {
		t.Fatalf("GetContractAt block = %d, want %d", stub.contractAtBlock, wantBlock)
	}
	if stub.liveContractCalls != 0 {
		t.Fatalf("live GetContract called %d times, want 0", stub.liveContractCalls)
	}
}

func TestSolidityConstantExecutionUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertConstantExecutionUsesBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftConstantExecutionUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertConstantExecutionUsesBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertConstantExecutionUsesBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	body := `{"owner_address":"411111111111111111111111111111111111111111","contract_address":"412222222222222222222222222222222222222222","data":"00"}`
	resp, err := http.Post(prefix+"/triggerconstantcontract", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("triggerconstantcontract request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("triggerconstantcontract status: %d", resp.StatusCode)
	}
	var trigger struct {
		EnergyUsed     int64    `json:"energy_used"`
		ConstantResult []string `json:"constant_result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&trigger); err != nil {
		t.Fatal(err)
	}
	if trigger.EnergyUsed != 99 || len(trigger.ConstantResult) != 1 || trigger.ConstantResult[0] != "626f756e64" {
		t.Fatalf("trigger response = %+v, want bound result/energy", trigger)
	}
	if stub.constantAtBlock != wantBlock {
		t.Fatalf("TriggerConstantContractAt block = %d, want %d", stub.constantAtBlock, wantBlock)
	}
	if stub.liveConstantCalls != 0 {
		t.Fatalf("live TriggerConstantContract called %d times, want 0", stub.liveConstantCalls)
	}

	resp, err = http.Post(prefix+"/estimateenergy", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("estimateenergy request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("estimateenergy status: %d", resp.StatusCode)
	}
	var estimate struct {
		EnergyRequired int64 `json:"energy_required"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&estimate); err != nil {
		t.Fatal(err)
	}
	if estimate.EnergyRequired != 88 {
		t.Fatalf("energy_required = %d, want 88", estimate.EnergyRequired)
	}
	if stub.estimateAtBlock != wantBlock {
		t.Fatalf("EstimateEnergyAt block = %d, want %d", stub.estimateAtBlock, wantBlock)
	}
	if stub.liveEstimateCalls != 0 {
		t.Fatalf("live EstimateEnergy called %d times, want 0", stub.liveEstimateCalls)
	}
}

func TestSolidityBrokerageUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertBrokerageUsesBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftBrokerageUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertBrokerageUsesBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertBrokerageUsesBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Get(prefix + "/getbrokerage?address=411234567890")
	if err != nil {
		t.Fatalf("getbrokerage request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getbrokerage status: %d", resp.StatusCode)
	}
	var got struct {
		Brokerage int64 `json:"brokerage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Brokerage != 88 {
		t.Fatalf("brokerage = %d, want bound sentinel 88", got.Brokerage)
	}
	if stub.brokerageAtBlock != wantBlock {
		t.Fatalf("GetBrokerageInfoAt block = %d, want %d", stub.brokerageAtBlock, wantBlock)
	}
	if stub.liveBrokerageCalls != 0 {
		t.Fatalf("live GetBrokerageInfo called %d times, want 0", stub.liveBrokerageCalls)
	}
}

func TestSolidityListWitnessesUsesSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertListWitnessesUsesBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftListWitnessesUsesPbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertListWitnessesUsesBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertListWitnessesUsesBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Get(prefix + "/listwitnesses")
	if err != nil {
		t.Fatalf("listwitnesses request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listwitnesses status: %d", resp.StatusCode)
	}
	var got struct {
		Witnesses []tronapi.WitnessInfo `json:"witnesses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Witnesses) != 1 || got.Witnesses[0].URL != "bound-witness" || got.Witnesses[0].VoteCount != 9 {
		t.Fatalf("witnesses = %+v, want bound sentinel", got.Witnesses)
	}
	if !got.Witnesses[0].IsJobs {
		t.Fatalf("witness IsJobs = false, want true")
	}
	if stub.witnessesAtBlock != wantBlock {
		t.Fatalf("ListWitnessesAt block = %d, want %d", stub.witnessesAtBlock, wantBlock)
	}
	if stub.liveWitnessCalls != 0 {
		t.Fatalf("live ListWitnesses called %d times, want 0", stub.liveWitnessCalls)
	}

	resp, err = http.Post(prefix+"/getpaginatednowwitnesslist", "application/json", strings.NewReader(`{"offset":0,"limit":1}`))
	if err != nil {
		t.Fatalf("getpaginatednowwitnesslist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getpaginatednowwitnesslist status: %d", resp.StatusCode)
	}
	var page struct {
		Witnesses []tronapi.WitnessInfo `json:"witnesses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Witnesses) != 1 || page.Witnesses[0].URL != "bound-witness" || page.Witnesses[0].VoteCount != 9 {
		t.Fatalf("paginated witnesses = %+v, want bound sentinel", page.Witnesses)
	}
	if stub.witnessesAtBlock != wantBlock {
		t.Fatalf("ListWitnessesAt block after paginated call = %d, want %d", stub.witnessesAtBlock, wantBlock)
	}
	if stub.liveWitnessCalls != 0 {
		t.Fatalf("live ListWitnesses called %d times after paginated call, want 0", stub.liveWitnessCalls)
	}
}

func TestSolidityProposalRoutesUseSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertProposalRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftProposalRoutesUsePbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertProposalRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertProposalRoutesUseBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Post(prefix+"/listproposals", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("listproposals request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listproposals status: %d", resp.StatusCode)
	}
	var list struct {
		Proposals []tronapi.ProposalInfo `json:"proposals"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Proposals) != 1 || list.Proposals[0].ProposalID != 42 || list.Proposals[0].State != "APPROVED" {
		t.Fatalf("proposals = %+v, want bound sentinel", list.Proposals)
	}
	if stub.proposalsAtBlock != wantBlock {
		t.Fatalf("ListProposalsAt block = %d, want %d", stub.proposalsAtBlock, wantBlock)
	}
	if stub.liveProposalCalls != 0 {
		t.Fatalf("live ListProposals called %d times, want 0", stub.liveProposalCalls)
	}

	resp, err = http.Get(prefix + "/getproposalbyid?id=42")
	if err != nil {
		t.Fatalf("getproposalbyid request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getproposalbyid status: %d", resp.StatusCode)
	}
	var one tronapi.ProposalInfo
	if err := json.NewDecoder(resp.Body).Decode(&one); err != nil {
		t.Fatal(err)
	}
	if one.ProposalID != 42 || one.State != "BOUND" {
		t.Fatalf("proposal = %+v, want bound sentinel", one)
	}
	if stub.proposalIDAtBlock != wantBlock {
		t.Fatalf("GetProposalByIDAt block = %d, want %d", stub.proposalIDAtBlock, wantBlock)
	}
	if stub.liveProposalIDCalls != 0 {
		t.Fatalf("live GetProposalByID called %d times, want 0", stub.liveProposalIDCalls)
	}

	resp, err = http.Post(prefix+"/getpaginatedproposallist", "application/json", strings.NewReader(`{"offset":0,"limit":1}`))
	if err != nil {
		t.Fatalf("getpaginatedproposallist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getpaginatedproposallist status: %d", resp.StatusCode)
	}
	var page struct {
		Proposal []tronapi.ProposalInfo `json:"proposal"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Proposal) != 1 || page.Proposal[0].ProposalID != 43 || page.Proposal[0].State != "PAGED" {
		t.Fatalf("proposal page = %+v, want bound sentinel", page.Proposal)
	}
	if stub.proposalPageAtBlock != wantBlock {
		t.Fatalf("ListProposalsPaginatedAt block = %d, want %d", stub.proposalPageAtBlock, wantBlock)
	}
	if stub.liveProposalPageCalls != 0 {
		t.Fatalf("live ListProposalsPaginated called %d times, want 0", stub.liveProposalPageCalls)
	}
}

func TestSolidityGetProposalByIdSurfacesBackendError(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
		proposalIDAtErr:  errors.New("state history: cold proposal segment corrupt"),
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/walletsolidity/getproposalbyid?id=42")
	if err != nil {
		t.Fatalf("getproposalbyid request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getproposalbyid status = %d, want 500", resp.StatusCode)
	}
	if stub.proposalIDAtBlock != 42 {
		t.Fatalf("GetProposalByIDAt block = %d, want solid block 42", stub.proposalIDAtBlock)
	}
	if stub.liveProposalIDCalls != 0 {
		t.Fatalf("live GetProposalByID called %d times, want 0", stub.liveProposalIDCalls)
	}
}

func TestSolidityDynamicPropertyRoutesUseSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertDynamicPropertyRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftDynamicPropertyRoutesUsePbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertDynamicPropertyRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertDynamicPropertyRoutesUseBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Get(prefix + "/getchainparameters")
	if err != nil {
		t.Fatalf("getchainparameters request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getchainparameters status: %d", resp.StatusCode)
	}
	var params struct {
		ChainParameter []tronapi.ChainParameter `json:"chainParameter"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&params); err != nil {
		t.Fatal(err)
	}
	if len(params.ChainParameter) != 1 ||
		params.ChainParameter[0].Key != "bound_param" ||
		params.ChainParameter[0].Value != 99 {
		t.Fatalf("chain parameters = %+v, want bound sentinel", params.ChainParameter)
	}
	if stub.chainParametersAtBlock != wantBlock {
		t.Fatalf("GetChainParametersAt block = %d, want %d", stub.chainParametersAtBlock, wantBlock)
	}
	if stub.liveChainParameterCalls != 0 {
		t.Fatalf("live GetChainParameters called %d times, want 0", stub.liveChainParameterCalls)
	}

	resp, err = http.Get(prefix + "/getnextmaintenancetime")
	if err != nil {
		t.Fatalf("getnextmaintenancetime request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getnextmaintenancetime status: %d", resp.StatusCode)
	}
	var next struct {
		Num int64 `json:"num"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&next); err != nil {
		t.Fatal(err)
	}
	if next.Num != 9900 {
		t.Fatalf("next maintenance time = %d, want 9900", next.Num)
	}
	if stub.nextMaintenanceAtBlock != wantBlock {
		t.Fatalf("NextMaintenanceTimeAt block = %d, want %d", stub.nextMaintenanceAtBlock, wantBlock)
	}
	if stub.liveNextMaintenanceCalls != 0 {
		t.Fatalf("live NextMaintenanceTime called %d times, want 0", stub.liveNextMaintenanceCalls)
	}

	resp, err = http.Get(prefix + "/getburntrx")
	if err != nil {
		t.Fatalf("getburntrx request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getburntrx status: %d", resp.StatusCode)
	}
	var burn struct {
		Num int64 `json:"num"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&burn); err != nil {
		t.Fatal(err)
	}
	if burn.Num != 123456 {
		t.Fatalf("burn trx = %d, want 123456", burn.Num)
	}
	if stub.burnAtBlock != wantBlock {
		t.Fatalf("GetBurnTrxAt block = %d, want %d", stub.burnAtBlock, wantBlock)
	}
	if stub.liveBurnCalls != 0 {
		t.Fatalf("live GetBurnTrx called %d times, want 0", stub.liveBurnCalls)
	}

	resp, err = http.Get(prefix + "/getbandwidthprices")
	if err != nil {
		t.Fatalf("getbandwidthprices request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getbandwidthprices status: %d", resp.StatusCode)
	}
	var bandwidth struct {
		Prices string `json:"prices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bandwidth); err != nil {
		t.Fatal(err)
	}
	if bandwidth.Prices != "0:10,42:20" {
		t.Fatalf("bandwidth prices = %q, want bound sentinel", bandwidth.Prices)
	}
	if stub.bandwidthAtBlock != wantBlock {
		t.Fatalf("GetBandwidthPricesAt block = %d, want %d", stub.bandwidthAtBlock, wantBlock)
	}
	if stub.liveBandwidthCalls != 0 {
		t.Fatalf("live GetBandwidthPrices called %d times, want 0", stub.liveBandwidthCalls)
	}

	resp, err = http.Get(prefix + "/getenergyprices")
	if err != nil {
		t.Fatalf("getenergyprices request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getenergyprices status: %d", resp.StatusCode)
	}
	var energy struct {
		Prices string `json:"prices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&energy); err != nil {
		t.Fatal(err)
	}
	if energy.Prices != "0:100,42:300" {
		t.Fatalf("energy prices = %q, want bound sentinel", energy.Prices)
	}
	if stub.energyAtBlock != wantBlock {
		t.Fatalf("GetEnergyPricesAt block = %d, want %d", stub.energyAtBlock, wantBlock)
	}
	if stub.liveEnergyCalls != 0 {
		t.Fatalf("live GetEnergyPrices called %d times, want 0", stub.liveEnergyCalls)
	}
}

func TestSolidityAssetRoutesUseSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertAssetRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftAssetRoutesUsePbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertAssetRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertAssetRoutesUseBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()

	resp, err := http.Post(prefix+"/getassetissuebyid", "application/json", strings.NewReader(`{"value":1000001}`))
	if err != nil {
		t.Fatalf("getassetissuebyid request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getassetissuebyid status: %d", resp.StatusCode)
	}
	var byID struct {
		ID          string `json:"id"`
		TotalSupply int64  `json:"total_supply"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&byID); err != nil {
		t.Fatal(err)
	}
	if byID.ID != "bound-id" || byID.TotalSupply != 9 {
		t.Fatalf("asset by id = %+v, want bound sentinel", byID)
	}
	if stub.assetIDAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueByIDAt block = %d, want %d", stub.assetIDAtBlock, wantBlock)
	}
	if stub.liveAssetIDCalls != 0 {
		t.Fatalf("live GetAssetIssueByID called %d times, want 0", stub.liveAssetIDCalls)
	}

	resp, err = http.Post(prefix+"/getassetissuebyname", "application/json", strings.NewReader(`{"value":"544f4b454e"}`))
	if err != nil {
		t.Fatalf("getassetissuebyname request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getassetissuebyname status: %d", resp.StatusCode)
	}
	var byName struct {
		ID          string `json:"id"`
		TotalSupply int64  `json:"total_supply"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&byName); err != nil {
		t.Fatal(err)
	}
	if byName.ID != "bound-name" || byName.TotalSupply != 99 {
		t.Fatalf("asset by name = %+v, want bound sentinel", byName)
	}
	if stub.assetNameAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueByNameAt block = %d, want %d", stub.assetNameAtBlock, wantBlock)
	}
	if stub.liveAssetNameCalls != 0 {
		t.Fatalf("live GetAssetIssueByName called %d times, want 0", stub.liveAssetNameCalls)
	}

	resp, err = http.Post(prefix+"/getassetissuelistbyname", "application/json", strings.NewReader(`{"value":"544f4b454e"}`))
	if err != nil {
		t.Fatalf("getassetissuelistbyname request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getassetissuelistbyname status: %d", resp.StatusCode)
	}
	var listByName struct {
		AssetIssue []struct {
			ID          string `json:"id"`
			TotalSupply int64  `json:"total_supply"`
		} `json:"assetIssue"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listByName); err != nil {
		t.Fatal(err)
	}
	if len(listByName.AssetIssue) != 1 || listByName.AssetIssue[0].ID != "bound-name" || listByName.AssetIssue[0].TotalSupply != 99 {
		t.Fatalf("asset list by name = %+v, want bound name sentinel", listByName.AssetIssue)
	}
	if stub.assetNameAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueByNameAt block after list-by-name = %d, want %d", stub.assetNameAtBlock, wantBlock)
	}
	if stub.liveAssetNameCalls != 0 {
		t.Fatalf("live GetAssetIssueByName called %d times after list-by-name, want 0", stub.liveAssetNameCalls)
	}

	resp, err = http.Get(prefix + "/getassetissuelist")
	if err != nil {
		t.Fatalf("getassetissuelist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getassetissuelist status: %d", resp.StatusCode)
	}
	var list struct {
		AssetIssue []struct {
			ID          string `json:"id"`
			TotalSupply int64  `json:"total_supply"`
		} `json:"assetIssue"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.AssetIssue) != 1 || list.AssetIssue[0].ID != "bound-list" || list.AssetIssue[0].TotalSupply != 11 {
		t.Fatalf("asset list = %+v, want bound sentinel", list.AssetIssue)
	}
	if stub.assetListAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueListAt block = %d, want %d", stub.assetListAtBlock, wantBlock)
	}
	if stub.liveAssetListCalls != 0 {
		t.Fatalf("live GetAssetIssueList called %d times, want 0", stub.liveAssetListCalls)
	}

	resp, err = http.Post(prefix+"/getpaginatedassetissuelist", "application/json", strings.NewReader(`{"offset":0,"limit":10}`))
	if err != nil {
		t.Fatalf("getpaginatedassetissuelist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getpaginatedassetissuelist status: %d", resp.StatusCode)
	}
	var page struct {
		AssetIssue []struct {
			ID          string `json:"id"`
			TotalSupply int64  `json:"total_supply"`
		} `json:"assetIssue"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.AssetIssue) != 1 || page.AssetIssue[0].ID != "bound-page" || page.AssetIssue[0].TotalSupply != 12 {
		t.Fatalf("asset page = %+v, want bound sentinel", page.AssetIssue)
	}
	if stub.assetPageAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueListPaginatedAt block = %d, want %d", stub.assetPageAtBlock, wantBlock)
	}
	if stub.liveAssetPageCalls != 0 {
		t.Fatalf("live GetAssetIssueListPaginated called %d times, want 0", stub.liveAssetPageCalls)
	}

	resp, err = http.Post(prefix+"/getassetissuebyaccount", "application/json", strings.NewReader(`{"address":"410000000000000000000000000000000000000071"}`))
	if err != nil {
		t.Fatalf("getassetissuebyaccount request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getassetissuebyaccount status: %d", resp.StatusCode)
	}
	var byAccount struct {
		ID          string `json:"id"`
		TotalSupply int64  `json:"total_supply"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&byAccount); err != nil {
		t.Fatal(err)
	}
	if byAccount.ID != "bound-account" || byAccount.TotalSupply != 77 {
		t.Fatalf("asset by account = %+v, want bound sentinel", byAccount)
	}
	if stub.assetAccountAtBlock != wantBlock {
		t.Fatalf("GetAssetIssueByAccountAt block = %d, want %d", stub.assetAccountAtBlock, wantBlock)
	}
	if stub.liveAssetAccountCalls != 0 {
		t.Fatalf("live GetAssetIssueByAccount called %d times, want 0", stub.liveAssetAccountCalls)
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

	resp, err = http.Post(prefix+"/getmarketorderlistbypair", "application/json", strings.NewReader(`{"sell_token_id":"73656c6c","buy_token_id":"627579"}`))
	if err != nil {
		t.Fatalf("getmarketorderlistbypair request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getmarketorderlistbypair status: %d", resp.StatusCode)
	}
	var pairOrders struct {
		Orders []struct {
			SellTokenQuantity int64 `json:"sell_token_quantity"`
			BuyTokenQuantity  int64 `json:"buy_token_quantity"`
		} `json:"orders"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pairOrders); err != nil {
		t.Fatal(err)
	}
	if len(pairOrders.Orders) != 1 || pairOrders.Orders[0].SellTokenQuantity != 13 || pairOrders.Orders[0].BuyTokenQuantity != 130 {
		t.Fatalf("market pair orders = %+v, want bound sentinel", pairOrders.Orders)
	}
	if stub.marketOrderPairAtBlock != wantBlock {
		t.Fatalf("GetMarketOrderListByPairAt block = %d, want %d", stub.marketOrderPairAtBlock, wantBlock)
	}
	if stub.liveMarketOrderPairCalls != 0 {
		t.Fatalf("live GetMarketOrderListByPair called %d times, want 0", stub.liveMarketOrderPairCalls)
	}

	resp, err = http.Post(prefix+"/getmarketpairlist", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("getmarketpairlist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getmarketpairlist status: %d", resp.StatusCode)
	}
	var pairs struct {
		OrderPair []struct {
			SellTokenID string `json:"sell_token_id"`
			BuyTokenID  string `json:"buy_token_id"`
		} `json:"orderPair"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		t.Fatal(err)
	}
	if len(pairs.OrderPair) != 1 ||
		pairs.OrderPair[0].SellTokenID != hex.EncodeToString([]byte("solid-sell")) ||
		pairs.OrderPair[0].BuyTokenID != hex.EncodeToString([]byte("solid-buy")) {
		t.Fatalf("market pair list = %+v, want bound sentinel", pairs.OrderPair)
	}
	if stub.marketPairListAtBlock != wantBlock {
		t.Fatalf("GetMarketPairListAt block = %d, want %d", stub.marketPairListAtBlock, wantBlock)
	}
	if stub.liveMarketPairListCalls != 0 {
		t.Fatalf("live GetMarketPairList called %d times, want 0", stub.liveMarketPairListCalls)
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

	resp, err = http.Post(prefix+"/getpaginatedexchangelist", "application/json", strings.NewReader(`{"offset":0,"limit":1}`))
	if err != nil {
		t.Fatalf("getpaginatedexchangelist request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getpaginatedexchangelist status: %d", resp.StatusCode)
	}
	var page struct {
		Exchanges []struct {
			ExchangeID         int64 `json:"exchange_id"`
			FirstTokenBalance  int64 `json:"first_token_balance"`
			SecondTokenBalance int64 `json:"second_token_balance"`
		} `json:"exchanges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Exchanges) != 1 || page.Exchanges[0].ExchangeID != 10 || page.Exchanges[0].FirstTokenBalance != 100 {
		t.Fatalf("exchange page = %+v, want bound sentinel", page.Exchanges)
	}
	if stub.exchangePageAtBlock != wantBlock {
		t.Fatalf("ListExchangesPaginatedAt block = %d, want %d", stub.exchangePageAtBlock, wantBlock)
	}
	if stub.liveExchangePageCalls != 0 {
		t.Fatalf("live ListExchangesPaginated called %d times, want 0", stub.liveExchangePageCalls)
	}

	resp, err = http.Post(prefix+"/getexchangebyid", "application/json", strings.NewReader(`{"id":11}`))
	if err != nil {
		t.Fatalf("getexchangebyid request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getexchangebyid status: %d", resp.StatusCode)
	}
	var one struct {
		ExchangeID         int64 `json:"exchange_id"`
		FirstTokenBalance  int64 `json:"first_token_balance"`
		SecondTokenBalance int64 `json:"second_token_balance"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&one); err != nil {
		t.Fatal(err)
	}
	if one.ExchangeID != 11 || one.FirstTokenBalance != 110 {
		t.Fatalf("exchange by id = %+v, want bound sentinel", one)
	}
	if stub.exchangeIDAtBlock != wantBlock {
		t.Fatalf("GetExchangeByIDAt block = %d, want %d", stub.exchangeIDAtBlock, wantBlock)
	}
	if stub.liveExchangeIDCalls != 0 {
		t.Fatalf("live GetExchangeByID called %d times, want 0", stub.liveExchangeIDCalls)
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
	resp, err := http.Post(srv.URL+"/walletsolidity/getdelegatedresource", "application/json", strings.NewReader(body))
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
	if len(got.DelegatedResource) != 1 || got.DelegatedResource[0].FrozenBalanceForBandwidth != 7 {
		t.Fatalf("legacy delegatedResource = %+v, want solid-bound sentinel", got.DelegatedResource)
	}
	if stub.legacyDelegatedAtBlock != 42 {
		t.Fatalf("GetDelegatedResourceAt block = %d, want solidNum=42", stub.legacyDelegatedAtBlock)
	}
	if stub.liveLegacyDelegatedCalls != 0 {
		t.Fatalf("live GetDelegatedResource called %d times, want 0", stub.liveLegacyDelegatedCalls)
	}

	resp, err = http.Post(srv.URL+"/walletsolidity/getdelegatedresourcev2", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("v2 request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("v2 status: %d", resp.StatusCode)
	}
	got = struct {
		DelegatedResource []tronapi.DelegatedResourceInfo `json:"delegatedResource"`
	}{}
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
	resp, err = http.Post(srv.URL+"/walletsolidity/getdelegatedresourceaccountindex", "application/json", strings.NewReader(indexBody))
	if err != nil {
		t.Fatalf("index request failed: %v", err)
	}
	defer resp.Body.Close()
	var legacyIdx struct {
		ToAccounts   []string `json:"toAccounts"`
		FromAccounts []string `json:"fromAccounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&legacyIdx); err != nil {
		t.Fatal(err)
	}
	if len(legacyIdx.ToAccounts) != 1 || !strings.EqualFold(legacyIdx.ToAccounts[0], hex.EncodeToString(common.Address{0x41, 0x9f}.Bytes())) ||
		len(legacyIdx.FromAccounts) != 1 || !strings.EqualFold(legacyIdx.FromAccounts[0], hex.EncodeToString(common.Address{0x41, 0xaf}.Bytes())) {
		t.Fatalf("legacy delegation index = %+v, want solid-bound sentinel", legacyIdx)
	}
	if stub.legacyDelegIndexAtBlock != 42 {
		t.Fatalf("GetDelegatedResourceAccountIndexAt block = %d, want solidNum=42", stub.legacyDelegIndexAtBlock)
	}
	if stub.liveLegacyDelegIndexCalls != 0 {
		t.Fatalf("live GetDelegatedResourceAccountIndex called %d times, want 0", stub.liveLegacyDelegIndexCalls)
	}

	resp, err = http.Post(srv.URL+"/walletsolidity/getdelegatedresourceaccountindexv2", "application/json", strings.NewReader(indexBody))
	if err != nil {
		t.Fatalf("v2 index request failed: %v", err)
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
	resp, err := http.Post(srv.URL+"/walletpbft/getdelegatedresource", "application/json", strings.NewReader(body))
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
	if len(got.DelegatedResource) != 1 || got.DelegatedResource[0].FrozenBalanceForBandwidth != 7 {
		t.Fatalf("legacy delegatedResource = %+v, want pbft-bound sentinel", got.DelegatedResource)
	}
	if stub.legacyDelegatedAtBlock != 13 {
		t.Fatalf("GetDelegatedResourceAt block = %d, want pbftNum=13", stub.legacyDelegatedAtBlock)
	}
	if stub.liveLegacyDelegatedCalls != 0 {
		t.Fatalf("live GetDelegatedResource called %d times, want 0", stub.liveLegacyDelegatedCalls)
	}

	resp, err = http.Post(srv.URL+"/walletpbft/getdelegatedresourcev2", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("v2 request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("v2 status: %d", resp.StatusCode)
	}
	got = struct {
		DelegatedResource []tronapi.DelegatedResourceInfo `json:"delegatedResource"`
	}{}
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

func TestSolidityStakeResourceRoutesUseSolidBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 42, pbftNum: -1},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertStakeResourceRoutesUseBound(t, srv.URL+"/walletsolidity", stub, 42)
}

func TestPbftStakeResourceRoutesUsePbftBoundArchivePath(t *testing.T) {
	stub := &isolationStubBackend{
		solidStubBackend: solidStubBackend{solidNum: 5, pbftNum: 13},
	}
	srv := newSolidTestServer(t, stub)
	defer srv.Close()

	assertStakeResourceRoutesUseBound(t, srv.URL+"/walletpbft", stub, 13)
}

func assertStakeResourceRoutesUseBound(t *testing.T, prefix string, stub *isolationStubBackend, wantBlock uint64) {
	t.Helper()
	addr := hex.EncodeToString(common.Address{0x41, 0x90}.Bytes())

	resp, err := http.Post(prefix+"/candelegateresource", "application/json", strings.NewReader(`{"owner_address":"`+addr+`","balance":123,"type":0}`))
	if err != nil {
		t.Fatalf("candelegateresource request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("candelegateresource status: %d", resp.StatusCode)
	}
	var canDelegate tronapi.CanDelegateInfo
	if err := json.NewDecoder(resp.Body).Decode(&canDelegate); err != nil {
		t.Fatal(err)
	}
	if canDelegate.MaxSize != 900 || canDelegate.CanDelegateSize != 700 || canDelegate.Balance != 123 {
		t.Fatalf("candelegateresource = %+v, want bound sentinel", canDelegate)
	}
	if stub.canDelegateAtBlock != wantBlock {
		t.Fatalf("CanDelegateResourceAt block = %d, want %d", stub.canDelegateAtBlock, wantBlock)
	}
	if stub.liveCanDelegateCalls != 0 {
		t.Fatalf("live CanDelegateResource called %d times, want 0", stub.liveCanDelegateCalls)
	}

	resp, err = http.Post(prefix+"/getcanwithdrawunfreezeamount", "application/json", strings.NewReader(`{"owner_address":"`+addr+`","timestamp":12345}`))
	if err != nil {
		t.Fatalf("getcanwithdrawunfreezeamount request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getcanwithdrawunfreezeamount status: %d", resp.StatusCode)
	}
	var withdraw tronapi.CanWithdrawUnfreezeInfo
	if err := json.NewDecoder(resp.Body).Decode(&withdraw); err != nil {
		t.Fatal(err)
	}
	if withdraw.Amount != 5000 {
		t.Fatalf("getcanwithdrawunfreezeamount = %+v, want bound sentinel", withdraw)
	}
	if stub.canWithdrawAtBlock != wantBlock {
		t.Fatalf("GetCanWithdrawUnfreezeAmountAt block = %d, want %d", stub.canWithdrawAtBlock, wantBlock)
	}
	if stub.liveCanWithdrawCalls != 0 {
		t.Fatalf("live GetCanWithdrawUnfreezeAmount called %d times, want 0", stub.liveCanWithdrawCalls)
	}

	resp, err = http.Post(prefix+"/getavailableunfreezecount", "application/json", strings.NewReader(`{"owner_address":"`+addr+`"}`))
	if err != nil {
		t.Fatalf("getavailableunfreezecount request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getavailableunfreezecount status: %d", resp.StatusCode)
	}
	var available tronapi.AvailableUnfreezeCountInfo
	if err := json.NewDecoder(resp.Body).Decode(&available); err != nil {
		t.Fatal(err)
	}
	if available.Count != 29 {
		t.Fatalf("getavailableunfreezecount = %+v, want bound sentinel", available)
	}
	if stub.availableAtBlock != wantBlock {
		t.Fatalf("GetAvailableUnfreezeCountAt block = %d, want %d", stub.availableAtBlock, wantBlock)
	}
	if stub.liveAvailableCalls != 0 {
		t.Fatalf("live GetAvailableUnfreezeCount called %d times, want 0", stub.liveAvailableCalls)
	}
}
