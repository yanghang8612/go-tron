package tronapi_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// stubBackend is a test double for tronapi.Backend.
// Pre-set fields control what each new method returns.
// All pre-existing methods return zero/nil values.
type stubBackend struct {
	delegatedResources []*tronapi.DelegatedResourceInfo
	delegationIndex    *tronapi.DelegationIndexInfo
	canDelegate        *tronapi.CanDelegateInfo
	canWithdraw        *tronapi.CanWithdrawUnfreezeInfo
	availableUnfreeze  *tronapi.AvailableUnfreezeCountInfo
	reward             *tronapi.RewardInfo
	pendingTx          *corepb.Transaction
	pendingTxList      []*corepb.Transaction
	nodes              []*tronapi.PeerInfo
	// M5.1 PR-1
	accountByID     *types.Account
	accountNet      *apipb.AccountNetMessage
	accountResource *tronapi.AccountResource
	// For inspecting what contract was passed to BuildContractTransaction
	lastContractType corepb.Transaction_Contract_ContractType
	lastContract     proto.Message
	// M9.7: controlled by test to simulate validate failure
	validateErr error
	// Proposal output divergence test (D-4): canned proposals returned
	// from ListProposals / ListProposalsPaginated / GetProposalByID.
	proposals []*tronapi.ProposalInfo
	// Canned chain parameters returned from GetChainParameters.
	chainParams []tronapi.ChainParameter
}

// --- Pre-existing Backend methods (all return zero values) ---
func (s *stubBackend) CurrentBlock() *types.Block                             { return nil }
func (s *stubBackend) GetBlockByNumber(n uint64) (*types.Block, error)        { return nil, nil }
func (s *stubBackend) GetAccount(addr common.Address) (*types.Account, error) { return nil, nil }
func (s *stubBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	return nil, nil
}
func (s *stubBackend) BroadcastTransaction(tx *types.Transaction) error { return nil }
func (s *stubBackend) GetNodeInfo() *tronapi.NodeInfo                   { return &tronapi.NodeInfo{} }
func (s *stubBackend) PendingTransactionCount() int                     { return 0 }
func (s *stubBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	return nil, nil
}
func (s *stubBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionByID(h common.Hash) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionInfoByID(h common.Hash) (*corepb.TransactionInfo, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionInfoByBlockNum(n uint64) ([]*corepb.TransactionInfo, error) {
	return nil, nil
}
func (s *stubBackend) GetBlockByHash(h common.Hash) (*types.Block, error) { return nil, nil }
func (s *stubBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	return nil, nil
}
func (s *stubBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte, feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte, feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	return nil, nil, nil
}
func (s *stubBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 0, nil
}
func (s *stubBackend) GetAccountResource(addr common.Address) (*tronapi.AccountResource, error) {
	return s.accountResource, nil
}
func (s *stubBackend) GetAccountResourceAt(addr common.Address, blockNum uint64) (*tronapi.AccountResource, error) {
	return nil, nil
}
func (s *stubBackend) GetChainParameters() []tronapi.ChainParameter   { return s.chainParams }
func (s *stubBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) { return nil, nil }
func (s *stubBackend) NextMaintenanceTime() int64                     { return 0 }
func (s *stubBackend) BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) ListProposals() ([]*tronapi.ProposalInfo, error) { return s.proposals, nil }

// --- New Phase 10 methods ---
func (s *stubBackend) GetDelegatedResourceV2(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResources, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	return s.delegationIndex, nil
}
func (s *stubBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return s.canDelegate, nil
}
func (s *stubBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return s.canWithdraw, nil
}
func (s *stubBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return s.availableUnfreeze, nil
}
func (s *stubBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) {
	return s.reward, nil
}
func (s *stubBackend) GetRewardAt(addr common.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	return s.reward, nil
}
func (s *stubBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	if s.pendingTx == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return s.pendingTx, nil
}
func (s *stubBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	return s.pendingTxList, nil
}
func (s *stubBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	return s.nodes, nil
}

// --- New Phase 12 methods (TRC10 asset queries) ---
func (s *stubBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
	return nil
}
func (s *stubBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	return nil
}
func (s *stubBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	return nil
}
func (s *stubBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	return nil
}
func (s *stubBackend) GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract {
	return nil
}

// --- New Phase 13 methods (Market order queries) ---
func (s *stubBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder { return nil }
func (s *stubBackend) GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder {
	return nil
}
func (s *stubBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	return nil
}
func (s *stubBackend) ListExchanges() ([]*corepb.Exchange, error) { return nil, nil }
func (s *stubBackend) GetBrokerageInfo(addr common.Address) int64 { return 0 }
func (s *stubBackend) TotalTransaction() int64                    { return 0 }
func (s *stubBackend) GetBurnTrx() int64                          { return 0 }
func (s *stubBackend) BuildFreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnfreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode, lock bool) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildCancelAllUnfreezeV2Transaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildWithdrawExpireUnfreezeTransaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildVoteWitnessTransaction(owner common.Address, votes map[common.Address]int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetBandwidthPrices() string { return "" }
func (s *stubBackend) GetEnergyPrices() string    { return "" }
func (s *stubBackend) ListProposalsPaginated(offset, limit int) ([]*tronapi.ProposalInfo, error) {
	if len(s.proposals) == 0 {
		return nil, nil
	}
	if offset >= len(s.proposals) {
		return []*tronapi.ProposalInfo{}, nil
	}
	end := offset + limit
	if end > len(s.proposals) {
		end = len(s.proposals)
	}
	return s.proposals[offset:end], nil
}
func (s *stubBackend) ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error) {
	return nil, nil
}

// --- M5.1 PR-1: Account / permission ---
func (s *stubBackend) BuildCreateAccountTransaction(owner, account common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateAccountTransaction(owner common.Address, name []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildSetAccountIdTransaction(owner common.Address, accountID []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	if s.accountByID != nil {
		return s.accountByID, nil
	}
	return nil, fmt.Errorf("account not found")
}
func (s *stubBackend) GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error) {
	if s.accountNet != nil {
		return s.accountNet, nil
	}
	return nil, nil
}

// --- M5.1 PR-2: Transaction builders ---
func (s *stubBackend) BuildTransferAssetTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildParticipateAssetIssueTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildCreateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildWithdrawBalanceTransaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateBrokerageTransaction(owner common.Address, brokerage int32) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildFreezeBalanceV1Transaction(owner common.Address, amount, duration int64, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnfreezeBalanceV1Transaction(owner common.Address, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

// --- M5.1 PR-3+: Generic contract builder + misc ---
func (s *stubBackend) BuildContractTransaction(contractType corepb.Transaction_Contract_ContractType, contract proto.Message, feeLimit int64) (*corepb.Transaction, error) {
	s.lastContractType = contractType
	s.lastContract = contract
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetProposalByID(id int64) (*tronapi.ProposalInfo, error) {
	for _, p := range s.proposals {
		if p.ProposalID == id {
			return p, nil
		}
	}
	if id == 1 {
		return &tronapi.ProposalInfo{ProposalID: 1}, nil
	}
	return nil, fmt.Errorf("not found")
}
func (s *stubBackend) ValidateAddress(addr string) (bool, string) {
	return len(addr) == 42, "test"
}

// --- M8.1: confirmation-depth stubs ---
func (s *stubBackend) SolidifiedBlockNum() uint64 { return 0 }
func (s *stubBackend) LatestPbftBlockNum() int64  { return -1 }

// --- M9.7: synchronous actuator validate ---
func (s *stubBackend) ValidateTransaction(tx *types.Transaction) error {
	return s.validateErr
}

// --- Helpers ---
func newTestServer(t *testing.T, stub *stubBackend) *httptest.Server {
	t.Helper()
	api := tronapi.NewAPI(stub)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	return httptest.NewServer(mux)
}

func postJSON(t *testing.T, url, body string) map[string]interface{} {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d", url, resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON from %s: %v", url, err)
	}
	return result
}

// --- Tests: delegation group ---

func TestGetDelegatedResourceV2WithData(t *testing.T) {
	stub := &stubBackend{
		delegatedResources: []*tronapi.DelegatedResourceInfo{
			{
				FromAddress:               "4101",
				ToAddress:                 "4102",
				FrozenBalanceForBandwidth: 1000000,
				ExpireTimeForBandwidth:    0,
			},
			{
				FromAddress:            "4101",
				ToAddress:              "4102",
				FrozenBalanceForEnergy: 1000000,
				ExpireTimeForEnergy:    123456,
			},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 2 {
		t.Fatalf("expected delegatedResource=[2 entries], got %v", result)
	}
}

func TestGetDelegatedResourceV2Empty(t *testing.T) {
	stub := &stubBackend{} // delegatedResources is nil
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 0 {
		t.Fatalf("expected delegatedResource=[], got %v", result)
	}
}

func TestGetDelegatedResourceAccountIndexV2(t *testing.T) {
	stub := &stubBackend{
		delegationIndex: &tronapi.DelegationIndexInfo{
			Account:     "4101",
			ToAddresses: []string{"4102", "4103"},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourceaccountindexv2",
		`{"value":"4101"}`)
	addrs, ok := result["toAddresses"].([]interface{})
	if !ok || len(addrs) != 2 {
		t.Fatalf("expected 2 toAddresses, got %v", result)
	}
}

func TestCanDelegateResource(t *testing.T) {
	stub := &stubBackend{
		canDelegate: &tronapi.CanDelegateInfo{MaxSize: 1000000, CanDelegateSize: 800000, Balance: 500000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/candelegateresource",
		`{"owner_address":"4101","balance":500000,"type":0}`)
	if result["maxSize"].(float64) != 1000000 || result["canDelegateSize"].(float64) != 800000 {
		t.Fatalf("unexpected canDelegate response: %v", result)
	}
}

// --- Tests: unfreeze/reward group ---

func TestGetCanWithdrawUnfreezeAmount(t *testing.T) {
	stub := &stubBackend{
		canWithdraw: &tronapi.CanWithdrawUnfreezeInfo{Amount: 5000000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getcanwithdrawunfreezeamount",
		`{"owner_address":"4101","timestamp":1712345678000}`)
	if result["amount"].(float64) != 5000000 {
		t.Fatalf("unexpected amount: %v", result)
	}
}

func TestGetAvailableUnfreezeCount(t *testing.T) {
	stub := &stubBackend{
		availableUnfreeze: &tronapi.AvailableUnfreezeCountInfo{Count: 30},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getavailableunfreezecount",
		`{"owner_address":"4101"}`)
	if result["count"].(float64) != 30 {
		t.Fatalf("unexpected count: %v", result)
	}
}

func TestGetReward(t *testing.T) {
	stub := &stubBackend{
		reward: &tronapi.RewardInfo{Reward: 123456},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getreward",
		`{"address":"4101"}`)
	if result["reward"].(float64) != 123456 {
		t.Fatalf("unexpected reward: %v", result)
	}
}

// --- Tests: pool group ---

func TestGetTransactionFromPendingFound(t *testing.T) {
	stub := &stubBackend{
		pendingTx: &corepb.Transaction{RawData: &corepb.TransactionRaw{}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasRawData := result["raw_data"]; !hasRawData {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestGetTransactionFromPendingNotFound(t *testing.T) {
	stub := &stubBackend{} // pendingTx is nil → returns error
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasError := result["Error"]; !hasError {
		t.Fatalf("expected Error field in not-found response, got %v", result)
	}
}

func TestGetTransactionListFromPending(t *testing.T) {
	stub := &stubBackend{pendingTxList: nil} // empty pool
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionlistfrompending", `{}`)
	txList, ok := result["transaction"].([]interface{})
	if !ok {
		t.Fatalf("expected transaction array, got %v", result)
	}
	if len(txList) != 0 {
		t.Fatalf("expected empty transaction list, got %d entries", len(txList))
	}
}

// --- Tests: network group ---

func TestListNodes(t *testing.T) {
	stub := &stubBackend{
		nodes: []*tronapi.PeerInfo{{Host: "127.0.0.1", Port: 18888}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/wallet/listnodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	nodes, ok := result["nodes"].([]interface{})
	if !ok || len(nodes) != 1 {
		t.Fatalf("expected nodes=[1 entry], got %v", result)
	}
	node, ok := nodes[0].(map[string]interface{})
	if !ok {
		t.Fatalf("nodes[0] is not an object")
	}
	addr, ok := node["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("node address is not an object")
	}
	if addr["host"] != "127.0.0.1" {
		t.Fatalf("unexpected host: %v", addr["host"])
	}
}

// --- Tests: M5.1 PR-1 account/permission ---

func TestCreateAccount(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/createaccount",
		`{"owner_address":"4101","account_address":"4102"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestUpdateAccount(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/updateaccount",
		`{"owner_address":"4101","account_name":"deadbeef"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestSetAccountId(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	// account_id hex-encodes "myid". Pre-fix the test silently parsed
	// non-hex via the swallowed-error FromHex path; now we either
	// hex-encode or set visible:true.
	result := postJSON(t, srv.URL+"/wallet/setaccountid",
		`{"owner_address":"4101","account_id":"6d796964"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestAccountPermissionUpdate(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/accountpermissionupdate",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestGetAccountByIdFound(t *testing.T) {
	pb := &corepb.Account{Address: []byte{0x41, 0x01}}
	stub := &stubBackend{accountByID: types.NewAccountFromPB(pb)}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountbyid",
		`{"account_id":"myid"}`)
	if result["address"] == nil {
		t.Fatalf("expected address in response, got %v", result)
	}
}

func TestGetAccountByIdNotFound(t *testing.T) {
	stub := &stubBackend{} // accountByID is nil
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountbyid",
		`{"account_id":"unknown"}`)
	if len(result) != 0 {
		t.Fatalf("expected empty object for not-found, got %v", result)
	}
}

func TestGetAccountNet(t *testing.T) {
	stub := &stubBackend{
		accountNet: &apipb.AccountNetMessage{
			FreeNetUsed:   100,
			FreeNetLimit:  1500,
			TotalNetLimit: 43200000000,
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountnet",
		`{"address":"4101"}`)
	// protojson encodes int64 as string
	if result["freeNetUsed"] != "100" {
		t.Fatalf("unexpected freeNetUsed: %v", result)
	}
}

// TestGetAccountResource_PopulatedFields pins the HTTP JSON contract for the
// fields that previously always came back empty: each must appear under its
// java-matching key (casing matters for SDKs), and an omitempty field left at
// zero must be omitted rather than emitted.
func TestGetAccountResource_PopulatedFields(t *testing.T) {
	stub := &stubBackend{
		accountResource: &tronapi.AccountResource{
			NetLimit:             5000,
			TotalNetWeight:       1000,
			TotalTronPowerWeight: 3000,
			TronPowerUsed:        100,
			TronPowerLimit:       800,
			EnergyUsed:           333,
			EnergyLimit:          9000,
			TotalEnergyWeight:    2000,
			StorageUsed:          555,
			// StorageLimit deliberately left 0 to assert omitempty omits it.
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountresource", `{"address":"4101"}`)

	want := map[string]float64{
		"NetLimit":             5000,
		"TotalNetWeight":       1000,
		"TotalTronPowerWeight": 3000,
		"tronPowerUsed":        100,
		"tronPowerLimit":       800,
		"EnergyUsed":           333,
		"EnergyLimit":          9000,
		"TotalEnergyWeight":    2000,
		"storageUsed":          555,
	}
	for key, v := range want {
		got, ok := result[key]
		if !ok {
			t.Errorf("missing field %q in response %v", key, result)
			continue
		}
		if got.(float64) != v {
			t.Errorf("%s = %v, want %v", key, got, v)
		}
	}
	if _, ok := result["storageLimit"]; ok {
		t.Errorf("storageLimit should be omitted when zero, got %v", result["storageLimit"])
	}
}

// --- Tests: M5.1 PR-2 transaction builders ---

func testTxBuilder(t *testing.T, url, body string) {
	t.Helper()
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+url, body)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response for %s, got %v", url, result)
	}
}

func TestTransferAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/transferasset",
		`{"owner_address":"4101","to_address":"4102","asset_name":"1000001","amount":100}`)
}
func TestParticipateAssetIssue(t *testing.T) {
	testTxBuilder(t, "/wallet/participateassetissue",
		`{"owner_address":"4101","to_address":"4102","asset_name":"1000001","amount":100}`)
}
func TestCreateWitness(t *testing.T) {
	// URL hex-encodes "https://witness.example.com". The handler used to
	// silently accept non-hex via common.FromHex's swallowed error; the
	// P0-2 audit flagged that path so the test must use real hex now.
	testTxBuilder(t, "/wallet/createwitness",
		`{"owner_address":"4101","url":"68747470733a2f2f7769746e6573732e6578616d706c652e636f6d"}`)
}
func TestVoteWitnessAccount(t *testing.T) {
	testTxBuilder(t, "/wallet/votewitnessaccount",
		`{"owner_address":"4101","votes":[{"vote_address":"4102","vote_count":100}]}`)
}
func TestUpdateWitness(t *testing.T) {
	// update_url hex-encodes "https://updated.example.com". Same reason
	// as TestCreateWitness: the FromHex silent-swallow path used to mask
	// non-hex input.
	testTxBuilder(t, "/wallet/updatewitness",
		`{"owner_address":"4101","update_url":"68747470733a2f2f757064617465642e6578616d706c652e636f6d"}`)
}
func TestWithdrawBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/withdrawbalance",
		`{"owner_address":"4101"}`)
}
func TestUpdateBrokerage(t *testing.T) {
	testTxBuilder(t, "/wallet/updatebrokerage",
		`{"owner_address":"4101","brokerage":20}`)
}
func TestFreezeBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/freezebalance",
		`{"owner_address":"4101","frozen_balance":1000000,"frozen_duration":3,"resource":0}`)
}
func TestUnfreezeBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/unfreezebalance",
		`{"owner_address":"4101","resource":0}`)
}
func TestFreezeBalanceV2(t *testing.T) {
	testTxBuilder(t, "/wallet/freezebalancev2",
		`{"owner_address":"4101","frozen_balance":1000000,"resource":0}`)
}
func TestUnfreezeBalanceV2(t *testing.T) {
	testTxBuilder(t, "/wallet/unfreezebalancev2",
		`{"owner_address":"4101","unfreeze_balance":1000000,"resource":0}`)
}
func TestCancelAllUnfreezeV2(t *testing.T) {
	testTxBuilder(t, "/wallet/cancelallunfreezev2",
		`{"owner_address":"4101"}`)
}
func TestDelegateResource(t *testing.T) {
	testTxBuilder(t, "/wallet/delegateresource",
		`{"owner_address":"4101","receiver_address":"4102","balance":1000000,"resource":0}`)
}
func TestUndelegateResource(t *testing.T) {
	testTxBuilder(t, "/wallet/undelegateresource",
		`{"owner_address":"4101","receiver_address":"4102","balance":1000000,"resource":0}`)
}
func TestWithdrawExpireUnfreeze(t *testing.T) {
	testTxBuilder(t, "/wallet/withdrawexpireunfreeze",
		`{"owner_address":"4101"}`)
}

// TestTransferAsset_VisibleBase58: end-to-end exercise of P1 visible=true
// support. java-tron's HTTP API accepts Base58Check addresses + UTF-8
// strings for `asset_name` when visible=true; gtron now does the same.
// Pre-fix the request silently routed to addr(0) (the Base58Check string
// isn't a valid hex address — hex.DecodeString errored, the error was
// swallowed by common.FromHex, BytesToAddress promoted nil to zero).
func TestTransferAsset_VisibleBase58(t *testing.T) {
	owner := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	body := `{` +
		`"owner_address":"` + crypto.AddressToBase58(owner) + `",` +
		`"to_address":"` + crypto.AddressToBase58(to) + `",` +
		`"asset_name":"1000001",` +
		`"amount":100,` +
		`"visible":true}`
	testTxBuilder(t, "/wallet/transferasset", body)
}

// TestTransferAsset_RejectsBadHexOwner: the FromHex silent-swallow fix in
// action — a typo'd owner now returns 400 instead of building a tx that
// would later route to addr(0).
func TestTransferAsset_RejectsBadHexOwner(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	body := `{"owner_address":"nothex","to_address":"4102","asset_name":"1000001","amount":100}`
	resp, err := http.Post(srv.URL+"/wallet/transferasset", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-hex owner_address, got %d", resp.StatusCode)
	}
}

// --- Tests: M5.1 PR-3 TRC10 + PR-4 ClearABI ---

func TestCreateAssetIssue(t *testing.T) {
	testTxBuilder(t, "/wallet/createassetissue",
		`{"owner_address":"4101000000000000000000000000000000000000000000","name":"74657374","abbr":"74","total_supply":1000000,"trx_num":1,"num":1,"start_time":1000,"end_time":2000,"precision":6}`)
}
func TestUpdateAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/updateasset",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestGetAssetIssueListByName(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getassetissuelistbyname", `{"value":"deadbeef"}`)
	if _, ok := result["assetIssue"]; !ok {
		t.Fatalf("expected assetIssue key, got %v", result)
	}
}
func TestClearABI(t *testing.T) {
	testTxBuilder(t, "/wallet/clearabi",
		`{"owner_address":"4101","contract_address":"4102"}`)
}

// --- Tests: M5.1 PR-5 Exchange/Market ---

func TestExchangeCreate(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangecreate",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestExchangeInject(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangeinject",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100}`)
}
func TestExchangeTransaction(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangetransaction",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100,"expected":50}`)
}
func TestExchangeWithdraw(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangewithdraw",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100}`)
}
func TestMarketSellAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/marketsellasset",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestMarketCancelOrder(t *testing.T) {
	testTxBuilder(t, "/wallet/marketcancelorder",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}

// --- Tests: M9.1 hex decode verification ---

func TestExchangeCreateHexDecode(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	// owner_address, first_token_id, second_token_id are all hex strings
	result := postJSON(t, srv.URL+"/wallet/exchangecreate",
		`{"owner_address":"41e2ba4c4a3a8d31db8d893a13c3b0bc40f27ec2ff","first_token_id":"5f","first_token_balance":1000,"second_token_id":"313030303030","second_token_balance":1000}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}

	// Verify that the contract passed to BuildContractTransaction has hex-decoded bytes.
	if stub.lastContract == nil {
		t.Fatal("lastContract was not captured")
	}
	ec, ok := stub.lastContract.(*contractpb.ExchangeCreateContract)
	if !ok {
		t.Fatalf("expected *ExchangeCreateContract, got %T", stub.lastContract)
	}
	wantOwner := common.FromHex("41e2ba4c4a3a8d31db8d893a13c3b0bc40f27ec2ff")
	if string(ec.OwnerAddress) != string(wantOwner) {
		t.Fatalf("OwnerAddress: got %x, want %x", ec.OwnerAddress, wantOwner)
	}
	wantFirstToken := common.FromHex("5f")
	if string(ec.FirstTokenId) != string(wantFirstToken) {
		t.Fatalf("FirstTokenId: got %x, want %x", ec.FirstTokenId, wantFirstToken)
	}
	if ec.FirstTokenBalance != 1000 {
		t.Fatalf("FirstTokenBalance: got %d, want 1000", ec.FirstTokenBalance)
	}
	wantSecondToken := common.FromHex("313030303030")
	if string(ec.SecondTokenId) != string(wantSecondToken) {
		t.Fatalf("SecondTokenId: got %x, want %x", ec.SecondTokenId, wantSecondToken)
	}
}

// --- Tests: M5.1 PR-6 Proposal/Monitoring ---

func TestGetProposalByIdFound(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":1}`)
	if result["proposal_id"].(float64) != 1 {
		t.Fatalf("expected proposal_id=1, got %v", result)
	}
}
func TestGetProposalByIdNotFound(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":999}`)
	if len(result) != 0 {
		t.Fatalf("expected empty object, got %v", result)
	}
}
func TestGetPaginatedProposalList(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getpaginatedproposallist", `{"offset":0,"limit":10}`)
	if _, ok := result["proposal"]; !ok {
		t.Fatalf("expected proposal key, got %v", result)
	}
}

// --- Wire-format parity: parameters MUST be array, not dict ---
//
// java-tron's HTTP serialization flattens proto map<int64,int64> as a
// repeated MapEntry, producing `[{"key":N,"value":V},...]`. SDKs that
// target java-tron break when gtron emits a dict instead.
// Cross-impl divergence ref: docs/dev/cross-impl-divergences-2026-05-02.md.

func TestProposalParametersArrayShape_GetProposalById(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID:      7,
			ProposerAddress: "41" + strings.Repeat("ab", 20),
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 19, Value: 259200000},
				{Key: 5, Value: 1},
			},
			ExpirationTime: 1234,
			CreateTime:     1000,
			State:          "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":7}`)
	params, ok := result["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array (java-tron parity), got %T: %v",
			result["parameters"], result["parameters"])
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 parameter entries, got %d: %v", len(params), params)
	}
	first, ok := params[0].(map[string]interface{})
	if !ok {
		t.Fatalf("parameter entry must be an object, got %T: %v", params[0], params[0])
	}
	// Field names must match java-tron exactly (lowercase "key"/"value").
	if _, ok := first["key"]; !ok {
		t.Fatalf("parameter entry missing \"key\": %v", first)
	}
	if _, ok := first["value"]; !ok {
		t.Fatalf("parameter entry missing \"value\": %v", first)
	}
	// Spot-check first entry's encoded values — the stub feeds entries
	// in the order {19, 5}, so first should be {key:19,value:259200000}.
	// (TronBackend sorts by key for determinism — covered separately.)
	if first["key"].(float64) != 19 || first["value"].(float64) != 259200000 {
		t.Fatalf("expected {key:19,value:259200000}, got %v", first)
	}
}

func TestProposalParametersArrayShape_ListProposals(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 1,
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 19, Value: 259200000},
			},
			State: "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/listproposals", `{}`)
	proposals, ok := result["proposals"].([]interface{})
	if !ok || len(proposals) != 1 {
		t.Fatalf("expected proposals array of length 1, got %v", result["proposals"])
	}
	first := proposals[0].(map[string]interface{})
	params, ok := first["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array, got %T: %v", first["parameters"], first["parameters"])
	}
	if len(params) != 1 {
		t.Fatalf("expected 1 parameter entry, got %d", len(params))
	}
	entry := params[0].(map[string]interface{})
	if entry["key"].(float64) != 19 || entry["value"].(float64) != 259200000 {
		t.Fatalf("expected {key:19,value:259200000}, got %v", entry)
	}
}

func TestProposalParametersArrayShape_PaginatedList(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 1,
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 11, Value: 100},
				{Key: 9, Value: 200},
			},
			State: "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getpaginatedproposallist", `{"offset":0,"limit":10}`)
	proposals, ok := result["proposal"].([]interface{})
	if !ok || len(proposals) != 1 {
		t.Fatalf("expected proposal array of length 1, got %v", result["proposal"])
	}
	first := proposals[0].(map[string]interface{})
	params, ok := first["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array, got %T: %v", first["parameters"], first["parameters"])
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 parameter entries, got %d: %v", len(params), params)
	}
	for _, e := range params {
		entry := e.(map[string]interface{})
		if _, ok := entry["key"]; !ok {
			t.Fatalf("parameter entry missing \"key\": %v", entry)
		}
		if _, ok := entry["value"]; !ok {
			t.Fatalf("parameter entry missing \"value\": %v", entry)
		}
	}
}

func TestProposalParametersArrayShape_EmptyMapStillArray(t *testing.T) {
	// A proposal with no parameters must still emit `"parameters": []`,
	// not `null` or a missing key — keep the field type stable for SDKs.
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 42,
			Parameters: []tronapi.ProposalParameterEntry{},
			State:      "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":42}`)
	params, ok := result["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array even when empty, got %T: %v",
			result["parameters"], result["parameters"])
	}
	if len(params) != 0 {
		t.Fatalf("expected empty array, got %v", params)
	}
}
func TestMetrics(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/wallet/metrics")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("metrics failed: %v %v", err, resp)
	}
}

// --- Tests: M5.1 PR-7 Transaction meta ---

func TestValidateAddressValid(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/validateaddress",
		`{"address":"411234567890123456789012345678901234567890ab"}`)
	if result["message"] != "test" {
		t.Fatalf("unexpected message: %v", result)
	}
}
func TestGetTransactionReceiptById(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/gettransactionreceiptbyid",
		`{"value":"aabbcc"}`)
	// stub returns nil tx info → empty object
	_ = result
}

// --- Tests: M9.7 broadcastTransaction synchronous actuator.Validate ---

// buildBroadcastEnvelope creates the JSON body for /wallet/broadcasttransaction.
// Uses a TransferContract so it matches a supported contract type.
func buildBroadcastEnvelope(t *testing.T) string {
	t.Helper()
	transfer := &contractpb.TransferContract{
		OwnerAddress: common.FromHex("410000000000000000000000000000000000000000"),
		ToAddress:    common.FromHex("410000000000000000000000000000000000000001"),
		Amount:       1000,
	}
	paramAny, err := anypb.New(transfer)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	rawData := &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: paramAny,
		}},
		Expiration: 9999999999000,
		Timestamp:  1000000,
	}
	rawBytes, err := proto.Marshal(rawData)
	if err != nil {
		t.Fatalf("proto.Marshal TransactionRaw: %v", err)
	}
	h := sha256.Sum256(rawBytes)
	_ = h // txID used internally

	body, err := json.Marshal(map[string]any{
		"raw_data_hex": hex.EncodeToString(rawBytes),
		"signature":    []string{},
	})
	if err != nil {
		t.Fatalf("json.Marshal envelope: %v", err)
	}
	return string(body)
}

func TestBroadcastTransactionValidateError(t *testing.T) {
	stub := &stubBackend{
		validateErr: errors.New("owner account not found"),
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	envelope := buildBroadcastEnvelope(t)
	result := postJSON(t, srv.URL+"/wallet/broadcasttransaction", envelope)

	if result["result"] != false {
		t.Fatalf("expected result=false, got %v", result["result"])
	}
	if result["code"] != "CONTRACT_VALIDATE_ERROR" {
		t.Fatalf("expected code=CONTRACT_VALIDATE_ERROR, got %v", result["code"])
	}
	if result["message"] == "" {
		t.Fatalf("expected non-empty message hex, got empty")
	}
	// message must decode to the original error string
	msgHex, ok := result["message"].(string)
	if !ok {
		t.Fatalf("message is not a string: %T %v", result["message"], result["message"])
	}
	decoded, err := hex.DecodeString(msgHex)
	if err != nil {
		t.Fatalf("message is not valid hex: %v", err)
	}
	if string(decoded) != "owner account not found" {
		t.Fatalf("decoded message mismatch: got %q", string(decoded))
	}
}

func TestBroadcastTransactionValidateSuccess(t *testing.T) {
	stub := &stubBackend{
		validateErr: nil, // passes validation
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	envelope := buildBroadcastEnvelope(t)
	result := postJSON(t, srv.URL+"/wallet/broadcasttransaction", envelope)

	if result["result"] != true {
		t.Fatalf("expected result=true on success, got %v", result["result"])
	}
	if result["code"] != "SUCCESS" {
		t.Fatalf("expected code=SUCCESS, got %v", result["code"])
	}
}

// TestGetChainParametersJavaShape pins the wire shape of
// /wallet/getchainparameters to java-tron's: camelCase java getter keys, and
// java's omit-zero quirk where an entry with value 0 serializes without a
// "value" field (negative values like getRemoveThePowerOfTheGr's -1 are kept).
func TestGetChainParametersJavaShape(t *testing.T) {
	stub := &stubBackend{chainParams: []tronapi.ChainParameter{
		{Key: "getEnergyFee", Value: 420},
		{Key: "getAllowTvmBlob", Value: 0},
		{Key: "getRemoveThePowerOfTheGr", Value: -1},
	}}
	srv := newTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getchainparameters", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST getchainparameters: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	want := `{"chainParameter":[{"key":"getEnergyFee","value":420},{"key":"getAllowTvmBlob"},{"key":"getRemoveThePowerOfTheGr","value":-1}]}`
	if got := strings.TrimSpace(string(body)); got != want {
		t.Fatalf("getchainparameters body mismatch\n got: %s\nwant: %s", got, want)
	}
}
