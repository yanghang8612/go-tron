package tronapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// stubBackend is a test double for tronapi.Backend.
// Pre-set fields control what each new method returns.
// All pre-existing methods return zero/nil values.
type stubBackend struct {
	delegatedResource *tronapi.DelegatedResourceInfo
	delegationIndex   *tronapi.DelegationIndexInfo
	canDelegate       *tronapi.CanDelegateInfo
	canWithdraw       *tronapi.CanWithdrawUnfreezeInfo
	availableUnfreeze *tronapi.AvailableUnfreezeCountInfo
	reward            *tronapi.RewardInfo
	pendingTx         *corepb.Transaction
	pendingTxList     []*corepb.Transaction
	nodes             []*tronapi.PeerInfo
	// M5.1 PR-1
	accountByID *types.Account
	accountNet  *apipb.AccountNetMessage
}

// --- Pre-existing Backend methods (all return zero values) ---
func (s *stubBackend) CurrentBlock() *types.Block                             { return nil }
func (s *stubBackend) GetBlockByNumber(n uint64) (*types.Block, error)        { return nil, nil }
func (s *stubBackend) GetAccount(addr common.Address) (*types.Account, error) { return nil, nil }
func (s *stubBackend) BroadcastTransaction(tx *types.Transaction) error       { return nil }
func (s *stubBackend) GetNodeInfo() *tronapi.NodeInfo                          { return &tronapi.NodeInfo{} }
func (s *stubBackend) PendingTransactionCount() int                            { return 0 }
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
	return nil, nil
}
func (s *stubBackend) GetChainParameters() []tronapi.ChainParameter { return nil }
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
func (s *stubBackend) ListProposals() ([]*tronapi.ProposalInfo, error) { return nil, nil }

// --- New Phase 10 methods ---
func (s *stubBackend) GetDelegatedResourceV2(from, to common.Address) (*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResource, nil
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
func (s *stubBackend) ListExchanges() ([]*corepb.Exchange, error)    { return nil, nil }
func (s *stubBackend) GetBrokerageInfo(addr common.Address) int64    { return 0 }
func (s *stubBackend) TotalTransaction() int64                       { return 0 }
func (s *stubBackend) GetBurnTrx() int64                             { return 0 }
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
	return nil, nil
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
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetProposalByID(id int64) (*tronapi.ProposalInfo, error) {
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
		delegatedResource: &tronapi.DelegatedResourceInfo{
			FromAddress:            "4101",
			ToAddress:              "4102",
			FrozenBalanceForEnergy: 1000000,
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 1 {
		t.Fatalf("expected delegatedResource=[1 entry], got %v", result)
	}
}

func TestGetDelegatedResourceV2Empty(t *testing.T) {
	stub := &stubBackend{} // delegatedResource is nil
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

	result := postJSON(t, srv.URL+"/wallet/setaccountid",
		`{"owner_address":"4101","account_id":"myid"}`)
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
			FreeNetUsed:  100,
			FreeNetLimit: 1500,
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
	testTxBuilder(t, "/wallet/createwitness",
		`{"owner_address":"4101","url":"https://witness.example.com"}`)
}
func TestVoteWitnessAccount(t *testing.T) {
	testTxBuilder(t, "/wallet/votewitnessaccount",
		`{"owner_address":"4101","votes":[{"vote_address":"4102","vote_count":100}]}`)
}
func TestUpdateWitness(t *testing.T) {
	testTxBuilder(t, "/wallet/updatewitness",
		`{"owner_address":"4101","update_url":"https://updated.example.com"}`)
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

// --- Tests: M5.1 PR-3 TRC10 + PR-4 ClearABI ---

func TestCreateAssetIssue(t *testing.T) {
	testTxBuilder(t, "/wallet/createassetissue",
		`{"owner_address":"4101000000000000000000000000000000000000000000","name":"dGVzdA==","abbr":"dA==","total_supply":"1000000","trx_num":1,"num":1,"start_time":"1000","end_time":"2000","precision":6}`)
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
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":"1","token_id":"dHJ4","quant":"100"}`)
}
func TestExchangeTransaction(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangetransaction",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":"1","token_id":"dHJ4","quant":"100","expected":"50"}`)
}
func TestExchangeWithdraw(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangewithdraw",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":"1","token_id":"dHJ4","quant":"100"}`)
}
func TestMarketSellAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/marketsellasset",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestMarketCancelOrder(t *testing.T) {
	testTxBuilder(t, "/wallet/marketcancelorder",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
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
