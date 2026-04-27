package tronapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type API struct {
	backend Backend
}

func NewAPI(backend Backend) *API {
	return &API{backend: backend}
}

func (api *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/wallet/getnowblock", api.getNowBlock)
	mux.HandleFunc("/wallet/getblockbynum", api.getBlockByNum)
	mux.HandleFunc("/wallet/getaccount", api.getAccount)
	mux.HandleFunc("/wallet/broadcasttransaction", api.broadcastTransaction)
	mux.HandleFunc("/wallet/getnodeinfo", api.getNodeInfo)
	mux.HandleFunc("/wallet/gettransactioncountinpool", api.getTxPoolCount)
	mux.HandleFunc("/wallet/getcontract", api.getContract)
	mux.HandleFunc("/wallet/triggerconstantcontract", api.triggerConstantContract)

	// Transaction building
	mux.HandleFunc("/wallet/createtransaction", api.createTransaction)
	mux.HandleFunc("/wallet/deploycontract", api.deployContract)
	mux.HandleFunc("/wallet/triggersmartcontract", api.triggerSmartContract)
	mux.HandleFunc("/wallet/estimateenergy", api.estimateEnergy)

	// Transaction queries
	mux.HandleFunc("/wallet/gettransactionbyid", api.getTransactionByID)
	mux.HandleFunc("/wallet/gettransactioninfobyid", api.getTransactionInfoByID)
	mux.HandleFunc("/wallet/gettransactioninfobyblocknum", api.getTransactionInfoByBlockNum)

	// Block queries
	mux.HandleFunc("/wallet/getblockbyid", api.getBlockByID)
	mux.HandleFunc("/wallet/getblockbylimitnext", api.getBlockByLimitNext)

	// Resource & chain queries
	mux.HandleFunc("/wallet/getaccountresource", api.getAccountResource)
	mux.HandleFunc("/wallet/getchainparameters", api.getChainParameters)
	mux.HandleFunc("/wallet/listwitnesses", api.listWitnesses)
	mux.HandleFunc("/wallet/getnextmaintenancetime", api.getNextMaintenanceTime)

	// Proposal APIs
	mux.HandleFunc("/wallet/proposalcreate", api.proposalCreate)
	mux.HandleFunc("/wallet/proposalapprove", api.proposalApprove)
	mux.HandleFunc("/wallet/proposaldelete", api.proposalDelete)
	mux.HandleFunc("/wallet/listproposals", api.listProposals)

	// Phase 10: delegation/resource queries
	mux.HandleFunc("/wallet/getdelegatedresourcev2", api.getDelegatedResourceV2)
	mux.HandleFunc("/wallet/getdelegatedresourceaccountindexv2", api.getDelegatedResourceAccountIndexV2)
	mux.HandleFunc("/wallet/candelegateresource", api.canDelegateResource)

	// Phase 10: unfreeze/reward queries
	mux.HandleFunc("/wallet/getcanwithdrawunfreezeamount", api.getCanWithdrawUnfreezeAmount)
	mux.HandleFunc("/wallet/getavailableunfreezecount", api.getAvailableUnfreezeCount)
	mux.HandleFunc("/wallet/getreward", api.getReward)

	// Phase 10: pool and network queries
	mux.HandleFunc("/wallet/gettransactionfrompending", api.getTransactionFromPending)
	mux.HandleFunc("/wallet/gettransactionlistfrompending", api.getTransactionListFromPending)
	mux.HandleFunc("/wallet/listnodes", api.listNodes)

	// Phase 12: TRC10 asset queries
	mux.HandleFunc("/wallet/getassetissuebyid", api.getAssetIssueByID)
	mux.HandleFunc("/wallet/getassetissuebyname", api.getAssetIssueByName)
	mux.HandleFunc("/wallet/getassetissuelist", api.getAssetIssueList)
	mux.HandleFunc("/wallet/getpaginatedassetissuelist", api.getPaginatedAssetIssueList)
	mux.HandleFunc("/wallet/getassetissuebyaccount", api.getAssetIssueByAccount)

	// Phase 13: Market order queries
	mux.HandleFunc("/wallet/getmarketorderbyid", api.getMarketOrderByID)
	mux.HandleFunc("/wallet/getmarketordersfromaccount", api.getMarketOrdersFromAccount)
	mux.HandleFunc("/wallet/getmarketpricebypair", api.getMarketPriceByPair)

	// M5.1 PR-1: Account / permission
	mux.HandleFunc("/wallet/createaccount", api.createAccount)
	mux.HandleFunc("/wallet/updateaccount", api.updateAccount)
	mux.HandleFunc("/wallet/setaccountid", api.setAccountId)
	mux.HandleFunc("/wallet/accountpermissionupdate", api.accountPermissionUpdate)
	mux.HandleFunc("/wallet/getaccountbyid", api.getAccountById)
	mux.HandleFunc("/wallet/getaccountnet", api.getAccountNet)

	// M5.1 PR-2: Transaction builders
	mux.HandleFunc("/wallet/transferasset", api.transferAsset)
	mux.HandleFunc("/wallet/participateassetissue", api.participateAssetIssue)
	mux.HandleFunc("/wallet/createwitness", api.createWitness)
	mux.HandleFunc("/wallet/votewitnessaccount", api.voteWitnessAccount)
	mux.HandleFunc("/wallet/updatewitness", api.updateWitness)
	mux.HandleFunc("/wallet/withdrawbalance", api.withdrawBalance)
	mux.HandleFunc("/wallet/updatebrokerage", api.updateBrokerage)
	mux.HandleFunc("/wallet/freezebalance", api.freezeBalance)
	mux.HandleFunc("/wallet/unfreezebalance", api.unfreezeBalance)
	mux.HandleFunc("/wallet/freezebalancev2", api.freezeBalanceV2)
	mux.HandleFunc("/wallet/unfreezebalancev2", api.unfreezeBalanceV2)
	mux.HandleFunc("/wallet/cancelallunfreezev2", api.cancelAllUnfreezeV2)
	mux.HandleFunc("/wallet/delegateresource", api.delegateResource)
	mux.HandleFunc("/wallet/undelegateresource", api.undelegateResource)
	mux.HandleFunc("/wallet/withdrawexpireunfreeze", api.withdrawExpireUnfreeze)

	// M5.1 PR-3: TRC10 asset extras
	mux.HandleFunc("/wallet/createassetissue", api.createAssetIssue)
	mux.HandleFunc("/wallet/updateasset", api.updateAsset)
	mux.HandleFunc("/wallet/getassetissuelistbyname", api.getAssetIssueListByName)

	// M5.1 PR-4: Smart contract extras
	mux.HandleFunc("/wallet/clearabi", api.clearABI)

	// M9.8: Smart contract update endpoints
	mux.HandleFunc("/wallet/updatesetting", api.updateSetting)
	mux.HandleFunc("/wallet/updateenergylimit", api.updateEnergyLimit)

	// M5.1 PR-5: Exchange / Market
	mux.HandleFunc("/wallet/listexchanges", api.listExchanges)
	mux.HandleFunc("/wallet/exchangecreate", api.exchangeCreate)
	mux.HandleFunc("/wallet/exchangeinject", api.exchangeInject)
	mux.HandleFunc("/wallet/exchangetransaction", api.exchangeTransaction)
	mux.HandleFunc("/wallet/exchangewithdraw", api.exchangeWithdraw)
	mux.HandleFunc("/wallet/marketsellasset", api.marketSellAsset)
	mux.HandleFunc("/wallet/marketcancelorder", api.marketCancelOrder)

	// M5.1 PR-6: Proposal / Monitoring extras
	mux.HandleFunc("/wallet/getproposalbyid", api.getProposalById)
	mux.HandleFunc("/wallet/getpaginatedproposallist", api.getPaginatedProposalList)
	mux.HandleFunc("/wallet/metrics", api.metricsStub)

	// M5.1 PR-7: Transaction meta
	mux.HandleFunc("/wallet/gettransactionreceiptbyid", api.getTransactionReceiptById)
	mux.HandleFunc("/wallet/validateaddress", api.validateAddress)

	// M8.1: /walletsolidity/ and /walletpbft/ confirmation-depth variants
	api.RegisterSolidityRoutes(mux)
	api.RegisterPbftRoutes(mux)
}

func (api *API) getNowBlock(w http.ResponseWriter, r *http.Request) {
	block := api.backend.CurrentBlock()
	if block == nil {
		http.Error(w, "no current block", http.StatusInternalServerError)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getBlockByNum(w http.ResponseWriter, r *http.Request) {
	numStr := r.URL.Query().Get("num")
	if numStr == "" {
		var body struct {
			Num int64 `json:"num"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			numStr = strconv.FormatInt(body.Num, 10)
		}
	}
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid block number", http.StatusBadRequest)
		return
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getAccount(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrHex = body.Address
		}
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	acc, err := api.backend.GetAccount(addr)
	if err != nil || acc == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, acc.Proto())
}

func (api *API) broadcastTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	var envelope struct {
		RawDataHex string   `json:"raw_data_hex"`
		Signature  []string `json:"signature"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.RawDataHex == "" {
		http.Error(w, "invalid transaction: missing raw_data_hex", http.StatusBadRequest)
		return
	}

	rawBytes, err := hex.DecodeString(envelope.RawDataHex)
	if err != nil {
		http.Error(w, "invalid raw_data_hex", http.StatusBadRequest)
		return
	}

	var rawData corepb.TransactionRaw
	if err := proto.Unmarshal(rawBytes, &rawData); err != nil {
		http.Error(w, "invalid raw_data proto", http.StatusBadRequest)
		return
	}

	sigs := make([][]byte, 0, len(envelope.Signature))
	for _, s := range envelope.Signature {
		sigBytes, err := hex.DecodeString(s)
		if err != nil {
			http.Error(w, "invalid signature hex", http.StatusBadRequest)
			return
		}
		sigs = append(sigs, sigBytes)
	}

	pbTx := &corepb.Transaction{
		RawData:   &rawData,
		Signature: sigs,
	}

	// Compute txID from raw_data
	h := sha256.Sum256(rawBytes)
	txID := hex.EncodeToString(h[:])

	tx := types.NewTransactionFromPB(pbTx)

	// Validate business logic synchronously (mirrors java-tron Wallet#broadcastTransaction).
	// ValidateTransaction returns nil for unsupported contract types (no blocking).
	if err := api.backend.ValidateTransaction(tx); err != nil {
		data, _ := marshalValidateError(txID, err.Error())
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}

	if err := api.backend.BroadcastTransaction(tx); err != nil {
		data, _ := MarshalBroadcastResult(false, txID, err.Error())
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}

	data, _ := MarshalBroadcastResult(true, txID, "")
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// marshalValidateError builds a java-tron-compatible CONTRACT_VALIDATE_ERROR response.
func marshalValidateError(txID string, errMsg string) ([]byte, error) {
	result := map[string]any{
		"result":  false,
		"txid":    txID,
		"code":    "CONTRACT_VALIDATE_ERROR",
		"message": hex.EncodeToString([]byte(errMsg)),
	}
	return json.Marshal(result)
}

func (api *API) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := api.backend.GetNodeInfo()
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getTxPoolCount(w http.ResponseWriter, r *http.Request) {
	count := api.backend.PendingTransactionCount()
	resp := map[string]int{"count": count}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func writeTronJSON(w http.ResponseWriter, msg proto.Message) {
	data, err := marshalTronJSON(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func writeBlockJSON(w http.ResponseWriter, msg proto.Message) {
	data, err := MarshalBlock(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getContract(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("value")
	if addrHex == "" {
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrHex = body.Value
		}
	}
	if addrHex == "" {
		http.Error(w, "contract address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	sc, err := api.backend.GetContract(addr)
	if err != nil || sc == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, sc)
}

func (api *API) triggerConstantContract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string `json:"owner_address"`
		ContractAddress string `json:"contract_address"`
		FunctionSelector string `json:"function_selector"`
		Parameter       string `json:"parameter"`
		Data            string `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))

	// Build calldata: if data is provided directly, use it;
	// otherwise build from function_selector + parameter
	var data []byte
	if body.Data != "" {
		data = common.FromHex(body.Data)
	} else if body.FunctionSelector != "" {
		// Hash the function selector to get the 4-byte selector
		selectorHash := common.Keccak256([]byte(body.FunctionSelector))
		data = selectorHash[:4]
		if body.Parameter != "" {
			paramBytes := common.FromHex(body.Parameter)
			data = append(data, paramBytes...)
		}
	}

	result, err := api.backend.TriggerConstantContract(owner, contract, data, 30_000_000)

	resp := map[string]interface{}{
		"result": map[string]interface{}{
			"result": err == nil,
		},
	}
	if result != nil {
		resp["energy_used"] = result.EnergyUsed
		if len(result.Result) > 0 {
			resp["constant_result"] = []string{hex.EncodeToString(result.Result)}
		}
	}
	if err != nil {
		resp["result"].(map[string]interface{})["message"] = hex.EncodeToString([]byte(err.Error()))
	}

	jsonData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func writeTransactionJSON(w http.ResponseWriter, tx *corepb.Transaction) {
	if tx == nil {
		http.Error(w, "nil transaction", http.StatusInternalServerError)
		return
	}
	result := marshalMessage(tx.ProtoReflect())
	addTxComputedFields(result, tx.ProtoReflect())
	data, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) createTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ToAddress    string `json:"to_address"`
		Amount       int64  `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	to := common.BytesToAddress(common.FromHex(body.ToAddress))
	tx, err := api.backend.BuildTransferTransaction(owner, to, body.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) deployContract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress               string `json:"owner_address"`
		ABI                        string `json:"abi"`
		Bytecode                   string `json:"bytecode"`
		FeeLimit                   int64  `json:"fee_limit"`
		CallValue                  int64  `json:"call_value"`
		Name                       string `json:"name"`
		ConsumeUserResourcePercent int64  `json:"consume_user_resource_percent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	bytecode := common.FromHex(body.Bytecode)
	tx, err := api.backend.BuildDeployContractTransaction(owner, body.ABI, bytecode,
		body.FeeLimit, body.CallValue, body.Name, body.ConsumeUserResourcePercent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) triggerSmartContract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress     string `json:"owner_address"`
		ContractAddress  string `json:"contract_address"`
		FunctionSelector string `json:"function_selector"`
		Parameter        string `json:"parameter"`
		Data             string `json:"data"`
		FeeLimit         int64  `json:"fee_limit"`
		CallValue        int64  `json:"call_value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))

	var data []byte
	if body.Data != "" {
		data = common.FromHex(body.Data)
	} else if body.FunctionSelector != "" {
		selectorHash := common.Keccak256([]byte(body.FunctionSelector))
		data = selectorHash[:4]
		if body.Parameter != "" {
			data = append(data, common.FromHex(body.Parameter)...)
		}
	}

	tx, triggerResult, err := api.backend.BuildTriggerContractTransaction(owner, contract, data, body.FeeLimit, body.CallValue)

	resp := map[string]interface{}{
		"result": map[string]interface{}{
			"result": err == nil,
		},
	}
	if tx != nil {
		txResult := marshalMessage(tx.ProtoReflect())
		addTxComputedFields(txResult, tx.ProtoReflect())
		resp["transaction"] = txResult
	}
	if triggerResult != nil {
		resp["energy_used"] = triggerResult.EnergyUsed
		if len(triggerResult.Result) > 0 {
			resp["constant_result"] = []string{hex.EncodeToString(triggerResult.Result)}
		}
	}
	if err != nil {
		resp["result"].(map[string]interface{})["message"] = hex.EncodeToString([]byte(err.Error()))
	}

	jsonData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (api *API) estimateEnergy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress     string `json:"owner_address"`
		ContractAddress  string `json:"contract_address"`
		FunctionSelector string `json:"function_selector"`
		Parameter        string `json:"parameter"`
		Data             string `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))

	var data []byte
	if body.Data != "" {
		data = common.FromHex(body.Data)
	} else if body.FunctionSelector != "" {
		selectorHash := common.Keccak256([]byte(body.FunctionSelector))
		data = selectorHash[:4]
		if body.Parameter != "" {
			data = append(data, common.FromHex(body.Parameter)...)
		}
	}

	energy, err := api.backend.EstimateEnergy(owner, contract, data)
	resp := map[string]interface{}{
		"result": map[string]interface{}{
			"result": err == nil,
		},
	}
	if err == nil {
		resp["energy_required"] = energy
	} else {
		resp["result"].(map[string]interface{})["message"] = hex.EncodeToString([]byte(err.Error()))
	}

	jsonData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (api *API) getTransactionByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)
	tx, err := api.backend.GetTransactionByID(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getTransactionInfoByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)
	info, err := api.backend.GetTransactionInfoByID(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, info)
}

func (api *API) getTransactionInfoByBlockNum(w http.ResponseWriter, r *http.Request) {
	numStr := r.URL.Query().Get("num")
	if numStr == "" {
		var body struct {
			Num int64 `json:"num"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			numStr = strconv.FormatInt(body.Num, 10)
		}
	}
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid block number", http.StatusBadRequest)
		return
	}
	infos, err := api.backend.GetTransactionInfoByBlockNum(num)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var result []map[string]interface{}
	for _, info := range infos {
		m := marshalMessage(info.ProtoReflect())
		result = append(result, m)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	data, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getBlockByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)
	block, err := api.backend.GetBlockByHash(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getBlockByLimitNext(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StartNum int64 `json:"startNum"`
		EndNum   int64 `json:"endNum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	blocks, err := api.backend.GetBlocksByRange(uint64(body.StartNum), uint64(body.EndNum))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var blockList []map[string]interface{}
	for _, b := range blocks {
		data, err := MarshalBlock(b.Proto())
		if err != nil {
			continue
		}
		var m map[string]interface{}
		json.Unmarshal(data, &m)
		blockList = append(blockList, m)
	}
	if blockList == nil {
		blockList = []map[string]interface{}{}
	}
	resp := map[string]interface{}{
		"block": blockList,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAccountResource(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrHex = body.Address
		}
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	res, err := api.backend.GetAccountResource(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getChainParameters(w http.ResponseWriter, r *http.Request) {
	params := api.backend.GetChainParameters()
	resp := map[string]interface{}{
		"chainParameter": params,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) listWitnesses(w http.ResponseWriter, r *http.Request) {
	witnesses, err := api.backend.ListWitnesses()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]interface{}{
		"witnesses": witnesses,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getNextMaintenanceTime(w http.ResponseWriter, r *http.Request) {
	t := api.backend.NextMaintenanceTime()
	resp := map[string]int64{"num": t}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) proposalCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Parameters   []struct {
			Key   int64 `json:"key"`
			Value int64 `json:"value"`
		} `json:"parameters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	params := make(map[int64]int64, len(body.Parameters))
	for _, p := range body.Parameters {
		params[p.Key] = p.Value
	}
	tx, err := api.backend.BuildProposalCreateTransaction(owner, params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) proposalApprove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress  string `json:"owner_address"`
		ProposalID    int64  `json:"proposal_id"`
		IsAddApproval bool   `json:"is_add_approval"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	tx, err := api.backend.BuildProposalApproveTransaction(owner, body.ProposalID, body.IsAddApproval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) proposalDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ProposalID   int64  `json:"proposal_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	tx, err := api.backend.BuildProposalDeleteTransaction(owner, body.ProposalID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) listProposals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	proposals, err := api.backend.ListProposals()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"proposals": proposals})
}

func (api *API) getDelegatedResourceV2(w http.ResponseWriter, r *http.Request) {
	var body struct {
		FromAddress string `json:"fromAddress"`
		ToAddress   string `json:"toAddress"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.FromAddress == "" || body.ToAddress == "" {
		http.Error(w, "fromAddress and toAddress required", http.StatusBadRequest)
		return
	}
	from := common.BytesToAddress(common.FromHex(body.FromAddress))
	to := common.BytesToAddress(common.FromHex(body.ToAddress))
	info, err := api.backend.GetDelegatedResourceV2(from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	list := []*DelegatedResourceInfo{}
	if info != nil {
		list = []*DelegatedResourceInfo{info}
	}
	data, _ := json.Marshal(map[string]interface{}{"delegatedResource": list})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getDelegatedResourceAccountIndexV2(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.Value))
	info, err := api.backend.GetDelegatedResourceAccountIndexV2(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) canDelegateResource(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Balance      int64  `json:"balance"`
		Type         int32  `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OwnerAddress == "" {
		http.Error(w, "owner_address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	info, err := api.backend.CanDelegateResource(addr, body.Balance, corepb.ResourceCode(body.Type))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getCanWithdrawUnfreezeAmount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Timestamp    int64  `json:"timestamp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OwnerAddress == "" {
		http.Error(w, "owner_address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	info, err := api.backend.GetCanWithdrawUnfreezeAmount(addr, body.Timestamp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAvailableUnfreezeCount(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("owner_address")
	if addrHex == "" {
		var body struct {
			OwnerAddress string `json:"owner_address"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		addrHex = body.OwnerAddress
	}
	if addrHex == "" {
		http.Error(w, "owner_address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	info, err := api.backend.GetAvailableUnfreezeCount(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getReward(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		addrHex = body.Address
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	info, err := api.backend.GetReward(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getTransactionFromPending(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	tx, err := api.backend.GetTransactionFromPending(body.Value)
	if err != nil {
		data, _ := json.Marshal(map[string]string{"Error": err.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getTransactionListFromPending(w http.ResponseWriter, r *http.Request) {
	txs, err := api.backend.GetTransactionListFromPending()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var result []map[string]interface{}
	for _, tx := range txs {
		m := marshalMessage(tx.ProtoReflect())
		addTxComputedFields(m, tx.ProtoReflect())
		result = append(result, m)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	data, _ := json.Marshal(map[string]interface{}{"transaction": result})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) listNodes(w http.ResponseWriter, r *http.Request) {
	peers, err := api.backend.ListNodes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type nodeAddress struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	type node struct {
		Address nodeAddress `json:"address"`
	}
	nodes := make([]node, len(peers))
	for i, p := range peers {
		nodes[i] = node{Address: nodeAddress{Host: p.Host, Port: p.Port}}
	}
	data, _ := json.Marshal(map[string]interface{}{"nodes": nodes})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAssetIssueByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value int64 `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	id := body.Value
	if id == 0 {
		// fallback: query param
		s := r.URL.Query().Get("value")
		if s == "" {
			http.Error(w, "value required", http.StatusBadRequest)
			return
		}
		var err error
		id, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			http.Error(w, "invalid token ID", http.StatusBadRequest)
			return
		}
	}
	asset := api.backend.GetAssetIssueByID(id)
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}

func (api *API) getAssetIssueByName(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	asset := api.backend.GetAssetIssueByName(common.FromHex(body.Value))
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}

func (api *API) getAssetIssueList(w http.ResponseWriter, r *http.Request) {
	assets := api.backend.GetAssetIssueList()
	var list []map[string]any
	for _, a := range assets {
		list = append(list, marshalMessage(a.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	resp := map[string]any{"assetIssue": list}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getPaginatedAssetIssueList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if body.Limit <= 0 {
		body.Limit = 20
	}
	assets := api.backend.GetAssetIssueListPaginated(body.Offset, body.Limit)
	var list []map[string]any
	for _, a := range assets {
		list = append(list, marshalMessage(a.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	resp := map[string]any{"assetIssue": list}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAssetIssueByAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Address string `json:"address"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Address == "" {
		body.Address = r.URL.Query().Get("address")
	}
	if body.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.Address))
	asset := api.backend.GetAssetIssueByAccount(addr)
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}

func (api *API) getMarketOrderByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	orderID := common.FromHex(body.Value)
	order := api.backend.GetMarketOrderByID(orderID)
	if order == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, order)
}

func (api *API) getMarketOrdersFromAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Address string `json:"address"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Address == "" {
		body.Address = r.URL.Query().Get("address")
	}
	if body.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.Address))
	orders := api.backend.GetMarketOrdersByAccount(addr)
	var list []map[string]any
	for _, o := range orders {
		list = append(list, marshalMessage(o.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	data, _ := json.Marshal(map[string]any{"orders": list})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getMarketPriceByPair(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SellTokenId string `json:"sell_token_id"`
		BuyTokenId  string `json:"buy_token_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.SellTokenId == "" || body.BuyTokenId == "" {
		http.Error(w, "sell_token_id and buy_token_id required", http.StatusBadRequest)
		return
	}
	pl := api.backend.GetMarketPriceByPair(common.FromHex(body.SellTokenId), common.FromHex(body.BuyTokenId))
	if pl == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, pl)
}
