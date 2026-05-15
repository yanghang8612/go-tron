package tronapi

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// RegisterSolidityRoutes registers /walletsolidity/ and /walletpbft/ prefixed endpoints.
// Most handlers are identical to /wallet/ and are registered by reference.
// Only block-returning endpoints differ: they clamp to the solid/pbft head.
func (api *API) RegisterSolidityRoutes(mux *http.ServeMux) {
	// Block queries — variant-specific (return solid block)
	mux.HandleFunc("/walletsolidity/getnowblock", api.getSolidNowBlock)
	mux.HandleFunc("/walletsolidity/getblockbynum", api.getSolidBlockByNum)
	mux.HandleFunc("/walletsolidity/gettransactioninfobyblocknum", api.getSolidTxInfoByBlockNum)

	// All other endpoints are identical to /wallet/ — re-register by reference.
	// Block-by-hash and tx-by-hash lookups are state-independent (hash → block/tx
	// already keyed in rawdb), so live and solid handlers return the same bytes.
	mux.HandleFunc("/walletsolidity/getblockbyid", api.getBlockByID)
	mux.HandleFunc("/walletsolidity/getblockbylimitnext", api.getBlockByLimitNext)
	// State-dependent endpoints route through the solid bound so the
	// response reflects the post-solidified state, not live head.
	mux.HandleFunc("/walletsolidity/getaccount", api.getSolidAccount)
	mux.HandleFunc("/walletsolidity/getaccountbyid", api.getAccountById)
	mux.HandleFunc("/walletsolidity/getaccountresource", api.getAccountResource)
	mux.HandleFunc("/walletsolidity/getaccountnet", api.getAccountNet)
	mux.HandleFunc("/walletsolidity/listwitnesses", api.listWitnesses)
	mux.HandleFunc("/walletsolidity/getchainparameters", api.getChainParameters)
	mux.HandleFunc("/walletsolidity/getnextmaintenancetime", api.getNextMaintenanceTime)
	mux.HandleFunc("/walletsolidity/gettransactionbyid", api.getTransactionByID)
	mux.HandleFunc("/walletsolidity/gettransactioninfobyid", api.getTransactionInfoByID)
	mux.HandleFunc("/walletsolidity/getassetissuebyid", api.getAssetIssueByID)
	mux.HandleFunc("/walletsolidity/getassetissuebyname", api.getAssetIssueByName)
	mux.HandleFunc("/walletsolidity/getassetissuelist", api.getAssetIssueList)
	mux.HandleFunc("/walletsolidity/getpaginatedassetissuelist", api.getPaginatedAssetIssueList)
	mux.HandleFunc("/walletsolidity/getmarketorderbyid", api.getMarketOrderByID)
	mux.HandleFunc("/walletsolidity/getmarketordersfromaccount", api.getMarketOrdersFromAccount)
	mux.HandleFunc("/walletsolidity/getmarketpricebypair", api.getMarketPriceByPair)
	mux.HandleFunc("/walletsolidity/listexchanges", api.listExchanges)
	mux.HandleFunc("/walletsolidity/getdelegatedresourcev2", api.getDelegatedResourceV2)
	mux.HandleFunc("/walletsolidity/getdelegatedresourceaccountindexv2", api.getDelegatedResourceAccountIndexV2)
	mux.HandleFunc("/walletsolidity/getreward", api.getReward)
	mux.HandleFunc("/walletsolidity/estimateenergy", api.estimateEnergy)
	mux.HandleFunc("/walletsolidity/triggerconstantcontract", api.triggerConstantContract)
}

// RegisterPbftRoutes registers /walletpbft/ prefixed endpoints.
func (api *API) RegisterPbftRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/walletpbft/getnowblock", api.getPbftNowBlock)
	mux.HandleFunc("/walletpbft/getblockbynum", api.getPbftBlockByNum)
	mux.HandleFunc("/walletpbft/gettransactioninfobyblocknum", api.getPbftTxInfoByBlockNum)

	mux.HandleFunc("/walletpbft/getblockbyid", api.getBlockByID)
	mux.HandleFunc("/walletpbft/getblockbylimitnext", api.getBlockByLimitNext)
	mux.HandleFunc("/walletpbft/getaccount", api.getPbftAccount)
	mux.HandleFunc("/walletpbft/getaccountbyid", api.getAccountById)
	mux.HandleFunc("/walletpbft/getaccountresource", api.getAccountResource)
	mux.HandleFunc("/walletpbft/getaccountnet", api.getAccountNet)
	mux.HandleFunc("/walletpbft/listwitnesses", api.listWitnesses)
	mux.HandleFunc("/walletpbft/getchainparameters", api.getChainParameters)
	mux.HandleFunc("/walletpbft/getnextmaintenancetime", api.getNextMaintenanceTime)
	mux.HandleFunc("/walletpbft/gettransactionbyid", api.getTransactionByID)
	mux.HandleFunc("/walletpbft/gettransactioninfobyid", api.getTransactionInfoByID)
	mux.HandleFunc("/walletpbft/getassetissuebyid", api.getAssetIssueByID)
	mux.HandleFunc("/walletpbft/getassetissuebyname", api.getAssetIssueByName)
	mux.HandleFunc("/walletpbft/getassetissuelist", api.getAssetIssueList)
	mux.HandleFunc("/walletpbft/getpaginatedassetissuelist", api.getPaginatedAssetIssueList)
	mux.HandleFunc("/walletpbft/getmarketorderbyid", api.getMarketOrderByID)
	mux.HandleFunc("/walletpbft/getmarketordersfromaccount", api.getMarketOrdersFromAccount)
	mux.HandleFunc("/walletpbft/getmarketpricebypair", api.getMarketPriceByPair)
	mux.HandleFunc("/walletpbft/listexchanges", api.listExchanges)
	mux.HandleFunc("/walletpbft/getdelegatedresourcev2", api.getDelegatedResourceV2)
	mux.HandleFunc("/walletpbft/getdelegatedresourceaccountindexv2", api.getDelegatedResourceAccountIndexV2)
	mux.HandleFunc("/walletpbft/getreward", api.getReward)
	mux.HandleFunc("/walletpbft/estimateenergy", api.estimateEnergy)
	mux.HandleFunc("/walletpbft/triggerconstantcontract", api.triggerConstantContract)
}

// solidBoundNum returns the solid block number as the upper bound.
func (api *API) solidBoundNum() uint64 {
	return api.backend.SolidifiedBlockNum()
}

// pbftBoundNum returns the PBFT-confirmed block number, falling back to the solid
// block if PBFT has not been activated yet (ReadLatestPbftBlockNum returns -1).
func (api *API) pbftBoundNum() uint64 {
	n := api.backend.LatestPbftBlockNum()
	if n < 0 {
		return api.solidBoundNum()
	}
	return uint64(n)
}

// --- State-bounded variants ---

func (api *API) getSolidAccount(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccount(w, r, api.solidBoundNum)
}

func (api *API) getPbftAccount(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccount(w, r, api.pbftBoundNum)
}

// --- Solid-block variants ---

func (api *API) getSolidNowBlock(w http.ResponseWriter, r *http.Request) {
	block, err := api.backend.GetBlockByNumber(api.solidBoundNum())
	if err != nil || block == nil {
		http.Error(w, "solid block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getSolidBlockByNum(w http.ResponseWriter, r *http.Request) {
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
	if num > api.solidBoundNum() {
		http.Error(w, "block not yet solidified", http.StatusNotFound)
		return
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getSolidTxInfoByBlockNum(w http.ResponseWriter, r *http.Request) {
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
	if num > api.solidBoundNum() {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}
	api.getTransactionInfoByBlockNum(w, r)
}

// --- PBFT-block variants ---

func (api *API) getPbftNowBlock(w http.ResponseWriter, r *http.Request) {
	block, err := api.backend.GetBlockByNumber(api.pbftBoundNum())
	if err != nil || block == nil {
		http.Error(w, "pbft block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getPbftBlockByNum(w http.ResponseWriter, r *http.Request) {
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
	if num > api.pbftBoundNum() {
		http.Error(w, "block not yet pbft-confirmed", http.StatusNotFound)
		return
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getPbftTxInfoByBlockNum(w http.ResponseWriter, r *http.Request) {
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
	if num > api.pbftBoundNum() {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}
	api.getTransactionInfoByBlockNum(w, r)
}
