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
	mux.HandleFunc("/walletsolidity/getaccountresource", api.getSolidAccountResource)
	mux.HandleFunc("/walletsolidity/getreward", api.getSolidReward)
	mux.HandleFunc("/walletsolidity/getbrokerage", api.getSolidBrokerage)
	mux.HandleFunc("/walletsolidity/getBrokerage", api.getSolidBrokerage)
	mux.HandleFunc("/walletsolidity/getaccountbyid", api.getSolidAccountById)
	mux.HandleFunc("/walletsolidity/getaccountnet", api.getSolidAccountNet)
	mux.HandleFunc("/walletsolidity/listwitnesses", api.getSolidWitnesses)
	mux.HandleFunc("/walletsolidity/getchainparameters", api.getSolidChainParameters)
	mux.HandleFunc("/walletsolidity/getnextmaintenancetime", api.getSolidNextMaintenanceTime)
	mux.HandleFunc("/walletsolidity/gettransactionbyid", api.getTransactionByID)
	mux.HandleFunc("/walletsolidity/gettransactioninfobyid", api.getTransactionInfoByID)
	mux.HandleFunc("/walletsolidity/getassetissuebyid", api.getSolidAssetIssueByID)
	mux.HandleFunc("/walletsolidity/getassetissuebyname", api.getSolidAssetIssueByName)
	mux.HandleFunc("/walletsolidity/getassetissuelist", api.getSolidAssetIssueList)
	mux.HandleFunc("/walletsolidity/getpaginatedassetissuelist", api.getSolidPaginatedAssetIssueList)
	mux.HandleFunc("/walletsolidity/getmarketorderbyid", api.getSolidMarketOrderByID)
	mux.HandleFunc("/walletsolidity/getmarketordersfromaccount", api.getSolidMarketOrdersFromAccount)
	mux.HandleFunc("/walletsolidity/getmarketpricebypair", api.getSolidMarketPriceByPair)
	mux.HandleFunc("/walletsolidity/listexchanges", api.getSolidExchanges)
	mux.HandleFunc("/walletsolidity/getdelegatedresourcev2", api.getSolidDelegatedResourceV2)
	mux.HandleFunc("/walletsolidity/getdelegatedresourceaccountindexv2", api.getSolidDelegatedResourceAccountIndexV2)
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
	mux.HandleFunc("/walletpbft/getaccountresource", api.getPbftAccountResource)
	mux.HandleFunc("/walletpbft/getreward", api.getPbftReward)
	mux.HandleFunc("/walletpbft/getbrokerage", api.getPbftBrokerage)
	mux.HandleFunc("/walletpbft/getBrokerage", api.getPbftBrokerage)
	mux.HandleFunc("/walletpbft/getaccountbyid", api.getPbftAccountById)
	mux.HandleFunc("/walletpbft/getaccountnet", api.getPbftAccountNet)
	mux.HandleFunc("/walletpbft/listwitnesses", api.getPbftWitnesses)
	mux.HandleFunc("/walletpbft/getchainparameters", api.getPbftChainParameters)
	mux.HandleFunc("/walletpbft/getnextmaintenancetime", api.getPbftNextMaintenanceTime)
	mux.HandleFunc("/walletpbft/gettransactionbyid", api.getTransactionByID)
	mux.HandleFunc("/walletpbft/gettransactioninfobyid", api.getTransactionInfoByID)
	mux.HandleFunc("/walletpbft/getassetissuebyid", api.getPbftAssetIssueByID)
	mux.HandleFunc("/walletpbft/getassetissuebyname", api.getPbftAssetIssueByName)
	mux.HandleFunc("/walletpbft/getassetissuelist", api.getPbftAssetIssueList)
	mux.HandleFunc("/walletpbft/getpaginatedassetissuelist", api.getPbftPaginatedAssetIssueList)
	mux.HandleFunc("/walletpbft/getmarketorderbyid", api.getPbftMarketOrderByID)
	mux.HandleFunc("/walletpbft/getmarketordersfromaccount", api.getPbftMarketOrdersFromAccount)
	mux.HandleFunc("/walletpbft/getmarketpricebypair", api.getPbftMarketPriceByPair)
	mux.HandleFunc("/walletpbft/listexchanges", api.getPbftExchanges)
	mux.HandleFunc("/walletpbft/getdelegatedresourcev2", api.getPbftDelegatedResourceV2)
	mux.HandleFunc("/walletpbft/getdelegatedresourceaccountindexv2", api.getPbftDelegatedResourceAccountIndexV2)
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

func (api *API) getSolidAccountResource(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountResource(w, r, api.solidBoundNum)
}

func (api *API) getPbftAccountResource(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountResource(w, r, api.pbftBoundNum)
}

func (api *API) getSolidReward(w http.ResponseWriter, r *http.Request) {
	api.handleGetReward(w, r, api.solidBoundNum)
}

func (api *API) getPbftReward(w http.ResponseWriter, r *http.Request) {
	api.handleGetReward(w, r, api.pbftBoundNum)
}

func (api *API) getSolidBrokerage(w http.ResponseWriter, r *http.Request) {
	api.handleGetBrokerage(w, r, api.solidBoundNum)
}

func (api *API) getPbftBrokerage(w http.ResponseWriter, r *http.Request) {
	api.handleGetBrokerage(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAccountById(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountById(w, r, api.solidBoundNum)
}

func (api *API) getPbftAccountById(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountById(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAccountNet(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountNet(w, r, api.solidBoundNum)
}

func (api *API) getPbftAccountNet(w http.ResponseWriter, r *http.Request) {
	api.handleGetAccountNet(w, r, api.pbftBoundNum)
}

func (api *API) getSolidWitnesses(w http.ResponseWriter, r *http.Request) {
	api.handleListWitnesses(w, r, api.solidBoundNum)
}

func (api *API) getPbftWitnesses(w http.ResponseWriter, r *http.Request) {
	api.handleListWitnesses(w, r, api.pbftBoundNum)
}

func (api *API) getSolidChainParameters(w http.ResponseWriter, r *http.Request) {
	api.handleGetChainParameters(w, r, api.solidBoundNum)
}

func (api *API) getPbftChainParameters(w http.ResponseWriter, r *http.Request) {
	api.handleGetChainParameters(w, r, api.pbftBoundNum)
}

func (api *API) getSolidNextMaintenanceTime(w http.ResponseWriter, r *http.Request) {
	api.handleGetNextMaintenanceTime(w, r, api.solidBoundNum)
}

func (api *API) getPbftNextMaintenanceTime(w http.ResponseWriter, r *http.Request) {
	api.handleGetNextMaintenanceTime(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAssetIssueByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueByID(w, r, api.solidBoundNum)
}

func (api *API) getPbftAssetIssueByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueByID(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAssetIssueByName(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueByName(w, r, api.solidBoundNum)
}

func (api *API) getPbftAssetIssueByName(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueByName(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAssetIssueList(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueList(w, r, api.solidBoundNum)
}

func (api *API) getPbftAssetIssueList(w http.ResponseWriter, r *http.Request) {
	api.handleGetAssetIssueList(w, r, api.pbftBoundNum)
}

func (api *API) getSolidPaginatedAssetIssueList(w http.ResponseWriter, r *http.Request) {
	api.handleGetPaginatedAssetIssueList(w, r, api.solidBoundNum)
}

func (api *API) getPbftPaginatedAssetIssueList(w http.ResponseWriter, r *http.Request) {
	api.handleGetPaginatedAssetIssueList(w, r, api.pbftBoundNum)
}

func (api *API) getSolidMarketOrderByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketOrderByID(w, r, api.solidBoundNum)
}

func (api *API) getPbftMarketOrderByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketOrderByID(w, r, api.pbftBoundNum)
}

func (api *API) getSolidMarketOrdersFromAccount(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketOrdersFromAccount(w, r, api.solidBoundNum)
}

func (api *API) getPbftMarketOrdersFromAccount(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketOrdersFromAccount(w, r, api.pbftBoundNum)
}

func (api *API) getSolidMarketPriceByPair(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketPriceByPair(w, r, api.solidBoundNum)
}

func (api *API) getPbftMarketPriceByPair(w http.ResponseWriter, r *http.Request) {
	api.handleGetMarketPriceByPair(w, r, api.pbftBoundNum)
}

func (api *API) getSolidExchanges(w http.ResponseWriter, r *http.Request) {
	api.handleListExchanges(w, r, api.solidBoundNum)
}

func (api *API) getPbftExchanges(w http.ResponseWriter, r *http.Request) {
	api.handleListExchanges(w, r, api.pbftBoundNum)
}

func (api *API) getSolidDelegatedResourceV2(w http.ResponseWriter, r *http.Request) {
	api.handleGetDelegatedResourceV2(w, r, api.solidBoundNum)
}

func (api *API) getPbftDelegatedResourceV2(w http.ResponseWriter, r *http.Request) {
	api.handleGetDelegatedResourceV2(w, r, api.pbftBoundNum)
}

func (api *API) getSolidDelegatedResourceAccountIndexV2(w http.ResponseWriter, r *http.Request) {
	api.handleGetDelegatedResourceAccountIndexV2(w, r, api.solidBoundNum)
}

func (api *API) getPbftDelegatedResourceAccountIndexV2(w http.ResponseWriter, r *http.Request) {
	api.handleGetDelegatedResourceAccountIndexV2(w, r, api.pbftBoundNum)
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
