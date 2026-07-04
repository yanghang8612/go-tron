package tronapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
)

// RegisterSolidityRoutes registers /walletsolidity/ and /walletpbft/ prefixed endpoints.
// Most handlers are identical to /wallet/ and are registered by reference.
// Only block-returning endpoints differ: they clamp to the solid/pbft head.
func (api *API) RegisterSolidityRoutes(mux *http.ServeMux) {
	// Block queries — variant-specific (return solid block)
	mux.HandleFunc("/walletsolidity/getnowblock", api.getSolidNowBlock)
	mux.HandleFunc("/walletsolidity/getblockbynum", api.getSolidBlockByNum)
	mux.HandleFunc("/walletsolidity/gettransactioninfobyblocknum", api.getSolidTxInfoByBlockNum)

	// Other block-returning endpoints are bound-aware too: block ids carry their
	// number prefix, and block ranges must not cross the solid boundary.
	mux.HandleFunc("/walletsolidity/getblockbyid", api.getSolidBlockByID)
	mux.HandleFunc("/walletsolidity/getblockbylimitnext", api.getSolidBlockByLimitNext)
	// State-dependent endpoints route through the solid bound so the
	// response reflects the post-solidified state, not live head.
	mux.HandleFunc("/walletsolidity/getaccount", api.getSolidAccount)
	mux.HandleFunc("/walletsolidity/getaccountresource", api.getSolidAccountResource)
	mux.HandleFunc("/walletsolidity/getcontract", api.getSolidContract)
	mux.HandleFunc("/walletsolidity/getreward", api.getSolidReward)
	mux.HandleFunc("/walletsolidity/getbrokerage", api.getSolidBrokerage)
	mux.HandleFunc("/walletsolidity/getBrokerage", api.getSolidBrokerage)
	mux.HandleFunc("/walletsolidity/getaccountbyid", api.getSolidAccountById)
	mux.HandleFunc("/walletsolidity/getaccountnet", api.getSolidAccountNet)
	mux.HandleFunc("/walletsolidity/listwitnesses", api.getSolidWitnesses)
	mux.HandleFunc("/walletsolidity/getchainparameters", api.getSolidChainParameters)
	mux.HandleFunc("/walletsolidity/getnextmaintenancetime", api.getSolidNextMaintenanceTime)
	mux.HandleFunc("/walletsolidity/listproposals", api.getSolidProposals)
	mux.HandleFunc("/walletsolidity/getproposalbyid", api.getSolidProposalByID)
	mux.HandleFunc("/walletsolidity/getpaginatedproposallist", api.getSolidPaginatedProposalList)
	mux.HandleFunc("/walletsolidity/gettransactionbyid", api.getSolidTransactionByID)
	mux.HandleFunc("/walletsolidity/gettransactioninfobyid", api.getSolidTransactionInfoByID)
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
	mux.HandleFunc("/walletsolidity/candelegateresource", api.getSolidCanDelegateResource)
	mux.HandleFunc("/walletsolidity/getcanwithdrawunfreezeamount", api.getSolidCanWithdrawUnfreezeAmount)
	mux.HandleFunc("/walletsolidity/getavailableunfreezecount", api.getSolidAvailableUnfreezeCount)
	mux.HandleFunc("/walletsolidity/estimateenergy", api.estimateSolidEnergy)
	mux.HandleFunc("/walletsolidity/triggerconstantcontract", api.triggerSolidConstantContract)
}

// RegisterPbftRoutes registers /walletpbft/ prefixed endpoints.
func (api *API) RegisterPbftRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/walletpbft/getnowblock", api.getPbftNowBlock)
	mux.HandleFunc("/walletpbft/getblockbynum", api.getPbftBlockByNum)
	mux.HandleFunc("/walletpbft/gettransactioninfobyblocknum", api.getPbftTxInfoByBlockNum)

	mux.HandleFunc("/walletpbft/getblockbyid", api.getPbftBlockByID)
	mux.HandleFunc("/walletpbft/getblockbylimitnext", api.getPbftBlockByLimitNext)
	mux.HandleFunc("/walletpbft/getaccount", api.getPbftAccount)
	mux.HandleFunc("/walletpbft/getaccountresource", api.getPbftAccountResource)
	mux.HandleFunc("/walletpbft/getcontract", api.getPbftContract)
	mux.HandleFunc("/walletpbft/getreward", api.getPbftReward)
	mux.HandleFunc("/walletpbft/getbrokerage", api.getPbftBrokerage)
	mux.HandleFunc("/walletpbft/getBrokerage", api.getPbftBrokerage)
	mux.HandleFunc("/walletpbft/getaccountbyid", api.getPbftAccountById)
	mux.HandleFunc("/walletpbft/getaccountnet", api.getPbftAccountNet)
	mux.HandleFunc("/walletpbft/listwitnesses", api.getPbftWitnesses)
	mux.HandleFunc("/walletpbft/getchainparameters", api.getPbftChainParameters)
	mux.HandleFunc("/walletpbft/getnextmaintenancetime", api.getPbftNextMaintenanceTime)
	mux.HandleFunc("/walletpbft/listproposals", api.getPbftProposals)
	mux.HandleFunc("/walletpbft/getproposalbyid", api.getPbftProposalByID)
	mux.HandleFunc("/walletpbft/getpaginatedproposallist", api.getPbftPaginatedProposalList)
	mux.HandleFunc("/walletpbft/gettransactionbyid", api.getPbftTransactionByID)
	mux.HandleFunc("/walletpbft/gettransactioninfobyid", api.getPbftTransactionInfoByID)
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
	mux.HandleFunc("/walletpbft/candelegateresource", api.getPbftCanDelegateResource)
	mux.HandleFunc("/walletpbft/getcanwithdrawunfreezeamount", api.getPbftCanWithdrawUnfreezeAmount)
	mux.HandleFunc("/walletpbft/getavailableunfreezecount", api.getPbftAvailableUnfreezeCount)
	mux.HandleFunc("/walletpbft/estimateenergy", api.estimatePbftEnergy)
	mux.HandleFunc("/walletpbft/triggerconstantcontract", api.triggerPbftConstantContract)
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

func (api *API) getSolidContract(w http.ResponseWriter, r *http.Request) {
	api.handleGetContract(w, r, api.solidBoundNum)
}

func (api *API) getPbftContract(w http.ResponseWriter, r *http.Request) {
	api.handleGetContract(w, r, api.pbftBoundNum)
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

func (api *API) getSolidProposals(w http.ResponseWriter, r *http.Request) {
	api.handleListProposals(w, r, api.solidBoundNum)
}

func (api *API) getPbftProposals(w http.ResponseWriter, r *http.Request) {
	api.handleListProposals(w, r, api.pbftBoundNum)
}

func (api *API) getSolidProposalByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetProposalById(w, r, api.solidBoundNum)
}

func (api *API) getPbftProposalByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetProposalById(w, r, api.pbftBoundNum)
}

func (api *API) getSolidPaginatedProposalList(w http.ResponseWriter, r *http.Request) {
	api.handleGetPaginatedProposalList(w, r, api.solidBoundNum)
}

func (api *API) getPbftPaginatedProposalList(w http.ResponseWriter, r *http.Request) {
	api.handleGetPaginatedProposalList(w, r, api.pbftBoundNum)
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

func (api *API) getSolidCanDelegateResource(w http.ResponseWriter, r *http.Request) {
	api.handleCanDelegateResource(w, r, api.solidBoundNum)
}

func (api *API) getPbftCanDelegateResource(w http.ResponseWriter, r *http.Request) {
	api.handleCanDelegateResource(w, r, api.pbftBoundNum)
}

func (api *API) getSolidCanWithdrawUnfreezeAmount(w http.ResponseWriter, r *http.Request) {
	api.handleGetCanWithdrawUnfreezeAmount(w, r, api.solidBoundNum)
}

func (api *API) getPbftCanWithdrawUnfreezeAmount(w http.ResponseWriter, r *http.Request) {
	api.handleGetCanWithdrawUnfreezeAmount(w, r, api.pbftBoundNum)
}

func (api *API) getSolidAvailableUnfreezeCount(w http.ResponseWriter, r *http.Request) {
	api.handleGetAvailableUnfreezeCount(w, r, api.solidBoundNum)
}

func (api *API) getPbftAvailableUnfreezeCount(w http.ResponseWriter, r *http.Request) {
	api.handleGetAvailableUnfreezeCount(w, r, api.pbftBoundNum)
}

func (api *API) triggerSolidConstantContract(w http.ResponseWriter, r *http.Request) {
	api.handleTriggerConstantContract(w, r, api.solidBoundNum)
}

func (api *API) triggerPbftConstantContract(w http.ResponseWriter, r *http.Request) {
	api.handleTriggerConstantContract(w, r, api.pbftBoundNum)
}

func (api *API) estimateSolidEnergy(w http.ResponseWriter, r *http.Request) {
	api.handleEstimateEnergy(w, r, api.solidBoundNum)
}

func (api *API) estimatePbftEnergy(w http.ResponseWriter, r *http.Request) {
	api.handleEstimateEnergy(w, r, api.pbftBoundNum)
}

// --- Solid-block variants ---

func (api *API) getSolidNowBlock(w http.ResponseWriter, r *http.Request) {
	block, err := api.backend.GetBlockByNumber(api.solidBoundNum())
	if err != nil {
		writeEmptyJSON(w)
		return
	}
	if block == nil {
		writeEmptyJSON(w)
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
	if err != nil {
		writeEmptyJSON(w)
		return
	}
	if block == nil {
		writeEmptyJSON(w)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getSolidBlockByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetBlockByIDAtBound(w, r, api.solidBoundNum, "block not yet solidified")
}

func (api *API) getSolidBlockByLimitNext(w http.ResponseWriter, r *http.Request) {
	api.handleGetBlockByLimitNextAtBound(w, r, api.solidBoundNum, "block range exceeds solidified boundary")
}

func (api *API) getSolidTransactionByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetTransactionByIDAtBound(w, r, api.solidBoundNum)
}

func (api *API) getSolidTransactionInfoByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetTransactionInfoByIDAtBound(w, r, api.solidBoundNum)
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
	api.writeTransactionInfoByBlockNum(w, num)
}

// --- PBFT-block variants ---

func (api *API) getPbftNowBlock(w http.ResponseWriter, r *http.Request) {
	block, err := api.backend.GetBlockByNumber(api.pbftBoundNum())
	if err != nil {
		writeEmptyJSON(w)
		return
	}
	if block == nil {
		writeEmptyJSON(w)
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
	if err != nil {
		writeEmptyJSON(w)
		return
	}
	if block == nil {
		writeEmptyJSON(w)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getPbftBlockByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetBlockByIDAtBound(w, r, api.pbftBoundNum, "block not yet pbft-confirmed")
}

func (api *API) getPbftBlockByLimitNext(w http.ResponseWriter, r *http.Request) {
	api.handleGetBlockByLimitNextAtBound(w, r, api.pbftBoundNum, "block range exceeds pbft-confirmed boundary")
}

func (api *API) getPbftTransactionByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetTransactionByIDAtBound(w, r, api.pbftBoundNum)
}

func (api *API) getPbftTransactionInfoByID(w http.ResponseWriter, r *http.Request) {
	api.handleGetTransactionInfoByIDAtBound(w, r, api.pbftBoundNum)
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
	api.writeTransactionInfoByBlockNum(w, num)
}

func (api *API) handleGetBlockByIDAtBound(w http.ResponseWriter, r *http.Request, boundFn func() uint64, notReadyMessage string) {
	hash, hashBytes, ok := parseBlockIDRequest(w, r)
	if !ok {
		return
	}
	num, ok := blockNumberFromBlockIDBytes(hashBytes)
	if !ok {
		http.Error(w, "invalid block id", http.StatusBadRequest)
		return
	}
	if num > boundFn() {
		http.Error(w, notReadyMessage, http.StatusNotFound)
		return
	}
	api.writeBlockByHash(w, hash)
}

func (api *API) handleGetBlockByLimitNextAtBound(w http.ResponseWriter, r *http.Request, boundFn func() uint64, notReadyMessage string) {
	var body struct {
		StartNum int64 `json:"startNum"`
		EndNum   int64 `json:"endNum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if body.StartNum < 0 || body.EndNum < 0 {
		http.Error(w, "invalid block range", http.StatusBadRequest)
		return
	}
	if body.EndNum <= body.StartNum {
		http.Error(w, "invalid block range", http.StatusBadRequest)
		return
	}
	if uint64(body.EndNum) > boundFn()+1 {
		http.Error(w, notReadyMessage, http.StatusNotFound)
		return
	}
	api.writeBlockRange(w, uint64(body.StartNum), uint64(body.EndNum))
}

func (api *API) transactionWithinBound(w http.ResponseWriter, hash common.Hash, boundFn func() uint64) bool {
	blockNum, ok, err := api.backend.GetTransactionBlockNumByID(hash)
	if err != nil || !ok || blockNum > boundFn() {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return false
	}
	return true
}

func (api *API) handleGetTransactionByIDAtBound(w http.ResponseWriter, r *http.Request, boundFn func() uint64) {
	hash, ok := parseTransactionIDRequest(w, r)
	if !ok {
		return
	}
	if !api.transactionWithinBound(w, hash, boundFn) {
		return
	}
	api.writeTransactionByID(w, hash)
}

func (api *API) handleGetTransactionInfoByIDAtBound(w http.ResponseWriter, r *http.Request, boundFn func() uint64) {
	hash, ok := parseTransactionIDRequest(w, r)
	if !ok {
		return
	}
	if !api.transactionWithinBound(w, hash, boundFn) {
		return
	}
	api.writeTransactionInfoByID(w, hash)
}
