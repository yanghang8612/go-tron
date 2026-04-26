package tronapi

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

// createAssetIssue accepts the full AssetIssueContract proto JSON.
func (api *API) createAssetIssue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	var c contractpb.AssetIssueContract
	if err := protojson.Unmarshal(body, &c); err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_AssetIssueContract, &c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) updateAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	var c contractpb.UpdateAssetContract
	if err := protojson.Unmarshal(body, &c); err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_UpdateAssetContract, &c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getAssetIssueListByName(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("value")
	if name == "" {
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			name = body.Value
		}
	}
	asset := api.backend.GetAssetIssueByName(common.FromHex(name))
	var items []*contractpb.AssetIssueContract
	if asset != nil {
		items = []*contractpb.AssetIssueContract{asset}
	}
	writeAssetIssueList(w, items)
}

func (api *API) clearABI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string `json:"owner_address"`
		ContractAddress string `json:"contract_address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))
	c := &contractpb.ClearABIContract{
		OwnerAddress:    owner[:],
		ContractAddress: contract[:],
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_ClearABIContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func writeAssetIssueList(w http.ResponseWriter, items []*contractpb.AssetIssueContract) {
	type response struct {
		AssetIssue []*contractpb.AssetIssueContract `json:"assetIssue"`
	}
	if items == nil {
		items = []*contractpb.AssetIssueContract{}
	}
	data, _ := json.Marshal(response{AssetIssue: items})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
