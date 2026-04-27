package tronapi

import (
	"encoding/json"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// createAssetIssue accepts the AssetIssueContract fields as hex-encoded bytes.
func (api *API) createAssetIssue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress          string `json:"owner_address"`
		Name                  string `json:"name"`
		Abbr                  string `json:"abbr"`
		TotalSupply           int64  `json:"total_supply"`
		FrozenSupply          []struct {
			FrozenAmount int64 `json:"frozen_amount"`
			FrozenDays   int64 `json:"frozen_days"`
		} `json:"frozen_supply"`
		TrxNum                int32  `json:"trx_num"`
		Precision             int32  `json:"precision"`
		Num                   int32  `json:"num"`
		StartTime             int64  `json:"start_time"`
		EndTime               int64  `json:"end_time"`
		VoteScore             int32  `json:"vote_score"`
		Description           string `json:"description"`
		URL                   string `json:"url"`
		FreeAssetNetLimit     int64  `json:"free_asset_net_limit"`
		PublicFreeAssetNetLimit int64 `json:"public_free_asset_net_limit"`
		ID                    string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	frozen := make([]*contractpb.AssetIssueContract_FrozenSupply, 0, len(body.FrozenSupply))
	for _, fs := range body.FrozenSupply {
		frozen = append(frozen, &contractpb.AssetIssueContract_FrozenSupply{
			FrozenAmount: fs.FrozenAmount,
			FrozenDays:   fs.FrozenDays,
		})
	}
	c := &contractpb.AssetIssueContract{
		OwnerAddress:            common.FromHex(body.OwnerAddress),
		Name:                    common.FromHex(body.Name),
		Abbr:                    common.FromHex(body.Abbr),
		TotalSupply:             body.TotalSupply,
		FrozenSupply:            frozen,
		TrxNum:                  body.TrxNum,
		Precision:               body.Precision,
		Num:                     body.Num,
		StartTime:               body.StartTime,
		EndTime:                 body.EndTime,
		VoteScore:               body.VoteScore,
		Description:             common.FromHex(body.Description),
		Url:                     common.FromHex(body.URL),
		FreeAssetNetLimit:       body.FreeAssetNetLimit,
		PublicFreeAssetNetLimit: body.PublicFreeAssetNetLimit,
		Id:                      body.ID,
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_AssetIssueContract, c, 0)
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
	var body struct {
		OwnerAddress   string `json:"owner_address"`
		Description    string `json:"description"`
		URL            string `json:"url"`
		NewLimit       int64  `json:"new_limit"`
		NewPublicLimit int64  `json:"new_public_limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.UpdateAssetContract{
		OwnerAddress:   common.FromHex(body.OwnerAddress),
		Description:    common.FromHex(body.Description),
		Url:            common.FromHex(body.URL),
		NewLimit:       body.NewLimit,
		NewPublicLimit: body.NewPublicLimit,
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_UpdateAssetContract, c, 0)
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

func (api *API) updateSetting(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress              string `json:"owner_address"`
		ContractAddress          string `json:"contract_address"`
		ConsumeUserResourcePercent int64 `json:"consume_user_resource_percent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:              common.FromHex(body.OwnerAddress),
		ContractAddress:          common.FromHex(body.ContractAddress),
		ConsumeUserResourcePercent: body.ConsumeUserResourcePercent,
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) updateEnergyLimit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress     string `json:"owner_address"`
		ContractAddress string `json:"contract_address"`
		OriginEnergyLimit int64 `json:"origin_energy_limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:     common.FromHex(body.OwnerAddress),
		ContractAddress: common.FromHex(body.ContractAddress),
		OriginEnergyLimit: body.OriginEnergyLimit,
	}
	tx, err := api.backend.BuildContractTransaction(
		corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}
