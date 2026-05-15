package tronapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// ResourceField represents a resource type that can be unmarshaled from either
// a JSON number (0/1/2) or a JSON string ("BANDWIDTH"/"ENERGY"/"TRON_POWER").
type ResourceField int32

func (r *ResourceField) UnmarshalJSON(data []byte) error {
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		v, err := n.Int64()
		if err != nil {
			return fmt.Errorf("invalid resource: %w", err)
		}
		*r = ResourceField(v)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("invalid resource field")
	}
	switch s {
	case "BANDWIDTH":
		*r = 0
	case "ENERGY":
		*r = 1
	case "TRON_POWER":
		*r = 2
	default:
		return fmt.Errorf("unknown resource: %q", s)
	}
	return nil
}

// httpFieldErr writes a 400 with a "<field>: <err>" prefix so the caller
// can pinpoint which body field failed to parse. Saves repeating the same
// inline pattern across every tx-builder handler.
func httpFieldErr(w http.ResponseWriter, field string, err error) {
	http.Error(w, field+": "+err.Error(), http.StatusBadRequest)
}

// parseOptionalAddress handles fields like receiver_address that are
// allowed to be empty (delegated freeze without receiver, etc.) — empty
// returns the zero Address with no error, otherwise the parse rules apply.
func parseOptionalAddress(s string, visible bool) (common.Address, error) {
	if s == "" {
		return common.Address{}, nil
	}
	return parseAddress(s, visible)
}

func (api *API) transferAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ToAddress    string `json:"to_address"`
		AssetName    string `json:"asset_name"`
		Amount       int64  `json:"amount"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	to, err := parseAddress(body.ToAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "to_address", err)
		return
	}
	assetName, err := parseBytes(body.AssetName, body.Visible)
	if err != nil {
		httpFieldErr(w, "asset_name", err)
		return
	}
	tx, err := api.backend.BuildTransferAssetTransaction(owner, to, assetName, body.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) participateAssetIssue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ToAddress    string `json:"to_address"`
		AssetName    string `json:"asset_name"`
		Amount       int64  `json:"amount"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	to, err := parseAddress(body.ToAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "to_address", err)
		return
	}
	assetName, err := parseBytes(body.AssetName, body.Visible)
	if err != nil {
		httpFieldErr(w, "asset_name", err)
		return
	}
	tx, err := api.backend.BuildParticipateAssetIssueTransaction(owner, to, assetName, body.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) createWitness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		URL          string `json:"url"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	url, err := parseBytes(body.URL, body.Visible)
	if err != nil {
		httpFieldErr(w, "url", err)
		return
	}
	tx, err := api.backend.BuildCreateWitnessTransaction(owner, url)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) voteWitnessAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Votes        []struct {
			VoteAddress string `json:"vote_address"`
			VoteCount   int64  `json:"vote_count"`
		} `json:"votes"`
		Visible bool `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	votes := make(map[common.Address]int64, len(body.Votes))
	for i, v := range body.Votes {
		addr, err := parseAddress(v.VoteAddress, body.Visible)
		if err != nil {
			httpFieldErr(w, fmt.Sprintf("votes[%d].vote_address", i), err)
			return
		}
		votes[addr] = v.VoteCount
	}
	tx, err := api.backend.BuildVoteWitnessTransaction(owner, votes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) updateWitness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		UpdateURL    string `json:"update_url"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	url, err := parseBytes(body.UpdateURL, body.Visible)
	if err != nil {
		httpFieldErr(w, "update_url", err)
		return
	}
	tx, err := api.backend.BuildUpdateWitnessTransaction(owner, url)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) withdrawBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildWithdrawBalanceTransaction(owner)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) updateBrokerage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Brokerage    int32  `json:"brokerage"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildUpdateBrokerageTransaction(owner, body.Brokerage)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) freezeBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string        `json:"owner_address"`
		FrozenBalance   int64         `json:"frozen_balance"`
		FrozenDuration  int64         `json:"frozen_duration"`
		Resource        ResourceField `json:"resource"`
		ReceiverAddress string        `json:"receiver_address"`
		Visible         bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	receiver, err := parseOptionalAddress(body.ReceiverAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "receiver_address", err)
		return
	}
	tx, err := api.backend.BuildFreezeBalanceV1Transaction(owner, body.FrozenBalance, body.FrozenDuration,
		corepb.ResourceCode(body.Resource), receiver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) unfreezeBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string        `json:"owner_address"`
		Resource        ResourceField `json:"resource"`
		ReceiverAddress string        `json:"receiver_address"`
		Visible         bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	receiver, err := parseOptionalAddress(body.ReceiverAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "receiver_address", err)
		return
	}
	tx, err := api.backend.BuildUnfreezeBalanceV1Transaction(owner,
		corepb.ResourceCode(body.Resource), receiver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) freezeBalanceV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress  string        `json:"owner_address"`
		FrozenBalance int64         `json:"frozen_balance"`
		Resource      ResourceField `json:"resource"`
		Visible       bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildFreezeBalanceV2Transaction(owner, body.FrozenBalance,
		corepb.ResourceCode(body.Resource))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) unfreezeBalanceV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string        `json:"owner_address"`
		UnfreezeBalance int64         `json:"unfreeze_balance"`
		Resource        ResourceField `json:"resource"`
		Visible         bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildUnfreezeBalanceV2Transaction(owner, body.UnfreezeBalance,
		corepb.ResourceCode(body.Resource))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) cancelAllUnfreezeV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildCancelAllUnfreezeV2Transaction(owner)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) delegateResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string        `json:"owner_address"`
		ReceiverAddress string        `json:"receiver_address"`
		Balance         int64         `json:"balance"`
		Resource        ResourceField `json:"resource"`
		Lock            bool          `json:"lock"`
		Visible         bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	receiver, err := parseAddress(body.ReceiverAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "receiver_address", err)
		return
	}
	tx, err := api.backend.BuildDelegateResourceTransaction(owner, receiver, body.Balance,
		corepb.ResourceCode(body.Resource), body.Lock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) undelegateResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress    string        `json:"owner_address"`
		ReceiverAddress string        `json:"receiver_address"`
		Balance         int64         `json:"balance"`
		Resource        ResourceField `json:"resource"`
		Visible         bool          `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	receiver, err := parseAddress(body.ReceiverAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "receiver_address", err)
		return
	}
	tx, err := api.backend.BuildUnDelegateResourceTransaction(owner, receiver, body.Balance,
		corepb.ResourceCode(body.Resource))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) withdrawExpireUnfreeze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		Visible      bool   `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	tx, err := api.backend.BuildWithdrawExpireUnfreezeTransaction(owner)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}
