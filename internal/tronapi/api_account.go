package tronapi

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

func (api *API) createAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress   string `json:"owner_address"`
		AccountAddress string `json:"account_address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	account := common.BytesToAddress(common.FromHex(body.AccountAddress))
	tx, err := api.backend.BuildCreateAccountTransaction(owner, account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) updateAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		AccountName  string `json:"account_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	tx, err := api.backend.BuildUpdateAccountTransaction(owner, common.FromHex(body.AccountName))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) setAccountId(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		AccountID    string `json:"account_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	tx, err := api.backend.BuildSetAccountIdTransaction(owner, []byte(body.AccountID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) accountPermissionUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	var c contractpb.AccountPermissionUpdateContract
	if err := protojson.Unmarshal(body, &c); err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	tx, err := api.backend.BuildAccountPermissionUpdateTransaction(&c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getAccountById(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		var body struct {
			AccountID string `json:"account_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			accountID = body.AccountID
		}
	}
	acc, err := api.backend.GetAccountById([]byte(accountID))
	if err != nil || acc == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, acc.Proto())
}

func (api *API) getAccountNet(w http.ResponseWriter, r *http.Request) {
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
	msg, err := api.backend.GetAccountNet(addr)
	if err != nil || msg == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	data, merr := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(msg)
	if merr != nil {
		http.Error(w, merr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
