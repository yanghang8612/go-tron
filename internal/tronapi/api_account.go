package tronapi

import (
	"encoding/json"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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

// permBodyKey is a helper struct for JSON decoding a Permission.
type permBodyKey struct {
	Address string `json:"address"`
	Weight  int64  `json:"weight"`
}

type permBody struct {
	Type           int32         `json:"type"`
	Id             int32         `json:"id"`
	PermissionName string        `json:"permission_name"`
	Threshold      int64         `json:"threshold"`
	Operations     string        `json:"operations"`
	Keys           []permBodyKey `json:"keys"`
}

func buildPermission(p *permBody) *corepb.Permission {
	if p == nil {
		return nil
	}
	keys := make([]*corepb.Key, 0, len(p.Keys))
	for _, k := range p.Keys {
		keys = append(keys, &corepb.Key{
			Address: common.FromHex(k.Address),
			Weight:  k.Weight,
		})
	}
	return &corepb.Permission{
		Type:           corepb.Permission_PermissionType(p.Type),
		Id:             p.Id,
		PermissionName: p.PermissionName,
		Threshold:      p.Threshold,
		Operations:     common.FromHex(p.Operations),
		Keys:           keys,
	}
}

func (api *API) accountPermissionUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string     `json:"owner_address"`
		Owner        *permBody  `json:"owner"`
		Witness      *permBody  `json:"witness"`
		Actives      []permBody `json:"actives"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	actives := make([]*corepb.Permission, 0, len(body.Actives))
	for i := range body.Actives {
		actives = append(actives, buildPermission(&body.Actives[i]))
	}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: common.FromHex(body.OwnerAddress),
		Owner:        buildPermission(body.Owner),
		Witness:      buildPermission(body.Witness),
		Actives:      actives,
	}
	tx, err := api.backend.BuildAccountPermissionUpdateTransaction(c)
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
