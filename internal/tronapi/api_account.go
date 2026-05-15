package tronapi

import (
	"encoding/json"
	"net/http"

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
		Visible        bool   `json:"visible"`
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
	account, err := parseAddress(body.AccountAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "account_address", err)
		return
	}
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
	name, err := parseBytes(body.AccountName, body.Visible)
	if err != nil {
		httpFieldErr(w, "account_name", err)
		return
	}
	tx, err := api.backend.BuildUpdateAccountTransaction(owner, name)
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
	accountID, err := parseBytes(body.AccountID, body.Visible)
	if err != nil {
		httpFieldErr(w, "account_id", err)
		return
	}
	tx, err := api.backend.BuildSetAccountIdTransaction(owner, accountID)
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

// buildPermission converts a JSON permBody into the wire-format Permission.
// All addresses in keys[] are parsed under the same `visible` flag the
// outer request used so a wallet posting visible=true with Base58Check
// addresses gets consistent handling for the embedded key list. Returns
// an error if any key address fails to parse — the audit's silent-swallow
// bug applied here too.
func buildPermission(p *permBody, visible bool) (*corepb.Permission, error) {
	if p == nil {
		return nil, nil
	}
	keys := make([]*corepb.Key, 0, len(p.Keys))
	for i, k := range p.Keys {
		addr, err := parseAddress(k.Address, visible)
		if err != nil {
			return nil, addressFieldErr("keys["+itoa(i)+"].address", err)
		}
		keys = append(keys, &corepb.Key{
			Address: addr.Bytes(),
			Weight:  k.Weight,
		})
	}
	ops, err := parseBytes(p.Operations, visible)
	if err != nil {
		return nil, addressFieldErr("operations", err)
	}
	return &corepb.Permission{
		Type:           corepb.Permission_PermissionType(p.Type),
		Id:             p.Id,
		PermissionName: p.PermissionName,
		Threshold:      p.Threshold,
		Operations:     ops,
		Keys:           keys,
	}, nil
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
		Visible      bool       `json:"visible"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	ownerAddr, err := parseAddress(body.OwnerAddress, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner_address", err)
		return
	}
	ownerPerm, err := buildPermission(body.Owner, body.Visible)
	if err != nil {
		httpFieldErr(w, "owner", err)
		return
	}
	witnessPerm, err := buildPermission(body.Witness, body.Visible)
	if err != nil {
		httpFieldErr(w, "witness", err)
		return
	}
	actives := make([]*corepb.Permission, 0, len(body.Actives))
	for i := range body.Actives {
		ap, err := buildPermission(&body.Actives[i], body.Visible)
		if err != nil {
			httpFieldErr(w, "actives["+itoa(i)+"]", err)
			return
		}
		actives = append(actives, ap)
	}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: ownerAddr.Bytes(),
		Owner:        ownerPerm,
		Witness:      witnessPerm,
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
	addrStr := r.URL.Query().Get("address")
	visible := r.URL.Query().Get("visible") == "true"
	if addrStr == "" {
		var body struct {
			Address string `json:"address"`
			Visible bool   `json:"visible"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrStr = body.Address
			if body.Visible {
				visible = true
			}
		}
	}
	if addrStr == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr, err := parseAddress(addrStr, visible)
	if err != nil {
		httpFieldErr(w, "address", err)
		return
	}
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
