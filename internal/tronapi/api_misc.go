package tronapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// getProposalById handles GET/POST /wallet/getproposalbyid
func (api *API) getProposalById(w http.ResponseWriter, r *http.Request) {
	api.handleGetProposalById(w, r, nil)
}

func (api *API) handleGetProposalById(w http.ResponseWriter, r *http.Request, boundFn blockBoundFunc) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		var body struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			idStr = strconv.FormatInt(body.ID, 10)
		}
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var p *ProposalInfo
	if boundFn == nil {
		p, err = api.backend.GetProposalByID(id)
	} else {
		blockNum, ok := resolveBound(w, boundFn)
		if !ok {
			return
		}
		p, err = api.backend.GetProposalByIDAt(id, blockNum)
	}
	if err != nil {
		if proposalLookupNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{}"))
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if p == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	data, _ := json.Marshal(p)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func proposalLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	return strings.HasPrefix(msg, "proposal ") &&
		(strings.HasSuffix(msg, " not found") || strings.Contains(msg, " not found at block "))
}

// getPaginatedProposalList handles POST /wallet/getpaginatedproposallist
func (api *API) getPaginatedProposalList(w http.ResponseWriter, r *http.Request) {
	api.handleGetPaginatedProposalList(w, r, nil)
}

func (api *API) handleGetPaginatedProposalList(w http.ResponseWriter, r *http.Request, boundFn blockBoundFunc) {
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
	var (
		proposals []*ProposalInfo
		err       error
	)
	if boundFn == nil {
		proposals, err = api.backend.ListProposalsPaginated(body.Offset, body.Limit)
	} else {
		blockNum, ok := resolveBound(w, boundFn)
		if !ok {
			return
		}
		proposals, err = api.backend.ListProposalsPaginatedAt(body.Offset, body.Limit, blockNum)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if proposals == nil {
		proposals = []*ProposalInfo{}
	}
	data, _ := json.Marshal(map[string]interface{}{"proposal": proposals})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// metricsStub handles GET /wallet/metrics — placeholder until Prometheus metrics
func (api *API) metricsStub(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// getTransactionReceiptById is an alias for getTransactionInfoByID
func (api *API) getTransactionReceiptById(w http.ResponseWriter, r *http.Request) {
	api.getTransactionInfoByID(w, r)
}

// validateAddress handles GET/POST /wallet/validateaddress
func (api *API) validateAddress(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	if addr == "" {
		var body struct {
			Address string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addr = body.Address
		}
	}
	valid, msg := api.backend.ValidateAddress(addr)
	data, _ := json.Marshal(map[string]interface{}{
		"result":  valid,
		"message": msg,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
