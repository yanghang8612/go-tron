package tronapi

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (api *API) getPendingSize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "GET or POST required", http.StatusMethodNotAllowed)
		return
	}
	writeInt64JSON(w, "pendingSize", int64(api.backend.PendingTransactionCount()))
}

func (api *API) getTransactionCountByBlockNum(w http.ResponseWriter, r *http.Request) {
	api.handleGetTransactionCountByBlockNum(w, r, nil)
}

func (api *API) handleGetTransactionCountByBlockNum(w http.ResponseWriter, r *http.Request, boundFn blockBoundFunc) {
	var num int64
	var err error
	switch r.Method {
	case http.MethodGet:
		num, err = strconv.ParseInt(r.URL.Query().Get("num"), 10, 64)
	case http.MethodPost:
		var body struct {
			Num int64 `json:"num"`
		}
		err = json.NewDecoder(r.Body).Decode(&body)
		num = body.Num
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "GET or POST required", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, "invalid block number", http.StatusBadRequest)
		return
	}
	if num >= 0 {
		if boundFn != nil {
			bound, ok := resolveBound(w, boundFn)
			if !ok {
				return
			}
			if uint64(num) > bound {
				writeInt64JSON(w, "count", 0)
				return
			}
		}
		if block, err := api.backend.GetBlockByNumber(uint64(num)); err == nil && block != nil {
			writeInt64JSON(w, "count", int64(len(block.Transactions())))
			return
		}
	}
	writeInt64JSON(w, "count", 0)
}

func writeInt64JSON(w http.ResponseWriter, field string, value int64) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{field: responseTronJSON(w).int64(value)})
}
