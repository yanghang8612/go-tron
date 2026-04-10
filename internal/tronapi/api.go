package tronapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type API struct {
	backend Backend
}

func NewAPI(backend Backend) *API {
	return &API{backend: backend}
}

func (api *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/wallet/getnowblock", api.getNowBlock)
	mux.HandleFunc("/wallet/getblockbynum", api.getBlockByNum)
	mux.HandleFunc("/wallet/getaccount", api.getAccount)
	mux.HandleFunc("/wallet/broadcasttransaction", api.broadcastTransaction)
	mux.HandleFunc("/wallet/getnodeinfo", api.getNodeInfo)
	mux.HandleFunc("/wallet/gettransactioncountinpool", api.getTxPoolCount)
}

func (api *API) getNowBlock(w http.ResponseWriter, r *http.Request) {
	block := api.backend.CurrentBlock()
	if block == nil {
		http.Error(w, "no current block", http.StatusInternalServerError)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getBlockByNum(w http.ResponseWriter, r *http.Request) {
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
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getAccount(w http.ResponseWriter, r *http.Request) {
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
	acc, err := api.backend.GetAccount(addr)
	if err != nil || acc == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, acc.Proto())
}

func (api *API) broadcastTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	var pbTx corepb.Transaction
	if err := protojson.Unmarshal(body, &pbTx); err != nil {
		http.Error(w, "invalid transaction JSON", http.StatusBadRequest)
		return
	}

	// Compute txID from raw_data
	var txID string
	if pbTx.RawData != nil {
		rawBytes, _ := proto.Marshal(pbTx.RawData)
		h := sha256.Sum256(rawBytes)
		txID = hex.EncodeToString(h[:])
	}

	tx := types.NewTransactionFromPB(&pbTx)
	if err := api.backend.BroadcastTransaction(tx); err != nil {
		data, _ := MarshalBroadcastResult(false, txID, err.Error())
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}

	data, _ := MarshalBroadcastResult(true, txID, "")
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := api.backend.GetNodeInfo()
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getTxPoolCount(w http.ResponseWriter, r *http.Request) {
	count := api.backend.PendingTransactionCount()
	resp := map[string]int{"count": count}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func writeTronJSON(w http.ResponseWriter, msg proto.Message) {
	data, err := marshalTronJSON(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func writeBlockJSON(w http.ResponseWriter, msg proto.Message) {
	data, err := MarshalBlock(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

