package tronapi

import (
	"encoding/json"
	"net/http"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func (api *API) listExchanges(w http.ResponseWriter, r *http.Request) {
	exchanges, err := api.backend.ListExchanges()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var list []map[string]any
	for _, e := range exchanges {
		list = append(list, marshalMessage(e.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	data, _ := json.Marshal(map[string]any{"exchanges": list})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) exchangeCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress       string `json:"owner_address"`
		FirstTokenID       string `json:"first_token_id"`
		FirstTokenBalance  int64  `json:"first_token_balance"`
		SecondTokenID      string `json:"second_token_id"`
		SecondTokenBalance int64  `json:"second_token_balance"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.ExchangeCreateContract{
		OwnerAddress:       common.FromHex(body.OwnerAddress),
		FirstTokenId:       common.FromHex(body.FirstTokenID),
		FirstTokenBalance:  body.FirstTokenBalance,
		SecondTokenId:      common.FromHex(body.SecondTokenID),
		SecondTokenBalance: body.SecondTokenBalance,
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_ExchangeCreateContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) exchangeInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ExchangeID   int64  `json:"exchange_id"`
		TokenID      string `json:"token_id"`
		Quant        int64  `json:"quant"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.ExchangeInjectContract{
		OwnerAddress: common.FromHex(body.OwnerAddress),
		ExchangeId:   body.ExchangeID,
		TokenId:      common.FromHex(body.TokenID),
		Quant:        body.Quant,
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_ExchangeInjectContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) exchangeTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ExchangeID   int64  `json:"exchange_id"`
		TokenID      string `json:"token_id"`
		Quant        int64  `json:"quant"`
		Expected     int64  `json:"expected"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.ExchangeTransactionContract{
		OwnerAddress: common.FromHex(body.OwnerAddress),
		ExchangeId:   body.ExchangeID,
		TokenId:      common.FromHex(body.TokenID),
		Quant:        body.Quant,
		Expected:     body.Expected,
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_ExchangeTransactionContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) exchangeWithdraw(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ExchangeID   int64  `json:"exchange_id"`
		TokenID      string `json:"token_id"`
		Quant        int64  `json:"quant"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.ExchangeWithdrawContract{
		OwnerAddress: common.FromHex(body.OwnerAddress),
		ExchangeId:   body.ExchangeID,
		TokenId:      common.FromHex(body.TokenID),
		Quant:        body.Quant,
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_ExchangeWithdrawContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) marketSellAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress      string `json:"owner_address"`
		SellTokenID       string `json:"sell_token_id"`
		SellTokenQuantity int64  `json:"sell_token_quantity"`
		BuyTokenID        string `json:"buy_token_id"`
		BuyTokenQuantity  int64  `json:"buy_token_quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.MarketSellAssetContract{
		OwnerAddress:      common.FromHex(body.OwnerAddress),
		SellTokenId:       common.FromHex(body.SellTokenID),
		SellTokenQuantity: body.SellTokenQuantity,
		BuyTokenId:        common.FromHex(body.BuyTokenID),
		BuyTokenQuantity:  body.BuyTokenQuantity,
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_MarketSellAssetContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) marketCancelOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		OrderID      string `json:"order_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	c := &contractpb.MarketCancelOrderContract{
		OwnerAddress: common.FromHex(body.OwnerAddress),
		OrderId:      common.FromHex(body.OrderID),
	}
	tx, err := api.backend.BuildContractTransaction(corepb.Transaction_Contract_MarketCancelOrderContract, c, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}
