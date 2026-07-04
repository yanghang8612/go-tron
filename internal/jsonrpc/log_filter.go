package jsonrpc

import (
	"encoding/json"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
)

// logFilterArgs is the eth log filter object. Address and Topics are kept raw
// because each is polymorphic: address is string|[]string and topics is an
// array of null|string|[]string.
type logFilterArgs struct {
	FromBlock string          `json:"fromBlock"`
	ToBlock   string          `json:"toBlock"`
	BlockHash string          `json:"blockHash"`
	Address   json.RawMessage `json:"address"`
	Topics    json.RawMessage `json:"topics"`
}

// parseLogFilterJSON parses an eth filter object into the shared LogFilter
// shape used by eth_getLogs, eth_newFilter, and log subscriptions. When
// resolveLatest is nil, "latest"/"pending" tags remain as the parseBlockParam
// sentinel; this preserves subscription filter behavior, which does not bind
// a backend head at subscription creation time.
func parseLogFilterJSON(raw json.RawMessage, resolveLatest func() uint64) (LogFilter, error) {
	var obj logFilterArgs
	if err := json.Unmarshal(raw, &obj); err != nil {
		return LogFilter{}, fmt.Errorf("invalid filter: %w", err)
	}
	return logFilterFromArgs(obj, resolveLatest)
}

func logFilterFromArgs(obj logFilterArgs, resolveLatest func() uint64) (LogFilter, error) {
	filter := LogFilter{}
	if obj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(obj.BlockHash))
		filter.BlockHash = &h
	} else {
		if obj.FromBlock != "" {
			n, err := parseBlockParam(obj.FromBlock)
			if err != nil {
				return filter, fmt.Errorf("invalid fromBlock: %w", err)
			}
			if n == ^uint64(0) && resolveLatest != nil {
				n = resolveLatest()
			}
			filter.FromBlock = &n
		}
		if obj.ToBlock != "" {
			n, err := parseBlockParam(obj.ToBlock)
			if err != nil {
				return filter, fmt.Errorf("invalid toBlock: %w", err)
			}
			if n == ^uint64(0) && resolveLatest != nil {
				n = resolveLatest()
			}
			filter.ToBlock = &n
		}
	}

	if len(obj.Address) > 0 && string(obj.Address) != "null" {
		var addrStr string
		var addrSlice []string
		if json.Unmarshal(obj.Address, &addrStr) == nil {
			filter.Addresses = []common.Address{common.BytesToAddress(common.FromHex(addrStr))}
		} else if json.Unmarshal(obj.Address, &addrSlice) == nil {
			for _, a := range addrSlice {
				filter.Addresses = append(filter.Addresses, common.BytesToAddress(common.FromHex(a)))
			}
		}
	}

	if len(obj.Topics) > 0 && string(obj.Topics) != "null" {
		var rawTopics []json.RawMessage
		if err := json.Unmarshal(obj.Topics, &rawTopics); err == nil {
			filter.Topics = make([][]common.Hash, len(rawTopics))
			for i, rt := range rawTopics {
				if string(rt) == "null" {
					continue
				}
				var single string
				var multi []string
				if json.Unmarshal(rt, &single) == nil {
					var h common.Hash
					copy(h[:], common.FromHex(single))
					filter.Topics[i] = []common.Hash{h}
				} else if json.Unmarshal(rt, &multi) == nil {
					for _, s := range multi {
						var h common.Hash
						copy(h[:], common.FromHex(s))
						filter.Topics[i] = append(filter.Topics[i], h)
					}
				}
			}
		}
	}

	return filter, nil
}

// parseLogFilterObject parses a websocket eth_subscribe log filter. It is kept
// as a small wrapper for the subscription code path.
func parseLogFilterObject(raw json.RawMessage) (LogFilter, error) {
	return parseLogFilterJSON(raw, nil)
}
