package jsonrpc

import (
	"encoding/json"
	"fmt"
	"strings"

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
	return logFilterFromArgs(obj, resolveLatest, nil)
}

func logFilterFromArgs(obj logFilterArgs, resolveLatest, resolveFinalized func() uint64) (LogFilter, error) {
	if resolveFinalized == nil &&
		(strings.EqualFold(obj.FromBlock, "finalized") || strings.EqualFold(obj.ToBlock, "finalized")) {
		return LogFilter{}, &invalidParamsError{message: "invalid block range"}
	}
	parseRangeBound := func(tag string) (uint64, error) {
		if strings.EqualFold(tag, "finalized") {
			return resolveFinalized(), nil
		}
		height, err := parseBlockParam(tag)
		if height == ^uint64(0) && resolveLatest != nil {
			height = resolveLatest()
		}
		return height, err
	}

	filter := LogFilter{}
	if obj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(obj.BlockHash))
		filter.BlockHash = &h
	} else {
		if obj.FromBlock != "" {
			n, err := parseRangeBound(obj.FromBlock)
			if err != nil {
				return filter, fmt.Errorf("invalid fromBlock: %w", err)
			}
			filter.FromBlock = &n
		}
		if obj.ToBlock != "" {
			n, err := parseRangeBound(obj.ToBlock)
			if err != nil {
				return filter, fmt.Errorf("invalid toBlock: %w", err)
			}
			filter.ToBlock = &n
		}
	}

	if len(obj.Address) > 0 && string(obj.Address) != "null" {
		addresses, err := parseFilterAddresses(obj.Address)
		if err != nil {
			return filter, err
		}
		filter.Addresses = addresses
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
					const maxSubTopics = 1000
					if len(multi) > maxSubTopics {
						return filter, &invalidParamsError{message: fmt.Sprintf("exceed max topics: %d", maxSubTopics)}
					}
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

// MaxLogBlockRange is the default block-number difference for log scans.
const MaxLogBlockRange = 5000

func rpcLogFilter(obj logFilterArgs, head uint64, resolveFinalized func() uint64) (LogFilter, error) {
	if obj.BlockHash != "" && (obj.FromBlock != "" || obj.ToBlock != "") {
		return LogFilter{}, &invalidParamsError{message: "cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other"}
	}
	if strings.EqualFold(obj.FromBlock, "latest") {
		obj.FromBlock = "latest"
	}
	if strings.EqualFold(obj.ToBlock, "latest") {
		// An open end follows the head when a stored filter is queried.
		obj.ToBlock = ""
	}
	filter, err := logFilterFromArgs(obj, func() uint64 { return head }, resolveFinalized)
	if err != nil || filter.BlockHash != nil {
		return filter, err
	}
	if filter.FromBlock == nil {
		from := head
		if filter.ToBlock != nil {
			from = min(from, *filter.ToBlock)
		}
		filter.FromBlock = &from
	}
	if filter.ToBlock != nil && *filter.FromBlock > *filter.ToBlock {
		return LogFilter{}, &invalidParamsError{message: "please verify: fromBlock <= toBlock"}
	}
	return filter, nil
}

func validateLogBlockRange(filter LogFilter, head uint64) error {
	if filter.BlockHash != nil || filter.FromBlock == nil {
		return nil
	}
	to := head
	if filter.ToBlock != nil {
		to = min(to, *filter.ToBlock)
	}
	if to > *filter.FromBlock && to-*filter.FromBlock > MaxLogBlockRange {
		return &invalidParamsError{message: fmt.Sprintf("exceed max block range: %d", MaxLogBlockRange)}
	}
	return nil
}

// parseLogFilterObject parses a websocket eth_subscribe log filter. It is kept
// as a small wrapper for the subscription code path.
func parseLogFilterObject(raw json.RawMessage) (LogFilter, error) {
	return parseLogFilterJSON(raw, nil)
}
