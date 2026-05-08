package main

import (
	"context"
	"encoding/json"
	"fmt"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// Typed wrappers around the wallet/* HTTP endpoints we need for capture.
// Each one rehydrates the JSON body into the corresponding proto via
// unmarshalTronJSON, so callers handle proto values directly.

// getNowBlock returns the current head block.
func (c *httpClient) getNowBlock(ctx context.Context) (*corepb.Block, error) {
	body, err := c.postRetry(ctx, "/wallet/getnowblock", []byte("{}"), 3)
	if err != nil {
		return nil, fmt.Errorf("getnowblock: %w", err)
	}
	var blk corepb.Block
	if err := unmarshalTronJSON(body, &blk); err != nil {
		return nil, fmt.Errorf("getnowblock parse: %w", err)
	}
	return &blk, nil
}

// getBlockByNum returns the block at exactly height h. Returns error when the
// node has pruned that height or when the response is empty.
func (c *httpClient) getBlockByNum(ctx context.Context, h uint64) (*corepb.Block, error) {
	req, _ := json.Marshal(map[string]uint64{"num": h})
	body, err := c.postRetry(ctx, "/wallet/getblockbynum", req, 3)
	if err != nil {
		return nil, fmt.Errorf("getblockbynum %d: %w", h, err)
	}
	var blk corepb.Block
	if err := unmarshalTronJSON(body, &blk); err != nil {
		return nil, fmt.Errorf("getblockbynum %d parse: %w", h, err)
	}
	if blk.BlockHeader == nil || blk.BlockHeader.RawData == nil {
		return nil, fmt.Errorf("getblockbynum %d: empty response (pruned?)", h)
	}
	if uint64(blk.BlockHeader.RawData.Number) != h {
		return nil, fmt.Errorf("getblockbynum %d: response carries height %d", h, blk.BlockHeader.RawData.Number)
	}
	return &blk, nil
}

// getAccount returns the account state for `addr` (41-hex). Returns nil
// (without error) when the account doesn't exist on chain.
func (c *httpClient) getAccount(ctx context.Context, addrHex string) (*corepb.Account, error) {
	req, _ := json.Marshal(map[string]string{"address": addrHex})
	body, err := c.postRetry(ctx, "/wallet/getaccount", req, 3)
	if err != nil {
		return nil, fmt.Errorf("getaccount %s: %w", addrHex, err)
	}
	// Empty `{}` response means address has no on-chain record.
	if len(body) <= 2 {
		return nil, nil
	}
	var a corepb.Account
	if err := unmarshalTronJSON(body, &a); err != nil {
		return nil, fmt.Errorf("getaccount %s parse: %w", addrHex, err)
	}
	if len(a.Address) == 0 {
		return nil, nil
	}
	return &a, nil
}

// getContract returns the runtime bytecode for a smart contract address.
// Returns nil (without error) for non-contract addresses.
func (c *httpClient) getContract(ctx context.Context, addrHex string) (*contractpb.SmartContract, error) {
	req, _ := json.Marshal(map[string]string{"value": addrHex})
	body, err := c.postRetry(ctx, "/wallet/getcontract", req, 3)
	if err != nil {
		return nil, fmt.Errorf("getcontract %s: %w", addrHex, err)
	}
	if len(body) <= 2 {
		return nil, nil
	}
	var sc contractpb.SmartContract
	if err := unmarshalTronJSON(body, &sc); err != nil {
		return nil, fmt.Errorf("getcontract %s parse: %w", addrHex, err)
	}
	if len(sc.Bytecode) == 0 {
		return nil, nil
	}
	return &sc, nil
}

// chainParameter is the wire shape returned by getchainparameters.
type chainParameter struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

// getChainParameters returns the dynamic-property snapshot. Keys are
// java-tron getter names (e.g. "getEnergyFee") which LoadSeed normalizes.
func (c *httpClient) getChainParameters(ctx context.Context) (map[string]int64, error) {
	body, err := c.postRetry(ctx, "/wallet/getchainparameters", []byte("{}"), 3)
	if err != nil {
		return nil, fmt.Errorf("getchainparameters: %w", err)
	}
	var resp struct {
		ChainParameter []chainParameter `json:"chainParameter"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("getchainparameters parse: %w", err)
	}
	out := make(map[string]int64, len(resp.ChainParameter))
	for _, p := range resp.ChainParameter {
		out[p.Key] = p.Value
	}
	return out, nil
}

// listWitnesses returns witness records for every SR-candidate (active +
// inactive). corepb.Witness fields populated: Address, VoteCount, Url,
// TotalProduced, TotalMissed, IsJobs, LatestBlockNum, LatestSlotNum.
func (c *httpClient) listWitnesses(ctx context.Context) ([]*corepb.Witness, error) {
	body, err := c.postRetry(ctx, "/wallet/listwitnesses", []byte("{}"), 3)
	if err != nil {
		return nil, fmt.Errorf("listwitnesses: %w", err)
	}
	// Wrapper: {"witnesses": [{...}, ...]}.
	var raw struct {
		Witnesses []json.RawMessage `json:"witnesses"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("listwitnesses parse: %w", err)
	}
	out := make([]*corepb.Witness, 0, len(raw.Witnesses))
	for i, item := range raw.Witnesses {
		var w corepb.Witness
		if err := unmarshalTronJSON(item, &w); err != nil {
			return nil, fmt.Errorf("witness[%d]: %w", i, err)
		}
		out = append(out, &w)
	}
	return out, nil
}

