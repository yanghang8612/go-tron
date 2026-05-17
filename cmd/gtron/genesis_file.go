package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

// genesisFile is the JSON schema for `--genesis <file>`. The shape is
// documented in docs/superpowers/specs/2026-05-02-genesis-file-loader-design.md.
type genesisFile struct {
	ChainID                int64                `json:"chain_id"`
	P2PVersion             int32                `json:"p2p_version"`
	BlockNumForEnergyLimit *int64               `json:"block_num_for_energy_limit"`
	TimestampMs            int64                `json:"timestamp_ms"`
	ParentHash             string               `json:"parent_hash"`
	Accounts               []genesisFileAccount `json:"accounts"`
	Witnesses              []genesisFileWitness `json:"witnesses"`
	DynamicProperties      map[string]int64     `json:"dynamic_properties"`
}

type genesisFileAccount struct {
	Address     string `json:"address"`
	Balance     string `json:"balance"`
	Name        string `json:"name"`
	AccountType int32  `json:"account_type"`
}

type genesisFileWitness struct {
	Address   string `json:"address"`
	VoteCount int64  `json:"vote_count"`
	URL       string `json:"url"`
}

// loadGenesisFile reads a JSON genesis file and returns a `*params.Genesis`
// suitable for `core.SetupGenesisBlock`. Addresses may be hex (`41…`) or
// Base58Check (`T…`). Balances are strings to fit `int64.Min` cleanly.
func loadGenesisFile(path string) (*params.Genesis, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var f genesisFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	parentHash, err := decodeHashHex(f.ParentHash)
	if err != nil {
		return nil, fmt.Errorf("parent_hash: %w", err)
	}

	accounts := make([]params.GenesisAccount, 0, len(f.Accounts))
	for i, a := range f.Accounts {
		addr, err := decodeAddress(a.Address)
		if err != nil {
			return nil, fmt.Errorf("accounts[%d].address: %w", i, err)
		}
		bal, err := strconv.ParseInt(a.Balance, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("accounts[%d].balance: %w", i, err)
		}
		accounts = append(accounts, params.GenesisAccount{
			Address:     addr,
			Balance:     bal,
			AccountName: a.Name,
			AccountType: a.AccountType,
		})
	}

	witnesses := make([]params.GenesisWitness, 0, len(f.Witnesses))
	for i, w := range f.Witnesses {
		addr, err := decodeAddress(w.Address)
		if err != nil {
			return nil, fmt.Errorf("witnesses[%d].address: %w", i, err)
		}
		witnesses = append(witnesses, params.GenesisWitness{
			Address:   addr,
			VoteCount: w.VoteCount,
			URL:       w.URL,
		})
	}

	dp := f.DynamicProperties
	if dp == nil {
		dp = map[string]int64{}
	}
	if _, ok := dp["maintenance_time_interval"]; !ok {
		dp["maintenance_time_interval"] = 21600000
	}
	// java-tron's `Manager.initGenesis` schedules the first maintenance at
	// `genesis_timestamp + maintenance_time_interval` (see java-tron
	// `DynamicPropertiesStore.saveNextMaintenanceTime`). Without this,
	// applyBlock's `NextMaintenanceTime() > 0` gate stays false forever and
	// maintenance / reward-cycle rollover never runs.
	if _, ok := dp["next_maintenance_time"]; !ok {
		dp["next_maintenance_time"] = f.TimestampMs + dp["maintenance_time_interval"]
	}
	blockNumForEnergyLimit := params.DefaultBlockNumForEnergyLimit
	if f.BlockNumForEnergyLimit != nil {
		blockNumForEnergyLimit = *f.BlockNumForEnergyLimit
	}

	return &params.Genesis{
		Config: &params.ChainConfig{
			ChainID:                f.ChainID,
			P2PVersion:             f.P2PVersion,
			BlockNumForEnergyLimit: &blockNumForEnergyLimit,
		},
		Timestamp:         f.TimestampMs,
		ParentHash:        parentHash,
		Accounts:          accounts,
		Witnesses:         witnesses,
		DynamicProperties: dp,
	}, nil
}

func decodeAddress(s string) (tcommon.Address, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "T") {
		return crypto.Base58ToAddress(s)
	}
	s = strings.TrimPrefix(s, "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return tcommon.Address{}, fmt.Errorf("hex: %w", err)
	}
	if len(b) != tcommon.AddressLength {
		return tcommon.Address{}, fmt.Errorf("expected %d bytes, got %d", tcommon.AddressLength, len(b))
	}
	return tcommon.BytesToAddress(b), nil
}

func decodeHashHex(s string) (tcommon.Hash, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return tcommon.Hash{}, fmt.Errorf("hex: %w", err)
	}
	if len(b) != 32 {
		return tcommon.Hash{}, fmt.Errorf("expected 32 bytes, got %d", len(b))
	}
	return tcommon.BytesToHash(b), nil
}
