package params

import "github.com/tronprotocol/go-tron/common"

type ChainConfig struct {
	ChainID     int64
	P2PVersion  int32
	GenesisHash common.Hash
	P2PPort     int
	HTTPPort    int
	GRPCPort    int
	JSONRPCPort int
	// Java-tron config key: enery.limit.block.num.
	// A nil pointer means the java-tron default.
	BlockNumForEnergyLimit *int64
	// HistoryEnabled toggles the State History Index (SHI) capture path.
	// false (the default) leaves applyBlock and StateDB on the zero-overhead
	// fast path — no per-mutation accounting, no per-block flush. Archive
	// operators opt in via node config; the gate is independent of any
	// java-tron proposal, so flipping it never affects consensus.
	HistoryEnabled bool
}

const DefaultBlockNumForEnergyLimit int64 = 4_727_890

func chainConfigInt64(v int64) *int64 { return &v }

func (c *ChainConfig) EnergyLimitForkBlockNum() int64 {
	if c != nil && c.BlockNumForEnergyLimit != nil {
		return *c.BlockNumForEnergyLimit
	}
	return DefaultBlockNumForEnergyLimit
}

var MainnetChainConfig = &ChainConfig{
	ChainID:                1,
	P2PVersion:             11111,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}

var NileChainConfig = &ChainConfig{
	ChainID:                3448148188,
	P2PVersion:             201910292,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}
