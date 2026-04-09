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
}

var MainnetChainConfig = &ChainConfig{
	ChainID:     1,
	P2PVersion:  11111,
	P2PPort:     18888,
	HTTPPort:    8090,
	GRPCPort:    50051,
	JSONRPCPort: 8545,
}

var NileChainConfig = &ChainConfig{
	ChainID:     3448148188,
	P2PVersion:  201910292,
	P2PPort:     18888,
	HTTPPort:    8090,
	GRPCPort:    50051,
	JSONRPCPort: 8545,
}
