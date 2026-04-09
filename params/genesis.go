package params

import "github.com/tronprotocol/go-tron/common"

// GenesisAccount defines a genesis account allocation.
type GenesisAccount struct {
	Address     common.Address
	Balance     int64
	AccountType int32  // 0=Normal, 1=AssetIssue, 2=Contract
	AccountName string
}

// GenesisWitness defines a genesis super representative.
type GenesisWitness struct {
	Address   common.Address
	VoteCount int64
	URL       string
}

// Genesis defines the initial state of the blockchain.
type Genesis struct {
	Config            *ChainConfig
	Timestamp         int64
	ParentHash        common.Hash
	Accounts          []GenesisAccount
	Witnesses         []GenesisWitness
	DynamicProperties map[string]int64
}
