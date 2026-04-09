package params

import "github.com/tronprotocol/go-tron/common"

type GenesisAccount struct {
	Address common.Address
	Balance int64
}

type GenesisWitness struct {
	Address   common.Address
	VoteCount int64
	URL       string
}

type Genesis struct {
	Timestamp  int64
	ParentHash common.Hash
	Accounts   []GenesisAccount
	Witnesses  []GenesisWitness
}
