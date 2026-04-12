package params

// NileNetworkID is the HelloMessage.networkId value for Nile testnet.
// Source: java-tron config.conf comment "Mainnet:11111; Nile:201910292; Shasta:1".
const NileNetworkID int32 = 201910292

// ShastaNetworkID is the HelloMessage.networkId value for Shasta testnet.
const ShastaNetworkID int32 = 1

// NileBootstrapNodes is the list of TRON Nile testnet discovery seed nodes.
var NileBootstrapNodes = []string{
	"47.252.19.181:18888",
	"47.252.3.238:18888",
}

// DefaultNileGenesis returns the genesis configuration for the Nile testnet.
func DefaultNileGenesis() *Genesis {
	return &Genesis{
		Config:    NileChainConfig,
		Timestamp: 0,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("41928c9af0651632157ef27a2cf17ca72c575a4d21"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("41a614f803b6fd780986a42c78ec9c7f77e6ded13c"), Balance: 0, AccountName: "Sun"},
			{Address: hexToAddress("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8"), Balance: -9223372036854775808, AccountName: "Blackhole"},
		},
		Witnesses: nileWitnesses(),
		DynamicProperties: map[string]int64{
			"maintenance_time_interval":                 21600000,
			"account_upgrade_cost":                      9999000000,
			"create_account_fee":                        100000,
			"transaction_fee":                           10,
			"asset_issue_fee":                           1024000000,
			"witness_pay_per_block":                     16000000,
			"witness_standby_allowance":                 115200000000,
			"create_new_account_fee_in_system_contract": 0,
			"create_new_account_bandwidth_rate":         1,
			"energy_fee":                                100,
			"max_cpu_time_of_one_tx":                    80,
			"total_energy_current_limit":                50000000000,
			"total_net_limit":                           43200000000,
			"unfreeze_delay_days":                       14,
		},
	}
}

func nileWitnesses() []GenesisWitness {
	return []GenesisWitness{
		{Address: hexToAddress("41f16412b9a17ee9408646e2a21e16478f72ed1e95"), VoteCount: 100000, URL: "http://Nile-SR1.com"},
	}
}
