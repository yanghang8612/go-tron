package params

// NileNetworkID is the HelloMessage.networkId value for Nile testnet.
// Source: java-tron `nile/Nile` branch `framework/src/main/resources/config-nile.conf`
//     p2p { version = 201910292 }
const NileNetworkID int32 = 201910292

// ShastaNetworkID is the HelloMessage.networkId value for Shasta testnet.
const ShastaNetworkID int32 = 1

// NileBootstrapNodes is the list of TRON Nile testnet discovery seed nodes.
// Source: java-tron `nile/Nile` branch `config-nile.conf::seed.node.ip.list`.
var NileBootstrapNodes = []string{
	"44.236.192.97:18888",
	"44.236.125.107:18888",
	"44.232.119.174:18888",
	"52.39.105.180:18888",
	"54.70.52.47:18888",
}

// NileParentHash is the genesis block's parent_hash as defined in
// java-tron's `nile/Nile` branch `config-nile.conf::genesis.block.parentHash`.
// Without this the computed Nile genesis blockID diverges from the live
// `0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc` and
// Nile java-tron seeds drop the connection at TRON Hello.
var NileParentHash = mustHashFromHex("e58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f")

func DefaultNileGenesis() *Genesis {
	return &Genesis{
		Config:     NileChainConfig,
		Timestamp:  0,
		ParentHash: NileParentHash,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("417e95e45f5a60cc45f2d0afe37ee9f77fb8ce9fff"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("4184292b9ee2e685591a926b82f2ed4dbcac06e3c1"), Balance: 99_000_000_000_000_000, AccountName: "Sun"},
			{Address: hexToAddress("412576ed42ef309f840211bae07c859ef1f2c2dabd"), Balance: -9223372036854775808, AccountName: "Blackhole"},
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

// nileWitnesses returns the 27 GR witness records the Nile testnet genesis
// block was deployed with. Source: java-tron `nile/Nile` branch
// `framework/src/main/resources/config-nile.conf::genesis.block.witnesses`.
func nileWitnesses() []GenesisWitness {
	return []GenesisWitness{
		{Address: hexToAddress("41217179d498883cdbda5699402905d1feb258796c"), VoteCount: 100000026, URL: "http://GR1.com"},
		{Address: hexToAddress("411e9c28e5a531e5a14c55530d1db2b62ef7ceeda4"), VoteCount: 100000025, URL: "http://GR2.com"},
		{Address: hexToAddress("41c23b971d9f273be3e158b083eb3172b982b88604"), VoteCount: 100000024, URL: "http://GR3.com"},
		{Address: hexToAddress("413c85a420eed66135a378eb7e5f9246f6fb896b07"), VoteCount: 100000023, URL: "http://GR4.com"},
		{Address: hexToAddress("41f24cbabb5cf3e639e1869a5cc6be49614f8e6a14"), VoteCount: 100000022, URL: "http://GR5.com"},
		{Address: hexToAddress("41e084c0bacb0e50ace507398e3b79ea0349099e74"), VoteCount: 100000021, URL: "http://GR6.com"},
		{Address: hexToAddress("416d56e4c300a38614bff371518d0941935db78924"), VoteCount: 100000020, URL: "http://GR7.com"},
		{Address: hexToAddress("412ace82d8505e63df01f788f7ac825f9a164f9c60"), VoteCount: 100000019, URL: "http://GR8.com"},
		{Address: hexToAddress("4139b88be263c171b98562e7348f0ec03844135398"), VoteCount: 100000018, URL: "http://GR9.com"},
		{Address: hexToAddress("41210410322f83f78648f8632120dfc2807bee43bd"), VoteCount: 100000017, URL: "http://GR10.com"},
		{Address: hexToAddress("4116921563a787b4ab3d86bf12b28d3e9ce5da9638"), VoteCount: 100000016, URL: "http://GR11.com"},
		{Address: hexToAddress("41579787d38cc0b8bb8430d33b78d981cdcfaffd23"), VoteCount: 100000015, URL: "http://GR12.com"},
		{Address: hexToAddress("41e257a0591dd22aa5ba5dbe88cc4a1d29a61273e4"), VoteCount: 100000014, URL: "http://GR13.com"},
		{Address: hexToAddress("41dbfa3f36fe0581b7f0049e114fe5762309426ce9"), VoteCount: 100000013, URL: "http://GR14.com"},
		{Address: hexToAddress("41dabf85a8c272ef14248258a705c9afc7cea3a10d"), VoteCount: 100000012, URL: "http://GR15.com"},
		{Address: hexToAddress("41d4a4fc2743ef08c88eb5aece7cc4ad1a37449290"), VoteCount: 100000011, URL: "http://GR16.com"},
		{Address: hexToAddress("4139822aa747103d981878e5ee8494c3f12cd8e916"), VoteCount: 100000010, URL: "http://GR17.com"},
		{Address: hexToAddress("41da04cfbd58dde93d5640691df336c57bdc44ec75"), VoteCount: 100000009, URL: "http://GR18.com"},
		{Address: hexToAddress("41b98455e10279744c548ec2a8bfe0aa150213a416"), VoteCount: 100000008, URL: "http://GR19.com"},
		{Address: hexToAddress("41c55038ac8cf0a48f714d4c0146bb6192eee5ee5b"), VoteCount: 100000007, URL: "http://GR20.com"},
		{Address: hexToAddress("41c6a08c037a102ba3103014ce38d95c4b99f14cf1"), VoteCount: 100000006, URL: "http://GR21.com"},
		{Address: hexToAddress("41aecbeb5ef02eda6dd74fc3a1a4880a8bb032a1b8"), VoteCount: 100000005, URL: "http://GR22.com"},
		{Address: hexToAddress("41f57bbf6b0c6530eea1f3c5718ebb0c4cdbde2c79"), VoteCount: 100000004, URL: "http://GR23.com"},
		{Address: hexToAddress("41a3f6f188f222016740fcd4e733569d252b159af0"), VoteCount: 100000003, URL: "http://GR24.com"},
		{Address: hexToAddress("41f40cc0264e9655af1f361c35cee7c954afef5841"), VoteCount: 100000002, URL: "http://GR25.com"},
		{Address: hexToAddress("41b0a1489d2688c5cf31efb3d083a8f52809b0f8f9"), VoteCount: 100000001, URL: "http://GR26.com"},
		{Address: hexToAddress("416bb84a7fc361486ded6c7ddaa2ffd799c0e4743e"), VoteCount: 100000000, URL: "http://GR27.com"},
	}
}
