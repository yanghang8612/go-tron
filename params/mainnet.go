package params

import (
	"encoding/hex"

	"github.com/tronprotocol/go-tron/common"
)

// MainnetNetworkID is the HelloMessage.networkId value for TRON mainnet.
// Source: java-tron framework/src/main/resources/config.conf:
//     p2p { version = 11111 # Mainnet:11111; Nile:201910292; Shasta:1 }
// Mapped to libp2p's networkId in TronNetService.java (setNetworkId).
const MainnetNetworkID int32 = 11111

// MainnetBootstrapNodes is the list of TRON mainnet discovery seed nodes.
// The first 12 are java-tron's default seeds from config.conf. The trailing
// entries are mainnet peers verified by gtron's M3.5 sanity (2026-05-09) to
// complete TRON-Hello reliably; they're not in java-tron's default list but
// are observed advertising themselves through java-tron's NEIGHBOURS replies.
var MainnetBootstrapNodes = []string{
	"47.90.247.237:18888",
	"47.90.214.128:18888",
	"52.53.189.99:18888",
	"18.196.99.16:18888",
	"34.253.187.192:18888",
	"18.133.82.227:18888",
	"35.180.51.163:18888",
	"54.252.224.209:18888",
	"18.228.15.36:18888",
	"52.15.93.92:18888",
	"34.220.77.106:18888",
	"15.207.144.3:18888",
	"3.218.137.187:18888",
	"34.237.210.82:18888",
}

func hexToAddress(h string) common.Address {
	b, _ := hex.DecodeString(h)
	return common.BytesToAddress(b)
}

// MainnetParentHash is the genesis block's parent_hash, taken verbatim from
// java-tron `framework/src/main/resources/config.conf::genesis.block.parentHash`.
// Without this the computed genesis blockID diverges from mainnet's
// `00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc` and
// java-tron seeds drop the connection at TRON Hello.
var MainnetParentHash = mustHashFromHex("e58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f")

func mustHashFromHex(h string) common.Hash {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	var out common.Hash
	copy(out[:], b)
	return out
}

func DefaultMainnetGenesis() *Genesis {
	return &Genesis{
		Config:     MainnetChainConfig,
		Timestamp:  0,
		ParentHash: MainnetParentHash,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("4171b0af54e0a1182a5e0947d6a64f3b22740ef318"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("41ef1bd15b5b657f69611b053a6f4fcd7268a50858"), Balance: 0, AccountName: "Sun"},
			{Address: hexToAddress("4177944d19c052b73ee2286823aa83f8138cb7032f"), Balance: -9223372036854775808, AccountName: "Blackhole"},
		},
		Witnesses: mainnetWitnesses(),
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
			// mainnet's `proposal_expire_time = 259_200_000` (3 days, from
			// config.conf:681) is already the gtron bare default — no
			// override needed here. Nile overrides to 600_000 in nile.go.
		},
	}
}

// mainnetWitnesses returns the 27 GR witness records the mainnet genesis block
// was deployed with. Source: java-tron config.conf::genesis.block.witnesses.
func mainnetWitnesses() []GenesisWitness {
	return []GenesisWitness{
		{Address: hexToAddress("415095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2"), VoteCount: 100000026, URL: "http://GR1.com"},
		{Address: hexToAddress("41d32b3fa8ca0b4896257fdf1821ac8d116da84c45"), VoteCount: 100000025, URL: "http://GR2.com"},
		{Address: hexToAddress("41df3bd4e0463534cb7f1f3ffc2ec14ac4693dc3b2"), VoteCount: 100000024, URL: "http://GR3.com"},
		{Address: hexToAddress("4127a6419bbe59f4e64a064d710787e578a150d6a7"), VoteCount: 100000023, URL: "http://GR4.com"},
		{Address: hexToAddress("4108b55b2611ec829d308a62b3339fba9dd5c27151"), VoteCount: 100000022, URL: "http://GR5.com"},
		{Address: hexToAddress("416419765bacf1dc441f722cabc8b661140558bb5d"), VoteCount: 100000021, URL: "http://GR6.com"},
		{Address: hexToAddress("414b4778beebb48abe0bc1df42e92e0fe64d0c8685"), VoteCount: 100000020, URL: "http://GR7.com"},
		{Address: hexToAddress("411661f25387370c9cd3a9a5d97e60ca90f4844e7e"), VoteCount: 100000019, URL: "http://GR8.com"},
		{Address: hexToAddress("41e40de6895c142ade8b86194063bcdbaa6c9360b6"), VoteCount: 100000018, URL: "http://GR9.com"},
		{Address: hexToAddress("41207ab1585b9cc6c4c1232f67e4a10e19a442fe68"), VoteCount: 100000017, URL: "http://GR10.com"},
		{Address: hexToAddress("41410e468919155aa847d83b0c206148511b6dc848"), VoteCount: 100000016, URL: "http://GR11.com"},
		{Address: hexToAddress("4186f5793eb678c65d9673d5498c550439d762c1cc"), VoteCount: 100000015, URL: "http://GR12.com"},
		{Address: hexToAddress("417040583133e831953ea4f65a8196fcffcfbf0d80"), VoteCount: 100000014, URL: "http://GR13.com"},
		{Address: hexToAddress("412edce151c81d9b4aae17f974f7f646242eff989d"), VoteCount: 100000013, URL: "http://GR14.com"},
		{Address: hexToAddress("41ffd564656556a8b6b79311a932e3d216f4fc030b"), VoteCount: 100000012, URL: "http://GR15.com"},
		{Address: hexToAddress("414593d27b70d21454b39ab60bf13291dae8dc0326"), VoteCount: 100000011, URL: "http://GR16.com"},
		{Address: hexToAddress("41746e6af4ac9db3473c0c955f1fca11d4013f32ed"), VoteCount: 100000010, URL: "http://GR17.com"},
		{Address: hexToAddress("41e72d833e0c46837c0802864acc5f119a0a904d05"), VoteCount: 100000009, URL: "http://GR18.com"},
		{Address: hexToAddress("41f8c7acc4c08cf36ca08fc2a61b1f5a7c8dea7bec"), VoteCount: 100000008, URL: "http://GR19.com"},
		{Address: hexToAddress("411d7aba13ea199a63d1647e58e39c16a9bb9da689"), VoteCount: 100000007, URL: "http://GR20.com"},
		{Address: hexToAddress("410694981b116304ed21e05896fb16a6bc2e91c92c"), VoteCount: 100000006, URL: "http://GR21.com"},
		{Address: hexToAddress("411155d10415fac16a8f4cb2f382ce0e0f0a7e64cc"), VoteCount: 100000005, URL: "http://GR22.com"},
		{Address: hexToAddress("41318b2b6b4c7fcaa4b62f25a282329e1952a3c0d1"), VoteCount: 100000004, URL: "http://GR23.com"},
		{Address: hexToAddress("41a857362c1b77cb04e8f2b51b6e970f24fa5c1e5b"), VoteCount: 100000003, URL: "http://GR24.com"},
		{Address: hexToAddress("41a8bb7680d85f9821b3d82505edc4663f6fbd8fde"), VoteCount: 100000002, URL: "http://GR25.com"},
		{Address: hexToAddress("4127bf0d1a57f335c11bc5d002dd82e9e0727cb967"), VoteCount: 100000001, URL: "http://GR26.com"},
		{Address: hexToAddress("4172fd5dfb8ab36eb28df8e4aee97966a60ebf9efe"), VoteCount: 100000000, URL: "http://GR27.com"},
	}
}
