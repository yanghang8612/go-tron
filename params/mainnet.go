package params

import (
	"encoding/hex"

	"github.com/tronprotocol/go-tron/common"
)

// MainnetBootstrapNodes is the list of TRON mainnet discovery seed nodes.
// These are the java-tron default seed nodes from config.conf.
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
}

func hexToAddress(h string) common.Address {
	b, _ := hex.DecodeString(h)
	return common.BytesToAddress(b)
}

func DefaultMainnetGenesis() *Genesis {
	return &Genesis{
		Config:    MainnetChainConfig,
		Timestamp: 0,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("41928c9af0651632157ef27a2cf17ca72c575a4d21"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("41a614f803b6fd780986a42c78ec9c7f77e6ded13c"), Balance: 0, AccountName: "Sun"},
			{Address: hexToAddress("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8"), Balance: -9223372036854775808, AccountName: "Blackhole"},
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
		},
	}
}

func mainnetWitnesses() []GenesisWitness {
	return []GenesisWitness{
		{Address: hexToAddress("41f16412b9a17ee9408646e2a21e16478f72ed1e95"), VoteCount: 100000026, URL: "http://GR1.com"},
		{Address: hexToAddress("41f0b7e8c1f1c15ac97b29efbd5e24e780d4e1be09"), VoteCount: 100000025, URL: "http://GR2.com"},
		{Address: hexToAddress("4116637e5de202808cbbe2a4dfcc72e79e855830a8"), VoteCount: 100000024, URL: "http://GR3.com"},
		{Address: hexToAddress("41b8f03ff75ddc0e8da4caa0e9c4a8b7e0a69bcfe2"), VoteCount: 100000023, URL: "http://GR4.com"},
		{Address: hexToAddress("41dccb07da377c92e2b12de534b4ca03f9981e7b74"), VoteCount: 100000022, URL: "http://GR5.com"},
		{Address: hexToAddress("4130bfe02f52d40e6c3de6b37b5da0de979dac7c31"), VoteCount: 100000021, URL: "http://GR6.com"},
		{Address: hexToAddress("41f068ef9a4ae8dbd3c29a7781e23f0fb5e9df1f5c"), VoteCount: 100000020, URL: "http://GR7.com"},
		{Address: hexToAddress("41b56445cd243e7da09d36d2ec6d7fee7ce9b4e11b"), VoteCount: 100000019, URL: "http://GR8.com"},
		{Address: hexToAddress("4145bafaa059f20c39a1caad80ed3c5deab3c12f74"), VoteCount: 100000018, URL: "http://GR9.com"},
		{Address: hexToAddress("41d2e6bcbadecf7ed0a51c2bb86f62d15c6be2c80d"), VoteCount: 100000017, URL: "http://GR10.com"},
		{Address: hexToAddress("41df4e74e9c05bb7e46e56e52c4d19f01a8340b02e"), VoteCount: 100000016, URL: "http://GR11.com"},
		{Address: hexToAddress("417a40fe3a5a6a40bf3518f0acacfabcab09d881bf"), VoteCount: 100000015, URL: "http://GR12.com"},
		{Address: hexToAddress("416c9a0e72f5b67e14e24c8d69baf6c64d6c4faae8"), VoteCount: 100000014, URL: "http://GR13.com"},
		{Address: hexToAddress("41ffbacf49a252373ec9fcdfeb2c3f6b4f1c8b5bcf"), VoteCount: 100000013, URL: "http://GR14.com"},
		{Address: hexToAddress("41ffd564656556a8b6b79311a932e3d216f4fc030b"), VoteCount: 100000012, URL: "http://GR15.com"},
		{Address: hexToAddress("4115fcee4a0aca62f1a9c45af83d8d2c6a447a1fb7"), VoteCount: 100000011, URL: "http://GR16.com"},
		{Address: hexToAddress("41b4d0fc4ef7c30ad6de53a79dc181d76c8a8ddd33"), VoteCount: 100000010, URL: "http://GR17.com"},
		{Address: hexToAddress("41750e9025ba46a14135c10ce8da8ea89fc2af7cda"), VoteCount: 100000009, URL: "http://GR18.com"},
		{Address: hexToAddress("41ac0a6e97a0b85fc8e68ec9f04f8dff5da96e6c32"), VoteCount: 100000008, URL: "http://GR19.com"},
		{Address: hexToAddress("4116349a5c5b3f2fd30dd12e8ef7bba79eb41ac5d9"), VoteCount: 100000007, URL: "http://GR20.com"},
		{Address: hexToAddress("41dcabc8a49d0ac6d06da3a7ea4aa4c263715ffb5c"), VoteCount: 100000006, URL: "http://GR21.com"},
		{Address: hexToAddress("41bf5c1fdca6e4dc0f0e3c15ca26703e96e18ce4de"), VoteCount: 100000005, URL: "http://GR22.com"},
		{Address: hexToAddress("4117b97d8ab6c05e11e89e1dbb0ca3d64c3c08ddaa"), VoteCount: 100000004, URL: "http://GR23.com"},
		{Address: hexToAddress("41775c87e0fa287b75bcc7310b3bac8ee20b8c3ca5"), VoteCount: 100000003, URL: "http://GR24.com"},
		{Address: hexToAddress("41a0d72c6b85f5a5a16d5e31ae95b75f1f61ab3ecc"), VoteCount: 100000002, URL: "http://GR25.com"},
		{Address: hexToAddress("41c8dd76a0be3bdc1c8bf8df82b29db4dab988fbb4"), VoteCount: 100000001, URL: "http://GR26.com"},
		{Address: hexToAddress("41c1bdfa53c0a7c24a2a35e05a757e975fe9c52a33"), VoteCount: 100000000, URL: "http://GR27.com"},
	}
}
