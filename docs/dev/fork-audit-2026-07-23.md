# Fork-Gate Audit — full-tree evidence

Generated: 2026-07-23T23:36:53Z

- java-tron: `e89c0d66520231b0b8abb2baee776d1d570e5fca`
- go-tron: `55963043ec4792dfb03008b06616b72cefe22fc0`
- Scope: production sources only; Java and Go tests are excluded from callsite inventories.
- Interpretation: this file inventories syntactic gates. Semantic parity conclusions belong in the accompanying reviewed audit.

## Proposal universe and mapping

| ID | java `ProposalType` | Validation case | Apply case | go-tron DP key |
|---:|---|---:|---:|---|
| 0 | `MAINTENANCE_TIME_INTERVAL` | 35 | 32 | `maintenance_time_interval` |
| 1 | `ACCOUNT_UPGRADE_COST` | 42 | 36 | `account_upgrade_cost` |
| 2 | `CREATE_ACCOUNT_FEE` | 43 | 40 | `create_account_fee` |
| 3 | `TRANSACTION_FEE` | 44 | 44 | `transaction_fee` |
| 4 | `ASSET_ISSUE_FEE` | 45 | 52 | `asset_issue_fee` |
| 5 | `WITNESS_PAY_PER_BLOCK` | 46 | 56 | `witness_pay_per_block` |
| 6 | `WITNESS_STANDBY_ALLOWANCE` | 47 | 60 | `witness_standby_allowance` |
| 7 | `CREATE_NEW_ACCOUNT_FEE_IN_SYSTEM_CONTRACT` | 48 | 64 | `create_new_account_fee_in_system_contract` |
| 8 | `CREATE_NEW_ACCOUNT_BANDWIDTH_RATE` | 49 | 69 | `create_new_account_bandwidth_rate` |
| 9 | `ALLOW_CREATION_OF_CONTRACTS` | 55 | 73 | `allow_creation_of_contracts` |
| 10 | `REMOVE_THE_POWER_OF_THE_GR` | 62 | 77 | `remove_the_power_of_the_gr` |
| 11 | `ENERGY_FEE` | 74 | 83 | `energy_fee` |
| 12 | `EXCHANGE_CREATE_FEE` | 75 | 91 | `exchange_create_fee` |
| 13 | `MAX_CPU_TIME_OF_ONE_TX` | 77 | 95 | `max_cpu_time_of_one_tx` |
| 14 | `ALLOW_UPDATE_ACCOUNT_NAME` | 90 | 99 | `allow_update_account_name` |
| 15 | `ALLOW_SAME_TOKEN_NAME` | 97 | 103 | `allow_same_token_name` |
| 16 | `ALLOW_DELEGATE_RESOURCE` | 104 | 107 | `allow_delegate_resource` |
| 17 | `TOTAL_ENERGY_LIMIT` | 111 | 111 | `total_energy_limit` |
| 18 | `ALLOW_TVM_TRANSFER_TRC10` | 123 | 115 | `allow_tvm_transfer_trc10` |
| 19 | `TOTAL_CURRENT_ENERGY_LIMIT` | 134 | 119 | `total_energy_limit` |
| 20 | `ALLOW_MULTI_SIGN` | 143 | 123 | `allow_multi_sign` |
| 21 | `ALLOW_ADAPTIVE_ENERGY` | 153 | 129 | `allow_adaptive_energy` |
| 22 | `UPDATE_ACCOUNT_PERMISSION_FEE` | 163 | 143 | `update_account_permission_fee` |
| 23 | `MULTI_SIGN_FEE` | 173 | 147 | `multi_sign_fee` |
| 24 | `ALLOW_PROTO_FILTER_NUM` | 182 | 151 | `allow_proto_filter_num` |
| 25 | `ALLOW_ACCOUNT_STATE_ROOT` | 192 | 155 | `allow_account_state_root` |
| 26 | `ALLOW_TVM_CONSTANTINOPLE` | 202 | 159 | `allow_tvm_constantinople` |
| 29 | `ADAPTIVE_RESOURCE_LIMIT_MULTIPLIER` | 243 | 175 | `adaptive_resource_limit_multiplier` |
| 30 | `ALLOW_CHANGE_DELEGATION` | 253 | 179 | `change_delegation` |
| 31 | `WITNESS_127_PAY_PER_BLOCK` | 263 | 184 | `witness_127_pay_per_block` |
| 32 | `ALLOW_TVM_SOLIDITY_059` | 217 | 164 | `allow_tvm_solidity059` |
| 33 | `ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO` | 233 | 168 | `adaptive_resource_limit_target_ratio` |
| 35 | `FORBID_TRANSFER_TO_CONTRACT` | 309 | 204 | `forbid_transfer_to_contract` |
| 39 | `ALLOW_SHIELDED_TRC20_TRANSACTION` | 347 | 216 | `allow_shielded_trc20_transaction` |
| 40 | `ALLOW_PBFT` | 325 | 208 | `allow_pbft` |
| 41 | `ALLOW_TVM_ISTANBUL` | 336 | 212 | `allow_tvm_istanbul` |
| 44 | `ALLOW_MARKET_TRANSACTION` | 358 | 220 | `allow_market_transaction` |
| 45 | `MARKET_SELL_FEE` | 370 | 228 | `market_sell_fee` |
| 46 | `MARKET_CANCEL_FEE` | 384 | 232 | `market_cancel_fee` |
| 47 | `MAX_FEE_LIMIT` | 398 | 236 | `max_fee_limit` |
| 48 | `ALLOW_TRANSACTION_FEE_POOL` | 416 | 240 | `allow_transaction_fee_pool` |
| 49 | `ALLOW_BLACKHOLE_OPTIMIZATION` | 427 | 244 | `allow_blackhole_optimization` |
| 51 | `ALLOW_NEW_RESOURCE_MODEL` | 438 | 248 | `allow_new_resource_model` |
| 52 | `ALLOW_TVM_FREEZE` | 449 | 252 | `allow_tvm_freeze` |
| 53 | `ALLOW_ACCOUNT_ASSET_OPTIMIZATION` | 517 | 277 | `allow_account_asset_optimization` |
| 59 | `ALLOW_TVM_VOTE` | 480 | 256 | `allow_tvm_vote` |
| 60 | `ALLOW_TVM_COMPATIBLE_EVM` | 539 | 265 | `allow_tvm_compatible_evm` |
| 61 | `FREE_NET_LIMIT` | 496 | 269 | `free_net_limit` |
| 62 | `TOTAL_NET_LIMIT` | 506 | 273 | `total_net_limit` |
| 63 | `ALLOW_TVM_LONDON` | 528 | 261 | `allow_tvm_london` |
| 65 | `ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX` | 550 | 281 | `allow_higher_limit_for_max_cpu_time_of_one_tx` |
| 66 | `ALLOW_ASSET_OPTIMIZATION` | 561 | 286 | `allow_asset_optimization` |
| 67 | `ALLOW_NEW_REWARD` | 572 | 290 | `allow_new_reward` |
| 68 | `MEMO_FEE` | 587 | 295 | `memo_fee` |
| 69 | `ALLOW_DELEGATE_OPTIMIZATION` | 598 | 318 | `allow_delegate_optimization` |
| 70 | `UNFREEZE_DELAY_DAYS` | 609 | 303 | `unfreeze_delay_days` |
| 71 | `ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID` | 620 | 322 | `allow_optimized_return_value_of_chain_id` |
| 72 | `ALLOW_DYNAMIC_ENERGY` | 632 | 327 | `allow_dynamic_energy` |
| 73 | `DYNAMIC_ENERGY_THRESHOLD` | 649 | 331 | `dynamic_energy_threshold` |
| 74 | `DYNAMIC_ENERGY_INCREASE_FACTOR` | 660 | 335 | `dynamic_energy_increase_factor` |
| 75 | `DYNAMIC_ENERGY_MAX_FACTOR` | 675 | 339 | `dynamic_energy_max_factor` |
| 76 | `ALLOW_TVM_SHANGHAI` | 690 | 343 | `allow_tvm_shanghai` |
| 77 | `ALLOW_CANCEL_ALL_UNFREEZE_V2` | 701 | 347 | `allow_cancel_all_unfreeze_v2` |
| 78 | `MAX_DELEGATE_LOCK_PERIOD` | 717 | 355 | `max_delegate_lock_period` |
| 79 | `ALLOW_OLD_REWARD_OPT` | 736 | 359 | `allow_old_reward_opt` |
| 81 | `ALLOW_ENERGY_ADJUSTMENT` | 756 | 363 | `allow_energy_adjustment` |
| 82 | `MAX_CREATE_ACCOUNT_TX_SIZE` | 771 | 367 | `max_create_account_tx_size` |
| 83 | `ALLOW_TVM_CANCUN` | 815 | 380 | `allow_tvm_cancun` |
| 87 | `ALLOW_STRICT_MATH` | 785 | 371 | `allow_strict_math` |
| 88 | `CONSENSUS_LOGIC_OPTIMIZATION` | 800 | 375 | `consensus_logic_optimization` |
| 89 | `ALLOW_TVM_BLOB` | 830 | 384 | `allow_tvm_blob` |
| 92 | `PROPOSAL_EXPIRE_TIME` | 860 | 392 | `proposal_expire_time` |
| 94 | `ALLOW_TVM_SELFDESTRUCT_RESTRICTION` | 845 | 388 | `allow_tvm_selfdestruct_restriction` |
| 95 | `ALLOW_TVM_PRAGUE` | 889 | 400 | `allow_tvm_prague` |
| 96 | `ALLOW_TVM_OSAKA` | 874 | 396 | `allow_tvm_osaka` |
| 97 | `ALLOW_HARDEN_RESOURCE_CALCULATION` | 912 | 405 | `allow_harden_resource_calculation` |
| 98 | `ALLOW_HARDEN_EXCHANGE_CALCULATION` | 928 | 410 | `allow_harden_exchange_calculation` |

## Proposal-validation software-version gates

Every production `forkController.pass(...)` occurrence in `ProposalUtil.java`:

| File | Line | Check |
|---|---:|---|
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 112 | `if (!forkController.pass(ForkBlockVersionConsts.ENERGY_LIMIT)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 115 | `if (forkController.pass(ForkBlockVersionEnum.VERSION_3_2_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 135 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_2_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 144 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 154 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 164 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 174 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 183 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 193 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 203 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 218 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 234 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 244 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 254 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 264 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 310 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_3_6_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 326 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 337 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 348 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_0_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 359 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1)` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 360 | `\|\| forkController.pass(ForkBlockVersionEnum.VERSION_4_8_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 371 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 385 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 399 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 417 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 428 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_1_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 439 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 450 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 481 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_3)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 497 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_3)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 507 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_3)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 518 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_3)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 529 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_4)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 540 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_4)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 551 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 562 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 573 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 588 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 599 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_6)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 610 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 621 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 633 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 650 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 661 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 676 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 691 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 702 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 718 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 737 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_4)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 757 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 772 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_5)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 786 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_7_7)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 801 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_0)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 816 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_0)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 831 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_0)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 846 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 861 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_1)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 875 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 890 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 913 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_2)) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 929 | `if (!forkController.pass(ForkBlockVersionEnum.VERSION_4_8_2)) {` |

## Java production proposal-feature reads

Every direct production call whose method begins with `getAllow`, `allow`, or `support`.
This deliberately scans the complete Java tree, not only `*Actuator.java`.

| File | Line | Check |
|---|---:|---|
| `chainbase/src/main/java/org/tron/common/overlay/message/Message.java` | 65 | `return dynamicPropertiesStore.getAllowProtoFilterNum() == 1;` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 101 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 111 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 124 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/TransferAssetActuator.java` | 65 | `dynamicStore.getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/actuator/TransferAssetActuator.java` | 87 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/MarketSellAssetActuator.java` | 128 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/MarketSellAssetActuator.java` | 181 | `if (!dynamicStore.supportAllowMarketTransaction()) {` |
| `actuator/src/main/java/org/tron/core/actuator/MarketCancelOrderActuator.java` | 98 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/MarketCancelOrderActuator.java` | 168 | `if (!dynamicStore.supportAllowMarketTransaction()) {` |
| `actuator/src/main/java/org/tron/core/actuator/AccountPermissionUpdateActuator.java` | 55 | `if (chainBaseManager.getDynamicPropertiesStore().supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/AccountPermissionUpdateActuator.java` | 162 | `if (dynamicStore.getAllowMultiSign() != 1) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeCreateActuator.java` | 80 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeCreateActuator.java` | 120 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeCreateActuator.java` | 188 | `if (dynamicStore.getAllowSameTokenName() == 1) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeWithdrawActuator.java` | 195 | `if (dynamicStore.getAllowSameTokenName() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/WitnessCreateActuator.java` | 137 | `if (dynamicStore.getAllowMultiSign() == 1) {` |
| `actuator/src/main/java/org/tron/core/actuator/WitnessCreateActuator.java` | 143 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeInjectActuator.java` | 190 | `if (dynamicStore.getAllowSameTokenName() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/AbstractExchangeActuator.java` | 14 | `return chainBaseManager.getDynamicPropertiesStore().allowHardenExchangeCalculation();` |
| `actuator/src/main/java/org/tron/core/actuator/WithdrawExpireUnfreezeActuator.java` | 84 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UpdateAssetActuator.java` | 64 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/UpdateAssetActuator.java` | 131 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 272 | `this.getAllowMultiSign();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 275 | `.getAllowMultiSign());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 460 | `this.getAllowAdaptiveEnergy();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 463 | `.getAllowAdaptiveEnergy());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 563 | `this.getAllowMarketTransaction();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 565 | `this.saveAllowMarketTransaction(CommonParameter.getInstance().getAllowMarketTransaction());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 587 | `this.getAllowTransactionFeePool();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 589 | `this.saveAllowTransactionFeePool(CommonParameter.getInstance().getAllowTransactionFeePool());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 647 | `this.getAllowDelegateResource();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 650 | `.getAllowDelegateResource());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 654 | `this.getAllowTvmTransferTrc10();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 657 | `.getAllowTvmTransferTrc10());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 661 | `this.getAllowTvmConstantinople();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 664 | `.getAllowTvmConstantinople());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 668 | `this.getAllowTvmSolidity059();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 671 | `.getAllowTvmSolidity059());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 698 | `this.getAllowSameTokenName();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 701 | `.getAllowSameTokenName());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 705 | `this.getAllowUpdateAccountName();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 711 | `this.getAllowCreationOfContracts();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 714 | `.getAllowCreationOfContracts());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 718 | `this.getAllowShieldedTransaction();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 724 | `this.getAllowShieldedTRC20Transaction();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 727 | `CommonParameter.getInstance().getAllowShieldedTRC20Transaction());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 731 | `this.getAllowTvmIstanbul();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 734 | `CommonParameter.getInstance().getAllowTvmIstanbul());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 790 | `this.getAllowAccountStateRoot();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 793 | `.getAllowAccountStateRoot());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 797 | `this.getAllowProtoFilterNum();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 800 | `.getAllowProtoFilterNum());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 811 | `this.getAllowPBFT();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 813 | `this.saveAllowPBFT(CommonParameter.getInstance().getAllowPBFT());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 829 | `this.getAllowBlackHoleOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 832 | `CommonParameter.getInstance().getAllowBlackHoleOptimization());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 836 | `this.getAllowNewResourceModel();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 838 | `this.saveAllowNewResourceModel(CommonParameter.getInstance().getAllowNewResourceModel());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 842 | `this.getAllowTvmFreeze();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 844 | `this.saveAllowTvmFreeze(CommonParameter.getInstance().getAllowTvmFreeze());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 848 | `this.getAllowTvmVote();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 850 | `this.saveAllowTvmVote(CommonParameter.getInstance().getAllowTvmVote());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 851 | `if (CommonParameter.getInstance().getAllowTvmVote() == 1) {` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 858 | `this.getAllowTvmLondon();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 860 | `this.saveAllowTvmLondon(CommonParameter.getInstance().getAllowTvmLondon());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 864 | `this.getAllowTvmCompatibleEvm();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 866 | `this.saveAllowTvmCompatibleEvm(CommonParameter.getInstance().getAllowTvmCompatibleEvm());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 870 | `this.getAllowAssetOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 873 | `.getInstance().getAllowAssetOptimization());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 877 | `this.getAllowAccountAssetOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 880 | `.getInstance().getAllowAccountAssetOptimization());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 914 | `this.getAllowHigherLimitForMaxCpuTimeOfOneTx();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 917 | `CommonParameter.getInstance().getAllowHigherLimitForMaxCpuTimeOfOneTx());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 923 | `if (CommonParameter.getInstance().getAllowNewRewardAlgorithm() == 1) {` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 933 | `this.getAllowNewReward();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 935 | `this.saveAllowNewReward(CommonParameter.getInstance().getAllowNewReward());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 936 | `if (CommonParameter.getInstance().getAllowNewReward() == 1) {` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 951 | `this.getAllowDelegateOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 954 | `CommonParameter.getInstance().getAllowDelegateOptimization());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 966 | `this.getAllowOptimizedReturnValueOfChainId();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 969 | `CommonParameter.getInstance().getAllowOptimizedReturnValueOfChainId()` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 974 | `this.getAllowDynamicEnergy();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 976 | `this.saveAllowDynamicEnergy(CommonParameter.getInstance().getAllowDynamicEnergy());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 1943 | `.getAllowTvmConstantinople() != 0) {` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 1945 | `.getAllowTvmConstantinople());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2592 | `return CommonParameter.getInstance().getAllowAssetOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2841 | `.orElse(CommonParameter.getInstance().getAllowTvmShangHai());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2853 | `.orElse(CommonParameter.getInstance().getAllowCancelAllUnfreezeV2());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2889 | `.orElse(CommonParameter.getInstance().getAllowOldRewardOpt());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2900 | `.orElse(CommonParameter.getInstance().getAllowEnergyAdjustment());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2918 | `.orElse(CommonParameter.getInstance().getAllowStrictMath());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2929 | `return this.allowConsensusLogicOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2949 | `return this.allowConsensusLogicOptimization();` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2961 | `.orElse(CommonParameter.getInstance().getAllowTvmCancun());` |
| `chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` | 2972 | `.orElse(CommonParameter.getInstance().getAllowTvmBlob());` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceV2Actuator.java` | 52 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceV2Actuator.java` | 107 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceV2Actuator.java` | 148 | `if (!dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceV2Actuator.java` | 154 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/TransferActuator.java` | 52 | `dynamicStore.getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/actuator/TransferActuator.java` | 61 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/TransferActuator.java` | 143 | `if (dynamicStore.getAllowTvmCompatibleEvm() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/UnDelegateResourceActuator.java` | 205 | `if (!dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnDelegateResourceActuator.java` | 209 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeTransactionActuator.java` | 69 | `dynamicStore.allowStrictMath(), allowHarden());` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeTransactionActuator.java` | 177 | `if (dynamicStore.getAllowSameTokenName() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/ExchangeTransactionActuator.java` | 218 | `dynamicStore.allowStrictMath(), allowHarden());` |
| `actuator/src/main/java/org/tron/core/actuator/VoteWitnessActuator.java` | 131 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VoteWitnessActuator.java` | 166 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeAssetActuator.java` | 62 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeAssetActuator.java` | 124 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 134 | `if ((VMConfig.allowTvmFreeze() \|\| VMConfig.allowTvmFreezeV2())` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 195 | `if (VMConfig.allowEnergyAdjustment()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 204 | `if (code.length != 0 && VMConfig.allowTvmLondon() && code[0] == (byte) 0xEF) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 219 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 317 | `if (VMConfig.allowTvmOsaka()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 325 | `if (!rootRepository.getDynamicPropertiesStore().supportVM()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 334 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 368 | `if (VMConfig.allowTvmTransferTrc10()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 424 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 444 | `if (!VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 451 | `if (VMConfig.allowTvmTransferTrc10() && tokenValue > 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 465 | `if (!rootRepository.getDynamicPropertiesStore().supportVM()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 490 | `if (VMConfig.allowTvmTransferTrc10()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 539 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 557 | `if (VMConfig.allowTvmTransferTrc10() && tokenValue > 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 573 | `if (VMConfig.allowTvmFreeze() \|\| VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 583 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 650 | `if (Objects.isNull(creator) && VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 663 | `if (VMConfig.allowTvmTransferTrc10() && VMConfig.allowMultiSign()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 738 | `if (VMConfig.allowTvmFreeze() \|\| VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/actuator/VMActuator.java` | 759 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/actuator/ClearABIContractActuator.java` | 65 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 78 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 90 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 113 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 169 | `if (dynamicStore.getAllowSameTokenName() != 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 178 | `&& dynamicStore.getAllowSameTokenName() != 0` |
| `actuator/src/main/java/org/tron/core/actuator/AssetIssueActuator.java` | 210 | `if (dynamicStore.getAllowSameTokenName() == 0` |
| `consensus/src/main/java/org/tron/consensus/dpos/IncentiveManager.java` | 21 | `if (consensusDelegate.allowChangeDelegation()) {` |
| `actuator/src/main/java/org/tron/core/actuator/CreateAccountActuator.java` | 43 | `dynamicStore.getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/actuator/CreateAccountActuator.java` | 52 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/DelegateResourceActuator.java` | 69 | `long lockPeriod = getLockPeriod(dynamicStore.supportMaxDelegateLockPeriod(),` |
| `actuator/src/main/java/org/tron/core/actuator/DelegateResourceActuator.java` | 118 | `if (!dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/DelegateResourceActuator.java` | 122 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/DelegateResourceActuator.java` | 212 | `if (lock && dynamicStore.supportMaxDelegateLockPeriod()) {` |
| `consensus/src/main/java/org/tron/consensus/dpos/DposService.java` | 121 | `&& consensusDelegate.getDynamicPropertiesStore().allowConsensusLogicOptimization()) {` |
| `consensus/src/main/java/org/tron/consensus/dpos/DposService.java` | 135 | `&& consensusDelegate.getDynamicPropertiesStore().allowConsensusLogicOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 64 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 83 | `&& dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 99 | `&& dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 136 | `long weight = dynamicStore.allowNewReward() ? increment : frozenBalance / TRX_PRECISION;` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 221 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 233 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 245 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 262 | `if (dynamicStore.getAllowTvmConstantinople() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 271 | `if (dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/FreezeBalanceActuator.java` | 320 | `if (!dynamicPropertiesStore.supportAllowDelegateOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UpdateBrokerageActuator.java` | 69 | `if (!dynamicStore.allowChangeDelegation()) {` |
| `consensus/src/main/java/org/tron/consensus/dpos/MaintenanceManager.java` | 154 | `if (dynamicPropertiesStore.allowChangeDelegation()) {` |
| `chainbase/src/main/java/org/tron/core/service/MortgageService.java` | 55 | `dynamicPropertiesStore.allowWitnessSortOptimization());` |
| `chainbase/src/main/java/org/tron/core/service/MortgageService.java` | 90 | `if (!dynamicPropertiesStore.allowChangeDelegation()) {` |
| `chainbase/src/main/java/org/tron/core/service/MortgageService.java` | 137 | `if (!dynamicPropertiesStore.allowChangeDelegation()) {` |
| `chainbase/src/main/java/org/tron/core/service/MortgageService.java` | 261 | `if (dynamicPropertiesStore.allowOldRewardOpt()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UpdateAccountActuator.java` | 90 | `&& chainBaseManager.getDynamicPropertiesStore().getAllowUpdateAccountName() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/UpdateAccountActuator.java` | 95 | `&& chainBaseManager.getDynamicPropertiesStore().getAllowUpdateAccountName() == 0) {` |
| `plugins/src/main/java/x86/org/tron/plugins/ArchiveManifest.java` | 131 | `executor.allowCoreThreadTimeOut(true);` |
| `actuator/src/main/java/org/tron/core/actuator/CancelAllUnfreezeV2Actuator.java` | 130 | `if (!dynamicStore.supportAllowCancelAllUnfreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 78 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 91 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 119 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 157 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 166 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 312 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceV2Actuator.java` | 345 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 81 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 90 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 115 | `if (dynamicStore.getAllowTvmConstantinople() == 0 \|\|` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 121 | `if (dynamicStore.getAllowTvmSolidity059() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 136 | `if (dynamicStore.getAllowTvmSolidity059() == 1` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 163 | `if (!dynamicStore.supportAllowDelegateOptimization()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 243 | `long weight = dynamicStore.allowNewReward() ? decrease : -unfreezeBalance / TRX_PRECISION;` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 263 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 288 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 339 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 350 | `if (dynamicStore.getAllowTvmConstantinople() == 0` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 373 | `if (dynamicStore.getAllowTvmConstantinople() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 383 | `if (dynamicStore.getAllowTvmSolidity059() != 1` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 404 | `if (dynamicStore.getAllowTvmConstantinople() == 0) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 414 | `if (dynamicStore.getAllowTvmSolidity059() != 1` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 460 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/UnfreezeBalanceActuator.java` | 473 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/actuator/ShieldedTransferActuator.java` | 141 | `dynamicStore.getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/actuator/ShieldedTransferActuator.java` | 214 | `if (dynamicStore.getAllowSameTokenName() != 1) {` |
| `actuator/src/main/java/org/tron/core/actuator/ShieldedTransferActuator.java` | 219 | `if (!dynamicStore.supportShieldedTransaction()) {` |
| `consensus/src/main/java/org/tron/consensus/pbft/PbftManager.java` | 43 | `if (!chainBaseManager.getDynamicPropertiesStore().allowPBFT()) {` |
| `consensus/src/main/java/org/tron/consensus/pbft/PbftManager.java` | 58 | `if (!chainBaseManager.getDynamicPropertiesStore().allowPBFT()) {` |
| `consensus/src/main/java/org/tron/consensus/ConsensusDelegate.java` | 136 | `return dynamicPropertiesStore.allowChangeDelegation();` |
| `consensus/src/main/java/org/tron/consensus/ConsensusDelegate.java` | 140 | `witnessStore.sortWitness(list, dynamicPropertiesStore.allowWitnessSortOptimization());` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 456 | `if (VMConfig.allowTvmVote()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 478 | `if (VMConfig.allowTvmTransferTrc10()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 486 | `if (VMConfig.allowTvmTransferTrc10()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 490 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 497 | `if (VMConfig.allowTvmFreeze()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 505 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 546 | `if (VMConfig.allowTvmVote()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 558 | `if (VMConfig.allowTvmTransferTrc10()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 562 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 570 | `if (VMConfig.allowTvmFreeze()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 575 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 614 | `if (VMConfig.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 731 | `boolean freezeCheck = !VMConfig.allowTvmFreeze()` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 737 | `//    boolean voteCheck = !VMConfig.allowTvmVote()` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 753 | `if (!VMConfig.allowTvmFreeze()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 778 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 830 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 835 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 847 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 863 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 914 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 927 | `if (code.length != 0 && VMConfig.allowTvmLondon() && code[0] == (byte) 0xEF) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1036 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1089 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1102 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1145 | `if (VMConfig.allowTvmCompatibleEvm()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1393 | `if (VMConfig.allowTvmCompatibleEvm() \|\| VMConfig.allowOptimizedReturnValueOfChainId()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1619 | `if (VMConfig.allowTvmOsaka()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1624 | `if ((VMConfig.allowTvmCompatibleEvm() \|\| VMConfig.allowTvmOsaka())` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1632 | `if (VMConfig.allowTvmIstanbul()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1757 | `if (VMConfig.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1786 | `if (VMConfig.allowMultiSign()) { //allowMultiSign proposal` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1792 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1803 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1814 | `if (VMConfig.allowMultiSign()) { //allowMultiSign proposal` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1822 | `if (VMConfig.allowMultiSign()) { //allowMultiSigns proposal` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1828 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1843 | `if (VMConfig.allowTvmCompatibleEvm() && getContractVersion() == 1) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1852 | `if (VMConfig.allowTvmCompatibleEvm() && getContractVersion() == 1) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 1883 | `if (VMConfig.allowTvmSolidity059()) {` |
| `actuator/src/main/java/org/tron/core/vm/program/Program.java` | 2374 | `VMConfig.allowStrictMath(),` |
| `chainbase/src/main/java/org/tron/core/db2/core/SnapshotRoot.java` | 46 | `.getAllowAccountAssetOptimizationFromRoot() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/program/ProgramPrecompile.java` | 49 | `if (VMConfig.allowTvmConstantinople()) {` |
| `chainbase/src/main/java/org/tron/core/db/TransactionTrace.java` | 134 | `if (dynamicPropertiesStore.getAllowTvmConstantinople() == 1) {` |
| `chainbase/src/main/java/org/tron/core/db/TransactionTrace.java` | 261 | `if (dynamicPropertiesStore.supportUnfreezeDelay()` |
| `chainbase/src/main/java/org/tron/core/db/TransactionTrace.java` | 292 | `if (dynamicPropertiesStore.supportAllowCancelAllUnfreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/VM.java` | 27 | `final boolean allowDynamicEnergy = VMConfig.allowDynamicEnergy();` |
| `framework/src/main/java/org/tron/core/services/NodeInfoService.java` | 192 | `configNodeInfo.setAllowCreationOfContracts(parameter.getAllowCreationOfContracts());` |
| `framework/src/main/java/org/tron/core/services/NodeInfoService.java` | 193 | `configNodeInfo.setAllowAdaptiveEnergy(parameter.getAllowAdaptiveEnergy());` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceProcessor.java` | 205 | `if (VMConfig.allowTvmVote() && !accountCapsule.getVotesList().isEmpty()) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 108 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 117 | `&& dynamicPropertiesStore.getAllowTvmFreeze() == 0` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 118 | `&& !dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 123 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 137 | `if (dynamicPropertiesStore.getAllowAdaptiveEnergy() == 1) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 147 | `if (dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/EnergyProcessor.java` | 156 | `if (dynamicPropertiesStore.allowNewReward() && totalEnergyWeight <= 0) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 254 | `if (VMConfig.allowTvmSolidity059() && address.equals(batchValidateSignAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 257 | `if (VMConfig.allowTvmSolidity059() && address.equals(validateMultiSignAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 260 | `if (VMConfig.allowShieldedTRC20Transaction() && address.equals(verifyMintProofAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 263 | `if (VMConfig.allowShieldedTRC20Transaction() && address.equals(verifyTransferProofAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 266 | `if (VMConfig.allowShieldedTRC20Transaction() && address.equals(verifyBurnProofAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 269 | `if (VMConfig.allowShieldedTRC20Transaction() && address.equals(merkleHashAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 272 | `if (VMConfig.allowTvmVote() && address.equals(rewardBalanceAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 275 | `if (VMConfig.allowTvmVote() && address.equals(isSrCandidateAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 278 | `if (VMConfig.allowTvmVote() && address.equals(voteCountAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 281 | `if (VMConfig.allowTvmVote() && address.equals(usedVoteCountAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 284 | `if (VMConfig.allowTvmVote() && address.equals(receivedVoteCountAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 287 | `if (VMConfig.allowTvmVote() && address.equals(totalVoteCountAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 290 | `if (VMConfig.allowTvmCompatibleEvm() && address.equals(ethRipemd160Addr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 293 | `if (VMConfig.allowTvmCompatibleEvm() && address.equals(blake2FAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 296 | `if (VMConfig.allowTvmOsaka() && address.equals(p256VerifyAddr)) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 300 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 670 | `if (VMConfig.allowTvmOsaka()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 697 | `if (VMConfig.allowTvmOsaka()` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 712 | `if (VMConfig.allowTvmOsaka()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 849 | `if (VMConfig.allowTvmIstanbul()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 903 | `if (VMConfig.allowTvmIstanbul()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 956 | `if (VMConfig.allowTvmIstanbul()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1053 | `if (VMConfig.allowTvmOsaka()` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1066 | `if (VMConfig.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1072 | `byte[][] signatures = VMConfig.allowTvmSelfdestructRestriction() ?` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1158 | `if (VMConfig.allowTvmOsaka()` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1165 | `if (VMConfig.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1173 | `byte[][] signatures = VMConfig.allowTvmSelfdestructRestriction() ?` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1965 | `if (getDeposit().getDynamicPropertiesStore().supportUnfreezeDelay()` |
| `actuator/src/main/java/org/tron/core/vm/PrecompiledContracts.java` | 1966 | `&& getDeposit().getDynamicPropertiesStore().supportAllowNewResourceModel()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 57 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 103 | `.getDynamicPropertiesStore().allowConsensusLogicOptimization();` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 116 | `if (chainBaseManager.getDynamicPropertiesStore().supportVM()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 126 | `if (chainBaseManager.getDynamicPropertiesStore().supportVM()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 218 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 228 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 335 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 362 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 380 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 400 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 434 | `if (dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 442 | `if (dynamicPropertiesStore.allowNewReward() && totalNetWeight <= 0) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 475 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/BandwidthProcessor.java` | 491 | `if (!dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `actuator/src/main/java/org/tron/core/vm/EnergyCost.java` | 368 | `if (!VMConfig.allowEnergyAdjustment()) {` |
| `actuator/src/main/java/org/tron/core/vm/EnergyCost.java` | 395 | `if (!VMConfig.allowTvmOsaka()) {` |
| `actuator/src/main/java/org/tron/core/vm/EnergyCost.java` | 513 | `if (VMConfig.allowDynamicEnergy()) {` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 26 | `snapshot.allowMultiSign = ds.getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 27 | `snapshot.allowTvmTransferTrc10 = ds.getAllowTvmTransferTrc10() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 28 | `snapshot.allowTvmConstantinople = ds.getAllowTvmConstantinople() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 29 | `snapshot.allowTvmSolidity059 = ds.getAllowTvmSolidity059() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 30 | `snapshot.allowShieldedTRC20Transaction = ds.getAllowShieldedTRC20Transaction() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 31 | `snapshot.allowTvmIstanbul = ds.getAllowTvmIstanbul() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 32 | `snapshot.allowTvmFreeze = ds.getAllowTvmFreeze() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 33 | `snapshot.allowTvmVote = ds.getAllowTvmVote() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 34 | `snapshot.allowTvmLondon = ds.getAllowTvmLondon() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 35 | `snapshot.allowTvmCompatibleEvm = ds.getAllowTvmCompatibleEvm() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 37 | `ds.getAllowHigherLimitForMaxCpuTimeOfOneTx() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 38 | `snapshot.allowTvmFreezeV2 = ds.supportUnfreezeDelay();` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 39 | `snapshot.allowOptimizedReturnValueOfChainId = ds.getAllowOptimizedReturnValueOfChainId() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 40 | `snapshot.allowDynamicEnergy = ds.getAllowDynamicEnergy() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 44 | `snapshot.allowTvmShanghai = ds.getAllowTvmShangHai() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 45 | `snapshot.allowEnergyAdjustment = ds.getAllowEnergyAdjustment() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 46 | `snapshot.allowStrictMath = ds.getAllowStrictMath() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 47 | `snapshot.allowTvmCancun = ds.getAllowTvmCancun() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 49 | `snapshot.allowTvmBlob = ds.getAllowTvmBlob() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 50 | `snapshot.allowTvmSelfdestructRestriction = ds.getAllowTvmSelfdestructRestriction() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 51 | `snapshot.allowTvmOsaka = ds.getAllowTvmOsaka() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/config/ConfigLoader.java` | 52 | `snapshot.allowHardenResourceCalculation = ds.getAllowHardenResourceCalculation() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/OperationRegistry.java` | 82 | `if (VMConfig.allowHigherLimitForMaxCpuTimeOfOneTx()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationRegistry.java` | 86 | `if (VMConfig.allowEnergyAdjustment()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationRegistry.java` | 90 | `if (VMConfig.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationRegistry.java` | 94 | `if (VMConfig.allowTvmOsaka()) {` |
| `common/src/main/java/org/tron/common/entity/NodeInfo.java` | 192 | `configBuilder.setAllowCreationOfContracts(configNodeInfo.getAllowCreationOfContracts());` |
| `common/src/main/java/org/tron/common/entity/NodeInfo.java` | 193 | `configBuilder.setAllowAdaptiveEnergy(configNodeInfo.getAllowAdaptiveEnergy());` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 88 | `if (dynamicPropertiesStore.supportAllowCancelAllUnfreezeV2()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 119 | `if (dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 192 | `if (dynamicPropertiesStore.supportAllowCancelAllUnfreezeV2()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 308 | `if (dynamicPropertiesStore.supportTransactionFeePool()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 310 | `} else if (dynamicPropertiesStore.supportBlackHoleOptimization()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 329 | `if (dynamicPropertiesStore.supportBlackHoleOptimization()) {` |
| `chainbase/src/main/java/org/tron/core/db/ResourceProcessor.java` | 347 | `return dynamicPropertiesStore.allowHardenResourceCalculation();` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 318 | `if (VMConfig.allowMultiSign()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 337 | `if (VMConfig.allowMultiSign()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 445 | `if (VMConfig.allowTvmCompatibleEvm() && program.getContractVersion() == 1) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 793 | `if (VMConfig.allowTvmVote() && program.isStaticCall()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 801 | `if (VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 812 | `if (VMConfig.allowTvmVote() && program.isStaticCall()) {` |
| `actuator/src/main/java/org/tron/core/vm/OperationActions.java` | 998 | `exeCall(program, adjustedCallEnergy, codeAddress, value, tokenId, VMConfig.allowMultiSign());` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/FreezeBalanceV2Processor.java` | 52 | `if (!repo.getDynamicPropertiesStore().supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/FreezeBalanceV2Processor.java` | 58 | `if (repo.getDynamicPropertiesStore().supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/FreezeBalanceV2Processor.java` | 74 | `if (dynamicStore.supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/vm/VMUtils.java` | 38 | `return VMConfig.allowEnergyAdjustment() ?` |
| `actuator/src/main/java/org/tron/core/vm/repository/Value.java` | 23 | `if (VMConfig.allowMultiSign()) {` |
| `actuator/src/main/java/org/tron/core/utils/TransactionUtil.java` | 280 | `if (dps.supportMaxDelegateLockPeriod()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnDelegateResourceProcessor.java` | 39 | `if (!dynamicStore.supportDR()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 78 | `if (dynamicPropertiesStore.getAllowHigherLimitForMaxCpuTimeOfOneTx() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 128 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 210 | `if (dynamicPropertiesStore.getAllowTvmTransferTrc10() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 226 | `if (dynamicPropertiesStore.getAllowCreationOfContracts() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 287 | `//        if (!dynamicPropertiesStore.supportShieldedTransaction()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 291 | `//        if (dynamicPropertiesStore.getAllowCreationOfContracts() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 318 | `if (dynamicPropertiesStore.getAllowCreationOfContracts() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 374 | `if (!dynamicPropertiesStore.supportAllowMarketTransaction()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 388 | `if (!dynamicPropertiesStore.supportAllowMarketTransaction()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 406 | `if (dynamicPropertiesStore.getAllowTvmLondon() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 458 | `if (dynamicPropertiesStore.getAllowDelegateResource() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 463 | `if (dynamicPropertiesStore.getAllowMultiSign() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 468 | `if (dynamicPropertiesStore.getAllowTvmConstantinople() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 473 | `if (dynamicPropertiesStore.getAllowTvmSolidity059() == 0) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 577 | `if (dynamicPropertiesStore.allowNewReward()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 741 | `if (dynamicPropertiesStore.allowOldRewardOpt()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 761 | `if (dynamicPropertiesStore.getAllowEnergyAdjustment() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 790 | `if (dynamicPropertiesStore.allowStrictMath()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 820 | `if (dynamicPropertiesStore.getAllowTvmCancun() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 835 | `if (dynamicPropertiesStore.getAllowTvmBlob() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 850 | `if (dynamicPropertiesStore.allowTvmSelfdestructRestriction()) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 879 | `if (dynamicPropertiesStore.getAllowTvmOsaka() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 898 | `if (dynamicPropertiesStore.getAllowTvmShangHai() != 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 902 | `if (dynamicPropertiesStore.getAllowTvmPrague() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 917 | `if (dynamicPropertiesStore.getAllowHardenResourceCalculation() == 1) {` |
| `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` | 937 | `if (dynamicPropertiesStore.getAllowHardenExchangeCalculation() == value) {` |
| `actuator/src/main/java/org/tron/core/vm/repository/RepositoryImpl.java` | 641 | `if (VMConfig.allowTvmConstantinople()) {` |
| `actuator/src/main/java/org/tron/core/vm/repository/RepositoryImpl.java` | 964 | `return VMConfig.allowHardenResourceCalculation();` |
| `actuator/src/main/java/org/tron/core/vm/repository/RepositoryImpl.java` | 1155 | `getDynamicPropertiesStore().getAllowMultiSign() == 1;` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/VoteWitnessProcessor.java` | 89 | `if (repo.getDynamicPropertiesStore().supportUnfreezeDelay()` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/VoteWitnessProcessor.java` | 90 | `&& repo.getDynamicPropertiesStore().supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 69 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 78 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 128 | `if (repo.getDynamicPropertiesStore().supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 139 | `if (repo.getDynamicPropertiesStore().supportAllowNewResourceModel()` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 214 | `if (!VMConfig.allowTvmVote() \|\| accountCapsule.getVotesList().isEmpty()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 217 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/UnfreezeBalanceV2Processor.java` | 250 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/VoteRewardUtil.java` | 17 | `if (!VMConfig.allowTvmVote()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/VoteRewardUtil.java` | 58 | `if (!VMConfig.allowTvmVote()) {` |
| `actuator/src/main/java/org/tron/core/vm/nativecontract/DelegateResourceProcessor.java` | 40 | `if (!dynamicStore.supportDR()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/BlockCapsule.java` | 196 | `if (dynamicPropertiesStore.getAllowMultiSign() != 1) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 24 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 40 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 69 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 108 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 127 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 143 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `actuator/src/main/java/org/tron/core/vm/utils/FreezeV2Util.java` | 196 | `if (!VMConfig.allowTvmFreezeV2()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ExchangeCapsule.java` | 174 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 707 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 738 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 753 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 785 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 800 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 855 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ContractStateCapsule.java` | 84 | `dps.allowStrictMath(),` |
| `chainbase/src/main/java/org/tron/core/capsule/DelegatedResourceCapsule.java` | 120 | `if (dynamicPropertiesStore.getAllowMultiSign() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 216 | `if (Objects.isNull(origin) && dynamicPropertiesStore.getAllowTvmConstantinople() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 245 | `if (dynamicPropertiesStore.getAllowTvmFreeze() == 1` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 246 | `\|\| dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 269 | `if (dynamicPropertiesStore.getAllowTvmFreeze() == 1` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 270 | `\|\| dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 282 | `dynamicPropertiesStore.getAllowAdaptiveEnergy() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 304 | `if (dynamicPropertiesStore.supportTransactionFeePool() &&` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 307 | `} else if (dynamicPropertiesStore.supportBlackHoleOptimization()) {` |
| `framework/src/main/java/org/tron/core/services/RpcApiService.java` | 1142 | `if (dbManager.getDynamicPropertiesStore().supportAllowNewResourceModel()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AssetIssueCapsule.java` | 123 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/utils/TransactionUtil.java` | 80 | `.getChainBaseManager().getDynamicPropertiesStore().supportTransactionFeePool();` |
| `chainbase/src/main/java/org/tron/core/capsule/utils/AssetUtil.java` | 64 | `return dynamicPropertiesStore.supportAllowAssetOptimization();` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 568 | `if (chainBaseManager.getDynamicPropertiesStore().supportVM()) {` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 810 | `chainBaseManager.getDynamicPropertiesStore().allowWitnessSortOptimization());` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1140 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowCreationOfContracts())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1170 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowUpdateAccountName())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1176 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1182 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowDelegateResource())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1194 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowTvmTransferTrc10())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1206 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowMultiSign())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1212 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowAdaptiveEnergy())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1237 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowAccountStateRoot())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1242 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowProtoFilterNum())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1248 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowTvmConstantinople())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1253 | `.setValue(chainBaseManager.getDynamicPropertiesStore().getAllowTvmSolidity059())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1259 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmIstanbul()).build());` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1265 | `//            .setValue(dbManager.getDynamicPropertiesStore().getAllowShieldedTransaction())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1289 | `dbManager.getDynamicPropertiesStore().getAllowShieldedTRC20Transaction())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1321 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowMarketTransaction())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1336 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowPBFT())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1341 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTransactionFeePool())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1351 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowBlackHoleOptimization())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1356 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowNewResourceModel())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1361 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmFreeze())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1366 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmVote())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1371 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmLondon())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1376 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmCompatibleEvm())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1381 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowAccountAssetOptimization())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1396 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowHigherLimitForMaxCpuTimeOfOneTx())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1400 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowAssetOptimization())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1405 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowNewReward())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1415 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowDelegateOptimization())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1425 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowOptimizedReturnValueOfChainId())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1430 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowDynamicEnergy())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1450 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmShangHai())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1455 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowCancelAllUnfreezeV2())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1464 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowOldRewardOpt())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1469 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowEnergyAdjustment())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1479 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowStrictMath())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1489 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmCancun())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1494 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmBlob())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1499 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmSelfdestructRestriction())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1509 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmOsaka())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1514 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowTvmPrague())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1519 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowHardenResourceCalculation())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1524 | `.setValue(dbManager.getDynamicPropertiesStore().getAllowHardenExchangeCalculation())` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1594 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {` |
| `framework/src/main/java/org/tron/core/Wallet.java` | 1714 | `if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 828 | `.getDynamicPropertiesStore().allowConsensusLogicOptimization();` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 849 | `&& chainBaseManager.getDynamicPropertiesStore().allowConsensusLogicOptimization()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 893 | `.supportShieldedTransaction()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 968 | `if (getDynamicPropertiesStore().supportBlackHoleOptimization()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1006 | `if (getDynamicPropertiesStore().supportBlackHoleOptimization()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1579 | `if (getDynamicPropertiesStore().supportVM()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1649 | `if (getDynamicPropertiesStore().getAllowMultiSign() == 1) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1810 | `if (getDynamicPropertiesStore().allowHardenExchangeCalculation()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1886 | `if (chainBaseManager.getDynamicPropertiesStore().allowConsensusLogicOptimization()` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1910 | `if (getDynamicPropertiesStore().getAllowAdaptiveEnergy() == 1) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1950 | `if (getDynamicPropertiesStore().allowChangeDelegation()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1955 | `if (chainBaseManager.getDynamicPropertiesStore().supportTransactionFeePool()) {` |
| `framework/src/main/java/org/tron/core/db/Manager.java` | 1972 | `if (chainBaseManager.getDynamicPropertiesStore().supportTransactionFeePool()) {` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 446 | `PARAMETER.allowCreationOfContracts = cc.getAllowCreationOfContracts();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 447 | `PARAMETER.allowMultiSign = (int) cc.getAllowMultiSign();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 448 | `PARAMETER.allowAdaptiveEnergy = cc.getAllowAdaptiveEnergy();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 449 | `PARAMETER.allowDelegateResource = cc.getAllowDelegateResource();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 450 | `PARAMETER.allowSameTokenName = cc.getAllowSameTokenName();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 451 | `PARAMETER.allowTvmTransferTrc10 = cc.getAllowTvmTransferTrc10();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 452 | `PARAMETER.allowTvmConstantinople = cc.getAllowTvmConstantinople();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 453 | `PARAMETER.allowTvmSolidity059 = cc.getAllowTvmSolidity059();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 455 | `PARAMETER.allowShieldedTRC20Transaction = cc.getAllowShieldedTRC20Transaction();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 456 | `PARAMETER.allowMarketTransaction = cc.getAllowMarketTransaction();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 457 | `PARAMETER.allowTransactionFeePool = cc.getAllowTransactionFeePool();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 458 | `PARAMETER.allowBlackHoleOptimization = cc.getAllowBlackHoleOptimization();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 459 | `PARAMETER.allowNewResourceModel = cc.getAllowNewResourceModel();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 460 | `PARAMETER.allowTvmIstanbul = cc.getAllowTvmIstanbul();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 461 | `PARAMETER.allowProtoFilterNum = cc.getAllowProtoFilterNum();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 462 | `PARAMETER.allowAccountStateRoot = cc.getAllowAccountStateRoot();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 464 | `PARAMETER.allowPBFT = cc.getAllowPbft();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 466 | `PARAMETER.allowTvmFreeze = cc.getAllowTvmFreeze();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 467 | `PARAMETER.allowTvmVote = cc.getAllowTvmVote();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 468 | `PARAMETER.allowTvmLondon = cc.getAllowTvmLondon();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 469 | `PARAMETER.allowTvmCompatibleEvm = cc.getAllowTvmCompatibleEvm();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 471 | `cc.getAllowHigherLimitForMaxCpuTimeOfOneTx();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 472 | `PARAMETER.allowNewRewardAlgorithm = cc.getAllowNewRewardAlgorithm();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 474 | `cc.getAllowOptimizedReturnValueOfChainId();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 475 | `PARAMETER.allowTvmShangHai = cc.getAllowTvmShangHai();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 476 | `PARAMETER.allowOldRewardOpt = cc.getAllowOldRewardOpt();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 477 | `PARAMETER.allowEnergyAdjustment = cc.getAllowEnergyAdjustment();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 478 | `PARAMETER.allowStrictMath = cc.getAllowStrictMath();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 480 | `PARAMETER.allowTvmCancun = cc.getAllowTvmCancun();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 481 | `PARAMETER.allowTvmBlob = cc.getAllowTvmBlob();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 484 | `PARAMETER.allowAccountAssetOptimization = cc.getAllowAccountAssetOptimization();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 485 | `PARAMETER.allowAssetOptimization = cc.getAllowAssetOptimization();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 486 | `PARAMETER.allowNewReward = cc.getAllowNewReward();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 488 | `PARAMETER.allowDelegateOptimization = cc.getAllowDelegateOptimization();` |
| `framework/src/main/java/org/tron/core/config/args/Args.java` | 489 | `PARAMETER.allowDynamicEnergy = cc.getAllowDynamicEnergy();` |
| `framework/src/main/java/org/tron/core/net/service/relay/RelayService.java` | 167 | `if (manager.getDynamicPropertiesStore().getAllowMultiSign() != 1) {` |
| `framework/src/main/java/org/tron/core/db/accountstate/callback/AccountStateCallBack.java` | 55 | `this.allowGenerateRoot = chainBaseManager.getDynamicPropertiesStore().allowAccountStateRoot();` |
| `framework/src/main/java/org/tron/core/net/messagehandler/PbftMsgHandler.java` | 36 | `if (!tronNetDelegate.allowPBFT()) {` |
| `framework/src/main/java/org/tron/core/net/messagehandler/FetchInvDataMsgHandler.java` | 114 | `if (!tronNetDelegate.allowPBFT() \|\| peer.isSyncFinish()) {` |
| `framework/src/main/java/org/tron/core/net/messagehandler/PbftDataSyncHandler.java` | 57 | `if (!chainBaseManager.getDynamicPropertiesStore().allowPBFT()) {` |
| `framework/src/main/java/org/tron/core/net/messagehandler/PbftDataSyncHandler.java` | 69 | `if (!chainBaseManager.getDynamicPropertiesStore().allowPBFT()) {` |
| `framework/src/main/java/org/tron/core/consensus/ProposalService.java` | 124 | `if (manager.getDynamicPropertiesStore().getAllowMultiSign() == 0) {` |
| `framework/src/main/java/org/tron/core/consensus/ProposalService.java` | 130 | `if (manager.getDynamicPropertiesStore().getAllowAdaptiveEnergy() == 0) {` |
| `framework/src/main/java/org/tron/core/consensus/ProposalService.java` | 189 | `//  if (manager.getDynamicPropertiesStore().getAllowShieldedTransaction() == 0) {` |
| `framework/src/main/java/org/tron/core/consensus/ProposalService.java` | 221 | `if (manager.getDynamicPropertiesStore().getAllowMarketTransaction() == 0) {` |
| `framework/src/main/java/org/tron/core/consensus/ProposalService.java` | 348 | `if (manager.getDynamicPropertiesStore().getAllowCancelAllUnfreezeV2() == 0) {` |
| `framework/src/main/java/org/tron/core/net/TronNetDelegate.java` | 383 | `return chainBaseManager.getDynamicPropertiesStore().allowPBFT();` |

## Java proposal-aware capsule/helper reads

These are especially important because callers may invoke a neutral-looking helper
while the helper internally switches consensus behavior by proposal state.

| File | Line | Check |
|---|---:|---|
| `chainbase/src/main/java/org/tron/core/capsule/BlockCapsule.java` | 196 | `if (dynamicPropertiesStore.getAllowMultiSign() != 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ExchangeCapsule.java` | 174 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 101 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 111 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/common/utils/Commons.java` | 124 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 707 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 738 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 753 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 785 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 800 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/AccountCapsule.java` | 855 | `if (dynamicStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ContractStateCapsule.java` | 84 | `dps.allowStrictMath(),` |
| `chainbase/src/main/java/org/tron/core/capsule/AssetIssueCapsule.java` | 123 | `if (dynamicPropertiesStore.getAllowSameTokenName() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 216 | `if (Objects.isNull(origin) && dynamicPropertiesStore.getAllowTvmConstantinople() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 245 | `if (dynamicPropertiesStore.getAllowTvmFreeze() == 1` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 246 | `\|\| dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 269 | `if (dynamicPropertiesStore.getAllowTvmFreeze() == 1` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 270 | `\|\| dynamicPropertiesStore.supportUnfreezeDelay()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 282 | `dynamicPropertiesStore.getAllowAdaptiveEnergy() == 1) {` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 304 | `if (dynamicPropertiesStore.supportTransactionFeePool() &&` |
| `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` | 307 | `} else if (dynamicPropertiesStore.supportBlackHoleOptimization()) {` |
| `chainbase/src/main/java/org/tron/core/capsule/DelegatedResourceCapsule.java` | 120 | `if (dynamicPropertiesStore.getAllowMultiSign() == 0) {` |
| `chainbase/src/main/java/org/tron/core/capsule/utils/AssetUtil.java` | 64 | `return dynamicPropertiesStore.supportAllowAssetOptimization();` |
| `chainbase/src/main/java/org/tron/core/capsule/utils/TransactionUtil.java` | 80 | `.getChainBaseManager().getDynamicPropertiesStore().supportTransactionFeePool();` |

## go-tron production proposal/fork reads

| File | Line | Check |
|---|---:|---|
| `consensus/dpos/verify.go` | 93 | `if dp.ConsensusLogicOptimization() {` |
| `consensus/dpos/verify.go` | 123 | `if dp.AllowMultiSign() {` |
| `consensus/dpos/verify.go` | 215 | `return dp.AllowFnDsa512()` |
| `consensus/dpos/verify.go` | 217 | `return dp.AllowMlDsa44()` |
| `actuator/undelegate_resource.go` | 27 | `if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/undelegate_resource.go` | 30 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `consensus/dpos/maintenance.go` | 21 | `if !chain.ChangeDelegation() {` |
| `actuator/unfreeze_balance.go` | 37 | `delegated := len(uc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource()` |
| `actuator/unfreeze_balance.go` | 56 | `if !ctx.DynProps.AllowNewResourceModel() {` |
| `actuator/unfreeze_balance.go` | 84 | `if !ctx.DynProps.AllowTvmConstantinople() && !ctx.State.AccountExists(receiverAddr) {` |
| `actuator/unfreeze_balance.go` | 107 | `if !ctx.DynProps.AllowMultiSign() {` |
| `actuator/unfreeze_balance.go` | 126 | `delegated := len(uc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource()` |
| `actuator/unfreeze_balance.go` | 167 | `if ctx.DynProps.AllowDelegateOptimization() {` |
| `actuator/unfreeze_balance.go` | 210 | `if ctx.DynProps.AllowTvmConstantinople() && (receiver == nil \|\| receiver.Type() == corepb.AccountType_Contract) {` |
| `actuator/unfreeze_balance.go` | 213 | `decrease = ctx.State.DecrementReceiverAcquired(receiverAddr, removed, uc.Resource, ctx.DynProps.AllowTvmSolidity059())` |
| `actuator/unfreeze_balance.go` | 216 | `if !ctx.DynProps.AllowNewReward() {` |
| `actuator/unfreeze_balance.go` | 226 | `if ctx.DynProps.AllowNewResourceModel() {` |
| `actuator/unfreeze_balance.go` | 242 | `if ctx.DynProps.AllowNewResourceModel() {` |
| `actuator/vm_actuator.go` | 38 | `if !ctx.DynProps.AllowCreationOfContracts() {` |
| `actuator/vm_actuator.go` | 92 | `if ctx.DynProps.AllowTvmTransferTrc10() && csc.CallTokenValue > 0 && ctx.State.GetTRC10Balance(owner, csc.TokenId) < csc.CallTokenValue {` |
| `actuator/vm_actuator.go` | 137 | `if ctx.DynProps.AllowTvmTransferTrc10() && tsc.CallTokenValue > 0 && ctx.State.GetTRC10Balance(owner, tsc.TokenId) < tsc.CallTokenValue {` |
| `actuator/vm_actuator.go` | 285 | `if ctx.DynProps.AllowTvmCompatibleEvm() {` |
| `actuator/vm_actuator.go` | 422 | `if !ctx.DynProps.AllowTvmTransferTrc10() \|\| !ctx.DynProps.AllowMultiSign() {` |
| `actuator/vm_actuator.go` | 528 | `if !ctx.State.AccountExists(origin) && ctx.DynProps.AllowTvmConstantinople() {` |
| `actuator/unfreeze_v2.go` | 30 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/unfreeze_v2.go` | 47 | `newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)` |
| `actuator/unfreeze_v2.go` | 86 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/unfreeze_v2.go` | 98 | `expireTime := ctx.PrevBlockTime + ctx.DynProps.UnfreezeDelayDays()*86_400_000` |
| `actuator/unfreeze_v2.go` | 107 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/unfreeze_v2.go` | 135 | `newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)` |
| `vm/instructions_tron.go` | 324 | `if in.tvm.DynProps != nil && in.tvm.DynProps.AllowMultiSign() {` |
| `vm/instructions_tron.go` | 598 | `if in.tvm.DynProps != nil && in.tvm.DynProps.SupportUnfreezeDelay() && in.tvm.DynProps.AllowNewResourceModel() {` |
| `vm/instructions_tron.go` | 844 | `return tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel()` |
| `vm/instructions_tron.go` | 893 | `if tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel() {` |
| `vm/instructions_tron.go` | 914 | `if tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel() {` |
| `vm/instructions_tron.go` | 1199 | `if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {` |
| `vm/instructions_tron.go` | 1255 | `if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {` |
| `vm/instructions_tron.go` | 1263 | `if in.tvm.DynProps != nil && in.tvm.DynProps.UnfreezeDelayDays() > 0 {` |
| `vm/instructions_tron.go` | 1264 | `delayDays = in.tvm.DynProps.UnfreezeDelayDays()` |
| `vm/instructions_tron.go` | 1272 | `if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {` |
| `vm/precompile_tron.go` | 1135 | `val = dp.UnfreezeDelayDays()` |
| `actuator/exchange_create.go` | 68 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `actuator/exchange_create.go` | 131 | `if !ctx.DynProps.AllowSameTokenName() \|\| bytes.Equal(tokenID, []byte("_")) {` |
| `actuator/update_brokerage.go` | 25 | `if !forks.IsActive(forks.AllowChangeDelegation, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/unfreeze_asset.go` | 42 | `if ctx.DynProps.AllowSameTokenName() {` |
| `actuator/unfreeze_asset.go` | 77 | `if ctx.DynProps.AllowSameTokenName() {` |
| `actuator/unfreeze_asset.go` | 84 | `if ctx.State.GetTRC10BalanceFinal(owner, name, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-amount {` |
| `actuator/unfreeze_asset.go` | 87 | `ctx.State.AddTRC10BalanceFinal(owner, name, tokenID, amount, ctx.DynProps.AllowSameTokenName())` |
| `actuator/delegate_resource.go` | 51 | `if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/delegate_resource.go` | 54 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/delegate_resource.go` | 103 | `if c.Lock && ctx.DynProps.SupportMaxDelegateLockPeriod() {` |
| `actuator/delegate_resource.go` | 105 | `maxLock := ctx.DynProps.MaxDelegateLockPeriod()` |
| `actuator/delegate_resource.go` | 172 | `lockPeriodBlocks := getLockPeriod(ctx.DynProps.SupportMaxDelegateLockPeriod(), c.LockPeriod)` |
| `vm/dynamic_energy.go` | 48 | `if cs.CatchUpToCycle(currentCycle, threshold, increaseFactor, maxFactor, dp.AllowStrictMath()) {` |
| `actuator/proposal_validation.go` | 66 | `if dp.AllowHigherLimitForMaxCpuTimeOfOneTx() {` |
| `actuator/proposal_validation.go` | 74 | `return validateProposalRequires(dp.AllowSameTokenName(), "allow_same_token_name", id)` |
| `actuator/proposal_validation.go` | 88 | `return validateProposalRequires(dp.AllowTvmTransferTrc10(), "allow_tvm_transfer_trc10", id)` |
| `actuator/proposal_validation.go` | 100 | `return validateProposalRequires(dp.AllowCreationOfContracts(), "allow_creation_of_contracts", id)` |
| `actuator/proposal_validation.go` | 107 | `return validateProposalRequires(dp.AllowCreationOfContracts(), "allow_creation_of_contracts", id)` |
| `actuator/proposal_validation.go` | 109 | `if err := validateProposalRequires(dp.AllowMarketTransaction(), "allow_market_transaction", id); err != nil {` |
| `actuator/proposal_validation.go` | 117 | `if value > proposalMarketFeeMax && !dp.AllowTvmLondon() {` |
| `actuator/proposal_validation.go` | 128 | `if err := validateProposalRequires(dp.AllowDelegateResource(), "allow_delegate_resource", id); err != nil {` |
| `actuator/proposal_validation.go` | 131 | `if err := validateProposalRequires(dp.AllowMultiSign(), "allow_multi_sign", id); err != nil {` |
| `actuator/proposal_validation.go` | 134 | `if err := validateProposalRequires(dp.AllowTvmConstantinople(), "allow_tvm_constantinople", id); err != nil {` |
| `actuator/proposal_validation.go` | 137 | `return validateProposalRequires(dp.AllowTvmSolidity059(), "allow_tvm_solidity059", id)` |
| `actuator/proposal_validation.go` | 142 | `return validateProposalRequires(dp.ChangeDelegation(), "change_delegation", id)` |
| `actuator/proposal_validation.go` | 148 | `if dp.AllowNewReward() {` |
| `actuator/proposal_validation.go` | 161 | `return validateProposalRequires(dp.ChangeDelegation(), "change_delegation", id)` |
| `actuator/proposal_validation.go` | 172 | `return validateProposalRequires(dp.UnfreezeDelayDays() != 0, "unfreeze_delay_days", id)` |
| `actuator/proposal_validation.go` | 174 | `if err := validateProposalRequires(dp.UnfreezeDelayDays() != 0, "unfreeze_delay_days", id); err != nil {` |
| `actuator/proposal_validation.go` | 177 | `if value <= dp.MaxDelegateLockPeriod() \|\| value > proposalOneYearBlockNumbers {` |
| `actuator/proposal_validation.go` | 178 | `return fmt.Errorf("bad chain parameter value for id %d, valid range is (%d,%d]", id, dp.MaxDelegateLockPeriod(), proposalOneYearBlockNumbers)` |
| `actuator/proposal_validation.go` | 182 | `if dp.AllowOldRewardOpt() {` |
| `actuator/proposal_validation.go` | 188 | `return validateProposalRequires(dp.UseNewRewardAlgorithm(), "allow_new_reward", id)` |
| `actuator/proposal_validation.go` | 190 | `if dp.AllowEnergyAdjustment() {` |
| `actuator/proposal_validation.go` | 197 | `if dp.AllowTvmCancun() {` |
| `actuator/proposal_validation.go` | 202 | `if dp.AllowStrictMath() {` |
| `actuator/proposal_validation.go` | 207 | `if dp.ConsensusLogicOptimization() {` |
| `actuator/proposal_validation.go` | 212 | `if dp.AllowTvmBlob() {` |
| `actuator/proposal_validation.go` | 222 | `if dp.AllowTvmSelfdestructRestriction() {` |
| `actuator/proposal_validation.go` | 227 | `if err := validateProposalRequires(dp.AllowTvmShanghai(), "allow_tvm_shanghai", id); err != nil {` |
| `actuator/proposal_validation.go` | 230 | `if dp.AllowTvmPrague() {` |
| `actuator/proposal_validation.go` | 235 | `if dp.AllowTvmOsaka() {` |
| `actuator/proposal_validation.go` | 240 | `if dp.AllowHardenResourceCalculation() {` |
| `actuator/proposal_validation.go` | 248 | `if dp.AllowHardenExchangeCalculation() == (value == 1) {` |
| `actuator/proposal_validation.go` | 256 | `if dp.AllowFnDsa512() == (value == 1) {` |
| `actuator/proposal_validation.go` | 264 | `if dp.AllowMlDsa44() == (value == 1) {` |
| `vm/tvm_config.go` | 71 | `return forks.IsActive(flag, blockNum, dp)` |
| `vm/tvm_config.go` | 76 | `higherLimit = dp.AllowHigherLimitForMaxCpuTimeOfOneTx()` |
| `vm/tvm_config.go` | 77 | `unfreezeDelay = dp.UnfreezeDelayDays() > 0` |
| `vm/tvm_config.go` | 98 | `FnDsa512:             dp != nil && dp.AllowFnDsa512(),` |
| `vm/tvm_config.go` | 99 | `MlDsa44:              dp != nil && dp.AllowMlDsa44(),` |
| `vm/tvm_config.go` | 106 | `OptimizedReturnValueOfChainId:   dp != nil && dp.AllowOptimizedReturnValueOfChainId(),` |
| `actuator/market_cancel_order.go` | 32 | `if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {` |
| `net/pbft_handler.go` | 180 | `// short-circuits every future call. Note that forks.IsActive(AllowPbft,` |
| `actuator/freeze_v2.go` | 27 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/freeze_v2.go` | 50 | `newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)` |
| `actuator/freeze_v2.go` | 79 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| `vm/tvm.go` | 307 | `if tvm.DynProps.AllowMultiSign() {` |
| `actuator/witness.go` | 73 | `if ctx.DynProps.AllowMultiSign() {` |
| `net/pbft_producer.go` | 284 | `return forks.IsActive(forks.AllowPbft, headNum, dp)` |
| `actuator/cancel_unfreeze.go` | 25 | `if !ctx.DynProps.SupportCancelAllUnfreezeV2() {` |
| `core/proposal.go` | 36 | `nileMarketDisable := isNile && forks.PassVersionFromStoreWithRate(statedb, 33, maintenanceTime, dynProps.MaintenanceTimeInterval(), 80)` |
| `core/energy_adaptive.go` | 37 | `if dp.AllowHardenResourceCalculation() {` |
| `core/energy_adaptive.go` | 72 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/reward.go` | 33 | `if !dp.ChangeDelegation() {` |
| `core/reward.go` | 68 | `if !dp.AllowTransactionFeePool() {` |
| `core/reward.go` | 104 | `// gtron exposes as dynProps.ConsensusLogicOptimization() — so we reuse` |
| `core/reward.go` | 152 | `if !dp.ChangeDelegation() {` |
| `core/reward.go` | 161 | `set = buildStandbyWitnessPaySet(db, statedb, cycle, dp.ConsensusLogicOptimization())` |
| `core/reward.go` | 269 | `if dp.UseNewRewardAlgorithm() {` |
| `core/reward.go` | 295 | `if !dp.ChangeDelegation() {` |
| `actuator/exchange_inject.go` | 92 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `actuator/exchange_inject.go` | 172 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `core/tx_validator.go` | 253 | `return dp.AllowFnDsa512()` |
| `core/tx_validator.go` | 255 | `return dp.AllowMlDsa44()` |
| `actuator/actuator.go` | 112 | `return forks.PassVersionFromStoreWithRate(ctx.State, 33, ctx.PrevBlockTime, ctx.DynProps.MaintenanceTimeInterval(), 80)` |
| `actuator/participate_asset_issue.go` | 90 | `if ctx.State.GetTRC10BalanceFinal(issuer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < tokenAmount {` |
| `actuator/participate_asset_issue.go` | 93 | `if ctx.State.GetTRC10BalanceFinal(buyer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-tokenAmount {` |
| `actuator/participate_asset_issue.go` | 126 | `if ctx.State.GetTRC10BalanceFinal(buyer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-tokenAmount {` |
| `actuator/participate_asset_issue.go` | 137 | `if err := ctx.State.SubTRC10BalanceFinal(issuer, c.AssetName, tokenID, tokenAmount, ctx.DynProps.AllowSameTokenName()); err != nil {` |
| `actuator/participate_asset_issue.go` | 140 | `ctx.State.AddTRC10BalanceFinal(buyer, c.AssetName, tokenID, tokenAmount, ctx.DynProps.AllowSameTokenName())` |
| `core/genesis.go` | 259 | `if dp.AllowTvmConstantinople() {` |
| `core/genesis.go` | 287 | `sortOpt = dp.ConsensusLogicOptimization()` |
| `actuator/exchange_store.go` | 14 | `if ctx.DynProps.AllowSameTokenName() {` |
| `actuator/exchange_store.go` | 24 | `if ctx.DynProps.AllowSameTokenName() {` |
| `core/bandwidth.go` | 100 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/bandwidth.go` | 105 | `if dp.UnfreezeDelayDays() > 0 {` |
| `core/bandwidth.go` | 132 | `if !dynProps.SupportUnfreezeDelay() {` |
| `core/bandwidth.go` | 142 | `harden := dynProps.AllowHardenResourceCalculation()` |
| `core/bandwidth.go` | 143 | `cancelAllV2 := dynProps.SupportCancelAllUnfreezeV2()` |
| `core/bandwidth.go` | 187 | `txSize := txBandwidthSize(tx, dynProps.AllowCreationOfContracts())` |
| `core/bandwidth.go` | 190 | `if dynProps.ConsensusLogicOptimization() {` |
| `core/bandwidth.go` | 255 | `if dynProps.AllowTransactionFeePool() {` |
| `core/bandwidth.go` | 259 | `if dynProps.AllowBlackHoleOptimization() {` |
| `core/bandwidth.go` | 327 | `if dynProps.AllowSameTokenName() {` |
| `core/bandwidth.go` | 355 | `if dynProps.AllowSameTokenName() {` |
| `core/bandwidth.go` | 367 | `if dynProps.AllowSameTokenName() {` |
| `core/bandwidth.go` | 393 | `if dynProps.AllowSameTokenName() {` |
| `core/bandwidth.go` | 441 | `if dynProps.AllowBlackHoleOptimization() {` |
| `actuator/clear_abi.go` | 30 | `if !ctx.DynProps.AllowTvmConstantinople() {` |
| `actuator/account.go` | 62 | `if ctx.DynProps.AllowMultiSign() {` |
| `actuator/account_permission.go` | 31 | `if !forks.IsActive(forks.AllowMultiSign, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/market_sell_asset.go` | 41 | `if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/market_sell_asset.go` | 226 | `if ctx.State.GetTRC10BalanceFinal(ownerAddr, tokenID, tid, ctx.DynProps.AllowSameTokenName()) < amount {` |
| `actuator/market_sell_asset.go` | 252 | `if ctx.State.GetTRC10BalanceFinal(addr, tokenID, tid, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-amount {` |
| `actuator/market_sell_asset.go` | 255 | `ctx.State.AddTRC10BalanceFinal(addr, tokenID, tid, amount, ctx.DynProps.AllowSameTokenName())` |
| `actuator/market_sell_asset.go` | 268 | `return ctx.State.SubTRC10BalanceFinal(addr, tokenID, tid, amount, ctx.DynProps.AllowSameTokenName())` |
| `actuator/fees.go` | 25 | `if forks.IsActive(forks.AllowBlackholeOptimization, ctx.BlockNumber, ctx.DynProps) {` |
| `core/state/dynamic_properties.go` | 666 | `if dp.AllowNewReward() && next < 0 {` |
| `core/state/dynamic_properties.go` | 678 | `if dp.AllowNewReward() && next < 0 {` |
| `core/state/dynamic_properties.go` | 690 | `if dp.AllowNewReward() && next < 0 {` |
| `core/state/dynamic_properties.go` | 708 | `return dp.UnfreezeDelayDays() > 0` |
| `core/state/dynamic_properties.go` | 716 | `return dp.AllowCancelAllUnfreezeV2() && dp.SupportUnfreezeDelay()` |
| `core/state/dynamic_properties.go` | 859 | `func (dp *DynamicProperties) AllowTvmShieldedToken() bool { return dp.AllowShieldedTrc20Transaction() }` |
| `core/state/dynamic_properties.go` | 891 | `func (dp *DynamicProperties) AllowStakingV2() bool     { return dp.AllowNewResourceModel() }` |
| `core/state/dynamic_properties.go` | 1230 | `if !dp.AllowAdaptiveEnergy() {` |
| `core/state/dynamic_properties.go` | 1320 | `return dp.MaxDelegateLockPeriod() > params.DelegatePeriod/params.BlockProducedInterval &&` |
| `core/state/dynamic_properties.go` | 1321 | `dp.UnfreezeDelayDays() > 0` |
| `actuator/transfer_asset.go` | 26 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/transfer_asset.go` | 51 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/transfer_asset.go` | 105 | `if ctx.State.GetTRC10BalanceFinal(from, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < c.Amount {` |
| `actuator/transfer_asset.go` | 110 | `if ctx.DynProps.ForbidTransferToContract() && toAccount.Type() == corepb.AccountType_Contract {` |
| `actuator/transfer_asset.go` | 113 | `if ctx.State.GetTRC10BalanceFinal(to, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-c.Amount {` |
| `actuator/transfer_asset.go` | 149 | `if ctx.State.GetTRC10BalanceFinal(from, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < c.Amount {` |
| `actuator/transfer_asset.go` | 155 | `if recipientExists && ctx.State.GetTRC10BalanceFinal(to, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-c.Amount {` |
| `actuator/transfer_asset.go` | 160 | `if ctx.DynProps.AllowMultiSign() {` |
| `actuator/transfer_asset.go` | 171 | `if err := ctx.State.SubTRC10BalanceFinal(from, c.AssetName, tokenID, c.Amount, ctx.DynProps.AllowSameTokenName()); err != nil {` |
| `actuator/transfer_asset.go` | 174 | `ctx.State.AddTRC10BalanceFinal(to, c.AssetName, tokenID, c.Amount, ctx.DynProps.AllowSameTokenName())` |
| `actuator/update_asset.go` | 48 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/update_asset.go` | 98 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/update_asset.go` | 133 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `core/forks/controller.go` | 237 | `// During Task 5 migration, existing forks.IsActive(flag, blockNum, dp)` |
| `actuator/withdraw_expire_unfreeze.go` | 26 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/freeze_balance.go` | 59 | `if !ctx.DynProps.AllowNewResourceModel() {` |
| `actuator/freeze_balance.go` | 69 | `if len(fc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource() {` |
| `actuator/freeze_balance.go` | 80 | `if ctx.DynProps.AllowTvmConstantinople() {` |
| `actuator/freeze_balance.go` | 87 | `if ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/freeze_balance.go` | 104 | `if ctx.DynProps.AllowNewResourceModel() {` |
| `actuator/freeze_balance.go` | 107 | `delegated := len(fc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource() && fc.Resource != corepb.ResourceCode_TRON_POWER` |
| `actuator/freeze_balance.go` | 142 | `if ctx.DynProps.AllowDelegateOptimization() {` |
| `actuator/freeze_balance.go` | 172 | `if dp.AllowNewReward() {` |
| `actuator/transfer.go` | 48 | `if ctx.DynProps.ForbidTransferToContract() && toAccount != nil {` |
| `actuator/transfer.go` | 53 | `if ctx.DynProps.AllowTvmCompatibleEvm() && toAccount != nil && toAccount.Type() == corepb.AccountType_Contract {` |
| `actuator/transfer.go` | 108 | `if ctx.DynProps.AllowMultiSign() {` |
| `actuator/energy_bill.go` | 140 | `if ctx.DynProps.AllowTransactionFeePool() && !outOfTime {` |
| `actuator/energy_bill.go` | 144 | `if ctx.DynProps.AllowBlackHoleOptimization() {` |
| `actuator/energy_bill.go` | 178 | `if dp == nil \|\| !dp.SupportUnfreezeDelay() {` |
| `actuator/energy_bill.go` | 186 | `harden := dp != nil && dp.AllowHardenResourceCalculation()` |
| `actuator/energy_bill.go` | 197 | `if dp != nil && !dp.AllowTvmFreeze() {` |
| `actuator/energy_bill.go` | 210 | `harden := dp.AllowHardenResourceCalculation()` |
| `actuator/energy_bill.go` | 211 | `cancelAllV2 := dp.SupportCancelAllUnfreezeV2()` |
| `actuator/energy_bill.go` | 266 | `if !ctx.State.AccountExists(originAddr) && ctx.DynProps.AllowTvmConstantinople() {` |
| `actuator/energy_bill.go` | 324 | `return ctx.DynProps.AllowTvmFreeze() \|\| ctx.DynProps.SupportUnfreezeDelay()` |
| `actuator/energy_bill.go` | 389 | `if dp == nil \|\| !dp.SupportUnfreezeDelay() {` |
| `actuator/energy_bill.go` | 393 | `harden := dp != nil && dp.AllowHardenResourceCalculation()` |
| `actuator/energy_bill.go` | 399 | `dp.AllowHardenResourceCalculation(), dp.SupportCancelAllUnfreezeV2())` |
| `actuator/energy_bill.go` | 446 | `if dp.AllowHardenResourceCalculation() {` |
| `core/reward/voter_reward.go` | 55 | `if dp.AllowOldRewardOpt() {` |
| `actuator/exchange_withdraw.go` | 101 | `if !exchangeWithdrawPreciseEnough(otherBalance, c.Quant, thisBalance, anotherTokenQuant, ctx.DynProps.AllowHardenExchangeCalculation()) {` |
| `actuator/exchange_withdraw.go` | 154 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `actuator/withdraw_reward.go` | 25 | `if dp == nil \|\| statedb == nil \|\| !dp.ChangeDelegation() {` |
| `actuator/withdraw_reward.go` | 89 | `if dp == nil \|\| statedb == nil \|\| !dp.ChangeDelegation() {` |
| `actuator/shielded_transfer.go` | 101 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/shielded_transfer.go` | 104 | `if !ctx.DynProps.AllowShieldedTransaction() {` |
| `actuator/shielded_transfer.go` | 465 | `if ctx.DynProps.AllowMultiSign() {` |
| `actuator/energy_precharge.go` | 53 | `if !ctx.DynProps.SupportUnfreezeDelay() {` |
| `actuator/energy_precharge.go` | 64 | `harden := ctx.DynProps.AllowHardenResourceCalculation()` |
| `actuator/energy_precharge.go` | 65 | `cancelAllV2 := ctx.DynProps.SupportCancelAllUnfreezeV2()` |
| `actuator/energy_precharge.go` | 116 | `cancelAllV2 := ctx.DynProps != nil && ctx.DynProps.SupportCancelAllUnfreezeV2()` |
| `core/blockchain.go` | 1097 | `if dynProps.ChangeDelegation() && dynProps.Witness127PayPerBlock() > 0 {` |
| `core/blockchain.go` | 1098 | `standbyPaySet = bc.cachedStandbyPaySet(statedb, dynProps.CurrentCycleNumber(), dynProps.ConsensusLogicOptimization())` |
| `core/blockchain.go` | 1228 | `sortOpt := dynProps.ConsensusLogicOptimization()` |
| `core/blockchain.go` | 1230 | `if !dynProps.ChangeDelegation() {` |
| `core/blockchain.go` | 2261 | `multiSigByAddress := forks.PassVersionFromStore(statedb, 27,` |
| `core/blockchain.go` | 2298 | `return dp.AllowShieldedTransaction() \|\| blockContainsShieldedTransfer(block)` |
| `core/blockchain.go` | 2342 | `return a.dynProps.ChangeDelegation()` |
| `actuator/asset_issue.go` | 47 | `if ctx.DynProps.AllowSameTokenName() && strings.ToLower(string(c.Name)) == "trx" {` |
| `actuator/asset_issue.go` | 50 | `if c.Precision != 0 && ctx.DynProps.AllowSameTokenName() && (c.Precision < 0 \|\| c.Precision > 6) {` |
| `actuator/asset_issue.go` | 140 | `if !forks.IsActive(forks.AllowSameTokenName, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/asset_issue.go` | 167 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/asset_issue.go` | 178 | `if !ctx.DynProps.AllowSameTokenName() {` |
| `actuator/asset_issue.go` | 212 | `if ctx.DynProps.AllowSameTokenName() {` |
| `core/block_energy_usage.go` | 32 | `if !dp.AllowAdaptiveEnergy() \|\| result.EnergyUsageTotal <= 0 {` |
| `actuator/vote.go` | 47 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/vote.go` | 101 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| `actuator/exchange_transaction.go` | 79 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `actuator/exchange_transaction.go` | 96 | `anotherTokenQuant, err := exchangeQuote(sellBalance, buyBalance, c.Quant, ctx.DynProps.AllowStrictMath(), harden)` |
| `actuator/exchange_transaction.go` | 139 | `harden := ctx.DynProps.AllowHardenExchangeCalculation()` |
| `actuator/exchange_transaction.go` | 140 | `receive, err := exchangeQuote(sellBalance, buyBalance, c.Quant, ctx.DynProps.AllowStrictMath(), harden)` |
| `actuator/account_update.go` | 38 | `if ctx.State.GetAccountName(ownerAddr) != "" && !ctx.DynProps.AllowUpdateAccountName() {` |
| `actuator/account_update.go` | 41 | `if ctx.State != nil && ctx.State.HasAccountNameIndex(c.AccountName) && !ctx.DynProps.AllowUpdateAccountName() {` |
| `core/delegation/usage.go` | 57 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/delegation/usage.go` | 58 | `cancelAllV2 := dp.AllowCancelAllUnfreezeV2()` |
| `core/delegation/usage.go` | 120 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/delegation/usage.go` | 121 | `cancelAllV2 := dp.AllowCancelAllUnfreezeV2()` |
| `core/delegation/usage.go` | 151 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/delegation/usage.go` | 152 | `cancelAllV2 := dp.AllowCancelAllUnfreezeV2()` |
| `core/delegation/usage.go` | 185 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/delegation/usage.go` | 186 | `cancelAllV2 := dp.AllowCancelAllUnfreezeV2()` |
| `core/block_builder.go` | 135 | `if dynProps.AllowAccountStateRoot() {` |
| `core/block_builder.go` | 145 | `if dynProps.AllowAdaptiveEnergy() {` |
| `core/block_builder.go` | 178 | `sorted := dpos.SortWitnessesByVotesWithOptimization(allWitnesses, dynProps.ConsensusLogicOptimization())` |
| `core/block_builder.go` | 179 | `if !dynProps.ChangeDelegation() {` |
| `core/block_builder.go` | 218 | `if dynProps.AllowAccountStateRoot() {` |
| `core/resource.go` | 28 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/resource.go` | 30 | `if dp.UnfreezeDelayDays() > 0 {` |
| `core/resource.go` | 154 | `return recoverUsageWithHarden(oldUsage, lastTime, now, dp != nil && dp.AllowHardenResourceCalculation())` |
| `core/tron_backend.go` | 235 | `cfg.MultiSigCheckV2 = forks.PassVersionFromStore(statedbCopy, 27,` |
| `core/tron_backend.go` | 237 | `cfg.CpuTimeGuard = forks.PassVersionFromStore(statedbCopy, 35,` |
| `core/tron_backend.go` | 303 | `tvmCfg.MultiSigCheckV2 = forks.PassVersionFromStore(statedbCopy, 27,` |
| `core/tron_backend.go` | 305 | `tvmCfg.CpuTimeGuard = forks.PassVersionFromStore(statedbCopy, 35,` |
| `core/tron_backend.go` | 975 | `harden := dp.AllowHardenResourceCalculation()` |
| `core/tron_backend.go` | 976 | `cancelAll := dp.SupportCancelAllUnfreezeV2()` |
| `core/tron_backend.go` | 983 | `if dp.SupportUnfreezeDelay() {` |
| `core/tron_backend.go` | 996 | `if dp.SupportUnfreezeDelay() {` |
| `core/tron_backend.go` | 1035 | `if dp.SupportMaxDelegateLockPeriod() {` |
| `core/tron_backend.go` | 1036 | `first.LockPeriod = dp.MaxDelegateLockPeriod()` |
| `core/tron_backend.go` | 1167 | `if !dp.AllowSameTokenName() {` |
| `core/tron_backend.go` | 1225 | `if !b.chain.DynProps().AllowSameTokenName() {` |
| `core/tron_backend.go` | 1240 | `if !b.chain.DynProps().AllowSameTokenName() {` |
| `core/state_processor.go` | 97 | `exchangeRejected = forks.PassVersionFromStoreWithRate(statedb, 34, prevBlockTime, dynProps.MaintenanceTimeInterval(), 70)` |
| `core/state_processor.go` | 107 | `validateResultSize := !trustTransactionRet \|\| dynProps.ConsensusLogicOptimization()` |
| `core/state_processor.go` | 120 | `if dynProps.ConsensusLogicOptimization() {` |
| `core/state_processor.go` | 542 | `if dynProps.ConsensusLogicOptimization() {` |
| `core/state_processor.go` | 568 | `info := buildTransactionInfo(tx, result, block.Number(), block.Timestamp(), dynProps.AllowTransactionFeePool())` |
| `core/state_processor.go` | 598 | `if dynProps.AllowAdaptiveEnergy() {` |
