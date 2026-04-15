# Fork-Gate Audit

Generated: 2026-04-15T10:23:48Z
java-tron:  /Users/asuka/Projects/tron/java-tron
go-tron:    /Users/asuka/Projects/asuka/go/go-tron

## (a) Execution-path gates in java-tron actuators

Source: `actuator/src/main/java/org/tron/core/actuator/*Actuator.java`

| File | Line | Check |
|---|---|---|
| ShieldedTransferActuator.java | 219 | `if (!dynamicStore.supportShieldedTransaction()) {` |
| UnfreezeBalanceActuator.java | 81 | `if (dynamicStore.supportAllowNewResourceModel()` |
| UnfreezeBalanceActuator.java | 90 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| UnfreezeBalanceActuator.java | 163 | `if (!dynamicStore.supportAllowDelegateOptimization()) {` |
| UnfreezeBalanceActuator.java | 243 | `long weight = dynamicStore.allowNewReward() ? decrease : -unfreezeBalance / TRX_PRECISION;` |
| UnfreezeBalanceActuator.java | 263 | `if (dynamicStore.supportAllowNewResourceModel()` |
| UnfreezeBalanceActuator.java | 288 | `if (dynamicStore.supportAllowNewResourceModel()` |
| UnfreezeBalanceActuator.java | 339 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| UnfreezeBalanceActuator.java | 460 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| UnfreezeBalanceActuator.java | 473 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| UnfreezeBalanceV2Actuator.java | 78 | `if (dynamicStore.supportAllowNewResourceModel()` |
| UnfreezeBalanceV2Actuator.java | 91 | `if (dynamicStore.supportAllowNewResourceModel()` |
| UnfreezeBalanceV2Actuator.java | 119 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| UnfreezeBalanceV2Actuator.java | 157 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| UnfreezeBalanceV2Actuator.java | 166 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| UnfreezeBalanceV2Actuator.java | 312 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| UnfreezeBalanceV2Actuator.java | 345 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| CancelAllUnfreezeV2Actuator.java | 130 | `if (!dynamicStore.supportAllowCancelAllUnfreezeV2()) {` |
| UpdateBrokerageActuator.java | 69 | `if (!dynamicStore.allowChangeDelegation()) {` |
| FreezeBalanceActuator.java | 64 | `if (dynamicStore.supportAllowNewResourceModel()` |
| FreezeBalanceActuator.java | 83 | `&& dynamicStore.supportDR()) {` |
| FreezeBalanceActuator.java | 99 | `&& dynamicStore.supportDR()) {` |
| FreezeBalanceActuator.java | 136 | `long weight = dynamicStore.allowNewReward() ? increment : frozenBalance / TRX_PRECISION;` |
| FreezeBalanceActuator.java | 221 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| FreezeBalanceActuator.java | 233 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| FreezeBalanceActuator.java | 245 | `if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {` |
| FreezeBalanceActuator.java | 271 | `if (dynamicStore.supportUnfreezeDelay()) {` |
| FreezeBalanceActuator.java | 320 | `if (!dynamicPropertiesStore.supportAllowDelegateOptimization()) {` |
| DelegateResourceActuator.java | 69 | `long lockPeriod = getLockPeriod(dynamicStore.supportMaxDelegateLockPeriod(),` |
| DelegateResourceActuator.java | 118 | `if (!dynamicStore.supportDR()) {` |
| DelegateResourceActuator.java | 122 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| DelegateResourceActuator.java | 212 | `if (lock && dynamicStore.supportMaxDelegateLockPeriod()) {` |
| CreateAccountActuator.java | 52 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| AssetIssueActuator.java | 89 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| VoteWitnessActuator.java | 131 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| VoteWitnessActuator.java | 166 | `if (dynamicStore.supportAllowNewResourceModel()` |
| ExchangeTransactionActuator.java | 70 | `dynamicStore.allowStrictMath());` |
| ExchangeTransactionActuator.java | 210 | `dynamicStore.allowStrictMath());` |
| UnDelegateResourceActuator.java | 205 | `if (!dynamicStore.supportDR()) {` |
| UnDelegateResourceActuator.java | 209 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| TransferActuator.java | 61 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| FreezeBalanceV2Actuator.java | 52 | `if (dynamicStore.supportAllowNewResourceModel()` |
| FreezeBalanceV2Actuator.java | 107 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| FreezeBalanceV2Actuator.java | 148 | `if (!dynamicStore.supportAllowNewResourceModel()) {` |
| FreezeBalanceV2Actuator.java | 154 | `if (dynamicStore.supportAllowNewResourceModel()) {` |
| WithdrawExpireUnfreezeActuator.java | 84 | `if (!dynamicStore.supportUnfreezeDelay()) {` |
| WitnessCreateActuator.java | 143 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| ExchangeCreateActuator.java | 120 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| MarketCancelOrderActuator.java | 99 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| MarketCancelOrderActuator.java | 169 | `if (!dynamicStore.supportAllowMarketTransaction()) {` |
| MarketSellAssetActuator.java | 129 | `if (dynamicStore.supportBlackHoleOptimization()) {` |
| MarketSellAssetActuator.java | 182 | `if (!dynamicStore.supportAllowMarketTransaction()) {` |
| TransferAssetActuator.java | 87 | `if (dynamicStore.supportBlackHoleOptimization()) {` |

## (b) Execution-path gates in go-tron actuators / VM

Source: `actuator/*.go`, `vm/*.go`, `core/*.go`

| File | Line | Check |
|---|---|---|
| actuator/freeze_balance.go | 33 | `if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/account_permission.go | 27 | `if !forks.IsActive(forks.AllowMultiSign, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/cancel_unfreeze.go | 27 | `if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/market_cancel_order.go | 33 | `if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/delegate_resource.go | 28 | `if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/update_brokerage.go | 27 | `if !forks.IsActive(forks.AllowChangeDelegation, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/unfreeze_v2.go | 29 | `if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/asset_issue.go | 82 | `if !forks.IsActive(forks.AllowSameTokenName, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/shielded_transfer.go | 41 | `if !ctx.DynProps.AllowShieldedTransaction() {` |
| actuator/withdraw_expire_unfreeze.go | 26 | `if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/market_sell_asset.go | 35 | `if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/freeze_v2.go | 27 | `if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {` |
| actuator/undelegate_resource.go | 28 | `if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {` |
| vm/tvm_config.go | 30 | `return forks.IsActive(flag, blockNum, dp)` |
| core/forks/controller.go | 126 | `// During Task 5 migration, existing forks.IsActive(flag, blockNum, dp)` |

## Proposal-validation gates (deferred — M4)

java-tron's ProposalUtil.java contains ~61 `forkController.pass` calls
governing which proposals may be submitted given the current software
version. These are NOT execution-path gates; they only matter once
go-tron exposes `/wallet/createproposal`. Count (for backlog):

- ProposalUtil.java: 61 pass() callsites
