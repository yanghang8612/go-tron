# 2026-04-27 — System-Test Findings & Compatibility Gaps

**背景**：基于当前实现对 8 个交易类型 flow（账户/资产/Freeze/Vote/Proposal/Exchange/Contract/Solidity-API）做端到端 HTTP build → sign → broadcast → confirm → state-side-effect 的扫描（脚本 `scripts/system_test_flows.sh`）。本文罗列所有偏离 java-tron 行为或破坏 SDK 兼容性的问题，并给出修复路径。

**严重等级**：
- **P0** — 主流 SDK（tronweb / tronpy / TronLink）按 java-tron HTTP 契约调用 gtron 即报错或静默写入错值。
- **P1** — gtron 自身能跑，但开发/测试体验明显劣于 java-tron。

---

## P0-1 · `protojson.Unmarshal` 直接解析 → bytes 字段被当作 base64

**位置**：`internal/tronapi/api_account.go:91`、`api_exchange.go:42/65/88/111/134/157`、`api_trc10.go:26/50`、`api.go:215` (broadcasttransaction)。

**症状**：handler 直接 `protojson.Unmarshal(body, &contractProto)`，但 protojson 把 proto `bytes` 字段（`owner_address`/`to_address`/`asset_name`/`description`/`url` …）解码为 **base64**。java-tron HTTP API 契约里这些字段一律是 **hex string**。

**复现**：
```bash
curl -s -X POST -d '{"owner_address":"41cd2a3d9f938e13cd947ec05abc7fe734df8dd826", "name":"464c5754ff",
 "abbr":"4654","total_supply":1000000,"trx_num":1,"num":10,
 "start_time":...,"end_time":...,"description":"666c6f77","url":"687474703a2f2f78","precision":0}' \
  http://localhost:18090/wallet/createassetissue
# → builder 返回 200，但 owner_address 被解为 base64("41cd...") 的 garbage bytes
# → broadcast 后被 actuator.Validate 拒：`owner account does not exist`
```

**影响 endpoint**：createAssetIssue、updateAsset、accountPermissionUpdate、exchangeCreate、exchangeInject、exchangeTransaction、exchangeWithdraw、marketSellAsset、marketCancelOrder、broadcasttransaction（外层 transaction proto 的内层 contract 也走这条路径）。

**修复**：每个 endpoint 改为定义和现有 `createTransaction` / `freezeBalance` 一样的 plain JSON struct（hex string 字段），用 `common.FromHex` 显式转 bytes，再装填到 contract proto。**禁止** 在 HTTP 路径上直接 `protojson.Unmarshal` 整个 contract proto。

---

## P0-2 · 部分 handler 用 `[]byte(stringField)` 而非 hex 解码

**位置**（confirmed）：
- `api_account.go:72` setAccountId.account_id
- `api_tx.go:28` transferAsset.asset_name
- `api_tx.go:53` participateAssetIssue.asset_name
- `api_tx.go:75` createWitness.url
- `api_tx.go:127` updateWitness.update_url
- `api.go:1055` getAssetIssueByName.value
- `api.go:1185` getMarketPriceByPair.sell_token_id / buy_token_id

**症状**：handler 收到例如 `"account_id":"6d796e616d653031"`（hex of "myname01"），直接 `[]byte(body.AccountID)` 写入 32 个 ASCII 字符 `'6','d','7','9'…`，而 java-tron 应该写入 8 字节 `myname01`。**Tx 在 chain 上看起来"成功"，但写了错误数据**——这是最危险的一类 silent-corruption bug。

**修复**：把 `[]byte(body.X)` 全部改为 `common.FromHex(body.X)`。

**反例（保留）**：`triggerSmartContract.function_selector` (`api.go:327`) 走 `Keccak256([]byte(...))` 计算 4-byte selector——java-tron 这个字段就是 ASCII signature，**不应改**。

---

## P0-3 · Freeze/Delegate handler 把 `resource` 当作 int32

**位置**：`api_tx.go:178/205/230/254/278/299/326` 等。

**症状**：handler `Resource int32` 严格期望数字（0/1/2）；发 `{"resource":"BANDWIDTH"}` 直接 HTTP 400 `invalid request`。java-tron HTTP API 同时接受字符串和数字（兼容旧 SDK 与 enum-typed proto）。

**修复**：定义 `type ResourceField int32`，`UnmarshalJSON` 同时识别数字和 `"BANDWIDTH"/"ENERGY"/"TRON_POWER"`。

---

## P0-4 · `proposalCreate` 期望 `parameters` 是对象，java-tron 用数组

**位置**：`internal/tronapi/api.go:730` `Parameters map[string]int64 \`json:"parameters"\``

**症状**：java-tron HTTP API 用 `"parameters":[{"key":3,"value":17000000}]`，gtron 期望 `"parameters":{"3":17000000}`。前者直接 HTTP 400。

**修复**：handler 接受 `[]struct{Key int64; Value int64}` 并转 `map[int64]int64` 后传给 backend。可同时保留对象形式向后兼容（不必，java-tron 不支持）。

---

## P0-5 · `walletsolidity/*` 永远返回 genesis（`latest_solidified_block_num` 从未更新）

**位置**：`core/state/dynamic_properties.go:599 SetLatestSolidifiedBlockNum` 仅在 test 中被调用，**没有任何生产路径**写入它。`core/tron_backend.go:90 SolidifiedBlockNum()` 永远读到 0，导致 `walletsolidity/getnowblock` 返回空块或 genesis。

**症状**：M8.1 整套 solidity 端点不可用；任何依赖 solid head 的 SDK 调用均报"无块"。

**修复**：移植 java-tron `Manager.updateSolidifiedBlock()`：
1. 在 `BlockChain.InsertBlock` 末尾或单独 hook：取 active SR 集合的 `latest_block_header.raw_data.number`（每个 SR 各自最新出块号），排序，取第 `len(srs)*2/3 + 1`-th 小（即 `SOLIDIFIED_THRESHOLD` 之上的那位）。
2. `dynProps.SetLatestSolidifiedBlockNum(value)`；commit dynamic properties。
3. 单见证人 dev 链：solid 应等于当前 head（active set 大小为 1，2/3+1 = 1，取唯一项）。

**单测**：在 `dynamic_properties_test.go` / `blockchain_test.go` 加：插入 5 块，断言 `SolidifiedBlockNum() == head.Number()`（单 SR 场景）；模拟 27 SR 场景断言取第 19th smallest。

---

## P1-6 · `producer.BuildBlock` 丢 tx 时无原因可见 ✅ 已修

**位置**：`core/block_builder.go:65` 旧版 `continue` 直接吞错误。

**修复（已落盘）**：在丢弃前 `log.Printf("BuildBlock: skipping tx %x: %v", h[:8], err)`。后续观察：所有 EVICTED tx 现在都有可读的 actuator.Validate 错误。

---

## P1-7 · dev 模式默认禁用大部分 proposal flag

**位置**：`cmd/gtron/config.go:58 makeDevGenesis` 只在 `DynamicProperties` 里设了 5 个 key（`maintenance_time_interval`/`transaction_fee`/`witness_pay_per_block`/`witness_standby_allowance`/`total_net_limit`），其他 80+ 个 `allow_*` flag 全是 0。

**症状**：dev 节点上无法直接测：
- Staking V2 / Delegate / Cancel V2 / Withdraw expire（`allow_new_resource_model=0`、`allow_delegate_resource=0`）
- Multi-sign 权限（`allow_multi_sign=0`）
- Witness brokerage 更新（`allow_change_delegation=0`）
- TVM 任何特性（`allow_tvm_*` 全 0）
- Market、Exchange 的部分参数

**修复**：
- 加 `--dev.full-features`（默认 true）：在 `makeDevGenesis` 里把所有"主网已激活、风险无副作用"的 allow flag 直接设为激活值。
- 单独 `--dev.maintenance-interval <ms>`（默认 21600000）：dev 链改为 30000 时可在 30s 内完成一次维护周期，覆盖 reward distribution / maintenance proposal 激活。
- `--dev.witness-count <N>`：未来支持多见证人 dev 链。

---

## P1-8 · `/wallet/broadcasttransaction` 验证太浅，与 java-tron 不一致

**症状**：当前 broadcast 仅做 sig + ref-block 检查，actuator.Validate 等业务校验留给 producer。java-tron `Wallet#broadcastTransaction` 在广播前会调 `Manager.pushTransaction → ChainBaseManager.processTransaction → actuator.validate`，把业务错误同步返回。我们的 broadcast 永远 result.true，导致 SDK 收到"成功"但 tx 实际被静默丢弃。

**复现**：见 system_test_flows 全部 EVICTED 条目。

**修复**：在 `broadcasttransaction` handler 里在 push 到 pool 之前调一次 actuator.Validate（用 read-only StateDB 即可）；返回 `code=CONTRACT_VALIDATE_ERROR, message=<reason>`；继续 push 到 pool 也无所谓——producer 仍会再次校验。

---

## P2-9 · `updateSetting` / `updateEnergyLimit` HTTP endpoint 未注册

**位置**：`internal/tronapi/api.go` 中无 `/wallet/updatesetting` 与 `/wallet/updateenergylimit` 注册。actuator 实现都在 `actuator/update_setting.go` / `update_energy_limit.go` 里——**只是少了路由**。

**修复**：在 M5.1 PR-4 cluster 里补两个路由（handler 仿照 `clearABI` 即可）。

---

## P2-10 · `F8 deployContract` 在 trigger info 里 contractRet 显示运行时字节码而非 SUCCESS

**位置**：`gettransactioninfobyid` 对 deploy tx 把 runtime bytecode 写入了 `contractResult[0]`，java-tron 同样行为——这是**正确的**。脚本判定逻辑要修：deploy tx 应只看 `receipt.result == SUCCESS`，不要拿 contractResult[0] 与字面 "SUCCESS" 比较。

**修复**：测试脚本侧改 `run_tx`：deploy 类 tx（contract type == CreateSmartContract）跳过 contractResult[0] 检查。

---

## 测试产物

**保留并入仓**：
- `scripts/system_test_flows.sh` — 8 flow 端到端 smoke。修完 P0-1 ~ P0-4 后期望通过率应跃升至 ≥85%；当前基线（不修任何 bug）= 11/34 PASS。
- `core/block_builder.go` 加的 `BuildBlock: skipping tx %x: %v` 日志（P1-6 修复）。

**测试数据基线**（2026-04-27 18:30 本地，34 项断言）：
- ✅ PASS: 11 — 转账、createAccount、setAccountId（实际写入 corrupted ID）、updateWitness（同样 corrupted url）、F5 query、F8 deploy、F9 整套
- ⚠️ WARN: 21 — 大部分 P0 bug 暴露
- ❌ FAIL: 1 — walletsolidity stuck-at-0
- ❓ SKIP: 1 — F5 reward distribution（要 dev maintenance flag）

---

## 修复编排（建议拆为 M9 里程碑）

**M9 · HTTP API 兼容性 + dev 模式硬化**（P0/P1）

- M9.1 P0-1 删除所有 protojson.Unmarshal-on-contract 路径，改 plain JSON + hex（约 9 个 endpoint）+ 单测断言 hex 解码正确
- M9.2 P0-2 把 `[]byte(stringField)` 全部换成 `common.FromHex(stringField)` + 单测覆盖
- M9.3 P0-3 `ResourceField` 自定义 JSON unmarshal（同时接受 string/int），所有 freeze/delegate handler 复用
- M9.4 P0-4 `proposalCreate` 接受 `parameters` 数组形式，handler 转换
- M9.5 P0-5 移植 `Manager.updateSolidifiedBlock` 到 `BlockChain.InsertBlock`；blockchain_test 加单测
- M9.6 P1-7 `--dev.full-features` 与 `--dev.maintenance-interval` flag；`makeDevGenesis` 重构
- M9.7 P1-8 `broadcasttransaction` 同步 actuator.Validate 校验
- M9.8 P2-9 注册 `updateSetting` / `updateEnergyLimit` HTTP route
- M9.9 把 `scripts/system_test_flows.sh` 接入 `make` 与 CI 作为回归 smoke

**退出**：在 dev 模式下，`scripts/system_test_flows.sh` PASS ≥ 30 / WARN ≤ 4。
