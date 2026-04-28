# go-tron ↔ java-tron 兼容性开发计划

本计划把 [TODO.md](./TODO.md) 中识别的差距编排成可执行里程碑。每个里程碑包含范围、交付物、可验证退出条件、依赖关系、以及指向 `docs/superpowers/specs/` 与 `docs/superpowers/plans/` 的详细设计文档（存在则引用，不存在则在对应里程碑启动时创建）。

**文档约定**：沿用仓库既有规范——每个里程碑启动前，先写一份 `YYYY-MM-DD-<name>-design.md` spec（入 `docs/superpowers/specs/`），再写一份 `YYYY-MM-DD-<name>.md` plan（入 `docs/superpowers/plans/`，任务按 `- [ ]` checkbox 展开）。本 PLAN.md 只做顶层编排，不替代分阶段的详细 plan。

**执行原则**
1. **可测量优先**：任何状态机/共识级改动必须先有能捕捉分叉的测试框架（M0），再动核心逻辑。
2. **小步提交**：每个里程碑拆为 ≤5 个 PR，每个 PR 自带新增单测与（适用时）对 java-tron 的交叉测试。
3. **不破坏既有交互**：P2P 已验证握手/块同步可用（`docs/dev/p2p-interop-status.md`），所有涉及 P2P 的改动都必须在合入前重跑 `JAVA_TRON_ADDR=… go test -tags=integration ./p2p/`。
4. **分叉门控优先于功能**：任何新 DP key / 新能力，都先接 `forks.IsActive()` 门控，再填逻辑，避免激活状态下旧代码被误触发。
5. **java-tron 即真相**：遇到规范与 go-tron 现状冲突，以 java-tron 为准；更新 go-tron。

**全局退出门**

| 门 | 定义 | 依赖里程碑 |
|---|---|---|
| G1 **可跟链** | 本地 gtron 能连入主网，持续同步 7×24h 无 state root 分叉 | M0′ + M1 + M2 + M3 + M0″ |
| G2 **生态可用** | 主流 TRON 钱包/浏览器 SDK 可直接指向 gtron HTTP+gRPC 端点正常工作 | M4 + M5 |
| G3 **可当验证人** | 持 SR 私钥的 gtron 节点可加入 PBFT quorum 出块 | G1 + M6 |
| G4 **主网前置就绪** | 所有 P0/P1 关闭，TVM Cancun 激活前已有对应支持 | G1+G2+G3 + M7 + M8 |

---

## M0′ · Fixture 抽取工具 — **轻前置**

**背景**：现状离 java-tron 还差太多特性（§1），直接跑完整 mainnet 回放只会在第 1 个块就分叉、无信号可言。改为先做最小的 fixture 工具，让每个 M1 子里程碑在写代码时就有 java-tron 产出的 golden 数据作为对照。

**范围**：从一个本地 java-tron 节点按脚本化场景抽取 post-state 快照（JSON），作为 go-tron 单测的 oracle。**不做** state trie 比对，**不做** 活回放。

**交付物**
1. `scripts/fixtures/run.sh` 入口 + `lib/` 下的 java-tron 启停、API 封装、dump 助手脚本。
2. `test/fixtures/` 目录约定：每个场景一个子目录，含 `setup.sh`（构交易）、`run.sh`（广播等待确认）、`dump.sh`（抓取 state）、`README.md`、`fixture.json`（携带 java-tron 版本哈希）。
3. 两个种子场景：`00-genesis-dp`（全量链参数 dump）、`01-mainnet-dp`（主网 config 初始化后的参数 dump）。这两个已足以给 M1.1 使用。
4. `test/fixtures/load.go`：Go 侧载入帮助函数，供所有 M1 子里程碑的测试复用。
5. `docs/dev/fixture-tooling.md`：使用说明。

**退出条件**
- `./scripts/fixtures/run.sh 00-genesis-dp` 能从零跑到 fixture 落盘。
- `go test ./test/fixtures -run TestLoadFixture` 通过。
- fixture.json 的 schema 稳定，后续场景只新增不改结构。

**依赖**：无。

**文档**：`docs/superpowers/specs/2026-04-15-fixture-extraction-design.md` + plan。

---

## M0″ · 完整一致性回放 — **Phase 1 完成；Phase 2 待操作员 (2026-04-17)**

本里程碑天然分两阶段：

### Phase 1 · 引擎 + CLI + 捕获工具 + 文档  ✅ 完成

**交付物**
1. `core/conformance/` — 纯 Go 回放引擎：seed loader、DigestB/DigestC（确定性 sha256 + 人类可读 JSON）、allowlist（带 stale 检测）、Report、ReplayRange、Snapshot 序列化。22 个单测全绿。
2. `cmd/gtron-replay` — 薄 CLI wrapper，退出码 0/1/2/3 按 spec §5.4。
3. `cmd/fixture-closure` — 从 blocks.bin 按 ContractType 提取 touched-address 闭包。
4. `cmd/fixture-digest` — 从 java-tron 状态快照算 OracleEntry。
5. `scripts/conformance_replay.sh` + `make conformance-replay` — 顶层编排。
6. `test/fixtures/mainnet-blocks/smoke/` — 5 块合成 range（由 `go run ./scripts/fixtures/cmd/gen-smoke` 再生）；`make conformance-replay` 绿。
7. `docs/dev/conformance-harness.md` — 运行说明 + Phase 2 操作员协议（prune + replay-and-snapshot + snapshot JSON schema + allowlist 策略）。

### Phase 2 · 录真实语料 + cross e2e — 操作员任务

**前置**：需要访问本地 mainnet-synced java-tron 节点（个人/运维持有）；Phase 1 产物已 merge。

**交付物**
1. `test/fixtures/mainnet-blocks/range-freeze-v2/`、`range-maintenance/`、`range-contract/` — 3 段 500 块级别的真实区间（具体高度在录入时定）。
2. 每个 range 的初版 `divergence-allowlist.json`：catalog 所有已知 parity gap（reward v2 VI timing、freeze-v2 window、lock/unlock key split），每条带 `trackingIssue`。
3. `scripts/system_test_cross.sh` — 1 gtron + 1 java-tron 双节点 e2e。

**退出条件**：`make conformance-replay-exit-gate` 返回 0（allowlist 全空、无 stale）—— 作为 G1 的准入检查。在那之前 M0″ 整体"未完成"。

**依赖**：Phase 1；java-tron 操作员访问。

---

## M1 · 状态分叉杀手 — **P0，G1 必经**

**范围**：TODO §1.1 ~ §1.10 中所有"激活后就分叉"的项。

**子里程碑（顺序敏感）**

### M1.1 DynamicProperties 全量 backfill
- 按 TODO §1.8 清单向 `core/state/dynamic_properties.go` 补齐缺失 key（资源、奖励、费率、TVM 标志、系统簿记、Freeze V2 窗口、公共带宽池）。
- 每个 key 带默认值、getter/setter、单测；默认值与 java-tron `DynamicPropertiesStore` 初始值逐一对照。
- 同步在 `core/forks/forks.go` 的 `ProposalParamKey` 映射补齐 proposal ID → key。
- **退出**：`core/state/dynamic_properties.go` 的 key 列表与 java-tron `DynamicPropertiesStore` 公开字段一一对应；`params/mainnet.go` 和 `params/nile.go` 初始化值与 config.conf 匹配；M0 回放语料中由"缺 key"导致的分叉点归零。

### M1.2 Freeze V1 legacy 路径
- `core/bandwidth.go`、`core/resource.go` 支持 `GetFrozenAmount`（V1 格式）与 V2 并存的消费逻辑。
- 补 V1 解冻延迟记账（`LatestUnfrozenTime` 等）。
- 新增单测覆盖纯 V1、纯 V2、混合账户。
- **退出**：M0 回放覆盖的 V1 激活前历史区块 state root 一致。

### M1.3 分叉版本位与激活门
- 在区块头里携带 SR 版本位（参考 `ForkController.statsByVersion`）；`consensus/dpos` 产块时写入，校验时累计。
- `core/forks/forks.go` 增加 `IsActiveByVersion()`：基于最近 N 块的版本投票决定激活，而不是仅靠高度。
- 对全部 actuator 和 VM 操作码做 `forks.IsActive()` 调用点 audit（参考 java-tron 的 `forkController.pass(...)` 调用点清单），缺的补上。
- **退出**：`core/forks/forks_audit_test.go` 扫一遍所有 actuator/opcode，断言凡是 java-tron 有 `forkController.pass()` 的位置 go-tron 也调用了 `IsActive()`。

### M1.4 自适应能量上限（TIP-341） ✅ 完成
- `core/energy_adaptive.go`：移植 java-tron `EnergyProcessor.updateAdaptiveTotalEnergyLimit`（收缩 99/100、扩张 1000/999）与 `updateTotalEnergyAverageUsage`（20 块滑动窗口均值）。
- `ProcessBlock` 每块末尾在 `allow_adaptive_energy` 激活时执行调整。
- `availableAccountEnergy` 改用 `TotalEnergyCurrentLimit`（动态值）。
- `SetTotalEnergyLimit` 移植 `saveTotalEnergyLimit2` 副作用。提案 #21 激活时 version ≥ 3.6.5 自动调整 ratio→2880、multiplier→50。
- 补缺 DP key：`total_energy_average_time`、`block_energy_usage`。`ForkController` 集成到 `BlockChain`。

### M1.5 奖励算法 v2 & 委托奖励 ✅ 完成
- `core/rawdb/accessors_reward.go` + 新前缀 `dl-`：per-cycle 奖励池 / 投票快照 / VI / brokerage / 投票人 begin/endCycle 游标。
- `core/reward.go`：`payBlockReward`（brokerage 拆分 → witness 佣金入 allowance，voter pool 累积到 DelegationStore），`payStandbyWitness`（top-127 按票按比例分），`accumulateWitnessVi`（reward × 10^18 / voteCount），`applyRewardMaintenance`（维护点执行 VI 累积 + cycle rollover + brokerage/vote 快照）。
- `core/reward/voter_reward.go`：`ComputeVoterReward` 支持 old 和 new 两种算法，按 `new_reward_algorithm_effective_cycle` 分段。
- `actuator/withdraw_reward.go` + `VoteWitnessActuator.Execute` 改造：投票变更前先结算投票人奖励，保留 java-tron 不变量。
- 提案 #67（ALLOW_NEW_REWARD）激活时设置 `new_reward_algorithm_effective_cycle = currentCycle + 1`。
- `WithdrawBalanceActuator` 去掉 `IsWitness` 门控，支持投票人提现。
- **验证完毕**：java-tron `WithdrawBalanceActuator.validate()` 无 `isWitness` 检查——任何账户只要有 allowance 或投票奖励均可提现。go-tron 实现与之一致，无需额外门控。

### M1.6 存储租金（StorageTaxProcessor） ✅ 完成（stub）
- java-tron 存储市场功能（BuyStorage/SellStorage）在主网从未被激活；对应的 actuator 实现在 java-tron 当前代码中不存在。
- 向 `core/state/dynamic_properties.go` 补齐 4 个缺失的 DP key（`total_storage_pool`、`total_storage_tax`、`total_storage_reserved`、`storage_exchange_tax_rate`），默认值与 java-tron `DynamicPropertiesStore` 初始值一致。
- 不实现 StorageTaxProcessor 逻辑——该功能从未触发，M0 回放退出条件天然满足（所有账户 storage 字段均为 0）。

### M1.7 动态能量价格（TIP-1327） ✅ 完成
- `core/types/contract_state.go`：ContractState 包装器 + `CatchUpToCycle`（上界 clamp maxFactor、下界 floor 0、衰减基数 `1 - increaseFactor/4/decimal`）。
- `core/rawdb/accessors_contract_state.go` + 新前缀 `cs-`：per-contract 状态存储，映射 java-tron ContractStateStore。
- `vm/dynamic_energy.go`：`updateContractEnergyFactor` 在 Contract 入口取因子；`applyDynamicEnergyPenalty` 按 `cost × factor / decimal` 计算加价；`recordContractEnergyUsage` 在 return/revert 时累加**未 scaling 的基础用量**到 energy_usage，供下次 threshold 比较使用。
- `vm/interpreter.go Run()`：每条 opcode 先扣基础 cost，若 factor > 1.0× 则加 penalty；`rawEnergyUsed` 累加基础用量在收尾写回。

### M1.8 Freeze-V2 委托资源消费账 ✅ 完成
- 新包 `core/delegation`：`TransferUsageFromReceiver`、`FoldUsageIntoOwner`、`RecoverUsageWindow`。actuator 和 vm 共用避免循环依赖。
- `actuator/delegate_resource.go`：委托前刷新委托方 usage。
- `actuator/undelegate_resource.go`：解委托时先恢复接收方 usage，按解委托比例（capped by `unDelegateBalance/TRX × totalLimit/totalWeight`）扣除并折入委托方 usage。
- `vm/instructions_tron.go`：opcode `0xDE DELEGATERESOURCE` 和 `0xDF UNDELEGATERESOURCE` 走同一套 helper。
- `core/state/statedb.go` 补缺 `SetLatestConsumeTimeForEnergy`。
- 已知差异：go-tron 用全局 24h 滑窗，java-tron 用 per-account window size；锁/解锁委托条目未分开（java-tron `createDbKeyV2(owner, receiver, lock)`）。

**总退出**：M0 回放 3 段语料全部通过（state root 一致）；`docs/dev/p2p-interop-status.md` 追加一条"state conformance verified"。

**依赖**：M0；子里程碑内部顺序 M1.1 → M1.3 → {其余并行}。

**文档**：每个子里程碑独立 spec+plan；M1.1 优先复用既有 `2026-04-12-hard-fork-mechanism-design.md`。

---

## M2 · rawdb schema 与访问器补齐 — **P0，与 M1 并行**

**范围**：TODO §1.9。

**交付物**
- `core/rawdb/schema.go` 增加 §1.9 列出的 17 个缺失前缀。
- 对应 `accessors_*.go` 文件 + 单测。
- 迁移脚本：若既有 chaindata 需要补索引（如 `delegated-resource-account-index`），提供一次性构建命令 `gtron migrate index`。

**分组 PR 建议**
- PR-1 索引类：`account-asset`、`account-id-index`、`account-trace`、`delegated-resource-account-index`。
- PR-2 共识类：`witness-schedule`、`section`、`pbft-signdata`、`tree-block-index`。
- PR-3 历史/审计类：`transaction-history`、`transaction-retstore`、`balance-trace`、`receipt`、`check-point-v2`、`reward-vi`、`accumulated-reward`。
- PR-4 市场补齐：`market-account`、`market-pair-to-price`、`market-pair-price-to-order`。
- PR-5 shielded：`zkproof`、`note-commitment`、`incremental-merkle-tree`、`roots`。
- PR-6 合约 ABI：`abi`（把 ABI 从 ContractState 内联迁到独立 store）。

**退出**：`core/rawdb/schema.go` 的前缀集合是 java-tron `chainbase/.../store/` 的超集；M1 的子里程碑能直接调用对应访问器。

**依赖**：无；M1 使用时按需拉取相应 PR。

---

## M3 · P2P/Net 健壮性 — **P1（对 G1 加分，对 G3 必需）**

**范围**：TODO §2.2 ~ §2.6（§2.1 PBFT 单独列入 M6）。

**子里程碑**

### M3.1 同步稳定性
- `net/sync.go` 多 syncPeer + 指数退避 + 超时重试。
- 新增 `FetchBlockService` 等价：未收到 block 响应在 X 秒后换 peer 重试。
- Fork 恢复：引 `ResilienceService` 等价；检测到本地 fork 深度 > K 时触发回滚。
- peer 状态机 `PeerState = {Init, Handshaked, SyncStarted, Good, Bad}`。
- **退出**：杀一个 syncPeer 后 ≤30s 内切换完成；人为制造 10 块深度 fork 能自动恢复。

### M3.2 广播/中继
- `AdvService` 等价：交易广播批量化、`MAX_TRX_FETCH_PER_PEER`、spread 调度。
- `RelayService` 等价：per-IP 连接数上限、backup peer 机制。
- **退出**：交易到达全网 50 节点的 P95 时延接近 java-tron 基线（通过 M0 cross 脚本测量）。

### M3.3 速率限制与统计
- `p2p/ratelimiter.go`：逐 message-type 的 token bucket。
- `p2p/stats.go`：peer 级消息计数与 bytes 计数，供 JSON-RPC `net_*` 返回。
- **退出**：对单 peer 发送超额 `MsgFetchInvData` 会被节流而非崩溃。

### M3.4 discovery 完善
- `p2p/discover/table.go` 加入 15s ping timeout 驱逐 + candidate queue。
- **退出**：长时间运行后 table 中不会出现大量 dead 节点；`table_test.go` 覆盖驱逐。

**退出**：7×24h 主网连接不需人工干预；cross 系统测试下 10 节点拓扑稳定。

**依赖**：无；与 M1/M2 并行。

**文档**：复用 `2026-04-12-p2p-discovery.md` 的风格，拆为 `2026-04-XX-p2p-sync-hardening-*`。

---

## M4 · gRPC Wallet 服务 — **P1，G2 必经**

**范围**：TODO §4.1。

**交付物**
1. `make proto` 增加 `protoc-gen-go-grpc` 调用，生成 `proto/api/api_grpc.pb.go`。
2. 新建 `internal/grpcapi/` 目录，按 java-tron `WalletImplBase` 实现所有 RPC 的 server handler。单个 PR 不试图把 200+ 方法全塞进来，分组：
   - PR-A：Wallet 只读查询（GetAccount、GetBlock、GetTransactionInfoById…，约 60 个）。
   - PR-B：Wallet 交易构建（CreateTransferContract、CreateAssetIssue…，约 50 个）。
   - PR-C：Wallet 广播 + 签名助手（BroadcastTransaction、GetTransactionSign、EstimateEnergy…，约 20 个）。
   - PR-D：WalletSolidity 变种（读 solid 状态，约 40 个）——与 M8 绑定。
   - PR-E：Monitor / Node 服务。
3. gRPC server 与 HTTP server 共享同一 `TronBackend`；启动顺序与 lifecycle 在 `cmd/gtron/main.go` 中注册。
4. CLI flag `--grpc.port`（默认 50051）。

**退出**：`grpc_cli call localhost:50051 wallet.Wallet/GetNowBlock` 返回与 java-tron 同格式的响应；主流 SDK（tronweb、tronpy）可直连。

**依赖**：M1.1（DP 查询返回值要齐）。

**文档**：新建 `2026-04-XX-grpc-wallet-server-*`。

---

## M5 · HTTP & JSON-RPC 缺口补齐 — **P1**

**范围**：TODO §4.2、§4.3。

**子里程碑**

### M5.1 HTTP servlet 补齐
按 TODO §4.2 的 cluster 分组 PR：
- PR-1 账户 / 权限（7 个）
- PR-2 交易构建（15+ 个）
- PR-3 TRC10（5 个）
- PR-4 委托资源 + 冻结（Freeze V1/V2 + Delegate）
- PR-5 Exchange / Market（6 个）
- PR-6 Shielded（9 个）—— 依赖 M2 PR-5 的 shielded 存储
- PR-7 提案 / 监控补充

### M5.2 JSON-RPC 写路径 + filter
- 实现 `eth_sendRawTransaction`、`eth_sendTransaction`、`eth_sign`、`eth_signTransaction`、`eth_accounts`。
- 实现 filter subsystem：`eth_newFilter` 族 + `eth_getFilterLogs`。
- 补齐 `eth_gasPrice`、`web3_sha3`、`net_listening`、`net_peerCount`。
- 校验既有 `eth_call`、`eth_estimateGas` 与 java-tron 行为一致。
- **退出**：MetaMask 指向 gtron 能发交易；Hardhat `npx hardhat test --network gtron` 通过样例合约。

**依赖**：M4（很多 HTTP 底层复用 gRPC backend）；M3.3 的 stats（`net_peerCount`）。

---

## M6 · PBFT 消息路由与验证人能力 — **P0（验证人） / P2（全节点）**

**范围**：TODO §2.1（全节点接收路径）。SR 签名发送（M6b）留待后续。

**交付物**
1. `net/pbft_handler.go`：PBFT 消息（0x34 = PBFT_MSG）类型路由、SHA-256 签名恢复、SR 成员检查（当前+前一维护期）、去重 cache（10 分钟 TTL，10000 上限）、三阶段状态机（PREPREPARE → PREPARE → COMMIT）、quorum 达成后写入 `pbft-signdata` 前缀。
2. `net/pbft_data_sync.go`：PBFT_COMMIT_MSG（0x14）预聚合提交结果接收、签名验证（≥19 有效 SR 签名）、InsertBlock 后触发写入。
3. `core/rawdb/accessors_pbft_sign.go`：`WriteLatestPbftBlockNum` / `ReadLatestPbftBlockNum`（key: "LATEST_PBFT_BLOCK_NUM"，big-endian int64，只增不减）。
4. `core/rawdb/accessors_witness_schedule.go`：`WritePreviousShuffledWitnesses` / `ReadPreviousShuffledWitnesses`（key: "ws-prev-shuffled"）。
5. PBFT 消息源码级参考：java-tron `PbftMsgHandler.java`、`PbftDataSyncHandler.java`、`PbftMessageHandle.java`。
6. 协议常量勘误：消息类型码为 0x34（PBFT_MSG）和 0x14（PBFT_COMMIT_MSG），**不是** 0x40 起。

**退出条件（全节点路径）**：
- `rawdb.ReadBlockSignData(db, N)` 对已确认块 N 返回非 nil（需连接 java-tron 观察）。
- `make test` 28 包全绿（不依赖 java-tron 进程）。
- 无 DisconnectReason 被 java-tron 对端踢出。

SR 出块/签名发送能力（G3 门）留待 M6b 实现。

**依赖**：M2 PR-2（witness-schedule / pbft-signdata）、M3.1（peer 状态机）。

**文档**：`docs/superpowers/specs/2026-04-26-m6-pbft-routing-design.md` + `docs/superpowers/plans/2026-04-26-m6-pbft-routing.md`。

---

## M7 · TVM Cancun 对齐 — **P2**

**范围**：TODO §3。仅在 TRON 治理正式提上 Cancun 激活时合入；提前实现但用 `allow_tvm_cancun` 门控。

**交付物**
- `vm/opcodes.go` 加 `MCOPY` / `BLOBHASH` / `BLOBBASEFEE` 常量。
- `vm/instructions_cancun.go`（新建）handler 实现。
- `vm/jump_table.go` 注册项，`enabledFn` 绑定 `tvmConfig.Cancun`。
- `vm/energy.go` 补 MCOPY 动态成本。
- `opSstore` refund 逻辑 audit & 单测。

**退出**：go-ethereum 的 Cancun 官方测试向量（trimmed 到 TVM 支持的子集）通过。

**依赖**：M1.1（`allow_tvm_cancun` 已在 DP 中）。

---

## M9 · HTTP API 兼容性 + dev 模式硬化 — **P0（G2 真正退出条件）**

**背景**：2026-04-27 用 `scripts/system_test_flows.sh` 对 8 个交易类型 flow 做端到端 build→sign→broadcast→confirm→state 扫描，34 项断言只 11 PASS / 21 WARN / 1 FAIL / 1 SKIP。绝大多数 WARN 不是 actuator 逻辑问题，而是 HTTP API 层 bytes/枚举的 wire-format 与 java-tron 不兼容；最严重的是 protojson 把 hex 当 base64、`[]byte(string)` silent-corrupt、以及 `walletsolidity` 永远停在 block 0。完整 catalog 见 `docs/superpowers/specs/2026-04-27-system-test-findings.md`。

**范围**：

### M9.1 protojson-on-contract 路径全部改 plain JSON + hex
- `internal/tronapi/api_account.go:91 accountPermissionUpdate`
- `api_exchange.go:42/65/88/111/134/157` exchange*+market*
- `api_trc10.go:26/50` createAssetIssue / updateAsset
- `api.go:215` broadcasttransaction（外层 transaction proto 内层 contract）
- 单测：每个 endpoint 用 hex `owner_address` 调用 → 解出的 contract.OwnerAddress 必须等于 hex 解码值。

### M9.2 `[]byte(stringField)` → `common.FromHex(stringField)`
- setAccountId.account_id、transferAsset.asset_name、participateAssetIssue.asset_name、createWitness.url、updateWitness.update_url、getAssetIssueByName.value、getMarketPriceByPair.{sell,buy}_token_id
- 反例保留：triggerSmartContract.function_selector（java-tron 这个字段就是 ASCII signature）

### M9.3 `resource` 字段同时接受 string + int
- 自定义 `type ResourceField int32; func (r *ResourceField) UnmarshalJSON(...)` 识别 `0/1/2` 与 `"BANDWIDTH"/"ENERGY"/"TRON_POWER"`
- 替换 freeze/freezeV2/unfreeze/unfreezeV2/delegate/undelegate handler 的 `Resource int32` 字段

### M9.4 `proposalCreate.parameters` 接受数组形式
- `[]struct{Key int64 \`json:"key"\`; Value int64 \`json:"value"\`}` → 转 `map[int64]int64` 后送 backend。

### M9.5 `walletsolidity` solid-head 更新（M8.1 续）
- 移植 `java-tron Manager.updateSolidifiedBlock()`：取 active SR 集合的 latestBlockHeader.raw.number，排序，取第 `(2/3)*N+1`-th 小（`SOLIDIFIED_THRESHOLD`）。
- Hook 点：`BlockChain.InsertBlock` 末尾或独立 block hook。
- `dynProps.SetLatestSolidifiedBlockNum(value)` + commit。
- 单测：单 SR dev 链 → solid == head；27 SR 模拟 → 取第 19th。

### M9.6 dev 模式硬化
- `--dev.full-features`（默认 true）：在 `makeDevGenesis` 把所有"主网已激活、风险无副作用"的 `allow_*` flag 直接设成激活值（new_resource_model / delegate_resource / change_delegation / multi_sign / 全 TVM_*）。
- `--dev.maintenance-interval <ms>`（默认 21600000；支持 30000）：dev 链 30s 内完成一次维护周期，覆盖 reward distribution / proposal 激活。
- 验证：当前 system_test_flows 未跑通的 F4/F5 在 dev.full-features 下应直接 PASS。

### M9.7 broadcasttransaction 同步业务校验
- 在 push to pool 之前调用 actuator.Validate（read-only StateDB）。失败 → 返回 `code=CONTRACT_VALIDATE_ERROR, message=<reason>` 而非静默 `result.true`。
- 与 java-tron `Wallet#broadcastTransaction` 行为对齐。

### M9.8 注册缺失 endpoint
- `/wallet/updatesetting`（actuator/update_setting.go 已存在）
- `/wallet/updateenergylimit`（actuator/update_energy_limit.go 已存在）

### M9.9 把 `system_test_flows.sh` 接入 CI
- `make system-test-flows`（启 dev 节点 + 跑脚本 + 解析 FINDINGS_BEGIN/END 生成测试矩阵）
- 退出条件：M9.1 ~ M9.8 完成后，PASS ≥ 30 / WARN ≤ 4。

**退出**：M9.1~M9.8 全部完成 + system_test_flows 达到上述阈值。这是 G2（生态可用）真正的功能性退出条件——目前 G2 只是"代码完成"，但 SDK 连过来仍会因 wire-format 故障。

**实施顺序（重要）**：
1. **M9.6 first**（dev.full-features + maintenance-interval）——否则 M9.1~M9.4 修完后，F4/F5 仍会因 proposal 未激活而 wedge，无法用 system_test_flows 验证修复。
2. **M9.1~M9.4 并行**——HTTP 入参 wire-format 修复（互不依赖）。
3. **M9.5 + M9.7 + M9.8**——M9.5 不依赖前者；M9.7（broadcasttransaction 同步 actuator.Validate）需要 M9.1~M9.4 完成才能有意义地测试（否则每个 tx 在错误层失败）；M9.8 路由注册独立。
4. **M9.9 last**——所有修复完成后接入 CI。

**依赖**：无（所有改动局限在 internal/tronapi/，core/state/dynamic_properties.go，core/blockchain.go 中 InsertBlock 收尾段，cmd/gtron/config.go）。

**已验证假设**（2026-04-27 通过 java-tron 源码确认，避免修复方向反转）：
- M9.1/M9.2 **bytes 字段一律为 hex**：`framework/.../JsonFormat.java:826` `unescapeBytes(...) → ByteArray.fromHexString(...)`。
- M9.3 **enum 字段同时接受 int 与 string**：`JsonFormat.java:698-714`，`lookingAtInteger()` 走数字分支，否则取 identifier 并 normalize 首字母大写后 `findValueByName`。
- visible 模式（`visible=true`）特殊化：地址 → base58、部分 string 字段 → UTF-8。默认 visible=false 时全 hex。M9 修复同样应支持 visible 切换（参考 java-tron `selfType` 标志）。

**文档**：findings catalog `docs/superpowers/specs/2026-04-27-system-test-findings.md`；分项 plan 待实施时再写。

---

## M8 · Solidity/PBFT API 变种 + 事件订阅 — **P1/P2**

**范围**：TODO §4.4、§4.5。

### M8.1 Solidity / PBFT 变种
- `internal/tronapi/solidity.go` 和 `internal/tronapi/pbft.go`：复用既有 handlers，但从 solid block / PBFT-committed 状态查询。
- gRPC 对应 `WalletSolidity` / `WalletPBFT`（M4 PR-D 已预留）。
- 仅暴露只读端点。

### M8.2 事件订阅
- 新建 `internal/events/`：block/tx/log 三类 subject；WebSocket 订阅 + （可选）外部 plugin trigger。
- JSON-RPC `eth_subscribe` / `eth_unsubscribe`（WebSocket 信道）。
- 参考 java-tron `services/event/` 结构。

**退出**：浏览器可订阅某合约 logs；solidity 端点在 solid block 滞后 ~20 块时正确返回。

**依赖**：M1.3（finality 可判定）、M6（PBFT 确认可信源）。

---

## 跨里程碑事项

**测试矩阵**
- 单元：`make test`（保持 `-count=1 -timeout 300s`）。
- 集成：`go test -tags=integration ./p2p/`（每次 P2P/net 改动必跑）。
- 一致性：M0 回放（每次 state layer 改动必跑）。
- 系统：`scripts/system_test.sh` + `scripts/system_test_cross.sh`（pre-merge）。
- 长跑：一个 always-on 的 mainnet-follower，由 M0 产出的 state root 哈希每小时与 Nile 公共节点比对，失配即报警。

**风险跟踪**
| 风险 | 里程碑 | 缓解 |
|---|---|---|
| DP 初始值与 java-tron 有 1 字节偏差 → 难排查 | M1.1 | 单测强制 byte-for-byte 对比从 config.conf 生成的 fixtures |
| Fork 版本位改动若部署早于 M6 可能卡验证人选举 | M1.3 | 先上默认 off；配 feature flag 可关 |
| gRPC server handler 回归 HTTP 路径的 backend 语义 | M4 | backend 层保持单一实现，HTTP/gRPC 仅做编解码层 |
| 主网 TVM Cancun 激活早于 M7 完成 | M7 | 每周监控 TIP 提案；启动 M7 必须领先 TIP 2 周 |

**进度可视化**
- 在本文件底部维护进度表（见下），每关闭一个子里程碑在对应行打勾并注明 commit hash。
- 每个里程碑收尾需：(a) 更新 `TODO.md`，把已关闭项打勾并加 commit ref；(b) 更新 `docs/dev/p2p-interop-status.md` 或等价状态文档；(c) 追加 `CLAUDE.md` 中如有范式变化。

---

## 进度表

| 里程碑 | 状态 | 退出日期 | 主 PR / commit | 备注 |
|---|---|---|---|---|
| M0′ Fixture 抽取工具 | 完成（Nile 场景延后） | 2026-04-15 | 待提交 | 场景 00 mainnet DP 已入库；01 Nile 因缺 config 延后 |
| M0″ Phase 1 引擎+CLI+文档 | 完成 | 2026-04-17 | 6d3396d..2ef1152 (19 commits) | `core/conformance/` 11 文件 + 26 单测；`cmd/{gtron-replay,fixture-closure,fixture-digest}` + `scripts/fixtures/cmd/gen-smoke`；`test/fixtures/mainnet-blocks/smoke/` (regen via `go run ./scripts/fixtures/cmd/gen-smoke`)；`scripts/conformance_replay.sh` + make targets；`docs/dev/conformance-harness.md` 操作员协议；`go vet -copylocks` 清；全 23 包测试绿 |
| M0″ Phase 2 录真实语料+cross e2e | 待操作员 | — | — | blocker = java-tron mainnet 访问。退出 = `make conformance-replay-exit-gate` 返回 0（三段 allowlist 全空） |
| M1.1 DP backfill | 完成 | 2026-04-15 | 待提交 | 76-key fixture 全 match；ProposalParamKey 按 ProposalUtil.java 重建 |
| M1.2 Freeze V1 | 完成 | 2026-04-15 | 待提交 | V1+V2 share formula (availableAccountNet/Energy) + freeze/unfreeze weight sync + post-fork gate；VM actuator 能量重接走线延至 M1.8 |
| M1.3 版本位 + 分叉 audit | 完成 | 2026-04-15 | 待提交 | BlockHeader.version + ForkController（版本位投票/时间窗/比率门）+ fc.IsActive + audit 脚本/doc + AllowStakingV2/TvmShieldedToken 别名修正。剩余执行路径门缺口文档化为 backlog（见 docs/dev/fork-audit-2026-04-15.md），提案合法性门延至 M4。 |
| M1.4 自适应能量 | 完成 | 2026-04-25 | 62d1b22 | core/energy_adaptive.go + 20-block avg; ProcessBlock/BuildBlock per-block hooks; proposal #21 side-effect |
| M1.5 奖励 v2 + 委托奖励 | 完成 | 2026-04-25 | 62d1b22 | payBlockReward/payStandbyWitness/applyRewardMaintenance; voter ComputeVoterReward (old+new hybrid); withdrawReward in vote + withdraw actuators; proposal #67 side-effect |
| M1.6 存储租金 | 完成（stub） | 2026-04-25 | 62d1b22 | 4 DP keys (total_storage_pool/tax/reserved, storage_exchange_tax_rate)；StorageTaxProcessor 未实现（功能从未激活） |
| M1.7 动态能量 | 完成 | 2026-04-25 | 62d1b22 | ContractState + CatchUpToCycle; cs- rawdb prefix; interpreter factor-per-opcode + rawEnergyUsed tracking |
| M1.8 委托资源消费 | 完成 | 2026-04-25 | 62d1b22 | core/delegation/usage.go; undelegate usage-transfer math; DELEGATERESOURCE/UNDELEGATERESOURCE opcodes wired |
| M2 rawdb schema 补齐 | 完成 | 2026-04-26 | ae59a48..8b3877d | ✅ PR-1 indexing: `aa-`, `aid-`, `at-`, `drax-`. ✅ PR-2 consensus: `ws-shuffled`, `psd-` PbftSignData, `sb-` SectionBloom, `tbi-` TreeBlockIndex. ✅ PR-3 history/audit: `btrace-` BalanceTrace, `rvi-` RewardVi (IS_DONE sentinel + per-cycle VI), `cpv2-` CheckPointV2 (dormant); ti-/tib- documented as TransactionHistoryStore/RetStore equivalents. ✅ PR-4 market: `mptop-` MarketPairToPriceCount; mao-/mop-/mpl- documented. ✅ PR-5 shielded: `zkp-` ZKProof, `imt-` IncrementalMerkleTree. ✅ PR-6 abi: `abi-` AbiStore (inline→dedicated store migration prefix). Total: 36 tests, all green. |
| M3.1 sync 稳定性 | 完成 | 2026-04-28 | d5ff47a..5436f4d | slice-1: fetch-timeout(30s)+peer failover；slice-2a: peerConnState enum, lastInsertNano+LastInsertTime, SyncService.Start/Stop+watchdog(30s isolation check, 60s stall threshold)；slice-2b: KhaosDB in-memory fork buffer (khaosdb.go + 12 unit tests) + BlockChain.switchFork (applyBlock state-root stamping + state correctness test). 10-block fork 恢复完整实现，`make test` 全绿。 |
| M3.2 Adv/Relay | 完成 | 2026-04-26 | — | slice-1: batched INV spread (30ms ticker), immediate block flush, two-gen seen cache, BroadcastBlockFrom/BroadcastTxFrom origin exclusion, MAX_SPREAD_SIZE=1000 backpressure, block 3s expiry. Start/Stop lifecycle. 4 新测试。slice-2: per-IP inbound cap (MaxConnectionsWithSameIP=2, from ChannelManager.processPeer()); Server.stopOnce idempotent Stop; maintainLoop+maintainPeers() seed reconnect on disconnect (ConnPoolService.triggerConnect() equivalent). 2 新测试全绿。 |
| M3.3 速率限制 | 完成 | 2026-04-26 | — | p2p/ratelimiter.go: 零依赖 token bucket。NewRateLimiter() 默认 SyncBlockChain=3/s, FetchInvData=3/s, Disconnect=1/s (java-tron clearParam 值)。handleProtocolMessage 检查 rl.Allow(code)，超限则 drop+log。4 率限测试全绿。 |
| M3.4 discovery 驱逐 | 完成 | 2026-04-26 | — | Table.RemoveByAddr(ip,port)；evictTimedOutPings 现在在清理 pendingPings 的同时驱逐无响应节点；pendingPing struct 存储 sentAt+ip+port。service_test+table_test 新增 3 测试全绿。 |
| M4 gRPC Wallet server | 完成 | 2026-04-26 | 已完成 | PR-A0~E 全部实现：proto codegen, 48个 bufconn tests全绿，`--grpc.port` flag，Wallet 只读/交易构建/广播/分页/价格 RPC，TronBackend 扩展 14 个新方法，system_test.sh 修复 grpc port 冲突。 |
| M5.1 HTTP 补齐 | 完成（路由全部覆盖；wire-format 兼容性 gap 见 M9） | 2026-04-26 | — | PR-1~7 全部实现：api_account/api_tx/api_trc10/api_exchange/api_misc 5 个 cluster 文件，BuildContractTransaction 泛型构建器，GetProposalByID/ListProposalsPaginated/ValidateAddress，共 ~35 个端点，28 包全绿。**已知 gap（2026-04-27 系统测试）**：(a) 用 protojson.Unmarshal 直接解 contract proto 的 9 个 endpoint 把 hex bytes 当成 base64 → 见 M9.1；(b) `[]byte(stringField)` 对 7 个 endpoint silent corrupt → 见 M9.2；(c) freeze/delegate `resource` 字段不接受 string → M9.3；(d) `proposalCreate.parameters` 不接受 java-tron 数组形式 → M9.4。完整 catalog: docs/superpowers/specs/2026-04-27-system-test-findings.md。 |
| M5.2 JSON-RPC 写路径 | 完成 | 2026-04-26 | — | PR-1~4 全部实现：eth_gasPrice/web3_sha3/net_listening/net_peerCount/eth_accounts，write方法返回-32601（与java-tron一致），eth_estimateGas，filter subsystem（eth_newFilter/newBlockFilter/uninstallFilter/getFilterChanges/getFilterLogs + FilterManager + BlockChain.AddBlockHook），28包全绿。 |
| M6 PBFT 路由（全节点） | 完成 | 2026-04-26 | — | PR-1~5 全部实现：协议常量 0x34/0x14（勘误 0x40+），SHA-256 签名恢复，SR 成员检查（当前+前一维护期），去重 cache，三阶段状态机，quorum 写 pbft-signdata，PBFT_COMMIT_MSG 验证，WriteLatestPbftBlockNum，PbftHandler/PbftDataSyncHandler node.Lifecycle，block hook 接入，28 包全绿，13 新测试。SR 签名发送留 M6b。 |
| M7 TVM Cancun | 未开始 | — | — | 等待 TIP |
| M8.1 Solidity/PBFT API | 完成（路由层；solid-head 更新缺失见 M9.5） | 2026-04-26 | — | /walletsolidity/ + /walletpbft/ HTTP routes; gRPC WalletSolidity service (SolidityServer, ~30 methods); SolidifiedBlockNum()/LatestPbftBlockNum() backend methods; 10 new tests; 28 包全绿。**已知 gap（2026-04-27 系统测试）**：`SetLatestSolidifiedBlockNum` 仅在测试中调用，生产路径从未写入，`walletsolidity/getnowblock` 永远返回 genesis（block 0）。需移植 java-tron `Manager.updateSolidifiedBlock()` 到 InsertBlock。详见 M9.5。 |
| M8.2 事件订阅 | 完成 | 2026-04-26 | — | eth_subscribe/eth_unsubscribe over WebSocket; newHeads + logs subscription types; gorilla/websocket direct dep; SubscriptionManager wired into FilterManager.fanOut; 4 new tests (newHeads, logs, unsubscribe, HTTP coexistence); 28 包全绿。 |
| M9 HTTP wire-format + dev hardening | 完成 | 2026-04-27 | — | 9 个子项全部落地：M9.6 `--dev.full-features`/`--dev.maintenance-interval` flags（28 allow_* flags 激活）；M9.1 protojson→plain JSON+hex（api_exchange/trc10/account/broadcasttransaction）；M9.2 `[]byte(x)`→`common.FromHex(x)`（7 处 silent-corrupt fix）；M9.3 ResourceField 双形式（string+int）；M9.4 proposalCreate.parameters 数组形式；M9.5 updateSolidifiedBlock 移植（java-tron 70%-threshold，rawdb wlb- 前缀，InsertBlock hook）；M9.7 broadcasttransaction 同步 actuator.Validate（CONTRACT_VALIDATE_ERROR）；M9.8 /wallet/updatesetting + /wallet/updateenergylimit 路由注册；M9.9 `make system-test-flows` CI target（PASS≥30/WARN≤4 exit gate）。全包绿。 |

**退出门追踪**

| 门 | 依赖 | 状态 |
|---|---|---|
| G1 可跟链 | M0′+M1+M2+M3+M0″ | ❌ (M0″ Phase 1 完成；Phase 2 是 G1 的最后一块) |
| G2 生态可用 | M4+M5+M9 | ✅ (2026-04-27: M4+M5 路由完成 + M9 所有 P0 wire-format/solid-head/dev-mode 修复落地，全包绿) |
| G3 可当验证人 | G1+M6 | ❌ |
| G4 主网前置就绪 | G1+G2+G3+M7+M8 | ❌ |
