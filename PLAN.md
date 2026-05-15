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

#### 2026-05-08 · live HTTP capture 路径的根本约束（已验证）

`cmd/fixture-capture` 走 `--http=<远程 java-tron>` + 可选 SOCKS5 这条路径，已经构建并端到端跑通（2026-05-08 用 `http://3.12.206.71:8088` + `127.0.0.1:1088`）。引擎/格式/digest 全 OK，gtron-replay 在被喂正确 seed 时能立刻报真实分叉。

但**这条路径不能产出可信主网语料**，原因：

1. `wallet/getaccount` 只返回**当前 head 状态**——没有 `at-height` 变体。已实测：endpoint 接受 `block_num` 字段但**忽略**之（返回当前 head 的同一份数据）。
2. 主网 3s 出块 + SOCKS5 单跳 ~1s/调用：closure size > ~60 个地址时单 block snapshot 就跨过下一个 slot，结果是「snapshot 里一半地址在 state(h) 一半在 state(h+k)」的内部不一致快照——digest 永远对不上。
3. ProcessBlock 要求所有 tx 触及的 sender/receiver 在 seed 里有账户；典型 1-block 触及 ~400 唯一地址（372 txs × ~2 each），而 27-witness closure 只能让 ProcessBlock 在第 1 个非 witness sender 上报 `insufficient balance for create_account_fee`——不是真 bug，只是 closure 不全。

加大 closure 让 ProcessBlock 能跑完 → snapshot 时长爆掉 → race 加剧。两端冲突。

**结论**：live HTTP capture **本质上需要可控 java-tron 节点（能停、能取一致快照）**，跟 Phase 1 spec 写的 "rewind your mainnet java-tron to start-1" 完全一致——这条约束没法绕过。`cmd/fixture-capture` 留作备件，可在拥有"私链 / 已停 fullnode"的运维场景下跑（注意 `--start-auto-buffer` 与 listWitnesses + getChainParameters 已并行化，详见 `cmd/fixture-capture/capture.go::captureSeed/captureSnapshot`）。

**剩下的现实选项**（按成本递增）：
- 接受公开 HTTP endpoint 噪声，多跑、把 reproducible 分叉当信号——慢但零基础设施投入。
- 找 archive endpoint 真支持 `at-height` 的（TronGrid 可能；待探）。
- 自建本地 mainnet-synced java-tron（一次性 sync 4–7 天 → 之后能停可取一致快照）——这是 Phase 1 spec 的原始假设。

`scripts/system_test_cross.sh` 的双节点 e2e 部分**不**受这一约束阻塞——它跑私链。已在 cross-impl slice 3 中验证过 4 SR / 71190 块全 byte-identical。差的是 mainnet 语料 + replay。

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

### M3.5 · discovery 接入 production binary（**P1，2026-05-08 发现的 gap**）
**背景**：`p2p/discover/` Kademlia 实现完整（FIND_NODE/NEIGHBOURS/Ping/Pong + 路由表 + 驱逐）且 `TestJavaTronDiscoverPing` 集成测试通过 java-tron 端验证。但 `cmd/gtron/main.go:250` 构造 `p2p.NewServer` 时没有传 `Discovery: discover.New(...)` —— 生产 binary 走 `p2p/server.go:109` 的 fallback 分支（"Dial seed nodes directly when discovery is disabled"），永远只连 `--seednode` 列表，找不到新 peer。

**症状**：实测 19 个主网 seed 中只有 4 个被 dial（其余靠 discovery 才能发现），`net_peerCount` 长期停在 3-5。如果手动指定的 seed 全部下线，gtron 找不到新 peer，sync 中断。

**修复**：
1. 在 `cmd/gtron/main.go` `p2pServer := p2p.NewServer(...)` 之前构造 `discSvc, err := discover.New(udpAddr, networkID, nodeID)`，加入 `Discovery: discSvc` 字段。
2. 加 `--discover.port` flag（默认与 `--p2p.port` 同值）；`discover.New` 的 UDP 监听走 `0.0.0.0:<port>`。
3. 集成测试：起 gtron 不带 `--seednode`，仅用 `discover.AddBootstrap(...)` 引入 1 个 mainnet seed，断言 30s 后 `net_peerCount > 5`（discovery 发现新邻居）。
4. 7×24h 长跑断言：杀掉所有手动 seed peer 后，gtron 仍能维持 `net_peerCount > 0`（靠 discovery 的 candidate queue 重连）。

**退出**：上面 (3) 集成测试绿；TODO §2.3 "无 fork-recovery service" 之外的 P2P/Net 项全部覆盖。

**依赖**：无；改动局限在 `cmd/gtron/main.go` 与 `p2p/server.go` 已有的 optional `Discovery` 字段。

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
| M0′ Fixture 抽取工具 | 完成（Nile 场景延后） | 2026-04-15 | 9863df2..7fce4c6 | 场景 00 mainnet DP 已入库；01 Nile 因缺 config 延后 |
| M0″ Phase 1 引擎+CLI+文档 | 完成 | 2026-04-17 | 6d3396d..2ef1152 (19 commits) | `core/conformance/` 11 文件 + 26 单测；`cmd/{gtron-replay,fixture-closure,fixture-digest}` + `scripts/fixtures/cmd/gen-smoke`；`test/fixtures/mainnet-blocks/smoke/` (regen via `go run ./scripts/fixtures/cmd/gen-smoke`)；`scripts/conformance_replay.sh` + make targets；`docs/dev/conformance-harness.md` 操作员协议；`go vet -copylocks` 清；全 23 包测试绿 |
| M0″ Phase 2 录真实语料+cross e2e | 待操作员 | — | — | blocker = java-tron mainnet 访问。退出 = `make conformance-replay-exit-gate` 返回 0（三段 allowlist 全空） |
| M1.1 DP backfill | 完成 | 2026-04-15 | 3be3d4c..1e76d08 | 76-key fixture 全 match；ProposalParamKey 按 ProposalUtil.java 重建 |
| M1.2 Freeze V1 | 完成 | 2026-04-15 | 16956d4..01d4afe | V1+V2 share formula (availableAccountNet/Energy) + freeze/unfreeze weight sync + post-fork gate；VM actuator 能量重接走线延至 M1.8 |
| M1.3 版本位 + 分叉 audit | 完成 | 2026-04-15 | 48ff35e..b8636c4 | BlockHeader.version + ForkController（版本位投票/时间窗/比率门）+ fc.IsActive + audit 脚本/doc + AllowStakingV2/TvmShieldedToken 别名修正。剩余执行路径门缺口文档化为 backlog（见 docs/dev/fork-audit-2026-04-15.md），提案合法性门延至 M4。 |
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
| M3.5 discovery 接入 production | 完成 | 2026-05-09 | — | `cmd/gtron/main.go` 在 `p2p.NewServer` 之前构造 `discover.NewService(...)` 并传 `Discovery: discSvc`；`SetOnNewPeer` 直接走 `p2pServer.AddPeer` 完成 pong→TCP 闭环。新增 `--discover.port` flag（0 → 复用 `--p2p.port`）。`scripts/system_test.sh` 77/77 全绿（本地双节点 seed→UDP→pong→dial 路径无回归）。集成测试 `TestDiscoveryWireIn` 已落地（//go:build integration，门 `JAVA_TRON_ADDR`+`JAVA_TRON_NETWORK`，断言 60s 内 PeerCount > 1）。**2026-05-09 live mainnet sanity**：用 `--seednode` 指 5 个 mainnet seed 起 gtron，4 分钟内同步至 block #52,300（peer 报 head=#82,540,599），1 个 TRON-Hello 完成的 peer（3.218.137.187，~200 blk/s）；其余 4 seed libp2p 完成但被 TRON-Hello EOF（与 `reference_tron_mainnet_seeds.md` 记录的 rate-limit 模式一致，不是 wire-in 引入的回归）。 |
| M4 gRPC Wallet server | 完成 | 2026-04-26 | 已完成 | PR-A0~E 全部实现：proto codegen, 48个 bufconn tests全绿，`--grpc.port` flag，Wallet 只读/交易构建/广播/分页/价格 RPC，TronBackend 扩展 14 个新方法，system_test.sh 修复 grpc port 冲突。 |
| M5.1 HTTP 补齐 | 完成（路由全部覆盖；wire-format 兼容性 gap 见 M9） | 2026-04-26 | — | PR-1~7 全部实现：api_account/api_tx/api_trc10/api_exchange/api_misc 5 个 cluster 文件，BuildContractTransaction 泛型构建器，GetProposalByID/ListProposalsPaginated/ValidateAddress，共 ~35 个端点，28 包全绿。**已知 gap（2026-04-27 系统测试）**：(a) 用 protojson.Unmarshal 直接解 contract proto 的 9 个 endpoint 把 hex bytes 当成 base64 → 见 M9.1；(b) `[]byte(stringField)` 对 7 个 endpoint silent corrupt → 见 M9.2；(c) freeze/delegate `resource` 字段不接受 string → M9.3；(d) `proposalCreate.parameters` 不接受 java-tron 数组形式 → M9.4。完整 catalog: docs/superpowers/specs/2026-04-27-system-test-findings.md。 |
| M5.2 JSON-RPC 写路径 | 完成 | 2026-04-26 | — | PR-1~4 全部实现：eth_gasPrice/web3_sha3/net_listening/net_peerCount/eth_accounts，write方法返回-32601（与java-tron一致），eth_estimateGas，filter subsystem（eth_newFilter/newBlockFilter/uninstallFilter/getFilterChanges/getFilterLogs + FilterManager + BlockChain.AddBlockHook），28包全绿。 |
| M6 PBFT 路由（全节点） | 完成 | 2026-04-26 | — | PR-1~5 全部实现：协议常量 0x34/0x14（勘误 0x40+），SHA-256 签名恢复，SR 成员检查（当前+前一维护期），去重 cache，三阶段状态机，quorum 写 pbft-signdata，PBFT_COMMIT_MSG 验证，WriteLatestPbftBlockNum，PbftHandler/PbftDataSyncHandler node.Lifecycle，block hook 接入，28 包全绿，13 新测试。SR 签名发送留 M6b。 |
| M7 TVM Cancun | 未开始 | — | — | 等待 TIP |
| M8.1 Solidity/PBFT API | 完成（路由层；solid-head 更新缺失见 M9.5） | 2026-04-26 | — | /walletsolidity/ + /walletpbft/ HTTP routes; gRPC WalletSolidity service (SolidityServer, ~30 methods); SolidifiedBlockNum()/LatestPbftBlockNum() backend methods; 10 new tests; 28 包全绿。**已知 gap（2026-04-27 系统测试）**：`SetLatestSolidifiedBlockNum` 仅在测试中调用，生产路径从未写入，`walletsolidity/getnowblock` 永远返回 genesis（block 0）。需移植 java-tron `Manager.updateSolidifiedBlock()` 到 InsertBlock。详见 M9.5。 |
| M8.2 事件订阅 | 完成 | 2026-04-26 | — | eth_subscribe/eth_unsubscribe over WebSocket; newHeads + logs subscription types; gorilla/websocket direct dep; SubscriptionManager wired into FilterManager.fanOut; 4 new tests (newHeads, logs, unsubscribe, HTTP coexistence); 28 包全绿。 |
| M9 HTTP wire-format + dev hardening | 完成 | 2026-04-27 | — | 9 个子项全部落地：M9.6 `--dev.full-features`/`--dev.maintenance-interval` flags（28 allow_* flags 激活）；M9.1 protojson→plain JSON+hex（api_exchange/trc10/account/broadcasttransaction）；M9.2 `[]byte(x)`→`common.FromHex(x)`（7 处 silent-corrupt fix）；M9.3 ResourceField 双形式（string+int）；M9.4 proposalCreate.parameters 数组形式；M9.5 updateSolidifiedBlock 移植（java-tron 70%-threshold，rawdb wlb- 前缀，InsertBlock hook）；M9.7 broadcasttransaction 同步 actuator.Validate（CONTRACT_VALIDATE_ERROR）；M9.8 /wallet/updatesetting + /wallet/updateenergylimit 路由注册；M9.9 `make system-test-flows` CI target（PASS≥30/WARN≤4 exit gate）。全包绿。 |
| M10 fee conformance | 完成 | 2026-04-29 | 086a79d..8123faf | TRON_POWER 资源门 + FreezeV2/UnfreezeV2 InitializeOldTronPower；consumeMultiSignFee + consumeMemoFee per-contract（修了一次按 tx 算的 bug）+ burn_trx_amount 累计；total transaction counter（rawdb 非共识层）。全包绿。 |
| M11.1 witness statistics + BLOCK_FILLED_SLOTS | 完成 | 2026-04-29 | 2f3ef15 | core/state DP 加 128 字节 rolling ring + ApplyBlockToFilledSlots/CalculateFilledSlotsCount；Witness 增 LatestBlockNum/LatestSlotNum accessor；新 consensus/dpos/statistic.go 实现 ApplyBlockStatistics（producer 计数 + miss 归因 + filled-slots ring）；wired in BlockChain.applyBlock 的 ProcessBlock 后/maintenance 前。10 个新单测，28 包全绿。**已知 gap**：(a) 出块端 LOW_PARTICIPATION 门未接；(b) WitnessProductBlockService 作弊检测未接。 |
| M11.2 conformance digest fold-in | 完成 | 2026-04-29 | 5b339f5 | DigestB salt v1→v2，每个 touched-addr 折入 rawdb.ReadWitness 字节，每个 stringProps 键值对折入；DigestC 加 witness 子条目 + 顶层 dpStrings map（block_filled_slots hex 编码）。补 5 单测；smoke oracle regen。这是 M11.1 计数器在 M0″ Phase 2 cross-impl 比对中可见的前置。 |
| M11.3 contract-type bitmap DPs | 完成 | 2026-04-29 | 677129b | DP 加 32-byte available_contract_type / active_default_operations 双 bitmap（默认 7fff1fc0037e... / 7fff1fc0033e... byte-for-byte 对齐 java-tron）；IsContractTypeAvailable + AddSystemContractAndSetPermission helper；AccountPermissionUpdate.Validate 加每位检查（"N isn't a validate ContractType"）；applyProposalSideEffects 5 个新 case（提案 26→48, 30→49, 44→52,53, 70→54-58, 77→59）。13 单测，28 包全绿。**已知 gap**：active_default_operations 维护值正确但未在新账户 active permission 默认装载（需配 AllowMultiSign 门 + 影响 4 个 actuator + 影响账户 proto bytes，留 M11.5）。 |
| M11.4 total_create_witness_cost counter | 完成 | 2026-04-29 | dd69dae | DP 加 total_create_witness_cost 键 + AddTotalCreateWitnessCost helper；WitnessCreateActuator.Execute 在 burnFee 后累加 fee。1 单测。total_create_account_cost 类似 counter 留待 BandwidthProcessor 加 consumeFeeForCreateNewAccount fallback 时一并接入（不在 M11 范围）。 |
| M11.5 default active permission ops on new account | 完成 | 2026-04-30 | a28f5ba | core/types `MakeDefaultOwnerPermission`/`MakeDefaultActivePermission` helper；`StateDB.ApplyDefaultAccountPermissions(addr, dp)` 注入 owner+active[0]，operations bitmap 来自 `dp.ActiveDefaultOperations()`；4 actuator 新账户路径接入（CreateAccount / Transfer / TransferAsset / ShieldedTransfer），门为 `ctx.DynProps.AllowMultiSign()`（直读 DP，对齐 java-tron 5 处构造点）。9 新单测，28 包全绿。**Out-of-slice deferred**：(a) VM 内 CALL-with-value 创号路径（vm/tvm.go RepositoryImpl.createNormalAccount 等价物），(b) WitnessCreate `setDefaultWitnessPermission` 回填，(c) `Account.create_time` 字段（java-tron 写 latestBlockHeaderTimestamp，go-tron StateDB.CreateAccount 未写），(d) 旧账户回溯迁移。详见 plan 文件 Out-of-slice 段。**Pending**：byte-level parity 待 M0″ Phase 2 fixture replay 验证。 |
| M6b SR-side PBFT signing 脚手架（slice 1） | 完成 | 2026-04-30 | c592947 | `net/pbft_producer.go`：PbftProducer + SR key reader + 三个消息构造器（BuildBlockPrePrepareMsg / BuildPrepareMsg / BuildCommitMsg），消息签名通过 `net/pbft_handler.go` receive 解析器双向 round-trip 验证；no-op `OnBlockApplied` block hook 经 `BlockChain.AddBlockHook` 在 `--witness` 模式下注册（不动 blockchain.go）。9 新单测，28 包全绿。**Slice 2 deferred**：三阶段状态机（PREPREPARE→PREPARE→COMMIT）producer 端、peer broadcast、COMMIT quorum 聚合、SRL pre_prepare 维护周期触发、多 SR key、live byte-level cross-impl 验证。 |
| M6b SR-side PBFT 三阶段状态机（slice 2） | 完成 | 2026-05-01 | ea30f69 | `net/pbft_producer.go` 扩展：`srKeys []*ecdsa.PrivateKey` 支持多 SR key（通过 `--witness.keys-file` 加载）；producer 端三阶段状态机——`OnBlockApplied` 触发 PREPREPARE 广播、`handlePrepare` quorum 达成后触发 COMMIT 广播、`handleCommit` quorum 达成后写 `WriteLatestPbftBlockNum`；`AddMaintenanceHook` 接入（M6b 新增钩子 API）用于 SRL PREPREPARE 在维护点发送（`BuildSrlPrePrepareMsg`）。`EmitPrepare`/`EmitCommit` 通过 `TronHandler.Send` 广播给全连接 peer。slice-2 在 merge 时遇到与 slice-3（`vm.TVM.DB` 窄化 + M11.5 slice 2c `DynProps` 新增字段）的 merge conflict，已解决：保留 `DB KVReadWriter`（slice 3 窄化）+ `DynProps *state.DynamicProperties`（slice 2c 新增），两字段共存。28 包全绿。**Slice 3 deferred**（仍为 live byte-level cross-impl 验证）：需 mainnet SR private key + java-tron 对端；G3 门的最后验证步骤。 |
| switchFork rewind correctness（slice 1） | 完成 | 2026-04-30 | f85bde0 | `core/blockbuffer/`：分层 in-memory write-set，`BeginBlock`/`CommitBlock`/`DiscardBlock`/`DiscardActive`，读穿透到 base store；`core/blockchain.go::applyBlock` 每块开 layer，witness 统计 writer 走 buffer，成功时 commit、失败时 discard active；`switchFork` 丢弃 orphan-branch layer 防双计数。`consensus/dpos/statistic.go` retrofit 为 buffered write。新增 reorg 测试断言 witness 计数在 3 块 reorg 后只反映 canonical chain。28 包全绿。**Slice 2 deferred**：retrofit 余下 5 个 writer（DP Flush、cycle brokerage / VI、total-tx counter、burn_trx_amount、solidified-block）。M0″ Phase 2 准入前必须落 slice 2。 |
| M11.5 slice 2a · WitnessCreate default permissions | 完成 | 2026-04-30 | 4d4f031 | `core/types.MakeDefaultWitnessPermission(addr)` + `(*StateDB).ApplyWitnessPermissions(addr, dp)`：mirror java-tron `AccountCapsule.setDefaultWitnessPermission`，**条件性**填充——Owner 仅当原本 nil 时设、Active[0] 仅当列表为空时追加（advisor flagged 全覆盖会破坏自定义 permission；agent 改为 java-tron 实际行为）；Witness 总是覆盖。`actuator/witness.go` 在 `SetIsWitness(true)` 后按 `AllowMultiSign` 门调用。8 新单测含 `PreservesCustomOwner`/`PreservesCustomActives` 锁定。28 包全绿。**Out-of-slice deferred**：(a) VM CALL-with-value 创号路径，(b) `Account.create_time` 字段 parity，(c) 旧账户回溯迁移。 |
| M11.5 slice 2b · Account.create_time parity | 完成 | 2026-05-01 | 20fc56c | `(*StateDB).CreateAccountWithTime(addr, type, ts)` 旁路 helper（不改既有 `CreateAccount` 签名以保 VM-internal 路径 slice 2c 之纯）；4 actuator 新账户路径（CreateAccount / Transfer / TransferAsset / ShieldedTransfer）改调 `CreateAccountWithTime(_, _, ctx.DynProps.LatestBlockHeaderTimestamp())`，**无条件**写 create_time（mirror java-tron `AccountCapsule.java:158-180`、`TransferActuator.java:53-54`、`TransferAssetActuator.java:66-67`、`ShieldedTransferActuator.java:142-143`、`CreateAccountActuator.java:44-45`，独立于 AllowMultiSign 门）。1 新 statedb 单测 + 5 actuator 单测（含 zero-DP 锁定与 AllowMultiSign=false 锁定）。28 包全绿。**Out-of-slice deferred**：(c) VM CALL-with-value 创号路径（slice 2c），(d) 旧账户回溯迁移（slice 2d）。 |
| M11.5 slice 2c · VM 内创号路径 parity | 完成 | 2026-05-01 | 99b88ee | `vm.TVM` 新增 `DynProps *state.DynamicProperties` 字段（`NewTVM` 签名扩参，5 个 production caller + 12 个测试 caller 全部接入；测试传 `nil` 走 pre-Solidity059 老路径）；新 `(*TVM).maybeCreateNormalAccountForValueTransfer(addr)` helper mirror java-tron `Program.createAccountIfNotExist` (Program.java:1874-1882) → `RepositoryImpl.createNormalAccount` (RepositoryImpl.java:1103-1114)：门 `cfg.Solidity059`，路径上 `!AccountExists(addr)` 时调 `CreateAccountWithTime(addr, Normal, dp.LatestBlockHeaderTimestamp())`，再按 `dp.AllowMultiSign()` 调 `ApplyDefaultAccountPermissions`。3 处接入：`tvm.Call`、`tvm.CallToken`（**仅 TRX `value > 0` 时**触发 mirror Program.java:1081-1083 `endowment > 0` 门，纯 token 转账不触发；且**precompile 目标跳过** mirror `OperationActions.java:1034-1042` 的 `callToPrecompiledAddress` 早分发路径——java 不让 precompile 走 `callToAddress`）、`opSelfDestruct`（mirror Program.java:483/555 `suicide`/`suicide2` 在 balance transfer 前无条件调用，java 此处不查 precompile）。CREATE/CREATE2 路径**不动**——java-tron 用 3-arg `RepositoryImpl.createAccount(addr, "CreatedByContract", Contract)` 既不写 create_time 也不装 default permission（RepositoryImpl.java:285-292）。6 新 vm 单测：`FromCALLWithValue`（create_time + 默认 perm）、`FromCALL_NoMultiSign`（create_time 写但无 perm，锁定独立门）、`FromCALL_PreSolidity059`（fork gate 关时旧行为锁定）、`FromCALLToken_TokenOnly`（token-only 不触发 mirror endowment 门）、`FromCALL_PrecompileAddrUntouched`（precompile 目标跳过 helper，锁定 wire-compat）、`FromSUICIDE`（SELFDESTRUCT-with-balance auto-create）。28 包全绿。 |
| M11.5 slice 2d · 旧账户回溯迁移 | 不实现 | 2026-05-01 | — | java-tron 同样不做回溯迁移：新行为整体由 AllowMultiSign DP 门 + Solidity059 fork 门控制，proposal #20 / Solidity059 激活之前创建的账户保留 `create_time = 0` 与空 default permission，激活之后新建的账户走 slice 2a-2c 的填充路径。回溯迁移会引入 java-tron 不存在的状态 mutation，违反 wire-compat 约束（M0″ Phase 2 fixture replay 会立刻失败）。无代码动作；slice 2c 完成即关闭 M11.5 全部 deferred 项。 |
| switchFork rewind correctness（slice 2） | 完成 | 2026-04-30 | 6f6c457 | Retrofit 4 类剩余 rawdb-direct writer：`state.DynamicProperties.Flush`、`applyRewardMaintenance`（cycle brokerage / vote / VI）、`InsertBlock` total_transaction_count、`updateSolidifiedBlock` 全部走 `bc.buffer`；reward.go 接口窄化为 `kvReadWriter`。Buffer 加 `FlushUpTo(blockHash, lookup, target)` + RWMutex 支持并发读（DynProps RPC 走 buffer）。**Flush policy**：每个 applyBlock 末在 CommitBlock 后调 `flushBufferUpToSolidified`，把 ≤ `latest_solidified_block_num`（java-tron `floor(N×0.3)` 规则）的 layer 落盘。`TestForkSwitch_WitnessCountersNoDoubleCount` 扩展到也断言 total_tx_count / latest_block 游标 / latest_solidified_block_num / pending-buffer-layer 数；新增 `TestFlushAtSolidified_SurvivesRestart` 验证 restart 不丢数据。28 包全绿，`-race` clean。**仍 deferred**：`payBlockReward → AddCycleReward` 与 actuator 内 rawdb writes 需要扩 `actuator.Context.DB` 表面，撞 M11.5 slice 2a territory；优雅 shutdown flush。 |
| LOW_PARTICIPATION 出块端门 | 完成 | 2026-05-01 | b249d1f | `params.MinParticipationRate = 15`（java-tron `config.conf:179` + `CommonParameter.java:122` default）；`core/producer/producer.go::shouldSkipLowParticipation` 用 `chain.DynProps().CalculateFilledSlotsCount() < threshold` 严格 `<`（mirror java-tron `StateManager.java:56`）。在 `tryProduceBlock` 中位置：`scheduled != p.witnessAddr` 检查后、`produceBlock` 前；命中则 log `LOW_PARTICIPATION rate=%d threshold=%d, skipping slot` 跳过。4 新单测：`Skips@10`、`Produces@50`、`Produces@15`（边界严格 `<`）、`Skips@14`。M11.1 已知 gap (a) 关闭。 |
| WitnessProductBlockService 作弊检测 | 完成 | 2026-05-01 | 3958739 | `consensus/dpos/cheat_detection.go`：`CheatDetector` + `CheatWitnessInfo`，`HistoryBlockCacheSize = 200`（mirror java-tron `WitnessProductBlockService.java:20-21` Guava cache），FIFO 200 槽 + 按 block.num 索引；`CheckBlock` 按 `(num,witness,hash)` 比对，命中相同 num+相同 witness+不同 hash 则 `recordCheatLocked`（`Times++`、重建 `BlockSet={prev,new}`、`LatestBlockNum`、`Time`），mirror `validWitnessProductTwoBlock` java-tron `WitnessProductBlockService.java:25-45`。Hook 在 `net/handler.go::handleBlock` 在 `chain.InsertBlock` 成功后、relay 前调用——只在 adv-block 路径接（mirror `BlockMsgHandler.processAdvBlock` java-tron line 153），sync 路径在 `syncService.HandleBlock` return 处提前 bail，producer 路径不经过此 handler。**Monitoring-only**：与 java-tron 一致——不动 `Witness.is_jobs`（该字段仅由 genesis `DposService` + `MaintenanceManager` 维护周期改写）、不写 DP、不持久化、不影响共识；`CheatDetector` 通过 `TronHandler.CheatDetector()` 暴露，供未来 NodeInfo RPC `cheatWitnessInfoMap` 字段填充（proto 已存在）。8 新单测（DoubleSign_Recorded / NormalProduction_NotRecorded / OldEntriesEvicted（200 边界）/ DifferentWitnessSameHeight / RepeatedDoubleSign_Increments / NilBlock_Ignored / ReorgDoesNotCorruptCache / ConcurrentCheckBlock）。M11.1 已知 gap (b) 关闭。28 包全绿。 |
| switchFork rewind correctness（slice 3） | 完成 | 2026-05-01 | 807f098 | 关闭 slice-2 deferred 项：(A) `actuator.Context.DB` 由 `ethdb.KeyValueStore` 窄化为新接口 `actuator.BufferedKVStore = Reader+Writer`；`vm.TVM.DB`/`SetDB`、`ApplyTransaction`/`ProcessBlock`/`ProcessProposals`/`payBlockReward`/`payStandbyWitness`/`removeOrderFromBook`/`withdrawReward`/`queryReward` 均同步窄化；`rawdb.AddCycleReward`/`AppendNoteCommitment` 由 `KeyValueStore` 改为局部 Reader+Writer 复合接口。`bc.applyBlock` 现把 `bc.buffer` 传给 `ProcessBlock`，使 actuator-side `WriteAssetIssue`/`WriteExchange`/`WriteProposal`/`WriteContractState`（VM dynamic-energy）/`WriteNullifier` 等所有 `ctx.DB.Put` 路径都走 buffer，`switchFork` 的 `DiscardBlock` 一并回滚。(B) `payBlockReward → AddCycleReward`（gated on `change_delegation`）改走 `kvReadWriter`，per-block 写入也走 buffer。(C) 新增 `BlockChain.Close()`：在 `cmd/gtron/main.go` 的 SIGINT/SIGTERM 路径上 `db.Close()` 之前调用，仅 flush 到 `latest_solidified_block_num`、丢弃 above-solidified 层（option A，匹配 java-tron `revokingStore` 重启语义）。3 新测试：`TestForkSwitch_AssetIssueActuatorRollback`（actuator-side rollback）、`TestForkSwitch_AddCycleRewardRollback`（reward path rollback）、`TestGracefulShutdown_FlushesSolidified` + `TestGracefulShutdown_DropsLayersAboveSolidified`（shutdown 行为分别覆盖 flushed 与 dropped 分支）。全包绿，`-race` clean。**仍 deferred**：BuildBlock 在 producer 侧仍直接写盘（构建阶段 disk 写不可回滚——`InsertBlock` 通过 `applyBlock` 二次入 buffer）；进程崩溃（`kill -9`）丢失 above-solidified 层的回归靠 re-sync。 |
| genesis 文件加载器（slice 1，块 parity） | 完成 | 2026-05-02 | ce3dd7a | `core/types/merkle.go`：`MerkleRoot` mirrors java-tron `MerkleTree.createTree`（空→`Hash{}`、单 leaf 直返、odd-count 末尾 leaf 不复制）；`core/genesis.go`：genesis 现按 `Accounts` 顺序构造 TransferContract txs（含 negative-balance Blackhole），`txTrieRoot = MerkleRoot([SHA256(tx.proto bytes)...])`，`BlockHeaderRaw` 不再写 `AccountStateRoot/WitnessSignature/Version`，`WitnessAddress` = 98 字节 famous-quote。java-tron 私链 genesis BlockID `75da3fe749503edb5d6121d96d450b980294a03648934988`（前 8 字节归零）byte-for-byte 一致。`Account` 创建仍调 `statedb.Commit()`（写 in-memory 账户），但返回 root 不进 header。 |
| genesis 文件加载器（slice 2，CLI flag） | 完成 | 2026-05-02 | 3c606c2 | `cmd/gtron/genesis_file.go` JSON loader（`chain_id` / `p2p_version` / `timestamp_ms` / `parent_hash` / `accounts` / `witnesses` / `dynamic_properties`，address 支持 hex `41…` 与 Base58 `T…`，balance 允许负数）；`--genesis <path>` flag 加在 root + `init` 子命令；与 `--testnet` / `--dev` 互斥；`cfg.NetworkID` 自动覆盖为 `genesis.Config.P2PVersion`。`test/fixtures/cross-impl/java-tron-private.json` 入库；`TestLoadGenesisFile_JavaTronPrivate` 验证派生的 genesis hash 与 slice-1 的 byte-for-byte 一致。 |
| cross-impl 块同步互操作（slice 3） | 完成 | 2026-05-02 | 1b323ed..3d029f6 | gtron 与 live java-tron 私链对接的 5 处 parity 修复：(1) `1b323ed` `SyncBlockChain` summary 改升序、batch 起点去重、`FETCH_INV_DATA` 接 3 msg/s 速率门——首次完整同步 71190 块。(2) `7122d3f` genesis loader 写 `next_maintenance_time = genesis_ts + interval`（mirror java-tron `Manager.initGenesis`），否则 `applyBlock` 的 `NextMaintenanceTime() > 0` 永远 false，maintenance 不触发；`listwitnesses` 暴露 `totalProduced/totalMissed/latestBlockNum/latestSlotNum` 用于 cross-check。(3) `aa51ddf` 三处协议门：outbound TRX (0x01) 改走 TRXS (0x03，`protocol.Transactions` 包装单条)，java-tron 对裸 TRX 答 NO_SUCH_MESSAGE；只读路径（GetAccount/GetContract/getReward）从 `block.AccountStateRoot` 改读 `BlockChain.HeadStateRoot`（cross-impl parity 后 header root 永远空）；sync watchdog 在没有 better-head 时 fallback 到任意 handshaked peer，`HandleChainInventory` 仅在响应是单 id 且等于本地 head 时退出 sync——这是 java-tron `needSyncFromUs` 翻成 false、INV 广播放行的转折点。(4) `906450a` 创号 tx 走专用 bandwidth 路径（个人 staked-bandwidth + create_account_fee fallback），绕开 free-quota 和 tx-byte fee；fee 烧入或入 blackhole 由 proposal #49 决定；`total_create_account_cost` 计数从 actuator level 移到 bandwidth path。(5) `3d029f6` `state_flag` DP key（mirror java-tron `lastHeadBlockIsMaintenance`）：旧的 `previousHeadTimestamp >= NextMaintenanceTime` 启发式在 `applyBlock` 时永远 false（DoMaintenance 已经把 NextMaintenanceTime 推到下一周期），导致 `SlotForTime` 漏算 `+MAINTENANCE_SKIP_SLOTS`，每周期 totalMissed 偏离上百万。**已验证**：H=78179 时全 71190 块 hash byte-identical；4 个 SR DPoS 计数器 byte-identical；双向 TransferContract 传播；fee 帐目（fee=100000、net_fee=100000）双侧一致。**Slice 3 follow-up（不在范围）**：`scripts/system_test_cross.sh` 自动化（追加在本表后续行）；`docs/dev/p2p-interop-status.md` 已落档；mainnet genesis-hash audit 仍待 M0″ Phase 2。 |
| M12.1 ExchangeTransactionContract VERSION_4_8_0_1 reject | 完成 | 2026-05-02 | 9261636 | 新 `core/forks/version_pass.go::PassVersion`（stateless reader API，复用 ForkController 既有 `passFromStats` 内核）。`core/txpool/pool.go::Add` 无条件 reject `ExchangeTransactionContract`（mirror java-tron `Manager.pushTransaction` 的 unconditional reject，错误串 `"ExchangeTransactionContract is rejected"` 与 java-tron wire-format 一致）。`core/state_processor.go::ApplyTransaction` 在 actuator 派发前对 `ExchangeTransactionContract` 调 `forks.PassVersion(db, 33, blockTime, dp.MaintenanceTimeInterval())` 早 reject（mirror java-tron `Manager.processBlock::rejectExchangeTransaction`，pre-fork 区块仍能 replay 因为 PassVersion 在 quorum 未达时 false）。3 类新单测：mempool unconditional reject 锁定、block-apply post-fork reject 锁定、block-apply pre-fork replay-safety 锁定。28 包绿。Mirror java-tron `45e3bf88ca` (PR #6507, master VERSION_4_8_0_1, 2026-01-13)。 |
| M12.2 AssetIssue FrozenSupply VERSION_4_8_1 overflow gate | 完成 | 2026-05-02 | b04a995 | `actuator/asset_issue.go::Validate` FrozenSupply 循环内追加 v34 fork-gated 溢出检查：`frozenPeriod = f.FrozenDays * params.FrozenPeriod`（**故意**沿用 Java `long *` 静默溢出语义，不引入 strict-multiply——parity 优先于改良）；`sum = c.StartTime + frozenPeriod` 走 signed-add 溢出检测，溢出时返回 `"Start time and frozen days would cause expire time overflow"`（错误串与 java-tron 一致）。4 个新单测：gated-off replay safety、post-fork overflow reject、post-fork realistic-values pass、boundary-at-MaxInt64（just-fits + just-overflows）。Mirror java-tron `44a4bc8263` (v4.8.1 release `AssetIssueActuator`, 2026-02-04)。 |
| 跨实现差异批次（D-1..D-10） | 完成 | 2026-05-07 | b8b851e..4faf86a | `make system-test-cross-flows`（私链 cross-impl 比对）暴露并修复的一批共识/wire-format 差异：VM 能量费扣减 parity（`952a3b3`）、dynamic-energy factor 接 inline op cost + EXTCODECOPY base（`b8b851e`）、producer 端 `payBlockReward` 不再直写 rawdb（`97bd726`）、`StateDB.FlushWitnesses` 把 VoteCount/URL 跨块持久化（D-2.c，SR allowance 10.2M sun 漂移根因，`6e50dfb`）、WitnessUpdate URL + WitnessCreate 记录持久化（`4895bcc`）、V2 freeze/unfreeze 更新 `total_*_weight`（`540a467`）、`consume_user_resource_percent` origin-stake 拆分（`8748568`）、`SPECIAL_TIER` 门提案 #65（`de4cb47`）、proposal 输出走 MapEntry 数组 + `proposal_id` 从 1 起（`7b202d4`/`42c597f`）、DelegatedResourceInfo snake_case wire-format（`3c3ce3e`）。D-1..D-10 全部关闭，cross-flows 在 H=123154 时 37/37 assertion byte-equal。详见 `docs/dev/cross-impl-divergences-2026-05-02.md` + `docs/dev/d2c-investigation-2026-05-03.md`。 |
| Nile genesis 校正 + fixture-capture live HTTP | 完成 | 2026-05-09 | e19ea7b..10db448 | Nile 跟链引导：mainnet genesis byte-equal（`13a7b18`）、drop Hello timestamp skew check（`1bc6376`，java-tron parity）、sync 期间忽略 adv block + 去掉 state-less 静默 fallback（`bbe4684`）、TransferAsset/ParticipateAssetIssue 接受 pre-fork 字面 asset name（`0762b8c`/`972d8c2`）、`--testnet` 时从 genesis 派生 networkID（`dca5b94`）、缺省 seed `next_maintenance_time`（`4a4188a`）、Nile genesis 校正（parentHash + accounts + 27 GR witnesses + seed list，`10db448`）；`cmd/fixture-capture` live HTTP 抓取驱动落地（`e19ea7b`/`a2791f8`，附 Phase 2 live-HTTP 约束文档）。 |
| Nile soak parity 波次 | 完成 | 2026-05-15 | f3d6d46..9c38403 | 从 genesis 跑 Nile soak 暴露并修复的 parity 缺口：genesis witness 建 Account + `IsWitness=true`（`f3d6d46`）、`ProcessProposals` 接入维护边界（此前提案永远 PENDING，`0633655`）、bandwidth 在 supportVM 下按 contract 算 `clearRet+64`（`072c510`）、Nile DP override `proposal_expire_time=600000` + `maintenance_time_interval=600000`（`9f8de49`/`41f8d71`）、维护周期 parity（block #1 skip + GR vote strip，`8167699`）、consensus 相关读取改用上一块时间戳（`870fe79`）、`is_jobs` 在 genesis + 维护轮换两侧接线（`e1b9920`）、AssetIssue 在 issuer 账户上持久化 `asset_issued_name/ID` + `frozen_supply` 条目 + JSON map 形状对齐（`24d6a4a`/`a54c4e3`/`723da1c`）、IsJobs 跨维护边界测试 + `SetActiveWitnesses` 走 rewind buffer（`0d749ac`/`9c38403`）。soak 已于 2026-05-15 从 genesis 干净重启验证。详见 `docs/dev/nile-soak.md`。 |

**退出门追踪**

| 门 | 依赖 | 状态 |
|---|---|---|
| G1 可跟链 | M0′+M1+M2+M3+M0″ | ❌ (M0″ Phase 1 完成；Phase 2 是 G1 的最后一块) |
| G2 生态可用 | M4+M5+M9 | ✅ (2026-04-27: M4+M5 路由完成 + M9 所有 P0 wire-format/solid-head/dev-mode 修复落地，全包绿) |
| G3 可当验证人 | G1+M6 | ❌ |
| G4 主网前置就绪 | G1+G2+G3+M7+M8 | ❌ |
