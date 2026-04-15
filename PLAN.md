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

## M0″ · 完整一致性回放 — **后置（M1 大头落地后）**

**前提**：M1.1 + M1.3 + M1.4 + M1.5 至少落地，此时跑回放能真正给出"还差在哪"的信号，而不是噪声。

**交付物**
1. `scripts/conformance_replay.sh` — 给定起止块高，从本地 java-tron mainnet 节点抓区块，喂给 gtron `state_processor`，每块末尾产出账户集合 + DP 的 digest 与 java-tron 同高值比对。
2. `scripts/system_test_cross.sh` — 1 gtron + 1 java-tron 双节点端到端。
3. `test/fixtures/mainnet-blocks/` — 3 段代表性区间（Freeze V2 激活前后、一次 maintenance 边界、合约密集块）。
4. `docs/dev/conformance-harness.md`。

**退出条件**：3 段语料在 master 上跑完，全量匹配；作为 G1 的准入检查。

**依赖**：M0′、M1 核心子项。

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

### M1.4 自适应能量上限（TIP-341）
- 在 DP 中补齐 §1.1 所列 6 个 key，在 maintenance 边界计算 `total_energy_current_limit` 新值。
- 引 `core/resource_adaptive.go`（新文件）；state processor 每块末尾更新 `average_usage`/`average_time`。
- **退出**：主网某次明显负载变化区间的 `total_energy_current_limit` 值与 java-tron `getchainparameters` 返回一致。

### M1.5 奖励算法 v2 & 委托奖励
- 引 `core/rawdb/accessors_reward.go` 与 `accumulated-reward` / `reward-vi` 前缀（见 M2）。
- `consensus/dpos/reward.go` 实现 per-cycle 累积奖励桶、动态 brokerage、`WITNESS_127_PAY_PER_BLOCK` 候补奖励。
- **退出**：一个完整 maintenance 周期内，27 SR + 100+ 候选的账户余额增量与 java-tron 一致。

### M1.6 存储租金（StorageTaxProcessor）
- 新建 `core/storage_tax.go`；合约存储余额按时间衰减。
- 接入 DP 的 `total_storage_*` 簿记。
- **退出**：长期合约账户的 storage 字段在 M0 回放中匹配。

### M1.7 动态能量价格（TIP-1327）
- `allow_dynamic_energy` 激活后，合约调用能量成本按 24h 调用量缩放。
- **退出**：压力测试脚本下能量消耗随调用量线性变化。

### M1.8 Freeze-V2 委托资源消费账
- 委托方带宽/能量从被委托方 quota 扣除；解委托后反写。
- **退出**：专项单测 + M0 回放匹配。

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

**范围**：TODO §2.1。

**交付物**
1. `net/pbft_handler.go`：PBFT 消息（0x40 起）类型路由、签名校验、去重 cache（10 分钟过期）、stripelock 并行化。
2. `net/pbft_data_sync.go`：quorum 数据补齐通道（独立于块同步）。
3. PBFT 消息源码级参考：java-tron `framework/src/main/java/org/tron/core/net/messagehandler/PbftMsgHandler.java` 和 `PbftDataSyncHandler.java`。
4. 与 `consensus/dpos` 的集成：出块者产生 pre-commit / commit 签名；收集 2f+1 后落盘到 M2 PR-2 的 `pbft-signdata` 前缀。
5. CLI flag `--pbft.enable`；默认全节点启用（只中继），持 SR 私钥时启用签名发送。

**退出**：2 SR gtron + 1 SR java-tron 组成小型测试网，PBFT 轮次能正常推进；mainnet 观察窗口 48h 无被踢出的 DisconnectReason。

**依赖**：M2 PR-2（witness-schedule / pbft-signdata）、M3.1（peer 状态机）。

**文档**：新建 `2026-04-XX-pbft-message-routing-*`。

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
| M0′ Fixture 抽取工具 | 未开始 | — | — | 轻前置，先做 |
| M0″ 完整一致性回放 | 未开始 | — | — | M1 核心落地后再做 |
| M1.1 DP backfill | 未开始 | — | — | 依赖 `2026-04-12-hard-fork-mechanism` 现状 |
| M1.2 Freeze V1 | 未开始 | — | — | |
| M1.3 版本位 + 分叉 audit | 未开始 | — | — | |
| M1.4 自适应能量 | 未开始 | — | — | |
| M1.5 奖励 v2 + 委托奖励 | 未开始 | — | — | |
| M1.6 存储租金 | 未开始 | — | — | |
| M1.7 动态能量 | 未开始 | — | — | |
| M1.8 委托资源消费 | 未开始 | — | — | |
| M2 rawdb schema 补齐 | 未开始 | — | — | 分 6 PR |
| M3.1 sync 稳定性 | 未开始 | — | — | |
| M3.2 Adv/Relay | 未开始 | — | — | |
| M3.3 速率限制 | 未开始 | — | — | |
| M3.4 discovery 驱逐 | 未开始 | — | — | |
| M4 gRPC Wallet server | 未开始 | — | — | 分 5 PR |
| M5.1 HTTP 补齐 | 未开始 | — | — | 分 7 PR |
| M5.2 JSON-RPC 写路径 | 未开始 | — | — | |
| M6 PBFT 路由 | 未开始 | — | — | G3 依赖 |
| M7 TVM Cancun | 未开始 | — | — | 等待 TIP |
| M8.1 Solidity/PBFT API | 未开始 | — | — | |
| M8.2 事件订阅 | 未开始 | — | — | |

**退出门追踪**

| 门 | 依赖 | 状态 |
|---|---|---|
| G1 可跟链 | M0′+M1+M2+M3+M0″ | ❌ |
| G2 生态可用 | M4+M5 | ❌ |
| G3 可当验证人 | G1+M6 | ❌ |
| G4 主网前置就绪 | G1+G2+G3+M7+M8 | ❌ |
