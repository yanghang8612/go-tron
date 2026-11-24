# java-tron 正式版兼容性核对（2026-11-24）

核对范围为 2024-11-24 至 2026-11-24 发布的 10 个主网正式版，Go 基线为 `d05f2a060`。逐版对照 release notes、标签间源代码差异和 Go 的执行入口。主网以 `GreatVoyage-v4.8.2.4` 为准，Nile 另查其 PQ1 标签。[主网发布记录](https://github.com/tronprotocol/java-tron/releases) · [Nile 参考代码](https://github.com/tron-nile-testnet/nile-testnet/tree/GreatVoyage-Nile-v4.8.2.4-PQ1-build1)

## 分版本结果

| 正式版 | 发布日期（UTC） | Go 基线已有的行为与本次处理 |
|---|---|---|
| [4.7.7](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.7.7) | 2024-11-29 | 已有 TIP-697 的 StrictMath 路径，并保留历史交易所定价的旧舍入规则。Java backup 进程和事件插件的生命周期属于两端运维差异。 |
| [4.8.0](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.0) | 2025-04-29 | 已有 Cancun 的 TLOAD、TSTORE、MCOPY、blob 指令和 SR 排序开关。本次修正 JSON-RPC 固化高度标签及日志范围和子主题限制。Java Event Service V2 支持从指定历史高度向插件重放事件，Go 原生事件归档另有接口。 |
| [4.8.0.1](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.0.1) | 2026-01-12 | 已有历史 TRC10 交易所限制。本次修正后续参数 98 生效后恢复交易的条件，使交易池、出块、验块使用各阶段的相应规则。 |
| [4.8.1](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.1) | 2026-02-04 | 已有 TIP-6780 和 `eth_getBlockReceipts`。本次补实时票数分页（含尚未结算的投票）、库存消息检查、批量及单笔收据的累计能量和价格。普通见证人列表继续返回持久票数。 |
| [4.8.1.1](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.1.1) | 2026-06-02 | 已有分叉控制的 CREATE2 和 MODEXP 特殊处理。结合后续版本检查本地执行期限与历史回放，防止本机耗时影响已签区块的回执。 |
| [4.8.2](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.2) | 2026-07-15 | 已有 Osaka 的 CLZ、MODEXP、P-256、TIP-2935、资源窗口及签名预编译规则。本次把 TIP-2935 的系统写入接入新版存储，补调用字段 `input`、Wallet GET 行为、常量调用超时、gRPC 并发流和两项 Prometheus 指标。重新生成了 Wallet 的 proto 描述符。 |
| [4.8.2.1](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.2.1) | 2026-07-31 | 非强制升级。上游改 HTTP 性能并升级 Java libp2p、grpc-java，没有新增 SR 投票版本。用 4.8.2.4 的官方节点验证了 P2P 同步，HTTP 吞吐和所有对端组合缺少基准。 |
| [4.8.2.2](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.2.2) | 2026-09-08 | 补 SR 版本门槛、合约部署时的哈希字段校验，以及 Stake 2.0、SELFDESTRUCT 的执行限制和调用帧回滚。Go 已在一次执行的嵌套调用中复用指令表。 |
| [4.8.2.3](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.2.3) | 2026-09-30 | 补治理参数 99、新旧存储键和访问同一旧行时的判定，补 BN128 pairing 的本地期限检查。JSON 兼容主要检查 Wallet、RPC 的输入输出，Java Jackson 与 Go 解码器的所有差异未逐项验证。 |
| [4.8.2.4](https://github.com/tronprotocol/java-tron/releases/tag/GreatVoyage-v4.8.2.4) | 2026-10-22 | 补治理参数 102 独立启用、新旧存储行的读写与零值、EXTCODEHASH 空代码元数据和回滚，联动历史查询与并行执行。 |

## 主网和 Nile 的差别

| 项目 | 主网 | Nile |
|---|---:|---:|
| 修改前 Go 出块版本 | 36 | 37 |
| 4.8.2.2 / 4.8.2.3 / 4.8.2.4 出块版本 | 37 / 38 / **39** | 38 / 39 / **40** |
| 接受参数 99 / 102 的 SR 版本 | 38 / 39 | 39 / 40 |
| `getchainparameters` 对齐的 Java 字段数 | 81 | 83 |
| 11 月 24 日抽样的区块高度与头版本 | 88,272,497 / 39 | 72,982,829 / 40 |
| 抽样的参数 99 / 102 | 0 / **1** | 0 / **1** |
| 设置参数 102 的链上提案序号 | 108 | 20789 |
| 时间戳达到该提案到期时间的第一个区块 | 87,649,101 | 72,265,211 |

两个区块的时间戳都恰好等于对应提案的到期时间，前一块早 3 秒。这两个高度可作为核对维护期处理的入口。更早的软件版本投票和提案创建同样影响回放起点。[主网提案 108](https://api.trongrid.io/wallet/getproposalbyid?id=108) · [主网区块 87,649,101](https://api.trongrid.io/wallet/getblock?id_or_num=87649101&detail=false) · [Nile 提案 20789](https://nile.trongrid.io/wallet/getproposalbyid?id=20789) · [Nile 区块 72,265,211](https://nile.trongrid.io/wallet/getblock?id_or_num=72265211&detail=false)

SR 先达到软件版本投票门槛，提案随后进入治理流程，到维护期才写入动态参数。Nile 的版本 33、34 分别对应 4.8.1、4.8.0.1，版本 37 对应 PQ1。这三版的投票比例分别为 80%、70%、80%。主网这三个版本的比例分别为 70%、80%、70%。Go 现在按网络解释位图，交易校验、历史执行和出块使用相应规则。[主网分叉表](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2.4/common/src/main/java/org/tron/core/config/Parameter.java) · [主网链参数](https://api.trongrid.io/wallet/getchainparameters) · [Nile 链参数](https://nile.trongrid.io/wallet/getchainparameters)

两条链的实际组合都是只启用 102。此时读取旧存储行保留原行，修改时才迁移，明确写入的零值可遮住旧值。99 单独启用时，读取旧行也会触发迁移。区分这两种情况会影响后续 SSTORE 的能量、回滚和按块查询的结果。EXTCODEHASH 同样区分缺省代码哈希、已存哈希、空代码以及含 STOP 的非空代码。[Java 存储实现](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2.4/actuator/src/main/java/org/tron/core/vm/program/Storage.java) · [Java 回归用例](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2.4/framework/src/test/java/org/tron/common/runtime/vm/StorageMigrationTest.java)

## 独立验证

1. 从主网发布标签 `4e8389f159` 运行 24 个 Java 原生测试，覆盖 TIP-2935 历史区块哈希、两种迁移模式、撤销已执行交易，以及 EXTCODEHASH 元数据与回滚。
2. 用同一发布的官方 `FullNode-aarch64.jar` 启动本机隔离私链。JAR 的 SHA-256 为 `a185d0ddff6e3cebd69ac46b7f6c6fa8051c7d4c2e9fe8c55be0867bd06a21d4`。普通跨节点脚本通过 13 项，跳过 1 项（一个节点省略零值 `totalMissed`）。
3. 另建单 SR 的同创世私链，由 Java 出块，在链上创建和批准参数 102，保持 99 为 0。用带 Sapling 的 Go 二进制和空数据库从创世通过 P2P 同步。第 1–57 块的区块 ID 全部相同。逐笔比较下表七笔交易的块号、费用、执行返回值、回执结果与能量。区块 ID 反映块头的一致性，回执及存储另行对照。

| 私链动作 | 块高 | Java / Go 能量 |
|---|---:|---:|
| 部署单槽合约 | 8 | 4,821 |
| 旧布局写入 7 | 9 | 20,027 |
| 创建 / 批准治理提案 | 10 / 11 | 0 / 0 |
| 新布局写入 11 | 55 | 5,027 |
| 合约读取 11 | 56 | 85 |
| 清零 | 57 | 5,027 |

Go 归档 RPC 在第 9、54、55、57 块分别返回 7、7、11、0。Java 和 Go 的最新状态都返回 0。对这七笔交易分别调用两端的 `eth_getTransactionReceipt` 和 `eth_getBlockReceipts`，14 组输出的全部 14 个字段一致，包含生效价格、累计能量、交易地址以及 JSON null。仓库的五块合成冒烟样本由 Go 工具重新生成，只有动态参数摘要和相应派生摘要变化。独立实现的对照来自上述 Java 私链。

**API 行为。** 对照了 4.8.2 的 10 条直接受影响的手写 GET 成功路径。`getburntrx`、`getReward`、`gettransactioncountbyblocknum` 分别覆盖 Wallet、Solidity、PBFT，另有 Wallet 的 `getpendingsize`。Proto 更新移除了 56 条 HTTP 注解，gRPC 描述符仍有 6 个服务、203 个方法。JSON-RPC 和 WebSocket 的重组测试覆盖旧日志撤回、同一交易出现在两条分支以及替换失败的情况。成功切换时先撤回孤块日志，再依高度顺序推送新分支。[Java GET 变更](https://github.com/tronprotocol/java-tron/pull/6699) · [Java 重组通知变更](https://github.com/tronprotocol/java-tron/pull/6819)

私链上另外对拍了主网配置下全部 81 个链参数的字段顺序与数值、上述四个 Wallet GET 的字符串响应，以及同一块中三笔调用的单笔和批量收据。第 476 块这三笔调用各耗能量 85，两个节点返回的累计值依次为 85、170、255。

前 57 块的 Java 原始 protobuf、对应创世配置和独立观察值一并保存在仓库。以下命令使用内存数据库复验签名、区块、参数切换、存储值与七笔回执。[样本说明](../../cmd/gtron/testdata/java-4.8.2.4-storage/README.md)

```sh
go test ./cmd/gtron -run '^TestJava4824StorageProposalReplay$' -count=1
```

使用 Go 1.25.5 构建了带 Sapling 的 `gtron`。冻结代码后运行 `make test`，含测试的 64 个包全部通过，另有 17 个包无测试文件。`go vet ./...`、影响范围内的竞态检测、Sapling 标签下的执行器与 TVM 定向测试均通过。`make conformance-replay` 的合成样本通过 5 块，允许差异清单命中 0 项。

## 主网交易与旧版存储提交顺序

2026-08-19 11:50:51（北京时间），主网第 **85,480,074** 块中，交易 `b5e06b0e4b620b4d5d66fc709650cf0b41d742039a78ba3f803be0c69657a8c5` 成功部署 `V0StorageCollisionProbe`，地址为 `TGBpa57B3uuRTzdpDwg3WW5h1QdxwS3UzR`。区块头版本为 36，对应 4.8.2 的主网版本号。收据记录能量 28,505，能量费 2.8505 TRX、带宽费 0.485 TRX，共 3.3355 TRX。交易早于 4.8.2.3 和 4.8.2.4 的正式发布，分析适用当时的旧存储路径。[交易](https://tronscan.org/transaction/b5e06b0e4b620b4d5d66fc709650cf0b41d742039a78ba3f803be0c69657a8c5/overview) · [固化收据](https://api.trongrid.io/walletsolidity/gettransactioninfobyid?value=b5e06b0e4b620b4d5d66fc709650cf0b41d742039a78ba3f803be0c69657a8c5)

构造代码对两个不同的逻辑槽分别写入 22 和 0，两者对应同一条 V0 物理存储行。部署日志中的 32 字节数据为十进制 22，反映构造执行期间的观察。部署留下的 12 字节运行时代码只读取存储。11 月 24 日用 TronGrid `eth_getStorageAt(latest)` 查询这两个已使用的位置，都返回了 32 字节零。读取前节点头为 88,274,872。指定部署高度时服务返回 `-32602`，只支持 `latest`。当前读数与提交时最后删除该行相符，公开收据没有记录缓存遍历轨迹，亦未取得该块刚提交时的存储快照。[TronGrid 存储查询](https://developers.tron.network/reference/eth_getstorageat) · [部署元数据](https://api.trongrid.io/wallet/getcontract?value=414433701a678936b5c1f01ea38a6bc7e1294726bb)

| 情形 | Java 旧版提交语义 |
|---|---|
| 同一个逻辑槽被反复写入 | 缓存只保留一个条目。最终值为零时删除，为非零时写入。 |
| 两个逻辑槽对应同一物理行 | 缓存中有两个条目。根 `Repository` 提交时，`Storage.commit()` 按该 `HashMap` 的实际遍历顺序处理脏行。先写后删最终为空，先删后写最终保留非零值。Java 没有规定“删除优先”或“最后执行的 SSTORE 优先”。 |

原 Go 实现（`876054cda`）在 TVM 中按逻辑槽缓存，区块提交时将脏槽按字节序排序，再依次生成物理行变更。本例按旧 Go 的排序也是先写 22、后删，因此没有证据表明这笔交易本身产生了持久状态分歧。该排序方法对其他键集、缓存复制与同一区块内的后续读取无法保证 Java 顺序。修复将旧布局的物理行决议移到成功的交易执行边界，保留子调用成功合并与失败回滚，以及新版治理参数使用的独立路径。[Java Storage](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2/actuator/src/main/java/org/tron/core/vm/program/Storage.java#L86-L105) · [Java Repository](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2/actuator/src/main/java/org/tron/core/vm/repository/RepositoryImpl.java#L1050-L1058)

回归测试分别检查最后一次物理操作为删除和写入的结果，并验证子调用回滚、外层提交和同一区块内后续交易的读取。另有用例先预热逻辑槽缓存，再提交物理行，确认复用同一个 StateDB 跨块读取时能看到新值。[提交顺序与调用帧测试](../../vm/legacy_storage_test.go) · [物理行与缓存测试](../../core/state/tvm_storage_test.go)

## 仍有差距及验证边界

- **已运行节点的升级。** 新规则会改变部分交易的执行或 Go 内部状态承诺。若旧节点曾越过相应 SR 投票或交易并按旧逻辑计算状态，应从最早受影响区块之前、已核验的状态重放。直接替换二进制不能重建已漏记的治理票、元数据或存储行。本次未对现网数据库做迁移，也没有在主网或 Nile 历史数据上连续跨越全部激活点重放。
- **事件消费。** Java 的 Event Service V2 历史推送与 V3 Kafka/MongoDB 插件有各自的消息格式、游标和回滚约定。Go 提供原生归档和 JSON-RPC 订阅，已有 Java 插件消费者直接接入仍缺兼容出口。[上游插件 v3](https://github.com/tronprotocol/event-plugin/releases/tag/v3.0.0) · [Go 事件订阅设计](../superpowers/specs/2026-04-26-m8.2-event-subscription-design.md)
- **客户端限流。** Java 的非阻塞选项切换既有全局、来源 IP 和端点三级限流器的等待方式。Go 已补 gRPC 并发流上限，Wallet、JSON-RPC 和 gRPC 仍缺这套共享请求配额及对应的超限响应。[Java 全局限流](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2/framework/src/main/java/org/tron/core/services/ratelimiter/GlobalRateLimiter.java) · [Java HTTP 行为](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2/framework/src/main/java/org/tron/core/services/http/RateLimiterServlet.java)
- **接口与运维。** Go 日志查询使用 Java 的默认区块跨度（块号差 5,000）和子主题上限（1,000），暂未提供同等配置项。部分旧 Wallet 路由仍有默认 JSON 整数格式差异。Java 早在 4.7.7 已提供的 JSON-RPC `buildTransaction` 在 Go 仍无同名方法。Java 的 active/passive/fastForward/backup 主机名配置、RocksDB 备份、JVM 工具及插件生命周期与 Go 部署方式有差别。Go 种子节点可使用主机名。两项 Prometheus 指标已接入。Go 在提交发布区块后统计交易数，Java 在推块入口采样，拒块时样本数可能不同。[Java RPC 定义](https://github.com/tronprotocol/java-tron/blob/GreatVoyage-v4.8.2.4/framework/src/main/java/org/tron/core/services/jsonrpc/TronJsonRpc.java)
