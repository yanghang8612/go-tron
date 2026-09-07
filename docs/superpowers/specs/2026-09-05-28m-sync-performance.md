# 28M 历史同步性能重构方案

## P2g：按完整内容复用清单解码结果（2026-09-06）

P2f 后继续核查回收：`Runner.compactHistory` 默认已开启 `DeleteObsolete`，合并发布后会删除未被 published manifest 租约保护的源文件；retired 清单累计字节不能代替 Lstat 物理盘点。因此先核对物理存在、租约保护和已缺失分类，再决定是否重构回收，不根据约 800 GiB 的清单记账直接删除文件。

已确认的重复工作是多个生命周期阶段加载同一大型 manifest；此前 P2e 样本观测到该成本，本轮首份 25 秒样本未采到此阶段。P2g 保留每次 `os.ReadFile`，仅当刚读到的完整字节与私有缓存逐字节相等时复用已完成的解码、归一化、排序和结构验证。缓存不以 mtime、inode、generation、hash 或文件路径代替内容身份；读取错误和非法更新照常返回，不返回旧清单。段文件本体 SHA、语义校验和删除门槛不受影响。

缓存只保留一份已成功解码的私有视图及完整原始字节，常规与 production 验证结果分别处理。每个调用者取得独立的 Manifest、Chain、Progress、Segments 和 Retired 容器，字符串作为不可变值共享，保持 nil/空切片形状。并发 miss 串行准入，防止同一份 JSON 同时重复解析；`GTRON_SNAPSHOT_MANIFEST_CACHE=0` 可关闭并清空缓存。驻留计费上限 64 MiB，按完整输入容量、对象、切片容量和字符串加保守系数计费；超限回到原路径，不把缓存限额解释为整个进程的内存限额。

验证包括相同大小/时间/代数的内容替换、非法 JSON/生产布局、文件消失、调用者修改与发布、并发读写代数切换、空切片形状、超限回退，以及固定大清单的冷热/并发对照。通过本地和 Linux 检查后直接主网部署，分别报告清单组件收益与线上吞吐。

## P2f：V6 合并独立范围流与借用记录（2026-09-06）

P2e 后 05:57/06:00 UTC 两份 25 秒样本中，Pebble 压实分别为 7.19/10.95 CPU-s，当前没有大型历史 merge；不把前一轮大型 merge 的 SHA 热点当作当前持续热点。静态跟踪发现，V6 合并仍用同一个单块解压缓存读 record 和 StateTxRange，每跨区块会挤掉 record block；V5 已有独立范围流，但 V6 未接入。V6 每帧仍进行 prefix/payload 两次 ReadAt 和 payload 复制。

本轮将压缩 V6 合并接入既有 ReadRecordFrameAt 借用接口，范围表使用第二个独立、单块缓存的 reader 和单线程 range cursor。完整 frame 直接在当前解压块内解码，跨块 frame 才复制到可复用 scratch。keyID 映射仍在每个 source 开始时生成，范围覆盖及顺序检查不变。借用的 Prev 必须在下一次 record reader 操作前被同步编码；范围读取不得访问 record reader。源全量 SHA、字典摘要、逐条解码、输出 CRC/SHA、Sync/rename/manifest、历史布局和根保持不变。记录复制阶段仍依次处理 source；单个 source 增加一份压缩块目录、压缩 scratch 和一个解压块，不把块缓存预算冒充总内存上限。

验证压缩格式 1/2、频繁切换区块、跨块 prefix/payload、空/不存在/大 Prev、错序、非法 keyID、范围缺失和取消。与未压缩 V6 的既有路径比较最终三件套字节及完整历史校验。独立固定输入 Linux 基准确认收益后直接主网部署并观测；不要求 28M 重放。新增借用/跨块复制记录计数按 source 批量更新，统计已消费记录，不等同已发布记录。

P2f 第二候选：第一候选在同一组约 429 MB 生产历史文件上的三轮中位数仅改善 1.8%，不能据此宣称稳定整段收益。真实文件 profile 显示源 SHA 约占合并主调用链 CPU 的 38%（该调用链不包含异步压缩 worker）。将完整源校验改为可配置的 1/2 worker，代码默认 1；服务器同一组生产文件三轮中位数 8.050→6.660 秒（耗时 -17.27%，完整输出校验及 SHA 一致），据此在正式主网启用 2。P2f 于 07:34:58 UTC 完成主网部署，线上观察结果见 `docs/dev/28m-history-stream-p2f-2026-09-06.md`。每个源独立只读，结果与首个错误按输入顺序收集；全部源校验通过后才创建输出；错误/取消必须取消并 join 所有读任务后返回。保留完整 SHA 覆盖和 sidecar 检查，最多增加一个并发校验流，不将此项称为减少校验字节数。

2026-09-06 部署补充：P2b2 首次停止时，`maintainPeers → AddPeer → OnPeerConnected → buildHello` 在数据库关闭后继续读取 genesis，导致 `Block.Hash(nil)` panic。P2d1 在 P2P server 的 `mu` 下关闭连接准入，并以独立 WaitGroup 等待已准入握手及应用回调结束，再截取并关闭 peers；握手完成时再次检查停止状态。停止后的连接不能调用应用层。不改变 wire、握手内容或状态语义。加入阻塞回调与晚完成握手的确定性回归；同一 P2d1 二进制用于 iterator/Get 对照，隔离此修复的影响。

2026-09-06 实测结果：P2d1 已完成 iterator/Get/iterator/Get 四组主网对照并保留 Get；最后一对窗口 5.921→11.844 blocks/s、879→1,946 tx/s，commit/update 336.1→57.0 µs。64 个原始点持续 active。不同历史区间的负载差异随结果一并报告；完整负载/压实/内存与回退证据见 `docs/dev/28m-point-get-p2d-2026-09-06.md`。

日期：2026-09-05。状态：P0a 已实现、完成本地 StateDB 旧新基准，并按用户后续明确授权直接部署到主网。持续导入正常，委托索引 CPU 热点明显减轻；后续性能验收按主网直接部署和连续观测推进。在线观测和回退入口见 `docs/dev/28m-server-ab-2026-09-05.md`。

静态审计基准：go-tron `94da94aca85b95a6a992baf864f6836e8c402c73`；Java 参照仓库 HEAD `75c0304681cd32804c2f3c030468a88bdf3d8e32`。昨天采样的二进制以 profile Build ID 为准，不将其冒认为当前 HEAD。

## 决策

首先重构 **legacy 资源委托账户索引**，消除每笔 FreezeBalance 对双方完整关系列表的解码、线性查重和编码。它是本次原始 CPU 采样中最强的串行热点。随后压缩状态历史构造和资源状态读写成本，再优化 Commitment。真正的交易并行化放在这些工作之后。

第一阶段维持现有磁盘格式和逐块 internal root；通过专用 native codec、有界有序邻接结构、精确 no-op 检测先兑现收益。第二阶段允许重构物理存储与历史表示，但仍保留原来的逻辑状态行、历史视图和 root。直接更换内部 root 算法或提前启用 Java proposal 69 不在本方案中。

性能目标：主网实际同步吞吐争取提高 50% 至两倍，组合阶段争取两到三倍。2026-09-06 用户明确取消同一 28M 区间重放验收；采用主网直接部署、连续采样、按需切换对照及运行健康检查，不要求准备 28M checkpoint 或整链重放副本。这些是实验目标，不是已有结果。20 秒 profile 不能证明整个 28M 区间都具有相同热点比例。

## 证据与口径

原始文件：

- `/private/tmp/gtron-cpu.pb.gz`：2026-09-04 10:04:06 CST，20.13 秒，37.81 CPU 秒，平均 1.88 核；Build ID `44b3f5d79658e12cae3cf4be746b52586dabe196`。
- `/private/tmp/gtron-metrics-before.json`：高度 28,241,861。
- `/private/tmp/gtron-metrics-after.json`：高度 28,242,065。
- `/private/tmp/gtron-goroutines.txt`：同次现场栈。
- 固化的 profile 摘要及文件指纹见 `docs/dev/28m-sync-profile-2026-09-04.txt`。

重新运行 `go tool pprof -top -cum` 的结果：

| 调用栈 | 累计 CPU 秒 | 全进程 CPU 占比 |
|---|---:|---:|
| applyTransactionWithScratch | 13.06 | 34.54% |
| FreezeBalanceActuator.Execute | 9.73 | 25.73% |
| WriteDrAccountIndexLegacyDelegate | 9.69 | 25.63% |
| ReadDrAccountIndexLegacyStrict | 5.45 | 14.41% |
| writeDrAccountIndexLegacy | 4.22 | 11.16% |
| VMActuator.Execute | 1.18 | 3.12% |
| Interpreter.Run | 1.06 | 2.80% |

以上存在父子包含关系，不能相加。legacy index 约占交易执行累计 CPU 的 74%，约为 VM Execute 的 8.2 倍。这里的 74% 不是交易墙钟时间占比。

同次 metrics 两端的约 33 秒滚动窗口显示：

| 项目 | 观测 |
|---|---:|
| 同步 | 10.74–12.62 blocks/s，1,208–1,415 tx/s |
| 密度 | 约 112 tx/block，VM 约 36% |
| import | 79.1–93.0 ms/block |
| execute / transaction | 70.3–84.7 / 70.0–84.3 ms/block |
| VM 本体 | 折算约 14.2–15.7 ms/block |
| Commitment fold | 208 jobs，平均 27.42 ms/job |
| branch seek | 89,491 次，约 430/job |
| enqueue backpressure 增量 | 0 |

不同滚动窗口 gauge 不能相减推算 profile 期间的速率。`Apply` 包含重叠的异步提交；多 lane 累计 read/wait 不能加到主线程耗时上。当前近似关键路径是 `max(execute, commitment service) + publication/其他串行尾部`，不能把两项计时简单相加。

9 月 3 日的 `/private/tmp/gtron-main-resample.k9RYx0` 是 27.38M 区间；9 月 1 日 trace 是 23M 区间，均不作为本次 wall 分解。9 月 4 日晚 28.71M 的窗口已经与 canary 同机运行，随后观察到 NVMe 利用率 98.6%，不能把它用作版本性能对照。4.52M canary 的高 energy/s 也不构成并行提速证据。

## P0：legacy 有序邻接索引

### 当前为什么慢

`actuator/freeze_balance.go:132` 在原 fork gate 之前调用 legacy 路径。`core/state/delegation_store.go:277` 每次执行：

1. 读取 from 和 to 各自的整行索引。
2. strict reader 用 `statecodec.Unmarshal` 重建两个 protobuf 形状对象及全部地址切片。
3. 线性查找对端是否存在。
4. 无论列表是否变化，都用 `statecodec.Marshal` 编码双方整行，再进入 generic SystemDelegation KV。

账户关系数为 D 时，单次操作是 O(D)；不断新增关系时，累计构建成本可接近 O(D²)。重复委托虽然不新增边，仍然付出大部分整表处理成本。

特别注意：这是 `00 GTSV 01` 的 **native rooted-state codec**，不是 protobuf wire。已有 `rawdb.DecodeDrAccountIndexLegacy` 是 protobuf decoder，而且现有 32+32 地址 benchmark 不覆盖此 rooted 热路径。不能直接把那个 decoder 接进来。

### P0a：同格式的专用执行表示，首先落地

由 canonical StateDB 的 system stateObject 持有私有 `legacyDelegationCache`，包括：

- 两个独立的有序地址序列：FromAccounts、ToAccounts。
- `map[string]struct{}` membership 索引；列表少于 16 项时线性扫描，任意长度字节串保持原语义。索引只判断存在性，遍历和删除使用原有序序列。
- Account、Timestamp、未知字段；仅已存在的行准入，缺失与空记录保持区别。
- generation 与 LRU 链；上限 1,024 行和保守计费 32 MiB，计费包含地址 arena、所有仍可能存活的 slice backing、membership keys/buckets 和管理开销。

缓存仅是已编码 KV 的派生表示，不拥有 dirty/历史版本；原 native KV、journal、history 继续作为权威。修改前将私有记录移出缓存，写入成功后重新准入；generic 写入和回滚精确失效。这样不需要额外的 cache undo/COW，也不把可变 protobuf 借给历史或 StateDB copy。连续 arena 减少初始解码分配，真实增删仍经原 SetAccountKV 完整保留撤销和历史。

首次加载 O(D)，此后 membership 预期 O(1)。已经存在的关系，在确认两侧各自逻辑状态都不需要改变时，不再重新编码未变化的一侧。新增或删除只改对应序列；P0a 仍在原有观察边界生成相同 native 字节，真实变化的整行编码暂时仍为 O(D)。这一步不夸称所有委托操作已变成 O(1)。

为该消息实现专用的 native reader/writer：直接读取 framed fields、连续地址 arena，已知字段直接编码，省反射列表访问和中间 payload 拼接。输出必须与现有 `statecodec` byte-for-byte 相同；未知字段、合法异常形状及错误接受范围通过旧 codec 对照/fuzz 确认，不能降低 strict read 标准。

no-op 快路也必须尊重原 codec 的规范化：只有已经验证的规范行/逻辑像才能跳过。若旧路径会规范化合法但不同的输入字节，仍须完成该规范化。两侧仍按原执行顺序检查错误、修改与回滚。

结构由状态层拥有，不能是一个绕过 StateDB 的辅助缓存：generic SystemDelegation put/delete、反序列化恢复、reorg、snapshot rollback、proposal 转换及 reset 都必须更新或失效它；缓存命中仍要记录对应逻辑依赖。worker/API 不共享可变序列，返回副本或绑定版本的只读视图。异常 from==to 和同块重复 mutation 要按原路径比较，不能假设所有输入都来自正常链。

单个巨大账户超过 resident 预算时，不建立 membership、不准入缓存，使用专用 codec 与稳定列表扫描；其他行按 LRU 淘汰。所有缓存项均可丢弃，权威 dirty/历史数据仍由原生命周期持有。事务版本 reader 和 self-edge 保留原来的双记录读取/写入流程。resident 上限不冒充整进程内存上限，回退路径自身的大行临时内存仍要计量。

本地结果见 `docs/dev/28m-delegation-benchmark-2026-09-05.md`：D100K 驻留重复委托 18.630 ms → 159.4 ns；冷 StateDB 重复 18.313 → 5.568 ms；追加/回滚 18.700 → 7.688 ms；删除交易与历史发布 19.289 → 9.064 ms。它们是固定形状内存数据库微基准，不代表整链吞吐；真实重复率、缓存竞争、服务器 I/O 与轻块回退门槛仍须测量。

### P0b：分块物理表示和事务增量历史

如果新增/删除边的整行 materialization 在 P0a 后仍是主热点，再进入这一阶段：

- header + 有序页/槽 + membership，修改只触及少量页；删除用 tombstone，新增排尾。
- 历史记录每个 txNum 的边/页变化及可重建原像，配定期完整 checkpoint；读取历史时恢复该 tx 的完整逻辑行。
- 回滚使用 immutable page/COW 或精确 undo，不能把同一大 slice 借给后续交易修改。
- 原逻辑 SystemDelegation 行通过适配层物化。commitment 仍看到原来的逻辑 key 和 native value；新物理页不作为额外的逻辑叶子加入 root。
- 块内多次修改可共享页和原像，在不丢失每 tx 历史的前提下，仅对块最终变化的逻辑行执行 Commitment 输入编码。

少量页修改适用于正常唯一列表。异常重复项需要 occurrence 集合或稳定扫描回退，删除成本随实际受影响页数增长。完整历史列表查询仍至少 O(D)；checkpoint 必须约束 delta 重放长度，避免把写入成本转成无界查询成本。

**成本下界仍存在**：若一个大型逻辑行每块都变化，为保持现有 leaf hash，该块仍须处理完整逻辑行字节。普通 Keccak 不能在任意列表增删后 O(1) 更新最终摘要。此阶段消除的是每交易的全量解码/拷贝/历史写放大，不承诺每块 root 计算 O(1)。

此阶段是存储工程，不是把一个 map 改成另一个 map：rawdb、latest iterator、as-of reader、历史索引、snapshot build/restore、dbcompare、reset 和 migration 都要认识物理/逻辑边界。新 key family 在 `core/rawdb/schema.go` 统一定义。版本 marker 必须绑定基准 hash/txNum，迁移完整发布前旧表示仍权威；不能新旧混读半套结构。没有完成这些适配前不切换 canonical writer。

### Java 顺序约束

Java `DelegatedResourceAccountIndexStore.convert` 按当前存活列表的顺序生成 `timestamp=i+1`；Go 对应 `delegation_store.go:329`。必须保持：

- 新增排尾、重复添加不增项。
- 删除保持剩余元素相对顺序；异常重复项的删除与原行为一致。
- 删除后重加排尾；转换重新按存活序列编号，不能直接使用有洞的内部槽号。
- 空 legacy aggregate 的存在性、双方列表及错误传播不变。
- 原 proposal/fork 边界保持不变；不能直接提前改成现有 V1 directional index。

## P1：typed cell 与事务 effect log

这是 P0 后的通用化，避免先大改整个执行器。

现有代码已跨块复用 StateDB 和 CommitScope，并保留活跃 stateObject；已有 compact account、代码缓存、arena、标量 undo。不能把这些当作新方案。

仍确认存在的成本：

- 资源行 `materializeAccountResource` 首读/解码后，首次 `SetAccountKV` 为 preimage 再读同一行（`account_resource.go:38,65`；`account_kv.go:1759`）。
- history 开启时 scalar undo 退回完整 account 字节（`statedb.go:5023`）。
- 每 tx 收集 history 再扫描 journal、建四类临时 map、复制、排序（`domain_change_journal.go:135`）；该工作在串行交易循环内。
- read-ahead 已覆盖 account/metadata/code 和 TRC10，但普通 owner 的 permission、V1 bandwidth/resource 点行尚未系统预取（`read_ahead.go:327`）。

以 canonical cell 的 `original/current/presence/version` 和 first-write stamp 统一 undo、dirty IDs、历史及依赖来源。transaction effect log 保留每 tx 前像和顺序，Commitment 只取 block-final touched cells。嵌套 CALL/revert 与 block rollback 的标记分别维护；不能把事务历史压成只有块末状态。先移植 P0 和 Resource 域，验证后再推广。

预读目标通过真实账户 generation 和原 fork 上下文提取，仍是可丢弃 hint。异步填充不得覆盖 canonical 新值；按实际消耗命中率和关键路径 read wall 决定范围。DP 字符串 map 改 enum/数组只作为后续基准项目，当前采样没有证明它是主瓶颈。

## P2：给提速后的执行器准备 Commitment 容量

执行若下降到约 30 ms/block，当前约 27–31 ms 的 fold 就会接管瓶颈。

1. **PreparedOps**：生产 touches 已去重，却仍 raw-key sort，再在 Submit 内验证和 hashed-path sort。增加有界准备阶段，直接产生最终 path/valueHash 分桶，移出 publisher；不把哈希放回 foreground。保留通用 API 的输入检查，所有队列合计受现有 inflight 上限约束。
2. **原地 compact branch arena**：每 lane 独占紧凑节点和整数句柄，避免约 1.3 KiB BranchData 的整对象拷贝。先保持逐块原格式输出，以独立测量收益。
3. **延迟 branch 编码**：有收益后再做跨块版本层。每块仍 fold/hash/root，只在 flush/checkpoint 对最终 branch 编码；`ValueAt(solidifiedCutoff)` 必须准确，不能把未来执行值写入较早持久边界。
4. **group publish**：只合并已完成的 FIFO 前缀，成功后仍逐块执行 head、DP、hooks、stage、layer promotion。当前 Pebble batch 是 NoSync，收益是减少 batch/apply/WAL 入队，不能称为省掉每块 fsync。

已有 spec P4.48 记录 naive decoded-trunk 缓存曾使 150-block 基准多分配 7.2 MiB、慢约 1%。因此不再实施 `map[prefix]BranchData` 加缓存。现有 blockbuffer 已跨块 last-write-wins 合并；单纯加大 batch 也不是本方案。

epoch 必须配套 rollback、history reader、rotation generation、checkpoint manifest 和 journal 恢复。checkpoint durable 前不得清理其恢复日志。区分 execution head、对外 published head 与已同步落盘的恢复边界；NoSync 写成功不等于断电持久。首次实现不改变这些生命周期，只在原格式下替换内存结构。

不根据昨天 branch-rotation=0 就强制线上全量重建：先核对版本、配置、累计时间及 I/O 成本，单独 A/B 再决定。

## P3：真正减少串行工作的并行执行

现有 Transfer/VM 路径包含 canonical oracle，VM 还是稀疏 canary，并有历史错误与资格门禁。打开开关或删除 oracle 都不构成合格优化。

P0/P1 的统一状态访问与 effect log 稳定后，再构建准确的 point/prefix/absence/generation 读写版本。worker 执行，canonical 边界按原顺序校验及发布，冲突重试；public bandwidth、fee/burn、energy 等共享状态不能未经证明就当作可交换增量。字段级依赖只能在底层别名一致时成立。

证明阶段继续全量双执行比对；性能阶段需要完成独立发布资格，并保留运行期版本校验与故障处理。不能用关闭安全校验所得的数字作为上线收益。当前旧 datadir 的并行资格限制保留。

## 验收与停止条件

- 直接在已授权主网节点部署，固定资源与 history/API 配置，记录实际二进制、模式及部署前后连续窗口；必要时顺序切换复测。canary 保持停止。不同高度负载差异如实报告，不设置同区间重放门槛。
- 持续窗口记录 tx/block、VM 占比、energy/tx、后台阶段和债务，覆盖现场实际遇到的交易形状；局部固定输入差分与基准用于验证实现，不要求主网倒回历史高度。
- 同 schema 阶段逐 tx 比较列表/账户/资源/receipt/energy，逐块比较 native latest bytes、内部 root、Java 可见输出和原历史记录；存储分块阶段额外比较全部逻辑投影与历史 as-of 结果。
- degree 32/1K/10K/100K，覆盖重复率 0/50/100%、追加、删除、重加、双向独立缺失、异常重复、未知字段、proposal 转换。bench 必须经过真实 StateDB native 路径并单独统计 history/commit。
- P0 目标：热点 CPU-s/同量操作减少至少 80%，高 degree 重复操作不再随 D 线性增长；主网实际吞吐 +50% 至两倍为争取目标，结合多轮负载与后台工作判断。其他形状窗口单独报告并守住回退门槛，不要求每个 VM 密集窗口同幅提升。未达标时依热点和重复率决定是否进入 P0b，不能靠改交易配比过门。
- P2 目标：同量工作 fold p50 <12 ms、p99 <20 ms，真实 enqueue blocked wall / 有效同步导入墙钟时间 <0.1%；降低 profile 占比而绝对用时不降不算成功。
- 所有阶段要求空/轻块回退不超过 3%，内存严格有界，compaction debt 不持续发散；提高 ingest 后 cold-history/freezer 的完成速率必须跟得上，否则长期收益不成立。
- crash/restart 覆盖 mutation、history、metadata、checkpoint、marker、rotation 各边界；深回滚、嵌套 revert、storage failure、缓存过期与跨 fork 重放均不得失真。

主要看局部同量工作的完成时间、主网连续窗口的 blocks/s、CPU-s/block、actuator wall、allocated bytes/tx、native 编码字节、历史/branch 写量及后台债务。energy/s 只作负载描述和辅助指标。

实施顺序和任务边界见配套 `docs/superpowers/plans/2026-09-05-28m-sync-performance.md`。


## P2a 实施：预取路径所有权、按需等待与清单校验

2026-09-05 05:48 UTC 主网 29.219M 采样（P0a 二进制，30.17 s / 48.59 CPU-s）中，SnapshotLifecycle 为 10.94 CPU-s，Manifest.Validate 为 4.10 CPU-s；另一个 25.11 s 窗口 Manifest.Validate 为 0.75 CPU-s。维护是间歇负载，不能只选一个窗口推算整链收益。78 s 指标差值中，depth-5 预取 durable/useful 为 62,331/61,711，depth-6-plus 为 51,515/50,459，说明直接关闭预取缺乏依据。

当前每 lane 的首层预取整批完成后才允许折叠。这不是状态一致性边界：前台与预取本来就各自拥有 snapshot-scoped cursor，同键读取由 session 内 singleflight 合并。新增启动期操作开关 `GTRON_COMMITMENT_PREFETCH_OVERLAP=1`，让前台立即执行并只在读取同一物理键时等待；默认保持旧调度用于现场对照。所有预取仍必须在 job 发布、session 关闭、scratch 归还之前 join，inflight 上限、逐 lane 跨块顺序、根和分支编码不变。`state/commitment/pipeline/prefetch_overlap` 标记实际模式。

删除/重插入的 Pebble 多块 race 测试同时暴露旧调度的真实竞态：`livePutsInPlace` 整理 fold-owned ops 时，原本已经异步运行的 lookahead 仍在读同一数组。首层 barrier 不能保护这一路。现在提交前提取独立 `[]common.Hash` 路径镜像供两个预取层共享，只在全部完成后回收。路径没有 GC 指针，pool 单个 backing 保留上限 65,536 项（2 MiB）；超大块完成后释放，既有 job 数上限不变。该所有权修复也适用于关闭 overlap 的模式。

清单校验复用重复路径检测建立的索引，按路径查找 history 的 index/accessor，同时保留 dataset、kind、范围和 aggregationSteps 的全部检查。event-log index 校验在已排序且已验证不重叠的同一分片族中维护单向游标，避免每个索引重新扫描全部已过去的 event refs；连续覆盖和边界检查仍由原函数执行。不缓存跨调用 manifest，不修改文件格式或校验范围。

本地同量清单基准（完整 Validate，Apple M1 Max）：2,560 个分片约 6.57 ms → 1.44 ms；10,240 个分片约 86.28 ms → 5.77 ms。分配基本不变，不能把这 4.6–15 倍局部收益当作同步吞吐提升。正式验收需比较相邻完整窗口的 tx/block、energy、ms/tx、commitment ms/block、IO 和冷数据推进，并保留不同高度非严格 A/B 的限制。

## P2b：历史压缩文件一次写成

P2a 现场采样把另一处成本具体化：06:46 UTC 的压实记录复制中 zstd 占 16.23/53.04 CPU-s；06:53 UTC 的 finalize-history 中，`finishWithPrefixMetadataContext` 的完整 body 复制和 SHA-256 占 13.12/32.97 CPU-s。当前压缩格式将索引表放在文件头，压缩大小确定后，需要把已写出的 compressed body 再复制一遍；这一轮 finalize 阶段实际持续约 4m34s。以下为最初候选设计；本轮已实现 footer 格式，实施约定及逐项验证结果见下文和 P2b 实验报告。

- 新版本将可变大小索引表放到 footer，固定 trailer 标出 footer 的位置与长度。固定文件头只包含提前已知的魔数、版本及格式标记。
- 继续保留可回填的第一个逻辑 chunk 在内存；后续 compressed chunks 直接顺序写到最终 staging 文件。完成时写第一个逻辑 chunk、逻辑顺序索引表和 trailer，允许第一个逻辑 chunk 的物理位置位于 body 之后。
- 每次输出同时更新现有整文件 SHA-256，保留 manifest 的 `sha256:` 语义。这样减少 compressed body 的第二次完整读写，不以取消校验或改用弱哈希换速度。
- 读取端按版本分派；新格式校验 footer/trailer 边界、溢出、逻辑连续性、物理覆盖、chunk 不重叠及整文件 checksum。旧格式读取保持支持，manifest 路径/范围和逻辑历史字节语义保持一致。
- 首先部署只增加读取支持、写开关仍关闭的版本；差分、损坏、取消、断电恢复及 snapshot 安装/裁剪证明通过后再启用新写入。新格式一旦发布，回退二进制必须具备新读取能力，不能沿用 P0a 的直接二进制回退入口。
- 验证以相同历史源构建，比较所有逻辑记录、字典/索引/as-of 查询、压缩比、总读写字节、完整 finalize 时间与同步吞吐。格式版本和 rollout marker 的具体存储位置须在实现前按 schema 约定确定。

这条路线直接减少大型压实的额外 I/O，优先级高于继续增大缓存或只移动等待点。仍需先用完整生命周期实验证明收益；不把阶段 CPU 占比当作整链可获得的加速比例。

### P2b 格式实施约定

外层 `gtcblk01` magic 不变，header version=2；48 字节 header 仅写 magic/version/blockSize，其余字段必须为零。后续完整 chunks 按序写入，保留的首逻辑 chunk 在结束时追加到 body 尾部。随后写逻辑顺序的 28 字节/项索引表，最后写 48 字节 trailer：`gtcend02`、recordCount、blockCount、uncompressedSize、tableOffset、tableLength（五个 uint64）。索引中的 compressedStart 相对固定 dataOffset=48；只允许首逻辑 chunk 旋转至物理末尾，其余物理范围必须顺序连续、无空洞、无重叠并完整覆盖 body。

写入途中对实际落到临时文件的字节顺序持续计算 SHA-256，不回填磁盘 header、不二次复制 compressed body。Sync/Close 成功后在同一文件系统 rename 到原 staging 输出路径，随后沿用既有历史发布和 manifest 校验。Reset/Abort/取消/写错均不得发布半成品；异步压缩仍须 join 后关闭临时文件。stream writer 接入新模式，blob 便捷写入入口在 format=2 时复用 stream writer；普通旧格式 writer 本身保持原行为。

`GTRON_HISTORY_COMPRESSION_FORMAT=1`（或未设）写旧格式，`2` 写 footer 格式；格式选择在 stream 构造时固定，Reset 保持同一格式。reader 与 blob decoder 无条件支持两版，space inspection 也按版本读取统计。磁盘版本存于每文件 header/trailer，不增加 KV 前缀或更改历史逻辑版本。启用前部署版本 2 reader、保持 writer=1；该 reader 版本成为新的最低回退基线，部署元数据记录启用时点与实际配置，回退只关闭新写入，不能换回不认识 version=2 的旧二进制。

## P1a 实施：复用已验证代码，保留逐字节损坏检测

canary 停止后 09:49 UTC 的 25.07 秒样本（29.375M 附近）中，GetCodeStrict 为 2.50/48.24 CPU-s，其中 2.26 秒是对对象已缓存代码重复 Keccak；交易执行为 11.13 CPU-s。09:51 UTC 第二样本相应为 1.42/70.30 CPU-s，其中 Keccak 1.21 秒，说明绝对占比随负载变化。当前大型历史 finalize 未在第一样本运行，清单校验只有 0.06 CPU-s，因此本轮优先减少持续串行代码验证成本，P2b 格式重构仍是后续候选。

数据库既有 64 MiB 正缓存拥有私有、已验证 hash 的代码副本。对象命中时，用完整 bytes.Equal 对比该副本；相等即证明现有对象字节与验证过的内容相同。缓存缺失、关闭、超限、类型化 store 或内容不同仍执行原 Keccak 校验及错误路径。没有 verified-once 标记，不信任对象地址/指针；调用者修改返回 slice 后仍能发现。缓存副本不向外暴露，eviction/close 只丢引用，不改变字节。原 StateDB 依赖追踪、hash/dirty、历史、copy 与 journal 不变。

共享缓存重复准入先查已存在的键，只有 miss 才分配副本；复制在 mutex 外进行，重新持锁后再次检查并发准入/关闭。原字节预算、LRU 淘汰和 canonical 字节所有权不变。新增 object_verified_matches/object_hash_checks 指标标记实际路径。存储格式和链规则不变，可直接回退 P2a。


## P1b：状态批量读取计划（2026-09-05）

P1a 后的新采样仍在 `ReadStateKVLatestNoCopy`、权限及冻结资源读取中看到显著开销。现有账户/合约预读队列没有丢块，但逐行 `DB.Get` 重复建立内部读取状态，并未覆盖普通交易所有者的权限、带宽冻结及能量资源行。

本阶段将状态预读改为两阶段有界计划：先读取账户 envelope，验证完整格式且仅保留 generation/code hash；再按准确 generation 计划 owner 指定的 permission ID、V1 带宽冻结第 0 行、AccountResource、合约 metadata/code，以及原有 TRC10 行。每阶段最多 4096 行，TRC10 仍最多 128 行；去重及物理键构造均在 rawdb。

blockbuffer 先解决 overlay、删除及缓存命中，剩余请求按物理键排序，在一个短生命周期 Pebble snapshot 中复用 exact-key cursor。继续使用 full-key Bloom 的 SeekPrefixGE，不引入全表扫描。所有失效 epoch 必须在 snapshot 创建之前采集；回填必须通过 epoch 校验，防止并发 flush 将旧快照数据写入当前缓存。回调值只在回调内有效，取消/错误关闭 cursor 和 snapshot。权限、fork、执行顺序、根及磁盘格式不变；计划只提供读取提示，执行仍经过当前 overlay/generation。

验证要求：真实 Pebble 精确读取（含空值/不存在/重复键）、overlay tombstone、snapshot 前后并发 flush 更新和删除、计划去重/容量/代际/损坏行、取消及 race；完整 core/actuator/VM 回归及两节点 system test。固定 SST 输入分别比较逐行与批量读取；线上另看相同资源配置下的状态读取 CPU、I/O 和 tx/s，不能把相邻不同交易组合的块速比当作因果收益。


## P2c：二层有序 Commitment 分区

2026-09-05 11:45 UTC 的 P1b 样本（25.17 s / 52.21 CPU-s）中，runLane 为 8.52 CPU-s，预取为 5.94 CPU-s；commitment 实际读取仍是主要成本。现场 depth-5 与 depth-6+ 的累计 useful/durable 分别为 1,758,031/1,780,148、1,035,226/1,063,686，未支持“大量预取失效”的假设。本轮将折叠通道从首 nibble 的 16 个，细分为首 nibble × 第二 nibble 的四组，共 64 个；预取保持 16 条、深度和预算不变。

每个分区独占第二层连续四个 child slots、cursor、leaf arena 和跨块队列，深度 ≥2 的 branch 写入互不相交。分区持有其部分深度 1 分支，不读取由异步 job 最终合成的深度 1 行；所以其他分区或后继 block 先完成，也不能向本分区注入未来父状态。每块 join 后通过原 linkChild 合成第一层并执行原有单叶 collapse，再写原格式根/分支，由现有 FIFO publisher 发布。启动时从已有根展开虚拟单叶或读取第一层 hash child，并校验 child hash。空更新依赖前一 job 的私有 completion/root，不再发送 64 个空任务，但仍写本层 root marker。

`GTRON_COMMITMENT_PARTITIONS=64` 选择新模式；默认/`16` 保持原模式，其他值报错。`state/commitment/pipeline/partitions` 暴露实际选择。全局 inflight 限额、16 条预取流、prefetch 路径所有权、per-key singleflight、snapshot cut、immutable/frozen/legacy fallback、持久格式及事务验证保持原契约。普通 parent reader 由 17 个增加到 65 个；有 fallback 时前台各占独立 65 个 reader，再加原有两组 16 个预取 reader。未改变 RPC/P2P 或磁盘布局。

固定 Pebble SST 延迟模型刻意关闭预取来隔离折叠容量，不是 EBS 测量：32,768 个种子、每块 512 更新、每次 durable cursor 额外 250 µs，M1 三轮中位数 26.942 ms → 10.076 ms。无注入延迟时 2.749 ms → 3.120 ms，16 更新 0.217 ms → 0.236 ms，说明 CPU/常驻数据形状存在 9–14% 回退，尚不满足全部形状 <3% 的门槛；不能默认普开或把延迟模型倍数当作主网收益。空块 20.3 µs → 7.0 µs。先在用户已授权的主网 I/O 等待场景启用，并根据持续实测决定保留或退回 16。

第一版 64 分区的主网后段窗口没有稳定收益，已用同二进制回切 16 对照。第二版将逻辑 owner 数和 durable cursor 并发解耦：64 模式默认 `GTRON_COMMITMENT_READ_CONCURRENCY=32`，允许 0..80，0 表示不设置限额；16 模式不应用此开关。所有 job/session 共用一个有界名额通道，仅 physical read leader 在 Cursor.View 外取得并通过 defer 归还；overlay/cache 命中及 singleflight follower 不占名额。名额配置在 session 开始读之前完成，运行期不关闭通道。新 gauge `state/commitment/pipeline/read_concurrency` 显示实际预算。

第二版还池化固定大小的分区 roots/stats/changed 工作区，只在分区、lookahead 全部 join 且 parent session 关闭后清理归还；跨块空更新依赖独立的 finalRoot/completed，不引用归还的工作区。parent session scratch pool 的有界上限由 66 调整至 162，覆盖两组 65 个 foreground/fallback reader 与两组 16 个 prefetch reader。未扩大 inflight，也没有跨 snapshot 缓存解码后的 branch。

第二版通过真实 Pebble 名额耗尽测试（缓存/overlay 继续运行）、错误与 panic 归还测试，以及两轮 race 和完整 core/actuator/VM、79 项系统测试。M1 固定延迟模型 512 更新为 27.381 ms → 13.640 ms；无延迟仍回退，默认保持 16。现场模式的最终取舍及不同窗口交易构成见 `docs/dev/28m-commitment-partitions-p2c-2026-09-05.md`，不能以合成基准替代整链验收。

现场最终完成 64/32→16→64/32 顺序验证。重复确认的后三分钟为 1369 tx/s，对照为 1251 tx/s，commit 约 289 对 432 µs/update；交易、缓存、冷历史与后台压实不同，不能宣称严格 9.5% 因果收益或整链 +50% 达标。用户授权的这台主网 I/O 场景保留 64/32，代码默认仍是 16；各形状回退与长期后台服务率的全局验收未通过，不将本次现场配置扩展为通用默认。


P2b 已完成主网两阶段上线、完整新旧三件套校验、writer=1 回退重启和最终版持续观察。跨盘测试还修正了 blob 入口的临时文件位置，确保 footer rename 总在目标文件系统内。最终 release 为 20260905-p2b2，保留新格式写入，默认代码仍写 v1。64 MiB 同数据盘基准总耗时约 -19%、收尾约 -75%；六分钟窗口未覆盖新的 footer 收尾完成事件，不能据此证明整链收益。完整证据、实际配置与仅关闭新写入的回退入口见 `docs/dev/28m-history-footer-p2b-2026-09-05.md`。

## P2d：按快照逐层点读，减少旧版本 SST 读取（2026-09-06）

30.087M 附近的 25.13 秒 CPU 样本中，pointReadCursor.View 为 10.33/51.11 CPU-s，后台压实为 9.58 CPU-s。当前 SeekPrefixGE 保留 full-key Bloom，但 merging iterator 仍需同时定位所有重叠层。Pebble v1.1.5 的 Snapshot.Get 使用惰性 getIter，遇到第一个快照可见值或删除便可停止向旧层搜索；合并操作仍由 Pebble 自身处理。此前单 SST 游标基准不能反映旧版本重叠成本。

增加进程启动开关 GTRON_PEBBLE_BOUNDED_POINT_READ=iterator|get，空值保持 iterator。仅非空 prefix 的 pointread cursor 使用新路径；无界、排序的状态批量预读保持原游标。共用现有 Pebble snapshot、blockbuffer overlay cut、cache epoch、singleflight 及 32 个 durable 名额，不改变 root、branch 编码或任何持久格式。prefix 必须私有并逐次检查；Snapshot.Get 返回的 closer 保持到回调结束，错误和 panic 也必须释放。未知配置在打开 DB 前报错。

Get 不提供内部块统计，必须新增独立 get/calls、hits、errors、nanos、cursors；nanos 只包含查找，不包含回调。不得把缺失的 block stats 当作零 I/O 或与原 iterator 计数直接对比。全局 VFS ReadAt、CPU、提交墙钟、工作量及 compaction debt 继续作为线上评价依据。

四层完全重叠 SST 的初始固定输入基准显示逐层读取显著降低 ReadAt；单 SST 则存在回退，故本阶段不更改全局默认。新增稀疏更新形状并在实际主网同一二进制上开关对照，依据连续窗口决定是否保留。所有回退仍使用 P2b2 或后续兼容 history format=2 的 reader；不回退到 P2c2。

## P2e：历史 posting 有界缓冲与顺序装配（2026-09-06）

03:53 UTC 的 P2d1 主网采样（25.18 s / 95.94 CPU-s）中，BuildAccessorContext 占 24.43 CPU-s，V7PostingWriter.flushKey 占 21.89 CPU-s；os.File.Truncate 为 9.86 CPU-s。当前每个 key 即使只有一个 posting，也要先写 scratch、Seek、回读、Truncate、再次 Seek。前一轮的 SHA 校验热点属于不同后台阶段，本轮先处理最新定位到的逐 key 系统调用。

每个 writer 为编码后的 posting payload 保留至多 64 KiB，超过后转入一个复用的磁盘临时文件，以 64 KiB 缓冲顺序写入；内存 key 不接触 scratch 文件。磁盘 key 完整输出后才截断并复用。计数与 offset 在内存累计，去掉逐 frame Seek。单个 frame 仍至多 128 postings，frame directory 与既有 V7 格式一致；超大 key 不累积完整 payload。metadata 顺序读取、key blocks 和最终目录缓冲顺序写入，保留逻辑 offset、完整 CRC/SHA、Sync/Close/rename 和 manifest 发布边界。

验证要求：与独立旧编码器逐字节比较，覆盖 1/127/128/129、多 frame、64 KiB 边界、超限 spill 及大小 key 交替；损坏/取消/临时文件或目标写入故障必须阻止发布并清理临时文件。局部固定输入基准比较旧源码和新源码，随后直接主网部署、连续观测后台阶段耗时、吞吐、错误、RSS 与磁盘/压实债务；不要求回到 28M。
