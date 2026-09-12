# 2026-09-12 数据库空间观测上线与验收

截至北京时间 **09:35–09:39**，新版本持续同步，10分钟处理 **26.29块/秒、2846笔交易/秒**；状态/区块体/索引距头仍为约33–39万块数量级。实际chaindata为 **151.39 GiB**，较08:30的156.51 GiB减少5.12 GiB；冷历史继续增长。新指标定位到commitment约47.94 GiB、变更索引37.14 GiB、热历史包36.45 GiB。

最明确的优化对象是**追块期间延期的派生变更索引清理**：热changeset已有清理水位，manifest却未记录完成的索引清扫水位。现有范围数据支持定位空间大头，但没有昨天的同范围基线，不能把昨日新增66.52 GiB精确分摊给这些类别。另发现Pebble obsolete记账下溢，旧/新引擎disk总量均受影响；实际目录数据独立保留。

本次在 master 实现并通过 GitHub 部署数据库空间观测。源码提交 `5664a53e87b20e05ff1204f40b026895d6f1f9db`；部署助手提交 `52a336fe2660da3c460be317b569c13b8aca2244`。北京时间 09:25:19 完成服务启动验证，生产 PID 7759，二进制 SHA-256 为 `4e2eb51c88ae262f2e3e83a4eb6f5598146864fd11fa1f9e57c9ec9e6c79998e`。本次增加观测，不宣称获得加速或空间回收收益。

此前实际目录增长、冷历史保留和磁盘余量见 [08:30 的盘点](chaindata-growth-status-20260912.md)。下文为随后部署的新指标证据，时间与口径不同，不覆盖此前的实际目录测量。

## 实现及部署验证

引擎空间指标复用原有每 3 秒一次的 Pebble Metrics 读取。另启用一个独立的串行 worker，按 schema 中的 12 个固定键范围调用 EstimateDiskUsage；每次完成后等 30 秒再处理下一个范围。没有全库 value 扫描；但单次估算没有硬时间或 I/O 上限。失败保留上次成功值和时间，并单独累计失败次数。

同一 metrics namespace 的新空间指标只由首个 DB owner 发布，避免辅助 DB 覆盖主库指标；关闭时先停止 worker，再关闭数据库。生产同时保留原缓存观测环境，并启用 `GTRON_DB_SPACE_OBSERVER=1`。

本地受影响包、race、增量 lint 均通过。全仓首轮有既有异步时序测试 `TestProcessBlockPublishesAsyncSenderRetryCohort` 失败，完整 core 包复跑通过；旧版本可复现同类失败，双线程条件下新旧版本各连续 10 次通过，未把首次全仓结果称为全部通过。

服务器 Go 1.25.5、CGO 和 Sapling 原生构建通过；rawdb、pebbledb、同步/下载、domain、blockbuffer 测试、双观测器开启测试及所选 core/state/zksnark 回归通过。Sapling Available/Uncommitted 探针通过。所选 cmd/gtron 模式无匹配测试，不计为已执行的功能回归。

服务器按固定提交构建独立 release，没有重置原工作区。启动检查验证二进制、argv、环境、区块推进、首项估算和引擎指标；配置、维护 hold、空间保护脚本及其他服务配置检查通过。主网端口实测为 TCP/UDP 18890、TCP 8090/8545/50051，6062/6071 仅监听回环地址。回滚目标仍为上一观测版本。

## 新发现：Pebble obsolete 记账异常

采样最初三点的 `sst/obsolete/files` 为 -54/-54/-24，bytes 为 -606072945/-606072945/-572072902。负数没有物理意义，不能把它们解释为真实占用或可回收量。旧 `disk/size` 和新 `storage/engine/disk/bytes` 三点完全相等，说明二者读取同一原生公式；异常并非新 worker 写入了业务数据。

固定依赖 Pebble v1.1.5 中，`deleteObsoleteFiles` 将待删除列表移交后台后清空列表（compaction.go:3806）；`updateObsoleteTableMetricsLocked` 按新列表覆盖统计（version_set.go:872），已移交的后台任务仍在 `onObsoleteTableDelete` 中逐项减计数/大小（compaction.go:1774）。两阶段交叠可使计数为负、uint64 大小下溢，转换为有符号 gauge 后显示负数。源码说明了可产生异常的路径，但本次未跟踪内部事件，不能确定具体由启动清理还是普通 compaction 交叠触发。

第三点原生 disk 为 161161071588 B，其余公式项之和为 161733144490 B，负项使它相对其他记账项少 572072902 B（0.533 GiB）。这不是对真实磁盘误差的修复估计；本轮将含异常项的引擎总量标为不可靠，保留原值，不归零冒充真实磁盘数据。非负时也不能据此保证没有少计。此前 `du` 的目录增长结论不受这个计数问题影响。

## 同步与完整轮转验收

09:25:41–09:35:42，共21组Metrics/Wallet，42次请求全部成功。精确进程标识为`1789176302979325075`，keyspace owner均为1；没有跨重启相减计数。12项全部known=1且errors=0，末点各项累计尝试1–2次，已观察到的单次估算最大6.352 ms。这只刻画已完成估算的调用耗时，不等于整套观测对CPU/IO的因果开销证明。

Wallet独立600.060秒内：currentBlock增加15765，头增长26.272块/秒；sessionBlocks增加15776，处理26.291块/秒；sessionTransactions增加1707813，2846.073笔/秒、108.254笔/session块。不同高度和交易负载，不能把它与早间23.73块/秒直接当作新版本加速证明。19个去重且完全落入窗口的import样本覆盖15361个apply block，VM占45.91%，commitment更新446.79次/apply块；execute/state commit/persist分别约30.10/12.25/4.96 ms/apply块，阶段重叠不相加。

| 距头/缓冲 | 首点 | 末点 | 窗口变化 |
| --- | ---: | ---: | ---: |
| 状态发布/热历史清理距头 | 327136 | 329446 | +2310 |
| 区块体覆盖距头 | 384287 | 334493 | -49794 |
| 交易索引清理距头 | 384287 | 391837 | +7550 |
| 接收缓冲块 | 1643 | 1936 | +293，范围488–1936 |
| 接收缓冲原始字节 | 60562193 | 54590460 | -5971733 |

这些距离重叠，不相加。全窗状态距头略增，Metrics后五分钟329786→329446（减少340），整体维持同数量级；交易索引距头仍较首点高，后五分钟只增5。21点均active、无pause/fetch backpressure。不能把结果简写为所有积压下降，也不能据10分钟外推追平时间。

所选canonical/storage/non-shadow错误均有字段覆盖且无新增；shadow sender-chain新增2、VM sender-chain新增679（readiness669、unsupported10，父子不相加）。三块27844000、27850000、27859500的区块ID、父哈希及有序交易ID分别与主网参考一致，共6次请求，未扩展为全链/状态根验证。

## 现有键范围空间分布

以下为末点保存的最近一次成功估算，单位GiB。每一行给出实际成功时间（北京时间）；各项异步轮转，不相加计算精确全库占比。

| 范围 | SST估算GiB | 成功时间 | 最近耗时ms | 累计尝试/错误 |
| --- | ---: | --- | ---: | ---: |
| commitment各族（`commitment`） | 47.9408 | 09:33:04 | 1.586 | 2/0 |
| 变更posting/目录索引范围（`state_change_index`） | 37.1428 | 09:34:04 | 3.207 | 2/0 |
| 热历史变更包（`state_changeset`） | 36.4495 | 09:33:34 | 0.750 | 2/0 |
| KV当前状态（多个domain）（`account_kv_latest`） | 11.7270 | 09:31:34 | 0.721 | 2/0 |
| 热区块体（`block_body`） | 8.6173 | 09:35:34 | 0.398 | 2/0 |
| 热交易收据（`transaction_receipts`） | 2.3119 | 09:30:34 | 1.420 | 1/0 |
| 热交易索引（`transaction_index`） | 2.1260 | 09:30:04 | 2.300 | 1/0 |
| 账户当前状态（`account_latest`） | 1.2659 | 09:31:04 | 0.144 | 2/0 |
| 区块交易范围（`state_tx_range`） | 1.2030 | 09:34:34 | 0.930 | 2/0 |
| 合约代码（`state_code`） | 0.1465 | 09:32:34 | 0.694 | 2/0 |
| 暂存区块体（`staged_body`） | 0.1043 | 09:35:04 | 0.163 | 2/0 |
| KV generation（`kv_generation`） | 0.0003 | 09:32:04 | 0.131 | 2/0 |

当期历史包自身计数为570.45变更行/块、74155.75编码字节/块（包含key为74190.75），分母是该writer自身处理的15760块，不是估算保留量。缓存flush抽样中commitment占编码写入字节78.4%、changeset占16.3%；此抽样不覆盖所有直接DB写入，不能据state_change_index在其中为0就说索引没有写入，也不能把写入比例当磁盘比例。

## 派生索引为何持续占空间

代码已证实两条清理路径不同：已被历史索引stage覆盖的块删除热changeset时刻意保留immutable packed posting（`core/rawdb/accessors_state_changeset.go:1192`）；独立posting/目录sweep要求cold已达到eligible cutoff且网络catchup闲置（`core/state/pruning/lifecycle.go:317`）。长时间追块期间，热历史包的块窗口能稳定，派生索引却不受同一个在线清理窗口约束。

09:36:40仅解析线上manifest前8192字节的头部：generation25647，history/accessor/hot-prune TxNum均1547121913，hotPruneBlockNum为27531798；`stateChangeIndexPruneBlockNum`未写出，按uint64/omitempty模型为0。即**尚未记录一次已完成的索引sweep水位**；不能据此排除曾有局部删除。文件约32.93MiB，未做全manifest/segment验证。

当前索引是key哈希→block列表的帧及完整key目录，不重复存每笔交易的旧value。posting按最多256块打包，不同ETL批次仍可产生许多短帧；目录、旧posting、SST旧版本的比例尚未分别量化。热查询会回读权威changeset过滤旧候选，冷查询使用自身段索引，因此已冷覆盖且热删除的完整旧帧有安全清理的设计基础；37.14 GiB并不等于可立即回收37.14 GiB。

下一步优先实现低占空比的**posting分片清理**：固定已持久热删除且冷覆盖验证过的水位，只删除整帧lastBlock不高于水位的记录；限制扫描行数、字节和删除批次，每片释放iterator，提交删除后再推进cursor，整轮成功才推进sweep水位。第一版仅清理posting帧，保留目录，并验证与builder/restore的协调。不能只删除syncActive门槛就运行现有全表sweep：它的目录阶段使用此前posting快照建立的live集合，放开并发新key发布后需要重新证明安全性。

其次量化commitment的generation/旧分支与有效分支，再决定轮换预算；当前47.94 GiB不能全叫垃圾。Pebble统计记账修复应有阻塞cleaner交叠的回归验证，先修正可观测性，避免基于错误总量调参。强制压缩的回收收益当前没有量化依据。

## 引擎保留与实际目录复查

当前层SST指标约155.285→149.743 GiB；这是层级文件记账，范围内可能含旧物理版本。obsolete files/bytes在13/21点为负，包含它的disk总量不用于精确空间或回收结论。zombie SST首末均0、最大约0.714 GiB；snapshot数量0–5、首末均1、最早内部sequence向前推进，未见整个窗口持续固定的旧读视图证据，仍不能排除更短或未采中的异常。pinned bytes累计增加62024389 B（59.15 MiB），不计为当前占用。

WAL末点live physical249315769 B、obsolete/recycler496400880 B。compaction debt约6.186→9.319 GiB、峰值9.748 GiB，写stall和物理SST读取错误均无新增。压缩压力仍在，但debt不是可回收空间；设备并发预算应结合延迟和同步表现继续评估。

采样结束后的只读`du -x -B1 --max-depth=2`盘点位于09:36:40–09:39:01之间，结果如下；没有把它和09:35:42的HTTP强行逐字节对账。

| 实际allocated目录 | 08:30 GiB | 部署后GiB | 增量GiB |
| --- | ---: | ---: | ---: |
| chaindata | 156.51 | 151.39 | -5.12 |
| state-snapshots | 424.83 | 428.91 | +4.08 |
| ancient | 173.97 | 175.59 | +1.62 |
| 整个gtron目录 | 755.31 | 755.89 | +0.59 |

数据卷09:39:01可用2185893814272 B，约1.988 TiB（28.73%）；根卷约24.13 GiB。冷history/log仍累计增加，chaindata短时回落说明增长并非单向；本次观测代码没有增加业务清理动作，不能把-5.12 GiB记作新版本的回收收益。目录结果为终端人工转录，无可见stderr且返回shell，但未记录该du的退出码。

## 口径与证据

12 项范围不覆盖全部键空间，边界数据块可能共享，而且采样时刻不同，不能求和当作精确全库大小、画总和 100% 的分布或相减计算“其他空间”。每项 SST 估算也可能含旧版本，不等于最新逻辑数据。`account_kv_latest` 包含多个 rooted domain；`commitment` 包含相应分支、generation/delta/domain 和标记，不等同唯一最新 commitment 数据。

snapshot pinned keys/bytes 是数据库打开以来 flush/compaction 因快照保留而产生的累计输出，可重复计数，不是当前占用或可回收空间。earliest sequence 是 Pebble 内部序号，不是区块高度或快照年龄。compaction debt 是预计压缩工作量；tombstone 是估计条数，两者都不是垃圾字节。

HTTP 原始响应、部署前单点、采样分析及程序、部署人工转录保存在 `build/benchmarks/20260912-db-space-observation/`。终端证明为人工转录，未冒充原始 stdout；分析程序的合成夹具独立标注为 synthetic，不参与线上结论。

本次操作记录另包含一次UI粘贴异常：工具将剪贴板原有的一条同步性能日志提交至Google搜索。已停止该粘贴方式，改用命令面板逐条核对；可见日志中未发现密码或密钥，未声称撤回该次提交。详情保留在`ui-input-incident.json`。
