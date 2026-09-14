# 冷历史 metadata 分配优化记录

2026-09-14。本轮已完成本地与原生验证，并将 `64ed654f` 部署到主网服务。部署前 baseline 为 `8a0681d3`。设计见 `docs/superpowers/specs/2026-09-14-cold-metadata-allocation.md`。

业务提交 `64ed654f582a58f8c03578ba38cd78f7ba4d385e`，运维提交 `7af1212cd56e60c28ac153c6998e78ac32ccd3af`；运维脚本 SHA256 为 `825082bf23b30c53b82a21d5df347b3c3166d82543184dfe034a2c739481d45d`。两次提交均已推送 GitHub master；原生构建、发布证明与后续观测见末节。

本轮保留完整验证，减少 manifest 校验和可信引用登记中的临时复制、分配。此前 companion lookup 修复在运行约 44 分钟后的采样中仍有效；剩余 metadata 成本值得继续优化，但这不是新的磁盘回收机制，也不保证同步或冷存积压立即下降。

## 稳定期线上 baseline

证据目录 `build/benchmarks/20260914-cold-density/`，主要输入为 `before-analysis.json`、`before-analysis-summary.json`、`cpu-before-45s.pb.gz`。13×30 秒完整同进程采样的 metrics 中点为 UTC 05:16:48.524669–05:22:48.670800（北京时间 13:16–13:22）。

- 业务 `8a0681d3`，PID `2830`，start ticks `4509485376`，process start `1789360360249684220`。
- UTC 05:30:53 对上述既有进程的部署身份和健康核对已通过；这不构成本轮新候选已经上线的证据。
- 固定历史余额 canary 返回 `0xb6283b0374b0e000`，原值在 `fixed-history-before.json`。新版已返回相同结果，原值保留在 `fixed-history-after.json`；单一查询不能替代全库内容审计。

| 同步指标 | 结果 |
|---|---:|
| session blocks 增量 | 6,688 |
| session transactions 增量 | 1,015,536 |
| 导入速度 | 18.575 blocks/s |
| 交易速度 | 2,820.50 TPS |
| 交易密度 | 151.84 tx/block |
| 平均进程 CPU | 3.364 cores |
| buffered blocks 首末 | 1,608 → 1,522 |
| buffered blocks 采样范围 | 1,359–1,871 |
| buffered bytes 首末 | 87,856,427 → 45,762,816 B |
| 同步状态 | 全部 active，无 pause/fetch backpressure 观察 |

Wallet session 速度用 Wallet 自身时间；metrics head 为 31,128,938 → 31,135,596，增加 6,658。二者并非同一时间点/计数口径，不强行对齐。以下头距均由同一 metrics 样本计算。

| 水位 | 首末 | head 到水位的差距 | 差距变化 |
|---|---:|---:|---:|
| state cold published | 30,649,363 → 30,654,598 | 479,575 → 480,998 | +1,423 |
| state domain-change pruned through | 30,649,363 → 30,654,598 | 479,575 → 480,998 | +1,423 |
| body coverage | 30,605,312 → 30,605,312 | 523,626 → 530,284 | +6,658 |
| transaction-index coverage | 30,605,312 → 30,605,312 | 523,626 → 530,284 | +6,658 |

state 水位六分钟推进 5,235，头距仍在约 48 万块量级，同时确实增加了 1,423，不能描述成“完全不增长”。state eligible-cutoff 到 published 的差距为 414,007 → 415,435，增加 1,428；它不是 head gap。body/index 暂未发布新水位，不足以单凭此短窗认定 worker 卡死或预测全天积压趋势。

GC scanned metadata +182、candidates +143、逻辑退休 +5、coverage defer +123、busy defer +15，GC errors +0。专属 queue attempts/admitted 均 +5，busy/errors 均 +0；admitted 闭包累计工作 447.705742 ms。queue work 的进程峰值由 274.686617 ms 到 314.098162 ms，峰值增量不是本窗口的最大延迟。候选与 defer 是可重复扫描计数，退休不是物理释放。

同批全 pack 逻辑成本：旧编码估算 2,223,659,050 B，实际暂存 384,756,384 B，少 82.70%。这是既有共享编码仍生效的证据，不能归功于采样当时尚未部署的本轮 metadata 优化，也不等于扣除 WAL/SST/压实后的净磁盘回收。

本窗口所检查的 **70 个错误指标均无非零观察**，write stall counter/duration 均为零。压实输入增加 22,916,098,272 B、输出增加 19,368,366,916 B；debt 首末 3,372,741,945 → 3,309,545,408 B，采样峰值 6,428,462,286 B。压实在工作，但输入减输出、debt 都不能直接换算成磁盘释放。obsolete bytes/files 仍出现负值，engine disk gauge 因此不作为物理净增依据。

## 独立物理目录大小

发布任务通过独立 `du` 进程取得 UTC **05:32:23**（北京时间 13:32:23）的数据，服务仍在运行，目录遍历并非原子快照：

| 目录 | 物理字节数 | 十进制 GB |
|---|---:|---:|
| chaindata | 247,233,126,400 | 247.233 |
| state snapshots | 677,133,164,544 | 677.133 |
| ancient | 234,350,960,640 | 234.351 |
| datadir 总计 | 1,158,717,267,968 | 1,158.717 |

该单点用于后续独立物理对照，不能单独给出增长率。早先在一次 `du` 命令中同时传入嵌套子目录和父目录得到的 datadir `16,384 B` 已排除：它是 `du` 不重复计入先前子目录后的残余，不是 datadir 实际大小。不能拿它计算回收量或容量变化。

## CPU 定位与 density 指标口径

稳定期 profile 内置起点为 UTC 05:16:48，持续 45.16 秒，总计 135.58 CPU 秒；传输捕获起点约早一秒，二者不混用。Build ID 为 `a3559dcf05fa6204890daf16ee588407ad72f924`，输入 294,264 B，SHA256 `1d39099f8ef37c5b2b3dabdc33ba59cecc75639d6a3c3f9d2c8597dd0cc675cc`。

| 路径 | cumulative CPU 秒 | 说明 |
|---|---:|---|
| Manifest.Validate | 2.66 | 本轮完整验证内部复制/分配目标 |
| RecordTrustedSnapshotSegments | 0.57 | 本轮 active membership 建表目标 |
| PublishManifest | 2.71 | 包含 Validate、JSON、发布及 seed，不能与 Validate 相加 |
| UpdateHotPruneProgress | 1.73 | 包含发布等子调用 |
| Aggregator.integrateWithManifest | 1.47 | 包含发布等子调用 |
| 完整 coverage gate | 0.40 | 前轮修复后保持低成本 |
| 新 companion view 构造 | 0.11 | 包含于 gate |
| findManifestRef 全部 | 0.02 | gate 内为 0 样本；零样本不等于零成本 |

CPU 秒是采样线程计算量，不是逐批墙钟等待；嵌套 cumulative 项禁止相加。没有与这 45 秒严格对应的 blocks/tx 计数，因此不拿六分钟平均吞吐伪造每块 CPU。

`density_metadata_work` 由 `history_load.go:recordHistoryWork` 调用 `historyDensityWork` 汇总，反映最近成功 history 批次的已计时 metadata 区域：builder 的 manifest load/visible-end 与 integrate/publish；before-merge 的可信登记和 pruner 内 gate、起始 cursor、进度发布。它不是整个 maintenance，也不是单独的“密度统计函数”CPU。

内容认证、扫描、压缩、删除、batch commit 仍属于可计费行工作；完整生命周期的 recovery 预算另算，未用扣除 metadata 后的密度估计替代。滚动 gauge 可以重复同一批次，不能将采样中位数当作独立批次均值。三个旁路 sidecar pruner 的全 manifest Validate 虽在 CPU 栈中可见，但不属于上述 density metadata 计时范围。

## 本轮实际改动与安全边界

`core/state/snapshots/manifest.go` 的 family overlap 检查由 `[]SegmentRef` 改为只保存 `from/to` 的 16 B 元组。原完整 ref 为 104 B；分组仍依据 normalized dataset/domain/kind，仍按相同范围顺序排序、做闭区间重叠判定并报告原错误。没有使用可能在 uint64 上界溢出的 end+1 算法。每个 active ref 仍做完整字段校验，seenPath 仍保存完整引用并检查重复路径，retired、chain、visible bounds 和各类 companion 验证均保留。

history/latest/production validator 改为按切片索引读取，并先筛 kind，再复制合格 ref 或读取 DomainCfg。ConfigForRef 是纯配置读取；跳过无关类型的配置查找，不等于跳过这些条目本应接受的验证。

`core/state/pruning/pruner.go` 与 `verification_cache.go` 的可信登记，原来为少量新 history 把全部 active refs 填入 map。现在小批次只登记候选，再完整扫描 active catalog 确认精确 membership；合格输入数量大于 active 数量时回到原完整 catalog map，避免输入异常大时扩大候选结构。

完整 SegmentRef 的九个字段仍严格比较，包括 checksum 和原始 aggregationSteps；此处的 `0` 与 `1` 不因其它场景的有效步数归一化而被视作同一引用。外层继续按原 refs 顺序执行，保留重复输入的重复 Stat/登记和首次失败前已成功的前缀副作用。非空但全为无关类型的输入仍先读取/验证 production manifest，不隐藏原先的读取错误。

完整 manifest ReadFile、逐字节 cache 核对、公共可变副本、每个匹配 history/index/accessor 的 fresh Stat、size/mtime 身份、原 companion 规则及 verification-cache 持久化均未改。trusted 原本承接本地 builder/compactor 的窄信任，不在此处重复全部内容哈希；重启后重新哈希、外部或变化引用的全验证、破坏性退休前认证都保留。

本轮没有更改 public PublishManifest 的完整验证、排序、JSON 格式、generation、文件/目录 fsync、atomic rename、publication seed；没有私有“进度已可信”跳验证路径，也没有改变 no-op 发布频率。9/10 的 publication-seed cache 和上一轮 gate view 沿用；cache 配额、运行并发、prefetch overlap、历史保留、共享格式、range prune、GC 及永久 reader guards 均不变。

## 相同输入本地对照

两组性能基准按顺序独占本机 CPU 窗口运行，环境为 Apple M1 Max / darwin-arm64 / Go 1.25.5 / GOMAXPROCS=2，`-benchtime=1x -count=5`。输入是精确规模的 synthetic metadata：70,198 active、92,667 retired；history/inverted/accessor 各 2,794，event-log/event-log-index 各 30,908。路径、范围、checksum 和占位文件不是逐条生产内容。

| 操作 | 旧版中位 ms | 候选中位 ms | 中位耗时变化 |
|---|---:|---:|---:|
| 完整 Manifest.Validate | 62.306 | 40.230 | −35.43% |
| 完整 Publish + 首次 Load | 176.794 | 140.683 | −20.43% |
| 单 history 首次可信登记 | 24.0915 | 18.2410 | −24.28% |
| 单 history 已登记后再登记 | 14.9890 | 8.5970 | −42.65% |
| 三 history 首次可信登记 | 44.8181 | 37.8398 | −15.57% |
| 三 history 已登记后再登记 | 14.8376 | 9.1921 | −38.05% |

Validate 原始耗时范围 60.91–67.29 ms，候选 37.98–41.07 ms；Publish+首次 Load 原始 162.38–210.47 ms，候选 135.35–149.24 ms。Validate 每轮分配从约 81,120,544 B 降到 51,224,848 B，少约 **29.90 MB**；分配次数却由约 168,915 增到 168,918，不能把减少字节写成减少次数。

单/三 history 可信登记每轮少分配约 **15,732,816 B**、255 次。测试还包括无关输入、缺失或身份不匹配引用、重复输入；这些完整数值保存在 trusted benchmark summary。空输入仅纳秒级早退，单次相对百分比没有有意义的性能结论。

`B/op` 是操作累计分配量，不是峰值 RSS、常驻内存或实际释放的内存；不能将两个不同调用频率操作的节省直接相加成每块/每秒收益。Publication 分配读数有约 134,218,576 B 跳变，与 JSON buffer pool/GC 生命周期影响一致，但未采 alloc profile 证明归因，故保留完整范围。

完整 Publish+首 Load 基准包含排序、全 Validate、JSON、文件 fsync、atomic rename、目录 fsync、原 publication seed、完整 ReadFile/bytes 对照与独立副本。每轮 generation 递增，全部样本为 1 seed/op、0 decode/op；旧 oracle 同时冻结旧 Validate 与 seed 内的 production validator，未借用新版验证制造不公平对照。

可信登记基准包含完整 manifest load/bytes 对照/副本、membership、fresh Stat、cache 登记；首次登记计入 durable 写入/fsync，再次登记保留重复行为。fixture/reset/prefill 在计时外且两侧相同。占位内容适用于原本不哈希内容的 trusted 登记路径，不用来证明段文件内容正确性。

来源分别为 `cold-density/manifest-validation-local-bench-summary.{md,json}` 和 `build/benchmarks/20260914-trusted-snapshot-membership/full-operation-bench-summary.{md,json}`，各自保留 raw SHA 与完整输出。组件提速、记账节省均不代表等比例同步提速，更不是新增 disk 回收。

## 测试和独立审查

- Manifest oracle 与 `44a97531` 原四个函数机械对照一致；保留独立 O(N²) overlap oracle、500 次固定种子输入、uint64 上界、单错误文本、直接未先 Validate 的 kind/dataset/extension 矩阵、输入不变与 publication bytes 对照。
- trusted 完整旧函数 oracle 对照错误、stats、observer 顺序、已验证/持久集合和磁盘字节；覆盖大批回退、全部身份字段、原始 steps 0/1 差异、缺 history/index/accessor、size/mtime、坏 manifest、持久化失败与成功前缀。随机 CreateTemp 名只在错误比较中规范化，不掩盖其它差异。
- snapshots 全包通过，73.623 秒；ManifestValidation/Lookup/Cache scoped race 通过，10.325 秒。
- pruning 全包通过，28.499 秒；trusted scoped race 通过，7.512 秒；额外 effective-steps 用例通过，1.338 秒。
- `core/state` 包通过，2.149 秒；`core` 全包通过，148.926 秒。冻结后的联合 68 项 Python 运维测试通过，2.823 秒；相关定向测试和 `git diff --check` 通过。race 链接出现既有 LC_DYSYMTAB warning，但成功链接并通过测试。
- 独立代理复核 `manifest.go`、`pruner.go`、`verification_cache.go` 差异，未发现生产语义问题；审查输入 SHA 在 `metadata-validation-independent-review-inputs.json`，详细结论在同目录 `metadata-validation-independent-review.md`。该审查不冒充独立重跑 benchmark。

## Commitment 缓存保持现状

稳定期 CPU 中 commitment pipeline 合计 37.05 秒，parent readDurable 22.01 秒，其中 prefetch 8.76 秒；runHandCold flat 为 2.95 秒，低于早前 15.62 秒峰值。嵌套值不相加，窗口/负载变化不能归因于采样当时尚未部署的本轮候选。

`commitment-cache-candidate.md` 选择的是后续离线测量方向：固定 SST 和相同 commitment 更新序列，仅对有序 prefetch 批次比较 exact-key iterator 与 Snapshot.Get，前台保持 Get；必须核对每块 root、全部 branch rows、snapshot/cut 隔离、reader 生命周期以及多层重叠数据的反例。该候选尚未实施或取得收益证据。

本轮不调整缓存容量、准入深度、prefetch overlap、并发或 Pebble CLOCK，也不把普通 iterator 没有的 no-fill 接口用其它可见性语义替代。此前缓存准入、深度和全局 iterator 实验存在回退或正确性反例，不能从一次 profile 直接推广为线上改动。

## 原生验证与发布

业务源 `64ed654f582a58f8c03578ba38cd78f7ba4d385e` 与运维版本
`7af1212cd56e60c28ac153c6998e78ac32ccd3af` 已推送 GitHub master。
原生机器从固定提交归档到独立 release，没有切换或覆盖服务器的旧 checkout。
Go 1.25.5 linux/amd64、Sapling 构建及兼容性测试通过；blockbuffer、pruning、
snapshots、core 专项分别用时 0.382/5.623/10.374/12.572 秒。

固定真实 JSON 的原生回放及完整登记流程均执行五次，按组中位数比较：

| 完整操作 | 原实现 ms | 候选 ms | 耗时降低 |
|---|---:|---:|---:|
| 真实清单 Validate | 138.812 | 93.030 | 32.98% |
| 真实清单 Publish＋首次 Load | 463.388 | 405.899 | 12.41% |
| 一个 history 首次登记 | 51.561 | 31.689 | 38.54% |
| 一个 history 已登记后再次调用 | 51.057 | 37.506 | 26.54% |
| 三个 histories 首次登记 | 54.941 | 36.272 | 33.98% |
| 三个 histories 已登记后再次调用 | 61.420 | 32.866 | 46.49% |

完整 Validate 的 B/op 从 78,333,968 降到 48,438,272，差值 29,895,696 B，
与本地一致。allocation 次数仍多三个小对象，不能称为次数也下降。
Publish 的分配受缓冲区复用影响而波动，不以其中位 B/op 差宣称稳定收益。
登记基准使用与实际数量匹配的合成清单和临时段文件；首次验证缓存为空，
不能代表完整生产缓存的精确登记延迟。上述结果均不是同步吞吐或物理回收量。

原生原始输出为 `/tmp/gtron-cold-metadata-bench-20260914-{manifest,trusted}.out`，
共 20/140 条结果；全部逐轮值保留在同目录的 `-summary.json`。
本地转录见 `native-benchmark-observed.json`，包括工具链、样本和解释限制。

| 发布证明 | 值 |
|---|---|
| Release | `/data/gtron/releases/20260914-cold-metadata` |
| 脚本 SHA256 | `825082bf23b30c53b82a21d5df347b3c3166d82543184dfe034a2c739481d45d` |
| Binary SHA256 | `1d58ee1a0a05d84599b66f4697bc6a3aa87f7f19660b1a5f0c63d1a1bfb7c6ff` |
| upgrade-prepared SHA256 | `7ed649e1f863ae363de7edaab164bd3062c4d45458c420fda783bf3ab4a615e8` |
| 事务开始 → 健康激活 UTC | 05:40:17.246 → 05:41:45.718 |
| 新进程 PID / start ticks | `18282` / `4509893088` |
| process/start/unix_nano | `1789364437368890714` |

事务返回 `active=true, phase=active`，退出码 0，没有触发回滚。
仅允许恢复到兼容的 `8a0681d3`（writer on/off），并保留其永久 reader marker
及 D656、8058、af989 祖先。候选及恢复健康检查均要求同一个精确进程的六项 GC
排队指标；本地 68 项恢复测试通过，不等于在线主动制造过故障或执行过回滚。

## 上线后完整观测

完整 after 为 UTC 05:42:15.783–05:48:15.745，13×30 秒、26 个 HTTP
请求全部成功；精确 process 始终为 `1789364437368890714`。
因整段同步明显较慢，追加同进程 UTC 05:49:36.638–05:52:36.612 的 7×30 秒
窗口，捕获/累计单调性/身份复核同样通过。`settled-analysis` 的内部 `before`
是复用分析器的角色标签，实际数据全部属于新版第二个观测窗。

| 指标 | 旧版 6 分钟 | 新版首个 6 分钟 | 新版追加 3 分钟 |
|---|---:|---:|---:|
| 导入 blocks/s | 18.575 | 7.288 | 9.244 |
| TPS | 2,820.50 | 1,348.03 | 1,731.25 |
| tx/block | 151.84 | 184.95 | 187.29 |
| 平均进程 CPU cores | 3.364 | 2.706 | 3.186 |
| state 冷水位推进块数 | 5,235 | 2,620 | 2,119 |
| state 头距变化 | +1,423 | −2 | −470 |
| state 窗口末头距 | 480,998 | 491,556 | 490,552 |
| body/index 窗口末头距 | 530,284 | 552,527 | 554,774 |
| 本进程 GC 退休增量 | 5 | 15 | 0 |
| 70 项错误指标非零项数 | 0 | 0 | 0 |

截至追加窗末，Wallet 高度 31,160,089，state cold/prune 水位 30,669,534；
body 与 txindex 水位均为 30,605,312。Wallet 与 metrics 的独立采样使 head 存在
几块差异，表内所有 head gap 均使用同一 metrics 样本。state 仍在约49万块量级，
追加窗减少470块；body/index 约55.5万块且继续增加，不能说所有积压都不增长。
三个窗口均同步 active，无 pause、fetch backpressure 或 write stall 观察。

首个新版窗 GC 逻辑退休累计 4→19、queue admitted +15、闭包工作37.872ms，
追加窗保持19，76次候选因coverage保守延后。不是物理释放，也不代表 GC 停摆。
新版首窗同批全 pack 的旧编码估算7,084,072,567B，实际暂存572,246,068B，
约91.92%逻辑成本节省；追加窗92.88%。这是已有共享格式继续生效的证据。

UTC05:44:55 再次执行候选配置、永久 reader markers、运行身份及健康检查通过，
PID18282/start ticks4509893088未变。历史高度 `0x1d19c00` 的余额查询，部署前后
JSON相同，均为 `0xb6283b0374b0e000`。单一canary不替代完整内容审计。

### 整体同步较慢的调查

本轮只能确认局部优化有效，**不能宣称端到端同步提速**。首个新版窗首点的
import/window尚未初始化，stateCommit/exec_busy等为0；它已从有效分布对比中
排除，保留原始值。有效12点stateCommit中位80.717ms/block，旧版26.360；
metadata滚动观测中位则为1.3666→1.1675秒（降低约14.57%）。这些是滚动样本，
不能将中位数相加或乘操作次数推算完整窗口耗时。

按同口径计数折算，历史行/tx为3.867→3.772，commitment updates/tx为
3.046→2.959，行数密度未暴涨；但全pack原始RLP输入/历史行为649→4,774B
（约7.35倍），输入/tx为2,511→18,005B。1/128 previous-image样本的
字节/行为730→4,408B，独立支持旧值变大的方向，但不能据此认定全部具体domain。

共享编码被选中次数从481增至1,653，占pack次数约7.20%→63.14%。已选共享
阶段每次工作墙钟约44.82→44.74ms，基本相同；该阶段累计为21.56→73.96秒。
它不含baseline编码、最终pack Put及fallback，不能当作CPU或总StateCommit。
这支持“较多大旧值触发既有共享路径”的解释，不能把所有减速都确定归因于它。

原生相邻五分钟日志也显示旧版13:30–35为14.57 blocks/s，13:35–40为13.26；
新版13:40–45的覆盖仅73.19%，均速6.43，13:45–50覆盖100%，均速6.90。
下降在部署前已出现，但这些窗口高度不同且包含原生准备工作，仍不能隔离
代码、工作负载和缓存预热各自的影响。追加窗9.24是部分恢复，尚低于旧版基线。

### CPU 与剩余优化方向

两个45秒profile各为135.58/121.48 CPU秒，after预热约322秒，before约2648秒。
新版SHA256 blockAVX2 flat为29.19秒（24.03%），旧版11.30秒。互斥调用来源
集中在历史shared chunk/pack解码校验、shared规划写入及CDC分块。
StateCommit范围内的FlushFinal为5.69→16.46 CPU秒，委托列表成员集合构建
`legacyDelegationMembership`为0.57→8.28秒。StateCommit计时包含FlushFinal和
deferred工作，不应仅用CommitStateCapture自身的1.50→0.61秒解释整个阶段。

commitment pipeline为37.05→18.12秒，缓存CLOCK淘汰runHandCold flat2.95→0样本，
Pebble压实CPU15.21→3.75秒。本次profile不支持“缓存淘汰/压实CPU变多导致减速”
的判断，但CPU采样不能排除I/O或锁的off-CPU等待，也不能把零样本当成零开销。

完整Manifest.Validate为2.66→2.86秒，Publish为2.71→4.06秒，RecordTrusted总量
0.57→0.57秒；工作次数与背景阶段不同，局部微基准收益没有表现为这些累计CPU
都下降。RecordTrusted主体（排除load及addTrusted）为0.17→0.01秒；ConfigForRef
0.62→0.13秒。上述cumulative路径嵌套，不可相加。

下一步应优先在固定输入、固定snapshot和相同更新序列上验证大旧值共享编码的
重复哈希/编解码，以及委托列表成员集合重复构建的可复用范围。保留内容认证、
切面隔离与共识语义；本轮没有将未验证的缓存或校验捷径部署。基准收益、滚动
metadata下降和不同工作负载的CPU证据均已分开记录。

### 物理大小变化

单位为十进制GB，每个路径使用独立du进程；运行中遍历并非原子快照。

| 北京时间 | chaindata GB | state snapshots GB | ancient GB | datadir总计 GB |
|---|---:|---:|---:|---:|
| 13:32:23 部署前 | 247.233 | 677.133 | 234.351 | 1,158.717 |
| 13:44:55 部署后 | 245.896 | 677.700 | 234.351 | 1,158.048 |
| 13:47:51 后续检查 | 245.970 | 677.963 | 234.351 | 1,158.289 |

首末chaindata净减少1.263GB，冷快照增加0.830GB，datadir净减少0.428GB。
中间到末点chaindata又增加约74MB，说明短时回落不能等同停止增长。
/data剩余约1.82TB。物理数值来自原生/tmp记录，本地转录为
`native-{baseline,postcheck,final}-observed.json`；未用异常obsolete gauge推算磁盘。
上述跨越部署的两次观测包含正常写入、冷迁移、GC、压实及目录遍历时差，不能
把全部净减少归因于本轮metadata优化，也不能据此外推全天增速。

真实 JSON 可通过 `GTRON_MANIFEST_VALIDATION_REPLAY` 与 `GTRON_MANIFEST_VALIDATION_REPLAY_SHA256` 显式启用；读取/校验输入在计时外，发布只写临时目录，不修改生产 manifest，不依赖源段仍存在。当前控制样本沿用 `/tmp/gtron-metadata-baseline-20260914/manifest.json`，SHA256 `d752baf77b3ef3503b38d6e89400b82c5cc2a4ea091506a100e2a81f3f46821e`，与本轮 live 时刻不是同一份 manifest；如换样本需记录新 hash/分布。

验收必须在各自精确进程内求 counter 增量，不跨重启相减；比较执行成本时记录交易密度、工作量和后台阶段。只有后续独立物理目录与持续水位证据，才能判断数据库增长和积压趋势；本轮分配优化本身不提供“chaindata 已停止膨胀”的结论。
