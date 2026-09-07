# P2g：完整内容身份的清单解码缓存

日期：2026-09-06。状态：本地实现、正确性/并发验证、合成输入组件对照和部署材料已完成；Chrome 专用跳板机终端未附着，Linux 测试、生产清单基准和主网部署尚未执行。正式主网仍运行 P2f。沿用用户授权的直接主网部署模式，不要求同一 28M 重放，不启动 canary。

## 证据与本轮选择

P2e 的 07:27:16 UTC 样本中，25 秒采样有 `LoadProductionManifest` 4.54 CPU-s，`decodeManifest` 4.34 CPU-s；这些是包含关系，不能相加。生命周期多个步骤会重新读取、解码、归一化、校验和排序同一大型清单。

本轮 P2f 的 08:49:19 UTC 样本长 25.16 秒、总计 68.02 CPU-s，Build ID `04486d3e8041675e52c7ce5cd8dc87a0888d8f49`。Pebble `runCompaction` 为 17.27 CPU-s（25.39%），未在 top 140 中观察到 `LoadProductionManifest` 或 SnapshotLifecycle。后续 goroutine 快照中 SnapshotLifecycle 在 select 等待；该瞬时状态不代表整个时段。清单解码是已观测的周期性成本，不能称为本轮短样本中持续运行的主瓶颈。

回收方面的静态核查修正了优先级：Runner 默认开启 `DeleteObsolete`，合并后会删除不受 published manifest 保护的旧文件；删除后 retired 引用仍留在清单。因此此前约 799.89 GiB 的 retired 记账可能包含大量已删除文件，不能推出同等物理回收积压。`DeferRetiredPruneWhileSyncing=true` 说明额外的 retired 清理被推迟，不能单独证明这些文件仍在盘上。已准备只读 Lstat 盘点，区分 active/published/unleased、存在/缺失、硬链接和实际分配块；未执行文件删除，也未修改回收校验门槛。

## 实现边界

`LoadManifest` 和 `LoadProductionManifest` 每次仍调用 `os.ReadFile`。仅当完整输入与缓存中的完整原始字节逐字节相等时，复用已完成的解码、归一化、排序和结构验证；不使用 mtime、inode、generation、路径或摘要代替内容身份。读取失败、非法更新、生产格式不合法照常返回错误，不以旧清单兜底。

只驻留一份私有 manifest 和完整输入；返回值复制 Manifest、Chain、Progress、Segments、Retired 容器，不把可变对象借给调用者。Go 字符串按不可变值共享，保留 nil/空切片形状。一般验证和 production 验证结果分别处理，不能用一般合法的 legacy 清单绕过生产格式限制。并发 miss 串行准入，同一输入只做一次解码；不同目录也必须内容完全相同才可复用。

驻留计费上限 64 MiB，包含完整原始输入容量、对象、切片容量、所有引用字符串以及保守倍率和管理余量；超限不复制私有视图，走原解码路径。该限额不包含调用者视图、读文件和解码的临时内存，不是进程内存上限。`GTRON_SNAPSHOT_MANIFEST_CACHE=0` 关闭并清空缓存，空值或 1 开启；其他值返回配置错误。新增 hits/misses/bypasses/resident_bytes 指标用于部署后确认收益。

段文件的 SHA、sidecar 语义校验、发布、文件租约和删除门槛保持原逻辑。缓存仅复用清单元数据验证，不复用或替代文件本体验证。

## 已完成验证

- 相同大小、mtime、inode、generation 下的内容替换仍被读取；坏 JSON 和文件消失不返回缓存旧值。
- 调用者修改全部可变容器、随后重新发布，不污染私有视图；空数组与 null 形状一致。
- 一般合法但 production 不合法的 legacy 历史布局持续被拒绝。
- 预算超限不准入、缩小预算释放旧条目、关闭释放缓存、非法开关报错。
- 四个读者与一个 writer 并发发布/加载 100 轮，检查代数与进度一致、修改输出无交叉污染；race 通过。
- 缓存命中后篡改真实历史段，完整文件验证仍失败。
- snapshots/pruning 全包回归 59.995/12.843 秒；manifest cache race 4.260 秒；state/CLI 回归 3.330/42.649 秒；增量 lint 0 issues。
- 从保存的 P2f manifest.go 基线应用增量补丁后，三个候选源码文件逐一 SHA-256 一致。Bash 和内嵌 Python 语法检查通过。以上不是 Linux/Sapling 或主网验证。

## 本地固定输入基准

Apple M1 Max、darwin/arm64，`GOMAXPROCS=2`、`GOMEMLIMIT=2GiB`。每种模式三次、每次至少 500 ms，取中位数。合成输入每组有 5 个 active 引用及 5 个 retired 引用；它们是验证元数据形状，不是物理生产文件。计时包含 `ReadFile`、内容比较、解码或副本返回；fixture 构造、发布和初次预热不在 warm 计时内。cold 每次清空缓存，包含清空与重新准入成本；parallel 数值是并发总墙钟除以操作数，不是单请求延迟。

| 总引用数 | 重复加载：关闭 → 开启 | 并发每操作：关闭 → 开启 | 冷加载：关闭 → 开启 | 开启后驻留计费 |
|---:|---:|---:|---:|---:|
| 5,120 | 6.473 → 0.393 ms | 3.754 → 0.303 ms | 6.439 → 6.724 ms | 5.18 MiB |
| 20,480 | 24.549 → 1.034 ms | 14.135 → 0.972 ms | 24.612 → 25.525 ms | 20.48 MiB |
| 60,000 | 75.320 → 3.043 ms | 42.757 → 2.824 ms | 75.578 → 74.947 ms | 61.41 MiB |

60,000 引用的 warm 分配由 75,428,449 B/op、133,125 allocs/op 降到 23,560,984 B/op、11 allocs/op；时间约改善 24.75 倍。原始 P2f loader 的 overlay 对照中位数为 76.735 ms，与新版关闭缓存的 75.320 ms 接近。两者约 1.8% 的差异不视为关闭路径的收益。

冷加载需要额外私有视图：60,000 引用时约 81.67 MB/op，比关闭的 75.43 MB/op 增加约 8.3%；较小两组冷加载耗时中位数增加约 4.4%/3.7%。缓存是以少量驻留和首次准入成本换取后续重复加载收益。上述比值不代表主网 tx/s；实际清单大小、发布频率和缓存竞争仍待测量。

## 待执行的生产工作

1. 恢复本任务的 Chrome 跳板机终端连接，读取当前 PID/exe、服务配置、磁盘和监听；先做只读 retired 实物盘点。已有终端被另一任务占用，未向其发送命令。
2. 在 `/data/gtron/manifest-cache-20260906` 复制 P2f 源码并核对基线 SHA，应用增量补丁，运行 Linux/Sapling snapshots/pruning 和 cache race，构建新二进制。
3. 读取一次生产 manifest 固定为独立副本，跑冷热/并发及原始 P2f overlay 对照；不打开链 DB，不改生产清单或段文件。
4. 对照部署前后连续指标，通过新计数器确认有效命中与内存；在生命周期实际工作窗口补采 CPU。符合组件和正确性检查后直接切正式主网，并分别报告组件收益、整链吞吐及后台债务。

候选发布脚本使用 `/data/gtron/start.lock` 和排在 P2f 之后的 `99-z-manifest-p2g-20260906.conf`，保留 Get、64 分区/32 读取名额、format=2、双源校验以及原服务端口；切换要求旧进程正常退出，验证实际二进制/环境与持续导入，失败恢复之前配置。候选回退参数为 `p2f`，不能仅运行旧 P2f 脚本，因为新的 override 排序更晚。脚本目前只在本地准备，尚未上传或执行。

## 证据

本地目录 `build/benchmarks/20260906-p2g/` 保存源码基线、完整补丁、源码/包 SHA、测试、原始基准、汇总 JSON、CPU/goroutine 样本、Linux 构建/生产基准/部署回退脚本及只读盘点脚本。`package-index.json` 标识可上传的 `deploy-inputs.tar.xz`，当前 13,768 字节、7 个 Base64 分片。后续改动须重新生成包与指纹。
