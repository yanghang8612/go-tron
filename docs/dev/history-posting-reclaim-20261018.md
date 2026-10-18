# History posting 清理饥饿与可回收量核验（2026-10-18）

状态：全量只读 census 和服务器候选版本复测已完成；私有副本回收实验进行中。
以下精确字节数均明确区分逻辑 KV 与磁盘 SST，尚未把逻辑量宣称为磁盘收益。
用户已批准上线，`55999aa4` 已推送 master。01:49:38 UTC 最后可见回执为自动部署
正在完整测试、生产仍为原 PID 12896；随后本机 SOCKS5 1088 及浏览器观测连接中断。
截至 02:03:46 UTC 尚无发布后样本，不能确认版本切换或宣称已生效，也不能据此认定服务故障。

## 已确认的可清理范围

冻结边界 `H=33,816,830` 下，有 **1,925,758,076 个完整 posting 帧**满足
`last(frame) <= H`，包含 **7,804,467,808 个区块引用**，编码键值合计
**133,489,815,146 B = 133.489815146 GB = 124.322078327 GiB**。
它们占 posting 逻辑字节的 **98.135903619%**。

这是一致性副本上的精确谓词统计。线上每批删除仍须获得本进程成功完成的
cold-covered hot-prune 权限，并在持有 writer locks 后重新验证 canonical proof、
Finish / StateHistoryIndex 进度及固定 anchor。冷 manifest 的水位本身不能授予删除权限。

| H=33,816,830 的分类 | 帧数 | 区块引用数 | 编码键值字节 B | 处理 |
|---|---:|---:|---:|---|
| 完整过期帧 | 1,925,758,076 | 7,804,467,808 | 133,489,815,146 | 有正常线上证明时允许删除 |
| 跨越 H 的帧 | 34,201 | 369,678 | 2,675,879 | 整帧保留 |
| 首区块晚于 H 的帧 | 37,047,271 | 116,640,379 | 2,532,969,814 | 保留 |
| 全部 posting | 1,962,839,548 | 7,921,477,865 | 136,025,460,839 | 三类守恒 |
| key 目录 | 831,511,333 行 | 不适用 | 72,953,208,545 | 全部保留 |

全量扫描逐帧校验编码、顺序和大小；遍历全部目录并验证其值为空。每个分类都记录
按长度分帧的原始 KV SHA-256，后续副本实验必须逐类一致。

另以旧 worker 目标 `H=32,511,274` 分类，完整过期帧为 **1,727,197,380** 个、
**7,230,148,874** 个引用、**119,953,531,923 B**；跨界帧 **139,759** 个、
**9,982,459 B**；较新帧 **235,502,409** 个、**16,061,946,457 B**。
这是同一个冻结库的两种边界划分，不是两次历史库大小采样。

## 数据分布与口径

posting 可见编码键共 **125,621,731,072 B**，值共 **10,403,729,767 B**。
每个物理键长 **64 B**：24 B schema prefix + 32 B key hash + 8 B 首区块号。
键占 posting 逻辑量 **92.351630568%**。此前进度消息中的 90.91% 是按 63 B
错误换算，已根据实际全量字节计数纠正。

全库 posting 含 **687,885,563** 个不同 key hash；**1,393,879,802** 帧仅有
一个区块引用，占帧数 **71.013435786%**。平均每帧 **4.035723589** 个引用。
这些是物理帧与引用统计，不能把 ETL 的 applied 引用数当成物理帧写入数。

补集扫描覆盖所有非 posting KV，包括目录、changeset、tx-range、元数据等：
**865,988,470 行、84,169,150,695 B**。因此可见 history 总逻辑量为
**220,194,611,534 B**，此次完整过期 posting 占 **60.623561229%**。
扣除本次过期 posting 后剩余 **86,704,796,388 B**，其中目录占 **84.139761102%**。
这说明目录是后续重要研究对象；本次不推断其中多少可安全删除。

逻辑量只加一次当前可见 KV 的编码键与值，不包含 SST 内部版本、tombstone、
索引、压缩差异、WAL 或压实临时空间。此前 history 约 69 GB、索引范围约
57.567 GB 是磁盘文件量或 SST 范围估算，与上述未压缩逻辑量不是同一口径。

## 冻结副本与一致性证据

用户明确批准短暂停机采集副本，并立即恢复原服务。停机及恢复验证窗口为
**2026-10-18 00:12:30.978709–00:12:53.562829 UTC，22.584120 秒**。
生产原 PID 11603，恢复后 PID 12896。恢复的可执行文件及参数与采集前一致：

```
/data/gtron/releases/auto-8da8655867cb-ae1af3cfa4fd584d/gtron
sha256 ae1af3cfa4fd584d0f553811c53f21f632b8d13ab64ac97d998ebb8611bb65bb
```

副本目录：`/data/gtron/posting-census-20261018-0016`。采集持有两把部署锁，
服务停止期间复制 chaindata、history 和冷 manifest。只对不可变 SST 建硬链接；
WAL、MANIFEST、CURRENT、OPTIONS 等独立复制，并检查源目录清单未变化。
实际采集版本的 DB LOCK 使用了 flock，而 Pebble 使用 fcntl；本次一致性依据是
服务已停止、部署入口被锁住、源文件清单稳定，不能把 flock 描述为 Pebble 锁互斥。
仓库内采集脚本已改为 lockf，并持续持锁至 manifest 复制结束。

捕获的冷 manifest generation **43260**，hotPrune **33,816,830**，
visibleTxEnd **2,460,017,153**。配对元数据验证 `pair_equal=true`：

- head / Finish / Execution / Commitment：**34,146,982**；对应 canonical hash 一致。
- 最新 solid：**34,146,932**；SnapshotPrune：**34,145,892**。
- StateHistoryIndex：**34,146,638**，位于 history，携带 canonical hash。
- `H=33,816,830` 的 canonical 记录存在；旧边界 `32,511,274` 已不在 hot。

副本只包含本次审计所需的 hot/history 和冷 manifest，不是包含所有冷正文的节点备份。
全量 census 于 **2026-10-18 00:14:03.801552864 UTC** 开始，用时
**4,016.361116087 秒**，扫描完成并关闭只读 DB 后才启动回收实验。

服务端原始证据保存在上述目录的 `capture.json`、`cold-manifest.json`、
`census-before.json`、`combined-metadata.json`、`sst-envelope.json` 中。
初始证据归档 `/tmp/gtron-posting-evidence-initial-20261018.tar.gz`：
**13,328,951 B**，SHA-256：
`a1b822fc0f25855166f187515ddc2113ed5381fc361ef6f897ce075359cf9754`。
census JSON 的 SHA-256：
`fadbef8e1934592384cd804b26045743b31ea4b2bdeecfd7180659ee61cf6fd5`。
本地结构化记录 `build/benchmarks/20261018-posting-liveness/observed-summary.json`
明确标为终端转录，并已通过服务端代码逐字段比对原 JSON；本地与服务端规范化 JSON
均为 **1759 B**，SHA-256
`afd12d19d869b9b00fd1b4531c9c371504c6194b9f494452662e78ffe6158bb4`。
校验捕获并纠正了补集字节数及 census SHA 的转录差异；过期帧计数与字节数始终一致。

## 饥饿原因与实现

优化前十分钟、61 点采样中，history compaction debt 为 38.95–40.17 GB，
hot 为 1.71–5.16 GB；组合值始终高于固定 32 GiB veto。worker 已开启，
但 callbacks / scanned / deleted / sweeps 均为 0，pressure defer 增加 599。
debt 是待压实工作量估计，既不是可删字节，也不能单独证明设备正在饱和。

候选修改：

1. 低于 32 GiB 的原设备门槛不变。达到或超过 32 GiB 时，允许新鲜设备观测
   `await <= 2ms`、平均队列 `<= 8` 的小批次；硬压力、L0 >= 8、观测过期或
   设备信息缺失仍拒绝。调度前与获取 chain lock 后使用完全相同的分类函数。
2. 每批仍为 4096 扫描帧、1 MiB 扫描 KV、256 KiB 删除 KV、10ms 协作式扫描预算。
   不改变 canonical / cold proof、cursor 提交规则、目录、gate、reservation 或 cooldown。
3. 完整 census 表明，原固定 1 秒重试即使每批满 4096 行，一次遍历也至少需要
   **5.55 天**。因此有进展且存在计时的成功批次改为至少休息 250ms，且至少休息
   实际 chain lock 持有时间的 49 倍，以完成的工作/恢复周期为单位目标占空比 2%。
   缺计时、无进展、拒绝或出错仍以配置间隔为下限；慢失败也按实测持锁时间恢复。
   排队时间不计作独占工作；恢复期间不持 gate 或 writer lock，取消可打断计时器。
4. 新增实际锁内获准的 `debt_relief/{callbacks,chunks,deleted_bytes}`、
   `last/recovery_ns`，以及批量索引成功推进时的 `posting_frames_written` 和
   `directory_writes`。后者是 upsert 写入量，不是不同目录键的净增长。

两个 census 边界间多出 198,560,696 个完整过期帧，跨 1,305,556 个区块，约
152.089 帧/区块；按先前观测 34.177 blocks/s 换算约 5198 帧/s。
这是高度窗口密度估算，不能替代新增物理写入计数器测量，也不能承诺上线后净追赶速度。
10ms 是逐行检查的协作式预算，无法中断单次 Pebble I/O 或限制锁排队的最长时间。

## 私有副本物理回收实验

实验位于 `/data/gtron/posting-census-20261018-0016/private-reclaim-985605299`。
最后可见阶段为 01:49:38 UTC 的 `control-compact`；连接恢复后需复核最终状态与回执。
input history 始终只读，创建 control 与 pruned 两个独立可写副本：

- control：只做全范围压实，核验全部 posting、目录和非 posting 补集摘要。
- pruned：用生产同一 whole-frame 原语删除，再做同样压实；删除数/逻辑字节须与
  census 一致，跨界/较新帧、目录和全部非 posting 数据摘要必须保持一致。

对照收益定义为两份副本完成压实、核验并关闭后的 SST 文件字节及分配字节之差。
这隔离普通压实本身的收益，是该冻结副本上的实测反事实差值，不是运行中 LSM
可逐字节兑现的空间承诺。副本中的共享输入硬链接不能按独立新增占用累加。
实验批次 65536 行/8 MiB/250ms 仅用于离线副本，不能据此宣称线上清理速度。

运行约束：CPUQuota=100%、MemoryLimit=2 GiB、低 CPU/I/O 优先级、单 compaction，
保留 500 GiB 可用空间；不对生产强制 compact。完成后保留回执并释放本次复制的 DB，
否则审计硬链接会继续固定旧 SST，影响真实主机空间回收。

## 验证与发布状态

本地包测试、core posting guard 重复十次、race、vet、Sapling 构建通过。
新增测试覆盖压力边界、新鲜度、硬保护、锁后重检、计时恢复与取消、失败退避、
真实 Pebble census/删除一致性、损坏帧和迭代错误，以及目录及补集保留。
离线工具另通过 100000 帧、64 KiB memtable 的多 flush 测试，关闭自动压实时仍能完成；
L0 stop 判断 sublevels/read amplification，不能把 SST 文件个数误当 sublevels。

服务器初版候选已通过相关测试、race、vet、构建。加入追赶调度后的完整候选也已通过：
完整 pruning/net/cmd 包测试、core guard 重复十次、相关 core/pruning/net/cmd race、
vet、Sapling release 构建，五步 exit code 均为 0。
使用已安装 GCC 11 执行测试/race/vet，最终 release 构建沿用 `CC=gcc`。
验证回执：`/tmp/gtron-posting-native-20261018/native-catchup-result.json`；
候选二进制 `/tmp/gtron-posting-native-20261018/gtron-catchup` 的 SHA-256：
`bf8d024641962131b8e68a8cdeb4e2bbc416838255395385c9a01d49a48c6e77`。
曾中止的仅是 GCC 4.8 慢速 cgo 验证任务，未中止生产节点。

本地提交：`3683b76b`（压力准入）、`8c4465ca`（审计与副本工具）、
`00590434`（按持锁时间恢复及写入指标）、`55999aa4`（初始报告）。
用户批准后，四个提交已推送 master。后续报告校正及实验回执先保存在本地，
避免单独的文档更新再次触发自动部署重启。

上线前、实验运行期间的原版本复采样为 01:37:58–01:47:58 UTC，共 61 点、
600.442699 秒：head 34,339,350→34,361,032，**36.110 blocks/s、3162.771 tx/s**；
冷发布 34,010,926→34,032,607，**36.108 blocks/s、3212.287 txNum/s**。
所有点均未暂停，冷错误仍为 0；posting callback/deleted 仍为 0，pressure defer +600。
这是相邻不同历史区块的运行观测，不是同输入 A/B；原始 JSONL 与摘要在本地证据目录。
