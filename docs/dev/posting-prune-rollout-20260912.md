# Posting 有界清理上线记录（2026-09-12）

**公平交接版已上线并取得实际清理进展**：北京时间12:44—12:54的完整10分钟窗口成功提交132片，扫描509472行，删除489679行、33702676编码字节（32.14 MiB逻辑量）。固定H=27813397的整轮扫描尚未完成。同步约21.12块/s、2511笔/s，积压有所增加但仍在33—37万块量级。

**物理chaindata仍在增长**：12:56实测170.448 GiB，较12:07的157.678 GiB增加12.770 GiB。确认了逻辑清理有效，尚未确认物理缩容或同步加速；目录变化横跨两个部署和持续导入，不能归因于本次清理。

## 最终部署与验证

| 项目 | 固定值 |
|---|---|
| 源码提交 | d3793a24f6a2f339c8d23dc2e9eb0bc6f2c2ac31 |
| 部署脚本提交 | 8593a423b7a95ffb48b9da0275d70e2367b0b46b |
| 脚本SHA256 | 1d827bb9c8f1facc7d3a0cad56320edbd45a34cb51eb6c4b56b0586db87174fc |
| 原生二进制SHA256 | 5fed1e1a9cbf46c2e24a5581d09920f34f83281d23bdbaff0ef720ddeb502836 |

发布目录为 /data/gtron/releases/20260912-posting-prune-fair-lock。最终服务active，PID18236、start_ticks4492264350，HTTP精确进程标识1789188149990537883；启动头28147312→28147315。后测服务复核与activation-after配置完全一致：三个开关均为1，40 GiB内存限制、guard、hold及其他服务配置保留。回滚恢复原unit完整字节和元数据，返回首版 /data/gtron/releases/20260912-posting-prune/gtron，二进制SHA256为067905d9c852ef13a19e44f549220aa9439da81ad7e466623c0b9763660e85e4。

本地受影响rawdb、pruning、snapshots、core、cmd/gtron全包测试及新增race通过，增量lint为0问题；公平交接的Guard/StateHistoryIndex/SyncInsertSession回归普通4.478秒、race6.076秒通过。最初受限环境的回环监听失败已在允许监听的环境完整重跑通过，不是全仓测试结论。两个部署helper各通过11项离线检查、14项固定版本审计。

第二版原生prepare在首版完整采样结束后开始，于12:36:59通过Go1.25.5、CGO、Sapling构建及受影响包、三个开关启用、目标回归和原生Sapling probe；prepared与独立sha256sum一致。正式后测没有构建或du重叠。后续对照TronGrid的28160000、28160715、28161400三个区块，6个请求的块哈希、父哈希、交易顺序全部一致（分别104/116/171笔）；这是抽样兼容性检查，不代表全部区块或状态根证明。

## 实现与首版修正

GTRON_POSTING_PRUNE=1仅在snap+history模式启用。正常热历史prune在pass前验证带哈希的durable Finish，成功完成冷覆盖核验、热删除和manifest进度发布后，才原子发布本进程许可H。每片再检查固定proof、Finish/index水位、canonical H及固化状态，只删除末块≤H的完整posting帧；目录和混合帧保留。

首版使用chainmu.TryLock，在完整21点窗口中仅成功1片、删除183916逻辑字节；425次核心回调结果有424次延期。早期零进展不作为最终结论。公平版保留index TryLock与index→chain锁顺序，改为在导入交接时排队获取链锁，拿锁后复核取消，持锁至批次提交；独立审阅未发现新增锁环，退出先join worker再关闭数据库。新开关启用时停用原posting/目录全表清扫，空值或0恢复旧路径。

每片预算4096行、1 MiB扫描编码字节、256 KiB删除编码字节、10 ms扫描时间，完成后间隔1秒。字节/时间是行间协作预算，单次I/O或提交可超出；不存在1秒稳定完成一片的保证。iterator先释放再提交，cursor仅成功提交后推进，重启从头幂等扫描，不写持久cursor或旧StateChangeIndexPruneBlockNum；后者仍只表示完整posting/目录清扫。

## 最终21点采样

Before为12:04—12:14，公平版为12:44—12:54，各21点、42请求全部有效并关联各自单一进程；不跨重启减累计计数。公平版Wallet/metrics分别599.987860/600.017788秒，before含一次只读du和git fetch，比较不是完全隔离实验。

| 指标 | Before全窗 | 公平版全窗 | 观察差异 |
|---|---:|---:|---:|
| session块/s | 21.3905 | 21.1204 | −1.26% |
| session tx/s | 2465.25 | 2511.49 | +1.88% |
| tx/session块 | 115.250 | 118.913 | +3.18% |

后半段公平版20.6254块/s，较before后半段21.4554低3.87%，但TPS高4.98%、每块交易量高9.21%。各版后半段均约270秒，公平版metrics/Wallet为011—020、270.065424/269.958226秒；full与后半段重叠。不同高度、交易/VM构成、重启预热与后台工作影响吞吐，不能把这些差异直接归因于清理。19个完整import发布窗口的执行/状态提交/persist约36.52/18.60/6.42 ms/块，阶段重叠，不相加。

| 公平版距头/缓冲 | 首→末 | 变化 |
|---|---:|---:|
| state published/pruned距头 | 332796→334446 | +1650 |
| body coverage距头 | 361455→374114 | +12659 |
| index coverage/pruned距头 | 361455→374114 | +12659 |
| 网络raw buffer | 1319→1876块 | +557 |

State发布和热清理均推进11009块；body/index窗口内未发布新批次，不等于后台停止。三类距头互有重叠，不相加；仍在同一数量级，但不能写成积压没有扩大。Raw buffer观察范围848—1895块，字节39.25→49.87 MB、峰值60.48 MB，全部active，无paused/backpressure。

公平版全窗成功132片、删除489679行/32.14 MiB逻辑量，sweeps、errors、resets均为0。pressure/gate/chain延期增量为286/165/1；后半窗成功95片，gate延期128次、pressure35次。核心回调提交从首版1/425变为132/133，支持主要准入饥饿得到缓解，但两窗负载和H不同，不将分片数当通用加速比。已扫描前缀约96.1%的行被删，不能据此推断整个约37.9 GiB索引都有相同比例可回收。

Compaction debt为37.419→11.139 GiB、峰值40.151 GiB，L0 sublevels为1—4；初期超过32 GiB压力门限时先延期，压力下降后才形成持续清理。写stall、SST读取/prefetch错误，以及所选canonical/storage/non-shadow错误增量均为0。Shadow sender错误+3、VM错误+87（76 readiness/8 unsupported/3 result），单独观察，不相加父子计数，也不视为canonical失败。

## 空间变化、瓶颈与下一步

最终du在12:56:44执行，返回0、无stderr；allocated口径如下。与12:07基线及12:54末次HTTP均非原子同刻，不能逐字节对账。

| 实际目录 | 12:07 GiB | 12:56 GiB |
|---|---:|---:|
| chaindata | 157.678 | 170.448 |
| snapshots | 439.841 | 443.217 |
| ancient | 178.281 | 179.239 |
| 整个gtron目录 | 775.799 | 792.905 |

数据盘可用2145615654912字节，约1.95 TiB。末次异步SST估算中，changeset由before约42.195增至50.811 GiB，commitment约48.851 GiB、变更索引约37.909 GiB；变化更突出的是热历史包。范围估算含旧版本/压实影响且边界可能重叠，12个范围不相加当精确全库，不能将changeset差值精确归因到du增长。11/21点仍有已知obsolete负记账，同源disk总量不用于回收量判断。逻辑删除、snapshot pinned累计量、compaction debt均不是已释放空间。

当前清理的限制主要转为**存储压力与共享维护调度**。排队链锁时持有index锁和共享gate，gate持有≥250 ms触发15秒冷却，等待时间也计入；gate延期还包含忙、预约及准入拒绝，不能全算作冷却。压力仅在排队前检查，长等待后暂未复核。最后回调约29.09 ms、运行期最大979.20 ms，包含排队、证明读取、扫描和提交，尚不能分离锁等待或实际持链锁时间。

下一步优先量化冷历史/changeset增长与state/body/index发布节奏，核对共享gate竞争是否拖慢归档和热清理；再补分片排队、证明读取、扫描/提交的分阶段耗时，评估获锁后压力复核和冷却策略。先确认多个窗口的清理持续性与积压趋势，保留压力门限，暂不盲目提高posting预算或强制压实。整轮posting扫描及其物理压实回收仍待后续观察。

证据位于build/benchmarks/20260912-posting-prune/：local-validation.json、server-fair-lock-deployment.json、before-summary.md、after-v1-summary.md、after-fair-lock-summary.md/json、after-fair-lock-analysis.json、fair-lock-independent-review.json、canonical-after-trongrid/summary.json、server-before.json和server-after.json；服务器记录为JumpServer截图人工转录。原始三组采样和fair-lock部署审计保留，文件摘要索引见evidence-index.json。实现约束见[设计说明](../superpowers/specs/2026-09-12-bounded-posting-prune.md)。
