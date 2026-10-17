# Async flush 队列容量实验

2026-10-17，基于 `462da0cbd04643d3ffa0ea4f86d08d6b54bc5dbf`。实验只在忽略目录 harness 中构造容量8/64的实际 BlockChain flushQueue，没有修改生产常量。证据位于 `build/benchmarks/20261017-flush-queue/`。

## 范围与语义

现有worker与postFlush原样运行：collect wait1920ms、inline queue-full backpressure、pending barrier、first-error-wins、错误后保留buffer层、stop/drain，以及32MiB base、64MiB expansion、128MiB source上限和range边界都不变。增大queue本身仍会放宽尚未durable的tail；多56个cutoff不是固定字节数或固定秒数，不能声称durability时序完全不变。

测量使用最小BlockChain+真实BlockBuffer+rawdb Pebble。每层512个通过rawdb accessor构造的合法depth7 branch key；value是真实BranchData.Encode，16个hash-child内容来自固定SHA256 seed，避免低熵压缩伪益。overwrite每层384个重复热点与128个新key；append全部新key。独立memory image与完整durable/reopen image进行逐key/value摘要对照；这些摘要不是共识state root。此前domains ordered pipeline已另行对独立oracle验证root，本实验不把随机child-hash记录称为完整合法共识树。

固定Go1.25.5、darwin/arm64。git archive冻结HEAD的core/blockchain.go、core/blockbuffer、core/rawdb/pebbledb及整个third_party/pebble production Go源码，避免并行压缩实验污染。测试只读反射取得adapter的底层engine Metrics，没有给生产API添加实验接口。实际Batch.Put/Delete/Write计数包含最终成功写入keys/logical bytes；engine累计WAL.BytesWritten、各level BytesFlushed/BytesCompacted/BytesRead分别保留。VFS/engine字节不是设备物理写入、文件系统journal或整节点吞吐。

每次均完整排空已post cutoff，stop worker，调用Compact与Sync，再核对完整reopen image。engine在drain及Compact之后分别取样，不能把留在memtable的工作当节省；最终debt0/in-progress0/L0=0仍不等于强制重写terminal L6、消除了所有历史版本。pipeline计时/分配排除trace构造、独立oracle、最终image验证及reopen，包含写入、post、drain、Compact/Sync和相同采样器开销。峰值source bytes是采样pending层的实际logical key/value之和，不是Go heap或RSS；heap另用runtime取样，含预制fixture/expected image的存活对象，不能把它作为节点额外内存比例。

## 小型初筛

64层，默认256MiB memtable，delay0/40ms每次Batch.Write，两trace×两arm×4次交错，共32次。没有跨自然memtable flush，因此不能作为SST压实压力证据。slow overwrite：实际Batch逻辑bytes5486592→5270016（−3.95%），WAL5527469→5309257 B，最终flush+compact均4793217 B，WAL+SST合计10320686→10102474 B（−2.11%）；batch4→3，queue_full1→0。sampled source峰15.30→17.90MB，foreground最大耗时中位143.633→4.246ms，含drain/Compact时间383.222→262.017ms。

append控制实际keys/bytes完全相同，SST也相同；slow仅少一次batch及相应模拟writer delay，不能把它当字节收益。zero-delay控制两arm写入字节相同；append总耗时724.433→746.462ms（约+3%），短本地时序不支持普遍CPU/吞吐提升。完整范围、heap、分配、读写形状在final-summary.json。

单独blocked/failure控制确认：两arm的barrier不越过阻塞writer，stop等待writer结束，失败保留全部层、所有已post计数结清、成功重试不覆盖首次错误。既有async flush方法和group选择逻辑未修改，没有为被拒绝候选扩大Linux/native验收。

## 跨memtable压力补证

512层overwrite只完成一次自然flush，未满足至少两次门槛，已中止并保留pressure-512-invalid.txt/source，不能作为接受证据。随后延长至1024层，显式32MiB memtable，仍为512keys/layer、相同合法value与40ms writer delay。两trace×两arm×2次ABBA。overwrite每48层调用现有WaitForFlushSettled形成固定burst，max encoded3.708MB量级（实际3707916 B），按相同固定key/value的759 B/node保守memtable估算为4954752 B，小于实际large threshold16776654 B。最初append的48层encoded约13.96MB，虽小于16MiB，却因skiplist最坏node开销令memTableSize超过large threshold；这部分被中止并标记无效。最终append改成40层burst，并直接读取每个实际Pebble batch.memTableSize和engine.largeBatchThreshold，在提交前断言前者更小；没有改生产timer/group limits。边界barrier的等待包含producer/总耗时，单次post延迟不包含该独立barrier。初始busy burst可触发queue-full；之后的barrier会让worker赶上，不能由此推断稳态queue-full发生频率。

最终8个有效case的operation/durable/reopen SHA在各trace内一致，所有批次低于large threshold，最终debt0/in-progress0/L0=0。overwrite的四个case在最初进程中已完整返回；随后因append large-path不合格主动终止进程（exit143），不能把该进程宣称为整体PASS。append修正后的独立进程四case整体PASS。两个无效过程均保留，不覆盖。不同trace的burst形状分别固定；每个trace内两arm相同，不能跨trace比较吞吐。

下表是每臂两次中位数，字节为精确计数而非设备物理流量。全部原始范围见pressure-summary.json；overwrite实际memtable estimate4954752 B，append直接测量15544320 B，均小于threshold16776654 B。

| 累计量 | overwrite cap8 | overwrite cap64 | append cap8 | append cap64 |
| --- | ---: | ---: | ---: | ---: |
| 实际written keys | 140672 | 140288 | 524288 | 524288 |
| 实际logical bytes | 79339008 | 79122432 | 295698432 | 295698432 |
| drain时自然flush次数 | 2 | 2 | 12 | 12 |
| 最终WAL bytes | 79929092 | 79710891 | 297896222 | 297896210 |
| 最终flush bytes | 73899918 | 73900386 | 293014179 | 293014179 |
| 最终compact bytes | 8442903 | 8442903 | 0 | 0 |
| 最终WAL+SST bytes | 162271913 | 162054180 | 590910401 | 590910389 |
| sampled source峰值bytes | 13860864 | 13860864 | 11550720 | 11550720 |
| 前台读p95 ns | 49333.5 | 49937.5 | 53187.5 | 54208.5 |
| 含drain/Compact秒 | 42.252 | 40.334 | 51.024 | 50.001 |

compact前overwrite的SST样本少约1.7至3.4MB，经过最后flush/Compact后差异消失，不能当成压实节省。append没有重复key，SST累计完全相同；少一次batch只省12 B WAL framing。两个trace的前台读p95均没有改善。表中整段耗时主要受固定burst/barrier与1920ms窗口影响，不能外推实际同步速度。

## 决策

拒绝cap64，不改生产flushQueueCap=8。有效overwrite压力最终WAL+SST只下降约0.134%，SST/compaction没有收益；append控制没有字节收益。前台更快与减少inline fallback不等于冷化服务率提高，而队列增大会放宽pending durable tail。没有为本负例运行Linux/native或部署。只有两个拒绝spec写入仓库，所有实验源码和日志保持在忽略目录；原27项WIP SHA验证全部未变。
