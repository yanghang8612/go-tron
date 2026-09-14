# 共享历史读侧优化与冷迁移门槛核验

2026-09-14。业务源码 `833e4a0b02f3d490386b74a8114006658f035b80`，运维
`4c1bc43be378e39f1f5c2154454770ab509df132`，已推送 master，原生验收通过，UTC08:29:31切换完成；部署后完整采样与最终健康复核通过。
设计见 [方案](../superpowers/specs/2026-09-14-history-reader-cpu.md)。

## 业务范围及固定输入

生产只修改 shared reader：Snappy 直接解码到已预检总长度的私有 pack 输出区，
减少临时大切片和复制。raw 路径仍复制一次。每次 Get/Has、完整引用表预检、
长度/Snappy/chunk SHA/整包 SHA 保持；不引入缓存、认证复用或持久化格式变化。
错误仍返回 nil，部分输出不对回调公开。writer 与独立 nil-dst decoder 所有权不变。

五个旧 reader 函数逐字冻结自 cda09766，仅重命名函数及内部调用；root 与独立代理
验证所有权、预检切片边界和完整读 trace。包含 raw/Snappy、唯一/重复引用、逐操作
故障、返回值中途变化、全部 pack 字节截断/突变、随机损坏，以及公共 owning/borrowed
多压缩片段读取的末段失败无回调/无快照泄漏。初次新增公共测试漏写 borrowed API
既有 BlockHash 预期，已修正测试 fixture；没有为通过测试修改生产语义，初次日志保留。

本机 Apple M1 Max / Go1.25.5 / GOMAXPROCS=2，完整方法 28 case×5、100ms：

| 固定输入/读取契约 | 旧→新中位数 | 时间变化 | 少分配字节/次数 |
|---|---:|---:|---:|
| 6MiB Snappy unique / Has+Get | 6.581→6.153ms | −6.50% | 6MiB / 48 |
| 6MiB Snappy repeated / Has+Get | 6.548→6.099ms | −6.85% | 6MiB / 48 |
| 6MiB Snappy unique / coupled | 6.604→6.053ms | −8.34% | 6MiB / 48 |
| 6MiB Snappy repeated / coupled | 6.584→6.045ms | −8.19% | 6MiB / 48 |

四组各自五轮范围均不重叠，仍不是统计显著性测试。raw 控制组 −3.51%至+1.01%，
多数范围重叠且无分配变化；mixed 减少3MiB/24次，coupled 耗时仅−0.91%且范围重叠。
基准包含完整 header、每个存储接口调用、解码与全部 SHA；采用固定内存输入，
不含磁盘延迟、fixture/RLP 构造、RLP解析或链执行。B/op 非 RSS 或持久化字节。

本地 rawdb/...、state/...、freezer、actuator 全包通过（snapshots95.874s）；
rawdb/state/snapshots 的 shared/history/as-of 定向 race 通过（macOS已有链接器警告，
退出0），core 历史重组/rewind/as-of定向通过6.446s，rawdb vet通过。
34项新部署/验收 Python测试在冻结前后均通过。此次没有把core定向冒充全包；
上一轮core全包两项旧版可复现断言限制见上一轮记录，本轮没有为拿全绿重跑全仓。

## 本轮旧版在线基线

cda09766 / PID3454 / start ticks4510362787 / process-start1789369134357715998，
UTC08:03:38.679173原生candidate check/healthy_mode通过。历史余额canary
`0xb6283b0374b0e000`于08:15:39再次通过。独立du：
chaindata250,464,681,984B；state-snapshots684,988,391,424B；ancient235,687,653,376B；
可用空间1,809,790,791,680B。原生完整证据 `/tmp/gtron-history-reader-before-native.json`。

13点窗口UTC08:00:55→08:06:55（同进程，不跨重启）：13.599块/秒、2715.36交易/秒、
199.67交易/块。Wallet session计数是吞吐分母；不能混用currentBlock变化（约13.65/s）。
stateCommit rolling中位数34.491ms；density_metadata_work的13个采样值中位数1.248652s，非每块耗时。状态cold30727219→30729774，
推进2555块约7.10/s；eligible lag499065→501392（+2327），head-cold gap564755→567113。
pruned-through最后30729821，指标非原子，不据短暂领先判错误。
body/index coverage30670848未变，headgap621126→626039，eligible V2 backlog9整段。
GC retired39→39，coverage defers+136；队列各累计增量0，错误无新增。
检查的非shadow错误均0；VM shadow errors544→626（readiness+74、unsupported+2、result+6）。
obsolete SST gauge最低−666,280,464B/−44files，不用于证明物理空间变化。

本轮CPU基线UTC08:01:28.134191起45.18秒，112.82 CPU秒；reader整包解码仅1.49秒，
全SHA3.68秒，commitment pipeline32.53秒。与上轮reader10.42秒的输入/背景任务差别很大，
不使用上轮高热点样本冒充此次before，也不把6分钟块数用于45秒CPU归一化。

## body/index 停留的具体原因与数据分布

原生只读单fd读取manifest，前后fstat一致、128MiB上限、未打开生产DB/segment/SST。
UTC08:10:20.073847取到generation35095（47,042,548B、active73113、retired96630），
读取后路径仍同代。event-log与event-log-index均从block1连续至30,730,803；
下一body段[30,670,848,30,736,383]还缺[30,730,804,30,736,383]，即5580块。
这直接证实ReceiptLogRangeCovered的元数据覆盖门槛尚未满足；它不是文件认证证明。
index受body coverage限制，没有独立index debt。

同一代active文件逻辑尺寸如下（不是du；retired条目数也不等于尚存文件数）：

| 数据 | active引用数 | 逻辑字节 |
|---|---:|---:|
| 状态变更历史 history | 2885 | 447,235,691,925 |
| 状态变更定位 accessor | 2885 | 123,203,316,901 |
| 事件日志 | 32229 | 88,223,394,288 |
| 事件日志索引 | 32229 | 16,385,827,926 |
| 状态变更倒排 inverted | 2885 | 9,892,304,227 |

状态三类坐标是TxNum（连续到1,931,653,819），事件两类坐标是block，不能混用。
末尾20,000行日志筛选freezer pass失败、event coverage错误与sync-critical event catchup
共0匹配；这只限被读尾部，不能替代完整历史日志。directV2覆盖/append错误不会都计入
v2/errors，因此未用该指标单独宣称无错误。原生摘要 `/tmp/gtron-history-reader-event-manifest.json`。

同range history/event builder已并行。13个last gauge里history中位数4.810s、event0.372s，
这些是重叠采样，不是33次构建完整分布；history侧明显更重。event目标被已发布状态
历史前沿限制，body再等下一段对应的65536块事件日志连续覆盖（可由多个event段组成）。因而当前优先减少历史读侧工作，不能通过
跳过覆盖验证提前删热数据。body/txindex/receipts的keyspace估计窗口约各增加297/93/88MB，
但各估计时点不同，SST边界可能重叠且含旧版本、无WAL/memtable，不相加冒充du净流入。

## Shadow 返回状态检查

进一步核对上轮+61：正式VM publisher、async VM retry全窗口未启用，影子采样仍运行。
readiness在其他六项有效性条件后拒绝contractRetMismatch，并在read-version分类前跳过。
已有源码/历史实例证明过期推测读取可能造成REVERT→SUCCESS，但本批无逐交易证据，
不能直接称为正常冲突或调度超时。普通shadow路径没有对应逐交易日志，另一路
async contract_ret/last指标不能替代它；本轮不为诊断启用VM publisher或改变共识。

## 发布记录

业务源码833e4a0b、运维4c1bc43b。部署脚本SHA256
`80ba01f22f40761bae9b8d6787cc1680778062a964531b2d6de07e3cbe586aa5`，验收脚本SHA256
`998d34e23f1c3aac2f5f50f025d45f9a15324d457982f0d955be05d9dc1c595a`。
即时回滚保留cda09766的writer ON/OFF与全部六代永久reader标记，端口/预算不变。
原生prepare于UTC08:21:22→08:23:40退出0；builder原有rawdb全包和历史相关验收通过。
prepared SHA256 `d28500787b3992e85d18843529558198491393b376e51ff2efced7412a0efeef`；
二进制SHA256 `0c7e2d411d385e936d77160ff7fa98c2bc6ea1923760f1ae0a87e7b2edbecda9`。
额外原生验收08:26:08→08:27:33，Go1.25.5 linux/amd64、Sapling、GOMAXPROCS=2；
state、snapshots、pruning、freezer、actuator全包退出0，47.864s；完整读侧28 case×5，
200ms基准37.866s通过。完整原生JSON：
`/tmp/gtron-history-reader-acceptance-20260914/20260914T082608046350Z-27335/native-summary.json`，
SHA256 `a03c34acac20005383b76c651be6649d443fc1c1c15b103541f265b81dfe4b92`。

| 原生6MiB Snappy输入/读取契约 | 旧→新中位数 | 时间变化 | 五轮范围 |
|---|---:|---:|---|
| unique / Has+Get | 47.715→41.917ms | −12.15% | 不重叠 |
| unique / coupled | 42.012→40.257ms | −4.18% | 重叠 |
| repeated / Has+Get | 43.950→39.875ms | −9.27% | 重叠 |
| repeated / coupled | 50.262→40.214ms | −19.99% | 不重叠 |

上述四组每次均少分配6,291,456B/48对象。raw四组−1.79%至+3.40%，范围均重叠；
mixed Has+Get +4.40%、coupled−2.49%，范围也重叠，每次少3MiB/24对象。
8KiB Snappy Has+Get +0.49%且范围重叠，coupled−9.93%且范围不重叠，各少8192B/1对象。
不省略无明显收益或变慢的控制组，不将原生在线共载基准视为整链速度对照。

activate UTC08:29:03→08:29:31退出0，phase=active，新PID28510、ticks4510905452、
process-start1789374561008796272。UTC08:30:49 candidate check/healthy_mode再次通过，
08:31:26历史余额canary仍等于部署前值。未改变writer、持久化格式、端口、预算或GC守卫。
部署后完整窗口和最终物理尺寸如下。


## 部署后CPU组成

UTC08:31:47.143313起45.14秒，共122.88 CPU秒，进程年龄146.1秒；
profile SHA256 `2a2768e4f1aa589f74a7f32cc7d1f5e31a41b933b330602a361dedf335814979`。
before年龄3753.8秒，块高/交易/冷热缓存和后台任务不同。本轮reader整包1.49→1.71 CPU秒，
占比1.32%→1.39%；未形成线上reader耗时下降证据，不能将固定输入收益泛化至本窗口。
reader下mallocgc采样0.10→0.07秒，Snappy0.01→0，样本太少，不当作调用次数或精确分配测量。
新旧chunk函数改名/内联不同，旧chunk行降到0不代表校验被省略；新Into下仍有0.73 CPU秒。

当前commitment pipeline34.21秒（27.84%），其parent readDurable20.85秒（16.97%），
其partition snapshot Get10.94秒、prefetch snapshot Get7.32秒；这些嵌套行不得相加。
Pebble compaction16.54秒（13.46%），shared-history chunk GC4.18秒（3.40%），Go GC4.74秒。
总SHA3.68→8.43秒，主要新增state-domain段checksum3.15秒、block交易hash1.32秒，
与新窗口背景验证/迁移一致；不能据此判定reader新增了SHA或全部校验均为冗余。
metadata目标并集6.91→2.58秒，同样只作负载组成对照。未把6分钟吞吐用作45秒CPU分母。


## 部署后完整窗口、积压与磁盘

13对请求全部成功、stderr为空、同一新进程；UTC08:30:48.112126→08:36:48.031847，
capture SHA256 `c41639cb734aaa54b40150df5c2314c7e9e1825e09f9325cdef37c20f511810f`。
Wallet session吞吐13.599块/秒、2879.19交易/秒、211.73交易/块，旧窗口分别13.599、2715.36、199.67。
吞吐块速率基本相同；交易密度升高，不据TPS提升宣称版本提速。stateCommit rolling13点
均已初始化，采样中位数34.491→29.026ms；这些是重叠窗口观测，非独立块延迟或因果收益。
density_metadata_work采样中位数1.248652→1.309112秒。

eligible state lag514,386→516,978（+2,592，约+0.50%），旧窗口+2,327；状态cold/pruned
30,740,061→30,742,471（+2,410，约6.70块/秒），旧cold约7.10/s。
仍是约50万块的同一数量级，但窗口内确实增长，不能称为积压不变或历史构建已追平同步。
head减state cold覆盖580,079→582,557，与eligible lag口径不同。

body在after首点已达30,736,384，相比before末点增加一个65,536块段；本窗口body未再前移。
tx index从30,687,232追到30,736,384（+49,152），末点已追平body。
因此before时存在的具体5580块事件覆盖缺口已跨过，但无法将跨越时刻或原因全部归于本版。
UTC08:36:21 manifest generation35233、47,229,885B，单fd/fstat稳定且路径仍同代；
event与event-index均从1连续到30,742,184，各32,359引用，元数据摘要
`/tmp/gtron-history-reader-final-manifest.json`。下一body段[30,736,384,30,801,919]仍缺
[30,742,185,30,801,919]，即59,735块；无需以此误判body每分钟不动为卡死。

本窗口shared GC完成退休5次；coverage defer79次、busy12次、errors0。
GC队列5次申请均获准、错误0，retirement闭包累计40,345,744ns；并非整个GC耗时或物理回收量。
核查的非shadow错误均无非零值；shadow errors+69（readiness+47、unsupported+2、result+20），
这些不是正式区块导入失败，也未通过此次reader修改解决。obsolete SST bytes最低−363,606,425B，
仍需修正该观测量；本报告使用独立du判断磁盘，未以负gauge当作释放空间。

| 原生du字节 | before UTC08:03:38 | after UTC08:30:49 | 最后 UTC08:36:18 |
|---|---:|---:|---:|
| chaindata | 250,464,681,984 | 250,321,047,552 | 250,372,816,896 |
| state-snapshots | 684,988,391,424 | 686,367,617,024 | 686,489,477,120 |
| ancient | 235,687,653,376 | 236,784,488,448 | 236,912,140,288 |
| 文件系统可用 | 1,809,790,791,680 | 1,807,146,811,392 | 1,806,873,817,088 |

每路径独立du，时点非原子。约33分钟内热库−91.87MB、冷快照+1.50GB、ancient+1.22GB，
包含部署前持续同步与切换，不作为新版本净收益。仅after两次du间约5.5分钟，热库+51.77MB、
冷快照+121.86MB、ancient+127.65MB；总历史存储仍增长，没有把数据转冷当作总空间消失。
本轮降低分配与复制，持久化字节表示未变；无法仅凭这段持平证明长期磁盘膨胀已根治。
此前共享writer去重、冷迁移、逻辑裁剪和Pebble压实分别解决不同环节，必须分开衡量。

UTC08:36:18最终candidate check/healthy_mode仍通过，同PID/ticks/process-start；
08:37:09历史余额canary再次通过。最后Wallet样本currentBlock31,325,060，
appliedTip31,325,089、targetHead86,234,271、remaining54,909,211（这些字段独立采样）；
21 peers、8 sync peers，同步active且未paused、未fetchBackpressured。
网络待下载/导入高度差与约51.7万块的本地冷历史积压是两种不同口径。

## 后续优先级

1. 同步CPU优先继续检查commitment持久层读取：本轮pipeline27.84%，其中readDurable占16.97%；
   先在匹配输入下量化读放大/命中与等待，再决定缓存或批量读取，保持父版本与快照一致性。
2. 磁盘与冷迁移优先关注状态history及accessor（同代逻辑尺寸447.24GB与123.20GB）和
   历史发布/裁剪吞吐差；事件覆盖与body段落是下游门槛，当前不是独立event CPU不足。
   后续需要更长连续窗口评价约50万积压的斜率；当前未创建定时任务，也不声称已获得隔夜结果。
3. shadow contractRetMismatch与obsolete负gauge应分别补有界观测和修正统计；不放宽共识校验、
   文件认证、覆盖检查或回收守卫。

完整原始采样、分析器、测试日志与本地转录摘要保留于
`build/benchmarks/20260914-history-reader/`；原生原始测试/基准与发布证据保留于上述服务器路径。
本地独立审查复核14对转录计算与范围、CPU组成及发布身份，原始140行由原生runner严格校验，
不冒充已在本地二次重算原生全部中位数。
