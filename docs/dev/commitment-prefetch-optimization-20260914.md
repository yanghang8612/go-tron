# Commitment 预取所有权与事件水位元数据优化

2026-09-14。继续master直接开发和GitHub固定提交原生部署。基线833e4a0b；本轮c329bf12已发布，原生验收和完整after窗口已完成。

## 范围

预取session的leader与共享结果follower只需要presence，却调用返回缓存backing的
prefetchIfEpoch，使本未外泄的缓存值被标为exposed。新增只返回存储结果的填充形式，
保留force准入、版本/epoch检查、预取引用和missing/error语义。普通返回字节的API不改。
预期收益发生在新准入后第一次非相等flush与回收，不代表每一次hot flush都节省。
值缩短时可保留原cap/charge，因此必须检查预算与压力行为，不宣称所有淘汰轨迹不变。

事件build frontier的纯计算原本抽取并重复排序完整SegmentRef；候选只整理私有范围小元组。
真实文件读取、认证、canonical hash、已验证覆盖、发布频次和持久化格式均保持。
两个候选独立oracle、固定输入基准与回归验收，不把局部基准等同整链提速。

方案：[预取所有权](../superpowers/specs/2026-09-14-commitment-prefetch-ownership.md)、
[事件水位元数据](../superpowers/specs/2026-09-14-event-frontier-allocation.md)。

## 新鲜线上基线

完整13对Metrics/Wallet成功、同进程1789374561008796272，UTC08:46:19.179757→08:52:19.186238。
capture SHA256 `b7c709da104eea9b4fa21df4c66f460a1d7e24fb16110eedca9114b2d389b1cb`。
Wallet session26.578块/秒、3533.14交易/秒、132.94交易/块。上一轮after约13.6块/秒、211.73交易/块；
速度变化发生于本轮新改动部署之前，不能记为本轮收益。stateCommit rolling13点中位18.881ms，
density_metadata_work中位1.288769秒，都是重叠采样而非独立逐块分布。

状态cold/pruned30,748,553→30,749,913，六分钟只推进1360块（3.78/s）；eligible lag519,690→527,793
增加8103块（1.56%），仍同数量级但增长加快。head-cold gap585,295→593,532口径不同。
body/index均30,736,384未变。本窗口GC retired9→9、coverage defers104、busy/errors及queue增量0。
非shadow核查错误无非零值；VM shadow+69（readiness65、unsupported3、result1），单列不当正式导入失败。
负obsolete gauge仍出现，最低−337,432,186B；不以引擎disk gauge代替独立du。

逻辑历史暂存408,465,909B，旧编码同输入估计1,903,883,080B；这是已有writer的编码观测，
不归于本轮ownership/metadata改动，不当作物理回收。

45秒CPU请求UTC08:47:24.308702→08:48:11.399294成功；profile内部08:47:24.880264起45.16秒，
183.36 CPU秒、进程年龄1083.9秒。profile SHA256
`7ef44f3a78062b27e1b3f8c1bcf6dc2cdccb749399679fbcbbc6a7298d7c2f0c`。
commitment pipeline44.08秒（24.04%），parent readDurable23.95秒（13.06%），
其中partition/prefetch Snapshot.Get分别11.00/9.83秒；嵌套路径不相加。
cache setFlushedLocked约1.03秒，cloneBaseReadCacheValue完整匹配路径约0.36秒，不能将整个pipeline份额
当作本次ownership修正的可消除量。shared reader整包19.61秒（10.69%），SHA16.64秒；
后台历史读取负载显著高于上轮短样本。Pebble compaction21.20秒，Go GC7.82秒。
CPU45秒与Metrics六分钟是独立窗口，不混用工作量分母。

UTC08:54:33.920264原生candidate check/healthy_mode通过，PID28510、ticks4510905452、
实际源码833e4a0b、process start同上。独立du：chaindata253,474,283,520B、
state-snapshots687,236,087,808B、ancient236,912,140,288B，可用1,802,550,235,136B。
与上一轮08:36:18相比hot+3.10GB，不能继续沿用“热库持平”的旧时点结论。
原生完整证据 `/tmp/gtron-commitment-prefetch-before-native.json`，SHA256
`86fe0393f359cb9a8c6770bc6a6e1ba01fab05dcd76ee9c7402dbf248b049fbe`。
UTC08:55:21历史余额canary返回既有黄金值0xb6283b0374b0e000。

## 验证、发布与部署后窗口

业务提交 `c329bf129bc08d891f45575dd1a1f8c7ec8da0e5`。Go1.25.5/GOMAXPROCS=2下，
blockbuffer、domains、state、snapshots、pruning、rawdb、freezer七包完整测试通过；
blockbuffer/domains/snapshots的commitment/prefetch/event/manifest定向race通过，
core的历史回退、restart unwind、AsOf、commitment定向测试通过，两生产包vet通过。
这不代表全仓或完整core通过。macOS race链接器LC_DYSYMTAB警告存在，测试退出码均0。

预取固定输入完整32键流程，28组合×5共140行。cold等长（不同内容）/缩短场景每操作
减少约32–33次分配；14组成对耗时区间全部重叠，无整体耗时提速证据。
读取次数一致；hot/grow/no_flush/direct_get控制保留。已热条目的后续刷新不能重复计算
第一次私有空间复用的收益。具体字节分配含Pebble后台噪声，不能套理论32KiB常数。

事件frontier固定输入16组合×5共80行。73,113 active / 96,630 retired目录frontier中位
7.083→0.952ms，32,996,768→1,032,192B/op、60→1分配；完整gap planner
16.360→9.273ms，仍有41,147,440B/op；stage write7.262→0.955ms。
该函数在前一轮profile仅约0.25% CPU，收益不等于整体同步吞吐增长。

原生Linux/amd64 Go1.25.5、Sapling验收通过：prepare中的rawdb/pebbledb完整测试及
历史兼容过滤测试，补充五个完整相关包测试，28×5预取基准及16×5事件水位基准均通过。
原生预取14对耗时范围全部重叠，cold等长/缩短场景每32键恰少32次分配，其他控制无
相应收益，额外实际读取/keys指标完全一致。原生大目录frontier22.870→2.367ms，
完整StageWrite22.126→1.910ms，gap planner50.628→30.644ms；分配口径与本地相同。
服务持续运行会影响基准墙钟，不把本地和原生耗时差异解释为代码差异。

原生补充完整summary：
`/tmp/gtron-commitment-prefetch-acceptance-20260914/20260914T091802779479Z-7442/native-summary.json`，
SHA256 `8bed980105df8a5e7344d806acf5292d802d070626dcf005801450ae9b556aa2`。
ops提交 `8f76ff8884301026cdf80cabe7b52f6199993111`，34项Python测试及Python3.6 AST通过。
GitHub推送后服务器fetch固定提交，原生git archive构建，未改服务器现有dirty checkout。
新release `/data/gtron/releases/20260914-commitment-prefetch`，binary SHA256
`5239f826a53c25d56fd301e9c17bb3a9eeda7e8af7cf418850d69ad207f4ffa6`，
upgrade-prepared SHA256 `ccdf989a515e8606257a4904c0d3ec27b8c07aeeb2f5c812e5cec3f244fd39c3`。
保持原端口、内存/缓存/预取并发、永久reader标记与回收/覆盖守卫。

激活UTC09:22:15.583224→09:23:00.400830退出0。UTC09:23:37.699686再次candidate check及
healthy_mode通过，PID8982、ticks4511224735、process-start1789377753834965736。
完整健康证据 `/tmp/gtron-commitment-prefetch-after-native.json`，SHA256
`cf784e42d0fa026625b2f5abe8e3e35817b5b95ab3f5691d735546405f919db9`。
UTC09:23:58历史余额canary黄金值通过。

切换前UTC09:22:14 chaindata257,099,579,392B、cold687,791,022,080B；
启动后UTC09:23:37 chaindata258,507,112,448B、cold687,865,778,176B；ancient两者均
236,912,140,288B。切换过程包含关闭flush和启动，不能当稳定净增长率。
完整after为UTC09:23:58.532436→09:29:58.516185，13对全部成功、同一新进程，capture
SHA256 `115abe4f487b3bf7e2e2e8cbdf77e76c3bd5f14a225511f018d5198290a03be3`。

|六分钟窗口|部署前|部署后|
|---|---:|---:|
|Wallet导入块/秒|26.578|13.690|
|交易/秒|3533.14|2939.30|
|交易/块|132.94|214.70|
|eligible状态积压首→末|519,690→527,793|556,414→560,917|
|状态冷覆盖推进|1360块|281块|
|状态裁剪推进|1360块|297块|
|stateCommit滚动中位|18.881ms/块|35.433ms/块|

块速率下降约48%，但交易密度增加约62%，交易速率下降约17%；历史输入、启动预热和后台
负载均不同，不能把这组before/after单独判为候选回归或提速。积压仍在56万块数量级，
after增加4503块（0.81%），增长斜率较before低，但冷覆盖仅0.78块/秒，不能称追赶改善。
body/index仍30,736,384；state cold30,752,765→30,753,046，prune末30,753,062，
16块差异为独立采样水位，不用作一致性失败证明。70项非shadow错误观测均0；VM shadow
+20（readiness18、unsupported1、result1），sender-chain另+1，仍需独立处理。
GC覆盖延后+97、busy+23、退休+4；排队尝试7、准入4、busy3、错误0，闭包工作550.5ms。

冷端31次成功forced build、28次event并行构建；采样batch中位13块，范围1–474块。
窗口最初保留的成功构建约65.9秒（density work65.4秒、metadata1.397秒），说明这轮
预热并非已经稳定。随后budget block_limit依次出现28、14、3、1、5、11、25、7、15、16，
多个poll是level1/duty20%，末尾恢复level2/duty80%；debt_growth进入+2、滞回+2、
recovery_samples+5，hard/device/new_stall进入均0。源码history_load.go立即缩小高密度
批次，每次成功完成后最多增长25%（1–3块允许有容量时+1）；该未改策略与本轮小批次及
缓慢恢复相符，尚不能把65秒的全部原因归于冷缓存或新代码。density metadata中位1.304秒
保持在同一水平，小批次下固定读写/验证/发布成本仍显著。

45秒after CPU内部UTC09:25:46.798874起45.17秒，进程年龄193.0秒、111.34 CPU秒；
SHA256 `55e94e80e1056b930d13a272c4dd990e2dc80f263d0c3e58f94c153db3097a3a`。
commitment31.93秒（28.68%）、durable parent read17.94秒（16.11%）、Pebble compaction
18.69秒（16.79%）、Go GC3.60秒（3.23%）。事件frontier0.55→0.03秒，cache刷新
1.03→0.79秒、clone路径0.36→0.22秒；这些短窗口的不同工作量不能证明具体线上节省量。
reader pack19.61→1.05秒、SHA16.64→0.88秒，after没有采中cold builder root CPU；
这主要提示后台组成变化，不能把总CPU183.36→111.34的下降全算作本轮收益。

最终UTC09:30:27.720604原生check/healthy通过，仍PID8982，头31,380,018→31,380,063；
最终历史余额canary UTC09:30:47再次通过。原生证据
`/tmp/gtron-commitment-prefetch-final-native.json` SHA256
`ebc02907b3a0b9f8cf27e7320cde2f16d0427a375d00373cb11f460336f8906b`。

|实际du字节|启动后09:23:37|最终09:30:27|差值|
|---|---:|---:|---:|
|chaindata|258,507,112,448|257,029,165,056|−1,477,947,392|
|state-snapshots|687,865,778,176|687,995,486,208|+129,708,032|
|ancient|236,912,140,288|236,912,140,288|0|

最终可用1,798,283,386,880B。与切换前09:22:14相比hot仅−70,414,336B；与08:54的基线
相比仍+3.55GB，时窗必须一起报告。after压实输入24,935,073,422B、输出21,033,383,540B，
debt4.673→4.296GB，write delay计数/时长均0；压实在持续工作。hot短时回落与压实进展
相容，不能把input-output差值、逻辑退休或本次内存优化直接当作物理释放量。

## 当前结论与后续优先级

两个窄范围优化的正确性与固定输入分配目标已达到，已部署；整体同步提速及chaindata
长期增速改善尚未证实，冷端产出仍不足。下一步优先以相同历史range分开测完整index
rebuild和cold history build，保留pack/chunk认证；before SHA16.64秒中index9.07秒、
builder7.57秒，不可全部归于一个可删除校验。再用实际load时间序列回放批次收缩/恢复和
滞回，量化小批次固定成本，避免直接扩大预算、跳过覆盖或关闭压实保护。

本轮after CPU的独立复核代理因配额限制未运行；此前生产实现、oracle、原生验收范围的
独立复核已完成，部署后CPU与完整Metrics解析由主代理执行并交叉核对原始数据。

## 证据范围

本地原始采样、分析器、测试和转录证据保留于build/benchmarks/20260914-commitment-read/。
最初分析完整Metrics前一次调用被13点完整性检查拒绝（只有7点），待收齐后完整重跑通过；
不使用那个未完成窗口。CPU身份先用前次原生证据及本轮精确process核对，再获08:54原生复核。
