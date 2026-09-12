# 热历史删除与维护准入优化

## 问题和范围

当前基线为 2026-09-12 13:47–13:57 UTC（北京时间 21:47–21:57）的 21 点采样，
精确进程标识为 `1789188149990537883`，不经浮点转换比较。session 处理约
21.12 块/秒，state 头距 329,189→327,850（−1,339）；body 头距为
340,880→353,546（+12,666），交易索引冷覆盖头距为 381,840→361,738（−20,102）。
三者重叠，不能相加或统称未增长。
交易索引冷覆盖来自 `chain/freezer/txindex/coverage`，不是 guard 要求的
`StageStateHistoryIndex` 或 posting-only sweep 水位。
Pebble debt 观察范围 62.05–69.29 GiB，十分钟压实输入/输出增量为
53.03/44.88 GiB；posting 新增 600 次压力退让、没有新增 chunk。
这说明自动压实仍工作，posting 本窗受原压力门限约束；debt 不是可回收空间。
此前公平链锁首轮曾真实删除 132 片、489,679 行、32.14 MiB 逻辑数据，
是另一时间窗，不能代替本轮前测。证据见
[前测分析](../../../build/benchmarks/20260912-reclaim-optimization/before-analysis.md)
及其 [JSON](../../../build/benchmarks/20260912-reclaim-optimization/before-analysis.json)。
本轮改进已获许可数据的删除表达和维护任务持有共享 gate 的时段，
不扩大可删除历史范围，不提高 posting 的压力门槛或工作预算。

基线是 `d3793a24f6a2f339c8d23dc2e9eb0bc6f2c2ac31`。
实现源码已提交 master：`4940853af2424d04b86236e4ec682dcfc32e75e2`；
最终本地测试和独立安全审查通过，31 项新指标 contract 与 29 个非文档变更文件清单已冻结。
两项候选分别验证；history range 候选只有通过真实 Pebble A/B 和安全测试，
才允许在部署配置中设置 `GTRON_HISTORY_RANGE_PRUNE=1`。
本地启用条件已满足，helper 已标记批准；它不替代现场身份校验或原生构建。
不改变共识、查询结果、冷文件、数据库键格式、Pebble v1.1.5 依赖或 format major，
不调用线上强制 `Compact`，不修改 archive/full 模式的保留政策。

## Pebble 回收语义

自动压实目前开启。数据库打开、memtable flush 完成、压实完成和删除统计载入后，
引擎会重新尝试选择工作；最老快照关闭也会尝试底层回收。没有固定 TTL，
也没有周期性保证全库墓碑在某时间内清零的承诺。

现有配置为 memtable 256 MiB、动态 LBase 1 GiB、L0 参数 8、停写门槛 64、
L0–L5 无压缩而 L6 使用 Snappy。并发上限是 `ceil(GOMAXPROCS/2)`，
额外任务受 L0 深度和 2 GiB debt 信号约束。L0 参数 8 不是“必须等 8 层”的硬条件：
v1.1.5 的初始分数是 `max(2*深度/8, 非压实文件数/500)`，还会考虑相邻层和输入冲突。
本轮保留这些配置。

普通 point delete 先进入 WAL/memtable，flush 后的删除估算会增加压实优先级；
压实时才有机会丢弃旧值。删除标记本身还要满足更深层无该键和快照可见性条件。
正常压实之外，L6 elision-only 是低优先级选择，要求有效统计、删除条目超过 10%
或 range deletion 的回收估算达到文件 10%，且候选文件早于最老快照。
这个比例只限制单独重写 L6，不限制普通层间压实顺便回收。

range tombstone 可以让完全覆盖且序列号、快照条件满足的 SST 走 delete-only
路径，也可以减少点墓碑编码和处理工作；它不保证无需重写所有重叠 SST。
当前 `TargetByteDeletionRate=0` 表示没有文件删除限速，
`FlushDelayDeleteRange=0` 表示不因 range delete 额外设置定时强制 flush。
旧 SST 仍需退出当前版本并释放所有读者引用，才由后台删除。
因此逻辑删除字节、SST 估算、compaction debt 和 `du` allocated 必须分别报告。

依据：`core/rawdb/pebbledb/pebble.go`；本地 Pebble v1.1.5 的
`compaction_picker.go`、`compaction_iter.go`、`table_stats.go`、
`compaction.go`、`snapshot.go`、`read_state.go`、`cleaner.go`。

## 候选一：连续已覆盖 changeset 的 range delete

`GTRON_HISTORY_RANGE_PRUNE=1` 是显式开启开关；未开启保持现有 point delete。
仅将原 hot-history prune 流程已经决定删除、且已验证冷覆盖的块组合，
原保留窗口、coverage 内容验证、停止条件、索引前置条件和进度发布顺序保持不变。
开关本身不提供删除许可，manifest 水位也不能替代原 coverage 决策。

每一个 range run 必须满足：

- 块号严格连续，实际存在相应 changeset，且全部经过原决策选择删除。
- 每块已完成所需历史索引，changeset 键合法，属于可证明安全的 seq=0 表达；
  这里的 seq 是 changeset 键中的块内序号，不是 Pebble MVCC sequence number。
- 至少 64 块，最多 1,024 块。扫描字节合作预算为 64 MiB；按完整记录检查，
  单条记录、一次迭代器或存储调用可能越过预算，不能宣传硬时延或内存上限。
- 起止键只来自 rawdb schema 的 changeset 边界函数，范围为半开区间；
  不覆盖 tx-range 元数据、posting 帧、目录、latest state、commitment 或其他前缀。

未选中的块、块号缺口、缺失 changeset、异常键、seq>0、索引条件不满足，
均打断 run；未满足 range 条件的部分走原 point 路径。
已索引快路径沿用原实现不解码 value 的语义；需要解码的旧路径仍保留其错误，
不宣称 range 扫描新增了完整值编码校验。fallback 不能静默吞掉迭代器或读取错误。
范围不能跨越一个未知或被保留的块，即使前后两个端点均可删除。
尾部不足 64 块走 point 路径；预算切片不能把未验证区间纳入 range。

线上 Worker 在原 coverage 选择之后，每次最多取 256 个 selected blocks，
按 index→chain 顺序非阻塞尝试两把锁，重新验证先于选块捕获的 canonical
proofHead/hash、durable Finish、历史索引和 solidified 范围。
只有锁竞争才可调用原 `DeleteStateDomainChangeBlocks` point 路径，并记
`fallback_blocks`；该路径保留原行为，不获得新 guard 的安全保证，也不新增 value 读取。
proof 缺失、变更或错误必须终止本 pass，不能伪装成 busy 后降级。

guard 还必须拒绝 commit/flush 错误，并确认 Buffer 的 committed/inflight 层均高于
本片最高块。Finish 可在一次异步 batch 已可见、但对应层尚未退出或失败待重试时出现；
因此 Finish 本身不足以替代 pending-prefix fence。scan 和所有 batch flush 必须在
同一个 guard 内完成，不等待整个异步队列排空。此 fence 与 guard 已实现，定向、race
及最终 `core/... + cmd/gtron` 验证已通过。离线 restore/repair 必须继续在停机路径进行。

256 是线上一次 guard 的块数限制；rawdb 原语仍支持 1,024 块 run 和 64 MiB
每 run 合作预算。一个 guard 可有多个 run，64 MiB 不是其总扫描字节或锁时延上限。
每片之间检查取消；单次 iterator、value 读取或 batch.Write 无硬中断保证。

范围删除必须落入原 bounded batch/flush 语义。仅在相应写入成功后累计成功删除，
所有删除提交成功后才按既有流程发布 hot-prune manifest/stage 进度和 posting 许可。
部分提交后失败可以幂等重试；错误、取消或重启不能使水位跨过未处理数据。
冷读、热读和历史索引查询在优化前后保持一致。

新 history 指标共 9 项，以 `state/prune/history/delete/` 为前缀：
`range/{enabled,runs,rows,logical_bytes,fallback_blocks,work/total_ns,work/max_ns}`
以及 `point/{rows,logical_bytes}`。enabled 表示配置；其余成功工作量在整个
Worker pass 成功后发布，部分批次已提交而后续失败不会计入，因此可能少报真实工作量。
work 仅计受保护删除回调（含 scan/flush），不含 core proof，也不是完整 chain-held 时间。
point 行/字节仅计新 rawdb 原语内部的 point 部分；锁忙原路径单列 fallback_blocks，
不能把两类行数相加当作全量删除覆盖率，块数也不等同于物理行数。

上线前必须用相同真实 Pebble 输入比较 point 和 range 两路，包括选中/保留数据、
提交工作量、扫描成本、自然 flush/压实后的空间与读结果。
若借助测试中的显式 Compact 验证最终语义，需单列为受控测试；不能将它的空间结果
冒充线上自然回收或在部署时执行。A/B 不得只比较发出 tombstone 的耗时。

## 候选二：posting 在取得链锁后持有真实 gate lease

公平链锁版会先取得共享 gate，再等待链锁；等待本身可超过 250 ms，
因此触发 15 秒维护冷却。本轮把非绑定预检和真正占用分开。

流程为：检查原许可与压力 → 共享 gate 非绑定预检 → 尝试索引锁 → 公平等待链锁
→ 立即检查取消/生命周期 → 重新检查新鲜 engine 和缓存 device 压力
→ 尝试取得真实 gate lease → 校验原 canonical/Finish/index/H 证明 → 扫描及写入。
非绑定预检只提示“现在可尝试”，不持有 lease、不预留未来时段，也不保证后续取得成功。
等待期间其他维护任务可以获得 gate；之后真实准入失败必须安全退让，不推进 cursor。

保留索引锁在前、链锁在后的顺序。取得真实 lease 必须使用非阻塞准入，
不能在持有链锁时等待另一个持有 gate 的任务释放它，避免制造锁环。
所有退出分支释放自己取得的资源；取消等待者获链锁后不扫描、不写入，Stop 仍等待退出。
不在链锁内新增设备采样 I/O：复用有时间戳的 device 缓存，engine 状态重新读取；
任一必需信号不可用、过期或超限即退让。

压力条件保持原值：engine/device 不超过 15 秒、无 hard limit、L0<8、debt<32 GiB、
device await≤5 ms、queue≤32,000 milli。1 秒工作间隔、4,096 行、1 MiB 扫描、
256 KiB 删除、10 ms 合作扫描预算不变；真实 lease 仍遵守共享 gate 既有冷却规则。
权限固定、重组证明、混合帧保留、目录不删、内存 cursor 和原整轮水位语义不变。

phase timing 至少能区分链锁等待、实际 gate lease、证明校验、rawdb 扫描/提交和总调用；
若某阶段合并计量需明确边界，不能将总回调耗时称为锁持有耗时。
冻结新增 `state/prune/posting/callbacks` 以及
`state/prune/posting/phase/{chain_wait,chain_held,admission,proof,gate_held,scan,write}/{last_ns,max_ns,total_ns}`
共 21 个 phase gauges。阶段可能重叠，不相加；last 不构成全部调用耗时分布。
压力/准入退让与 errors 分开，计数不跨进程累加。

## 验证、发布与验收

安全测试覆盖 range/point 等价、缺口及保留块、seq>0、无索引、malformed、
64/1,024 块边界、64 MiB 合作预算、范围上界溢出、批提交失败/取消/恢复；
posting 覆盖非绑定预检竞态、等待时 gate 可被别人取得、获锁后压力恶化、
真实准入失败、重组、停止、旧许可和资源释放。运行对应 race 与原生 Sapling 检查。

通过独立审阅和 A/B 后直接在 master 提交源码，再冻结独立部署 helper/ops commit，
经 GitHub 推送，由服务器隔离 release 源码原生构建。新 release 计划为
`/data/gtron/releases/20260912-reclaim-optimization`；旧公平锁二进制、PID/start ticks、
源码、完整 unit 配置需部署前现场复核。源码 SHA、文件 SHA 清单和新指标 contract
未冻结时 helper 必须拒绝 prepare。
本地原生 Chrome 当前停在 Mac 锁屏，尚不能现场复核服务器身份或执行部署；
helper 的 OLD_PID/OLD_START_TICKS 保持 0；源码 SHA/清单已按上述提交冻结，
实际 ops 提交通过部署命令的精确 `--script-revision` 参数传入。
可以先提交、推送已验证代码及文档；这不代表 prepare、activate 或线上后测已经执行。

既有三个开关保持 1；新 range 候选获准上线后只在 `[Service]` 中增加
`GTRON_HISTORY_RANGE_PRUNE=1`，并替换 ExecStart 的可执行文件路径。
保留 40 GiB、其他参数、其他服务、空间 guard 与全部 hold。
回滚恢复旧 unit 的完整字节及元数据，回到公平锁二进制，使用旧三个开关和旧指标验收；
不能让旧版没有的新指标阻断回滚。Pebble obsolete 的已知负值不作非负启动要求。
候选启动验收冻结上述 9 个 history 指标及 22 个 posting 新指标；range/enabled=1，
其余允许零，沿用 posting/errors=0。启动不要求实际删行或完成整轮，需后测单独证明。

前后各采 21 点，按 process/owner 固定窗口核对：清理工作量、phase 与退让、
history range 实际启用与 fallback、errors/stalls、同步速度与交易密度、
state/body/index 三类积压，以及 engine/SST/WAL/debt、异步 keyspace 估算。
另做同路径 allocated `du` 和 canonical canary；启动健康不等于已经取得冷覆盖许可
或完成清理。积压需同时报块数与变化，不把同数量级描述成绝对未增长；
不同高度、重启预热和交易密度限制吞吐因果比较。
