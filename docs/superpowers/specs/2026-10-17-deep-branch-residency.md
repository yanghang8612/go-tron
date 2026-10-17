# 深层 commitment branch 准入驻留实验（拒绝候选）

2026-10-17，基于 `4967120062fb5b8762d9cfec0c7229b75a306d32`。本轮没有修改生产缓存策略、预算、key、编码、root 或共识；实验原型仅通过 Go source overlay 编译。证据位于忽略目录 `build/benchmarks/20261017-deep-branch/`。

## 问题与边界

冻结线上窗口的深 6/7 前台读取以 `no_resident` 为主，缓存预算已经是 base512 MiB + tail1024 MiB。线上 8572 random FD SST ReadAt/s、平均 4110 B/call 和约 1.396 ms/call 属于 VFS 累计口径，不能当作设备 IOPS、纯盘延迟或全 pipeline 时间。memcg 约31 GiB，主机有大量 page reclaim，不能以再加预算替代算法验证。

现有深层分支既有 first-read window、two-hit probation、flush 准入和有限 CLOCK 信用。probation 用 full physical key 的 FNV64 指纹直映到按 base 预算固定的 table；tail 扩容并不扩大该 table。一个潜在机会是同容量减少准入证据碰撞，但线上 `no_resident` 本身并不证明碰撞是主因。10/15 取消 flush 信用、10/16 Bloom/同 partition reuse 及重复偏移 flight 的负面结果不重复包装为本轮收益。

## 两个受控原型

`assoc2` 把每相邻两个槽作为一个二路组，先匹配两个指纹，再找空槽，全满时用分片 uint8 counter 交替选择替换槽。table 总槽数和 resident charge 预算不变，组数减半；这可能使普通流或长距离复用更早失去证据。`admitObservedString` 和 `forgetAdmissionString` 检查同一两个槽；flush 未观察到的 key 不安装指纹。其额外元数据仅实验分片 counter，未改变 live entry。

`avalanche` 保留原直映 table 和全部准入策略，仅在 full-key FNV64 后加64位终结混合，检查低位与 shard selector 的相关性。byte/string 两种指纹走相同终结变换。它增加整数运算，不增加 table 或 payload。

两个原型都没有触及 overlay 优先、完整 physical generation key、per-key epoch、global snapshot version fill guard、旧 snapshot 隔离、canonical flush 成功后才准入/refresh、rewind/drop/range delete/tombstone/missing/empty/error 或 borrowed/owned/arena 生命周期；没有额外 durable equality read，没有池化 live immutable arena，没有更改预取。

## 固定输入与结果口径

初筛与冻结结果分别保留。初筛默认工具链自动解析为 Go1.27.2；最终 `final-*` 全部显式固定本地 Go1.25.5、darwin/arm64、CGO_ENABLED=1。不能把它等同 Linux AMD64 的 Sapling native full-suite 验收，本轮未运行该验收。

每变体126个 admission replay phase：两个预算、三 seed、read scan/read-modify-write scan/short & long reuse/hot migration/mixed namespaces/paired reuse，共七场景和 cold_start/pressure/late_reuse 三阶段。输入是合法 depth6 legacy/depth7 generation key，也包括 other/trunk/depth5 prefetch；相同 operation SHA 和 returned-value SHA 逐项核对。mixed 场景还包含 tombstone、recreate、write-only metadata。该 replay 只计缓存后必需的 durable lookup，不把 synthetic map lookup 称 SST I/O。

真实 pipeline 使用现有 domains fixture：1536个聚簇合法键，每模式五次固定8块，按同一 durable seed 执行 ordered fold→root oracle→commit→canonical flush→close。base32 KiB + tail64 KiB 保持生产1:2比例。hot/cold_scan/scan/delayed_read 都会读后更新和写入；独立顺序 staged-memory oracle 逐块比 root。开始前将 seed Compact 成 SST，关闭并重新打开空 blockcache；seed、Compact/reopen、oracle 和 forced GC 不计入 pipeline 时间。计量包括 pipeline/cache 构造及 Close，真实 cursor.View 次数和返回字节、现有 VFS SST ReadAt calls/bytes，以及 runtime 总分配和 wall time。

所有 production Pebble cache source 都由 HEAD source overlay 冻结，避免并行缓存淘汰工作污染对照。本地小 seed 可完全留在 Pebble cache，64-partition constructor 只有一个活跃 partition，不能代表真实生产64-owner并行吞吐。flush map 次序会轻微改变 FIFO/CLOCK 结果，因此保留 min/median/max。

最终各项原始数值及差值以 `final-summary.json` 为准，全部命令、工具链、起止时间和 source/log SHA 在 `final-runs.json`。该文件同时保留阶段结果，避免 cold_start 稍好掩盖 pressure 或迟读回退。

## 最终 Go1.25.5 数值

每格为五次中位数 `durable cursor reads / pipeline ms`；范围、深6/7、durable bytes 和分配详见 JSON。每一种模式和变体都是36 SST ReadAt、127636 B，不能据此声称真实 SST 读取改善。

| 模式 | baseline | assoc2 | avalanche |
|---|---:|---:|---:|
| hot | 442 / 14.805 | 436 / 15.006 | 438 / 14.620 |
| cold_scan | 819 / 14.713 | 819 / 14.619 | 818 / 15.196 |
| scan | 1223 / 26.792 | 1229 / 26.443 | 1230 / 27.292 |
| delayed_read | 1436 / 28.057 | 1460 / 28.218 | 1443 / 27.592 |

assoc2 的 short read-modify-write pressure durable lookups 增加2.196%，paired late reuse 增加约0.387%。其总计略降不能消除此回退。avalanche 各场景总计约−0.333%到+0.011%，不形成收益证据。全部126 phase 的 operation/returned SHA 与 baseline一致；每变体20次pipeline、每次8个root都与顺序oracle一致。

## 决策

二路准入的 aggregate replay 下降主要来自 warmup；short reuse pressure 会回退，paired late reuse 也可能回退。真实 pipeline 不呈现稳定的 total durable reduction，SST ReadAt 完全相同。终结混合的 aggregate 变化很小，也未证明实际 SST ReadAt 或全 pipeline 收益。拒绝两个候选，不增加生产旋钮，不要求上线扩大样本来替代本地失败门槛。

本轮不建议把概率或局部 deep-read 变化当成整节点提速，也不以 cache charge 直接等同 Go heap/RSS。下一步如继续调查，应先获取真实跨块 key reuse distance 和按 key 的准入/淘汰因果证据；当前线上按深度的 `no_resident` 总量不足以判定同预算的更好准入策略。
