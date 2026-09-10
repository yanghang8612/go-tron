# 归档资源恢复观察：部署与线上复采（2026-09-10）

本轮为 busy-history 资源拒绝增加独立的五秒轻量观察。观察只刷新预算，
恢复后请求一次正常维护；完整归档仍重新检查水位、存储、空间、共享租约和
恢复截止时间。协议、状态格式、裁剪窗口、缓存预算和归档批量上限均未改变。

## 实现与验证

- 仅为满足原 soft/busy 水位条件、但被资源预算拒绝的机会挂起观察。
- 使用独立定时器；观察不执行 catalog preflight、历史扫描、manifest、
  pruning、freezer、密度更新或租约获取。完整维护与观察通过 passMu 串行化。
- generation 与 CAS 撤销旧机会，覆盖等待锁的并发完整 pass、preflight
  失败、外层完成失败及 Stop。已到期的普通 tick、retry、wake 合并为一次维护。
- 读取最新绝对恢复期限，保持完整外层工作成本和失败等待；更频繁采样既可
  更早确认恢复，也可更早识别债务增长压力，不保证更高吞吐。
- 新指标：`state/snapshot/cold/history/busy_observation/checks`、`wakeups`
  为 Counter；`last_duration` 为纳秒 Gauge。

本地 Go 1.25.5、GOMAXPROCS=2：全仓 54 个测试包通过，13 个包无测试；
core 154.977s、snapshots 59.087s、pruning 13.506s。首次定向 race 暴露
新增 pruning 测试夹具的共享变量竞态，随后仅修改测试，用 atomic/mutex
建立同步；生产文件哈希未变。修正后 pruning 整包 14.133s、定向 race
8.584s 均通过；snapshots 相关 race 已通过。最终未截断的增量 lint 为
0 issues（基线 `8a3c6d43`），不代表全仓存量 lint 问题为零。

测试包括同一五秒压力轨迹由五秒/一分钟消费者读取的恢复与压力对照、
重复及不可用采样、后来延长的期限、并发取消、timer 合并、真实 preflight
失败和维护回调计数。独立审查发现并修复了旧周期 tick 导致重复维护的边界。

## GitHub 与部署身份

按用户要求直接在 master 开发、正常推送；没有新建 PR、强制推送或修改
GitHub 分支保护。

| 项目 | 值 |
|---|---|
| 源码提交 | `6e5122bc976fb8d01cc653fae7a248f021423a92` |
| 部署脚本提交 | `0d9492e50e85ee4bc8d871c4a828a5c94690543e` |
| 发布目录 | `/data/gtron/releases/20260910-history-observation` |
| 二进制 SHA-256 | `54e4f4b29b05d9a3fbd2c721923e6e42d568987b8c29049177e7d3077715e31a` |
| 脚本 SHA-256 | `7cbab651fae6662ae9e3505ecfde74cfbf2fef4b38a4d3c51c995ec4c9a8ef0d` |
| 新进程 PID / start_ticks | `16961` / `4476861661` |
| 新进程精确 start unix nano | `1789034123099447636` |
| 旧进程 PID / start unix nano | `32233` / `1789029728814002906` |
| 服务 | `gtron.service` |

服务器从 GitHub 固定提交生成隔离 archive，保持原工作 checkout
`19eda11f44424f673a40b06051edcbf629b50846` 和已有 Rust 静态库。
原生 Go 1.25.5 Linux amd64、CGO_ENABLED=1、tags=sapling、最多两名 Go
worker；定向测试、完整 snapshots（35.339s）及 pruning（8.941s）、
Sapling availability/Pedersen 探针和构建全部通过。

09:52:46 UTC 准备完成，构建期间未重启服务。新进程于约 09:55:23 UTC
启动（北京时间 17:55:23），激活健康检查观察到高度
22,888,528 → 22,888,554。09:56:17 UTC 磁盘保护通过，/data 剩余
2,489,373,597,696 字节，根分区剩余 27,241,885,696 字节。
后续确认服务 active/running，MemoryLimit 仍为 40 GiB；全局维护 hold 保留、
主网磁盘停止 hold 不存在，磁盘保护 timer active，Nile 与自动部署服务/timer
均 inactive。18890 TCP/UDP、8090、8545、50051、6062、6071 监听仍由新进程持有。

## 采样口径

本地证据目录：`build/benchmarks/20260910-history-observation/`，原始采样
与分析产物未纳入 Git。每 15 秒并行获取 metrics 和 Wallet；使用各自请求
中点、单调时钟和精确进程身份，失败请求保留。指标不是原子快照，观察点
占比不能视作时间占比；head 与不同归档前沿的差额分别计算，不能相加。

最终部署前基线采用 `before-refresh/`，09:43:06.679747—09:48:06.682973 UTC，
21/21 组请求有效，299.987 秒。较早的 `before/` 与
`before-early-analysis.*` 同样保留：它因工作恢复间隔较长而被补采，未删除。
`warmup/` 单列启动观察，不并入稳定复采。

基线整体 head 38.615 块/秒，state published/pruned 60.153 块/秒，
状态差额减少 6,461 块；后半段从第 011 点开始，实际 135.179 秒，
head 35.605、state 27.734 块/秒，差额增加 1,064 块。末尾约 89 秒
state 未前进。因此全窗净下降不等于持续消化。

基线 accepted sequence 增加 16；最后接受样本 age 中位数 17.864 秒、
最大 56.815 秒。age 是预算消费者最后接受的样本时间，不是底层 probe 的
当前采样延迟。shadow sender +2、VM +47（readiness 33、unsupported 6、
result 8 为其子项），不能归为主链失败；已覆盖 strict/hash 指标无增长。

## 部署后结果

稳定窗口为 09:58:52.332627—10:08:51.690137 UTC（北京时间
17:58:52—18:08:51），599.368 秒，41/41 组请求全部成功，进程身份一致。

| 窗口 | 实际秒数 | Head 块/秒 | State 块/秒 | 状态差额变化（块） | 平均 CPU 核 |
|---|---:|---:|---:|---:|---:|
| 部署前全窗 | 299.987 | 38.615 | 60.153 | −6,461 | 3.556 |
| 部署前后半段 | 135.179 | 35.605 | 27.734 | +1,064 | 3.862 |
| 部署后全窗 | 599.368 | 34.740 | 111.110 | −45,774 | 3.792 |
| 部署后后半段 | 298.309 | 35.708 | 126.952 | −27,219 | 3.819 |

State published 与 pruned 在本次稳定窗口起止点一致。全窗 head 前进
20,822 块，state 前进 66,596 块；后半段仍净消化积压。body 与 index
各前进 65,536 块，index 在窗口末追平 body，这两项不是两份独立区块工作量。

观察机制方面，checks 增加 13、wakeups 增加 3；busy-resource 与
forced-busy 的 attempts/builds 均增加 46，accelerated builds 为 0。
46 次构建不能归因于 3 次唤醒。第 021→022、029→030 点区间各有
checks +3，而完整 pass/build 均无增加；随后各发生 wakeup +1，并在
同一观测区间看到 build +1，说明短观察没有变成每五秒一次完整维护。
完整 cold passes 本窗 +128，另有原有周期、追赶和恢复重试触发。

accepted sequence +80；最后接受样本 age 中位数从基线 17.864 秒降至
6.715 秒，最大 16.551 秒，41 点中有 3 点超过 15 秒；后半段没有超过
15 秒的点。`last_duration` 被观测到的值为 28.478—106.897 微秒，
中位数 62.386 微秒；这是重复读取的最近一次墙钟耗时，不能求出全部检查的
总 CPU 成本，也不是对每次检查耗时的上限保证。

仍存在发布平台：第 007→013 点约 88.819 秒，head 前进 3,219 块而
state 不变。其间 runtime-ready 全为 1、7 点中有 6 点 storage burst 为 1，
accepted sequence 从 34 增至 48。资源采样已经前进，后续应定位完整维护
阶段、租约和实际恢复期限，不能把所有停顿归因于观察过慢。

本次记录说明归档在该窗口追赶更快，但不能认定补丁令主链同步提速。
不同历史区块、维护任务及缓存阶段同时变化；TPS 从 3,038.124 变为
2,864.150，tx/block 从 78.017 变为 82.467，Head 块速反而较部署前低。
同区块输入对照才能进一步分离调度变更的因果收益。
完整落入窗口的独立 import 统计窗口为部署前 9 个、部署后 18 个；
raw energy/VM tx 中位数约 32,364 → 36,015，billed energy/tx 约
25,609 → 33,442，VM 毫秒/VM tx 约 0.183 → 0.215。这些工作量与阶段
耗时差异也不足以构成同输入的性能归因。

## 窗口末积压与正确性

| 前沿 | 高度 | 距 Head | 超过 65,536 保留窗口的差额 |
|---|---:|---:|---:|
| Head | 22,915,324 | — | — |
| State published / pruned | 22,539,800 | 375,524 | 309,988 |
| Body coverage | 22,478,848 | 436,476 | 370,940 |
| Index coverage / pruned | 22,478,848 | 436,476 | 370,940 |

因此积压尚未清空。state 与 body 相差 60,952 块，index 已追平 body；
各前沿差额分别保留，不相加。Wallet 末点 remaining 为 63,205,525 块，
peer/sync-peer 为 23/8，buffered blocks 为 2,758；41 点中没有暂停或
fetch backpressure 的观察。
后半段 body 完成前沿未增长，其距 Head 增加 10,652 块；index 前沿
前进 40,960 块。状态净消化不能概括为所有前沿都在逐点追赶。

已覆盖 strict/hash rejection、诊断 logged/suppressed、cold/storage 错误
均无新增。shadow sender +13、VM +86，后者子项为 readiness 64、unsupported
5、result 17；它们属于 shadow 统计，不能累计成 canonical 失败数。

部署前 09:23:06 UTC、部署后 10:09:55 UTC 的历史 canary 均通过：
地址 `0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1` 在 2043/2044/2045
三个高度的固定 hash/余额保持一致，每次 12 个只读请求。此检查覆盖一个
账户的标量余额，不等于全历史域验证。

10:09:44 UTC 新版本导入区块抽检通过：22,893,702、22,894,102、
22,894,402，分别 65/76/58 笔交易；Go RPC 与 TRONGrid 的 block ID、
parent hash、按顺序排列的 tx ID 全部一致。三个高度均高于激活健康检查
最后高度。此次使用 Go 的简化区块 RPC，参考为公共
`https://api.trongrid.io/wallet/getblockbynum`；不推断所有区块或状态根均已校验。

## 观测链路与后续方向

metrics 远程请求中位耗时从基线约 3.336 秒上升至 7.087 秒。
10:09:58 UTC 在服务器本机做两个端点各两次对照：直连
`127.0.0.1:6062/debug/metrics` 为 6/7 毫秒，本机 Nginx
`127.0.0.1:6060/debug/metrics` 为 10/7 毫秒，全部 HTTP 200。
这支持优先排查远程访问/SOCKS5 链路；少量窗口外对照不能排除采样期间的
瞬时服务抖动。JSON 调试指标由 6062 提供，6071 是独立 Prometheus 服务。

下一步优先补齐共享租约持有者、实际 history recovery deadline 与完整
维护阶段耗时的关联，解释资源已就绪时仍出现的约 89 秒发布平台；随后
依据阶段成本优化 state 到 body/index 的归档衔接。同步吞吐应使用同一组
区块回放与 VM 工作量归一化复测，避免将输入差异当作优化收益。远程采样
链路可独立诊断，保持节点运行参数不因观测延迟而变化。

10:11:22 UTC 最后复核仍为 PID 16961、相同二进制哈希，磁盘保护通过：
/data 剩余 2,488,821,886,976 字节，根分区剩余 27,224,461,312 字节。
服务器 checkout 与原有 `third_party/librustzcash`、`solc/` 状态保留。
本轮复采已结束，没有创建持续监控自动任务。

主要证据：`before-analysis.*`、`after-analysis.*`、`independent-review.*`、
`validation/summary.json`、`deployment-{prepared,activated,final}.json`、
`metrics-loopback-comparison.json`、`canary-{before,after}.log` 及
`canonical-after-trongrid/summary.json`，均位于上述本地证据目录。
