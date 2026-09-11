# 待执行区块去重与 commitment 编解码优化（2026-09-11）

本轮解决下载缓存与导入器之间的 hash 去重空档，并缩短 commitment 路径叶节点的编解码循环。状态根、持久化编码、协议、签名校验、缓存预算、同步并发及归档策略均沿用原实现。

昨天已经完成并上线四轮：10:37 的状态元数据/奖励读取优化（`4c25c69b`）、15:59 的已发布 manifest 复用（`500b6127`）、16:42 的忙时资源准入（`0ab5b6d1`）、17:55 的五秒资源恢复观察（`6e5122bc`）。今天旧 ContractRuntime 与 IterateAccountKV 前台热点合计仅约 1.08% CPU。当前发现的是剩余开销与具体缓存缺口，不代表昨天没有实施优化；历史同高度约 42% 的整体版本收益也不能分摊给其中某一项。

## 已复现的下载缓存缺口

旧代码将整个 decoded prefix 从 raw buffer、hash 集合和 path reservation 中释放，再逐块执行。canonical head 尚未前进时，库存过滤、调度去重和迟到 body 准入都可能忘记这些区块。重复 body 可以再次入队，之后留在已经前进的 drain 游标后方。

新回归在原生产代码上实际失败：scheduler 和 inventory 都未识别正在导入的 hash，late requested body 的动作是 Stage，而非 Ignore。这个确定性复现证明代码存在缺口；部署前只采到了缓存总量，尚不能断言所有历史残留都来自同一原因。

修复增加 `importingHash`，只持有 hash 和高度，不保留 raw body 或 protobuf。锁内核验 decoded prefix 成功后，将去重所有权交给唯一串行 drain，直到 `InsertSession.Finish` 完成异步提交屏障才释放。三个入口使用同一所有权集合。

peer reset 可以发生在锁外执行期间，因此网络会话重置不会提前清掉 drain 所有权。若重连从 staged 表恢复了相同 raw body，结束时仅删除 hash 与当前 canonical `BlockIDByNumber` 都匹配的内存条目；失败、非 canonical 分支及同高度不同 hash 的恢复内容保留。清理不删除持久 staged 行，也不扫描整个 runahead buffer。正常所有权生命周期受现有 4096-applied-block 轮换约束，可超过该阈值一个配置的 import chunk。

计数说明：`sync/import/owned_blocks` 是当前待屏障完成的 hash 数；`duplicate_inventory`、`duplicate_fetch`、`duplicate_body` 是各入口的过滤观察次数，不能当作唯一重复块数或相加；`restored_duplicates` 记录屏障后移除的精确 canonical raw 恢复条目。旧版没有这些指标，部署前按缺失记录，不填零。

## Commitment 局部优化与收益边界

`encodingLayout` 通过现有 mask 一次计入定长 path leaf 大小，只遍历 legacy 变长 key；arena decoder 只扫描及复制 legacy leaf key。根、输出字节与 immutable arena 所有权不变，数据库读写次数也不变。

Go 1.25.5、darwin/arm64、GOMAXPROCS=2，固定相同输入、旧源码 overlay 对照，200ms × 3 中位数：

| Path leaf 数 | EncodeTo 耗时下降 | Arena decode 耗时下降 |
|---:|---:|---:|
| 2 | 14.4% | 20.7% |
| 4 | 21.3% | 20.5% |
| 16 | 21.6% | 14.1% |

各项零分配保持不变。这是相关小函数的收益，不是整体同步提升。完整 fold 首轮短基准的 batch=256 出现约 11% 回退疑点，原始结果保留；进一步协调停止其他重测试后，以 500ms 交替旧/新三轮复查，各 batch 中位变化为 −1.41% 至 +2.23%，分布重叠，没有证明整体 fold 提速。

更大的 depth 6/7 随机读问题尚未通过本补丁解决。额外指标分析发现 depth 7 的窗口晋升约 95.54%、tail 容量淘汰约 84.44% 只记录 flush 来源。这表示入驻后没有记到 resident 前台命中，不代表该 branch 从未被读取。写入信用是否把一次性 read-modify-write 分支误当作复用，需要物理 key 重用轨迹或固定状态回放验证；本轮没有据此修改缓存准入策略。

## 验证与发布记录

新回归覆盖真实迟到 `HandleRawBlock`、异步深度 2/4 的阻塞提交、失败释放、peer failover/reset 恢复、同高度不同 hash、真实重组、malformed prefix 原子性及 Stop。完整 `net/...` 和 race 均通过；domains 全包、相关 race 与 512 组独立 wire/输入毒化对照通过。

双节点 Sapling/CGO 系统测试为 79 passed、0 failed、0 skipped，临时节点已清理；不截断输出的增量 lint 相对 `2cdad61e` 为 0 issues，包含暂存的新 Go 测试文件。全仓存量 lint 问题不在本轮改动范围，增量结果不等于全仓零问题。

完整测试首次受沙箱无法访问 Go build cache 影响，失败原始日志保留；使用正常缓存权限重跑，不修改 Go 依赖或执行 `go mod tidy`。Go 1.25.5、CGO_ENABLED=0、GOMAXPROCS=2 的完整测试于 249.643 秒内通过，54 个有测试的包、13 个无测试包；两个生产文件哈希与验证开始时完全一致。

源码 `54c46b7fe1dd2b0521e50ab7dd1e4983e27c7cde` 与发布脚本提交 `8cca882dc9e27767ca3d1e520d46e403ef083d47` 已正常推送 GitHub master。服务器 Git 1.8.3.1 首次仅更新 FETCH_HEAD，发布前校验中止；随后使用不带强制标记的显式远端分支 refspec 拉取，通过原有版本校验。服务器保留原 checkout，在隔离 release 中用 Go 1.25.5 linux/amd64、CGO_ENABLED=1、tags=sapling 完成原生 affected/targeted 测试、Sapling 探针和构建。targeted 命令中的 cmd/gtron 没有匹配测试，不能计入额外覆盖。

北京时间 2026-09-11 10:04:01 激活检查通过：新 PID `2754`，精确进程启动标识 `1789092225326376992`，检查期间高度 `24,969,358→24,969,402`。release 为 `/data/gtron/releases/20260911-sync-import-ownership`，二进制 SHA-256 为 `0876e36b2053d7e0b8588b82fe3e3ee68e382548ffc5ef774782a216f443f990`。激活脚本校验仅执行文件路径变化，配置、资源预算、守护与 hold 状态保持一致；旧 release 保留作为回滚文件。监听确认 mainnet P2P 18890、Wallet 8090、RPC 8545、gRPC 50051、loopback pprof 6062/metrics 6071。发布身份与服务器检查采用终端结果转录，明确区别于本地保存的原始 HTTP 采样。

部署前完整六分钟基线为 13/13 metrics、13/13 Wallet，精确旧进程标识 `1789034123099447636`：41.63 块/秒、2,661.25 TPS，缓存 65,173→65,558 块，raw 字节增加 9,556,255（约 25.94 KiB/s）；每导入 1000 块，缓存净增约 25.71 块。状态归档距离增加 1,357 块但后半段下降 2,031；body/index 仍保持同数量级。部分指标点 pruned 比 published 稍前，是非原子观测，分别保留原值，不强行视作始终相等。

上线后的缓存数量不能直接和重启前相减来认定修复收益；应看重启后持续导入期间的增长、过滤命中和提交/恢复边界。新旧窗口必须各自使用进程身份、真实时长与 VM 工作量，不将当前不同高度吞吐差异直接归因于补丁。

## 部署后十分钟结果

北京时间 10:04:56—10:14:56 的 Wallet 窗口为 600.755 秒，21/21 Wallet 与 21/21 metrics 均有效，无请求失败，全部 metrics 使用同一精确新进程标识。按导入 session 差分处理 25,504 块、1,515,697 笔交易；metrics 末点高度 24,996,394，Wallet 末点高度 24,996,637，分别保留非原子采集值。

| 指标 | 部署前六分钟 | 部署后十分钟 |
|---|---:|---:|
| 同步块/秒 | 41.63 | 42.45 |
| TPS | 2,661.25 | 2,522.99 |
| 交易/块 | 63.93 | 59.43 |
| 同一进程内缓存块数 | 65,173→65,558 | 1,711→725 |
| 同一进程内 raw 字节变化速率 | +25.94 KiB/s | −43.82 KiB/s |
| 状态 published 距离链头变化 | +1,357 块 | −315 块 |

部署后 raw 从 38,790,028 降至 11,835,314 字节；后半段缓存 1,517→725 块，raw 端点斜率 −80.14 KiB/s、OLS 斜率 −91.45 KiB/s。这些是新进程内变化，没有把重启前后约 1.7 GB 的直接下降当作修复收益。21 个点中缓存 547–1,711 块，未观察到暂停或 fetch 背压。十分钟支持“该窗口未复现持续残留增长”，不能代替长时间同进程观察。

`duplicate_inventory` 窗口增量 1,453 次，后半段 838 次；fetch/body/restored 三项均为 0，分别保留，不相加。`owned_blocks` 在 224–4,064 之间随 session 周期回落，证实去重保护在真实流量中命中。它们不表示唯一重复块数，也不直接等于节省请求数。`signature_lookahead/mismatched` 全部 21 点均为 1、窗口增量为 0；它属于预取批次变化时重新解码的安全回退诊断，不能当作验签失败，首次事件的具体原因尚未证实。

完整窗口块速观测约 +1.99%，TPS 约 −5.20%，交易密度约 −7.04%。去重且完整落入窗口的 import 数据中，raw energy/VM tx 从 35,768 增至 43,721，commitment updates/apply block 从 363.72 增至 380.27，VM 占比从 71.58% 降至 69.11%。新进程启动年龄、不同高度、工作量与后台任务都不同，不能把块速变化归因于本补丁，也不宜从 TPS 下降判断代码回退。后半段块速 47.22 仍只是该工作量下的观测。

状态归档 published 距链头 329,094→328,779；body/index 距离 395,069→354,858，窗口减少 40,211 块。后半段状态距离增加 944 块，body 前沿一次批量推进后暂时不动，index 则继续追赶，不能将十分钟平均外推为稳定归档速度。总体仍在原来约 33 万/35 万块的数量级，符合本轮“积压保持同数量级即可接受”的判断；以上前沿距离互相重叠，不能相加。

已覆盖的 canonical guard 与 storage 错误类计数未见正增量。Shadow 诊断单列：vm_sender_chain/errors 增加 131（其中 readiness 98、apply_unsupported 32、result 1），sender_chain/errors 增加 8；它们不是 canonical 失败，仍不应概括为所有错误均为零。10:15:58 的服务器复核确认 PID、二进制 SHA、40 GiB MemoryLimit 和其他可读资源属性一致，磁盘守护正常；有界扫描覆盖启动至 10:15:40 共 121 条日志，ERROR/CRIT/FATAL/panic 为 0。

性能采样结束后，24,970,000、24,980,000、24,990,000 三个部署后区块与 TRONGrid 的区块 hash、父 hash、顺序交易 ID 全部一致（共 132 笔交易）。该抽查只覆盖这些区块，不是全链状态根验证。

本地证据目录为 `build/benchmarks/20260911-sync-import-ownership/`（基线、原始 after 采样、ownership-comparison、全仓/系统/lint/发布验证、canonical-after-trongrid）、`build/benchmarks/20260911-sync-optimization/`（旧版失败与网络修复回归）、`build/benchmarks/20260911-commitment-path-layout/`（固定输入基准、源码 overlay、完整 fold 复查与缓存假设）。原始测量文件不纳入 Git。
