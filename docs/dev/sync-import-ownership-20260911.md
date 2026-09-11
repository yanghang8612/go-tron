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

完整测试首次受沙箱无法访问 Go build cache 影响，失败原始日志保留；使用正常缓存权限重跑，不修改 Go 依赖或执行 `go mod tidy`。Go 1.25.5、CGO_ENABLED=0、GOMAXPROCS=2 的完整测试于 249.643 秒内通过，54 个有测试的包、13 个无测试包；两个生产文件哈希与验证开始时完全一致。GitHub 提交及生产发布身份随本次发布另行记录。

部署前完整六分钟基线为 13/13 metrics、13/13 Wallet，精确旧进程标识 `1789034123099447636`：41.63 块/秒、2,661.25 TPS，缓存 65,173→65,558 块，raw 字节增加 9,556,255（约 25.94 KiB/s）；每导入 1000 块，缓存净增约 25.71 块。状态归档距离增加 1,357 块但后半段下降 2,031；body/index 仍保持同数量级。部分指标点 pruned 比 published 稍前，是非原子观测，分别保留原值，不强行视作始终相等。

上线后的缓存数量不能直接和重启前相减来认定修复收益；应看重启后持续导入期间的增长、过滤命中和提交/恢复边界。新旧窗口必须各自使用进程身份、真实时长与 VM 工作量，不将当前不同高度吞吐差异直接归因于补丁。

本地证据目录为 `build/benchmarks/20260911-sync-import-ownership/`（基线、全仓/系统/lint/发布验证）、`build/benchmarks/20260911-sync-optimization/`（旧版失败与网络修复回归）、`build/benchmarks/20260911-commitment-path-layout/`（固定输入基准、源码 overlay、完整 fold 复查与缓存假设）。原始测量文件不纳入 Git。
