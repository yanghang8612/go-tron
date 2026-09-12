# 同步与历史回收优化：本地验证及待部署状态

本轮两项候选已通过最终本地测试：把符合原冷覆盖删除条件的连续 changeset 合并为粗范围，
以及让 posting 清理取得链锁后再占用共享维护 gate。尚未部署，也没有本轮线上后测。
当前原生 Chrome 停在 Mac 锁屏，无法重新核对服务器 PID/start ticks、unit 与二进制；
部署 helper 保持现场身份未冻结并拒绝 prepare。源码已提交 master：
`4940853af2424d04b86236e4ec682dcfc32e75e2`；此时尚未宣称已推送或部署。
ops 提交由部署命令的精确 `--script-revision` 参数指定，不在文档中形成自指哈希。

## 当前线上基线

前测为 2026-09-12 13:47–13:57 UTC（北京时间 21:47–21:57），21 个 metrics
和 21 个 Wallet 请求完整有效，进程标识为精确字符串 `1789188149990537883`。
身份比较不转浮点数。Wallet 和 metrics 使用各自请求中点、各自时间分母。

| 观察 | 本轮前测 |
|---|---:|
| Session 同步速度 / 交易速度 | 21.12 块/秒；2,260.17 tx/秒 |
| Session 交易密度 | 107.02 tx/块 |
| Wallet head 增长 | 12,672 块 / 600.031 秒，21.13 块/秒 |
| State 头距 | 329,189→327,850，缩小 1,339 块 |
| Body 头距 | 340,880→353,546，扩大 12,666 块 |
| 交易索引冷覆盖头距 | 381,840→361,738，缩小 20,102 块 |
| Pebble compaction debt | 观察范围 62.05–69.29 GiB |
| 十分钟 compaction 输入 / 输出增量 | 53.03 / 44.88 GiB |
| Posting 新增 chunk / 压力退让 | 0 / 600 |

自动压实仍在执行，posting 当前受既有 32 GiB debt 等压力门限约束。
本轮不提高门限；减少删除表达成本和无效 gate 占用并不能保证立即解除压力。
所选 canonical、storage、非 shadow 执行错误计数无新增，写 stall 计数无新增；
shadow 诊断单列，不能由所选指标推断所有路径无错。
state/body/交易索引冷覆盖头距互相重叠且分批推进，不能相加；本窗也不能统称积压没有扩大。
此表的交易索引来自 `chain/freezer/txindex/coverage`，不是下文 guard 所验证的
hash-bound `StageStateHistoryIndex`，也不是 posting-only sweep 的完成水位。

证据：[前测分析](../../build/benchmarks/20260912-reclaim-optimization/before-analysis.md)、
[完整 JSON](../../build/benchmarks/20260912-reclaim-optimization/before-analysis.json)。
这些文件位于 ignored build 证据目录，未随文档提交；不使用旧首轮十分钟数据替代本次基线。

后续 14:24:07 UTC 的独立状态复查仍是上述精确进程，Wallet currentBlock=28,957,993，
sync active=true、paused=false、fetchBackpressured=false，state/prune/errors 和
posting/errors 均为 0。这是旧版运行状态复查，不是候选上线后的验收。
三点六请求均成功；metrics 中点相隔 9.8264 秒，compaction 输入/输出又增加
907,475,188 / 803,701,609 B，说明压实仍进行，不表示释放了相等物理空间。
证据：[status-check 请求记录](../../build/benchmarks/20260912-reclaim-optimization/status-check/capture.jsonl)
与 [最终状态摘要](../../build/benchmarks/20260912-reclaim-optimization/final-status-summary.json)。

## 实现和安全边界

`GTRON_HISTORY_RANGE_PRUNE=1` 只允许 snap/history 模式开启。原 prune 决策先选出
已冷覆盖的块，rawdb 只合并实际存在、已索引、连续且键格式合法的 seq=0 行。
未选块、缺口、异常键、seq>0 切断范围；indexed value 延续旧快路径不解码。
精确半开端点排除最后 seq=0 的扩展键，不跨入其他前缀。

rawdb 原语最少 64、最多 1,024 块一段，64 MiB 是每段合作扫描预算。
线上每次 guard 最多 256 个 selected blocks，按 index→chain 顺序 TryLock，
重新核对选块前的 canonical proof、durable Finish、历史索引和 solidified 边界。
锁忙时保留原 point API 并记 fallback_blocks；proof 错误终止本 pass，不能降级掩盖。
只有新 range 路径获得此 guard 保证，不能将旧 point fallback 描述为新增保护路径。

审查发现 durable Finish 单独不足以排除已可见 batch 尚未返回或失败重试：
新 guard 还必须确认 Buffer committed/inflight 层都高于本片最高块，并拒绝
commit/flush 错误。该 pending-prefix fence 与 guard 已实现，定向、race 和最终
完整 core 测试已通过，独立只读签收无新增阻塞。scan 与全部 batch flush 在同一 guard 内完成；不长时间等待
异步队列清空。256 块和单段 64 MiB 都不是硬锁时延或总字节上限。

posting 改为非绑定 gate 预检→索引锁→公平链锁→取消检查→新鲜 engine/缓存 device
压力复核→非阻塞真实 lease→原 proof→扫描/写入。等待链锁不再占 gate；
压力阈值、1 秒节奏和原扫描/删除合作预算不变。获锁后不新增设备采样 I/O，
但单次 Pebble/区块读取仍可能等待。

新增 9 个 history 指标：`state/prune/history/delete/` 下的
`range/{enabled,runs,rows,logical_bytes,fallback_blocks,work/total_ns,work/max_ns}`
及 `point/{rows,logical_bytes}`。成功工作量仅在整个 pass 成功后发布，失败后可少报
已提交片段；work 只计受保护删除回调，不含 proof，不能称完整持锁时间。
原 point fallback 只记块数，point 行/字节指标仅覆盖新原语内部的 point 部分。
posting 新增 callbacks 和 7 阶段×3 个耗时 gauges；阶段重叠，不能相加。

## 已有验证与待完成项

真实 Pebble v1.1.5 A/B 使用合成 128 MiB history 和小 KV，从同一 checkpoint 派生
12 个独立数据库，未调用 Compact、format 始终为 1。删除相同 768 行时，粗范围
使用 3 条 range，delete batch 从 point 的 28,428 B 降到 234 B；两种布局各轮
均触发一次 delete-only。它仍扫描选中大 value，不代表免去扫描 I/O。

混合布局末态 SST 文件长度为粗范围 37.39 MiB、point 47.41–51.57 MiB；
纯 history 布局粗范围曾到 73.44 MiB，高于 point 的约 59.5 MiB。
这支持机制有效，但回收取决于 SST 布局和自动调度，不能承诺生产缩容比例或时间。
固定工作量并非等墙钟回收曲线；checkpoint 硬链接也使 SST 文件长度不能当全盘
释放空间。读结果、真实 snapshot 与 reopen 回归另有验证。
详见 [独立 A/B 摘要](../../build/benchmarks/20260912-coarse-range-prune/automatic-compaction-ab-summary.md)
及 [原始结果](../../build/benchmarks/20260912-coarse-range-prune/automatic-compaction-ab.json)。

第一轮 `core/... + cmd/gtron` 遇到沙箱禁止 bind；授权环境重跑中 snapshots
61.923 秒、cmd 32.357 秒通过。core 的既有
`TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock` 出现 candidates=0，
其实现未改，定向重跑 10 次在 0.887 秒通过。随后最终 `core/... + cmd/gtron`
完整重跑 exit 0：core 155.087 秒、blockbuffer 3.380 秒、rawdb 4.619 秒、
pebbledb 4.715 秒、pruning 15.922 秒、snapshots 76.166 秒、cmd 41.041 秒。
guard/prefix 定向和 race 已过；测试风格修正后 guard 定向复查 core 0.796 秒、
cmd 1.303 秒通过，pruning race 4.168 秒通过，增量 lint 为 0 issues。
记录：[第一次](../../build/benchmarks/20260912-reclaim-optimization/affected-tests.log)、
[授权重跑](../../build/benchmarks/20260912-reclaim-optimization/affected-network-tests.log)、
[定向重跑](../../build/benchmarks/20260912-reclaim-optimization/async-retry-recheck.log)、
[guard 验证记录](../../build/benchmarks/20260912-reclaim-optimization/history-range-guard-local-validation.json)
与 [guard 最终定向复查](../../build/benchmarks/20260912-reclaim-optimization/final-guard-targeted.log)。
最终记录：[完整测试](../../build/benchmarks/20260912-reclaim-optimization/final-affected-tests.log)、
[pruning race](../../build/benchmarks/20260912-reclaim-optimization/final-pruner-race.log)、
[增量 lint](../../build/benchmarks/20260912-reclaim-optimization/final-lint-new.log)。

新 helper 计划 release 为 `/data/gtron/releases/20260912-reclaim-optimization`。
保留旧三个开关、40 GiB、服务端口、guard/holds，只新增 history range 开关和替换 exe。
回滚恢复完整旧 unit 字节/元数据及公平锁二进制，旧健康不检查新版指标。
31 项候选新指标启动允许零工作量，不能将 enabled=1 当已清理。
source SHA 与 `d3793a24..4940853a` 全部 29 个非文档变更文件 SHA 已冻结，
其中 28 个 Go 文件和既有公平锁部署脚本；新 helper 自身由独立 ops 提交固定。
A/B 及独立安全审查满足本地启用条件，helper 的 HISTORY_RANGE_APPROVED=true；
该决定不保证生产提速或物理回收，前述 A/B 反例仍适用。
现场 PID/start ticks 仍为 0，prepare 因此继续拒绝。代码和文档可先推送，
部署仍需解锁后现场校验以及 Linux CGO/Sapling 原生 prepare。
离线审计见 [deployment-static-audit.json](../../build/benchmarks/20260912-reclaim-optimization/deployment/deployment-static-audit.json)。

上线后须重新完成 21 点、phase/退让、真实删行、canary、配置/日志/端口和 allocated du
验收。逻辑删除、异步且可能重叠的 keyspace SST 估算、snapshot pinned 累计量、
debt 与物理占用分别解释；本轮尚无物理回收或同步提速结论。
