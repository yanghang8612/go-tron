# 冷历史积压与恢复期观测优化（2026-09-14）

本轮恢复期压力观测修复已部署并验证生效，但尚未控制住积压。完整 after 窗口仍增加 5,986 块；不能将本轮称为积压问题已解决。

## 部署前基线

线上业务源 `c329bf129bc08d891f45575dd1a1f8c7ec8da0e5`，PID 8982，
process/start `1789377753834965736`。本地原始证据位于
`build/benchmarks/20260914-cold-backlog/`。

完整窗口 UTC 09:57:36.419855–10:03:36.426484（北京时间 17:57–18:03），
13 × 30 秒，所有请求成功、同一进程。

| 指标 | 部署前结果 |
|---|---:|
| 导入 | 6,592 块 / 1,064,453 笔交易 |
| 导入速度 / 交易密度 | 18.312 块/秒；161.48 笔/块 |
| 已符合迁移条件的状态历史积压 | 590,332 → 596,103 块，+5,771 |
| 状态冷覆盖高度 | 30,756,190 → 30,757,152，+962（2.672 块/秒） |
| 热状态裁剪高度 | 30,756,222 → 30,757,152，+930 |
| 区块体/索引冷覆盖 | 30,736,384，窗口内未动 |
| 网络待追高度 | 54,823,649 → 54,817,204，减少 6,445 |
| 下载缓冲 | 394 → 1,810 块；19.1 → 83.4 MB |

各指标独立采样；初始裁剪和冷覆盖的 32 块差异属于非原子观测，不能据此认定
裁剪越界。冷历史积压与网络下载缓冲/距离链头的差距不是同一指标。窗口中
45 次 forced 构建成功、45 次事件并行、93 次维护 pass；批量中位数 25 块，
10–33 块。完整维护中位数 5.724 秒，密度工作 3.903 秒，密度元数据 1.263 秒。

70 个非 shadow 错误观测均为零。shadow VM readiness 新增 46、apply_unsupported
新增 1、result 新增 1，另列而不掩盖；它们不等于正式导入失败。
GC coverage defer +176，queue/retired 未增长，GC errors 为零，busy 无新增。

45 秒 CPU 剖析 UTC 09:59:29.855 起，45.14 秒墙钟 / 154.31 CPU 秒：
commitment 42.71 CPU 秒（27.68%）；共享历史 pack 读取 14.63 秒（9.48%），
其中 SHA 校验 12.03 秒。共享读取分配链仅 0.45 秒，不支持优先投入缓冲池改造。
这些调用栈包含嵌套关系，不能相加；45 秒 CPU 不能除以 6 分钟的块数。

设备 busy 中位数 91.95%，queue 6.642，await 0.810 毫秒。高 busy 本身不能
证明磁盘饱和或有空闲吞吐；SST VFS 读取字节可能命中页缓存。引擎 obsolete
估算有负值，数据库大小以原生 du 为准。

UTC 09:59:10 原生健康检查 PASS，head 31,413,804 → 31,413,852：

| 路径/指标 | 原生字节数 |
|---|---:|
| chaindata | 261,363,671,040 |
| state-snapshots | 689,267,003,392 |
| ancient | 236,912,140,288 |
| 可用空间 | 1,792,241,119,232 |

与上轮 UTC 09:30:27 结尾相比，约 29 分钟内 hot +4,334,505,984 字节，cold
+1,271,517,184 字节；这个区间与上述 6 分钟速率窗口不同，不能混算。
历史余额 canary 高度 30,514,176，返回 `0xb6283b0374b0e000`，UTC 10:07:06 PASS。

## 本轮修改边界

详见 [设计说明](../superpowers/specs/2026-09-14-cold-recovery-observation.md)。
完成强制构建后，旧代码在恢复等待期间没有独立压力观测。恢复需要多个间隔
至少 5 秒的好样本，而长恢复会让这些样本跟着延后。新模式复用既有 timer，
通过缓存设备状态和当前引擎元数据更新压力控制器，始终不唤醒完整维护。
恢复期限、工作计费、所有限额和写入保护保持原来的语义。

73 × 5 秒补充观测 UTC 10:00:38–10:06:38：控制器 accepted sequence +41，
可见样本间隔中位数 7.119 秒，最大 24.144 秒；level 1/2/3 各 16/51/6 个采样点。
HTTP budget 字段是控制器最近观测，并非即时完整 StoragePressure，不能以此
伪造缺失的 memtable/stall/device 组合或宣称精确反事实回放。

## 证据身份

- 完整 before capture SHA256：`da845d405c4ac6905d73efb7acbbd252df305a55a9767edd3ac522fcd7959d7d`
- 5 秒 trace capture SHA256：`dc897b5061b0ffbe54eb2925d7aad959c87dd488f0b18741ed60b689638838f2`
- CPU profile SHA256：`f88488af8287a77c304150970e5c0c704bd6c769505378b1def580161d502c58`
- 原生 before 文件：`/tmp/gtron-cold-backlog-before-native-20260914.json`
- 原生 before SHA256：`1615bcd240c1a34776406cbf653815557b8d018c81fd4548ed872613319cc92f`

## 本地验证记录

Go 1.25.5、GOMAXPROCS=2、专用 GOCACHE：

- `go test ./core/state/snapshots ./core/state/pruning ./core/maintenance -count=1 -timeout=300s` 首轮 PASS，76.517 秒；三个包分别 74.834/34.023/1.020 秒。
- `go test ./cmd/gtron -run 'Test.*History' -count=1 -timeout=300s` PASS，13.525 秒。
- `go vet` 上述三个库包和 cmd/gtron，PASS。
- 新增 8 个 snapshots、2 个 lifecycle 测试，覆盖固定生产者序列、原始期限、不唤醒维护、失效样本、取消、外层完成以及下一次正常准入重查压力。

初轮 focused race 暴露测试夹具缺陷：共享 zstd codec 首次初始化发生在某个
synctest bubble 内，后续测试 bubble 复用 channel 引发 fatal；不是数据 race
报告。修正测试在 bubble 外通过真实单块构建初始化共享 codec，新旧真实
lifecycle 测试均执行该准备过程，避免依赖顺序或 GC。修正后的复验结果见下文，均已完成。

最终复验：snapshots 完整包 70.265 秒 PASS；pruning 完整包 22.177 秒 PASS；
最终 vet PASS。测试初始化还对 snapshots 包使用 TestMain，在所有 bubble 前完成
codec 的非空编解码，并遵守既有 DecodeAllCapLimit 的目标容量要求。
Focused race 三包 `-count=2 -shuffle=20260914` 全 PASS：snapshots 6.382 秒、
pruning 3.637 秒、cmd 1.868 秒。中间失败和修正记录均保留在 recovery/validation.md。

业务代码提交 `15d78319702d729f2ea320e68954f6d3629bf25e`；测试隔离补丁后最终
构建源 `7b2b374ae0d66d19971b72269f3fc14aeb1a8e53`。
运维提交 `aaa8967ad6d93bad84b45765a046d2886c3506e5`；36 个 Python 测试及
Python 3.6 语法检查 PASS，原生验收显式核对 13 项关键测试实际 PASS。
部署脚本 SHA256 `115ad3b5c7a52b9700e0338c9a452074485cc28e4b92f1b81d8ef559d798a42c`；
验收脚本 SHA256 `37d038d27c770831a74104c50a0f1eade0faea2376418047cab833c31edb0259`。
本轮没有性能微基准或 importer 自动限速改动。

## 原生部署

直接推送 master，origin 确认 `aaa8967ad6d93bad84b45765a046d2886c3506e5`。
服务器通过固定 Git 对象导出构建，保留原脏 checkout 和 Rust 子模块。
原生 prepare UTC 10:25:34–10:27:51，137.466 秒、rc=0；rawdb 完整包
13.812 秒、pebbledb 1.627 秒，历史兼容过滤的 blockbuffer/pruning/snapshots/core
分别 2.310/6.297/11.147/34.270 秒，全部 PASS；native Sapling=true。

原生额外验收 UTC 10:28:39–10:29:29，49.948 秒、rc=0、complete=true，
Go 1.25.5 linux/amd64。三库包完整测试和 cmd History 过滤测试，13 项关键测试
均 PASS。snapshots 1376、pruning 302、maintenance 51、cmd 154 个 PASS 事件
包含子测试；两项需显式环境输入的非关键测试按预期 skip，关键测试无 skip。

- release：`/data/gtron/releases/20260914-cold-recovery`
- binary SHA256：`409b08c1e916cc9953a5f66e91ecae897e265c43471227af2bf1b208b566d533`
- upgrade-prepared SHA256：`d48b20ced22d5040a094ce2cc4cf2d16eec03a947f8e0403e537b2ff35ad1966`
- 原生额外验收：`/tmp/gtron-cold-recovery-acceptance-20260914/20260914T102839267308Z-23968/native-summary.json`
- 验收 SHA256：`704b6960e63739bc503c65538c88431823558a8d2d94a04a8a5d3823b9e4b877`

activate UTC 10:30:45–10:31:24，38.767 秒、rc=0、active=true。
新 PID 24857，ticks 4511636088，process/start `1789381867367216192`。
UTC 10:32:11 再次 candidate check/healthy_mode PASS，head 31,447,232 → 31,447,257。
此时 hot 265,242,394,624、cold 690,300,801,024、ancient 236,912,140,288 字节；
可用空间 1,787,242,065,920 字节。原生记录
`/tmp/gtron-cold-backlog-after-native-20260914.json`，SHA256
`78de68ac95b32711f5d6546f4595ff1c2dfd10b290df94e3abba6fe76878ab7d`。

部署后历史余额 canary UTC 10:32:24 PASS，仍为 `0xb6283b0374b0e000`。
先单列 5 × 30 秒启动阶段，UTC 10:32:24–10:34:24；恢复观测 +8 次、接受 +8 个，
采样中的 last_duration 最大 0.038979 毫秒。这是最后值 gauge，不能作为真实
调用延迟的分位数或硬延迟保证。完整 after 窗口从 UTC 10:34:28 开始，结果如下。

## 完整 after 与实际效果

UTC 10:34:28.387645–10:40:28.393385（北京时间 18:34–18:40），13 × 30 秒，
全部请求成功、同一新进程。SHA256
`bc96d4a333e380893e754dc135c1dc317401aa4722d2e95a35fca0c83c66d84e`。

| 指标 | before | after |
|---|---:|---:|
| 导入块/秒 | 18.312 | 19.023 |
| 交易/秒 | 2,957.01 | 2,823.90 |
| 交易/块 | 161.48 | 148.45 |
| 冷历史覆盖推进/6 分钟 | 962 块 | 713 块 |
| 冷迁移块/秒 | 2.672 | 1.981 |
| eligible 历史积压增加/6 分钟 | 5,771 块 | 5,986 块 |
| 完整维护耗时采样中位数 | 5.724 秒 | 5.057 秒 |
| 密度元数据耗时采样中位数 | 1.263 秒 | 1.262 秒 |

after 积压 623,072 → 629,058 块（+0.96%），冷覆盖和热裁剪均
30,760,397 → 30,761,110。网络待追高度则减少 6,712 块；下载缓冲
1,093 → 1,622 块（64.3 → 82.0 MB）。区块体及交易索引冷覆盖仍为 30,736,384。

新观测 checks/accepted 各 +14，普通与恢复合计 accepted_sequence +53。
恢复观测的 last_duration 采样最大 0.030099 毫秒；level 1/2/3 分别出现在
1/9/3 个采样点，既有 duty 20%/80%/90%。这证明新入口已实际使用且能够更新
判断；不能据此承诺持续满足压力阈值或推算固定吞吐增益。

42 次强制构建成功，40 次事件并行，89 次维护 pass；采样批次中位数 20 块，
最小 1、最大 28。前/后半窗口冷覆盖分别推进 251/462 块，对应 1.394/2.567
块/秒；启动后批量收缩逐渐恢复，但后半窗口仍远慢于导入。半窗结果仅解释
恢复过程，完整六分钟结果仍是主要结论。输入高度、交易密度、缓存年龄和
后台索引工作不同，不能从 before/after 直接推出代码导致的提速或退化。

非 shadow 错误观测全零；shadow VM readiness +8、result +1 单列。GC retired
+3、coverage defer +152、busy defer +13，errors=0；队列尝试 +4、获准 +3、
busy +1、errors=0，队列累计工作 +80,465,965 ns。回收在推进，但大部分尚未
满足覆盖条件。负 obsolete 估算仍存在，实际空间以 du 为准。

设备 busy/queue/await 采样中位数 94.48% / 7.012 / 0.867 ms，最大 await
2.756 ms；不能据此声称磁盘有明确剩余吞吐。stateCommit 滚动采样中位数
20.320 ms/块，为重叠窗口 gauge，不是独立区块的延迟分位数。

## 剩余瓶颈与下一步

after CPU profile UTC 10:36:32 起，45.15 秒墙钟、172.00 CPU 秒，进程年龄
325.3 秒；SHA256 `90d59ddb93c47603166bdccfe9e67ef4c2771573aeb6ead5e22b828056cf9c36`。
commitment union 38.15 CPU 秒（22.18%），其中 durable parent 22.49 秒。
shared pack decode 19.49 秒（11.33%），其中 SHA 16.55 秒。CPU 采样及嵌套栈
不能与六分钟计数混算或直接相加。

共享 SHA 的来源与 before 不同：冷构建 9.92 秒（chunk 4.90、pack 5.02），
`RebuildStateHistoryIndexInterruptible` 经 `advanceStateHistoryIndexStage` 的
后台索引补建 6.63 秒（chunk 3.33、pack 3.30）。before 的 12.03 秒才全部来自
冷构建。两者都在读取验证共享数据，不能把 after 全部 shared 成本归到冷迁移。
元数据目标并集 9.80 CPU 秒（5.70%）；小批次还要反复承担约 1.26 秒元数据工作。

本轮只修复恢复期观测时机，未改变密度目标、批量上限、完整维护恢复期限和
导入限速。采样显示，单靠这一修复仍无法让冷迁移吞吐追上导入。下一候选是
一次固定 read view 内、严格有界的已认证 chunk 复用：保留 fresh Get 和读取
错误，比较完整存储字节与私有已验证副本，复用已验证解码结果，保留完整 pack
SHA。必须先在实际块/片段分布上验证命中率、内存和完整流程收益，不能仅凭
摘要命中省略认证；冷构建和索引补建应分别衡量。

完整 pack 跨两遍扫描复用可能因工作集超过预算全 miss；并发预取也会改变
读取前缀与取消行为，暂不直接投产。详细只读分析保存在本地
`build/benchmarks/20260914-cold-backlog/next-throughput-directions.md`。
如果完整稳定窗口仍显示净积压持续为正，要固定上限就需要导入背压，但这会
降低追链速度。本轮没有选择任意固定速率，也未默认启用新的导入限流。

## 结束状态

UTC 10:42:34（北京时间 18:42），原生 candidate check/healthy_mode 再次 PASS，
仍为 PID 24857 / ticks 4511636088 / 相同 process/start，head 31,458,030 →
31,458,069。最终历史余额 canary UTC 10:40:53 PASS，返回预期值。

| 原生目录/指标 | 最终字节 | 相对 UTC 10:32:11 |
|---|---:|---:|
| chaindata | 265,577,492,480 | +335,097,856 |
| state-snapshots | 690,736,254,976 | +435,453,952 |
| ancient | 236,912,140,288 | 0 |
| 可用空间 | 1,786,455,801,856 | −786,264,064 |

两次 du 相隔 623.136 秒，比指标窗口长，不能混算成该六分钟的磁盘增速。
短期净空间变化还包含压实/回收的相位，不代表长期稳定。
最终原生文件 `/tmp/gtron-cold-backlog-final-native-20260914.json`，SHA256
`ce418a1553624657a7f53f132a0697e53f9fbe8c8c0f41bb71990b45f9aa38ac`。
保留现有 reader guards、祖先标记、缓存/内存参数、端口和兼容回退路径。
本轮未进行停机 inspect、清库、格式变更或手动强制压实。
