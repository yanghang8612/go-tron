# 状态读取优化主网发布（2026-09-10）

主网已于北京时间 10:37:11 启动本轮版本，`gtron.service` 为 active/running，健康检查确认区块继续推进。十分钟连续采样完成，41/41 组请求成功，同一新进程持续推进，所观测的存储相关错误计数无新增。

## 发布身份与流程

- 生产提交：`4c25c69b22ef50c665b49a26e0501026cb0f0a2c`，已推送 GitHub `master`。
- 运维脚本提交：`b22b553c2a271398cde3b4846bcf3e2490c88d93`，位于 `ops/deploy-state-hotpaths-20260910`。生产代码与运维脚本分别固定提交。
- 服务器从 GitHub fetch 后将源码快进到生产提交，再用该提交的 `git archive` 建立独立构建目录。没有通过直接传包发布生产源码。
- 新 release：`/data/gtron/releases/20260910-state-hotpaths`。
- 新二进制 SHA-256：`081b4e6f60aeb348775c436d1e6821bd2cd646858809116be381395c2758b944`。
- 新 PID：`19704`，proc start_ticks：`4474232475`；metrics 进程标识：`1789007831235993682`。
- 旧 release：`/data/gtron/releases/20260909-parallel-history-event-9059eb7f/gtron`，SHA-256：`2d3f9707c176c3e15b30d492d676e8594b483200a9569912b649ef7407bf4991`，完整保留。

服务器使用 Go 1.25.5、Linux amd64、CGO=1、`-tags=sapling`、最多两个 Go worker，复用现有 Sapling 静态库。四个相关包的定向原生测试、`Available()`/`Uncommitted()` probe、二进制构建及构建设置检查全部通过。prepare 于 10:35:13 开始、10:36:05 完成，期间旧服务持续运行。

部署脚本核对固定提交来源、11 个源码文件哈希、旧进程身份、旧二进制哈希、服务及 drop-in 文件、有效参数、空间守护和维护 hold。仅替换 `/etc/systemd/system/gtron.service.d/zz-genesis-cpu-20260907.conf` 中生效 ExecStart 的二进制路径；启动参数、端口、数据库、Nile、自动部署和维护 hold 保持。新进程的 argv 与旧进程除二进制外逐项相同。

10:37:01 发起正常关闭，旧进程日志为 `Clean shutdown`，关闭头为 21,765,478；10:37:11 systemd 记录 stopped/starting。新服务 Result=success、ExecMainStatus=0，初次健康检查头由 21,765,481 推进至 21,765,556。切换采样保留了网关短暂不可用的原始失败点，没有将其删去。

## 连续窗口

切换前基线为 10:19:58–10:21:58，9 个 metrics 和 9 个 wallet 请求全部成功，旧进程身份稳定。wallet 请求中点间隔 120.052831 秒，session 增加 8,224 块、345,956 交易，即 68.503 块/秒、2,881.698 交易/秒。独立 metrics 中点间隔 120.201206 秒，head 增加 8,177 块，状态发布及裁剪增加 13,685 块，head 到状态前沿距离减少 5,508 块。所观测存储类错误计数无新增。

发布后十分钟窗口为 10:38:57–10:48:58，共 41 个 metrics 与 41 个 wallet 请求，全部成功且同一新进程。metrics 中点间隔 600.760562 秒，wallet 中点间隔 600.031167 秒；统计使用各自中点和同一进程内增量，不跨重启相减累计计数。两个版本覆盖不同历史区块，交易密度、TVM 工作量、缓存与后台任务阶段不同，不是同输入 A/B。

| 指标 | 开始 → 结束 | 完整窗口结果 |
| --- | --- | --- |
| metrics head | 21,771,661 → 21,813,915 | +42,254；70.334 块/秒 |
| wallet session blocks | 6,335 → 48,607 | +42,272；70.450 块/秒 |
| wallet session transactions | 218,854 → 1,905,116 | +1,686,262；2,810.291 交易/秒 |
| 状态发布及热裁剪 | 21,177,662 → 21,224,071 | +46,409；77.250 块/秒 |
| head 减状态发布 | 593,999 → 589,844 | 净减少 4,155 块 |
| 正文覆盖 | 21,168,128 → 21,168,128 | 未推进 |
| 交易索引覆盖及裁剪 | 21,135,360 → 21,168,128 | +32,768；追平正文覆盖 |

十分钟平均使用 3.495 个 CPU 核，累计分配速率 374.184 MB/s（十进制），不能将累计分配速率当作常驻内存。重启后 peerCount 从 11 恢复至 29，同步 peers 为 7–8；41 点均未暂停。parallel transfer/VM 开关保持 0，async commit 保持 1。

对照旧版两分钟基线，session 块速率为 68.503 → 70.450，交易速率为 2,881.698 → 2,810.291，平均交易密度为 42.067 → 39.891 笔/块。状态发布速率为 113.851 → 77.250 块/秒；两窗口背景任务及预算阶段不同，不能只挑块速率宣称整体性能提高。后半段出现阶段性落后：20→32 点债务增加 7,365 块，32→40 点又减少 7,637 块，完整十分钟净减少 4,155 块。正文覆盖全窗未推进，没有证据把这段暂停归因于正文 freezer 持有维护 lease。

history 预算的 20%/80%/90% duty 分别出现 17/19/5 次，hard 压力全窗为 0。中段批量缩至 459–560 块、恢复等待约 15.6–17.8 秒；末段批量恢复到 1,306–1,885 块、等待约 0.52–1.36 秒。rate_limit/resource/sync deferral 分别增加 41/3/4，支持预算与恢复调度影响阶段速率的解释，不能从轮询断言唯一触发因素或具体 lease owner。cold compaction 实际完成两次 merge，亦不能因轮询时 active=0 就认定后台从未工作。

所观测的 storage、commitment、prune、cold、freezer 等错误计数无新增。Transfer sender-chain shadow 拒绝计数增加 30；VM shadow 增加 42，由 readiness 32、apply_unsupported 6、result 4 解释，父子计数不重复相加。这些不是 canonical 失败总计数；现有采样也不能证明所有 canonical 失败为零。

## CPU 与资源

10:40:10.225–10:40:42.164 请求一次 CPU profile，内部采样长 30.14 秒、合计 105.03 CPU 秒；前后 metrics 进程标识一致，profile 映射为新 release，Build ID 为 `2be245dcad86d6c66bb1f6f7fdb05e7576b41b20`。旧两份样本分别开始于 06:33:39.438、06:35:17.389，区别于 10:19–10:21 的部署前吞吐基线；新 profile 内部开始于 10:40:10.991。以下对照三份约 30 秒样本，前台函数均按完整调用栈限定在 `applyBlockWithPlan` 内：

| CPU 秒 | 旧样本 1 | 旧样本 2 | 新样本 |
| --- | ---: | ---: | ---: |
| ContractRuntime | 2.40 | 2.57 | 0.22 |
| IterateAccountKV | 2.79 | 2.26 | 1.09 |
| 前台完整 apply | 24.21 | 24.16 | 21.96 |
| Commitment pipeline 去重并集 | 32.95 | 33.78 | 31.61 |
| GC 工作栈 | 2.66 | 10.36 | 4.53 |

目标读取路径的 CPU 成本在此样本中明显缩小，新 decoder 路径也已出现在调用栈。各行有包含关系，不相加；不同区块和后台工作量不能提供同输入因果比较。前台完整 apply 及 commitment 仍占主要 CPU 成本，不能把上述局部差值表述为同步吞吐提升。

profile 前后独立 metrics 包络为 33.609739 秒、head 增加 2,046 块，即 60.875 块/秒；没有将高度差除以 profile 内部 30.14 秒。10:43:03 单点 `/proc/19704/status` 的 RSS 与进程启动以来 VmHWM 均为 13,707,388 KiB（约 13.072 GiB），低于现有 40 GiB 硬上限；这不是十分钟 RSS 连续采样。10:49:25 再核验 RSS 为 14.868 GiB，进程启动以来 VmHWM 为 15.596 GiB，PID、运行文件哈希和服务状态不变。空间守护检查通过，数据盘可用 2,539,161,600,000 字节。

## 查询与范围限制

新版本已导入的 21,767,000、21,769,000、21,771,000 三个区块查询成功，分别返回 30、26、39 笔交易。现有 Java 网关对这三个历史高度返回空对象，因此没有完成跨节点哈希/交易序列比较，不能把空响应表述为已证实的链数据不一致，也不能宣称此项比较通过。

daemon-reload 报告现有基础 service 中 `StartLimitInterval`、`StartLimitBurst` 和 `MemorySoftLimit` 指令不被此 systemd 接受；本次未新增这些指令，配置快照验证除二进制路径外无变化。实际 `MemoryLimit` 为 42,949,672,960 字节。没有将被忽略的软限制当作生效保护。

## 证据与回退

本地证据：`build/benchmarks/20260910-sync-hotpaths-deploy/` 中的 `before/`、`transition/`、`after/`、`profiles/`、`canonical-checks/`、分析脚本和 `deployment-transcription.json`。transcription 是从已认证终端可见输出转录的摘要，不是服务器原始日志副本。

服务器原始证据位于 release 目录，包括 `prepared.json`、源码清单及 archive、Sapling 测试/probe/build 日志、`activation-before.json`、`activation-after.json`、`activation-state.json`。这些文件包含配置备份；不要通过公网路由公开。

回退仍需确认现场配置未被后续发布改变。当前脚本自动检查旧二进制哈希，以及本次替换的 ExecStart 文件是否仍逐字节等于原版或本次新版；其他 drop-in 和环境设置需先核对。它正常停止候选、恢复该文件后才检查空间保护，保护条件不满足时不会启动旧版。旧版成功启动后，还需确认区块继续推进：

```bash
sudo /usr/bin/python3 /data/gtron/releases/20260910-state-hotpaths/deploy.py rollback
```

本轮没有修改数据格式，也没有回退或删除链数据库。
