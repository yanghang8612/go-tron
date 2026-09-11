# 2026-09-11 Commitment 缓存深挖与复用观测

本轮沿用 master 开发和 GitHub 固定提交发布。缓存策略实验发现明确的反向负载，因此保留当前准入策略，补充真实 Pebble 快照回归和有界线上复用观测；不能把新增观测功能称为同步加速。

## 部署前基线

原始文件位于 `build/benchmarks/20260911-commitment-cache-admission/`。北京时间 10:35:55 开始，每 30 秒采一次，共 13 组 Metrics/Wallet 请求，均成功。每点精确校验进程启动标识 `1789092225326376992`，对应正在运行的 `54c46b7fe1dd2b0521e50ab7dd1e4983e27c7cde`。Metrics 请求中点窗口约 360.147 秒；跨端点的计数各用自己的时间窗口。

Wallet 平均 46.51 块/秒、2726.00 交易/秒、58.61 交易/块。接收缓冲 495→1748 块，观测范围 472–1905 块，原始字节 6.79→37.08 MB，峰值 48.38 MB。距先前去重修复部署约半小时，仍处在几百至约两千块范围。短窗口净增不能解释为无限积压，也不能称为一直下降。

状态 head-distance 328387→328918，增加 531；后半段减少 113。区块历史 head-distance 341713→358519，本窗口没有新的批量发布。两个距离互相重叠，不能相加。当前状态落后约 32.9 万块，维持原数量级。

前台 depth 6/7 非驻留分支读取分别为 780346/781444。depth 6 预取非驻留读取 626678；depth 7 没有该类预取。窗口推广的 flush-only 比例分别为 66.36%/95.66%，tail 淘汰的 flush-only 比例为 28.45%/82.87%。当前 `resident_newer` 和 snapshot-version 拒绝填充均为零。

北京时间 11:12:50 的额外 CPU profile 完整采集 10.18 秒，累计 CPU 样本 34.67 秒（约 3.41 核）。累计栈中 OrderedCommitmentPipeline.runPartition 占 17.97%，Pebble runCompaction 17.36%，状态历史索引重建 13.93%，commitmentParentReadSession.readDurable 11.19%；这些调用栈可能嵌套，不能相加。说明 commitment 点读取和后台存储仍值得深入，短 CPU 样本不能直接衡量 I/O 等待或推导提速幅度。前两次代理传输超时的 profile 不完整，已排除，第三次才成功；分析工具仅读取第三份。

## 准入策略实验与否决原因

`commitment_cache_admission_replay_test.go` 用现有完整缓存实现进行固定输入回放，独立最新值 oracle 检查每次逻辑读取，并检查实际字节预算。使用 4/16 MiB、3 个种子和 7 类负载，共 42 组；5 个策略合计 210 次回放、630 个阶段。源代码通过 Go overlay 精确替换，原策略 overlay 与部署前源码字节一致。该工具需要显式 `GTRON_CACHE_ADMISSION_REPLAY=1`，普通测试跳过。

下表是前台 durable lookup 总次数相对基准的变化；负数代表减少，并非线上磁盘 I/O 或吞吐变化。

| 策略 | 读改写与扫描压力 | 短间隔读改写压力 | 长间隔复用 | 配对读改写压力 |
| --- | ---: | ---: | ---: | ---: |
| 仅调整反馈 | -0.02% | -4.04% | -0.07% | -0.01% |
| 仅调整 window credit | -1.52% | +16.88% | -0.10% | +21.75% |
| depth 6/7 要求读取证据 | -7.89% | -43.73% | +6.16% | +84.01% |
| 仅 depth 7 要求读取证据 | -4.70% | -39.47% | +6.18% | +41.97% |

配对负载的第二次读取发生在 FIFO 已经走过、但总缓存尚未耗尽时，属于应由 tail 保留受益的有效工作负载。depth 7 限制策略在全部 6 个配对组合都退化，其 depth 7 自身前台读取增加 84.77%。故不能依据 flush-only 比例直接关闭这些准入路径。初始 smoke fixture 曾存在分片/命名空间低位相关性，已修正；不使用该版本数字作为结论。

## 快照边界与观测范围

`commitment_cache_snapshot_guard_test.go` 使用真实 Pebble：旧 session snapshot 建立后，canonical write/delete 经 CommitInflight、FlushUpTo，再让旧 session 首次读取，然后检查新 session。覆盖 foreground/prefetch × overwrite/delete/negative-to-create 共 6 种情形。保留全局版本校验时通过；仅去掉该校验的负向控制全部失败。首次读取时捕获的 per-key epoch 可能已经在快照之后，不能替代快照版本边界。生产 `layer_view.go` 未修改。

观测器默认关闭，构造 cache owner 时读取 `GTRON_BASE_CACHE_REUSE_OBSERVER=1`。16 个 shard 各 128 个固定槽，共 2048 槽；保存最多 48 字节的完整物理键（含 generation），不持有 entry/value/string 指针，原 80 字节缓存 entry 不变。结构预算上限 512 KiB，属于每 owner 的额外诊断内存，不能等同进程 RSS。

两组分别是 depth 6/7 的 flush-only window promotion 和 probation-triggered absent flush admission。后一组的 fingerprint probation 不证明完整键曾被读过。固定全键 hash 以 1/64 gate 入样，独立位选槽；重复热键 episode 不是独立随机样本，不乘 64 外推。

普通命中路径没有新 hook。实际容量淘汰时读取已积累的 foreground/prefetch 来源；冲突、episode 替换、delete、oversize、clear 都计为中断，尚未结束的样本保留为 live。每个 owner/depth/cohort 的数量与初始 charge 都必须守恒。来源引用包括 durable publication race，不能等同真实 cache hit 或节省的 I/O。本版不观测淘汰后重读、首次命中时刻、复用距离和因果性的缓存挤占。

线上分析逐点检查进程与 owner；任一身份切换、开关错误或 live_missing 使整个窗口不可用。暂时非守恒的 metric 发布点单独排除并记录。窗口完成样本可以来自窗口开始前的入组，必须同时报告中断和 live；不能称窗口新入组全部键的最终命中率。

## 验证与发布记录

Observer 32,768 次混合操作、285 个实际样本的关闭/开启缓存状态对照通过；最终 `TestBaseReadCache*` race 通过（3.155 秒）。元数据实测 139776 字节，即 136.5 KiB。旧版/新版关闭/开启轮换 3 次基准，普通命中及刷新差异接近噪声；开启后完整 window/probation 循环分别增加约 30/38 ns，未增加分配，其它命名空间淘汰无可辨认退化。满 2048 live 槽的统计扫描 0.950→32.682 μs，仅用于低频快照。Allocating invalidate/refill 基准波动大，不报告为改进。基准原始记录位于 `build/benchmarks/20260911-commitment-cache-reuse-observer/`。

增量 lint 初次发现 4 个新增测试问题（3 个 Close 错误检查、1 个 tagged switch），已修复，最终为 0 issues。修复不改变生产源码；修复后的 snapshot/observer 专项复验通过。Go 1.25.5、CGO_ENABLED=0、GOMAXPROCS=2 完整 `go test -p 2 ./... -count=1 -timeout=300s` 通过，耗时 236.678 秒。运行期间仅做上述测试 lint 清理，生产文件冻结哈希始终一致；清理后的专项另行通过，服务器原生测试还会覆盖最终提交。线上部署结果待下文补充。
