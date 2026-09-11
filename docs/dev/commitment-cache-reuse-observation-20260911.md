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

随后在独立 build overlay 中验证“受限 window 保留 flush-only 分支，并给予一次 FIFO second chance；出现读取引用后才进入 tail”。基线与两个候选执行相同的 42 条轨迹，合计 126 次回放、378 个阶段，值/操作流 SHA 和实际字节预算检查全部通过。窗口分别占同一总缓存预算的 1/64、1/8，没有增加总容量，也未修改已冻结 Go 文件或部署候选。

两档在配对读改写负载中仍全部回退：前台 durable lookup 分别增加 83.33%、43.90%，各为 6/6 组合变差；扫描压力分别减少 7.89%、8.76%，长间隔晚到再读则增加 6.76%、9.06%。小硬额度容纳不下随后整批再读的数据，一次 second chance 不能补足容量。该原型复用了 first-read window，扩大额度也会影响纯读流量，不能等同独立 flush 队列。证据位于 `build/benchmarks/20260911-flush-window-prototype/`。

独立 flush 队列、允许借用空闲容量、压力下按 soft quota 回收，仅是下一候选的待验证方向；仍需核对唯一 queue token、实际 charge、跨类借用及长距离复用反例，并结合线上 cohort 或真实状态回放校准，当前没有上线依据。

## 快照边界与观测范围

`commitment_cache_snapshot_guard_test.go` 使用真实 Pebble：旧 session snapshot 建立后，canonical write/delete 经 CommitInflight、FlushUpTo，再让旧 session 首次读取，然后检查新 session。覆盖 foreground/prefetch × overwrite/delete/negative-to-create 共 6 种情形。保留全局版本校验时通过；仅去掉该校验的负向控制全部失败。首次读取时捕获的 per-key epoch 可能已经在快照之后，不能替代快照版本边界。生产 `layer_view.go` 未修改。

观测器默认关闭，构造 cache owner 时读取 `GTRON_BASE_CACHE_REUSE_OBSERVER=1`。16 个 shard 各 128 个固定槽，共 2048 槽；保存最多 48 字节的完整物理键（含 generation），不持有 entry/value/string 指针，原 80 字节缓存 entry 不变。结构预算上限 512 KiB，属于每 owner 的额外诊断内存，不能等同进程 RSS。

两组分别是 depth 6/7 的 flush-only window promotion 和 probation-triggered absent flush admission。后一组的 fingerprint probation 不证明完整键曾被读过。固定全键 hash 以 1/64 gate 入样，独立位选槽；重复热键 episode 不是独立随机样本，不乘 64 外推。

普通命中路径没有新 hook。实际容量淘汰时读取已积累的 foreground/prefetch 来源；冲突、episode 替换、delete、oversize、clear 都计为中断，尚未结束的样本保留为 live。每个 owner/depth/cohort 的数量与初始 charge 都必须守恒。来源引用包括 durable publication race，不能等同真实 cache hit 或节省的 I/O。本版不观测淘汰后重读、首次命中时刻、复用距离和因果性的缓存挤占。

线上分析逐点检查进程与 owner；任一身份切换、开关错误或 live_missing 使整个窗口不可用。暂时非守恒的 metric 发布点单独排除并记录。窗口完成样本可以来自窗口开始前的入组，必须同时报告中断和 live；不能称窗口新入组全部键的最终命中率。

## 验证与发布记录

Observer 32,768 次混合操作、285 个实际样本的关闭/开启缓存状态对照通过；最终 `TestBaseReadCache*` race 通过（3.155 秒）。元数据实测 139776 字节，即 136.5 KiB。旧版/新版关闭/开启轮换 3 次基准，普通命中及刷新差异接近噪声；开启后完整 window/probation 循环分别增加约 30/38 ns，未增加分配，其它命名空间淘汰无可辨认退化。满 2048 live 槽的统计扫描 0.950→32.682 μs，仅用于低频快照。Allocating invalidate/refill 基准波动大，不报告为改进。基准原始记录位于 `build/benchmarks/20260911-commitment-cache-reuse-observer/`。

增量 lint 初次发现 4 个新增测试问题（3 个 Close 错误检查、1 个 tagged switch），已修复，最终为 0 issues。修复不改变生产源码；修复后的 snapshot/observer 专项复验通过。Go 1.25.5、CGO_ENABLED=0、GOMAXPROCS=2 完整 `go test -p 2 ./... -count=1 -timeout=300s` 通过，耗时 236.678 秒。运行期间仅做上述测试 lint 清理，生产文件冻结哈希始终一致；清理后的专项另行通过，服务器原生测试覆盖最终提交，结果见下文。


### 原生发布

源码固定为 `2fa0f2049a3996a3c43b58d436718fbdc65c2983`，发布助手固定为 `7db1cf117fd2b3d67cddcaa3ede1de6efe1b50b3`，均已正常推送 GitHub master。服务器从固定提交独立归档构建，保留原 checkout 与 Rust 工作区。Go 1.25.5、CGO_ENABLED=1、tags=sapling：net/sync/downloader、state/domains、blockbuffer 原生测试通过，观测器开启后的完整 blockbuffer 测试也通过；core/state/zksnark 定向回归和原生 Sapling 探针通过。cmd/gtron 在指定正则下无匹配测试，仅完成编译，不计为行为覆盖。

构建耗时主要来自 root 冷缓存依赖下载与旧 GCC 编译，准备阶段约 24 分钟；期间原服务持续运行。首次准备还拦截了旧发布记录中一个 62 字符的回滚 SHA 抄录错误，在任何服务变更前停止；已从运行中二进制重新计算 64 字符值并修正记录，没有放宽校验。

准备产物及原生日志在服务器 `/data/gtron/releases/20260911-commitment-cache-observation/`。二进制 SHA-256 为 `b09df4ade279779a0933ac08b8637e3496c8b97afabf76d72cece93f4a5c3cca`。北京时间 11:51:22 启动新 PID 9164，11:51:38 验收高度 25232792→25232822。仅切换二进制路径并增加观测开关，助手确认其余服务参数、内存限制、guard 和 hold 配置保持一致。

运行态开关为 1，owner 为 1，观测元数据 139776 字节；启动精确标识为 `1789098682718281030`。本地保存的部署 JSON 是从 JumpServer 终端手工转录，完整原始记录保留在服务器；进程标识另与直接 HTTP 原始 Metrics 整数交叉核对。采样使用每 30 秒一次、21 组 Metrics/Wallet，传输上限由部署前的 45 秒提高至 120 秒，实际端点时间单独记录。


### 部署后 21 点验收

北京时间 **11:52:54–12:02:54**，Metrics 实际中点窗口 600.186 秒，Wallet 600.001 秒，42/42 请求成功，21/21 点的进程及 owner 都一致。四个 cohort 的 eligible 逐点精确对应既有诊断事件；数量、初始 charge、完成来源和中断原因全部守恒，没有累计值回退或 live_missing。未排除任何点。原始数据及分析在前述目录的 `after/`、`after-analysis.json`、`ownership-after-analysis.json`、`diagnostics-after.json` 和 `reuse-after.json`。

| 观察项 | 完整窗口 | 后半段 |
| --- | ---: | ---: |
| Wallet 同步速度 | 40.59 块/秒 | 49.78 块/秒 |
| Wallet TPS | 2291.43 | 2451.75 |
| 交易/块 | 56.46 | 49.26 |
| 平均进程 CPU 核数 | 3.27 | 3.40 |
| 接收缓冲块数 | 1483→1399 | 1471→1399 |
| 状态 head-distance 净变化 | -1409 | -2349 |
| 区块历史 head-distance 净变化 | -41221 | +14428 |

后半段 Metrics 为 11 点约 300 秒；按 Metrics 中点分界筛出的 Wallet 为 10 点约 270 秒，二者不同，未共用分母。区块历史本轮批量发布 65536 块，后半段暂时没有下一批发布，形成锯齿，不能只取后半段涨幅判断持续恶化。

接收缓冲观测范围 1284–1832 块、中位数 1538；字节 32.01→33.63 MB，峰值 38.94 MB；21 点都 active，没有 pause 或 fetch backpressure。归属 gauge 为 704–4032；重复 inventory 过滤观察增加 1289，fetch/body/restored 重复计数未增加。这些过滤计数不代表不同区块数，也不相加。按用户接受的“积压不扩大或维持同量级”标准，本窗口符合：末点状态落后 **326939** 块，区块历史/交易索引落后 **355112** 块；它们互相重叠，不能相加。

采样时进程年龄约 92–692 秒，缓存 retained charge 从 358.81 MB 填充到 536.86 MB，存在明确的重启预热。完整窗口块速比部署前低，后半段高，但既不是相同高度回放，也不是隔离 A/B。后半段交易密度更低，commitment updates/apply block 由全窗 385.07 降至 344.89；不能把 40.59 或 49.78 与部署前 46.51 的差异归因于本次 observer。

全窗 19 个完整包含的 import 子窗口覆盖 23616 个 apply block，VM 交易占分类交易 69.94%，raw energy/VM 交易约 46192；state commit、execute、persist 加权耗时分别 9.45/18.87/3.99 ms/block。阶段可能重叠，不相加。子窗口覆盖约 579 秒，与 Wallet/全量读取计数窗口不同，不能把全窗 durable 次数除以子窗口 commitment updates。

### 复用分布与下一优化方向

下表为同 owner 的窗口增量；live 列保留起末值以显示守恒。`无后续引用`仅限因容量淘汰而完成的样本。

| cohort | 入样增量 | 容量完成 | 完成中无后续引用 | 中断增量 | live 起→末 |
| --- | ---: | ---: | ---: | ---: | ---: |
| depth 6 window flush-only | 10702 | 4525 | 4391（97.04%） | 6337 | 557→397 |
| depth 6 absent-flush probation | 300 | 116 | 112（96.55%） | 162 | 0→22 |
| depth 7 window flush-only | 18539 | 7901 | 7692（97.35%） | 10810 | 895→723 |
| depth 7 absent-flush probation | 393 | 123 | 120（97.56%） | 233 | 0→37 |

容量完成样本仍有真实后续调用来源：depth 6/7 window 分别 134/209 个，probation 为 4/3 个。中断主要是采样槽碰撞，live 总量最大 1743，未超过 2048；这些中断具有选择性，不能把约 97% 无引用外推为“全部写回缓存 97% 无用”。结合配对读改写反例，目前保留既有准入是必要的。

Commitment 仍有明确读取成本：depth 6/7 前台 no-resident 增量约 130.22/127.29 万，depth 6 预取约 99.52 万；depth 6/7 durable 读取约 102.24/127.29 万。resident-newer 与 snapshot-version 拒绝均未增加，且去掉版本边界已被正确性反例否决，因此不能靠放宽快照边界减少读取。

后台存储同样繁忙：L0 sublevels 为 2–4，compaction debt 3.01→2.07 GB（峰值 4.51 GB），设备 busy 观测中位数 86.36%，await 中位数约 0.774 ms。21 点 device-known 均为 1；busy 是设备级离散观测，不等于数据库独占，也不能单独证明设备吞吐已饱和。状态归档本窗口发布速度约 42.86 块/秒，超过主线 40.51 块/秒，暂不需要为追求更低积压进一步挤占前台资源。

下一候选优先验证**独立 flush 队列、空闲时借用总容量、压力下按软额度回收**，保留当前 first-read window，并继续用配对与晚到复用反例约束；固定小硬额度已被否决。策略上线前还缺真实复用距离和淘汰后重读证据，本版观测不能回答这两项。另一条可并行研究的方向是减少 commitment 点读取与后台 compaction 的竞争；应以每次实际 durable 读取成本和后台进度为证据，不直接增加后台并发。

### 正确性与服务最终状态

本窗口可见的存储、所选 canonical guard 和其他非 shadow 错误计数增量均为零。Shadow sender-chain 增加 9，VM sender-chain 增加 60，其子项 readiness 41、apply-unsupported 17、result 2；父子计数有重叠，保留这些诊断，不能报告“所有错误为零”。签名 lookahead mismatch 为零。

采样结束后，独立核对新版本导入的高度 25234000、25240000、25250000，区块哈希、父哈希及有序交易 ID 与 TronGrid 全部一致，分别含 66/87/27 笔交易。这仅覆盖三个样本，不是全状态证明。北京时间约 12:04:45 最终只读验收：PID 9164、二进制校验、运行参数、观测开关、guard/hold 和服务配置均通过，Wallet 高度已到 25263314。MemoryLimit 保持 40 GiB，LimitNOFILE 保持 131072，数据盘可用约 2.37 TB。有限日志尾部可见持续导入、归档和清理；一条 peer BAD_PROTOCOL 断连与历史 peer 不可用提示之后仍继续推进，未将其当成主链失败。

本轮完成的是正确性回归、候选策略反例验证、有界复用观测上线及实测闭环；没有将未证实的缓存策略收益包装成同步加速。

本地证据索引为 `build/benchmarks/20260911-commitment-cache-admission/evidence-index.json`，按文件记录 SHA-256；该 build 目录属于本地实验材料，生产代码、测试、设计说明和本报告已通过 GitHub 留档。
