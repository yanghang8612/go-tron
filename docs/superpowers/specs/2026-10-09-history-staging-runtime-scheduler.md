# 在线 history-staging mover 的公平量子调度

2026-10-09。本设计补充 `2026-10-01-history-staging-store.md` 的运行时资源门禁；不更改持久 receipt/claim/route 格式、唯一读源、冷认证、canonical 证明或离线迁移协议。

## 问题与准入

现网观测到 mover 的 13,582 次尝试全部因 hot pressure 跳过，尚未搬桶。设备平均队列约 3.5–4.6、busy 约 78–87%、await 约 0.7ms。原门禁同时要求 busy <90%、queue <1、await <20ms；queue 单项足以让低延迟并行设备持续饥饿。SOURCE census 是来源库存，不能视为已验证可搬桶数。

保留原 idle lane：busy <90%、queue <1、await <20ms。新增 bounded busy lane：busy <90%、queue <8、await <5ms。两个 engine 都必须满足其中一个 lane；引擎或设备 unavailable、零时间戳、未来超过 1 秒、超过 15 秒的样本均拒绝。WriteStalled、memtable 接近 stop、L0 达到原 HardLimitReached 阈值仍拒绝。空间预留不降低；初次准入与每次重获 heavy token 后复查 free-space floor，copy 仍保留原有 worst-case 及 source-sized 预留检查。

2026-10-10 补充：线上一次约 93 秒窗口里，mover 的工作字节、持有时间和 yield 数均不变，`device_busy` 拒绝从 28 增至 299；同时设备 await 约 1.14ms、queue 约 4.05。利用率降至约 38% 后工作恢复，说明单独的 busy 阈值确实阻塞了 mover。仅对采用协作量子的 mover 增加保守入口：90%≤busy≤100%、queue<8、0≤await<2ms 时按 busy lane 接纳。低利用率的原规则、所有 freshness/engine/memory/free-space 门禁、100ms/32MiB 量子及恢复时间不变；index GC 继续使用原分类函数，不能随之放宽。这个入口不证明设备还有带宽余量或尾延迟安全，上线仍须核验实际 await/queue、前台导入/stall 和归档推进。

生产 mover 另接同一后台 sampler 的 memory-only cached observation，不重复读取 proc/cgroup，不复用 CPU parallel-read 的 Available（CPU quota、idle、GOMAXPROCS 不决定 mover 内存准入）。样本 fresh ≤15 秒、OOM 状态已知且非 underOOM、经既有 probe 审核的 memoryAvailable ≥2GiB 才接纳；clean file cache credit 仅消费 probe 已审核结果，原 uncredited/credit 指标继续解释容量。运行中内存未知、OOM 或不足作为 typed expected resource deferral 结束 pass、释放 pinned views/ETL/decoded 状态，不能保留自身大 buffer 无限等待。2GiB 是准入余量目标，不是 RSS 硬上限；现有 ETL reader 的约 1MiB × run 数等峰值限制仍存在，不能保证 32GiB 环境零 OOM。

门禁每 32MiB 累计工作或 100ms 实际持有时间（先到者）复查，阶段边界另行强制复查。每次 row/ReadAt 保留取消检查，但不重复调用两引擎压力 probe。工作字节包含扫描、复制、认证和逻辑解码的记账，不能当作磁盘实际 I/O 或净新增容量。

## 一个 copy 可以跨多个 heavy lease

`HistoryStagingLimits.Checkpoint` 与 `maintenance.WithWorkCheckpoint/WorkCheckpoint` 提供同步协作边界。普通离线 CLI 无 hook，继续使用原协议。runtime copy 在原调用栈上保留 pinned source/target、iterator、hash 与 pending batch；yield 不从前缀重扫，不重复写已经复制的行，也不生成临时可读 receipt。进程崩溃后的恢复仍依赖已有 durable claim，允许重新复制 orphan；本设计不声称跨重启保留细粒度 copy 游标。

每个量子完成后真正释放共享 heavy token，本 worker 等待至少 `max(1s, 4 × 本量子实际持有时间)`，之后重新观察两引擎、设备和空间，并重获 token 才继续工作。自己的 per-lease recovery policy 使用同一公式，不缩短其他任务已生效的 recovery；共享 gate 原有 250ms minimum-work 阈值保留，较短量子由本 worker 的等待保证自身占空比。等待期间其他维护可以取得 token。压力转坏时保持当前 pinned copy 状态并让出 token；取消或终止性空间探针错误退出，保留 source/claim/orphan，不继续无 token 的 retirement。

物理 row、逐 block 逻辑验证、copy/delete batch、target GC 与 canceled-claim purge 都有边界。batch 的 checkpoint 在 route/reset/space 复核之前；任何等待结束后必须重新检查授权和当前空间，不能使用等待前的结果执行写入。canonical index→chain、route publication 与冷 publication admission 内不调用会等待的 hook。取消 claim 的早期分支也必须先释放 cold publication Read admission再 purge。

昂贵冷 proof、rebind 与 source/target 等价认证沿同一 context 获得工作边界。checkpointed 冷认证不等待全局认证 singleflight；可以使用已完成且指纹相符的成功 cache，cache miss 自行认证并在完成后发表成功，防止持 heavy token 的等待者与已经释放 token 的认证 leader 互相等待。ReadAt 的 hook 位于 codec mutex 之外。ETL 的主线程 spill/merge 边界遵守同一原则。

## 公平与候选提示

初始 reservation 仅来自最多 256 条的 route-only metadata 页，使用独立 wrapping hint cursor，在 canonical writer 锁外读 cached head/solid/stage 的近似边界。SOURCE 必须看起来是完整 eligible 桶；TARGET source clear 和 cold target cleanup 也可提示任务。已清源 TARGET 若当前冷可见 tx 范围明显不覆盖末块，则不预约。hint 不读取完整 claim proof/receipt，不授权移交或删除；真正执行仍重验全部原证明。

一次 reservation 最长 10 秒；本 mover 每 30 秒最多创建一次，已存在的其他 reservation、active lease、global admission 和 recovery 都继续有效。压力丢资格、取消、没有候选或本次任务完成立即取消。过期后不会连续霸占下一轮；cold/archive/index 工作保留运行机会。新的 hint 仅改善初始调度，不改变迁移、retirement、quarantine 各自游标的正确性。

## 非硬抢占边界与观测

100ms 是协作目标，不能承诺硬抢占。单个最大 16MiB physical row、rawdb state-history 单 block decoder 最高 128MiB 的内部解码上界、32MiB ETL sort/spill buffer、既有 compressed codec 单块最高 256MiB / reference codec 单块最高 32MiB 的调用，以及单次 batch/fsync 仍可能超过目标。runtime 64MiB MaxDecodedBytes 是 block.Info 返回后的接纳上限，不能描述成解码期间的最大暂态分配。quarantine 保留原 32MiB/128-row 小页，持 publication Read admission期间禁用 context hook，只在整个 phase 外设置 checkpoint。整桶 canonical proof capture/adopt 的 metadata 闭包保留原锁序，不能在其间让出 heavy token。

新增 `mover/admission_mode`（0 idle、1 busy，其余按代码枚举为拒绝原因）、`denied/{hot,stage}/{reason}`、`quantum_work_bytes`、`quantum_yields`、`quantum_wait_nanos` 、`quantum_held_nanos` 及 `resource_denied/{memory_unknown,memory_stale,memory_oom,memory_low}`。已有 skipped、moved、copied、last_success、source/uncleared census 继续保留。验收必须同时查看实际 source/target allocated bytes、冷发布进度、导入吞吐、write stall、设备物理 I/O 与峰值内存；量子数量或逻辑删除数不证明释放空间。

## 验证

测试覆盖低延迟 queue>1 的 busy lane、精确阈值、unknown/stale/hard rejection、量子中另一个 worker 取得 token、100k tiny rows 的 probe 次数与 row 数解耦、中途压力和取消、reservation 不绕过他人 recovery、瞬时 statfs 错误后的终止状态、copy 不从前缀重写、错误 proof/取消保留 source，以及等待结束后重新检查 free-space floor。原 HistoryStaging 的 mixed cold、repair/Prev、恢复、唯一来源和 retirement 测试仍须全部通过。上线后核验实际搬移/归档进度、导入吞吐与设备负载；部署验证仍须包含原生 Linux Sapling。本轮只产出本地可审阅候选，不 push 或部署。
