# 热历史范围清理的排队准入

09/13 首轮上线的完整后测中，17,856 个选中块全部锁忙回退，range runs 为 0。
新一轮开始的同一进程 `1789259053976239426` 仍为 runs=0、fallback=54,415。
现有指标不能区分 index 与 chain 竞争，不能从这些值单独确定哪把锁占主导。

调用链复核修正了之前的一个前提：cold_builder 的 onePassWithPressureContext
返回时执行 defer release；之后才调用 beforeMerge 中的 PrunePassContext。
热清理没有持有 heavy-work lease，仍同步处于 Runner.passMu 序列中。保留这个
序列，避免 coverage 对象跨任务后失去冷文件生命周期保障；验证缓存不是文件 lease。

## 调度和安全边界

- 原 TryWithStateDomainChangePruneGuard API 保持 index/chain 都非阻塞。
- 新 WithStateDomainChangePruneGuard：index TryLock，chain 公平排队，获锁立即
  检查取消，执行同一份 canonical、Finish/index、solidified、commit/flush 错误和
  Buffer.HistoryPrefixSettled 校验。两锁保持到同步 scan 与全部 batch flush 完成。
- 新入口调用方不得持维护 lease 或这两把锁；线上只由现有 inline 热清理调用。
  不新增 gate 或 posting 的 32 GiB 门限，沿用热清理原资源策略。冷构建刚释放后
  的 cooldown 不能当成扩大原热清理停顿的新理由。
- `GTRON_HISTORY_RANGE_QUEUE=1` 才启用，且要求原 `GTRON_HISTORY_RANGE_PRUNE=1`。
  每 pass 合计最多尝试 256 个 selected blocks，每次 64 块，最多四次；在尝试前
  消耗额度，index busy 也算。剩余块和不足 64 块的片段使用原 256 块 Try/point。
  额度在单个 Worker pass 的整个删除闭包中共享，不能按每次 callback 重置。
- 仅锁忙可 point fallback；取消和任何 proof/写入错误必须返回错误，不推进整 pass
  水位。前片已写、后片失败沿用原幂等重试和成功 pass 才发布统计的行为。
- 64 块和四次排队是数量限制，不是 I/O 或等待时限。sync.Mutex 不可立即取消，
  Stop 必须等当前持锁者退出后检查取消；不能用可遗弃 goroutine 包装 Lock。
  不改变共识、wire、数据库键、保留政策、Pebble 格式或压实配置。

## 可观测性

进程级 `state/prune/history/delete/range/guard/` 下新增 Counter.count：
attempts、busy_index、busy_chain、admitted、prefix_unsettled、proof_errors、
other_errors、work_errors。attempts 入口立即累加；admitted 在 work 前累加。
静止时前七类中排除 work_errors 满足尝试分类守恒；work_errors 是 admitted 子集。
证明进行中和非原子 scrape 可有暂时差额，不能强制修平原始数据。

另有 queued/attempts、queued/chain_busy、queued/chain_wait/total_ns、
queued/chain_held/total_ns 四个 Counter，以及相应 wait/held 的 max_ns Gauge。
等待计时不包括持锁；held 包括 proof 和 work，max 是该进程运行期峰值。
queued/chain_busy 是排队尝试的子集，不加入原尝试分类守恒式。
生产按进程聚合，不随 DB 关闭清零；测试使用独立 registry。
`state/prune/history/delete/range/queue/enabled` 为独立 Gauge.value，配置启用为 1。
新增共 15 项，旧版缺失为 N/A；旧版回滚验收不要求它们。

## 验证及上线

定向测试覆盖两 API 原证明等价、竞争与取消、等待期间证明改变、全程锁保护、
多回调共享额度、busy 消耗额度、失败不发布水位和部分写入重试。相关测试运行 race，
完整受影响测试和原生 CGO/Sapling 验证通过后直接 master 推送、隔离 release 构建。
旧版四开关保持 1，只增加第五个 queue 开关。前后固定进程各 21 点采样，比较
真实 range 行数、排队/持锁和错误，以及同步交易负载与三类积压；allocated du
与逻辑删量、compaction 流量分开。不得把开关或排队计数当作实际回收收益。
