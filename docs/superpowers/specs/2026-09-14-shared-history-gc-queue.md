# 共享热历史片段回收的有界排队准入

2026-09-14。已实现并部署，原生测试及六分钟线上验收完成。用户授权继续解决共享片段未退休及冷存积压，沿用
master、GitHub 推送和原生部署。保留所有旧历史及 v3 引用格式。

## 证据与范围

8058 跨夜同输入历史逻辑写入减少 88.866%，chaindata 实测 523.504→228.147 GB，
整个 datadir 1,375.222→1,131.659 GB。问题集中于后续回收：GC 累计 10,301 次
候选访问，coverage defer 10,197、busy 104、retired 0、errors 0。访问数不是独立桶数。
部分候选通过覆盖证明后仍未获锁，现有 GC 只调用机会式 Try guard。

部署前新窗口的首点已观测旧版本累计 retired=2、busy=154，证明旧路径并非永远无法
回收。本轮针对繁忙时的准入不稳定；新版退休不能称为整个系统首次成功回收。

本轮只改善共享桶的准入机会并补足观测，不调冷存预算。北京时间 10:38–10:44 的早间基线中，state 冷发布
与热裁剪推进 4,211 块，比 head 多 429；body/index 按完整 65,536 块段依赖 event
覆盖，短窗口静止不能证明 freezer 饥饿。后续 metadata 摊销优化需单独固定输入回放。

## 调度与安全

沿用 `GTRON_HISTORY_RANGE_QUEUE=1` 提供的 `HistoryRangeQueuedGuard`，每个 GC pass
只为首个完整覆盖候选消耗一次排队额度，尝试之前消耗；busy、error 均不能退还额度。
其余至多三个候选继续使用原 Try guard。关闭该开关或未提供 queued guard 时完全沿用
机会式准入。热删除原有四次 64 块排队额度不变，GC 是独立额外的一次桶退休尝试。

队列入口仍先 Try index 锁，再等待 chain 锁；获锁后重验取消、canonical、Finish、
history index、solidified、commit/flush 错误及 settled prefix。调用位于 cold-builder
lease 释放之后，处于原 inline Runner 生命周期内。覆盖对象不跨任务或脱离文件生命周期。
sync.Mutex 的等待不能立即取消；额度限制的是次数，不是硬性时延，不添加遗弃 goroutine。

所有 1024 块 tx-range 的连续性、冷覆盖、保留政策均在准入前验证。热 pack 删除已经
flush；退休闭包仍在锁内使用新物理视图确认整个桶无 pack/repair 残留，同一原子 batch
删除 chunk 范围并写永久 retired 标记。锁忙、证明改变、取消或写失败不能放宽这些条件。
旧 snapshot 的 MVCC 引用、retired 后重放的自包含 fallback、reset 和错误传播保持不变。

## 指标与验证

GC 单独记录排队 attempts/admitted/busy/errors 与退休闭包 work total/max 纳秒。
work 仅覆盖回收范围检查及批写，不含等待、guard proof 或整体同步 CPU。原 guard 的
全进程排队等待和持锁指标继续有效；实际 retired 代表逻辑退休，物理释放等待压实。

测试覆盖每轮额度、完整覆盖门槛、开关回退、busy 消耗、真实互斥锁竞争与取消、等待
期间证明变化、批写失败重试和旧读视图。复用生产 guard 的既有 fail-closed 和锁生命周期
测试。原生 Sapling 构建、受影响测试及历史查询通过后部署兼容 reader；恢复目标为当前
8058，保留 D656/8058 永久 marker，不允许回退到不认识 v3 的旧版本。

线上验收须观察新增排队实际准入和实际退休，并核对错误、历史 canary、同步交易负载、
冷覆盖差距及锁持有成本。仅看到排队计数不构成回收成功。

实际验收：新进程累计退休 13 桶；六分钟窗口 queued 准入 +12、退休 +12，错误为零，
后续 GC 闭包合计 78.652 ms。启动首个闭包 344.548 ms 峰值未再突破。冷积压仍小幅
增加，不能称为追平；最终 chaindata 236.447 GB。完整证据和解释边界见
[部署验收记录](../../dev/history-gc-queue-fix-20260914.md)。
