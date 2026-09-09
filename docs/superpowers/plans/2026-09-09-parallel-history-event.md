# 同批状态历史与事件构建并行

目标是在新鲜 CPU 余量和健康存储观测下，重叠同一已固化区块范围的历史与事件构建。只启动两个独立 builder，保留各自现有压缩 worker 数、范围和字节预算。未知、压力、CPU 不足、非同步吞吐模式继续串行。

历史读取热状态变更，事件读取 canonical 正文和严格校验的回执，两者没有文件依赖。当前选取范围已经受 solidified、HistoryWindow 和 StageFinish 约束；生产同一个 HeavyWorkGate lease 阻止 direct V2 新正文迁移和索引维护重叠。同一 SnapshotLifecycle 在 Runner.passMu 下构建、统一发布，再裁剪已验证覆盖的状态历史。既有 crash-leftover 清理只删除已经持久化到 ancient 的重复热正文，事件仍通过 ancient-first 接口读取；同步暂存清理只处理自己的暂存前缀。因此本轮继续使用并发安全的 live Pebble 和完整 ChainDB 读取接口，保留 ancient 回退和 external-log 还原，不引入假的可写快照适配器。每个 builder 独立持有迭代器和临时构建对象；这并不提供跨读取 MVCC，也不宣称所有清理均受同一 gate 控制。

在同一 lease 内启动历史及 V4 事件构建。两边各自返回 refs、耗时和错误，父流程 join 后统一检查错误及取消，再组装一次 manifest。未传递 context 的现有内部构建无法中途抢占：任一失败或父取消均等待另一边退出，不提前释放 lease。父流程在发布前再次检查取消；完整维护占空比继续计实际墙钟时间，不把两个重叠阶段耗时相加。

临时文件沿用 builder 原有关闭/清理流程。已完成但尚未写入 manifest 的内容地址文件沿用孤儿安全回收，不盲删可能已经被旧 manifest 引用的同名输出；manifest 一旦发布，后置阶段失败也不能删除输出。同步期间只并行必需的 history/event，idle balance-trace 和 section-bloom 的既有顺序保持。

新增可选 ParallelHistoryEventReady 回调，生产使用有新鲜度和真实 CPU 空闲证据的快速探针，nil 拒绝并行。它与吞吐同步模式、至少八个 Go 调度线程、非空共享 gate、健康的低延迟存储条件共同决定准入。

可观测指标位于 state/snapshot/cold/history/event_parallel/：attempts 是实际启动双构建的累计次数，builds 是其成功统一发布并完成阶段写入的次数；last/paired、last/build_wall、last/history、last/event 描述最近一次获准历史构建，含失败，不由普通延期轮询覆盖。时间单位纳秒；history/event 可重叠，build_wall 是内层完整构建墙钟。并行会增加两个有限读取/写入流的重叠，不能描述为仅增加 CPU 而完全不改变磁盘并发。

验证：真实 Pebble 同输入串并行生成文件校验和及历史/事件查询相等；阻塞源证明两边实际重叠且只有一个维护 lease；失败与取消时必须 join、不得发布或裁剪；重试能恢复，已存在文件不被误删；race 检查独立 source/结果所有权。固定相同输入计时完整 builder 与统一发布，分别报告阶段与总墙钟，验证之后才决定是否部署。
