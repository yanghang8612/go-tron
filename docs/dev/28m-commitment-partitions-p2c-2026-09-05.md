# 28M 后续：Commitment 二层分区与有界读取（P2c）

本轮在用户明确授权的主网同步节点直接验证，测试范围为 29.4M–29.52M；没有得到一致的完整 28M checkpoint，因此下面的线上窗口不是同块重放 A/B，不能据此声称整链 +50% 已实现。canary 保持停止。

最终主网保留第二版 **64 个有序分区 / 32 个 durable cursor 名额**，代码默认仍为 16 分区。重复回切验证中，负载较接近的 180 秒窗口观察为 1251→1369 tx/s，commit 从约 432→289 µs/update；这是现场保留该模式的依据，未达到固定输入整链 +50% 或全部形状回退 <3% 的正式验收。

## 实现

第一版把 16 个首 nibble owner 拆成 64 个第二层分区，每个分区管理连续 4 个第二 nibble 子树。跨块顺序由持久 owner 保证，各分区只写深度 ≥2 的互斥 branch 前缀；深度 1 的 branch 在 job join 后通过原 `linkChild` 合成、collapse，最后写原格式 root。每个分区持有自己的部分父节点，不读取尚未完成合成的中间父行。启动会验证根和首层 hash child。空更新通过私有 completion 继承前一根，仍向本层写 root marker。

第一版的 64 foreground + 16 prefetch 在机器上未显示稳定收益。第二版将逻辑 owner 数与物理请求数分离：`GTRON_COMMITMENT_PARTITIONS=64` 配合 `GTRON_COMMITMENT_READ_CONCURRENCY=32`，让所有 block/session 共享 32 个 durable cursor 名额。overlay/cache hit 和 singleflight follower 不取名额，foreground 与 prefetch 的 leader 在底层 Cursor.View 外取名额并在正常返回、错误、panic 时释放。原 16 条预取流、深度、每 lane 预算不变。复用 job 根/统计工作区，并把有限 scratch pool 上界匹配到 162 个 reader，避免常规 64 分区 session 的 scratch 每块被弃用。

默认 `PARTITIONS` 未设置或 `16` 保持原通道；第二版中 64 模式默认 read budget 32，显式 `READ_CONCURRENCY=0` 可复现无限额模式。非法分区值、64 模式下非法读取限额报错。`state/commitment/pipeline/{partitions,read_concurrency}` 标记实际模式；`read_concurrency=0` 表示没有显式预算，不表示停止读取。same-nibble critical wait 在 64 模式由多个分区共同等待，累计等待量不能直接与 16 模式比倍数。

不改变 schema、branch/root 编码、链规则、事务验证、history/as-of 语义、snapshot cut、FIFO publication、inflight 深度或端口。此次无需数据迁移，旧版仍能读取所有新写入。

## 验证

- 所有块 root 和最终完整 branch bytes 对照；4 个 inflight block、Pebble flush 后继续、删除到单叶/空树再插入、pipeline 重启、immutable base/frozen delta、prefetch overlap 两种模式。
- 首层损坏拒绝启动；故障注入验证 predecessor 读取失败能释放等待根的空块，所有 owner 退出并清空 inflight。
- 读取名额耗尽时缓存/overlay foreground 和 prefetch 仍完成；失败与 panic 不泄漏名额。
- 第一版与第二版均完成本地 core/actuator/VM 回归、相关 race 两轮、增量 lint 0，以及系统测试 79 passed / 0 failed / 0 skipped。全仓既有 lint 问题并未在本轮全部清理。
- 第一版 Linux Sapling：domains/blockbuffer/pebbledb 通过，race 两轮通过；补充故障 race 三轮通过。第二版 Linux Sapling同包回归与包含 budget 测试的 race 两轮通过。

固定基准使用实际 Pebble SST、32,768 个种子，每块指定更新量，逐块 fold/promote/flush；关闭预取以隔离折叠容量。额外 250 µs 是 cursor 延迟模型，实际睡眠受系统调度影响，并非 EBS 实测。

| M1 三轮中位数 | 16 通道 | 第一版 64 | 第二版 64 / 32 读取 |
|---|---:|---:|---:|
| 512 更新，250 µs 延迟 | 26.94–27.38 ms | 10.08 ms | 13.64 ms |
| 16 更新，250 µs 延迟 | 2.98–2.99 ms | 1.91 ms | 1.90 ms |
| 512 更新，无延迟 | 2.75–2.82 ms | 3.12 ms | 3.19 ms |
| 16 更新，无延迟 | 0.203–0.217 ms | 0.236 ms | 0.258 ms |
| 空更新，无延迟 | 18.3–20.3 µs | 7.0 µs | 6.8 µs |

表中 16 通道范围来自两次独立、各三轮完整基准。第二版延迟模型 512 更新约 2×，但 CPU/常驻数据场景仍回退，轻载最差约 27%，不满足全部形状 <3% 门槛，不能全局默认启用。第二版延迟模型 512 更新分配从第一版约 880 KB/block 降至 831 KB/block；这不是整链内存用量。

Linux Sapling / GOMAXPROCS=2 两轮、每项 30 次的第二版基准，512 更新延迟模型为 87.628/88.066 ms → 44.923/44.801 ms；无延迟为 5.977/5.944 ms → 6.388/6.748 ms。仍呈现相同的 I/O 延迟收益与常驻数据回退，不能推导主网倍数。

## 主网窗口

P2c 的前四组重启后分别采集 19 个点、间隔 30 s；下表取后段 180 s。最终重复确认组为 13 点、六分钟，取后三分钟。第一版最后窗口含短暂补充测试的影响，不能作为严格因果比较。第二版 Linux 构建等到 control 第 19 个采样文件产生后才开始。tx/s、tx/block 使用 session counter 差值；energy、VM 占比和 state commit 指标为窗口内滚动 gauge 的采样中位数，不能把表中数值彼此相乘还原精确总量。

| 指标 | P1b 初始基线 | 第一版 64 | 同二进制回切 16 |
|---|---:|---:|---:|
| 高度区间 | 29,463,953–29,466,059 | 29,487,546–29,488,811 | 29,491,970–29,493,581 |
| tx/s | 1516.619 | 1236.897 | 1249.583 |
| tx/block | 129.310 | 178.437 | 140.649 |
| energy/tx | 11130.750 | 9080.219 | 10423.665 |
| state commit ms/block | 123.548 | 199.004 | 181.403 |
| commit µs/update | 261.755 | 368.743 | 363.661 |
| parent seek/block | 444.455 | 601.269 | 501.183 |
| parent block read ms/block（各线程累计） | 1102.231 | 2637.912 | 1626.829 |
| async backpressure ms/block | 2.684 | 4.559 | 2.426 |
| compaction debt GiB | 82.33 | 82.61 | 85.41 |

第一版 64 的单次读取等待上升，回切后 tx/s 也未恢复到早先 P1b 窗口；交易组成和机器/后台状态有漂移。第一版不能认定有主网收益，已退出该模式。

| 第二版同二进制顺序对照 | 64 分区 / 32 读取 | 回切 16 分区 | 再次切回 64 / 32 |
|---|---:|---:|---:|
| 高度区间 | 29,506,798–29,509,663 | 29,514,100–29,515,751 | 29,518,401–29,520,154 |
| tx/s | 1540.121 | 1250.749 | 1369.429 |
| tx/block | 96.325 | 135.401 | 140.151 |
| energy/tx | 9120.475 | 8581.638 | 9632.677 |
| VM 占比 | 33.1% | 35.5% | 37.1% |
| state commit ms/block | 82.245 | 205.831 | 135.688 |
| commit µs/update | 223.697 | 431.732 | 289.437 |
| parent seek/block | 302.278 | 483.611 | 471.085 |
| parent block read ms/block（各线程累计） | 895.949 | 1790.563 | 1463.452 |
| async backpressure ms/block | 2.966 | 10.744 | 1.953 |
| compaction debt GiB | 86.85 | 87.63 | 82.82 |

两组完整九分钟均为 19/19 点 active。第二版 64/32 的观察吞吐较回切 16 高约 23%，但 tx/block、每次更新的读取数量和存储后台状态不同，仍不构成严格加速比。两组九分钟的 debt 分别为 86.245→86.851 GiB、85.766→87.630 GiB，没有证明后台债务长期收敛。

第二版两份 CPU profile 的请求外层窗口分别覆盖 37,709 与 37,158 笔交易，CPU 采样本身为 25.17/25.15 秒，总量 43.32/42.60 CPU-s；前台 fold owner 8.74/6.82 CPU-s，prefetch 4.56/4.90 CPU-s，Pebble compaction 5.28/3.75 CPU-s。64 分区没有减少这两份样本的总 CPU，不能把墙钟改善解释成总体工作量减少。root 合成开销未成为主要热点。

再次切回 64/32 后六分钟 13/13 点 active。后三分钟的 tx/block 比 16 对照多约 3.5%，energy/tx 高约 12.2%，VM 占比高 1.6 个百分点；观察吞吐高约 9.5%，commit µs/update 低约 33.0%，实际 async 排队累计低约 81.8%。第三份 25.14 秒 CPU 样本为 36.73 CPU-s，外层覆盖 38,388 笔交易，fold owner 7.99、prefetch 4.90、compaction 5.07 CPU-s。上述差值仍受高度、缓存和后台漂移影响，不等于因果加速比。

第三组启动后发生冷历史快照发布（28,187,788→28,188,723）和约 1.233 GB 历史校验，compaction debt 也发生回落。冷历史仍落后执行高度约 1.26M，六至九分钟窗口不能证明长期服务率已经追上。当前按这台节点的 I/O 场景保留有界 64 模式；继续减少随机读次数、编码/复制及后台历史 I/O 才可能取得更大的整体提升，不再扩大无界读取并发。

最终健康核验为 2026-09-05 13:23:44 UTC：PID 17090，head 29,520,504、22 peers、sync active；指标实际为 partitions=64 / read_concurrency=32，commitment 折叠、预取、singleflight leader、物理 branch 读取错误均为 0。RSS 约 16.56 GiB，debt 约 83.00 GiB；部署 timer active，Java PID 16568 仍在，只有主网一个 gtron 进程。第三组六分钟 debt 为 87.450→82.815 GiB，包含后台压实变化，不能视为新调度自身降低债务的证明。

## 发布与回退

服务器通过 Chrome JumpServer shell 操作，HTTP profile 使用 SOCKS5 `127.0.0.1:1088`。使用 `/data/gtron/start.lock` 与 timer 停启互斥；每次要求旧进程 `ExecMainStatus=0`，重启后 active、有 peers、head 至少推进 32 才完成。服务基本 unit、Java 节点和端口未修改。

第一版 stage `/data/gtron/commitment-p2c-20260905`，release `/data/gtron/releases/20260905-p2c/gtron`，SHA256 `90327ad63bf91a9351afe851595e7850734a9d76b0f3458c5018f29256e937b9`。64 模式于 12:17:45 UTC 部署完成（29,483,705→29,483,745）；12:30:40 UTC 回切 16 完成（29,489,528→29,489,562）。override `98-commitment-partitions-p2c-20260905.conf` 当前为该版本的 16 通道配置。

第二版 stage `/data/gtron/commitment-p2c2-20260905`，release `/data/gtron/releases/20260905-p2c2/gtron`，SHA256 `e6319739e9e39dec8c8e7cdbefd2df7917e8ad7c30b67a4fb4345c77c94de244`；override `99-commitment-budget-p2c2-20260905.conf`。12:52:24 UTC 首次部署完成（29,501,595→29,501,628）；13:04:22 UTC 回切 16 完成（29,510,942→29,510,980）；13:17:01 UTC 再次切回 64/32 完成（29,517,217→29,517,250）。最终保留该版本的 64/32，overlap=0，async depth=4，其余资源和端口保持原配置。

第二版的 `switch-partitions.sh 16 <新的记录名>` 可在同二进制切回 16；`rollback-mainnet.sh` 在持锁、暂停 timer、停止节点后撤除该专属 override，回到第一版 16 通道；第一版的回退脚本再撤除 98 可回到 P1b。只能在排空当前写入并完成停机后使用，不能直接手工删配置与部署 timer 竞争。

所有 release 都由独立 source 目录构建，保留源码 hash、patch、测试/构建日志、前后配置和恢复脚本。正常部署 timer 恢复 ACTIVE 后仍受这些 ExecStart override 约束，后续正式发布需显式处理它们。

本地原始基准、profile 和回归日志已归档至 `build/benchmarks/20260905-p2c`，含 SHA256 索引。服务器第二版 release 中 `sources.tar.xz` 是基于独立 P1b source 的九文件 P2c 覆盖层，SHA256 为 `ba0647364097ac2d7ecbf1a9cb4b576964718b751adfb3131b50bb399f1543b6`；原始三组指标在 `observations.tar.gz`，最终配置在 `final-override.conf` / `final-running-config.txt`，健康状态在 `final-audit.json`。这不是链数据库备份。
