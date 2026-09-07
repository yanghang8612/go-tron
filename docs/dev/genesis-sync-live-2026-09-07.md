# 固定容量主网从创世同步实测

用户在删除原数据库后授权从创世同步，并要求代码先提交 GitHub、服务器 pull。本报告记录 2026-09-07 实际部署与本轮验收；没有扩容、迁移 MySQL 或缩短历史保留。

## 部署身份

- 源码提交：`f6ef2e03a9073baa86c335b3081f67b96fe8ff4c`，已推送 `origin/master`，服务器 `/data/gtron/go-tron` 成功 `git pull --ff-only origin master`。保护脚本另在 `a3b439d786a9a7d535ba0674206324d8b6f162a2` 提交并拉取。
- 服务器原有 Rust 子模块修改和 `solc/` 保留；从确定提交归档到 `/var/tmp/gtron-genesis-20260907/src-f6ef2e03` 独立构建。
- 工具链：Go 1.25.5，Linux amd64，`CGO_ENABLED=1`、`-tags sapling`；原生 Sapling 全仓 `go test -p 4 -tags sapling ./... -count=1 -timeout=300s` 和正式构建均 exit 0。构建前的 Sapling 依赖测试也通过。此次未使用此前本地 CGO=0 交叉编译产物。
- 二进制 SHA256：`1769428aed52b237beda6738ad04ae311583d6ca8ec3f008ef462a976b99f69e`。
- 正式 release：`/data/gtron/releases/20260907-history-cpu-f6ef2e03/`，保存二进制、`SHA256SUMS`、`source-commit` 和 `build-info.txt`。
- 主网启动：**2026-09-07 12:53:18 UTC**，PID `4108`，`process/start/unix_nano=1788785598065582158`。核对了 `/proc/4108/exe` 和各监听的 PID。
- 启动前确认 `/data/gtron/main/datadir/gtron` 为空；API 返回主网创世 BlockID `00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc`。

启动配置保留 `snap` 全历史模式、65,536 块热窗口，显式启用 `--history.enabled=true --history.block-dedup=true --history.compression-format=auto`。日志确认热去重 enabled、冷 historyFormat auto、snapshotEnabled true；旧环境中固定格式 2 已被覆盖。主网 P2P TCP/UDP 18890，HTTP 8090、JSON-RPC 8545、gRPC 50051、pprof 127.0.0.1:6062、metrics 127.0.0.1:6071 均属于 PID 4108。

## 容量与运行保护

服务器 16 核、约 61.3 GiB RAM。共享 `/data` 为 `/dev/nvme1n1` ext4、约 7 TiB，启动前可用 `3,035,448,328,192 B`（约 2.76 TiB）；根分区约 80 GiB，可用约 30 GiB。这些空间包括现有 MySQL、Java 节点及其他服务，不能解释成全部属于 gtron 的预算。

部署了 [主网专用空间保护](genesis-sync-deployment-review-2026-09-07.md)，服务器 Python 3.6 的 14 项 mock 测试通过，实际只读 check 和 timer/service 执行结果通过。每 15 秒检查 data 384 GiB 停止保留线、512 GiB 启动线、根卷 5 GiB 及两卷 5% inode；触线或检查失败只锁存并正常停止 `gtron.service`，不会自动解除锁。首轮主网 `Restart=no`，保留原 600 秒停止超时。

仅精确替换主网维护条件，原全局 `/var/lib/gtron-offline-maintenance.hold` 及 Nile、自动部署 service/timer 三份 drop-in 保留并逐字节比对。启动后确认这三项仍 inactive。没有启动 Nile、修改 MySQL 或执行部署脚本的旧版本回滚路径。

## 采集与验收口径

服务器 `/var/tmp/gtron-genesis-20260907/` 保存原生测试/构建日志、配置备份及 `samples-0001.jsonl`。采集从本次 PID 开始，每 15 秒一次、121 点；包括进程 `/proc` CPU/IO、整盘 IO、容量、内存和冷化/裁剪前沿。采集结束不会停止节点，运行保护独立存在。

本地 `build/benchmarks/20260907-genesis-sync/` 保存直接 HTTP 原始 API/metrics 回应；使用已有 `/debug/metrics` JSON 入口，没有新增公网 metrics 路由。服务器终端展示的配置、构建退出码和 `/proc` 核验是现场观察，不能冒充已下载的原始完整服务器日志。

早期同步以空块和小记录为主。auto 未使用 V3、CDC 计数为零均可能正常。局部合成输入的压缩比例见 [CPU 与历史 I/O 报告](cpu-history-io-2026-09-07.md)；本次不会将其视为实际主网整库空间比例。

已捕获账户 `0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1` 在 2043/2044/2045 的三个固定区块哈希和余额，2044、2045 均发生真实余额变化，采集前后 `published=pruned=0`。详见 [canary 原始证据与边界](genesis-history-canary-2026-09-07.md)。后验必须等待 2045 被真实冷发布及热裁剪后，逐项比较这些固定历史查询；仅区块高度上涨不算冷化验收。

## 原策略实测与发现

12:53:48.378–13:03:03.388 UTC 的 38 点、555.01 秒窗口：head 36,058→489,466，平均 **816.94 blocks/s**；进程平均 **1.875 CPU 核**，采样 RSS 峰值 **1,737.375 MiB**。gtron `/proc/PID/io` 增量读 55,390,208 B、写 2,123,534,336 B；整盘读 18,904,363,008 B、写 8,582,086,656 B，平均队列约 0.625、busy fraction 约 0.204。共享盘净增 777,658,368 B（约 0.724 GiB）。这些数值由服务器原始 JSONL 计算后在终端核对；整盘计数包含其他业务，不能归为纯 gtron IO。

13:05:48.979 UTC 的直接 metrics 回应记录：head 632,386、solidified 632,368，cold published 与 hot pruned 同为 **2,500**，两次成功 forced-busy 构建，cold/prune/lifecycle 错误全为零。随后 13:06:43.340 UTC，在 published=pruned=**3,750** 后，使用原热历史基线执行 canary：2043/2044/2045 的三个区块哈希与精确余额全部一致，原基线 SHA256 未变。原始回应见 `build/benchmarks/20260907-genesis-canary/capture-20260907T130633Z/`。这已证明该账户的余额读取完成一次真实冷热转换闭环。

同时发现持续处理速度不足，不能将首次发布成功当作积压受控：

| 原策略的一个真实完成间隔 | 数值 |
| --- | ---: |
| 每批范围 | 1,250 blocks / 本轮 2,314 txnums |
| 本批 build duration | 167.14 ms |
| 配置的最低恢复 | 30 s |
| 两次完成间隔 | 30.181 s |
| 冷化总速度 | 41.417 blocks/s |
| 净追赶速度 | −808.598 blocks/s |

原因是 busy 强制路径将正常 5,000 块上限除以四，恢复时间又保底为正常一分钟间隔的一半。即使 CPU 和 IO 都有余量，便宜批次也会等待 30 秒；原“自适应”没有根据积压增长扩大批次。约 13:14:51 的发布日志中，published=25,000，而 eligible=1,083,193，积压仍在扩大。

当前正据此修正有界 busy 调度并准备第二轮实测。修复部署后应单列新 PID 的窗口，检查连续冷化/裁剪速度是否高于同期导入，并重跑原历史 canary；不能跨重启混算 CPU/IO 计数。

首轮完整 121 点在 13:23:48.383 UTC 结束，跨度 1,800.004 秒：head 36,058→1,486,372，平均 805.728 blocks/s；进程平均 2.543 CPU 核，采样 RSS 峰值 5,197.489 MiB。冷发布/热裁剪最终同为 46,250，eligible=1,404,690。共享盘可用空间减少 3,355,721,216 B，进程 IO 增量读 645,181,440 B、写 27,581,861,888 B；整盘读 82,125,672,448 B、写 52,892,012,544 B，平均队列 0.880、busy fraction 0.300，采样无缺项或错误。这一完整窗口同样显示积压扩大。

修复新增显式 `--history.catchup-mode=throughput`，默认 balanced 保持原策略；详见 [实测驱动的调度修复设计](../superpowers/plans/2026-09-07-busy-history-catchup.md)。新路径保留完整 5,000 块上限、共享重任务锁和最低空间检查，以完整维护墙钟决定恢复，外层失败与未完成交接均受到约束。定向 race 已通过（snapshots 5.724s、pruning 4.877s），CLI 定向测试、本地全仓 `go test ./... -count=1 -timeout 300s` 及 23 项 Python canary/采样/保护测试均通过；独立只读审查无剩余阻断。

## 初期冷段的实际空间口径

在线只读取已发布不可变文件的头和 footer，没有另开数据库。manifest generation 23、hotPruneBlockNum=28,750 的状态历史有 **23 个 V2 容器**；V2 逻辑编码流合计 **9,358,863 B**，容器物理大小合计 **1,877,965 B**，后者约为前者 **20.07%**。这些字节只比较当前 V2 容器与其逻辑编码流，不包含热库、其他数据域、索引、WAL、临时文件，也不是与旧版的新增节省比例。footer 的 recordCount 属于容器写入单位，不作为状态变更条数使用。

首次目录抽查约为 chaindata 4.4 GiB、state-snapshots 3.1 MiB；由于积压尚未回收，大部分历史仍在热库。早期实际选择 V2 与 auto 的小值策略一致，CDC 文件计数仍为零，后期巨大委托列表的 V3 收益尚未被本轮早期数据覆盖。

固定磁盘能否最终容纳全链，尤其后期大委托历史，需实际推进到相应业务阶段验证。
