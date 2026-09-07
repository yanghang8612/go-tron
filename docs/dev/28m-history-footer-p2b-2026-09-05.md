# 28M 同步优化：P2b 历史压缩文件一次写成

2026-09-05。已将最终修复版部署主网并启用新格式，完成新旧文件完整校验、回退重启及连续观察。相同输入的数据盘文件基准总耗时下降约 19%，收尾下降约 75%；本轮没有证明整链吞吐提升。没有固定真实 28M 起点备份，不能把不同高度窗口或文件微基准当作整链 +50% 验收。

## 改动与一致性

旧格式先写压缩 body，收尾时再复制到带前置索引表的输出文件。新外层格式 version=2 使用固定 48 B header、尾部逻辑索引和 48 B trailer；可回填的首逻辑 chunk 保留在内存，最终追加在 body 末尾。压缩数据顺序写一次，逐字节累计原 SHA-256，Sync/Close 后 rename 同一临时 inode 到原 staging 输出路径。逻辑历史、字典、索引、as-of 和最终 manifest 发布语义不变。

`GTRON_HISTORY_COMPRESSION_FORMAT` 未设或 1 保持旧写入，2 启用新写入，非法值报错。读取器无条件识别两种格式；Reset 沿用构造时选择。新格式校验边界、溢出、记录数、逻辑顺序及完整物理覆盖，只允许首 chunk 移到物理末尾。整文件 checksum 和语义覆盖仍由既有完整校验器负责，没有取消任何校验。

## 证据与验证

部署前 p2next-before 的 25.13 s CPU 样本为 57.23 CPU-s：pointReadCursor.View 11.47、runPartition 10.15、预取 5.42、SnapshotLifecycle 7.32、BuildStateDomainChangeHistory 3.47、旧 finalize 1.37 CPU-s。外层 HTTP 窗口为 28.485 s，覆盖 41,717 tx / 608 个 session blocks；其时间边界与 CPU profile 不同。14:17 UTC 冷历史发布 28,236,931→28,237,776，846 blocks / 97,784 tx，总输出 256,385,507 B，总计 12.595 s；大型压实的重复 I/O 更显著，不能由小段平均推出收益上限。

本地验证：

- 完整 `go test ./core/... ./actuator/... ./vm/...` 通过；随后 `GTRON_HISTORY_COMPRESSION_FORMAT=2 go test ./... -count=1 -timeout=300s` 全仓复验通过。
- 新格式模式完整 snapshots 通过，pruning/freezer 回归通过。
- 新格式随机写/跨 chunk ReadAt/全量还原、SHA-256、同 inode、空文件与首块边界；两种格式混合压实、生产 reader 和空间检查通过。
- 15 类损坏/溢出拒绝；取消前、flush 后取消、关闭文件、rename 失败均不替换旧目标，临时文件清理通过。
- 最终相关 race 两轮通过；增量 lint 0 issues；format=2 环境下系统测试 79 passed、0 failed、0 skipped。

修正过两类测试工具问题：旧损坏夹具按 v1 header 偏移定位，改为真实 reader 的 chunk 表；Linux 第一次测试命令误写 `./core/pruning`，改为 `./core/state/pruning` 后整组复验通过。原始失败日志保留，未混同为运行期故障。

## 相同输入文件基准

Apple M1 Max，10 次/组，三轮中位数；输入一半可压缩随机字节，4 个压缩 worker，包含真实写入和 fsync。

| 输入 | v1 总耗时 | v2 总耗时 | v1 收尾 | v2 收尾 | 每文件少复制 body |
|---|---:|---:|---:|---:|---:|
| 512 KiB | 8.440 ms | 8.360 ms | 5.910 ms | 5.990 ms | 200,763 B |
| 8 MiB | 24.649 ms | 21.905 ms | 12.451 ms | 9.677 ms | 4,213,177 B |
| 64 MiB | 118.729 ms | 93.156 ms | 37.556 ms | 12.220 ms | 34,173,560 B |

64 MiB 总耗时约下降 21.5%，收尾约下降 67.5%；小文件收尾未改善。新格式只多 48 B trailer，表项大小和压缩内容相同。避免复制的字节表示少一次临时 body 读取和一次输出写入，不能等同实际设备物理 I/O 或线上同步加速。

服务器 Go 1.25.5 / linux amd64，GOMAXPROCS=2，同数据盘 `/data`（两个目录的设备号均为 66304），TMPDIR 显式放在本轮 stage 的 bench-tmp 中。10 次/组、三轮中位数，64 MiB v1 总耗时 489.734 ms、收尾 203.506 ms；v2 总耗时 398.664 ms、收尾 50.302 ms。观察到总耗时下降约 18.6%、收尾下降约 75.3%。同机主网服务与后台 I/O 仍运行，这不是隔离硬件的严格因果基准。初次未设置 TMPDIR 的结果另存 `linux-bench-default-tmp.txt`，不将其充作数据盘结果。

## 发布与回退约束

服务器 stage `/data/gtron/history-footer-20260905`，基于隔离的 P2c2 source；七文件补丁压缩后 SHA-256 为 `c6a4ca0c7a8ad98faab8ce04b35e90038cf54b45d239c93f321c2a0a2a76ab76`。

专属配置 `99-history-footer-p2b-20260905.conf` 会固定新二进制和此前 64/32 commitment、overlap=0、async depth=4 等资源配置。所有切换使用 start.lock，暂停并恢复 deploy timer，要求 clean stop、实际二进制及环境一致，重启后有 peers 且高度推进至少 32。

一旦启用新格式，最低回退版本必须能读 version=2。最终修复版回退入口为 `switch-format2.sh 1 <唯一小写记录名>`：保留新二进制，只关闭新格式写入。不能再删除本轮专属 override，或直接退到 P2c2 及以前不支持 footer 的程序。

本地原始证据位于 `build/benchmarks/20260905-p2b`。读取器于 15:13:24 UTC 完成上线，head 29,617,222→29,617,257；format=2 于 15:20:30 UTC 启用，15:21:14 完成健康检查，head 29,624,511→29,624,552。初次已发布的 v2 历史文件为 1,146,902,152 B；写入计数显示完成 1 个压缩流，少复制 1,146,543,393 B body，收尾累计 1.080 s。该计数反映 staging 完成，另行通过生产 manifest 确认了实际发布。新旧两组三件套共六个文件通过 `VerifyLoadedManifestFiles`，启用 RequireRegistered、RequireChecksums 且使用默认完整语义校验器；先从生产 manifest 取引用，再创建硬链接固定本次输入，防止后台退休清理干扰。实际选中 v2 history 391,506,091 B、index 503,749 B、accessor 9,620,198 B；v1 相应为 385,035,820 / 504,050 / 9,274,654 B，总计 796,444,562 B。校验源码与 JSON 结果均在服务器 stage 归档。随后完成带新文件的回退重启及最终观察。


## 同二进制旧写入现场对照

reader1 连续三分钟 7/7 点 active：29,620,265→29,623,933，1491.456 tx/s、20.431 blocks/s、72.998 tx/block、energy/tx 9,072.106、VM share 0.3433；滚动 commit 中位 66.691 ms/block、236.162 µs/update，debt 末值 94.88 GiB。其 25.14 s CPU 样本为 41.79 CPU-s，外层 27.755 s / 40,322 tx，不能直接等同完整窗口的每 tx CPU。


## 跨文件系统补充修复

上线后的复核发现，blob 便捷入口沿用调用者给定的 scratch 目录；当历史子目录挂载到另一文件系统时，footer 最终 rename 会返回 EXDEV，旧复制路径则能成功。Linux 上以普通临时目录→`/dev/shm` 复现：format=1 通过，原 format=2 失败并报告 invalid cross-device link。

修复将该入口的 stream 临时文件放入输出文件的父目录；主流 streaming 构建入口原本已这样处理。真实跨文件系统用例再校验两种格式的完整逻辑内容，纳入 Linux snapshots 与 race 回归。本地没有 `/dev/shm` 时明确跳过该硬件条件，不能据此称跨盘测试在 macOS 运行过。新旧 reader、常规历史 streaming 及数据盘基准路径均未改变，无需重跑整个硬件基准来验证该路径修复。

最终补丁共八个文件，基于本轮开始时的 P2c2 source；压缩 patch SHA-256 为 `8b0b1cbd52178eaf1351c2d743f217baed15d510bee7a3cebf450c864e374f7b`。初版七文件 patch 和测试结果保留作为首次发布记录。


## 最终版本与重启记录

初版 release `/data/gtron/releases/20260905-p2b/gtron`，SHA-256 `417b0d247f6f207c7d16c3ce0ee42d7d1b866f7070f84c00f03723a8468494fb`。有已发布 v2 文件后，同二进制回切 writer=1 于 15:26:39 UTC 完成，head 29,629,130→29,629,163；footer staging 计数为 0，旧写入开关不影响新文件读取。再开启 writer=2 于 15:29:34 完成，head 29,630,650→29,630,687。所有切换均 clean stop，服务与 timer 恢复 active。

跨盘修复版 release `/data/gtron/releases/20260905-p2b2/gtron`，SHA-256 `d39f8847428a80833a045db4f8419f1d8c77c2525223db96e23c700e012e995f`；reader 代码与初版相同，增加的 runtime 改动仅为 blob staging 位置。最终八文件 patch 在本地与服务器重新生成，压缩文件 SHA-256 完全相同。修复后 Linux 新格式完整 snapshots 23.731 s、相关 race 8.700 s 通过，跨盘两个子用例均实际执行并通过；本地相关回归及增量 lint 0 issues。

最终版仍通过同一个 `99-history-footer-p2b-20260905.conf` 固定。`switch-format2.sh`、`candidate2.conf`、完整八文件 patch、source hashes、交叉验证源码和实际配置保存在本轮 stage 与 release。初版 reader 仍具备读取新格式的能力，但最终维护回退应使用修复版的 writer=1 入口。最终六分钟同步观察已完成，数据与限制见下表。


## 最终线上窗口与结论

修复版于 **15:41:21 UTC** 完成部署，head 29,639,381→29,639,414。六分钟观察覆盖 29,639,416→29,644,190，**13/13 点 active**，进程始终为 PID 20748。RSS 从启动预热期约 6.02 GiB 增至 16.12 GiB。

| 指标 | 同二进制逻辑的 writer=1 对照，三分钟 | 最终 writer=2，完整六分钟 | writer=2 后三分钟 |
|---|---:|---:|---:|
| tx/s | 1491.456 | 1237.111 | 1399.639 |
| blocks/s | 20.431 | 13.233 | 15.632 |
| tx/block | 72.998 | 93.483 | 89.539 |
| energy/tx | 9072.106 | 10187.716 | 8584.667 |
| VM 交易占比 | 0.3433 | 0.3883 | 0.3650 |
| 滚动 commit ms/block 中位数 | 66.691 | 113.739 | 91.850 |
| 滚动 commit µs/update 中位数 | 236.162 | 277.064 | 261.762 |
| 末值 compaction debt GiB | 94.88 | 94.98 | 94.98 |

对照使用初版 P2b，新窗口使用只修复 blob staging 位置的 P2b2；reader 和常规 streaming 路径相同。高度、交易组成、缓存预热、后台压实均不同，无法作严格因果比较。新窗口的同期 tx/s 反而较低，不能宣传整链提速；也不能仅由这个差值认定格式重构导致回退。更关键的是，六分钟两端 footer 完成流计数均为 2、避免复制量均为 1,475,216,547 B：**该窗口没有覆盖新的压缩收尾完成事件**，因此不拿这段 tx/s 证明文件重构的收益。

15:53:07 UTC 最终审计：head **29,651,449**，30 peers，active；SHA-256 与最终 release 一致，timer active。fold、pipeline、prefetch、singleflight leader、commitment rebuild、cold snapshot 的错误/失败计数全部为 0。该进程此时已完成 **6 个 footer 压缩流**，累计避免复制 **2,262,290,464 B（约 2.26 GB）**，收尾累计 3.347 s。计数指已完成 staging 流，实际发布另由生产 manifest 与完整三件套校验证明；避免复制量不等同磁盘物理读写计数。Java PID 16568 保持运行，canary 未启动，仅主网一个 gtron。

最终 CPU profile 为 25.10 s / 46.07 CPU-s，外层 27.715 s 覆盖 36,154 tx；pointReadCursor.View 8.93 CPU-s、runPartition 7.51 CPU-s。旧写入样本相应为 9.51/8.67 CPU-s，绝对总 CPU 和工作构成不同。commitment 读取、提交与冷历史长期服务率仍是下一轮应解决的问题。15:52:54 UTC 冷历史发布到 28,318,044，仍落后 eligible cutoff 约 1.267M；本轮未证明长期 backlog 收敛。

保留 writer=2 的依据是同输入文件基准、实际避免二次 body 复制及兼容性验证；代码默认仍为 writer=1，不将现场配置推广为全局默认。整链 +50% 和所有负载形状的回退门槛仍未通过。

服务器最终 release 已归档 sources.tar.xz、完整 patch、source hashes、observations.tar.gz、三个窗口汇总、CPU 以外的原始指标/进程样本、完整文件校验结果、最终服务配置及 ROLLBACK.txt。CPU 原始 profile、本地测试和基准位于本地 build/benchmarks/20260905-p2b，附 SHA-256 索引。诊断用六文件硬链接保留在 stage/published-audit，总计约 796 MB，不是可恢复整条链的 28M 数据库备份。


收尾连接情况：源码包、观测包、校验结果与实际配置已在服务器完成归档并读回校验和；其后 Chrome 跳板机报处理错误，本地 SOCKS5 127.0.0.1:1088 也停止监听，最后一次额外 HTTP 复核失败。最后成功的完整健康审计仍是 15:53:07 UTC（北京时间 23:53:07）。这不证明主网服务停止，但不能声称代理断开后的即时状态已核验。额外的服务器 artifact-index.json 补写未确认执行；本地证据目录的 SHA-256 索引已完成，服务器原始归档不受该补写影响。
