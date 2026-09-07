# 28M 同步优化 P2a：Commitment 预取与快照清单

状态：P2a 已部署并完成本轮验证。正式配置关闭预取重叠，保留清单优化和预取路径所有权修复；整链大幅提速尚未获得固定输入证明。

## 变更

- 预取只持有独立的 `[]common.Hash` 路径镜像，修复 `livePutsInPlace` 整理删除操作时与 lookahead 并发访问同一数组的竞态。该问题在 overlap=0 和 1 的真实 Pebble 测试中都复现过。路径数组全部读取结束后回收；单个 pool backing 最大保留 2 MiB。
- `GTRON_COMMITMENT_PREFETCH_OVERLAP=1` 允许前台折叠与首层预取同时工作，同键 durable read 由原 session singleflight 合并。默认开关关闭，可使用同一候选二进制对照。逐 lane 跨块顺序、inflight 限额、session 关闭、发布顺序、状态根和分支编码保持原语义。
- manifest 校验复用路径索引定位历史分片的 index/accessor，保留所有身份/范围/aggregationSteps 谓词。event coverage 改为单向游标，保留连续性检查。没有持久化格式变更，也没有跳过文件内容校验。

## 证据

主网 P0a 05:48 UTC 的 30.17 秒样本共 48.59 CPU 秒：SnapshotLifecycle 10.94 秒（22.51%），Manifest.Validate 4.10 秒（8.44%），其中 event coverage 2.43 秒。05:50 UTC 第二个 25.11 秒窗口的相同校验为 0.75 秒，显示背景维护是间歇性负载。

78 秒分项差值：depth-5 预取 durable/useful=62,331/61,711，depth-6-plus=51,515/50,459。不能据此直接关闭预取。Overlap 开启后，一些键会改由前台首先加载，useful-prefetch 比率不能单独作为读放大指标，应一起比较总 seek、读字节和 singleflight。

本地完整 Manifest.Validate 基准（Apple M1 Max，旧源码 overlay 对照，3 次、每次至少 500 ms）：

| 分片数 | 原中位耗时 | 新中位耗时 | 加速 | 内存 |
|---|---:|---:|---:|---|
| 2,560 | 6.574 ms | 1.437 ms | 4.57× | 均约 1.26 MB / 3,704 alloc |
| 10,240 | 86.281 ms | 5.771 ms | 14.95× | 均约 5.69 MB / 14,527 alloc |

主网部署前 180.1 秒完整窗口（29,227,873 → 29,229,770；session 工作计数允许包含提交中的领先部分）：

| 指标 | P0a |
|---|---:|
| blocks/s | 10.658 |
| transactions/s | 1,018.881 |
| tx/block | 95.598 |
| energy/s，中位 | 11,827,306.755 |
| VM share，中位 | 54.7% |
| transaction ms/tx，中位 | 1.005 |
| stateCommit ms/block，中位 | 66.901 |
| fold wall ms/block，累计差值 | 60.452 |
| parent seek/block | 267.23 |
| 预取整批等待，所有 lane 合计 ms/block | 273.308 |
| async enqueue blocked ms/block | 0.004 |
| compaction debt，窗口末 | 77.89 GiB |

因此当前提交队列不是主要瓶颈。局部时延下降是否释放 IO/CPU 并提高执行效率，必须由部署后工作量指标验证。

## 验证

- 本地 `go test ./core/...`、补齐 offline replay 审计入口后的 `go test ./core/rawdb`、最终 `make test` 全仓回归全部通过。
- 本地 domains/blockbuffer targeted race 通过。
- overlap=1 的 core async/reorg/rotation 及 ordered pipeline race 通过（core 84.576 秒、domains 12.170 秒）。
- 真实 Pebble 的 24 块、4 个 inflight、删除/重插入、每批落盘切换，对比旧调度、新调度及顺序实现的根和全部分支字节。
- 受控阻塞预取验证：前台可继续读取；发布与关闭仍等待预取结束。
- manifest 身份字段变异、重复路径、连续覆盖缺口、随机 coverage 对照及 uint64 上边界测试通过。
- 增量 golangci-lint：0 issues；git diff --check 通过。
- 最终 `GTRON_COMMITMENT_PREFETCH_OVERLAP=1 bash scripts/system_test.sh`：79 passed，0 failed，0 skipped。系统脚本本身使用 dev 节点；主网 async pipeline 的并发验证以真实 Pebble/race 测试为准。

## 部署与回退

远端工作目录：`/data/gtron/commitment-20260905`，隔离源码 `src/` 基于先前已部署 P0a 的构建目录。

正式候选：`/data/gtron/releases/20260905-p2a/gtron`。已以 `95-commitment-p2a-20260905.conf` 覆盖 ExecStart，继承 P0a 的所有端口、datadir、GOMEMLIMIT=10GiB、async depth=4。原 `90-state-p0a-20260905.conf` 保留。

补丁 SHA-256：`e2f0bfd99671f318123ef190090342fa8ac26be30d2f294843a1ff1852a6703e`。测试包 SHA-256：`ef442b081337a939e3abb4145cfc1afb1a9f4d115dfdd7512be1533cae69362f`（服务器测试副本仍为上传版；本地另将两处 defer Close 写成显式忽略错误，测试逻辑不变）。

不部署上一阶段的离线 replay CLI，本地相应的审计白名单补齐也仅用于工具回归。本任务没有对 Java/canary 执行停机或配置变更；canary 后来在 07:04 退出，详见环境干扰记录。

Linux 普通 Sapling 包回归已通过：domains 1.474 秒，snapshots 23.515 秒。GCC 4.8.5 首次构建 race 版 secp256k1 持续超过 13 分钟，主动终止了本次测试/编译器进程树，改用服务器已安装的 GCC 11 运行 race；发行版仍用 CC=gcc 和原有 Sapling 参数。没有终止任何节点进程。

GCC 11 的 Linux race 检查已通过（两次重复，3.614 秒）。正式 Sapling 二进制于 06:28:10 UTC 编译完成，部署操作已启动。

06:30 UTC 首次部署被每分钟运行的 `gtron-deploy.timer` 干扰：start.sh 在主网停机期间执行异常恢复，取消了原 stop job，部署脚本自动回退。重试时取得 `/data/gtron/start.lock` 并暂时停止 timer，主网干净退出后切换；06:39:04 UTC 健康检查通过（29,242,502 → 29,242,536，11 peers），随后恢复 timer。

运行 PID 2654，`/proc/2654/exe` SHA-256 为 `15d626c176d024b7ab93ee65c406cd5fee744c80d9f1d88212cde9fa56cde307`。确认环境 `GOMEMLIMIT=10GiB`、`GTRON_ASYNC_COMMIT_DEPTH=4`、`GTRON_COMMITMENT_PREFETCH_OVERLAP=1`。

回退入口：`bash /data/gtron/releases/20260905-p2a/rollback-mainnet.sh`。脚本使用相同部署锁，暂停/恢复 timer，仅移除本阶段 95 override，并启动保留的 P0a。未执行该最终回退脚本；语法检查通过，首次失败部署的同类回退已实际恢复过 P0a。

重启后的 06:41 UTC 25.18 秒 CPU 样本：50.69 CPU 秒，其中历史压实源校验 22.10 秒（43.60%），SHA-256 写入 15.68 秒（30.93%），Pebble compaction 11.40 秒（22.49%）。本窗口正在处理 256 个分片、125,349,718 条历史记录，不能与部署前常规 forced-busy build 窗口直接作吞吐因果比较。源校验约 2m57s 完成，然后进入 collect-keys/build-dictionary。

## 第一轮 overlap=1（含大型压实）

720.6 秒完整窗口，29,242,516 → 29,246,626：5.675 blocks/s、563.647 tx/s、99.327 tx/block。去除最初 6 分钟后的 360.3 秒窗口：6.483 blocks/s、626.866 tx/s、96.690 tx/block；交易 1.650 ms/tx、stateCommit 106.799 ms/block，fold wall 89.575 ms/block，parent seek 287.909/block，parent block-read 合计 126.951 ms/block，随机 SST 读 2,079,056.894 B/block，压实债务 79.19 GiB。整批预取等待为 0，但 job 完成等待 0.480 ms/block；消除等待指标本身不等于消除 I/O 延迟。

这一轮并未证明整链吞吐改善，背景压实负载与 P0a 基线不同。准备使用同一二进制的 overlap=0 对照，保持清单优化、竞态修复及所有其他参数不变。模式切换工具为 `/data/gtron/commitment-20260905/switch-overlap.sh 0|1`，同样使用部署锁和健康门槛。

06:58:45 UTC，大型历史压实完成（19m22.628s，125,349,718 records，37,338,003 remap rows），而后才采集 onclean 窗口并做开关对照，避免中断后重做。manifest generation=9254，共 18,550 个分片：event-log 和 event-log-index 各 9,218，state-domain-change history/index/accessor 各 38。

较后的 aftertail 360.3 秒窗口（29,246,825 → 29,249,940，压实 finalize/accessor 阶段）为 8.703 blocks/s、705.519 tx/s、81.064 tx/block、transaction 1.516 ms/tx、stateCommit 74.638 ms/block、fold wall 66.852 ms/block、parent seek 225.864/block、随机 SST 读 1,753,572.820 B/block、债务 83.4 GiB。交易形状及后台阶段差别仍须保留，不能拿此窗口与 P0a 作严格加速比较。

## 大型压实后的模式对照

onclean（overlap=1）：180.1 秒，29,251,379 → 29,253,423，11.371 blocks/s、849.520 tx/s、74.709 tx/block；交易 1.103 ms/tx，stateCommit 63.412 ms/block（187,040.182 ns/update），fold wall 58.002 ms/block，parent seek 216.076/block，parent block-read 79.450 ms/block，随机 SST 读 1,610,621.121 B/block，顺序 SST 读 2,037,072.657 B/block，债务 84.67 GiB。对应的 25.10 秒 CPU 样本中 Manifest.Validate 为 0.17/35.80 CPU-s（0.47%），event coverage 为 0.02 CPU-s。

完成此窗口后，使用同一二进制切换 overlap=0，等待预热后再比较。原先额外的 steady 采样在重启前终止，避免将重置前后的 session counter 混在一个统计窗口。

### 必须剔除的环境干扰

关闭模式的吞吐回升不能与 onclean 直接作开关归因。07:04:03 UTC，原 canary PID 23853 在 6,568,760 干净退出；off-00-process.txt（07:05:16 UTC）起已无该进程。它未达到原 `--sync.stop-at=20674403`，用户随后确认 canary 由其主动停止；该确认不构成软件提速证据。本任务没有对其执行停止命令；Java PID 16568 仍在运行。采样发现这一点后，撤销“关闭开关导致提速”的初步解释，在 canary 已停止的条件下补跑开启模式。

第一轮 off 的最终 180.2 秒窗口（排除前 6 分钟，29,259,439 → 29,261,993）：14.209 blocks/s、1,479.963 tx/s、104.157 tx/block；交易 0.599 ms/tx，stateCommit 34.159 ms/block（79,313.294 ns/update），fold wall 31.908 ms/block，parent seek 341.178/block，parent block-read 24.154 ms/block，随机 SST 读 2,395,232.485 B/block，顺序 SST 读 3,523,918.864 B/block；债务 81.89 GiB。此窗口是新环境下的 off 基线，不能直接拿它减去旧 onclean 的数据。

发布指纹和源码文件 SHA-256 已保存到 `/data/gtron/releases/20260905-p2a/build-inputs.json`，各次窗口的 node/metrics/process 原始文件均在远端工作目录。部署采用固定 release 的 ExecStart override；自动部署 timer 已恢复，但普通 start.sh 仅更新 build/bin/gtron 时不会替换这个 release。日后回归常规发布流程，需要明确迁移相应 override；本阶段回退脚本只移除自己拥有的 95 配置，保留 P0a 的 90 配置。

## 相同 canary 状态下的复验与最终取舍

canary 均已停止、Java 保持运行；每轮去掉前 6 分钟，取最后 180.2 秒。仍是主网不同高度的相邻窗口，不是固定输入 A/B。

| 指标 | overlap=0（off:12） | overlap=1（onsolo:12） |
|---|---:|---:|
| 高度 | 29,259,439 → 29,261,993 | 29,267,751 → 29,269,497 |
| blocks/s | 14.209 | 9.713 |
| tx/s | 1,479.963 | 1,248.328 |
| tx/block | 104.157 | 128.515 |
| energy/tx，中位 | 13,685.849 | 12,792.356 |
| VM share，中位 | 49.8% | 47.1% |
| transaction ms/tx，中位 | 0.599 | 0.720 |
| stateCommit ms/block，中位 | 34.159 | 36.078 |
| stateCommit ns/update，中位 | 79,313.294 | 77,295.396 |
| fold wall ms/block | 31.908 | 40.777 |
| parent seek/block | 341.178 | 486.938 |
| parent block-read 合计 ms/block | 24.154 | 51.703 |
| 随机 SST 读 MB/block，十进制 | 2.395 | 3.861 |
| 顺序 SST 读 MB/block，十进制 | 3.524 | 5.478 |
| compaction debt GiB，末值 | 81.89 | 75.41 |

重叠模式的整批等待归零，但没有观察到稳定的交易吞吐收益，后段 seek/读量反而上升。由于更新数、交易构成及地址集合不同，不把整段差值称为开关的因果退化比例。当前正式取舍是关闭 overlap，保留路径所有权修复和清单索引优化；默认值也继续是关闭。未达成固定 28M 工作量整体 +50% 的证明，不能将 canary 退出释放资源带来的速度回升记为代码优化收益。

07:23 UTC 的 25.13 秒复验 profile（40.02 CPU-s）中，applyTransactionWithScratch 占 8.84 CPU-s，Pebble compaction 7.78 CPU-s，commitment runLane 7.73 CPU-s。交易路径内 account latest 读取 2.62 CPU-s、VM Execute 3.07 CPU-s、GetCodeStrict 1.40 CPU-s、legacy delegation 写入 1.58 CPU-s；这些是不同阶段的新采样，不能沿用早期 P0a 的热点占比。后续同时需要区分前台账户/代码读取成本与后台大型压实的额外整文件复制，优先在固定输入上验证候选方案。

## 最终运行状态

07:30:06 UTC 最终 overlap=0 健康门槛通过。主网 PID 19440，实际 exe 为 `/data/gtron/releases/20260905-p2a/gtron`，环境为 GOMEMLIMIT=10GiB、async depth=4、overlap=0；随后确认高度 29,271,911、active=true、9 peers，pipeline errors=0，自动部署 timer=active。最终配置和状态保存为 `final-service.conf`、`final-verification.json`；最初的 mode-0 切换记录已归档到 `first-mode-0/`。

本阶段所有修改仍在本地工作区，未提交或推送 Git；生产只部署隔离的 P0a + P2a 二进制，不包含 offline replay CLI。下一阶段的“一次写成压缩历史文件”是设计候选，尚未实现，详见性能 spec 的 P2b 节。

最终一分钟复核：29,271,911 → 29,272,638，session blocks 1,187 → 1,891，交易 126,645 → 198,163，三个采样点均 active=true；PID 19440 保持运行，最终停启后的 journal 未见 ERROR/CRIT/panic。
