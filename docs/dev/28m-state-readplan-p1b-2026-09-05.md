# 28M 同步优化 P1b：状态读取计划

## 改动

P1a 之后，把状态预读改为两阶段有界计划，覆盖账户 envelope、交易实际 permission ID、owner V1 带宽冻结第 0 行、AccountResource、合约 metadata/code 和原有 TRC10 行。每阶段最多 4096 行；TRC10 仍最多 128 行。先解决 overlay/cache，按物理键排序剩余请求，再用短生命周期 Pebble snapshot + exact-key cursor 读取。

读计划集中在 rawdb 构造物理键和去重。只保留 envelope 的 generation/code hash，不复制账户 protobuf。执行仍按最新代际、overlay 和原有严格规则读取；预读不会决定权限、fork 或执行结果。所有回填 epoch 均在 snapshot 创建前捕获，且回填再次校验，避免并发 flush 让旧 snapshot 污染最新缓存。新增 state_prefetch 指标与 commitment cursor 指标分开。

未修改数据库格式，可回退到 P1a 二进制。canary 维持用户停止状态，Java 节点保持运行。

## 固定输入验证

真实 Pebble SST，32768 个 192 字节 value，固定置换访问；包含缓存准入及复制，不计每批前重建空缓存。M1 Max，两次 400ms：

| 每批行数 | 逐行 Prefetch | 批量游标 | 加速 | 分配内存变化 |
| --- | ---: | ---: | ---: | ---: |
| 128 | 0.835–0.847 ms | 0.501–0.503 ms | 1.67–1.69× | 155.8 KB → 138.8 KB |
| 512 | 3.249–3.288 ms | 1.170–1.186 ms | 2.74–2.81× | 382.0 KB → 312.7 KB |

这是相同读取工作量的局部基准，不能直接解释为整链同步倍数。

## 本地验证

- `go test ./core/... ./actuator/... ./vm/... -count=1` 全部通过；core 159.699s。
- 真实 Pebble 的精确存在性、空 value、重复 key、overlay tombstone、snapshot 前/后并发 flush 更新/删除均通过。
- 计划容量、去重、代际隔离、损坏 KV、乱序回调、取消，以及 owner 权限/资源预读后的零新增底层读取均通过。
- 定向 race 两次通过。
- 两节点 system test：79 passed，0 failed，0 skipped。
- 本次 diff 的 golangci-lint：0 issues；无配置全仓 lint 仍有既有问题，本轮未扩展修改其范围。
- 日志位于 `/tmp/gtron-readplan-{regression,race-final,lint-final,system-tests,bench}.txt`。

## 主网部署前

P1a PID 27630，GOMEMLIMIT=10GiB、async depth=4、commitment overlap=0。

`rpbefore` 180.1 秒窗口，高度 29,410,567 → 29,412,400：

- 10.126 block/s、1425.097 tx/s、140.730 tx/block。
- VM 交易占比 42.2%，energy/tx 11560.982。
- tx 0.621ms，commit 61.06ms/block，fold 56.251ms/block。
- commitment seek 489.116/block，parent read 50.958ms/block。
- async enqueue wait 0.200064ms/block。
- SST 随机读 4.664 MB/block，顺序读 6.848 MB/block，compaction debt 78.37GiB。

两份 25s P1a CPU 采样中，`ReadStateKVLatestNoCopy` 为 3.17s / 2.79s；后一次权限读取 0.78s，canonical frozen bandwidth 0.47s，read-ahead warmBlock 1.43s。不同交易组合应分开评估，不能只用块速比较。

## 可追溯资料

远端阶段目录 `/data/gtron/readplan-20260905`。初始 patch.xz SHA256 `3581b3f5941ca79d88cb34c5be93e18de24ce2ef87ea35b42bebcb84e1b9e9c9`；另同步了 cursor/snapshot 关闭错误处理，并按本地最终内容逐一校验全部 10 个源码文件，指纹在 `build-inputs.json`。本地最终完整补丁 `/tmp/gtron-readplan-final.patch` SHA256 `7e9421eec5cbc42ec219c78238515cdbba846d0d9c4d70271859d5f53fd90cfe`。

部署脚本使用 `/data/gtron/start.lock`，暂停并在退出时恢复原本 active 的 `gtron-deploy.timer`，正常停止主网后安装自己的 systemd override，以 active + peers + 前进至少 32 块判断启动健康；失败自动恢复上一版。


## Linux/Sapling 验证

使用服务器 `/data/go/bin` Go 1.25.5 和 java-tron 用户的现有缓存（`/home/java-tron/.cache/go-build`、`/home/java-tron/go`）。GOMAXPROCS=2，GOMEMLIMIT=2GiB。生产编译用 GCC；race 用 `/opt/rh/devtoolset-11/root/usr/bin/gcc`。root 的全新模块缓存下载被中止，未影响主网进程。

- blockbuffer 0.800s、rawdb 7.097s、pebbledb 0.230s、state 2.142s，全部通过。
- 定向 race 两次：blockbuffer 1.213s、rawdb 1.152s、state 1.182s，全部通过。
- Xeon Platinum 8175M 固定输入：128 行逐行 1.580–1.588ms、批量 0.965–0.979ms（约 1.63×）；512 行逐行 5.746–6.145ms、批量 2.316–2.368ms（约 2.54×）。
- 候选构建完成：2026-09-05 11:25:46 UTC。
- 生产 release 目标 `/data/gtron/releases/20260905-p1b/gtron`。
- 新 override：`/etc/systemd/system/gtron.service.d/97-state-readplan-p1b-20260905.conf`；P1a 的 96、P2a 的 95 及 P0a 的 90 保留。
- 回退入口：`bash /data/gtron/readplan-20260905/rollback-mainnet.sh`，仅移除本轮 97，回到 P1a，并验证主网高度继续前进。
- 新 release 继续 pin ExecStart，后续恢复普通部署须显式整理 override；timer active 不代表普通 build/bin/gtron 已在运行。


部署健康检查已通过：主网 PID 20970，高度 29,449,126 → 29,449,162，旧进程正常退出状态 0。持续观测使用 `rpafter`，原始文件位于 `/data/gtron/commitment-20260905/rpafter-*`。

生产二进制 SHA256：`b2dd9154f6cf3696fe51772318e8e964870871960fa2a3a4a94e7ff3bc988657`；部署完成 2026-09-05 11:28:06 UTC。已核验 `/proc/20970/exe` 与 release 一致，deploy timer 恢复 active。

初期 25s CPU 对照（`normalized-before` / `normalized-after-warm`）：窗口计数 45263 / 30876 tx；KV 读取累计 CPU 3.64s / 1.44s，权限 1.22s / 0.22s，canonical frozen bandwidth 0.60s / 0.05s。粗归一到每笔交易，KV 约降低 42%、权限约降低 74%、frozen bandwidth 约降低 88%。这是相邻不同输入窗口；计数窗口因代理请求开销约 27.7s，CPU 采样约 25.1s，且前窗口与 Linux 基准存在重叠。不能作为严格整链同输入 A/B。预读 warmBlock CPU 从 2.28s/45263tx 增至 2.80s/30876tx，表明前台等待部分被转移到后台；后续还需用总吞吐、I/O 及稳定采样判断。


## 最终观测与决定

保留 P1b。完整观测 19 点、约 9 分钟，高度 29,450,010 → 29,456,631，所有点均 active；最终 peers 24、唯一 gtron 进程 PID 20970。预读累计 3,839,701 行，批量 cursor 15,007 个、durable exact seek 1,663,585 次；预读错误 0、丢块 0。GOMEMLIMIT=10GiB、async depth=4、overlap=0 均与 P1a 一致；最终 RSS 19,740,988 KiB，约 18.83 GiB，timer active。原始确认文件 `final-verification.json` 已归档到 release。

最终稳定窗口 `rpafter:12`，180.1 秒，高度 29,454,435 → 29,456,631：

| 指标 | P1a 部署前 180.1s | P1b 预热后最后 180.1s |
| --- | ---: | ---: |
| tx/s | 1425.097 | 1646.133 |
| block/s | 10.126 | 12.079 |
| tx/block | 140.730 | 136.282 |
| VM 交易占比 | 42.2% | 40.0% |
| energy/tx | 11560.982 | 9134.871 |
| energy/s | 16993676.396 | 15110124.321 |
| tx wall ms/tx | 0.621 | 0.523 |
| commit ms/block | 61.060 | 124.664 |
| commitment ns/update | 119557.093 | 278334.551 |
| commitment seek/block | 489.116 | 367.232 |
| parent read ms/block | 50.958 | 102.055 |
| async enqueue wait ms/block | 0.200 | 31.663 |
| SST 随机读 MB/block | 4.664 | 3.464 |
| SST 顺序读 MB/block | 6.848 | 4.547 |
| compaction debt GiB | 78.37 | 81.07 |

最后窗口 tx/s 较基线 +15.5%，前一中间窗口约 1860.569 tx/s，说明还存在明显波动。两个窗口的交易/能量构成不同，且重启重建缓存、后台 compaction 不同；**不把这些数字当作严格整链因果 A/B，也不声称已达到整链 +50%**。尤其 commit 单位更新时间与 enqueue 等待上升，仍是下一阶段要消除的主要等待。最后窗口 enqueue wait 累计约占墙钟 38.2%，不能再把异步提交排队视为无关。

较可靠的局部收益来自相同 SST/key 工作量的 Linux 基准，以及连续 CPU 采样：

| 采样 | 窗口 tx | KV 读取 CPU µs/tx（粗归一） | 相对 P1a | 权限 CPU 降幅 |
| --- | ---: | ---: | ---: | ---: |
| P1a before | 45263 | 80.42 | — | — |
| P1b warm | 30876 | 46.64 | -42.0% | -73.6% |
| P1b mid | 45170 | 47.38 | -41.1% | -92.6% |
| P1b final | 43464 | 37.50 | -53.4% | -84.6% |

final 样本：KV 1.63 CPU-s、permission 0.18s、canonical frozen 0.06s、warmBlock 2.49s、commitment runLane 7.88s。对比 before 的 warmBlock 2.28s/45263tx，预读 CPU/tx 增约 13.7%，没有把后台成本排除在结论之外。CPU 文件和交易计数保存在本地 `/tmp/gtron-readplan-normalized-{before,after-warm,after-mid,after-final}.*`。

保留理由是固定输入批量读取约 1.6–2.5× 的收益、前台权限/资源读取成本的持续下降、现有资源预算内的连续健康同步；整链收益仍需同输入 replay 或进一步控制变量才能量化。下一阶段重点是 commitment 父分支读取的物理 I/O 与提交队列，避免继续单纯扩大预读。

最终 release 附带 `sources.tar.xz`（全部 10 个本轮文件按 build-inputs.json 再校验）、Linux 测试/基准、配置、运行二进制指纹、健康结果和回退脚本。
