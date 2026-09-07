# P1a：合约代码严格验证成本优化

状态：已部署主网并完成本轮采样。保留 P1a；代码验证局部成本显著降低，整链 +50% 目标尚未取得固定输入证明。

## 停止 canary 后的新基线

2026-09-05 09:50 UTC 起 180.1 秒，P2a PID 19440，现场只有一个 gtron，Java PID 16568 仍运行；端口与既有部署布局一致，overlap=0。29,376,501 → 29,378,187，session 计数 9.415 blocks/s、1,701.917 tx/s、180.774 tx/block。窗口中位交易 0.539 ms/tx、VM share 30.6%、energy/tx 8,158.249、stateCommit 59.038 ms/block；fold 58.763 ms/block、parent seek 609.085/block、parent read 53.902 ms/block、随机 SST 4.602 MB/block、顺序 SST 7.077 MB/block，债务 81.03 GiB。

09:49 UTC CPU 样本 25.07 秒/48.24 CPU-s，交易执行 11.13 CPU-s，GetCodeStrict 2.50 CPU-s，其中重复 Keccak 2.26 秒，重复 cache admission 0.21 秒。清单 Validate 0.06 CPU-s。第二个 09:51 UTC 样本 25.14 秒/70.30 CPU-s，相应 GetCodeStrict 1.42 秒、Keccak 1.21 秒、admission 0.20 秒。两次绝对比例不同，不能只选最高占比估计收益。

## 实现

- 对象内已载入代码与数据库正缓存的私有、已 hash 验证副本逐字节比较。全部相同才免除重复 Keccak；未命中或不同时仍执行原严格校验。
- 保留返回 slice 被修改后的损坏检测；不使用只验证一次的 flag、指针相等或弱 hash。
- 重复 admission 不再先分配完整代码副本；实际 miss 在锁外复制，然后重新持锁检查并发 admission/close。
- 延用既有 64 MiB 上限、LRU、数据库隔离和代码所有权。不改变持久化格式、交易依赖、历史、journal 或共识规则，允许回退 P2a。
- 新指标 state_code_strict_object_verified_matches、state_code_strict_object_hash_checks，用于区分实际路径。

## 固定输入基准

Apple M1 Max，完整 StateDB.GetCodeStrict，旧源码 overlay 对照，每组 3 次、每次 500 ms，中位值：

| 代码长度 | P2a | P1a | 局部加速 | 每次分配 |
|---|---:|---:|---:|---:|
| 256 B | 850.9 ns | 31.54 ns | 27.0x | 256 B → 0 |
| 4 KiB | 11.408 μs | 106.8 ns | 106.8x | 4 KiB → 0 |
| 32 KiB | 87.401 μs | 691.5 ns | 126.4x | 32 KiB → 0 |

这是已缓存代码的局部基准，不是同步整链加速比例；缓存 miss 仍计算 Keccak。

## 验证

- 1024 B 返回 slice 每个字节逐一修改、截断/追加/空内容均拒绝；覆盖关闭缓存、预算不足和可驻留三种状态。
- 代码 A/B 修改、执行副本、journal revert、cache close 后重新验证；独立 StateDB 的共享缓存未被污染。
- 并发准入/淘汰/关闭检查，targeted race 9.655 秒通过。
- core/...、actuator/...、vm/... 完整回归通过；增量 lint 0 issues，git diff --check 通过。

## 发布

补丁 SHA-256：b24dc78b619fc8a6113bf60e3a8c895ad96e0caacd8284a37d76c767958dbfca。服务器隔离目录 /data/gtron/codeverify-20260905，生产只包含原 P0a/P2a 加本轮 3 个源文件，不包括本地 offline replay CLI。发行版于 10:02:25 UTC 构建完成；Linux Sapling state/actuator/VM 回归通过（1.312/0.056/2.992 秒），GCC 11 race 两次重复通过（2.358 秒）。本地系统测试 79 passed，0 failed，0 skipped。部署已启动。

10:04:48 UTC 开始切换，10:05:03 旧进程干净停止，10:05:25 健康检查通过（29,385,795 → 29,385,831，4 peers）。新 PID 27630，运行 /data/gtron/releases/20260905-p1a/gtron；binary SHA-256 d6e3a8e51e45d98e9d68fab18020115f6892caef77f620cdec03d9d03d130cce。覆盖文件 96-codeverify-p1a-20260905.conf，保留 P2a 的 95 和 P0a 的 90。

回退入口：bash /data/gtron/releases/20260905-p1a/rollback-mainnet.sh。仅移除本轮 96 override，并使用保留的 P2a 配置；加锁、暂停/恢复自动部署 timer，等待干净退出和高度推进。脚本已通过语法检查，未执行实际回退。固定 release 覆盖仍会优先于常规 start.sh 的 build/bin/gtron，恢复常规发布时需明确迁移这些 override。

紧邻部署前的独立 25.234 秒窗口：29,384,734 → 29,384,977，38,072 tx，strict object 命中 26,696 次、shared-cache 1,741 次、hot 3 次，代码错误/最终缺失/拒绝均为零。profile 25.17 秒/53.24 CPU-s，GetCodeStrict 1.71 CPU-s、交易执行 8.61 CPU-s；整个 strict 路径除以记录到的三类成功读取约 60.1 μs/次（抽样估计，含不同代码大小）。此时存在本轮构建活动，整个进程/吞吐不用于与部署后作无干扰比较。

上线初期 warm 样本：25.184 秒，29,386,121 → 29,386,312，26,622 tx；strict object 20,642 次全部为 verified matches，object hash checks=0，shared-cache 1,018 次、hot 74 次，错误/拒绝/最终缺失均为零。profile 25.15 秒/44.25 CPU-s，GetCodeStrict 0.06 CPU-s，交易执行 5.61 秒。按三类成功 strict 读取合计 21,734 次折算约 2.76 μs/次，对比上述 P2a 60.1 μs/次约降低 95.4%。采样粒度、代码长度组合和新旧窗口不同，这不是固定输入的整链提速证明。新缓存等价快路在实际调用上生效，重复 Keccak 热点已明显减轻。

10:09:46 UTC 第二次上线后样本：25.18 秒/49.67 CPU-s，GetCodeStrict 0.07 秒（0.14%），未见重复 Keccak 采样；交易执行 8.34 秒，其中 ReadStateKVLatestNoCopy 3.18 秒（38.1% 交易 CPU），AccountPermissionByID 0.87 秒、GetAccountFrozenBandwidthV1 0.62 秒。Commitment runLane 8.10 秒，Pebble compaction 6.82 秒，SnapshotLifecycle 4.30 秒。热点占比有包含关系，不能相加。后续前台优化优先关注准确的 owner 权限/冻结资源点读和现有缓存的实际失效率，仍不能通过弱化权限或资源校验换取性能；大型历史单次写入路线另按完整生命周期验证。

稳定阶段 final profile：25.198 秒，29,391,640 → 29,392,121，43,644 tx；strict object 33,878 次全部 verified matches，object hash checks=0，shared-cache 2,090 次、hot 26 次，代码错误/拒绝/缺失均为零。profile 25.16 秒/56.06 CPU-s，GetCodeStrict 0.09 CPU-s（0.16%），交易执行 7.71 秒；按成功 strict 读取 35,994 次折算约 2.50 μs/次，相比 P2a 独立采样 60.1 μs/次约下降 95.8%。不同代码长度组合和采样误差依然存在，局部改善与固定输入微基准方向一致。

## 完整稳定窗口与结论

19 个采样点、每隔 30 秒。排除前 6 分钟，使用最后 180.1 秒；两轮 canary 均停止，Java 继续运行，overlap=0。仍是不同历史高度和地址/交易集合，不是固定区块重放 A/B。

| 指标 | P2a 基线 | P1a 稳定窗口 |
|---|---:|---:|
| 高度 | 29,376,501 → 29,378,187 | 29,391,093 → 29,394,001 |
| blocks/s | 9.415 | 16.167 |
| tx/s | 1,701.917 | 1,546.570 |
| tx/block | 180.774 | 95.662 |
| VM share，中位 | 30.6% | 39.7% |
| energy/tx，中位 | 8,158.249 | 10,991.414 |
| energy/s，中位 | 15,387,443.807 | 17,370,468.336 |
| transaction ms/tx，中位 | 0.539 | 0.545 |
| stateCommit ms/block，中位 | 59.038 | 47.321 |
| stateCommit ns/update，中位 | 99,598.575 | 119,375.555 |
| fold wall ms/block | 58.763 | 40.173 |
| parent seek/block | 609.085 | 294.215 |
| parent block read ms/block | 53.902 | 31.055 |
| async enqueue blocked ms/block | 0.021 | 0.770 |
| 随机 SST MB/block | 4.602 | 3.056 |
| 顺序 SST MB/block | 7.077 | 2.850 |
| compaction debt GiB，末值 | 81.03 | 79.32 |

新窗口块速更高但交易速率低约 9.1%，同时每块交易数降低约 47.1%，VM share 和 energy/tx 更高。不能把块速 +71.7% 称为代码改动带来的吞吐提升，也不能由这两个不同负载窗口断定交易性能退化。交易 wall/tx 基本持平，IO 和后台阶段仍影响关键路径。新窗口 enqueue 阻塞 0.770 ms/block，乘以 16.167 blocks/s 约占 1.25% 导入墙钟时间，已经高于此前接近零的水平和 spec 的 0.1% 目标，下一轮需要同时检查提交端尾延迟，不能继续沿用“提交队列不是瓶颈”的旧结论。本轮保留改动的依据是同输入局部基准与三次上线 profile 共同证实重复 hash/复制已消除，错误/存储/回滚语义保留；尚未完成同一 28M 区间的整链 +50% 性能验收。

最终核验：主网 PID 27630，已到 29,394,501，28 peers；19 个节点采样均 active=true；全部采样的代码 strict errors、hash rejections、final misses、Commitment pipeline errors 均为零。自动部署 timer=active，进程 RSS 19,474,316 KiB（约 18.57 GiB），仍只有一个 gtron 进程。启动后的 journal 未见 ERROR/CRIT/panic。GOMEMLIMIT=10GiB、async depth=4、overlap=0 已从运行进程确认。

原始窗口 /data/gtron/commitment-20260905/p1after-*，汇总 p1after-skip12-summary.json；本轮 profile、部署及构建证据 /data/gtron/codeverify-20260905；发布指纹 /data/gtron/releases/20260905-p1a/build-inputs.json。采样任务已结束。所有源码修改仍在本地工作区，未提交或推送。
