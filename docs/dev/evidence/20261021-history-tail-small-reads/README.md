# 固定副本：热尾部与小块读取证据

参见 [验收报告](../../history-tail-small-reads-20261021.md)。候选生产源码冻结于 `4d34e751f0d37add20451ca9aae2d68e0c930b6e`，第一轮失败性能试验冻结于 `d2e41fc5f4f1961ce5823e8776f0665953cbc77c`。两轮基线均为离线 `0b370448ec97aa8da87aa8e5b23edeadc59d2439`，不是线上 9030ce7a。

`first-trial/` 保存第一轮全部对照和后续测试专用诊断；`final/` 保存最终原生验证、派生索引、完整 HTTP、全量 MuHash、真实恢复窗口和排除于性能比较的追踪。原始路径保留在驱动和回执中，线上库没有作为写入目标。

## 输入与控制条件

- 同一不可变主网捕获：HEAD 11,818,192，TxNum 601,386,177，根 `8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5`。
- 基线派生视图 `account-gc-input`，候选派生视图 `tail-read-final-20261021-02/storage-route-input`。规范文件共享不可变数据，辅助 manifest/index 独立；`canonical-inodes.json` 与构造回执证明规范文件和前沿不变。
- 冷缓存请求另有私有 inode，发现读取集、私有文件来源、inode、字节长度、mincore 驻留率均保存。只对这些私有文件使用 fadvise，不对源或生产库清缓存。
- 每轮 154 次完整 HTTP、112 计时样本；每版本每高度 warm n=4、fresh n=2、private OS-cold n=2。发现和暖身请求单列，strace 与 CPU profile 另列，不混入主表。
- 仅一个计数器合约、七个高度。缺省 caller。没有生产并发、P2P、同步、冷发布或清理负载；不覆盖所有 eth_call 形态、设备冷缓存或 p95。

## 主要文件

| 文件 | 用途 |
|---|---|
| `source-identity.json` | 精确源码 SHA256 清单、commit、原生二进制、HTTP/MuHash/恢复宿主及工具链身份 |
| `prepare-native.py`、`native-*.log` | 相关包完整测试、针对性 race、MuHash/快照/恢复回归、vet 和构建 |
| `storage-route-build.json` / `.jsonl` | 73 批构造、4,609 对严格字典复核、V2 覆盖、活跃文件成本 |
| `tail-read-comparison/requests.jsonl`、`summary.json` | 逐请求完整返回值、期望值、时间；七高度三缓存条件中位数 |
| `tail-read-comparison/*/server-requests.jsonl` | handler CPU、分配、GC、进程 I/O 前后计数，和客户端唯一 ID 对应 |
| `aggregate.json` | 与客户端请求逐条匹配后计算的 handler/CPU/分配/I/O 中位数 |
| `muhash-census.json` | 三个最新状态族全量扫描摘要与 MuHash 根重算 |
| `history-restore.json` | 三个真实历史窗口、全行摘要、实际恢复结果及交易范围 |
| `nine-diagnostic/read.trace`、`read-breakdown.json` | 单次 OS 冷 9M 完整请求的读取归属；排除于性能表 |
| `nine-diagnostic/profile/` | 独立 warmed 20 秒 CPU profile、正确返回回执和文本渲染 |
| `resources.jsonl`、`resource-status.json` | 有界实验任务资源、内存和运行/结束状态 |
| `final-production-identity.json` | 审计结束时生产 PID/二进制不变，旧数据库仍存在 |

大文本可以 `.gz` 保存；解压字节与服务器清单一致。测试宿主使用 `.go.txt` 后缀，避免被 `go test ./...` 意外编译。`local-validation/` 为相同冻结源码的本地完成日志，与服务器传输清单分开记录。

## 完整性与独立复算

`transfer.json` 记录经堡垒机正常 SFTP 界面下载的 ZIP 长度和 SHA256；`evidence-inventory.json` 记录服务器封装时每个条目的原始字节长度与 SHA256。原 ZIP 保存在忽略的工作目录中，不重复提交。

在包含两个冻结 commit 的仓库工作树执行：

```bash
python3 docs/dev/evidence/20261021-history-tail-small-reads/verify.py
# 还保留原 ZIP 时，附加 --archive /absolute/path/to/archive.zip
```

脚本复算传输/条目 SHA256、Git blob 源码清单、每轮 154 个完整返回值、42 组时间中位数、112 条 handler/CPU/分配/I/O 匹配、私有 inode/驻留证明、构造覆盖、规范源摘要、MuHash、恢复摘要和生产身份。最新状态三族全量 source SHA256 还会与前阶段证据对比（若该目录存在）。

## 重现顺序与已知失败

原生驱动中的绝对路径是本轮冻结实验身份的一部分。重跑应在新的离线目录复制相同 Git 源及保存的 `.go.txt` 宿主，先核对源码/输入 SHA256，再依次准备、构造、完整 HTTP 对照、MuHash/恢复、独立诊断，最后封装；不要直接复用已有输出目录或把生产 datadir 作为目标。具体命令见 `prepare-native.py` 与各驱动；CPU/内存预算由资源回执验证。

第一轮 aligned pages 的 OS 冷回退是测量结果，未从报告中删除。其全量审计等待器在实际审计开始前被最终版本替代，见 `superseded-waiters.json`。`first-nine-quick` 首次 pprof 文本渲染因 Go 环境缺失失败，原始 profile 正常，后有 recovered 文本；不得解释成 MuHash 或查询错误。

最终封装等待器曾碰到非原子状态文件写入中的空 JSON；保留 `seal-recovery/original-wait-failure.json`、原驱动和补丁回执，恢复版仅重试不完整 JSON，仍要求每个验证阶段成功且单元 inactive。没有跳过正确性断言，也没有重跑或改动已完成性能样本。

563 个条目原始字节合计 33,737,903 B；ZIP 4,234,737 B，SHA256 `5284aa94b2413a98e8e19eca18e300328b00ff02b9b746a7a96521455a0a0703`。独立复算结果见 `verified-receipt.json`，验收截图见 `final-acceptance-and-production.png`。
