# Freezer 同步性能优化与实现审计（2026-08-28）

## 结论与范围

完成两项本地优化：交易索引遍历的保守快速路径、热区块大小统计的有界采样。
三个并行子任务分别负责两项实现和独立对抗性审计，主代理复核实现、整合测试并独立重测基准。
本次审计未发现阻断项；没有部署、重启节点、修改线上配置或开启正式并行转账/VM。
工作区原有的并行执行加固改动保持不动，不属于本次代码审计范围。

## 依据与实际改动

8 月 28 日主网样本中，正式并行转账和 VM 均关闭，128.848 秒前进
11,523 块（89.43 blocks/s）。30 秒 CPU profile 中，freezer 交易索引遍历
累计占进程 CPU 的 10.29%，热区块统计扫描占 3.43%。它们是局部累计
CPU 占比，不能直接换算成整节点提速比例。

### 交易索引遍历

- `core/types/block_transaction_hashes.go` 新增 `IterateBlockTransactionHashes`，
  接入 `core/freezer/runner.go` 的索引构建及裁剪共用遍历入口。
- 按当前 protobuf descriptor 建立保守白名单，验证整块及已知嵌套消息。
  只接受解码、重编码后字节不变的规范编码，验证通过后按原 ordinal
  对原始 `raw_data` 求 SHA-256，避免构造完整对象图和重新编码。
- 未知字段、重复 singular 字段、乱序、非最短 varint/长度、显式默认值、
  不支持的 map/字段形态等，整块回退原 `UnmarshalBlockBorrowed + tx.Hash`。
  历史 pre-PQ 兼容逻辑不变。
- 必须先验证整块再输出，后部畸形数据不能让前部交易先进入删除批次。
  callback 错误和取消直接返回，不触发回退、重放或重复输出。
- 冷索引仅保存哈希指纹，不能用来重建删除用的完整哈希。本次没有采用该路径，
  也没有扩大 `rawdb/txinfo_compact.go` 原有 canonical-wire helper 的使用范围。
- 没有消除 ancient 读取或必要的 SHA-256；索引格式、coverage、发布、
  16 MiB 删除批次、持久化水位及 fsync 顺序不变。

### 热区块统计

- `core/freezer/namespace_stats.go` 将每次完整 b-* 扫描改为最多
  1,024 行、8 MiB 逻辑字节或 25ms 的采样，任一预算耗尽即结束。
- 25ms 是软预算：单个数据库操作不能中断；字节预算最多超出最后一行。
  不跨 pass 保留 iterator/snapshot，也不拼接多次扫描冒充精确总量。
- 样本字节数、行数、完整标记、时间戳通过同一个不可变指针发布，
  `Runner.Snapshot` 不会读取到混合版本的样本。
- 错误/取消保留旧样本和原时间戳，并返回错误；已完成冻结先记入计数，
  不会因后续观测失败丢掉该轮计数。
- `chain/freezer/pebble/size` 仅包含 b-* 的逻辑 key+value 字节，不含 receipt，
  不是磁盘占用。必须结合 `pebble/size/complete`、`pebble/size/rows` 和
  `pebble/size/sampled_at`（Unix 秒）解读。`complete=0` 时只是前缀下界，
  不能用于总量增长趋势；时间戳为零表示尚未采样。no-op pass 不刷新样本。
- 新元数据通过 metrics 暴露；`gtron_freezerStatus` 原本返回独立的
  immutable-store 状态，本次未改变该 RPC。

## 验证

主代理执行并通过：

```sh
go test ./... -count=1 -timeout=300s
go test -race ./core/freezer ./core/types -count=1 -timeout=300s
go vet ./core/freezer ./core/types
go test ./core/types -run '^$' -fuzz '^FuzzIterateBlockTransactionHashes$' -fuzztime=30s -parallel=2 -timeout=120s
git diff --check
```

新增测试覆盖规范/非规范 wire、signed int32/enum 截断、未知/重复字段、
非法 UTF-8、畸形尾部、空/缺失 raw_data、历史 pre-PQ、callback/最终回调取消；
结构化生成覆盖支持的嵌套字段，并以原 borrowed decoder 和 generated
protobuf 作差分校验，验证 fast-path 输入整块 roundtrip 字节一致。
主代理增强 fuzz 本轮完成 2,987 次执行；它不是穷举或长期主网验证。

独立审计测试使用真实 V2 和已发布冷索引，注入部分删除后的 batch 写入失败
或取消：水位不前进，已删交易仍可走冷查询，重试幂等补齐。另验证 eligible
coverage 不越界、水位回退拒绝，以及统计错误、释放、预算和并发一致性。

## 独立微基准

Apple M1 Max / darwin arm64 / Go 1.27.0；各项单独运行，1 秒 × 3 次，
下表为均值。没有与其他主动压测、fuzz 或基准并跑。

| 测试 | 原路径 | 新路径 | 含义 |
| --- | ---: | ---: | --- |
| 同一 200 交易块的哈希遍历 | 160.899 µs、258,550 B、10 allocs | 49.977 µs、0 B、0 allocs | 局部快约 3.22 倍；不含读 ancient、索引删除和提交 |
| 同一 65,536 行 Pebble b-* 空间统计 | 25.714 ms、扫描全部行 | 0.095 ms、扫描 1,024 行 | 减少观测工作；新结果是下界，并非等价全量统计加速 |

复现：

```sh
go test ./core/types -run '^$' -bench '^BenchmarkBlockTransactionHashes$' -benchtime=1s -count=3 -benchmem -timeout=120s
go test ./core/freezer -run '^$' -bench '^BenchmarkHotBlockNamespaceSize$' -benchtime=1s -count=3 -benchmem -timeout=180s
```

## 上线后仍需确认

本次没有测得新的线上 blocks/s，不能将 3.22 倍局部加速当作整节点收益。
后续保持正式并行执行关闭，用相近交易密度/energy 的持续样本比较 CPU/块、
分配量、freezer 耗时、裁剪进度、生命周期错误、compaction、blocks/s 和
transactions/s。严格 A/B 仍需相同可信快照、相同区间及配置的独立数据副本。
只有该观测通过，才能评价收益是否抵消新的维护成本；本次结果不构成重新开启
正式并行转账或 VM 的依据。
