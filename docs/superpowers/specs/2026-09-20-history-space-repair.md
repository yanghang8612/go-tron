# 冷历史空间修复边界

2026-09-20。本说明记录当前冷历史空间修复的兼容边界和验证口径；它不替代引用容器设计、
compaction 正确性验证或生产切换门禁。

## R1 chunk 编码

R1 引用容器保留 codec 0 raw 和 codec 1 Snappy 的读取兼容。codec 2 仍是 writer 私有 scratch
标记，不能出现在最终文件；独立 Zstd chunk 使用 codec 3。每个 Zstd chunk 是可独立随机读取的
完整 frame，解码同时受目录声明长度、128 KiB chunk 上限、Zstd window/memory 上限和最终 SHA-256
约束。未知 codec 必须失败关闭，不能按 raw 或其他编码猜测。

写出 codec 3 文件前，所有可能读取 R1 的进程必须升级到支持 codec 3 的 binary，并绑定重启
guard。旧 reader 会拒绝 codec 3；首次发布后不能回滚到旧 binary。升级 writer 只影响之后新写或
重写的 R1，既有 raw/Snappy R1 和其他旧历史文件不会立即变小。

## 合并和上限

空间修复的 busy leaf fallback 采用有界流式重编码：R1 输出估计超限但通用输入预算仍允许时，
逐记录从逻辑 `ReaderAt` 复制到既有压缩 V6/V7 输出，不把完整 Prev 展开进内存。具体选择、
验证和发布规则见
[Bounded R1 busy-leaf merge fallback](2026-09-20-history-reference-bounded-stream-fallback.md)。fallback
不能放宽 R1 chunk、span、metadata、logical、physical 或 source-count 上限；这些上限仍是损坏
输入和内存/磁盘放大的安全边界。

现有 cold builder 汇总日志公开 compaction deferred 状态、原因、输入 logical bytes 和 source
数量，用于区分 `import-load`、恢复窗口、leaf target、输入预算和 reference 格式上限。它不新增
逐次 defer 日志或高基数指标，也不改变调度决策。生产 gauge
`lastpass/compaction/defer_reason` 使用固定数字：0 无 defer、1 import load、2 merge recovery、
3 heavy-work gate、4 leaf target、5 input budget、6 reference-container limit、7 其他有限兜底；
不会从原因字符串创建动态 metric 或 label。

## 证据口径

手动 R1 codec 诊断只接受 physical 不超过 512 MiB、logical 不超过 2 GiB 的不可变私有样本，
并受 10 分钟 context 限制。诊断先完整验证源，逐 chunk 重编码并按原 span 顺序重建虚拟流，再
完整验证候选，以固定缓冲比较逻辑长度和虚拟流 SHA-256。输出的 payload、目录、总物理字节及
墙钟只说明该 R1 容器样本的重编码结果。

该测试不包含 `.kv`/`.idx` companion、热源认证与构建、compaction 选择、manifest 发布、GC、
线上并发或 I/O 竞争，因此不能单独证明 trio 总量、全历史回收比例、同步吞吐或生产收益。
