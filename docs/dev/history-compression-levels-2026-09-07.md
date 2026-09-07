# 冷历史压缩等级与并行编码对照

2026-09-07。本文件记录本地可重复的合成对照与固定依赖源码审计；没有连接
生产，没有启动 gtron，没有改生产 factory、writer、rawdb 或命令默认值。

## 可行方向与边界

CPU 有余量、磁盘 I/O 紧张时，可以提高每个独立 frame 的编码工作量，或者
同时压缩多个已经确定内容与顺序的 frame。两者应分别验收：提高并行度主要
减少编码墙钟时间，不应改变同配置的输出字节；提高等级可能缩小输出，却
不保证每种输入都更小，也不保证节省的 I/O 足以抵消额外 CPU 与内存。

最初受控对照采用完整 V6 history 逻辑流、固定 128 KiB 独立 frame、明确 128 KiB
编码窗口、CRC 与 single-segment/FCS，比较 fastest/default/better 三档及
1/4/8 个外部并行任务。读取端要求每 frame 输出长度与 FCS 完全相符，
`WithDecoderMaxMemory(128KiB)`、`WithDecoderMaxWindow(128KiB)` 与
`WithDecodeAllCapLimit(true)` 同时启用，目标 slice 容量恰为声明长度。

这是**受控的等级比较**：现有生产 encoder 的默认搜索窗口可能为 4/8 MiB，
本实验显式缩为 128 KiB 以固定编码内存变量；没有声称本次数字就是现有
production writer 的严格前后 A/B。随后另加与生产 V2 `cbCodec()` 相同选项的
基线，并完成两个类别的短计时对照，见下文。128 KiB 是 frame 解码输出上限，
不是整个 codec、并行流水线或完整历史记录的总内存上限。

CPU 核数增加不能让独立 frame 利用别的 frame 中的重复数据。巨大随机
地址列表的相邻版本即使高度相似，独立 frame 等级调整仍可能收效很小；
CDC 的跨块重复引用与更高 zstd 等级是两项独立机制，不能将二者收益重复相加。

## 固定依赖的主源结论

本 checkout 使用 `github.com/klauspost/compress v1.17.11`，模块校验值为
`h1:In6xLpyWOi1+C7tXUUWv2ot1QvBjxevKAaI6IXrJmUc=`。

* `EncoderLevelFromZstd(1/3/6/9)` 分别映射为 fastest/default/better/better。
  6 与 9 在本依赖中是同一档，不能报告为两个独立等级；best 是另一个更昂贵
  的算法档位，当前正式矩阵暂不包含它。
  [本地源](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder_options.go:185>)。
* `EncodeAll` 允许并发调用，但每次调用只在一个 goroutine 上编码。
  `WithEncoderConcurrency` 设置可同时使用的 encoder 数量；串行调用循环仅
  调大这个值不会获得多个 frame 的并行吞吐。
  [实现](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder.go:485>)、
  [并发选项](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder_options.go:78>)。
* 显式 SingleSegment 确保 FCS 存在；默认 `EncodeAll` 的极小 frame 可以为
  非 SingleSegment 且不带 FCS，本次生产基线测试已覆盖这种情况。SingleSegment
  时 decoder 以 FCS 作为该 frame 的内存需求，不能仅依据压缩字节数控制分配。
  [frame 写入](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder.go:525>)、
  [SingleSegment 选项](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder_options.go:280>)。
* 默认编码窗口随算法为 4/8 MiB，显式窗口能减少编码器部分内存，但内部
  hash/chain 等工作区仍随算法与并行度增加。解码输出容量限制和窗口限制
  是不同的门。
  [编码选项](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/encoder_options.go:217>)、
  [解码限制](</Users/asuka/.gopath/pkg/mod/github.com/klauspost/compress@v1.17.11/zstd/decoder_options.go:122>)。

这里核验的是固定项目依赖，不把 upstream 注释中的 CPU 倍数当成本项目实测。

## 已实现的对照代码

[history_compression_levels_test.go](../../core/state/snapshots/history_compression_levels_test.go)
只添加测试和基准。fixture 由真实 `StorageCoreV4`、native delegation codec
及 V6 history encoder 生成，V5 无代码 envelope 按显式五字段 RLP schema 构造，
因此完整 `.seg` 逻辑流含 header、字典 digest、TxRange 和 keyID 记录；key
字典本体及 posting 留在独立 `.kv`，本次压缩对照不计 sidecar 大小。所有地址、余额、时间、unknown 字节、storage 与列表内容都是
固定 seed 的**合成数据**，并非本地已下载的生产 trio。

| 类别 | 普通基准规模 | 构造特点 |
| --- | ---: | --- |
| account-v5 | 32,768 记录 | 256 个 owner，变化的余额/时间/资源计数，稳定地址/名称及 unknown 字段；无代码 hash |
| storage-random32 | 32,768 记录 | 32B 随机值；相同 key 空间，不假设值可压缩 |
| storage-counter32 | 32,768 记录 | 高位零、低位变化的 32B 整数值 |
| delegation-list | 12 记录 | 每版 65,536 个随机 21B peer，加中部插入、时间与 unknown 变化；每版完整 native 列表 |

基准将同一 raw stream 按同一 offset 分块，多个任务写入各自预分配结果槽，
不使用完成顺序决定物理序号。初始化与预热不计入 steady-state 时间，最终
重新逐 frame 解码比较原始字节。它只测 codec，未写最终 `.seg`，没有执行
fsync、manifest 发布、索引构建或生产 CDC 字典/locator；报告的 `zstd-frame-B`
只包含 zstd frames，不能当作完整 trio 文件大小。

每个基准报告 `ns/op` 墙钟时间、MB/s、`cpu-ns/op`（Getrusage 的全进程
user+system CPU）、分配量、frame 数及压缩 frame 总字节。CPU 统计需单独运行，
不得和其它重型任务同时执行。fixture 与所有输出 frame 常驻本进程，`B/op`
表示分配工作，不是生产流水线峰值内存。每配置单独用 `/usr/bin/time` 测
max RSS，结合 codec 内部状态和受限 in-flight 预算，才能决定可用并行度；
下述短计时没有测 max RSS，因此没有给出峰值内存结论。

## 已运行的小正确性对照

最初运行 `TestHistoryCompressionLevelsByteExactAndWindowBound`，完整测试用时
0.820s。所有档位与 1/4/8 workers 均通过逐字节
还原、FCS/CRC/128KiB 解码门、输入 SHA 不变，以及同等级并行输出字节一致。
下表是小 fixture 的实际 zstd 编码字节，不能代表全链类别权重或生产空间比例。

| 小 fixture | 原始完整 V6 流 B | frames | fastest B | default B | better B |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4,096 account-v5 | 403,405 | 4 | 89,776 | 88,314 | 88,646 |
| 4,096 storage-random32 | 231,500 | 2 | 152,813 | 153,580 | 154,240 |
| 4,096 storage-counter32 | 231,500 | 2 | 20,225 | 18,216 | 18,184 |
| 4 delegation-list，每版 8,192 peers | 721,401 | 6 | 721,515 | 721,497 | 721,497 |

原始输出：[smoke.txt](../../build/benchmarks/20260907-compression-levels/smoke.txt)。
在这些具体字节上，better 比 default 的账户/随机 storage 输出略大；计数器
只额外减少 32B；随机大列表没有获得有意义的额外压缩。这些结果已经足以
否定“只要提高等级就必然减少磁盘 I/O”的假设，还不足以替真实主网样本选档。

## 与生产 V2 相同选项的短计时

`BenchmarkHistoryCompressionLevelsProductionOptions` 保留同一完整 V6 流与
128 KiB 独立 frame。default 编码器只传 `WithEncoderLevel(SpeedDefault)`；
解码器只传 `WithDecoderConcurrency(0)` 和 `WithDecodeAllCapLimit(true)`，
与当前生产 `cbCodec()` 相同。其它等级仅改变 EncoderLevel，不额外修改
window、single-segment、CRC 或内部并发池。1/4/8 表示调用 `EncodeAll` /
`DecodeAll` 的外部任务数。原显式 128 KiB window 对照仍保留为独立基准。

`TestHistoryCompressionLevelsProductionOptionsMatchFactory` 用 1、17、1,024、
1,025、131,071、131,072B 输入核对 default 与生产编码器逐字节相等，并用
严格限制的独立解码器还原；用时 1.020s。极小无 FCS frame 仍受实际目标
slice 容量与独立 decoder 的 memory/window 门约束。

2026-09-07 在本地 Apple M1 Max、darwin/arm64、GOMAXPROCS=8 的专用 CPU
窗口执行以下命令，PASS，总测试时间 7.038s。每个 case 10 次、重复 3 轮，
下表为每个完整 fixture 的中位数；它是短微基准，不是主网吞吐保证。

```sh
GOMAXPROCS=8 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkHistoryCompressionLevelsProductionOptions/(account-v5|storage-counter32)/' -benchtime=10x -count=3 -benchmem -timeout=180s
```

先比较单外部任务的等级效果，时间单位均为毫秒：

| 合成 fixture | 等级 | 压缩 frame 总 B | 编码墙钟 | 编码 CPU | 解码墙钟 | 解码 CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 32,768 account-v5 | fastest | 697,875 | 9.6822 | 9.6567 | 3.6308 | 3.6176 |
| 同上 | default | 706,200 | 11.7905 | 11.7475 | 3.0893 | 3.0815 |
| 同上 | better | 706,714 | 22.4466 | 22.3582 | 3.1744 | 3.1472 |
| 32,768 storage-counter32 | fastest | 171,022 | 3.7896 | 3.8485 | 1.8022 | 1.8002 |
| 同上 | default | 143,250 | 4.7295 | 4.7779 | 1.8646 | 1.8595 |
| 同上 | better | 143,435 | 10.4577 | 10.4283 | 1.8422 | 1.8400 |

在这两个具体输入上，better 分别比 default 多 514B、185B，同时编码 CPU
约为 default 的 1.90、2.18 倍，没有提高等级的依据。fastest 的账户输出
更小，但计数器输出比 default 大约 19.39%，因此也不能据此统一改为 fastest。
本次建议保持现有生产 default 等级。

default 等级的并行对照如下；同类别的 1/4/8 任务输出字节完全一致：

| 合成 fixture | 外部任务 | 编码墙钟 ms | 编码 CPU ms | 解码墙钟 ms | 解码 CPU ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| account-v5 | 1 | 11.7905 | 11.7475 | 3.0893 | 3.0815 |
| account-v5 | 4 | 3.1073 | 11.7662 | 0.9244 | 3.4033 |
| account-v5 | 8 | 1.9542 | 12.0732 | 0.5849 | 3.5510 |
| storage-counter32 | 1 | 4.7295 | 4.7779 | 1.8646 | 1.8595 |
| storage-counter32 | 4 | 1.3574 | 4.7512 | 0.5840 | 1.9946 |
| storage-counter32 | 8 | 0.8520 | 4.9486 | 0.3843 | 2.0762 |

并行降低的是这里的 codec 墙钟，不减少同配置输出字节。真实 writer 还包含
串行分块、hash、去重、目录、设备 I/O 和同步，不能把本表加速比直接用于
主网同步或 CDC 流水线。原始结果和全部等级/任务的中位数分别见
[production-options-bench.txt](../../build/benchmarks/20260907-compression-levels/production-options-bench.txt)、
[production-options-summary.json](../../build/benchmarks/20260907-compression-levels/production-options-summary.json)。

## 后续接入验收

由根任务协调空闲时段，先编译测试程序，再逐配置单独计时，避免编译 CPU
混入编码 CPU/max RSS。例如：

```sh
go test -c ./core/state/snapshots -o /tmp/gtron-compression-levels.test
GOMAXPROCS=8 /usr/bin/time -v /tmp/gtron-compression-levels.test -test.run '^$' -test.bench '^BenchmarkHistoryCompressionLevels/account-v5/better/workers8/encode$' -test.benchtime=1s -test.count=5 -test.benchmem
```

Linux 用 `/usr/bin/time -v`，macOS 改为 `/usr/bin/time -l`。轮换等级与并行度
执行顺序；明确记录 CPU 型号、核心数、GOMAXPROCS、内存限制及本次代码版本。
矩阵为 4 类 × 3 档 × 3 并行度 × encode/decode，先单独选 case 再按需要扩展，
无需在受压生产磁盘上运行任何实验。

正式选择需补充真实 copied trio：小账户主导、随机 storage 主导及大列表主导
三个区间都测，继续保留同一个 production writer 配置的基线。必须同时核验：

1. 相同逻辑内容与完整查询结果；完整 `.seg + .kv + .idx`、locator 和 metadata
   的物理字节，不能只报告压缩 payload。
2. build、merge、随机账户/存储查询和历史范围查询的 CPU、墙钟、RSS 与读取
   放大；CPU 占用提高不能阻塞同机重要任务。
3. 新文件更小能否减少实际设备写入和全流水线耗时，而非仅转移 CPU/缓存成本。
4. 并行输出确定性、pending anchor 引用、取消/失败后 join、临时文件清理及
   Sync/rename/目录 Sync；增加 codec 等级不能削弱恢复或全历史保留要求。

在这些证据齐全前，不推荐全库统一切换 better/best，也不承诺更多核心能
永久解决固定容量磁盘的历史增长。
