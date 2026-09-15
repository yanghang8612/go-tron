# R1 历史引用容器上线与首段迁移

2026-09-15，时间均为 UTC。新 writer 已上线，首个旧 trio 在原库就地迁移成功，
没有复制数据库。**首段迁移使体积增加，完整 10 分钟线上窗口基本持平，不能称为持续积压
已经彻底解决。暂停主动全量旧历史迁移**；旧冷文件可兼容读取，无需全部转换后才使用新 writer。

## 版本和实际切换

- 源码：`92767f83c54288210ec576cb7a61c1c972653b0a`。
- 操作脚本：`e3fcdafe2d4eaab73e16aecf7c47a8c71d03e2ca`，两版本均已推送。
- Release：`/data/gtron/releases/20260915-history-reference-v2`。
- 二进制 SHA-256：`0a9b878269fce399c6dc0a6c683908d6a4d0c323d2e6326b214216808f727e2f`。
- 原生准备与迁移退出码均为 0；切换操作结束于 08:58:11.670。
- 新进程 PID 4378、ticks 4519717282、process start `1789462679310623368`，启动观察 head 32,302,004。

切换操作总区间为 122.0894 秒，包含迁移及服务操作；它不是精确的服务不可用时长。
09:13:50 的 manifest generation 44,300 中有 47 个 R1 history / 3,545 个 history，
说明运行中的新格式库存已增加，不意味着主动迁移了 47 个旧段。

## 原生固定输入对照与迁移成本

08:52:10–08:53:34，同一二进制、同一份已认证的 16 块私有热输入，两个模式使用
R4/shared chunk cache，旧模式 CDC1；每模式 6 次完整构建的中位数为：

| 指标 | 原路径 | R1 | 变化 |
| --- | ---: | ---: | ---: |
| 完整构建耗时 | 0.6648537875 秒 | 0.533621421 秒 | −19.7385% |
| 完整 trio 文件长度 | 12,421,391 B | 13,225,799 B | +6.4760% |

完整逻辑 digest 一致、输入前后不变。该结果是热范围完整构建对照，**不是线上同高度
加速比例**，不含整个发布/裁剪/合并/GC/恢复周期，也不是旧冷文件转换速度或体积改善。

08:56:25.864–08:57:58.708，原库 txNum 1–1,659,775 的一个 trio 就地迁移成功。
迁移函数耗时 91.508877 秒，命令耗时 92.843555 秒；当次 3,514 个 trios 中完成 1 个，
剩余 3,513 个，journal 已清除。源三个文件已删除：

| 首段迁移指标 | 实测值 |
| --- | ---: |
| 源 / 新 trio | 222,407,358 B / 376,210,416 B |
| 净增加 | 153,803,058 B，+69.1538% |
| 逻辑 history / 记录 | 776,435,137 B / 6,707,959 条 |
| 新 chunks / spans | 5,924 / 5,924 |
| accessor / index | 均为 V7，保持原字节 |
| 删除源文件 | 3 个，共 222,407,358 B |

源删除不等于净节省空间。该段新 history 更大；不能用一个早期大记录量段的耗时或
增长比例外推全仓。分层存量、空间模型和完整成本边界见[迁移成本报告](history-structural-migration-cost-20260915.md)。
源码已确认转换器将旧 Zstd 页解码后统一改为 Snappy/raw。两个 companion 字节不变，
history 本身从 148,229,628 B 增至 302,032,686 B；新增目录只有约 0.57 MB，不能解释
153.8 MB 的增长。后续压缩修复必须同时覆盖转码和混合格式合并，不能只修改迁移 CLI。

## 完整 10 分钟上线窗口

09:05:27.832–09:15:27.789，按计划 30 秒间隔采集，**21/21 点全部成功且同一进程**，
cold、shared GC、cold read 错误计数均保持 0。下表使用请求开始时间差 599.956161 秒；
每次请求耗时 1.570–2.309 秒，因此端点速率是近似值，不是服务器原子计时。

| 指标 | 开始 | 结束 | 增量 | 约每秒 |
| --- | ---: | ---: | ---: | ---: |
| head | 32,306,665 | 32,315,989 | +9,324 | 15.541 块 |
| cold published | 31,285,574 | 31,294,694 | +9,120 | 15.201 块 |
| eligible | 32,241,071 | 32,249,924 | +8,853 | 14.756 块 |
| eligible lag | 955,497 | 955,230 | −267 | −0.445 块 |
| Pebble `disk/size` | 332,164,194,305 B | 332,323,063,577 B | +158,869,272 B | +264,801 B |

这里 eligible 由同一次响应中的 `cold + lag` 派生；各 gauge 独立更新，不是同一批次的
原子记录。全窗端点只显示小幅净追赶，期末仍有约 95.5 万块积压，结论是**基本持平**。
不能将本窗与其他高度/负载窗口直接相除当作代码提升，也不能用原生 19.7385% 替代它。

同期 LSM compaction debt 从 2,518,196,292 B 增至 4,524,618,619 B，增加
2,006,422,327 B；这是压实估算工作量，不是文件占用或历史 lag。预算 duty 采样值
20%–90%、中位 80%；device busy 中位 96.2862%，await 中位约 1.221 ms，hard gate
采样值均为 0。rate-limit 延后计数增加 46，forced-busy build 增加 40。高 busy 和调度
限制说明需要继续观察设备工作与维护调度，**不能单凭这些值断言硬件不足或带宽耗尽**。
以上中位数是 21 个 gauge 样本的统计，不是时间加权占比。

窗口内 snapshot compaction merge 计数保持 6，未完成新合并，因此没有覆盖未来大段
合并成本；不能称完整维护周期已全部验收。09:13:50 的独立目录观测为 chaindata
332,153,798,656 B、snapshots 773,855,272,960 B、ancient 247,465,566,208 B，
可用空间 2,071,173,222,400 B。顺序 `du`、卷可用量与 Pebble gauge 口径不同，不能混算。

## 后续单点与剩余瓶颈

09:21:01.695 本地响应文件完成时的独立单点仍为同进程：head 32,321,508、cold
31,301,628、eligible cutoff 32,255,885、lag 954,257，cold/GC/read 错误均为 0；
Pebble `disk/size` 为 332,541,674,655 B。这是窗口结束后的单点，时间是本地响应完成时间，
不并入上面的 10 分钟窗口，也不作为新的稳定速率结论。

窗口结束后 09:16:51 开始的 CPU profile 持续 30.17 秒，采样 CPU 总量 75.48 秒。
`OrderedCommitmentPipeline.runPartition` 累计占 21.90%，Pebble `DB.runCompaction`
累计占 15.81%；这些累计栈可能重叠，不能相加。点读、LSM 压实及调度限制仍需优化，
归档构建不是唯一成本；单个 30 秒 profile 不代表全天比例，也不计阻塞 I/O 的墙钟时间。

下一步评估 Zstd 保帧转存和当次完整语义证明的安全复用，继续保留输入身份、完整认证与
失败处理。这两项**尚未实现或验证**，不能写成已获得的收益。继续使用新 writer，暂不
主动推进全量旧历史迁移；不需要为使用新 writer 清空数据库或先转换全部旧冷文件。

## 本地证据

- 原生终端观察摘要：[准备](../../build/benchmarks/20260915-history-reference/native-v2/prepare-observed.json)、[ABBA](../../build/benchmarks/20260915-history-reference/native-v2/abba-observed.json)、[首段迁移](../../build/benchmarks/20260915-history-reference/native-v2/migration-observed.json)、[目录库存](../../build/benchmarks/20260915-history-reference/native-v2/disk-observed.json)。完整原始服务器报告没有被这些转录摘要替代。
- 本地完整 HTTP 采样：[samples.json](../../build/benchmarks/20260915-history-reference/native-v2/online-window/samples.json)、[汇总及限制](../../build/benchmarks/20260915-history-reference/native-v2/online-window/summary.json)，各点原始 `metrics-00.json` 至 `metrics-20.json` 在同目录。
- 窗口外证据：[最终单点](../../build/benchmarks/20260915-history-reference/native-v2/final-observed.json)、[CPU 来源和 SHA](../../build/benchmarks/20260915-history-reference/native-v2/online-cpu-provenance.json)、[累计 CPU 栈](../../build/benchmarks/20260915-history-reference/native-v2/online-cpu-cumulative.txt)。
