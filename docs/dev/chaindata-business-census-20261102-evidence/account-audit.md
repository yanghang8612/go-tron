# 账户 KV 独立复核（2026-11-02）

依据本地已取回的 `20261102-chaindata-census/business-evidence` JSON；未用终端 OCR 作最终数值依据。独立复算文件为 `verified-real-results.json`、`production-control-analysis.json`。

## 全域 census：权限才是账户数据大头

全扫描 2026-11-02 04:39:49–05:27:44 UTC，Pebble 合并可见 KV **1,266,853,461 行**、账户 **172,936,016 行**。逐域 all 与 current/obsolete/future/owner_missing 的行数及五项字节数闭合；域总数、导出行数和 framing 字节闭合。

**本地没有完整 4.8 GB owner fanout 流和全部 contract-owner 原流**，故这次只独立闭合 JSON 汇总，没有宣称重新汇总这些全流。源码扫描器已用小 fixture 验证三路合并、分类和导出，二者是不同证据。

| 域 | 可见行数（含非当前 generation） | 物理 key 的逻辑长度 B | wrapped value B | key+value B |
|---|---:|---:|---:|---:|
| AccountPermissionAux | 338,943,975 | 12,540,936,909 | 20,852,985,751 | 33,393,922,660 |
| AccountAssetV2 | 360,435,453 | 14,777,853,573 | 1,344,734,251 | 16,122,587,824 |
| AccountResourceAux | 111,382,602 | 3,898,391,070 | 2,732,590,783 | 6,630,981,853 |

以上是合并可见行的编码长度，**不是压缩 SST 物理占用**；不按这些比例摊此前混合块的 GB。

- 权限有 169,462,448 个 owner，其中 **169,455,802 个恰为两行，99.9961%**。45 B wrapped 长度有 **169,383,159 行**；65..128 B 桶有 169,490,530 行。符合 Owner+Active 的候选形状，但长度/扇出不能证明 SELF 或严格默认模板，需新 permission 探针逐值确认。
- 资产 V2 有 96,282,833 个 owner；40,422,319 个单行、19,212,350 个两行。不能默认每个 owner 都能组成满 8 行包。
- 资源有 111,382,601 个 owner，除一个 owner 两行外均为一行。
- generation 表/账户 envelope 冲突 **0**，缺失非零 account generation **0**，未来 generation **0**。表缺失且账户 generation 为零有 172,929,438 个，符合显式扫描的隐式零情形。
- 所有域旧 generation 共 **1,525,126 行 / 112,639,592 B key+wrapped value**；owner 缺失共 **1,385,063 行 / 127,646,279 B**。合计编码 240,285,871 B，不能当作 SST 可回收量，也不能自动删除；当前 MuHash source 包含这些物理行。

## 同一 500k 前缀 cohort 的空间对照

| 候选 | 编码 | baseline SST B | candidate SST B | 净节省 |
|---|---|---:|---:|---:|
| Resource 稀疏 value | None | 33,613,511 | 28,707,663 | 14.5949% |
| Resource 稀疏 value | Snappy | 17,606,413 | 15,831,034 | **10.0837%** |
| Resource 稀疏 value | Zstd | 14,787,203 | 14,770,586 | **0.1124%** |
| Asset V2 最多 8 行包 | None | 15,280,099 | 11,376,388 | 25.5477% |
| Asset V2 最多 8 行包 | Snappy | 8,308,251 | 7,613,591 | **8.3611%** |
| Asset V2 最多 8 行包 | Zstd | 7,796,680 | 6,762,856 | 13.2598% |

两组都是 owner 排序的前缀，非证明代表性的全库样本。资源改变 value 编码但保留 key；资产将 500,000 条原行变成 153,312 条存储行，其中 95,631 个包，需要新定位路径。

## 生产读取策略补测：资产 bundle 现形暂缓

旧 SST iterator `SeekGE` 对照未覆盖生产 L6 exact full-key Bloom 负查，不能作为上线依据。新控制将原 SST 独立复制后 ingest 至真实单 L6，保留原 SST，不重写/compaction；baseline/resource 均用 `Snapshot.NewGetCursorWithOptions(16,true)`，bundle 使用正确范围 Seek。固定 32 MiB 应用缓存、10,000 次同请求序列、AB/BA 两轮，各点返回值/存在性均核对。

以下为 Snappy、group32 scope、两轮均值；首次为空应用缓存，**OS 缓存不受控，不是 HTTP/整库冷查**。

| 候选/负载 | cache | baseline / candidate ms | 候选耗时比 |
|---|---|---:|---:|
| Resource 正查 | 初次 | 94.205 / 90.737 | 0.963 |
| Resource 正查 | 暖 | 27.494 / 29.358 | **1.068** |
| Asset 正查 | 初次 | 75.077 / 81.070 | 1.080 |
| Asset 正查 | 暖 | 26.757 / 45.243 | **1.691** |
| Asset 同 owner 负查 | 初次 | 8.941 / 95.617 | **10.694** |
| Asset 同 owner 负查 | 暖 | 6.968 / 46.716 | **6.705** |

Asset 初次负查 VFS 字节 **976,569→7,246,799 B（7.42 倍）**，ReadAt **161→2,702**；旧表 Bloom 拒绝 9,883/10,000，新范围查 0。每轮 group32 初次负查比为 9.83/11.56，暖为 6.02/7.38，方向一致。8.36% Snappy 收益不足以支持当前 bundle 路径。

Resource 两臂同一负查均拒绝 9,884/10,000，key/filter 策略保留；但暖 group32 正查两轮均变慢（1.052/1.084），其他短窗口有明显离散。可作为进一步小型 value-codec 研究，不能宣称整体执行提速；Zstd 空间收益尤其有限。

## 权限方向与后续边界

生产 `appendAccountPermission` 没有 owner 参数，完整写出 key address；并未覆盖账户 core 已有的 row-owner 地址省略。当前默认 Owner/Witness/Active 的 native 长度为 44/46/77 B，对应 wrapped 45/47/78 B。

独立 `permission-probe` 保持全部 physical keys、exact cursor 和显式存在行；仅在重建 native 与原字节完全相等时接受 SELF/默认形状模板，未知字段 raw fallback，Active 保留确切 32-byte ops 或严格不可变字典 ID。它不会读取当前 DP 来填旧权限，亦不会删除默认行。

### 权限真实 500k 前缀实测及独立回执复核

最终 JSON SHA256：`869824cb10c1ca37f19ed0e0d1f9251190b128a074ee37e427fc2284750b9ede`，本地文件为 `20261102-chaindata-census/final-evidence/permission-results/results.json`。独立派生汇总为 `permission-real-verification.json`。

- 仅迭代 **1,371,677 条 KV** 得到 500,000 条 PermissionAux，未重扫全库；处理原 key/value 99,864,462 B。
- cohort framing **53,261,305 B**，物理 key 18,500,012 B、old wrapped value 30,761,293 B、candidate wrapped value 11,469,108 B；行数、精确长度 histogram、ops 频次、模式计数及 framing 全部闭合。
- 严格模板 499,728 行；SELF 地址 499,954 次；226 行为只改 SELF 的 custom，46 行 raw fallback；样本 unknown 为 0。模板识别靠重建原 native 的完整字节匹配，非依据长度猜测。
- Active 有 248,257 行保留 exact raw 32-byte ops，只有 1,611 行匹配已知常量；结果直接说明不能用查询时的当前 DP 替代旧权限 bitmap。
- Snappy 完整文件 **11,990,650→8,107,628 B**，净省 **3,883,022 B / 32.383749%**。两臂真实 L6 Bloom 均 **625,093 B**，原 SST 保留、独立复制后 ingest 未重写；空间差不来自删 key 或移除过滤器。
- 已封存 producer 对全部 500k 行执行 compact→expand→原 native 字节相等检查，回执为 true。本地独立复核的是代码、SHA 回执和上述汇总闭合，**没有再次下载并重放全部 53 MB cohort**。

点查为**每整批 10k 查询共享一个 production GetCursor**，32 MiB 应用缓存、AB/BA 两轮；不是前述 request/group32 协议，不是完整 HTTP，也不是 OS 冷查。以下为两轮均值：

| 权限负载 | cache | baseline / candidate ms | 候选耗时比 |
|---|---|---:|---:|
| 正查 | 初次空应用缓存 | 66.069 / 60.445 | 0.915 |
| 正查 | 暖应用缓存 | 28.286 / 30.217 | **1.068** |
| 负查 | 初次空应用缓存 | 11.591 / 10.005 | 0.863 |
| 负查 | 暖应用缓存 | 7.718 / 6.705 | 0.869 |

正查初次 VFS **7,462,245→6,489,269 B（-13.04%）**；暖正查两轮均 +6.8% 左右，候选每批新增约 1.84 MB / 44,988 次分配。负查两臂均 Bloom 拒绝 **9,891/10,000**，两轮耗时有离散，不宣称稳定负查提速。

若仅作 1:1 正负合成均值，初次为 **38.830→35.225 ms（-9.28%）**，暖为 **18.002→18.461 ms（+2.55%）**。这不是实际请求权重或全节点吞吐。空间收益明确，暖正查的展开成本仍需在真正接入时优化；本轮不再扩实验。

若 value 模板收益仍有限，下阶段“默认 Owner/Active 显式内嵌 account，custom 仍在 KV”有结构性潜力：权限点查前已读取 account，可少额外 KV 行和点查。但要区分 absent/inline-present/external-custom，设计新 MuHash source 与历史投影，防止不变 permission/ops 随每次余额更新重复进入 account before-image/history。本轮不扩展此原型，也未证明其净收益。
