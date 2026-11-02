# ContractStorage SPG2 主网副本独立审计（2026-11-02）

**建议：放弃当前 SPG2 的生产集成，保留离线原型和证据。** 同一 cohort 的完整 SST 虽减少 4.269%，但减少量全部由完整逐行 Bloom 缩成页级 Bloom 解释，非 filter 部分反而增加 55,980 B。校正为真实线上 GetCursor/L6 Bloom 策略后，负查和复用 scope 的后续查询明显退化。单次全冷首查和部分正查有收益，因此不能表述为“每一种查询都变慢”；也不能用这部分收益声称完整 HTTP eth_call 已改善。

本审计只读取封存 JSON、源码身份和摘要；未修改冻结 Go、SST、数据库或线上服务。原始结果来自副本运行，并已按本地 archive-file-manifest 的 SHA256 重新核对。独立重算详见 [mainnet-audit.json](mainnet-audit.json)。

## 同数据空间与字节还原

| 指标 | baseline | SPG2 | 净减少 |
|---|---:|---:|---:|
| 全部 Snappy SST 长度 | 56,409,387 B | 54,001,367 B | **2,408,020 B / 4.2688285%** |
| inode 去重分配量，含实验 DB 控制文件和目录 | 56,438,784 B | 54,030,336 B | **2,408,448 B** |

物理分配基线是 **56,438,784**，不是终端截图可能误读的 54,638,784；因此净省 2,408,448 B，而非 608,448 B。

两臂均为同一 2,000,000 行，按同一 canonical input-byte cut 分成三个文件，4 KiB block、restart16、Snappy、Bloom10。候选最多 128 行、页值上限 4000 B，小于八行保留普通行。

选定输入与全部页逐行还原的长度帧 SHA256 同为：

`c4c1efbb06b918ebf776fc63c4a20beb89f8305c367d4c718e0865c8c25b938c`

这是 **cohort 中完整旧 key/wrapped-value 字节还原**，不是新物理格式与旧 MuHash 根自动相同的证明。没有生产 schema、可变 overlay、MuHash source 枚举或快照格式集成。

## 性能基线与正确性

先前 frozen ed335 的普通 `DB.Get` 不启用 L6 filter，不能代表线上 `0096c0af` foreground storage。新增独立 v2 控制复用相同 SST，用 `Snapshot.NewGetCursorWithOptions(MaxRetainedTables=16, UseL6Filters=true)`；candidate 保持正确的范围 `SeekGE`，没有错误套用 exact-key `SeekPrefixGE`。

两个 DB 都实测只有 **L6 三张 SST**。GetCursor 的请求、打开、复用计数符合 scope1/group32 协议，无 fallback。它代表生产点查策略的低层控制，**不是线上多层 LSM、VM 或完整 HTTP 请求回放**。

所有 24 个结果组 complete，逐查 presence/wrapped bytes 均相等。暖样本每组 4096 查询，2081 命中/2015 未命中；冷样本使用同一查询前缀的 128 查询，66 命中/62 未命中。每种冷热模式下，全部方法/分组/轮次的输出 SHA256 各自一致。服务器报告的 query stream SHA 已封存；原始 query stream 未随本地证据包下载，本审计没有声称在本地重新计算该二进制流。

下面将 AB/BA 两轮按相同查询数合并。scope 生命周期包含 snapshot/cursor 创建、查询与关闭，DB-open 不计入；CPU 为进程观察值，查询循环还包括计时、核对与结果哈希。

| 缓存与 scope | 每查平均生命周期，baseline→SPG2 | SPG2 总 CPU 变化 | SST ReadAt 字节变化 | SST ReadAt 次数变化 |
|---|---:|---:|---:|---:|
| 暖、fresh1 | 12.225→13.428 μs | **+11.02%** | **+71.12%** | +33.18% |
| 暖、group32 | 8.919→10.205 μs | **+14.45%** | **+73.91%** | +35.29% |
| 每 scope 冷、fresh1 | 3.463→1.992 ms | −72.79%，离散明显 | −99.05% | +3.50% |
| 每 scope 冷、group32 | 1.307→1.386 ms | −8.88% | −92.87% | **+71.73%** |

负查是关键退化项：

- 暖 group32：**1.957→9.211 μs，4.71 倍**；每轮负查 SST 读取 **20,314→4,756,594 B**，调用 **8→1331**。完整逐行 Bloom 能排掉绝大多数不存在的 key；页末键 Bloom 不能替代这一能力。
- 冷 group32：**0.532→1.395 ms，2.62 倍**。两轮合并负查读取次数 **60→318**。字节量虽小，随机读取次数明显增多。
- 冷 group32 的后续查询，两轮分别均退化，合并 **1.003→1.357 ms，+35.23%**。

正查也有真实改善：暖 group32 正查均值 **13.820→9.282 μs**。因此测试的约 49% 负查混合比例不能冒充真实业务比例；当前证据不支持“线上所有查询一定慢 14%”。

单次全冷首查优势主要来自避免反复加载完整 L6 Bloom。baseline fresh1 每轮 128 查读取 **113,800,436 B**，candidate **1,084,763 B**；一旦 scope 内摊销 filter 加载成本，优势就反转为更多负查随机读。冷 group32 每轮仅四个 scope，首查样本很少，基线两轮首查均值 **3.509/17.864 ms**，存在明显共享盘/调度噪声；不能挑最好一轮或只看字节减少。

## cohort 不代表全域

全域 ContractStorage 有 **422,867,214 行**；本 cohort 为按物理 key 顺序取出的前两百万行，仅 **0.472962%**，不是全域随机抽样。

这里的行是固定 Pebble reader 当前可见的物理 source 行，包含全量导出中的不同 generation/owner 状态；本 cohort 没有额外按 active generation 过滤，也不是生产 SLOAD 请求轨迹。

- 单一 owner `410170214e23a16d0ea8a9fd0332cee99e2fa4829c` 占 cohort **71.206%**。
- cohort 21 B wrapped value 占 **54.8907%**，全域为 **18.0124%**。
- cohort 平均 wrapped value **14.847645 B**，全域 **10.990455 B**。
- 全域最大 owner 有 **31,558,143 行**，不在这个前缀 cohort 中。

因此不应把 4.269% 乘全域行数、逻辑字节或之前 8.999 GB 物理下界来预测整库回收量。若将来重启该研究，先按全域 dominant owner/值分布选固定多 cohort，并计入完整 negative filter、可变 overlay 和写放大成本；本轮不追加大扫描。

全域 generation 分类闭合：active **420,346,288**，obsolete generation **1,524,014**，owner missing **996,912**，future **0**。非 current 两类合计仅 **0.596151% 行**、**188,134,626 B 未压缩 physical-key+wrapped-value 逻辑量**，不是相同数量的磁盘回收量，更不是删除许可。MuHash 和快照可能依赖原物理 source 集合，不能仅凭 owner/generation 分类清掉这些行。

## 集成取舍

当前 4.269% 的 cohort 空间收益不足以覆盖负查退化、scope 后续随机读、mutable 页更新和协议改造成本。直接 SSTORE 更新页会产生最多 128 行的 read/modify/write；引入 overlay 后还需解决合并、崩溃发布、generation、历史、MuHash 和恢复边界。现有字节还原/lookup 控制不能替代这些产品验证。

故本版 **不合入运行时、不迁移线上、不部署**。优先继续已找到的 schema-preserving 大头及其他业务中真实较大的 value 压缩收益。

## 最终归因：表面节省全部来自负查 Bloom 缩水

已只读六个实验 SST 的 footer/metaindex/properties，合计读取 **5,495 B**，数据区读取 **0 B**。各文件长度与先前空间结果逐一闭合；属性文件 SHA256：

`5907f792442e59e7c6cf867108ad26181cae4b9210e03c32e22d391501a95d20`

| 压缩文件的实际分区 | baseline B | SPG2 B | baseline−SPG2 B |
|---|---:|---:|---:|
| 数据块及其 trailer（DataSize） | 53,399,635 | 53,670,379 | **−270,744** |
| Bloom block（FilterSize） | 2,500,175 | 36,175 | **2,464,000** |
| 其余物理字节：索引/元数据/footer 等 | 509,577 | 294,813 | 214,764 |
| **文件合计** | **56,409,387** | **54,001,367** | **2,408,020** |

这里的 DataSize 在该 writer 中于数据块全部写完时直接取物理文件位置；FilterSize 取未压缩 filter block handle 的长度；其余是文件长减前两项的残差。**没有把 IndexSize property 当作压缩后物理字节**——它在当前 writer 中由 index 估算长度产生，不能直接相加。

baseline 有 **2,000,000 个 SST entries**，SPG2 变为 **28,835 个 page/raw entries**。Bloom 因此不再代表完整行键，缩小 **2,464,000 B**，是全部净节省的 **102.325%**。除去 filter 后，baseline **53,909,212 B**，candidate **53,965,192 B**，反而增长 **55,980 B**。

若维持其他分区不变，仅把 baseline 的完整逐行 Bloom 长度补回替代页级 Bloom，candidate 就会达到 **56,465,367 B**，已比 baseline 大 **55,980 B**，尚未计入新增完整行过滤定位协议的元数据和维护成本。

所以这不是已经证明的有效载荷压缩收益，而是削减逐行负查过滤能力获得的表面空间收益，与暖负查 4.71 倍退化一致。**明确否决当前版本；无需为了这份原型继续做产品 MuHash/恢复集成或追加大型性能实验。** 其字节还原与局部正查改善仍可留作未来研究证据，不能改写为本轮成功上线候选。
