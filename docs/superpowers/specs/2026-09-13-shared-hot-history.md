# 热历史跨块共享片段

2026-09-13。状态：实现与验证中，尚未部署。用户授权解决重复大委托名单的热历史
写放大、必要时停机检查，沿用 master 和 GitHub 原生部署；保留全部历史查询。

## 目标与范围

此前块内 CDC 和冷 V3 继续可用。本次处理进入冷层之前、不同区块之间的大值重复。
采用同 Pebble 内按固定高度分桶的内容寻址片段，避免另建跨文件 journal 提交协议。
完整 RLP、逐交易时间、顺序、存在性、generation、任意 Prev 字节不变。
旧 raw/Snappy/v2 和正序号 repair 行继续可读；新格式逐块机会性写入，不在线重编码存量。
普通不具备原子写和一致读能力的 writer 保持原自包含格式。

## 格式与发布

共享 pack 是已有压缩 magic 的版本 3：物理块号、原始 RLP 长度、整包 SHA256、
片段数、每片段长度和 SHA256。上限仍为原始 128 MiB；片段为既有 Gear 算法的
8–128 KiB，最后一个允许更短。验证完整引用表再读片段，禁止引用链和跨桶引用。

桶宽固定为 1024 块，是格式常量而非运行参数。chunk key 为
`state-history-chunk-v1- || bucketID BE64 || SHA256(rawChunk)`，value 为版本、
raw/Snappy 标记、解码长度和正文。已有片段须解码校验并逐字节比较，不能依赖进程
内“曾见 hash”缓存。bucket metadata 为 `state-history-chunk-bucket-v1- || bucketID`，
`[1,0]` 表示 active，`[1,1]` 表示 retired；版本 1 对应固定 1024 块桶宽。

仅含至少一个 Prev≥128 KiB 的包尝试共享，首次版本作为种子。完整规划后比较
pack + 本次新增唯一 chunk KV + 新桶 metadata 与原 writer 选出的表示；最多允许
12.5% 种子开销，超出则完整回退，不能留下副作用。此限额不是每种输入都节省的保证。
桶宽/种子策略须通过保留真实高度的实盘样本验证，不把稀疏样本重编号制造局部性。

canonical writer 必须显式提供 atomic capability：当前 blockbuffer 层存在、base
支持原子 batch 及一致 snapshot。先把全部新片段和 metadata 写同层，pack 最后写；
任一失败沿既有 block discard。祖先层失败后后继层不会单独提交，引用不会越过失败
祖先。继承现有 Pebble NoSync 持久化语义，不声称增加每块 fsync 保证。

## 读取、回收、恢复

pack、repair、posting 与片段读取必须固定同一 base MVCC 和 overlay 拓扑。
显式 pinned marker 与 snapshot factory 区分借用/新建视图，不能仅凭 Get/Close
结构接口将 live DB 误认为 snapshot。普通 DB iterator 后再用 live Get 不被允许。
新格式损坏、错桶、缺片段、校验失败都返回错误，不能重试 legacy 或当成 missing。
冷构建多遍扫描复用同一视图，borrowed 回调期间完整解码缓冲区保持有效。

整桶回收必须同时满足：经过验证的完整冷覆盖；所有块/tx-range 和 retention
检查；既有 index→chain guard、canonical/Finish/solidified proof 和 settled-prefix
fence；flush 后新视图中整桶 changeset 物理范围为空（包括 repair/异常残留）。
同一个原子 batch 删除该桶 chunk 范围并标记 retired。范围/次数有界，忙或非空时
退让并重扫；GC 错误保留空间、记录错误，不把正常裁剪进度当作已释放字节。
retired 桶永不写新外部引用；旧高度 restore/replay 仍可写自包含旧格式。
旧 snapshot 通过 Pebble MVCC 保留其引用与片段，释放前物理空间可能暂时保留。

## 升级和验收

先准备可读 v3、writer 关闭的 bridge，再显式启用 writer。出现 v3 后不能直接回滚
到不理解它的 b130/fdfe。新 marker 不能约束旧程序，受控 systemd 启动需要独立验证
可读二进制；不得宣传任何旧 binary 都会自动拒启。reader bridge 保留旧开关与查询。

验证包括：同批失败/重开、祖先依赖与失败后继、重组、桶边界、旧桶重写、故障
GC 重试、旧 snapshot 并发 GC、全部 owning/borrowed/as-of/unwind/coldbuild/repair
等价性、严格解码预算和错误传播。离线导出新引用包须显式物化为自包含样本，不能
把引用裸包冒充可独立解码的文件。

收益计量包括 pack、唯一新增 chunks、键和桶 metadata 的总和；区分编码尝试、
buffer 发布、实际 DB 提交、SST 分配和压实后磁盘净变化。在线验证须在同进程内
采样真实共享触发与写入减少；保留输入负载差异，不能用块速变化直接宣称版本加速。
