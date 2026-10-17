# Commitment 深层读取补充实验（拒绝候选）

2026-10-17，基于 `462da0cbd04643d3ffa0ea4f86d08d6b54bc5dbf`。实验仅用 Go source overlay；没有生产实现修改，没有改缓存预算、准入、key、持久编码、root 或共识。产物位于忽略目录 `build/benchmarks/20261017-deep-io-followup/`。

## 两条具体机制

**收缩 backing**：当前 `setFlushedLocked` 可在私有 backing 中原位刷新较小的 value，长期保留旧容量并按容量收费。实验在 `cap-oldLen >= 64` 且 `cap/2 > newLen` 时放弃原位复用，改为已有的精确 clone/charge 更新路径。exposed backing、相同字节省略、epoch/version、canonical flush 成功条件、所有 namespace 与 CLOCK 规则保持不变。只改 resident flush refresh，没有同时修改 recycled value 分配。

**cursor 构造后重查**：session 首次 miss 后创建 cursor，然后取得 singleflight；另一 reader 可能在构造返回前完成并发布缓存，原 reader 仍会再次读取。实验在首次创建 cursor 后回到已有 cache probe。每个新 cursor 多一次 probe，不增加 payload/metadata 预算。单次 cursor 构造不做 SST I/O，正常路径窗口很窄；这不是已证明的线上主要开销。

`PrefetchBatch` 排序后虽允许重复 key，生产 `StatePrefetchPlan` 已按完整物理 key 去重，因此没有把此通用接口情况当成本轮机会。generation fallback 未发现能够安全省略 durable lookup 的已有覆盖证据，也没有添加猜测性短路。

## 输入与验证

沿用现有 1536 个合法聚簇 key 的真实 Buffer→ordered fold→commit→canonical FlushUpTo→Pebble pipeline；base32 KiB + tail64 KiB、64 partitions、32 read permits、prefetch 保留。每次从相同 durable seed Compact/reopen 后开始，8 个 block root 逐一对独立顺序 memory oracle。加入两种控制：`shrink` 第一块更新全部，第二块删除四分之三，后续更新幸存 key；`oscillate` 偶数块重建全部、奇数块删除四分之三并更新幸存 key。正常 hot/cold_scan/scan/delayed_read 四模式保留。

固定 Go1.25.5、darwin/arm64、CGO_ENABLED=1。`final-runs.json` 记录 baseline→shrink→cursor→cursor→shrink→baseline 顺序；每臂每模式10次，共180次pipeline、1440个root。相同模式的 seed SHA、operation SHA 和全部 root 一致。冻结 HEAD 的 Pebble production cache sources，避免其他缓存实验污染。构造/cache/Close 包含在 pipeline 计时，seed/oracle/Compact/reopen/forced GC 不包含；分配来自 runtime TotalAlloc/Mallocs 差值。

diagnostic 文件的 println 仅定位收缩事件，其时序/分配不能用于最终性能结论。最终 source 去掉诊断打印。诊断 shrink 模式5次合计299个新 clone、41368 B；oscillate 1229个 clone、170023 B。容量减少的逐事件总和不是同时驻留节约，更不是 heap/RSS 下降。

## 最终无打印结果

每格为10次中位数 `durable cursor reads / pipeline ms`；完整范围、deep6/7、returned bytes、分配见 `final-summary.json`。

| 模式 | baseline | shrink | cursor recheck |
| --- | ---: | ---: | ---: |
| hot | 440 / 18.423 | 440 / 17.977 | 439.5 / 22.422 |
| cold_scan | 820.5 / 19.767 | 820 / 20.436 | 821.5 / 25.579 |
| scan | 1220.5 / 42.721 | 1223 / 41.378 | 1224.5 / 45.248 |
| delayed_read | 1438.5 / 42.556 | 1444.5 / 45.570 | 1439.5 / 42.712 |
| shrink | 1196 / 27.655 | 1188.5 / 25.274 | 1196 / 28.358 |
| oscillate | 2843 / 47.736 | 2817 / 54.924 | 2844 / 48.842 |

全部最终pipeline都是36次SST ReadAt、127636 B。小fixture只需首次读取便能驻留Pebble cache，不能用于声称实际SST减少。收缩场景少量durable reduction没有转化为SST收益；oscillate时间回退，普通scan/delayed还有durable回退。分配较噪：shrink模式总分配中位3161868→3601128 B，oscillate5784148→5643176 B，不能用其中一格声称普遍减少分配。

受控cursor构造回调让peer在首次miss后、构造返回前完成。present/missing × foreground/prefetch 共4例由2次durable降为1次。若peer随后advance version并flush新value，另4例仍读2次，返回旧snapshot值，没有误用新缓存。此用例证明窗口存在，但不含真实SST延迟。额外overlap=1初筛每模式5次也无稳定durable或SST收益，零星36/37次ReadAt在两臂互见。

## 决策与限制

两个候选均拒绝，没有将其写入生产源码。没有为被拒绝代码添加旋钮、增加预算或进行部署；不以追加极端冷cache实验替代常态失败门槛。完整相关包结果在`package-runs.json`：baseline的blockbuffer/domains均通过；shrink的domains通过、blockbuffer ownership oracle因既有“私有flush backing必须复用”断言失败；cursor的domains通过、blockbuffer diagnostics因新增probe令no_resident由3变5失败。这些候选没有满足现有全包门槛，未改测试来掩盖差异；既无SST收益，不继续修补被拒绝原型。拒绝候选不宣称已满足上线全部fault/race/native验收。

本地wall time受ARM64与进程调度影响，不是mainnet吞吐结果。VFS ReadAt是文件系统接口请求，不是设备IOPS。当前证据既未证明收缩在生产中的占比，也未证明cursor构造窗口的生产触发率。

## 最后一次只读路径审查

`apply`按完整hash排序的contiguous nibble run单次下降；`applyOnHash`是唯一深层`GetBranchInto`，每个prefix只下降一次，`bufferedBranchStore`又先解析本fold的puts/dels。没有找到可以省掉的同block重复foreground下降。并发相同key已由session singleflight合并；已完成prefetch若因global snapshot-version fill guard拒绝发布，稍晚foreground确可能再次读取，但现有数据不支持为它新增持有结构。

重新核对线上完整baseline-before sample000→049：`cache/fill/version_changed`增加107591，`resident_newer`增加22，两者含义不同；前者包括所有source的fill attempt，不是独立prefetch→foreground重复key计数。prefetch durable1348458/useful1325257，差23201（约1.72%，不是严格的唯一key损失上界）；unused-capacity eviction仅173。高useful比例不足以支持引入复杂的session handoff并持有更多字节。没有把这些aggregate计数当成逐key因果证据。

现有`invalidations[slot]`只有slot局部counter，不能说明最后失效相对session snapshot cut的时间。安全放松global fill guard需要额外stamp元数据或重设计整个version/epoch模型；本轮没有足够收益证据授权这种复杂度，也没有修改guard。

当前硬边界是随机hash路径与持续首次访问的新分支：这些访问没有可复用的旧结果，固定1.5GiB application cache不可能覆盖不断增长的全部分支集合。BranchData的主要payload是高熵child hash，不能假设普通压缩能显著提高驻留密度。完整物理generation key、overlay与snapshot隔离、canonical flush优先和borrowed lifetime必须保留；不能以缩短key、错误复用旧snapshot、盲增预算或更换共识编码制造命中率或减少读数。本轮deep调查在此收敛，不继续微调准入/哈希/CLOCK矩阵。
