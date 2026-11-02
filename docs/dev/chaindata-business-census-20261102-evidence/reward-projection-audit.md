# Votes-only 新语义投影：最终独立复核（2026-11-02）

结论：该固定 cohort 的 Snappy SST **4,512,508,220 → 823,022,838 B**，净少 **3,689,485,382 B / 81.76129997%**，文件缩小 **5.48285 倍**。这是真实同输入写出结果，明显强于保全完整 Account 的无损候选（净省 0.746 GB）。但收益来自有源码读集依据的**语义投影**，不是完整 Account 的无损压缩；不能保全旧 MuHash 或完整历史 Account 原值，不可直接安装生产。

## 独立核验范围

只读取本地已下载结果并与先前 baseline 交叉核算；未重新跑全库、MuHash、恢复或性能测试，未改任何 Go 源码。

- 新结果：`build/benchmarks/20261102-chaindata-census/final-evidence/reward-votes-projection-results/results.json`
- 新结果 SHA256：`bbffb5c883306ee521b01fe7fd74dddd936f87fb48981dcc8b7e25492fa42a52`
- baseline 结果 SHA256：`eaa0b262f516e7cf9051704189af3ebc6f14802853bfab402f501f0bff381260`，与新结果绑定值一致。
- 全输入流 SHA256 均为 `512ccfce04f593357cf8423031de0dd53c1a2709d47ca8e409c6d516dc708cc3`，流长 **10,532,380,397 B**、行数 **19,724,230** 一致。
- 相同原输入 64 MiB 切段，均 **157 个 SST**；4096 B 数据/索引块、Bloom10、PebbleV4 一致。candidate 仍写全部原 key/row，没有删除旧周期键，也没有做单槽合并。
- subtype 行数、原 wrapped 字节数全部与原结果一致。account-vote 以外的 candidate wrapped 字节数原样不变。
- 条件上界/单槽收益没有加进本次净收益；这次测得的 3.689 GB 仅来自投影。

## 内容及正确性证据

- **12,726,979 个 account-vote 快照**，包含 **19,363,802 个 Vote 条目**。
- 投影工具逐行对比原 native Votes 与候选对象的数量、顺序、原始地址、int64 票数及每个 Vote unknown；mismatch **0**。
- 候选仅 Votes 的 Account-native 解码/再编码 mismatch **0**，row presence mismatch **0**。
- 该真实 cohort 的空 Votes 快照为 **0**、Vote unknown 总字节为 **0**；非零 unknown 与 present-empty/missing 的路径由本地 fixture 覆盖，不能宣称真实数据包含了这些边界。
- Snapshot native 8,841,164,502 → 889,295,863 B，逻辑减少 **89.9414%**。该逻辑比例不是物理文件比例。
- 新候选较前一无损候选 3,766,082,494 B 再少 **2,943,059,656 B**。

这些 mismatch 检查针对 **Votes/presence 语义**，不是完整原 Account 字节一致。结果明确 `full_account_byte_equality_claimed=false`、`production_muhash_verified=false`，两项不能被省略后宣传为“完全正确/可直接上线”。

## 抽样性能边界

本次样本是对**全 account-vote 流**的 seeded Algorithm-R reservoir，4,096 个样本 / 28,672 次操作，约 3.11 MB retained bytes，32 MiB cap 内存拒绝数 **0**。因此没有前一工具“收满后停止”的早期 cycle 前缀偏差；它仍是样本，不是逐行性能 census。

测量为：native Account 解码后生成相同的确定性 **Votes-only 语义结果**。baseline 解码完整原快照，candidate 解码投影快照，交错 AB/BA；Votes-only 返回字节 mismatch 0。

| 微测 | baseline | candidate | 降低 |
|---|---:|---:|---:|
| mean | 19.3418 µs | 2.9304 µs | **84.8495%** |
| p50 | 12.061 µs | 1.931 µs | 83.9897% |
| p95 | 46.392 µs | 5.195 µs | 88.8019% |
| p99 | 89.816 µs | 12.350 µs | 86.2497% |

这不是完整 Account 返回 API、HTTP eth_call、Pebble 点查或冷缓存性能。也不能把旧实验的完整 API 54.96 µs 与这里 2.93 µs 直接当作同一个接口提速。新语义能够避免解码无关资产和权限，微测支持它具有降低 reward 专用读开销的潜力；同步与历史 eth_call 的实际收益仍需产品接入后固定副本验证。

## 推荐

值得进入独立的新状态模型设计/实现阶段，优先级高于继续打磨只省 0.746 GB 且增加完整解码成本的无损候选。正式接入必须：

1. 保持 reward 的 Votes 顺序、重复项、计数、地址及独立 presence；明确 typed API，避免空 protobuf 被误当成缺失。
2. 定义新的 MuHash/snapshot scheme 与 source schema 身份。原完整值无法重建，不能保留旧 root 或混用旧数据。
3. 验证 actuator 新旧算法、TVM VI 算法、周期边界、历史 reward 查询、unwind 和 snapshot restore。当前静态 Java 读集审计和 Go fixture 不是这些产品验证的替代品。
4. 明确处理完整 Account `dbcompare`、snapshot allowance/RawHex 诊断及任意旧 cycle API。不能通过静默忽略字段把变化伪装成全值兼容。
5. 单槽模型另行 join snapshot owners 与 begin/end cursors；本次没有 owner distinct/liveness 删除证明，没有执行 GC。

独立机器核算见 `independent-final-audit.json`；源码调用点和单槽条件详见同级目录外 `20261102-reward-state-compact/reward-account-vote-readset-20261102.md`。
