# 账户冷历史缺口：分布与索引选择

当前选择独立分页增量表，放弃将 storage 的逐源 Bloom 布局直接移植到账户。
这是表示选择依据；本轮未实现或部署账户增量表，未证明线上首查已达到目标。

## 线上复测仍未达标

2026-10-28 17:15–17:16 UTC（北京时间 10 月 29 日 01:15–01:16），
同一线上 `52aea62e` executable、相同合约 getter，Mac 经 SOCKS5/Nginx
完整 HTTP eth_call。按高度顺序各三次，不清缓存、不启停服务。

| 高度 | 本轮首次 | 第二次 | 第三次 | 返回整数 |
| --- | ---: | ---: | ---: | ---: |
| 10M | 23.651981 s | 7.307200 s | 2.989723 s | 213 |
| 15M | 2.763493 s | 2.833459 s | 2.770055 s | 238 |
| 20M | 4.843954 s | 3.511225 s | 3.282896 s | 249 |

9/9 HTTP 200，无 RPC error，同高度逐字节一致；这不是独立历史回放 oracle。
版本前后相同，高度 47,640,683 → 47,641,599。每次发送前准备约
0.335–0.392 s，不能把余数当作纯服务端耗时。首次仅指本轮首次采样，
不是进程首次请求或整机冷缓存。

## 固定副本原生完整统计

输入保持既有 H45,836,827 完整一致副本。私有 ablation 只移除了账户索引的
覆盖 ID，旧上界行未改；本次研究不发布辅助索引，也不改变输入 manifest。
所有其它 active refs 和前沿逐项等于此前封存的 fixed-original manifest。
本次精确覆盖的缺口为 1,521 个 history/accessor pair，范围 Tx
3,792,723,933–4,063,356,397。副本的缺口分布不直接等同于当前生产分布。

原生 Sapling 试验源码 `b88a5650`，CPU quota 200%、MemoryLimit 8 GiB。
完整认证各源 accessor 与配对字典、遍历 distinct keys，耗时 305.063610 s。
没有重算全状态 MuHash。1521/1521 supported，没有 hash 数组预算排除。

| 指标 | 数量 |
| --- | ---: |
| AccountLatest distinct key 出现次数，逐源求和 | 179,761,510 |
| KVGeneration distinct key 出现次数，逐源求和 | 13,033 |
| 全部业务类型字典 key 出现次数，逐源求和 | 349,838,119 |
| 每源账户＋generation 键数：最小 / 中位 / p90 / 最大 | 26,523 / 113,075 / 174,849 / 251,785 |
| accessor 文件总长度 | 12,868,512,408 B |
| 字典研究 rchar / physical read_bytes | 33,256,174,472 / 12,866,572,288 B |

这些是逐源键数之和，不是跨源全局 distinct 账户数。研究 I/O 包含完整认证，
不等同于生产一次查询 I/O。

真实 HTTP AccountLatest 批次的两个键分别为 system owner
`41fffffffffffffffffffffffffffffffffffffffe`、contract owner
`41a4997124a328a4615eab437cede31da47a31b375`。四项控制/目标结果如下：

| 类型 / owner | 源内实际出现 | 已有 small/broad 跳过 | 尚待 canonical 查询 |
| --- | ---: | ---: | ---: |
| AccountLatest / system（真实 HTTP） | 0 | 1,521 | 0 |
| AccountLatest / contract（真实 HTTP） | 0 | 0 | 1,521 |
| KVGeneration / system（控制） | 0 | 1,521 | 0 |
| KVGeneration / contract（控制） | 0 | 0 | 1,521 |

主要缺口是合约账户 envelope，不是已被小路由排除的系统账户。
旧 base 对 contract envelope 的 bound 是 Tx20,418,041；该 bound 只能用于
旧 base 自身 covered IDs，不能凭本次分布直接扩展生产旧表的覆盖集合。

## 为什么选分页增量表

复用 storage 的 8 probes、至少 16 bits/key、向上取 2 次幂位图，逐源完整
位图累计 **525,860,864 B**。目标 contract envelope 有 1 个假阳性，
可排除 1,520 源；查询仍需认证所有这些位图。之前同副本账户 ablation
从 31 个缺口变为 1,521 个缺口，10M 首查的 rchar 从约 87.42 MB 增至
206.19 MB（额外约 118.77 MB）。526 MB 位图不是对该访问模式的读量改善。
这个结论针对上述布局，未排除未来分块认证 Bloom 的其它设计。

GTACRT01 独立表的 key 为 flat-domain + AccountID，值为本对象内 key 出现源
的最大 ToTx；完整 root metadata 认证后，按需读取并认证 4 KiB 内目录/数据页。
64 源一组，共 24 组（最后 49 源）。每组 union 超过 2M 内存 cap，故本次
**没有得到精确的表行数/文件大小**。被截断 union 的页位置不可用于性能结论。

利用已遍历的逐源键数之和作为去重行数上界，重新计算完整布局：

| 投影，不是实建测量 | 值 |
| --- | ---: |
| 24 组文件总长度下界 | 1,412,914,008 B |
| 24 组文件总长度保守上界 | 5,291,231,853 B |
| 两键各至多内目录＋数据页，含全部 root 与重复 header 读取 | ≤ 1,323,117 B |
| 仅未被已有路由排除的 contract envelope，同口径 | ≤ 1,126,509 B |

上界基于 GTACRT01 的 141 records/page、77 directory entries/page、
metadata `64 + 32*sources + 53*directories`，未把构建输出大小当查询读取量。
还没有引入输入字节/工作时间预算导致的拆批，实际产品可能超过 24 表；
不能据此保证端到端耗时或宣称已实现 1–2 秒线上冷首查。

## 实现约束

新增独立 delta kind，保留 base singleton 与既有 canonical 格式。流式 ETL
只写本批新源，正常维护不再调用全表 account refresh；每表保留自己的 exact
pair IDs、key absence/bound，禁止 coverage union 后复用旧 base bound。

查询在已有 base/small/broad 不能排除一个源时按需加载 delta；完成本请求键的
lookup 后关闭 FD，保留请求级 proof。metadata、驻留内存、对象数分别有累计
预算；缺页、坏页、未加载或预算排除都是 unknown，继续 canonical，取消传播。
delta 不挤占 base 的单槽 shared metadata cache。

发布只追加或替换明确指定的 delta refs，不能按包络重叠自动退休其它表；
持 read lease，并在 publication lock 下重新核对 canonical pairs 和待替换
对象。后续合并只允许有界 delta 组，绝不以 base 全量重写作 fallback。
需要测量合并削弱逐对象否定能力的影响，不能只以对象数减少作为验收。

构建/调度还必须限制声明输入、源数、字典数、排序内存、临时写量、输出和软
工作窗口。超预算源不算覆盖但推进调度扫描，硬错/取消/发布冲突不提交进度。
EOF 有洞保持退避，tail append 不反复解除失败前缀退避。旧全表 builder 继续
留作显式离线工具，不能因 delta 达限或失败被自动调用。

下一阶段先完成上述 reader/builder/publication 与 Runner 接入，再在同一
副本比较完整 HTTP、实际读量及构建成本。必须覆盖新账户、比旧 bound 更晚
的正例、不同 delta 的缺键/有键、pair 替换、损坏/预算回退和首源命中提前停止。
热尾未索引阶段仍是独立剩余瓶颈，不因本次账户分布而关闭整个首查目标。

## 证据边界

ZIP `/tmp/AAA-gtron-account-distribution-20261029-01.zip`，13,933,840 B，
SHA256 `803e1ed63c4974e1cfba75d54e121e0dffe05e5502069113718f07352d8705cd`。
本机已核对服务端 seal、ZIP CRC、原 manifest/base metadata root、1521 个
exact pair identities，以及所有独立可复算的布局公式；源键计数/成员关系来自
服务器的认证遍历，没有再扫描一遍全部字典。

[统计及复核结果](evidence/20261029-account-routing-study/verification.json)、
[完整逐源数据](evidence/20261029-account-routing-study/distribution.json)、
[线上 HTTP 原始计时](evidence/20261029-account-routing-study/production-multiheight.json)。
生产 PID32436、启动时间 2026-10-28 14:26:39 UTC、executable 前后相同。
本轮仅推送 feature 分支以传输研究源码，未推 master、部署、重启或删库。
