# 10M/15M/20M 查询与首次冷查后续定位

线上源码仍是 `52aea62ef56799b7e6c1365293123dd029286935`，二进制
SHA256 为 `fcb488f6b9fdcbd401855ed6fbf3fac9ab7f0e240ebbedb6d17e212668134e18`。
本轮继续目标是降低首次历史 eth_call 延迟；没有新的 master 发布、部署、服务
启停、缓存清空或数据库重置。本报告的离线修复位于 feature 分支。

## 线上多高度测量

相同合约 `0xa4997124a328a4615eab437cede31da47a31b375`、calldata
`0x9fa6a6e3`，2026-10-28 14:03–14:04 UTC，从 Mac 经 SOCKS5/Nginx
执行完整 HTTP。逐高度连续三次，进程之前已查询过 9M/35M。

| 高度 | 本轮该高度首次 | 第二次 | 第三次 | 返回 uint256 |
| --- | ---: | ---: | ---: | ---: |
| 10M | 23.208767 s | 5.047872 s | 4.116940 s | 213 |
| 15M | 4.406637 s | 4.570841 s | 4.063501 s | 238 |
| 20M | 4.631822 s | 4.801460 s | 4.984145 s | 249 |

九次 HTTP 200、没有 RPC error，各高度三次返回字节一致。这不是独立全量
回放 oracle。各次准备/建立代理到发送前耗时 0.332–1.442 秒；剩余时间仍包含
网络和服务端处理，不能直接称为精确的服务器执行时间。

这些是线上缓存未受控的样本；尤其后测的 15M/20M 不能当作各自首次冷缓存。
**尚未证明任何相关高度都能稳定 1–2 秒。** 也不能以不同路径的 loopback
HTTP 与网关计时计算优化倍数。

原始记录：[multiheight-gateway-results.json](evidence/20261028-query-plan-rollout/multiheight-gateway-results.json)。

## 23 秒查询实际停在哪里

14:10 UTC 的新 11M 请求耗时 23.103946 秒，随后同进程重复为 6.784639 秒，
返回均为 220。六份 goroutine 快照与 30 秒 CPU profile 显示三个阶段：

1. 请求先预取两个账户 envelope；八个 ordered-read worker 在冷 accessor
   的打开、footer/目录 ReadAt 上等待。
2. 之后预取 141 个系统/元数据键，仍在打开晚期未覆盖源，包含 V11 tx 目录读取。
3. 后段进入未索引热尾，堆栈窗口为 `0x2d1fc19..0x2d20b1b`，约 3,843 块，
   在 owner 选择和 materialized history 解码中消耗时间。

这不是 TVM 执行本身占满 23 秒。CPU profile 同时包含后台同步/冷化，总计
158.12 CPU 秒，不能全部归为这一个请求。聚焦 ordered-read 的约 5 CPU 秒中
包含 syscall、V11 tx 目录和块解码；采样不能精确解释每一毫秒的 wall time。

## 精确路由覆盖率与账户缺口对照

统计实际当前 history/accessor pair ID，认证路由 SHA256 或 account lookupRoot，
不使用 route 头部 advertised count 直接推断覆盖率。第二次线上快照：

| 路由 | 当前源总数 | 精确覆盖 | 未覆盖 |
| --- | ---: | ---: | ---: |
| small system | 34,578 | 34,506 | 72 |
| account | 34,578 | 32,958 | 1,620 |
| broad system | 34,578 | 25,536 | 9,042 |
| metadata（229 个对象） | 34,578 | 24,191 | 10,387 |

当时现有 metadata filter 的 count 均为零；不能由此推断未覆盖源也没有元数据。
V11 身份变化只解释部分覆盖失效，不能解释全部缺口。

固定副本为 H45,836,827、Tx4,240,407,293，MuHash root
`4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627`。
它有 32,036 个当前源，account 缺口 31，metadata 全覆盖。只在私有辅助对象
移除 1,490 个 account source proof，保留原账户上界，得到 1,521 缺口；canonical
history、DB 和热尾索引保持相同。交错 gap/original/original/gap，五高度共
40 次完整 HTTP 返回全部一致。

新增 1,490 个未知账户源使第一次请求的 read syscall 增加约 22,164 次，
逻辑读取增加约 118.8 MB。例如 9M 约 26,104 vs 3,940 次 read，10M 约
24,114 vs 1,950 次。这个差异在缓存状态不同的两轮仍重复出现，说明覆盖缺口
确实放大随机读取。wall time 和 physical read_bytes 强烈受 OS 缓存影响，
小样本不能推出固定加速倍数。

## 没有上线的调度试验及其撤回

直接给账户路由公平调度会启用很重的旧构建器：每补至多 64 个新字典，都要
归并整个全局账户表。固定副本原表为 3,399,129,767 B、115,456,774 行；
私有副本实际一次构建 58.528628 秒，输出 3,399,083,207 B，进程
`write_bytes` 为 6,978,293,760 B（包含辅助输出）。它同步持有重工作 gate，
会延后冷发布。
因此公平调度产品改动已撤回，保留测试证据，不把这个独立改动推上线。
后续账户路由需要有界 delta 与独立的 absence 证明，不能在现有全局重建器上
简单提高频率。

## Metadata 修复

已复现：当一个旧 metadata 页内源被替换或原缺口可补建时，旧构建器只写新
证明；Integrate 会退休整个重叠页，其他仍有效的证明随之消失。新的回归断言
在修复前失败。线上 census 不能证明全部 10,387 个缺口均由这个机制产生。

`c63491da` 的修复认证所有相交旧页，只保留当前精确 pair 的证明，连游标之前
的有效行也带入替换页。合并超过 256 行就推迟，不发布缩水页。发布前在既有
进程内 manifest publication 锁下重读并比较受影响的 canonical refs 和 metadata
refs；冲突时重试，保留其他发布者新增的无关页。既有读租约保护旧文件。

本地 snapshots 全包（235.497 秒）、相关 race（67.482 秒）、rawdb/state/core/
gtron vet 均通过。包括替换、跨页、满页/溢出、游标之前的证明、并发发布冲突
与保留无关新页的回归测试。修复未改 canonical 状态、MuHash 算法或持久格式；
本轮没有反复执行全状态 MuHash 或整机恢复扫描。

## 原生补建及完整 HTTP 验收

服务器从不可变源码分别编译 baseline 52aea62e 与 candidate c63491da，使用
Sapling。candidate 原生相关测试通过（14.147 秒）。两份相同私有输入各在
48 个 metadata 页移除一个当前 proof，不改 canonical segment；从相同游标
开始执行 48 轮真实补建并发布，只操作副本。

| 指标 | baseline | candidate |
| --- | ---: | ---: |
| 补建前有效源覆盖 | 31,988 | 31,988 |
| 48 轮后有效源覆盖 | 31,910 | 32,036 |
| 48 轮后未覆盖 | 126 | 0 |
| 有效 metadata 对象 | 547 | 501 |
| build+publish 累计 | 48.458073 s | 55.640666 s |

candidate 每轮精确增加一条有效 proof；baseline 由于退休相交页而产生新的
缺口，随后多轮都在重填这些缺口。两边产出不同，不能用累计耗时比较补建
吞吐倍率。此实验验证的是 coverage 不再倒退，并非重现所有线上缺口。

随后 baseline/candidate/candidate/baseline 交错启动 HTTP 进程，每个进程依次
查询 9M/10M/11M/15M/20M，各两次，共 40 次，返回全部一致。CPU quota 200%、
GOMAXPROCS 2、history/hot cache 各 512 MiB；没有并行运行两套 HTTP host。
这些是 loopback HTTP；OS 文件缓存共享且未受控，没有清理生产缓存。

candidate 两个进程的计时范围：

| 高度 | 各进程中该高度首次 | 紧接重复 |
| --- | ---: | ---: |
| 9M | 0.834–1.655 s | 0.574–0.619 s |
| 10M | 0.539–0.591 s | 0.599–0.635 s |
| 11M | 0.613–0.677 s | 0.534–0.617 s |
| 15M | 0.624–0.651 s | 0.537–0.547 s |
| 20M | 1.474–2.531 s | 1.284–1.338 s |

9M 在 candidate 首次约 3,936 次 read，baseline 5,633 次；10M 为
1,946 vs 3,643–3,644 次。相同高度逻辑读取稳定减少约 12,792,922 B。
这 126 个 metadata 缺口造成约 1,697 次额外 read，有明确的读取放大证据。

但 20M 在 metadata 全覆盖的 candidate 仍读约 640 MB、94,858 次 read，
重复亦约 641 MB、94,839 次 read。这是独立于本次 126 个 metadata 缺口的
剩余大头。首次 20M 的 baseline 13.916 秒伴随约 1.32 GB physical read；
之后 baseline 2.739 秒、candidate 2.531/1.474 秒的缓存条件不同，不能
将 13.916/1.474 宣称为修复加速倍数，也不能据此保证线上冷首查时延。

本地独立复算并认证三份 manifest 中全部 1,549 个 metadata 对象；各自当前
pair 覆盖数与 Go 输出一致，canonical segment refs 完全一致。两组离线实验
合计 80 次 HTTP 返回均复核正确，所有 host 的 HEAD/Tx/root 一致，生产身份
在每一组内未变。线上另一次同版本重启由其他任务发生在较早 live profile
之后；没有把那次前后的在线计时作为同进程受控实验。

证据包 42,113,144 B、1,605 个成员，SHA256
`c57b2b0012dfa5cae5a3f7c77250d19902014f8a93c866a51fcb13003fdbdd98`。
本地已核对字节数、摘要、ZIP CRC、返回值及 pair/proof 覆盖。
小型原始记录与复算结果在
[evidence/20261028-first-cold-followup](evidence/20261028-first-cold-followup/)。

## 后续重点

修复先阻止补建过程中继续丢失证明；它不自动保证现有线上 10,387 个 metadata
缺口立即消失。既有 fixed-copy 的证明可按当前精确 pair 复用来加速补齐，
但此路径尚未实现或发布。账户全局表需要有界 delta，而不是提高全表重写频率。
同时应对 20M 的约 95,000 次读取按键族/读取阶段跟踪，并继续缩短未索引热尾。
这些完成后仍需实际线上首查观测，才能判断 1–2 秒目标。
