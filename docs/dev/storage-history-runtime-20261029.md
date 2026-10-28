# 存储历史分页索引的有界后台补建

本轮把前一轮验证过的 `storage-history-routing` 接入实际 Runner 调度。
产品源码为 `69d74ae09d953a40a65b98b0a43eb22d115e7f70`，仅推送 feature
分支。没有 master 发布、生产服务启停、生产缓存清理或生产 canonical 数据重写。
原性能与索引格式依据见
[storage-history-pages-20261028.md](storage-history-pages-20261028.md)。

## 调度与预算

- small、metadata、storage 三个轻量族轮询。旧的约 3.4 GB 全局 account
  路由重建器没有加入公平调度。
- storage 每次至多 256 个源、128 MiB 声明输入和约 2 秒软工作预算。
  预算在完整源之间检查，一个源可以超时；不以生成部分否定证明满足预算。
- 首次认证 broad v2 后，只按目录与完整 ref 身份缓存有界源 ID 和范围，
  跳过已经精确覆盖的重复补建。此缓存只影响调度，不能交给查询读者或用于
  写入否定证明；query 仍认证当前物理字节。
- storage 已读字典或认证 broad，即使没有输出也结束本次重任务机会，不能
  再叠加 broad/account 重建。统计区分 dictionary reads、声明输入、产出页、
  证明行、退避、冲突与 scan cursor。
- 每三次辅助产出、实际扫描或非取消错误后，下一次已获重任务锁的机会直接
  交给一次 canonical leaf 压实，绕过全部辅助族。该保留机会沿用 128 MiB
  输入、512 MiB 逻辑数据、400,000 记录、最多 4 源和单次 merge 的边界；
  没候选也消耗机会。负载/锁拒绝不会消耗保留机会。

这些预算只控制辅助维护/压实，不改变新冷段发布或热库清理前沿的准入截止。
storage 的 30 秒 idle 只表示自身重试时间；全局恢复、负载和锁仍可能延后它。

## 游标、失败与并发

状态归 Runner 的 pass lock 所有，不再增加跨目录全局游标。指纹覆盖旧扫描
边界内的 canonical pair 和 storage 页/broad ref；前部替换或退休会取消退避
并回绕，单纯追加尾部不取消无产出退避。

成功 guarded publication 后才提交发布游标；硬错误、取消与冲突不前进。
完整尝试但不支持的源允许推进纯扫描游标，保持 unknown。否则一个旧格式段
就能永远挡住后面有效数据。无产出按 1 分钟至 30 分钟退避；EOF 若仍有
unsupported/repair 容量洞，保留失败退避，不能当成全覆盖而改成 30 秒 idle。

报告非取消错误时也推进家族轮询并累计维护机会；持续错误不能永久饿死其他
家族或原始压实。构建仍持 history 读租约，发布沿用 manifest 锁下重新比较
受影响的 canonical/storage refs；冲突返回而不发布过时证明。

已有 storage 页的保留边界优先于 broad 调度提示。新页不能因为跳过中间
broad-covered 源，就跨过并退休一个没有复制其有效行的存储页。

## 正确性与验证范围

功能测试覆盖真实 legacy V2 trio 的 unsupported 扫描推进、EOF 洞退避、
真实尾部追加/前部替换、目录隔离、字节相同但 pair 身份变化、并发发布冲突、
取消与物理源缺失时保留游标、锁拒绝后保留 canonical 机会、实际有界 merge
以及 merge 后历史查询值。无产出试验故意隐藏 active account 路由，证明同一
lease 不会继续读取/重建它。

两条回归用旧 `e0c2fe1b` 源码 overlay 作反向验证，分别实际失败于“unchanged
storage proof 2 false”及“错误后家族 cursor/work 为 0/0”；修正版通过。
Astra 只读复核确认这两处修正，不代替测试证据。

小型两条 CDC 的边界快照先改变 immutable trio 路径，使旧 proof 失效，再
通过 runtime 修复。普通和签名 catalog 恢复均检查历史旧值，并重算 MuHash
通过。这是新 kind/runtime 路径的轻量功能回归，没有再次全扫生产状态。

`e0c2fe1b` 的 snapshots 全包为 242.689 秒、相关 race 为 13.337 秒，vet 通过。
后加的两条记录恢复测试为 2.611 秒。最终修正版受影响的相关套件为 108.184 秒、
race 为 21.699 秒，相关包 vet 通过；服务器原生 Sapling focused tests 为
2.949 秒，包括新恢复回归。具体收据保存在本轮证据目录。

## 原生固定副本试验

输入沿用 H45,836,827 / Tx4,240,407,293，MuHash root
`4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627`。
只在独立目录移除 8 个 storage 页的 manifest 引用，再通过实际 runtime
refresh 与逐页 guarded publication 修复。没有批量 Integrate 捷径。
测试逐轮比较 canonical refs、前沿、精确 source ID 覆盖，并记录实际源读取、
broad 认证、I/O 和 build/publish 耗时。覆盖统计耗时与维护耗时分开。

完整 HTTP 使用 control/runtime/runtime/control 四个新进程；各自依次查询
9M、10M、15M、20M，每高度两次，共 32 次，核对 uint256 为 207、213、238、249。
query 路径沿用上一轮索引实现。OS 缓存共享且未受控，不把后面高度称为独立
冷缓存，也不把本轮结果重新包装成一次全冷加速倍率。

最终 `69d74ae0` 副本结果已取回并独立复核：

| 指标 | 结果 |
| --- | ---: |
| 移除的页 / 精确源证明 | 8 / 102 |
| 精确 storage 覆盖 | 6,334 → 6,436 |
| 字典读取 | 102，恰好等于缺失源数 |
| broad 完整认证 | 1 次 |
| 逐页构建、guarded publish 及最后 idle 探测累计 | 21.027094 s |
| 包含各轮独立覆盖统计的试验总耗时 | 26.353973 s |
| 单次完整 refresh（包含发布等开销） | 1.437–3.100 s，idle 0.324 s |
| idle 重试源读取 | 0 |
| canonical refs / 前沿 | 均不变 |

八次成功发布的覆盖单调增加，最后一次无产出。所有 6,436 条过滤器逐源的
count/bitmap 与前一轮完整索引逐字节一致，包括经过此次重建的 102 条；输出
内容寻址路径因此也复用了原来的路径。它们经过真实重建/发布，没有直接把
旧页重新挂回 manifest。独立验证没有重复读取 canonical 全字典。

这是连续获准调用的补建成本，**不包含生产调度的恢复间隔、三族排队、负载
拒绝和 gate 等待**，不能把 102/21 秒当作线上实时追赶速度。声明输入/两秒
预算不包含全部 manifest 验证、指纹和发布耗时，也不等于严格两秒 deadline。

HTTP 32/32 返回一致。每个进程中该高度首次的范围如下：

| 高度 | control（原完整索引） | runtime 修复后 | runtime 紧接重复 |
| --- | ---: | ---: | ---: |
| 9M | 1.071–2.280 s | 0.820–0.845 s | 0.639–0.664 s |
| 10M | 0.673–0.698 s | 0.638–0.667 s | 0.640–0.718 s |
| 15M | 0.694–0.925 s | 0.618–0.639 s | 0.607–0.610 s |
| 20M | 0.836–0.837 s | 0.621–0.637 s | 0.664–0.747 s |

两组各高度的 rchar / read syscall 基本相同：20M 首次均约 92,953,490 B、
2,013 次 read，9M 首次均约 96,590,564 B、3,936 次 read。本轮验证的是运行时
补建保留既有索引收益，没有证据把 wall-time 波动归为另一次读取路径加速。

本地独立程序校验最终 ZIP 的 SHA256/CRC、21 项成员、32 次 HTTP 请求及返回、
HEAD/Tx/root、102 次修复读取和覆盖单调性、父 manifest 身份、canonical 引用
不变，并以两份之前已封存的原始 ZIP 重建 32,036 个当前 pair、认证所有存储
页及其精确 6,436 行，与原索引逐行比较。生产 PID 32436、启动时间
2026-10-28 14:26:39 UTC 和 `auto-52aea62ef567-fcb488f6b9fdcbd4/gtron` 在本轮
前后相同。随后 gateway `eth_blockNumber` 仍成功返回推进中的高度。

证据包 `/tmp/AAA-gtron-storage-maintenance-20261029-02.zip`，43,002 B，
SHA256 `d5b23d9dfacebdfd9fb10775ef6a910fb31e450e1d78bb9f4a525249c0a7c44f`。
原始 ZIP 留在本机 Downloads；小型复核结果位于
[evidence/20261029-storage-history-runtime/verification.json](evidence/20261029-storage-history-runtime/verification.json)。

## 尚未完成的目标

本轮没有上线，不能据副本数据宣称线上首查达标。线上 account/metadata
覆盖缺口、未索引热尾仍是独立读放大来源；账户需要有界增量设计，不能靠更
频繁重写全局表解决。首次冷查整体目标保持开放。
