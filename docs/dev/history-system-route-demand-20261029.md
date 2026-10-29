# 系统大路由按需读取与轻量覆盖：固定副本 HTTP 实测

已修复 ContractStorage 在检查轻量过滤器之前，先整读 66,948,766 B broad route 的路径。固定副本补齐 storage 轻量路由后，四高度完整 HTTP trace 中，受控基线共读 broad route **267,795,064 B（4 次整读）**，候选为 **0 B**。性能有改善，但本轮冷查仍为约 3–4 秒，不能宣称达到 1–2 秒，也不能保证任意查询不会出现几十秒。

本任务没有执行生产 gtron 启停、部署、删库、格式回退或清除共享文件页。master 最后核对仍是 `52aea62ef56799b7e6c1365293123dd029286935`。修改和证据仅推到实验分支 `codex/routing-bootstrap-20261028`。

根因有两部分。首先，`firstStateDomainChangesContext` 对 ContractStorage 提前调用 `loadBroad`，轻量 storage filter 在它后面才执行；已有覆盖也不能消除大对象读取。其次，旧 storage 调度器把 broad 覆盖作为“不必补建”的提示，旧 25,600 个源长期停留在 broad，新 tail 的 6,436 个源才有小页。这与 small/metadata 未完成覆盖是不同的问题。

精确按 history/accessor pairID 统计，固定副本共有 32,036 个源；small 和 metadata 都已经是 32,036 / 32,036，storage 从 **6,436 / 32,036 补齐为 32,036 / 32,036**。本轮没有把路由文件的行数简单当作有效覆盖。补建新增 100 个 256-source 页，总共 101 个调度 pass（包括结束检查），耗时 **150.429794173 秒**，原始 canonical dictionary 读取 **0 次、0 B**。storage route 总大小从 **10,119,232 B → 51,420,224 B**，净增加 **41,300,992 B**；旧 broad 仍保留作缺口回退。canonical refs、其余辅助 refs 和冷化进度未变。

投影对经过认证、完整结构验证的 V2 broad 做两次有界遍历：先数每个 exact source 的 storage prefix，再按与正常 builder 相同的 hash/Bloom 大小和探针生成过滤器。空源可以判负；V1、超限、未知或替换 source 都不能继承负证明。后台优先追未覆盖 tail，追平后才转换旧 broad；投影工作集限制 64 MiB。后续每次发布前再检查 broad 当前物理身份，并用既有 guarded publication 防止父 catalog 改变后发布。

查询侧先用精确 pair 的轻量页；阴性跳过该源，阳性仍读取 canonical accessor；未覆盖才加载 broad。页保留在 demand 顺序，先命中时不会预读后面的页。中间版本曾把 storage 加入逐键全目录规划，引入重复读页，并被既有早命中回归抓住，已撤销；该版本性能原始数据保存在 intermediate 中，不冒充最终结果。

缺口回退增加 128 MiB / 8 entry 的认证字节镜像缓存。新请求仍打开 exact ref 并比较 inode、size、mode、mtime、OS ctime；相同大小、恢复 mtime 的原地改写会失效。无 ctime 的平台不跨请求复用。缓存保存拥有所有权的 SHA256 认证字节，不能描述成“每次请求重新哈希磁盘”。首次完整结构验证后建立有界 uint32 行目录，后续只二分和复制请求键的 membership；未选键保持 unknown。失败、取消、目录超限不发布半成品。128 MiB 是缓存持有预算，不是并发冷 miss 的进程峰值限制。

原生 HTTP 候选源码为 **`1d93528771f35d7e564c11372a32079a6f71e442`**，HTTP test binary SHA256 为 `5750c13701c7c24316b2efc55499f1df06642a8419666e464755dc730e3ffc9a`。受控基线为 `a34afcd1cfb4ca8b4ede4b68cdce6e568c6e387d`，binary SHA256 `58644351e4d8344b3efed32b3d09f7b33f508e2e9819daed2bba106d2236ae04`。两者使用相同诊断开关禁用此前没有通过性能门槛的 key-filter 实验，保留相同 owner-plan 路径；这是隔离 snapshots 路由改动的 A/B，不是生产二进制的吞吐承诺。后续只补了 bootstrap 的 A→损坏 B→恢复 A 重试状态修复，其 HTTP 查询路径未变，已单独测试，没有重复正常查询矩阵。

两版在**相同、补建完成的 catalog** 和相同 canonical 固定切面上交错两轮，每轮四高度各一次冷首查、一次同进程重复。固定 H=45,836,827、Tx=4,240,407,293，posting 未索引热尾保留 1,418 块。MuHash HEAD root 每次 ready receipt 均为 `4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627`。

冷查覆盖 chaindata/history/ancient/state-snapshots 的完整实际 HTTP 文件读取集合：1,664 个文件、26,416,447,146 B。确认无数据库文件 mmap；全部是独立私有 inode。新进程启动前和 HTTP dispatch 时逐文件 fsync/fadvise/mincore 为零；发现未纳入文件则拒收并扩展。保留正常启动必需的应用元数据缓存，排除进程启动和公网代理时间；没有设备缓存全冷的声明。

请求合约为 `0xa4997124a328a4615eab437cede31da47a31b375`，calldata `0x9fa6a6e3`，完整 HTTP `eth_call`，返回分别为 uint256 **207 / 213 / 238 / 249**。40 个 HTTP 包括 8 个 trace discovery（不计时）和 32 个计时请求；完整 32 字节 result、RPC id、版本、ready HEAD/Tx/root 和冷页证据全部已下载独立复核。

以下为两次实测范围，单位秒，不计算 p95；15M 基线第二轮 9.907 秒是保留的离群样本，不用它宣传优化倍数。

| 高度 | 基线冷查 | 候选冷查 | 基线热查 | 候选热查 |
|---|---:|---:|---:|---:|
| 9M | 3.76749–3.84676 | 3.31467–3.43680 | 0.88047–0.92422 | 0.67899–0.68901 |
| 10M | 3.67928–3.76519 | 3.29913–3.48009 | 0.90767–1.00024 | 0.64610–0.67779 |
| 15M | 3.63411–9.90715 | 3.07238–3.59131 | 0.94598–0.99810 | 0.59408–0.60350 |
| 20M | 4.14007–4.30243 | 3.91909–4.05802 | 0.94926–0.95734 | 0.81434–0.81500 |

9M 冷查仍存在大量实际读取：候选两轮物理读取 **146,313,216 / 146,329,600 B**，读取 syscall 各 **6,942** 次；基线为 219,926,528 / 216,403,968 B、各 7,040 次。候选重复查询逻辑读取从 89,374,566 B 降到 **19,449,742 B**，但还有 2,017 次读取 syscall，分配约 219.5–220.1 MB。大路由已经不是剩余延迟来源；下一步应对这部分热 history 尾部、canonical 点读和分配做按阶段分析，不能继续把 3–4 秒归因于 broad 整读。全进程 trace 包含大量 ancient 启动读取，不能把其总字节误算为 HTTP 读取量。

验证采用定向路由、early-hit、同 mtime 损坏、替换 pair、投影与 canonical filter 对照、签名快照恢复和小型 MuHash fixture。本机最终定向测试 20.683 秒、race 23.093 秒、vet 通过；服务器 Sapling 原生定向测试 6.944 秒、完整 HTTP 编译及矩阵通过。另补恢复/小型 MuHash/verified catalog 回归 2.420 秒。中间版本全包测试抓到 early-hit 失败，修复后复测相关路径，未重复整个耗时包或全库 MuHash 扫描。

Astra 只读复核提出的重复预读、投影发布前物理身份检查、broad 移除清空投影及恢复旧 ref 重试边界均已处理，并由本任务复核。缓存未触及 MuHash 编码或 canonical 数据布局。

两次 ZIP 已下载并与服务器回执逐字节长度、SHA256 一致，独立复核在 `evidence/20261029-system-route-demand/verification.json`；大 trace 位于本工作树 `build/benchmarks/20261029-system-route-demand/final/`。最终 ZIP 为 4,975,677 B，SHA256 `45389b15cfb1ff1d38d54cb4805705e048d9be229c7f9bfc6d4cb555cfcdda13`。两个离线 unit 均 inactive、MainPID=0、exit=0。

生产在矩阵前后均为 PID 17471，原 52aea62e binary，测量窗口身份稳定；收尾时另一个启停动作使其变为 PID 30127（UTC 06:10:30 启动），SHA256 仍为 `fcb488f6b9fdcbd401855ed6fbf3fac9ab7f0e240ebbedb6d17e212668134e18`，active。不是本任务发出的命令。本实验分支还包含此前未通过性能门槛的热尾实现，**不能直接整分支合入发布**；后续如授权上线，应只移植本轮 snapshots 优化并保持已验证的 owner-plan 路径。
