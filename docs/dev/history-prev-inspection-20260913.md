# 热历史 Prev 有界检查与编码对比

2026-09-13。用于解释 chaindata 增长的诊断工具；不修改节点写入策略、历史格式或保留范围。
增长基线见 [当日采样](sync-chaindata-growth-20260913.md)。

`gtron db inspect-history-prev` 必须在停止节点、释放 Pebble 目录锁后运行。它以固定 seed 在显式高度范围内
分层选点，只读取现代 `state-changeset-v2` 的 seq=0 物理 pack。每个分层只选一个高度，空值保留、不补选。
这不是包含 repair 的完整有效历史视图，也不是 SST 物理字节或全库业务占比。

默认检查 256 个高度，编码值累计上限 256 MiB、解码累计上限 1 GiB、单包解码上限 128 MiB、200 万行、
60 秒工作时间。时间检查在读取、解码和各行之间协作执行；单次 Get 必须先取得值才能知道编码大小，
所以报告分别计量读取字节与预算接受字节。进程外层超时须同时覆盖数据库 Open/Close。
不完整、损坏、取消或超预算均输出明确状态并返回非零，不能把已处理前缀当成完整样本。

输出包括 FlatDomain/KVDomain 旧值字节与直方图、最大的 20 行、固定容量的大 key 聚合，以及现有块内 CDC
大小/重复 key 门限。大 key 统计只覆盖 Prev 至少 16 KiB 的行；溢出显式报告。CDC 门限通过不能证明运行开关开启，
也不能证明它比 Snappy 额外节省超过生产要求的 12.5%。

```sh
gtron db inspect-history-prev --datadir /path/to/datadir \
  --from-block 30000000 --to-block 30300000 --samples 128 \
  --db.cache 64 --db.handles 128 --max-duration 60s \
  --export-packs /private/diagnostics/new-packs > inspection.json
```

高度和路径是示例，运行时应从当次水位选择。可选导出目录必须是 chaindata 以外的新目录，父目录预先存在。
目录权限 0700、文件 0600，拒绝覆盖；仅完整解码验证通过的 pack 才导出。manifest 包含高度、编码/解码长度和
编码值 SHA256。它不包含 canonical block hash、repair 或 ancient，不能当作备份。

恢复服务后可直接在导出文件上运行：

```sh
gtron db benchmark-history-codecs --export-packs /private/diagnostics/new-packs \
  --max-duration 2m > codecs.json
```

这个命令不打开数据库。它验证 manifest、固定文件名、长度和 SHA256，逐包比较现有表示、生产 Snappy 基线、
忽略前置门限的生产 CDC、独立 zstd frame。每项都逐字节验证恢复结果；zstd 只用于实验，没有接入生产格式。
统计同时包含墙钟与进程 CPU，不能当作线上导入吞吐。汇总仅包含所有候选都完成的样本，保留取消/失败信息。

运维脚本 `scripts/inspect_history_prev_20260913.py` 分 prepare 和 inspect。prepare 从 GitHub 已取回的完整提交
归档到隔离目录，执行原生构建和相关测试。inspect 持有 start.lock，校验原进程、二进制、配置与磁盘保护，
停止原服务后运行 128 个高度的检查，finally 恢复同一服务、同一二进制与参数。脚本不移除真实磁盘保护锁，
也不调用会切回旧版本的部署 rollback。停机恢复结果以运行记录为准。
