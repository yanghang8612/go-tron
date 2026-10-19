# compact-v3 实现验证证据

对应 `docs/dev/compact-state-v3-20261019.md`。服务器隔离根目录为 `/data/gtron/compact-v3-validation-20261019-01`；本任务只创建验证副本和输出，没有部署、停机或修改生产数据。

`server/` 保存服务器原始输出，路径与隔离根目录内一致：

- `corpus/export.json`：在匹配旧格式的源码下全量扫描 5,715,086 高度固定热库；三个源族的行数/分帧 SHA 必须等于之前已验证 MuHash 的 census。导出文件本身有 SHA、大小和行数。新旧编码器对同一导出做逐行逻辑 round trip。
- `hot-sst/results.json`：1,349,700 个账户和 12,478,784 个精确 8 字节值。使用真实新编码器，Snappy/无压缩、4 KiB block、restart 16、Bloom 10、Pebble v4、500,000 行/文件；核验所有 SST 校验和。包含失效 generation，不能直接当作当前可达热库节省量。
- `tx-index/results.json`：完整 1,307 个固定 V1 run，491,120,809 行；每个原文件全 SHA 与此前 census 对照。重写真实 V2 后逐 bucket 比较全部 fingerprint/location，点查保留碰撞候选并比较原定位。
- `history-index/results.json`：同一 16 份 `.idx` 的 Zstd 4/16 KiB 分页对照；`history-index-snappy/results.json` 为最终 Snappy 对照。源码先校验每份 manifest SHA，重写后比较全部条目。
- `votes-final/results.json`：2,048 个随机样式地址、每块追加一个并真实提交/构建 latest 与历史 trio；旧基线逐次读/解码整表、线性去重、整值写回，与前身算法一致。两端完整 MuHash 重建通过。较早的 `votes/`、`votes-exact/` 保留过程证据，最终结论使用 `votes-final/`。
- `uint64-before.log` / `uint64-after.log`：同一 portable benchmark 分别在匹配前身源码与后继源码编译，3 次测量；MemoryDB 读取不是 Pebble 磁盘 IO。
- `account-read.log`：同一后继 decoder 读取全地址 core1 与 owner 地址 core2，3 次测量。
- `*-patch.json` 与 `run-*.py`：离线补丁前后 SHA 和实际命令。页缓冲复用、完整旧 Votes 算法、历史 Witness API、Snappy 对照均明确留痕；这些脚本硬编码隔离路径，不是部署脚本。
- `final-status.json`：合并线上 eaae18c8 后的基础源码归档 SHA；后续分页及 Witness 修正见补丁证据和 `snappy-status.json`。`delivery-proof.json` 仅为交付时服务活性/二进制身份，不代表排除了其他 agent 的独立变更。

服务器比较限制为一个 CPU、2 GiB cgroup、`GOMAXPROCS=1`、低 IO/CPU 优先级。索引查询使用已验证并预热的操作系统缓存，4 KiB 和 16 KiB reader 都使用 64 KiB 解压页缓存。查询结果为每文件均值及这些均值的中位数，不是单次 API 查询的 p50/p95，也不是冷设备 IO 测量。

`local-*` 保留 Go 1.25.5 的本机完整/定向/race/vet/Sapling 构建与测试日志。首次 300s 全仓测试的时限短于 Makefile 的 900s；后一次全仓测试发现独立历史 Witness API 仍解整值名单，已修复并补回归。最终验收以 `local-full-test-release.log` 等 release 日志为准。

无需联网复核：

```sh
python3 docs/dev/evidence/20261019-compact-v3/verify.py
```

`verify.py` 先校验本目录 `sha256.json`，再连接前两次固定 corpus 的证据，独立计算 `analysis.json`。`transfer*.json` 保存经过可见终端取回的文件名字典原文 SHA 和长度，服务器端、本机端均比较解压后的字典字节。
