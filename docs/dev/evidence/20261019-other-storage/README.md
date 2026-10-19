# 2026-10-19 其他存储压缩分析证据

对应 [分析报告](../../other-storage-compression-20261019.md)。本目录保存服务器输出的原始 JSON；`analysis.json` 是本地从原始证据独立计算的结果。

| 文件 | 内容/边界 |
|---|---|
| `distribution.json` | 5,715,086 高度全部三个 latest commitment 源族、全域逻辑字节、资产连接、native/账户/标量 SST 对照；匹配此前认证源 SHA |
| `code-distribution.json` | 同一 5.715M 离线库的 1,809 份内容寻址字节码；逐份 Keccak、Zstd round trip 与 SST 校验，未评估可达性/删除 |
| `consistency.json` | 599 个源文件前后检查、热扫描期间服务/二进制身份、工具二进制 SHA |
| `inventory.json` | 02:45 固定冷 manifest 的 active 文件大小/版本；同期目录遍历不是原子快照，outside-manifest 不表示安全垃圾 |
| `cold-selection.json` | 从 4,053 段确定性选择 16 段；48 个 trio 文件的大小、inode、manifest checksum |
| `cold-distribution.json` | 16 个完整历史段的所有逻辑变更/旧值，未将投票名单拆开 |
| `cold-distribution-votes.json` | 相同 16 段，额外区分 VotesIndex/普通 voter record |
| `headers.json` | 同冷 manifest 的 event/accessor 分区；稍晚的 ancient/header 清单。不是 bodies/receipts payload 全校验 |
| `cdc-comparison.json` | 16 个私有 trio 的实际 CDC 转换和 before/after；不能代表全体段收益 |
| `sidecar-comparison.json` | 16 对完整 sidecar 的独立页 Zstd 模型，计入外壳/目录、CRC、raw fallback，并逐页 round trip；没有新生产 reader/延迟测试 |
| `txindex-width.json` | 更晚固定 1,307 个 run 的完整 CRC/SHA/逐行定位重建；新定位宽度的布局模型，尚未写生产新格式 |
| `final-proof.json` | 03:21 服务仍 active，版本已变为 eaae18c8bbf2；`production_modified=false` 是采集动作范围注记，不代表全局没有其他部署 |
| `nodeinfo-delivery.json` | 报告交付前 HTTP 状态，仅作服务活性观测，不是统计数据切面 |
| `final-race.log` / `final-vet.log` | Go 1.25.5 最终三个新增命令的本地验证；vet 成功时无输出 |
| `collectors/` | 本次使用的采集脚本原文；硬编码本次隔离目录，非通用运维/回收命令；不要盲目重复运行 |
| `sha256.json` | 本目录原始证据、脚本、诊断源码和验证日志的校验和；`analysis.json` 单独由 verify.py 生成 |

完整服务器样本、原始 manifest、SST、代码归档及运行日志保留在 `/data/gtron/other-storage-distribution-20261019-0245`。热库来源 `/data/gtron/generation-census-20261018-1259/chaindata`，没有新增停机采集。源码基于匹配旧库的 `/data/gtron/generation-census-tool-20261018-1259/source` 构建；这与当前分支新格式 HEAD 不同。不能将新格式 reader 用于旧热库。

原始 JSON 经现有 JumpServer 终端可见输出取回。传输包为“文件名到原文的 JSON 字典”，用 zlib+base64 承载并在两端比较**解压后字典字节**的 SHA：

- 第一包 147,482 B，`2791555843bb9392949d825781a9110df187c64c6e7413913e3902efc1e7482c`，包含 distribution、inventory、headers、sample selection/distribution、consistency、CDC、sidecar。
- 第二包 687,724 B，`d2d64ac5890dd7cce0cc9b0523ea82268f56ce525dc215bfe0c355577f6cf9f0`，包含完整 txindex、votes 子分类、final-proof。
- 字节码补查原文 1,644 B，`588263af5b02868305fad9720601d42631cc53dce202f255e512cb1821673be7`；03:35:48–03:35:49 独立补查，报告直接压缩传输，非文件名字典包装。

分帧源 SHA 与此前已完整验证 MuHash 的 [census.json](../20261018-generation-census/census.json) 一致。immutable SST 前后检查的是设备/inode/大小；非 SST 独立复制文件检查 SHA。不要把 `source_files_verified_*` 理解成所有原始 SST 文件都重新做了全文件 SHA。

无需联网复核：

```sh
python3 docs/dev/evidence/20261019-other-storage/verify.py
```

工具使用示例（仅隔离、不与运行中 Pebble 争锁）：

```sh
# 旧库必须在匹配旧格式的 source 中编译这些诊断命令。
go run ./cmd/other-storage-distribution --db-dir /offline/chaindata \
  --baseline /offline/census.json --out-dir /new/exclusive-result
go run ./cmd/other-storage-distribution --code-only --db-dir /offline/chaindata \
  --baseline /offline/census.json --out-dir /new/exclusive-code-result
go run ./cmd/other-cold-distribution --dir /pinned/history --out /new/report.json
go run ./cmd/other-cold-distribution --dir /pinned/history --compare-accessors --out /new/pages.json
go run ./cmd/other-cold-distribution --dir /pinned/history --compare-cdc --out /new/cdc.json
go run ./cmd/tx-index-width-census --dir /pinned/tx-index --out /new/txindex.json
```

CDC 模式在输出旁新建独占 `.work` 目录，只有其中的私有 manifest/链接可以被替换；不向 migrator 传源目录。hardlink 固定文件会延长原 inode 的保留时间，需考虑后续 GC 的释放，不等于额外复制或可以随意删除线上段。

诊断源码的历史 SHA 固定于提交 `07265b6e`；验证器通过 `git show` 读取该提交，以免后续格式改造使历史证据校验依赖可变工作树。原始采样/统计内容不变。
