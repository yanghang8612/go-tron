# Owner/行范围索引固定副本证据（2026-10-21）

本目录记录本地候选 `43c3d47bc1229223af677ff4c56462744cf54631` 与上一轮候选 `4d34e751f0d37add20451ca9aae2d68e0c930b6e` 的完整 HTTP 对照。本线程未更新、停止或启动生产服务；不以离线结果替代线上验证。生产于 UTC 07:20:46 收到独立的停止请求并干净退出。准备阶段的身份守卫捕获 PID=0，零 HTTP 样本产生；原失败及依赖启动竞态均保留在 `preparation-recovery/`，随后以服务保持 inactive 的状态继续离线验证。与 SHA 试验跨轮的服务器负载不同，只比较每轮内部相同输入及交错顺序的新旧读取器。

- 输入来自 HEAD 11,818,192、TxNum 601,386,177 的同一固定捕获。两臂使用同一最终物理输入，基线忽略新增的可选 owner 索引。
- canonical 原包、共享 chunk、冷段及 V2 冷路由保持原格式。索引仅构建于 posting 水位 11,817,530 之后的 662 块；构建器只允许写该范围内的派生索引键，逐包严格重建复核，并核对该尾部及相关 chunk 桶的前后摘要。
- 主比较七个高度，每轮 154 个 HTTP 请求、112 个计时样本。暖缓存 n=4，新进程 OS 缓存未控制 n=2，私有 inode 的 OS 冷缓存 n=2。OS 冷仅针对观测读集，mincore <1%，未清空全机或设备缓存。
- 辅助索引体使用 GTHOS002 / BLAKE2b-256；规范包、共识哈希及 MuHash 原算法不变。第一轮 SHA 原型的完整验收单独保存在同级 `20261021-hot-owner-spans-sha-trial`。
- 全量 MuHash 与三段实际历史恢复另行串行执行。strace/20 秒 CPU profile 排除于主计时数据。
- `native/superseded-broad-experiment/` 是停止的过宽构建试验：未做完整 HTTP/MuHash/恢复验收，不能计为成功结果。部分派生副本保留于服务器，未删除生产数据。
- `local-validation/` 保留精确命令、日志及摘要；早期 microbench 另有明确说明，不与最终完整 HTTP 的统计混算。

证据取回后以 `verify.py` 重新检查 ZIP CRC、每条目的 SHA256、冻结 Git blob、逐请求结果及全部中位数、原始 trace、MuHash 和恢复回执。运行位置为此工作树根目录：

```bash
python3 docs/dev/evidence/20261021-hot-owner-spans/verify.py
# 可附加 --archive /绝对路径/服务器原始ZIP
```

最终取回 ZIP 2,694,556 B，322 个清单文件、18,783,808 B 原始内容；SHA256 `453e7b8ebac3f0cc822a8f3a8d04f6121ba07f7a7f63785f57b66535b1538267`。本地完整复核通过，见 `transfer.json` 和 `verified-receipt.json`。`partial-transfer.json` 仅保留早期 HTTP 部分包的来历，最终结论以完整包为准。`blake-owner-profile-lines.txt` 用冻结候选源码渲染，未改动清单中的原始 profile。
