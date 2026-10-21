# Owner/行范围索引固定副本证据（2026-10-21）

本目录记录本地候选 `4693a15485cbc0db810e331d21aab418baf22e38` 与上一轮候选 `4d34e751f0d37add20451ca9aae2d68e0c930b6e` 的完整 HTTP 对照。生产版本未更新；不以离线结果替代线上验证。

- 输入来自 HEAD 11,818,192、TxNum 601,386,177 的同一固定捕获。两臂使用同一最终物理输入，基线忽略新增的可选 owner 索引。
- canonical 原包、共享 chunk、冷段及 V2 冷路由保持原格式。索引仅构建于 posting 水位 11,817,530 之后的 662 块；构建器只允许写该范围内的派生索引键，逐包严格重建复核，并核对该尾部及相关 chunk 桶的前后摘要。
- 主比较七个高度，每轮 154 个 HTTP 请求、112 个计时样本。暖缓存 n=4，新进程 OS 缓存未控制 n=2，私有 inode 的 OS 冷缓存 n=2。OS 冷仅针对观测读集，mincore <1%，未清空全机或设备缓存。
- 全量 MuHash 与三段实际历史恢复另行串行执行。strace/20 秒 CPU profile 排除于主计时数据。
- `native/superseded-broad-experiment/` 是停止的过宽构建试验：未做完整 HTTP/MuHash/恢复验收，不能计为成功结果。部分派生副本保留于服务器，未删除生产数据。
- `local-validation/` 保留精确命令、日志及摘要；早期 microbench 另有明确说明，不与最终完整 HTTP 的统计混算。

证据取回后以 `verify.py` 重新检查 ZIP CRC、每条目的 SHA256、冻结 Git blob、逐请求结果及全部中位数、原始 trace、MuHash 和恢复回执。运行位置为此工作树根目录：

```bash
python3 docs/dev/evidence/20261021-hot-owner-spans-sha-trial/verify.py
# 可附加 --archive /绝对路径/服务器原始ZIP
```
