只读高度分布证据，详见 `../../cold-growth-25m-35m-20261024.md`。

- `growth-live-manifest.json.gz`：采样时的原始生产清单，SHA 在 driver 回执中。
- `growth-spans.jsonl.gz`：22,121 个历史段路径的头及首尾块范围；两个 35M 后的缺失路径保留错误，不充作已读。
- `frozen-kv-domains.jsonl.gz`：从先前完整 V7 字段统计无损投影的 19,949 文件域计数，覆盖至 33,248,191；不是跨段唯一键，也不是 V8/V9 压缩页的物理按域分摊。
- `growth-summary.json`、`growth-local-verification.json`：服务器汇总与本地独立重算。
- `archive-verification.json`：完整 ZIP 与原字段统计、原清单的 SHA256。

运行 `python3 verify.py` 会检查目录 SHA 清单，从压缩清单/逐段范围重算三个目标区间的文件长度、真实交易数，并重算全部冻结域计数。它不会重新读取线上 `.seg` 全部值，不代表全历史共识重放或过去各高度的文件系统快照。
