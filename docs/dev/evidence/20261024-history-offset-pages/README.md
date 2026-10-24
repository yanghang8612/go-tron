# V9 共享 offset 页离线证据

`verify.py` 复核 SHA 清单、19,949 个文件的索引计数及净空间、238 个计时/286 个普通 HTTP 检查、资源窗口、56 个 OS 冷驻留证明、MuHash 和实际恢复窗口，并对照本地 1,076 个生产源码文件。

源 cohort 的 SHA 连接与按真实高度重算依赖同仓库相邻 `20261024-history-inline-directory` 和 `20261024-cold-growth` 证据；避免再复制约 23 MB 既有数据。`.gz` 为原始字节的无损压缩。完整 534 成员归档及本地下载身份见 `download-receipt.json` 与报告。

`corpus-status.json` 的初次 allocated 可能仍受写回影响；以 `space-final.json` 和 `end-output-checksums.json.gz` 的 fsync/全量复读后结果为准。原 pprof 配置失败及后续只补诊断的记录均保留；未更换任何计时样本。

测试只覆盖固定 11.8M 副本的一个合约调用及七个高度；未在生产发布、重启或转换。9M V9 冷缓存一例 1.67 秒延迟及其他回退保留，不能据中位数声称已解决尾延迟。
