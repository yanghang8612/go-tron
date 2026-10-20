# 固定主网副本冷查询实验原始证据

对应报告：[cold-query-routing-20261021.md](../../cold-query-routing-20261021.md)。本目录保存可复算的原始请求、handler 计数、inode/驻留证明、构建日志、严格验证、MuHash、恢复与回收回执。生产没有重启或发布。

| 目录 | 冻结读取代码 | 用途 |
|---|---|---|
| prefix | 同一 b6db66fc 测试二进制、两种布局 | 单独测量小头部分帧；另含 e480 原生 writer 交叉验证及停止合并实验的成本 |
| routing | 0b57081f | b6 原始布局 vs prefix+系统路由 |
| account2 | 2e403c3d | b6 原始布局 vs prefix+系统路由+账户上界路由 |
| account-gc | c7ef42cf | 独占旧辅助文件的回收、清理后全量 MuHash 与真实冷历史恢复 |
| lazy-final | 0b370448 | 同一清理后固定副本：c7ef42cf vs 默认空 caller 按需读取 |
| lazy-nine | 同一 0b370448 二进制 | 单次 OS 冷 9M 文件级 strace，排除性能表 |
| local | 对应文件名 | 本地测试、race、vet 和构建日志 |

每个完整 HTTP 比较 154 次，112 次计时。表中基线均是离线版本，不能把相邻轮次的倍数相乘，或称为生产 A/B。诊断 overlay/profile、预飞检查和正确性 HTTP 不进入主要性能表。中间失败/主动停止的等待任务和试验日志保留，最终状态和用途由报告说明，不能把等待器中止等同于 MuHash 失败。

`transfers.json` 记录每份原始 ZIP 的服务器位置、本地保留位置、字节数与 SHA256。所有 ZIP 已验证 CRC 和完整逐文件 inventory；大体积 read.trace 与源码传输压缩包仅留在原 ZIP，不进入 Git。本目录较大的 JSON/JSONL 使用 gzip（mtime=0）存储，解压后的内容必须和原 inventory 相同。原始全量追踪可从列出的保留 ZIP 取得；本目录没有数据库或私钥。

本机可复算：

```sh
python3 docs/dev/evidence/20261021-cold-query-layout/verify.py
python3 docs/dev/evidence/20261021-cold-query-layout/verify.py --source --archives
```

第二条额外要求本地保留的 ZIP 尚在，且相关 Git 提交可读。脚本校验 inventory、所有 HTTP 返回值、42 组中位数与 min/max、28 份冷缓存驻留证明、固定 head/root、MuHash、22,496 行冷历史恢复和 144 文件回收；`--source` 逐文件验证服务器冻结生产源码和指定 Git 提交。它只读证据与 Git，不接触数据库。

新进程组没有控制 OS 缓存；私有 OS 冷缓存组只针对该轮 strace 发现的读集做独立 inode 的 fadvise/mincore，不是全设备冷启动。请求数量较少、单一计数器合约，适合定位结构性瓶颈，不构成并发吞吐、p95 或所有合约的性能保证。

最后的 [可视验收/线上身份回执](final-acceptance-and-production.png) 保存了完成后的实际终端状态：2026-10-20T19:10:02Z、PID 28737、高度 15,388,544、原二进制 SHA256 和旧库存在。此前结构化生产核验仍保存在 lazy-final/lazy-final-production-proof.json，不覆盖旧回执。

证据中的 Go 源码以 `.go.txt` 原样保存，避免被 `go test ./...` 当作项目包编译；读取器去掉 `.txt` 映射后核对原 inventory，原 ZIP 中的文件名与字节均保持不变。
