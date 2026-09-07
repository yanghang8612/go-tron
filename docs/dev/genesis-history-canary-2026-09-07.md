# 创世同步的历史余额 canary

2026-09-07。本项只新增只读脚本与本地测试，没有启动 gtron。通过已授权的 SOCKS5 代理对 Java 历史块 getter 做了六次小请求；根任务启动新 gtron 后，又授权从本机读取少量 gtron 早期块与固定历史余额。没有调用写 API，也没有大范围扫链。

## 已取得与尚缺的账户证据

Java 地址为 `http://3.12.206.71:6060/jm/wallet/getblockbynum`，本机代理 `socks5h://127.0.0.1:1088`。每请求设置 12–15 秒 timeout、1 MiB response cap：

| 请求 | 本次响应 |
|---|---|
| GET `?num=0` | 返回主网 genesis，blockID 为 `00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc` |
| GET `?num=1`、`100`、`1000`、`10000` | 均为 `{}` |
| POST getter、body `{"num":100}` | 同为 `{}`，不是因为只尝试了不支持的 HTTP 方法 |

Java `GetBlockByNumServlet` 的 GET/POST 都调用相同 `wallet.getBlockByNum`，未找到时返回 `{}`。该入口当前没有提供上述早期块；这不证明整条 Java 数据库的历史保留范围，也不能据此声称所有早期块都不可得。

genesis 中确实有 Zion `4171b0af54e0a1182a5e0947d6a64f3b22740ef318`、99,000,000,000,000,000 SUN 分配，与 [mainnet genesis](../../params/mainnet.go) 相同。但这**只证明创世分配，不证明之后发生两次余额变化**。现有本地 `test/fixtures/mainnet-blocks/smoke` 明确标注 synthetic，不能当成真实主网账户证据。因此未将 Zion 或 synthetic 地址直接设为 canary；实际可验证账户来自稍后新 gtron 的读取，见文末。

## API 的确切语义

- `eth_getBlockByNumber ["0xBLOCK", false]` 返回该高度 canonical block 的 `number`、`hash` 等；未知块为 null。`true` 可读取少量块的 from/to/value 候选。来源：[ethapi.go](../../internal/jsonrpc/ethapi.go:295)、[blockToRPC](../../internal/jsonrpc/api.go:1278)。
- `eth_getBalance ["0x20_BYTE_ADDRESS", "0xBLOCK"]` 返回**块末**余额。JSON-RPC 使用 SUN × `10^12` 的 wei-like 数量，不是直接 SUN；脚本做整数除法且要求整除。不要把它与转账 RPC 的 `value` 或 Java SUN 数未经单位转换直接比较。来源：[GetBalance](../../internal/jsonrpc/ethapi.go:137)、[BalanceAt](../../core/state/history.go:415)。
- 20 B hex 地址会补 TRON `41` 前缀，21 B 地址也接受但必须已有正确前缀；脚本使用 RPC tx 返回的标准 20 B hex。来源：[address.go](../../internal/jsonrpc/address.go:49)。
- 固定数字 block tag 走 archive 路径；未来块会报错，等于 head 可以走 live。脚本要求 `C < head` 且 `C ≤ solidified`，不使用 latest 当历史基线。当前实现不假定支持 EIP-1898 blockHash 对象，采用固定高度的 hash 前后钉住。来源：[requireArchive](../../core/tron_backend.go:3715)。
- 每个历史请求新建受限 MVCC/overlay 视图和 `PersistentHistoryReader`，读取该键在目标块之后的第一条变化 Prev 来重建旧值。来源：[historyReaderAtContext](../../core/tron_backend.go:3558)、[firstStateDomainChangeByKey](../../core/state/history.go:1239)。

例如实际调用形态如下，地址与块号必须来自后面的已验证基线：

```json
{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x<40个十六进制字符>","0x<固定B高度>"]}
```

## 最小脚本与执行时机

[genesis_history_canary.py](../../scripts/dev/genesis_history_canary.py) 只访问本机 8545 JSON-RPC 和 6071 metrics，可通过参数指定实际端口。根任务先确认两个 listener 属于本次 gtron PID，并已启用 metrics。默认不支持远端 URL，也不启动服务或打开数据库。

先在 head/solidified 已超过约 1,000、且第一次热裁剪尚未到来时运行 `discover`。默认抽 2、34、66、…、994 共最多 32 个块；未达到的块不访问。按实际出现重复的 from/to 选最多四个候选，空 input、正 value 仅用于缩小候选，不冒充执行成功证明。

```sh
python3 scripts/dev/genesis_history_canary.py discover \
  --rpc-port 8545 --metrics-port 6071 --start 2 --count 32 --stride 32 \
  > /var/tmp/UNIQUE_GENESIS_RUN/balance-before.json \
  2> /var/tmp/UNIQUE_GENESIS_RUN/balance-before.stderr
```

为防止覆盖已有基线，根任务应使用新文件、noclobber 或 exclusive-create 包装；只有 exit 0 且 `kind=pre_prune_baseline` 的完整 JSON 才有效。不得用 stderr 错误或空文件当基线。程序不自动扩大范围；若本窗口无合适候选，明确报告 examined blocks 和 attempts，由根任务选择下一个最多 32 块的小窗口，例如从 1,026 开始，而不是无界循环扫到找到为止。

候选必须通过以下全部条件才输出基线：

1. 在两个不同块 B、C 发现该地址，且 `1 < B < C < head`、`C ≤ solidified`。
2. 实际查询 B−1/B/C−1/C 的余额，B−1→B 与 C−1→C 均非零变化，B 余额大于零，而且 B 与 C 余额不同。这样可排除“不存在地址一直为零”和“重复 tx 提及但余额未变”。
3. B−1/B/C−1/C 涉及的最多四个不同高度的 canonical hash 在查询前后完全一致；保存全部 hash、RPC hex 数量、精确 SUN 和候选 tx hash。相邻 B/C 合并为三个不同高度。
4. 基线采集前后观察到 `hot_pruned < B−1`；已经冷裁剪的结果不能冒充裁剪前热基线。

达到实际 cold publication/prune 后，用同一只读脚本复验：

```sh
python3 scripts/dev/genesis_history_canary.py verify \
  --rpc-port 8545 --metrics-port 6071 \
  --baseline /var/tmp/UNIQUE_GENESIS_RUN/balance-before.json \
  > /var/tmp/UNIQUE_GENESIS_RUN/balance-after.json \
  2> /var/tmp/UNIQUE_GENESIS_RUN/balance-after.stderr
```

`verify` 先验证基线格式、主网 genesis 与原始两次变化条件，再要求 `C < head` 且 `C ≤ solidified/published/hot_pruned`。未到线则明确 not accepted/not ready，不当成历史错误。随后重新查询上述不同高度的 hash 和余额，逐项与基线相同才输出 `post_prune_balance_canary_pass`；任何 hash 变化或余额不同均拒绝接受，保留原始基线供调查。

程序最多 128 次 HTTP 请求、累计 response 16 MiB、单 response 4 MiB、candidate identities 2,048；四个可用候选以外不再尝试。它在请求之间检查 150 秒预算，每个 socket inactivity timeout 至多 5 秒；这是协作式时间限制，不能保证恶意慢发响应的绝对 wall deadline。根任务可为整条命令另设 180 秒外层 timeout。超时/超限只终止检查，不影响节点。只有三个白名单 RPC：`eth_blockNumber`、`eth_getBlockByNumber`、`eth_getBalance`；GET metrics 同样只读。

## 通过能证明什么

若 C 已经按原生产覆盖门完成热裁剪，且该账户 B 后确实在 C 发生余额变化，则重建 B 的状态需要在已覆盖冷历史中找到不晚于 C 的相关 Prev。反复只查不变账户或不存在地址无法提供这一证据。逻辑热删除已足够验证读取路由，不需要为了这个 canary 强行触发物理 compaction。

这是单账户、标量 balance 的端到端回归样本，不验证全部账户字段、委托大列表、storage/code 或每个历史键，也不是 Java 历史余额的独立 oracle。基线来自本次 gtron 在热记录仍在时的读取；代码已有单元/互操作约束与生产覆盖校验仍需保留。最多四个 hash/余额从多个请求取得，不宣称一次原子全链快照；solidified 条件加前后 hash 校验用于消除这个 canary 的换链歧义。

创世早期成功不能代表 28M–30M 大委托值的查询/压缩性能。它补足 [测量方案](genesis-sync-measurement-plan-2026-09-07.md) 中“首次冷热闭环后的历史读取”一项，后期大值仍须独立验收。

本地五项测试通过：`python3 -m unittest discover -s scripts/dev -p 'genesis_history_canary_test.py'`。测试覆盖真实变化条件、尚未裁剪拒绝通过、hash/余额变化拒绝、伪造已裁剪基线拒绝与 RPC 写方法在发请求前被拒绝；全部使用内存 fake，无 gtron/Java 网络调用。

## 新创世运行的实际裁剪前基线

根任务于 12:53:18 UTC 启动新主网节点后，本子任务仅经 SOCKS5/gateway 读取现有 `/gmr/` 与 `/debug/metrics`。后者支持重复 `prefix` 参数，JSON 取值形态为 `metrics["state/prune/last/domain_change/pruned_through_block"]["value"]`，不需要新增 metrics 公网路由。

第一次只取 2..994 的 32 个稀疏块，无候选；第二次直接查询 2048..129024 的 32 个稀疏高度，共见 10 笔交易，在 2048 找到重复 sender 线索。这不是扫描覆盖这整段链。随后只查询 2044..2051 八个相邻块，第一候选即通过真实余额门。三个小批次共查询 72 个块位置，其中 2048 重复一次，前两次失败记录也保留。

成功基线于 **2026-09-07T12:59:01.668878Z** 保存：[baseline.json](../../build/benchmarks/20260907-genesis-canary/capture-20260907T125844Z/baseline.json)、[完整原始请求/响应](../../build/benchmarks/20260907-genesis-canary/capture-20260907T125844Z/requests.json)。成功批共 22 次 HTTP 请求、52,690 B response；这些数字不包括前两次候选搜索。复用脚本原验证函数的 [本机 curl gateway 适配器](../../build/benchmarks/20260907-genesis-canary/capture_via_gateway.py) 保留了每次原始响应，只调用白名单 getter。

确定账户为 `0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1`（TRON hex：`41b87f2be4dede9fc25387f8df7e0944b5cb7900e1`），**B=2044、C=2045**：

| 块末 | 余额 SUN | 与前块差值 SUN |
|---|---:|---:|
| 2043 | 25,078,836,071,800,791 | — |
| 2044 | 24,804,279,899,563,131 | −274,556,172,237,660 |
| 2045 | 24,525,350,703,672,524 | −278,929,195,890,607 |

SUN 数值大于 JavaScript 精确整数范围，必须用任意精度整数读取；基线还保存 RPC hex 字符串作为无浮点歧义的比较值。不能将 JSON number 经 IEEE-754 Number 转换后再回写原基线。

三块固定 hash 的 `0x` 后分别为：

```text
2043 00000000000007fb1a6d1d3ffb6ede7e2741ff212ea0c7b4026f49c50906f888
2044 00000000000007fc08f3cea6e2d23011661a198611610068141bdcfe7ee07c69
2045 00000000000007fdff83c66e8b1809253408d6916da918e63018b390b09eb60d
```

采集前/后 `published=pruned=0`，head 为 267,005→286,185，solidified 为 265,336→284,346；两次 process start metric 均为 `1788785598065582158`。head 与 solidified 来自不同 HTTP 请求，不能拿二者瞬时差值当节点真实 solidified lag。这里使用它们证明 2045 远早于安全前沿，未据此测同步速率。三个固定 hash 在全部历史余额请求前后均匹配。

这是**已经成功保存的热历史基线**；该次 capture 本身不是 post-prune 通过。后续复验使用同一原始 baseline，没有重跑 discover 覆盖它；结果见下节。

也可继续从本机走现有代理适配器，无须先上传脚本到服务器：

```sh
python3 build/benchmarks/20260907-genesis-canary/capture_via_gateway.py verify \
  build/benchmarks/20260907-genesis-canary/capture-20260907T125844Z/baseline.json
```

此命令只读远端 API，创建新的本地唯一目录并保存 `verification.json` 与全部 `requests.json`，不修改原 baseline。前沿尚未满足时只保存 not-ready 错误；运行前仍由根任务确认时机。

## 实际裁剪后复验：通过

根任务观测到首轮 cold/prune 推进后，本子任务于 **2026-09-07T13:06:43.339956Z** 完成原基线复验，exit 0、`kind=post_prune_balance_canary_pass`。结果：[verification.json](../../build/benchmarks/20260907-genesis-canary/capture-20260907T130633Z/verification.json)、[完整原始请求/响应](../../build/benchmarks/20260907-genesis-canary/capture-20260907T130633Z/requests.json)。共 12 次只读 HTTP 请求、13,859 B response。

复验前取得 `published=3750`、`hot_pruned=3750`，均覆盖 C=2045；solidified=672,177、head=674,438，足以确认三个旧块远早于 head。两者是分开的请求，不作为瞬时 solidified lag 统计。进程 start metric 仍为 `1788785598065582158`，与裁剪前基线相同。

2043、2044、2045 的三个固定 block hash 在余额查询前后匹配，三项 RPC hex 与精确 SUN 余额全部与裁剪前一致。该账户在 2044 与 2045 的两次已证明变化，加上 2045 已在原生产热裁剪前沿以内，完成了本次选定账户的冷热历史余额读取回归。它没有扩大为全部历史字段或后期巨大委托数据的验收。

原 baseline SHA256 在复验后仍为 `b998c2320892c23e2cf6fddb02fcc1e6304ef5c6ad9cdfeac3297211fa260f4f`，未修改原基线。此次只执行只读 RPC/metrics 和本地结果存档，没有启动/停止节点、修改服务器文件或触发额外 compaction。

## 切换吞吐调度并重启后的复验：通过

主执行者将原生 Sapling release 切至 `876480f8`，在 13:34:59 UTC 以 throughput 模式沿现有数据库恢复同步。最初 13:35:27 的尝试因进程启动后 `hot_pruned` gauge 尚为零被明确拒绝为 not ready，未将其计为通过，也没有修改基线。

待实际维护完成后，于 **2026-09-07T13:37:12.743639Z** 再次复验，exit 0；[原始结果](../../build/benchmarks/20260907-genesis-canary/capture-20260907T133703Z/verification.json) 和 [全部请求/响应](../../build/benchmarks/20260907-genesis-canary/capture-20260907T133703Z/requests.json) 均保留。此时 `published=pruned=167500`、solidified=1,875,186、head=1,876,319，新进程标识 `1788788099969088273`。12 次只读请求、13,866 B response，三个旧块哈希及三项精确余额再次与原热基线一致。这证明本样本在本轮重启后的冷历史读取保持一致，不扩大原有单账户验收范围。
