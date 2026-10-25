# auto-columns 本地策略验证

运行 `python3 verify.py`，或用 `--source-root` 指定这一策略的源码目录。
校验器使用 Python 和 `rg`，只读验证 ZIP、37 个成员清单、源码、测试日志、
初轮中断记录及旧实验活进程回执；不运行归档内脚本或连接服务器。
可用 `--binary /绝对路径/gtron` 另外验证本地构建产物。

`local-policy-checks.zip`：393,904 B，SHA256
`b0211b34fe0ab3d39ac7df958a03018318298506c6add0b5fa9faa7b550acebf`。
包含全部 14 个新增／改动的 Go 文件、2,146 个 Go/module 文件指纹、
五项最终本地检查日志与可重放的检查／打包脚本。基线为
`8c55991935924d8d03417ad1f98367868073b663`；检查及构建时改动尚未提交，
因此精确身份由完整源码指纹和归档源码给出，不能只引用旧基线 Git ID。

最终本地检查均通过：

| 检查 | 秒 |
|---|---:|
| 针对性构建、回退、混合合并／恢复、CLI、pipeline 对照 | 22.255 |
| rawdb/state/snapshots/core/cmd-gtron 五包全量 | 582.505 |
| 针对性 race | 35.167 |
| 五包 vet | 4.641 |
| Sapling make gtron | 10.572 |

构建产物 SHA256：`88f16214ab573621cc954cf47a2e65dced2d58c340c35fdb4f7fd6ed7166cef4`。
复用的本地 Sapling 静态库在检查前后 SHA 一致，源码在检查前后未变。
初轮漏通用 footer 入口的源码和日志完整保留；只在精确确认本机检查进程组
归属后中断，再补回归并完整重跑。初轮未完成结果不计作通过。

旧 V5 固定副本实验仍绑定另一套 2,145 文件冻结源码。本次打包时逐文件
确认其源码未变；UTC 20:08:08 回执中同一 PID 391 正常运行，完成
3,650 / 4,609 段。这个回执只证明旧实验的进行状态，**不证明新策略的原生验收**。
本轮没有推送、部署、启停生产服务或改写线上库。新策略的原生、完整副本、
MuHash、恢复、完整 HTTP 与实际写入成本门槛仍需按报告分别完成。

## 原生检查已排队，尚未开始重检查

`native-queue.zip`：281,123 B，SHA256
`be25281a1afe225fb319fb52c43ec37a1ea08863321d17847c2995b61044fe83`。
运行 `python3 verify-native-queue.py` 只读复核这一份冻结队列证据。
其中包括 267,702 B 上传包、全部 14 个改动源码、两套完整指纹清单、
Python 3.6 兼容的安装／检查脚本、逐块和整包 SHA，以及实际启动回执。
上传包 SHA256 为 `9743f128190d97e45c8da5770458167c92aeaf15a8a18e38881dbe8207f5449b`。
源码身份仍为 `4d69f0720b27e7ab5c125b535975d042663694ac`。

原生 lab：`/data/gtron/history-auto-columns-policy-20261026-01`；
单次检查单元：`gtron-auto-columns-native-20261026-01.service`。
它必须等待旧实验 capture、acceptance、publication 三个门槛全部完成，
才复制独立源码／静态库并执行完整包、race、vet、Sapling 和后续诊断二进制构建。
复制时使用独立 inode；脚本会预检源／目标完整指纹并在检查后复核。
没有修改旧实验源码、二进制或旧单元预算。

UTC 20:28:45 观测确认 PID 12225 存活，RSS 10,596 KiB、CPU 0，
phase 为 `wait-frozen-acceptance-and-publication`，检查列表为空且 candidate-source
尚未建立。这说明原生编译／测试仍未开始，**不能计作原生通过**。
已核验新单元为 4 GiB／2 CPU、Nice 15；生产 gtron 的身份和预算在启动前后相同。
这是有界、一次性的实验检查作业，不是定时任务。

同份联合回执中，旧转换为 4,230 / 4,609 段，已处理 `.seg`
43,172,018,441 → 39,076,602,648 B，少 4,095,415,793 B。
完整 MuHash／恢复／HTTP／物理闭环和冷发布门槛仍未完成。

## 原生检查已完成，尚不是完整副本验收

后续旧实验全部验收和计时结束后，队列按计划复制独立源码／静态库并执行
检查。UTC 2026-10-25 22:25:55 原生检查全部完成，单元随后正常退出。
`native-complete.zip` 为 104,596 B，SHA256
`712a640f494b3e4f5ca5f8067533c2e709215898a98612f4d1cb60e705f3ee3e`。
运行 `python3 verify-native-complete.py` 只读复核这份后续完成证据。
先前 waiting 回执仍保留原样，其校验器只报告相应历史时点。

2,146 个源码／模块文件、1,060 个测试文件匹配，mismatches 为空，源码使用
独立 inode。针对性测试、五包全量、race、vet、Sapling 构建及两个测试
二进制构建七项均通过。Go 为 go1.25.5 linux/amd64；身份仍绑定
`4d69f0720b27e7ab5c125b535975d042663694ac` 和完整源码指纹。

三个原生构建物 SHA256：

- `gtron-policy`: `7ffec8473efe83ec5a45295263837201fcfbd78137f83eac3b2b72d0e60fe175`
- `policy-http.test`: `40fd5c92907e60da060119577e26e8eb578932ac695e702cc2a2980a4bac449b`
- `policy-snapshots.test`: `067ca9d546e349f5fa09bdd1b6e9b130074566c33bd18808c1db3f29b2813f99`

这是新策略的原生检查，不是其完整固定副本 MuHash／HTTP／实际新写入成本
验收。旧冻结 V5 实验已量出明显冷发布退化，当前不建议开启这一普通分列
写入路径；需要继续减少重复编码／分配并复测。没有推送、部署或启停线上服务。
