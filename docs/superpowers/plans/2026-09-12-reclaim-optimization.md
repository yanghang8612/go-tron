# 热历史删除与维护准入优化实施计划

设计依据：[2026-09-12-reclaim-optimization.md](../specs/2026-09-12-reclaim-optimization.md)。
本轮不升级 Pebble/数据库格式，不执行线上强制压实；源码与 ops 分别提交 master。
当前源码已提交 `4940853af2424d04b86236e4ec682dcfc32e75e2`，最终本地测试已过；
source/29 个非文档文件 SHA 和 31 项新指标合同已冻结，现场身份仍待解锁后核验。

1. **确认回收链路与测量边界。** 核对现有自动压实、point/range tombstone、
   读者引用和后台文件删除路径；保留现有 L0/debt/并发/压缩参数。
   固定当前公平锁版本和 13:47–13:57 UTC 的 21 点前测：session 21.12 块/秒、
   state 头距 329,189→327,850、body 340,880→353,546、交易索引冷覆盖 381,840→361,738，
   debt 62.05–69.29 GiB、压实输入/输出
   53.03/44.88 GiB、posting 压力退让 +600 且 chunk 无增量。物理 allocated 另测。
2. **实现可关闭的 history range 候选。** 在原 cold-coverage 和逐块 prune 决策之后，
   仅合并连续、真实存在、已索引、合法 seq=0 的选中 changeset 块。
   采用至少 64、最多 1,024 块和 64 MiB 合作扫描预算；缺口、非选、seq>0、
   异常键或不足门槛走原 point/error 语义，indexed value 仍按旧快路径不解码。
   线上每最多 256 selected blocks 尝试 index/chain guard，验证 canonical/Finish/index，
   补 pending-prefix 与 async error fence；同 guard 内 scan+flush。仅锁忙保留原 point
   并计 fallback_blocks；proof 异常终止。64 MiB 是每 run 预算，不是 guard 总量上限。
   9 项 history 指标中 work 仅计 callback，成功工作量整 pass 成功后发布。
3. **建立真实 Pebble A/B。** 同输入、同数据保留与查询结果，比较扫描、写批、
   tombstone 工作量和自然压实观察。补批失败、取消、恢复及所有范围边界测试。
   对受控显式 Compact 测试单独标注；A/B 无明确收益或安全条件不满足则不启用候选。
4. **缩短 posting 的 gate 占用。** 加入非绑定预检；索引锁后公平等待链锁，
   立即检查取消，再进行 fresh engine/cached device 压力复核和真实非阻塞准入。
   原许可和 canonical 证明保持；分开记录等待、lease、证明和扫描/提交阶段。
   冻结 7 阶段×3 gauges 以及 callbacks；测试竞争、压力变化、取消/Stop、重组、
   cursor 不推进和所有资源释放分支。阶段重叠不相加。
5. **冻结本地验证。** 运行 rawdb、pebbledb、snapshots、pruning、maintenance、
   core/cmd 相关测试及必要 race、增量 lint；独立审查后冻结源码 SHA。
   不改变未涉及的行为，也不将性能 A/B 当作共识正确性证明。当前 A/B 已有独立结果，
   pending-prefix/guard 定向与 race、最终 `core/... + cmd/gtron` 已过，增量 lint 为 0 issues。
   保留第一轮沙箱 bind 与既有 async 测试偶发失败记录，不把后续定向通过冒充第一次全过。
6. **冻结部署 helper。** 复核 live old exe/SHA/PID/start ticks/argv、完整 unit 和
   三个旧环境开关。固定 `d3793a24..新 source` 全部非 docs 文件 SHA，
   新 helper 自身置于独立 ops commit。若 range 获准，新增第四开关为 1；
   原生构建测试分别覆盖旧模式和候选启用模式，新增 metric contract 只约束候选健康。
   用离线 mock 审计配置变更、进程身份、源码 pin 与旧版完整回滚。
   Mac 锁屏时 OLD_PID/start ticks 保持 0、prepare 拒绝；可先提交/推送通过本地验证的
   代码、文档及现场身份尚未冻结的 helper。A/B 上线决定与源码清单已冻结，
   仍需验证实际旧服务身份；ops SHA 由 `--script-revision` 精确参数传入。
7. **经 GitHub 构建部署。** 在前测完成后 prepare，使用 Go 1.25.5 Linux amd64、
   CGO/Sapling，核对 archive/source/binary/static library SHA 与 build settings；
   prepare 不停止服务。激活只变更批准的 exe/第四开关，保持 40 GiB、guard/holds 和其他服务。
8. **线上验收并报告。** 固定新进程完成后测 21 点、全轮 keyspace 观察、
   canonical canary、配置/端口/日志复核和实际 `du`。同时比较清理进展、同步负载、
   三类积压和物理增长，明确未完成整轮、重启预热、异步估算及自然压实延迟。
   回归则恢复公平锁旧二进制与原 unit；旧健康检查不得依赖新指标。
