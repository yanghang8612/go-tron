# DELEGATECALL / CALLCODE 失败回滚修复

## 问题与修正

主网 H58,435,297 的交易 `29f9367a269ac20b6a1aafc47e40b6cec9db014169e26d1729c168376de7c388` 在内部委托调用失败后，由上层捕获错误并以 SUCCESS 结束。规范回执无日志；旧 go-tron 回执的外层结果、返回数据及 Energy 均一致，却保留了四条兑换日志以及池状态变化。此后兑换金额偏离，最终在 H58,435,596 因发送方少 574 sun 而暂停同步。

具体代码缺口在 `TVM.DelegateCall`：它拒绝失败的内部交易，但没有为委托执行建立 StateDB 和日志回滚边界。`DELEGATECALL`、`CALLCODE` 共用此方法。入口 `CALL` 的快照无法纠正这个场景，因为外层最终成功，不会撤销自己的整个执行。

修正为每个委托执行建立快照，并在 REVERT、执行异常或预编译失败时恢复状态和日志。失败委托中已经成功的后代调用，其 storage、余额及日志也随之撤销；父调用此前的状态和日志保留。成功委托保留原行为，失败标记、返回数据及 Energy 处理沿用原路径。

该规则与 [java-tron Program.callToAddress](https://github.com/tronprotocol/java-tron/blob/develop/actuator/src/main/java/org/tron/core/vm/program/Program.java#L947) 一致：CALLCODE/DELEGATECALL 使用独立子 Repository，只在成功时提交。此为既有协议语义修正，不新增 fork gate。

## 验证

新增 `TestCaughtDelegatedFrameRollsBackEffects`，组合 DELEGATECALL/CALLCODE、REVERT/非法指令/能量耗尽/成功，以及开启/关闭 tracer，共 16 个场景。调用树包含：父帧已有写入与日志、委托帧写入与日志、成功后代 CALL 的转账与 storage 写入，然后由父帧捕获委托失败。断言最终状态、余额、日志、返回数据、调用状态和内部交易 rejected 标记。

修复前 12 个失败路径场景均复现残留，四个成功路径场景通过；修复后全部通过。

- `go test ./vm ./vm/tracers ./core/state ./actuator ./core -count=1 -timeout 300s`：前四个包通过，core 整包到达五分钟上限；不能记为整组通过。
- 按当前 Makefile 的 900 秒整包预算重跑 `go test ./core -count=1 -timeout 900s`：通过，302.740 秒。
- `go test -race ./vm -run '^TestCaughtDelegatedFrameRollsBackEffects$' -count=1 -timeout 180s`：通过。
- `make gtron-pure`：通过。此为本地纯 Go 构建，不能替代生产 Linux/Sapling 构建验收。
- `git diff --check`：通过。

## 现有数据的恢复要求

代码修正不会自动撤销已持久化的错误状态。线上恢复必须先验证偏差前状态和回退窗口，再使用修正实现重放，核对 H58,435,297 的零日志及池状态、H58,435,577 的规范到账金额，并跨过 H58,435,596。

已有 `RestartSyncFromHeight` 在变化记录完整且未跨维护周期时支持逆向回退。恢复前应只读验证 H58,435,595 → H58,435,296 的变化记录和逆向 MuHash 是否精确命中保存的旧根；否则不能将该路径称为可用，更不能让命令意外回落到全链 reset+replay。现库应保留可回退副本，不通过手工补 574 sun 掩盖其他状态差异。

本文验证结果不代表生产数据库已经恢复或节点已经继续同步。
