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

服务器使用修复提交 `24344b918164380b67eed2f4088c92d4ae6352ac` 的独立源码副本、Go 1.25.5 linux/amd64 和已有 Sapling 静态库完成原生验证：

- `CGO_ENABLED=1 go test -tags=sapling ./vm ./actuator ./core/state -count=1 -timeout 300s`：全部通过（2.953 / 0.164 / 4.360 秒）。
- `CGO_ENABLED=1 go build -tags=sapling -o ../gtron ./cmd/gtron`：通过。
- 原生程序保存在服务器 `/data/gtron/repair-delegatecall-20261105-01a1a984/gtron`，没有切换生产服务的二进制。

## 现有数据的恢复要求

代码修正不会自动撤销已持久化的错误状态。线上恢复必须先验证偏差前状态和回退窗口，再使用修正实现重放，核对 H58,435,297 的零日志及池状态、H58,435,577 的规范到账金额，并跨过 H58,435,596。

已有 `RestartSyncFromHeight` 在变化记录完整且未跨维护周期时支持逆向回退。恢复前应只读验证 H58,435,595 → H58,435,296 的变化记录和逆向 MuHash 是否精确命中保存的旧根；否则不能将该路径称为可用，更不能让命令意外回落到全链 reset+replay。现库应保留可回退副本，不通过手工补 574 sun 掩盖其他状态差异。

## 首轮恢复预检：未通过

对原库进行了离线、只读检查。检查程序只以 read-only 打开两个 Pebble 引擎，所有逆向写入均留在一次性 `blockbuffer` 中，不 flush。停机前获取部署及启动互锁，结束后恢复原有服务和二进制；未执行 `RestartSyncFromHeight`，未修改数据库内容。

- H58,435,595 的 HEAD 和 commitment identity 与预期区块匹配；目标 H58,435,296 的区块、状态根及整个窗口的 tx-range 均存在。
- 窗口共有 268,489 条热状态变化，299 个区块均有记录；冷历史在该 tx-range 内未返回额外变化。
- 整段逆向 MuHash 与保存的目标根不一致。加入冷历史读取后结果相同，不能归因为单纯缺少已迁出的冷变化。
- 逐块逆向检查中，595 → 594、594 → 593 通过；首个失败边界是 **58,435,593 → 58,435,592**。这定位了恢复路径的不一致，尚未确定其具体原因，不能据此直接断言磁盘损坏。

因此首轮恢复结论为 **No-Go**：不能直接回退这 299 块，也不能绕过状态根检查或补余额。后续需在副本定位该边界的不一致，或验证一个偏差前的完整恢复点，再用修复程序重放并检查规范回执。

服务器已保留停机一致的热库/历史库诊断副本：`/data/gtron/repair-delegatecall-20261105-01a1a984/checkpoint/datadir/gtron`。9,015 个不可变 SST 使用硬链接，其他 12 个文件独立复制，共 2,199,451 字节；目录和可变元数据独立。该副本包含已经偏离的 H58,435,595 状态，**不是修复后的数据，也不是包含 cold/ancient 文件的完整恢复备份**。预检源码及日志保存在同一操作目录，后续可在副本继续检查，避免反复停原服务。

最终 HTTP 检查仍为 H58,435,595，区块 ID `00000000037ba80b156e9a956f9e8e2391e356981b8056a1d97bb9997573ec40`；原 `gtron.service` 已恢复 active。修复见[草稿 PR #4](https://github.com/yanghang8612/go-tron/pull/4)，尚未部署，节点尚未恢复同步。

## 回退不一致的根因与修复

首个失败边界来自 H58,435,593 的临时合约 `41eb22fffb736e79665f8ce21e8fc4ec4f5886475c`：同笔交易内创建并销毁，账户及普通 KV 的净变化为空，但删除提交仍会持久化一个原本不存在的 generation=0 物理计数器。旧 journal 仅为 generation reset 生成历史，遗漏了删除时的这个计数器 before-image。逆向执行因此遗留 tombstone，MuHash 与保存的前块根不一致。

在只读副本的内存覆盖层中，仅移除该计数器，593 → 592 即精确恢复保存的根 `8f8759fb4709a28b2276603b142acd5c9b360cc6128aebb791d5605fb9dbf307`。完整窗口发现 57 个区块缺少 59 条同类记录；每次修正都必须命中该块保存的前块根，299 个逆向边界全部通过。公开规范回执的 create/suicide 内部调用只用于缩小候选地址，状态根匹配才是修复记录的接受依据。

新增独立 generation 删除 journal entry，保存物理行的存在性和值；正常 journal 撤销会丢弃这个历史意图，既有账户 journal 负责恢复删除本身。它不改变账户删除结果、状态编码或共识语义。`SelfDestruct` 和 `DeleteAccount` 均使用该路径，读取失败向 StateDB 错误传播。

`TestDeletedAccountGenerationHistoryUnwind` 覆盖已有/临时账户、计数器不存在/已存在零值、自毁/删除、撤销/不撤销，共 16 个组合；另测删除后同块重建。测试提交真实变化记录与 MuHash，并用全物理状态 oracle 核验，再逆向恢复父块根及计数器存在性。修复前缺失计数器的路径可复现，修复后 17 个场景全部通过，race 检查通过。

恢复增加两个显式约束：`--sync.restart-require-incremental` 在逆向历史不可用时拒绝操作，禁止回落到 genesis reset+replay；`--sync.restart-only` 完成回退、关闭数据库后立即退出，不启动网络/API。修复无 ancient 数据库时向 rewind 传入 typed-nil writer 的崩溃。CLI 集成测试从真实持久化区块执行离线回退，在所有监听端口已被占用时仍成功，关闭后可重新打开数据库并验证目标 HEAD。

当前补丁验证：`go test ./core -count=1 -timeout 900s` 通过（312.186 秒），`core/state`、`vm`、`actuator` 全包通过；generation 专项 race 通过。实际副本重放及线上恢复结果在完成后另记。
