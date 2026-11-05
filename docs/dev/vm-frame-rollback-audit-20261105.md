# VM 调用帧与异常边界修复及 Java 差分验证

审查确认的三个问题已修复，回归测试已进入默认测试集。84 个原始回归场景和 80 个基于真实 Java VM 输出的差分场景全部通过；完整 core 套件、相关包、专项 race 检查和纯 Go 构建通过。本次未部署线上或修复现有节点数据。下列问题位置基于审查时的源版本。

## 范围与基线

- go-tron：`29e36027dca31b507d2d36dac7a286489527e35d`，已包含上一轮 DELEGATECALL 回滚及 generation 历史修复。
- java-tron：本机源码 `4e8389f159d0782f1ebcd9dab2fd88388c0e6fe9`。
- 覆盖 CALL / CALLTOKEN / CALLCODE / DELEGATECALL / STATICCALL、CREATE / CREATE2、日志与内部交易、动态能量状态、账户生命周期及状态 journal 的相关边界。
- 初始确认依据为 Go 最小复现和 Java 源码对照；修复后另执行真实 Java VM 差分验证，方法与结果见下文。本次没有双实现链上重放或生产部署，也未证明已有主网交易触发这些发现。

## 1. [P1] 失败 STATICCALL 遗留动态能量状态

位置：`vm/tvm.go:1394`、`vm/tvm.go:1444`、`vm/tvm.go:1455`；写入源为 `vm/dynamic_energy.go:37` / `:73`。

`StaticCall` 设置解释器 `readOnly`，但未建立 StateDB 快照，失败时只标记内部交易 rejected。只读限制针对写操作码，解释器仍会在进入合约时 catch up `ContractState` 的周期/因子，并在执行结束时记录能量用量。这些写入经过 StateDB，却不会在 STATICCALL 自身失败时撤销。

复现调用树：普通父 CALL → STATICCALL 子合约 → 成功 STATICCALL 孙合约；子合约随后 REVERT、非法指令、写保护失败或耗尽能量；父合约捕获失败并成功结束。子、孙合约的动态能量状态均泄漏。例如 REVERT 子合约原为 `cycle=1, usage=17`，调用后变为 `cycle=2, usage=66`。成功孙调用的状态也应随失败祖先一起撤销。

Java `Program.callToAddress` 在 `Program.java:1039` 建立子 Repository，动态能量通过 `updateContextContractFactor` / `addContextContractUsage` 写入它（`:2375`、`:2401`），仅在调用成功时提交（`:1168–1184`）。失败子帧因此不会向父帧发布这些修改。

影响：立即改变持久化的合约运行状态，并可能改变后续动态能量因子、扣费及回执。这与上一轮“父调用捕获失败后，子调用状态仍被保留”的缺陷同类。

修复边界：给 STATICCALL 建立完整子帧状态快照，在 REVERT 和异常路径恢复；成功 STATICCALL 的动态能量更新应按 Java 保留，不能将所有静态调用中的内部状态写入一概丢弃。

复现测试：`TestFailedStaticCallDynamicState`，动态能量开/关 × tracer 开/关 × 五种结局，共 20 个场景：8 个异常场景复现，12 个控制场景通过。

## 2. [P1] CALLCODE 省略 endowment 余额校验

位置：`vm/instructions_call.go:385–387`，共用实现 `vm/tvm.go:1472`。

`opCallCode` 将 value 传给 `DelegateCall`，但两个方法均未检查当前上下文账户是否有足够余额。普通 CALL 有该检查；DELEGATECALL 使用继承的 call value，本来不应以它检查余额，因此不能简单在共享函数中按 `value` 无条件补检查。

复现：父合约余额 0，执行 `CALLCODE(value=1)`。Java 返回 0、退还转发能量，且在创建内部交易和递增 nonce 之前退出；Go 返回 1，并执行目标代码，把父合约 storage 写成 `0x44`，同时新增内部交易。余额 1 / value 2 也复现。目标为普通合约或 identity 预编译时，成功标志均错误。

Java 普通调用路径见 `Program.java:1060–1065`，预编译路径见 `:1700–1711`：二者都先检查 `senderBalance < endowment`。

影响：余额不足的 CALLCODE 可以走入 Java 不会执行的分支，直接改变账户状态、内部交易、能量及外层交易结果。

修复边界：在 CALLCODE 自身或明确区分调用类型的共享入口校验 endowment；余额不足时不执行目标、不创建内部交易、不递增 nonce，并保持 Java 的转发能量退款行为。普通 DELEGATECALL 继续遵循其继承上下文语义。

复现测试：`TestCallCodeChecksEndowmentBalance`，普通/预编译目标 × tracer 开/关 × 四种余额/value 组合，共 16 个场景：8 个余额不足场景复现，8 个正常控制场景通过。

## 3. [P1] 内部 CREATE/CREATE2 的 EF 前缀检查晚于 REVERT

位置：`vm/tvm.go:950–974`。

London 开启时，构造函数 `REVERT(0xEF…)` 在 Go 的通用 `err != nil` 分支提前返回 `ErrExecutionReverted`，不会到达后面的 EF 检查。Java 内部创建先检查 `createResult.getHReturn()` 首字节并设置 InvalidCode（`Program.java:934–940`），再处理 exception/revert；存在 exception 时直接返回，不退款、不向父帧暴露 revert data（`:958–975`）。

复现中转发 100,000 energy，Go 退还 99,985，并返回普通 REVERT；Java 对应分支应为 InvalidCode，退还 0。父帧可通过剩余能量及 RETURNDATA 观察差异，进而改变交易结果和扣费。

这属于匹配 Java 既有行为的兼容性要求。外部顶层部署的 REVERT 不走 Java 的内部 `createContractImpl`，修复时应保留这一边界；不能将顶层 REVERT 一起转换成 InvalidCode。

复现测试：`TestCreateRevertEFExceptionOrdering`，CREATE / CREATE2 / 顶层部署 × London 开/关 × tracer 开/关 × EF/非 EF 前缀 × RETURN/REVERT，共 48 个场景：4 个内部 EF REVERT 场景复现，44 个控制场景通过。

## 验证结果与复现方式

原始 opt-in 审查测试现已转为默认运行的 `vm/frame_boundary_regression_test.go`。下面的表记录修复前的结果；修复后 84 个场景全部通过，包括错误路径和正常控制组。另有 `vm/frame_boundary_java_test.go` 对照实际 Java VM 生成的 golden 数据。

```sh
go test ./vm -run 'TestFailedStaticCallDynamicState|TestCallCodeChecksEndowmentBalance|TestCreateRevertEFExceptionOrdering|TestJavaFrameBoundaryParity' -count=1 -timeout 180s -v
```

| 场景组 | 场景数 | 复现偏差 | 控制通过 |
| --- | ---: | ---: | ---: |
| STATICCALL 动态状态 | 20 | 8 | 12 |
| CALLCODE 余额 | 16 | 8 | 8 |
| CREATE/CREATE2 EF 顺序 | 48 | 4 | 44 |
| 合计 | 84 | 20 | 64 |

修复后最终验证：

- `go test ./vm ./vm/tracers ./core/state ./actuator -count=1 -timeout 300s`：通过（2.387 / 0.239 / 4.365 / 0.317 秒）。
- `go test ./core -count=1 -timeout 900s`：通过（318.639 秒）。
- 上述四个新增测试族的 `go test -race ./vm -run ... -count=1 -timeout 180s`：通过（2.128 秒），覆盖 164 个场景。
- `make gtron-pure`、`git diff --check`：通过。

原始 JSON 测试输出：`/tmp/go-tron-vm-audit-20261105/probes.jsonl`；普通套件输出：`/tmp/go-tron-vm-audit-20261105/baseline.log`。

## 已核对的其他边界与限制

- DELEGATECALL 到预编译时继承 caller，与 Java `callToPrecompiledAddress` 的 DELEGATECALL 特判一致，未列为问题。
- 失败调用的 nonce 和能量 penalty 不应一概回滚：Java 明确在子调用后保留 nonce，并在 `ProgramResult.merge` 中累计 penalty；修复快照时须保留这些语义。
- FREEZE/UNFREEZE 的动态资源权重和 transient storage 已有独立 journal entry；现有回滚测试通过。
- `newContracts` 标记表在祖先失败时缺少整体回退，属于进一步核查点，但尚未构造能改变既有合约可观察语义的合法交易，本轮不计入已确认缺陷。
- 本次修复和差分验证针对上述三个缺陷及其控制边界，不代表已穷举全部操作码、密码学预编译或 fork 组合。


## 最终实现

生产实现仅修改 `vm/tvm.go`：

1. STATICCALL 在深度校验后建立状态与日志快照；所有失败返回统一回滚。成功调用保留 Java 允许发布的动态能量更新，nonce 与 penalty 沿用原行为。
2. 共用委托入口按 `internalValue` 检查上下文余额。它代表实际 endowment：CALLCODE 传入 value，DELEGATECALL 传入 0。由此保留“继承 call value 大于当前余额的 DELEGATECALL 仍可成功”，同时覆盖普通与预编译目标的 CALLCODE 余额失败。
3. 仅对内部创建，将 London 下 `REVERT(0xEF…)` 转为 InvalidCode，再进入现有失败回滚路径；不退款且不发布该 revert data。顶层部署的 REVERT 行为保留。CREATE2 在 Osaka 之前保留父帧已有 return buffer 的历史规则也保留。

回归额外断言：失败创建不遗留账户、代码和元数据，内部交易 rejected 标记正确；正常创建保留部署代码；普通 REVERT 仍保留回退数据和剩余能量。

## 真实 Java VM 对照

`vm/testdata/frame_boundaries/JavaFrameOracle.java` 执行真实 `VM`、`Program`、`OperationActions` 和 `RepositoryImpl`。仅底层空数据库和固定 DP 输入使用 mock；子 Repository 的提交/丢弃、Storage 与 ContractState 的算法没有 mock 或重写。每个向量使用新的根 Repository，不启用 Java 的 `byTestingSuite` 快捷路径，不启动网络、不向根数据库提交。

生成器 `scripts/dev/run_vm_frame_oracle.py` 将 Java VM 包、VMConfig 和 ProgramResult 从指定源码重新编译到独立 scratch，使用 JDK 17.0.19；其他依赖来自已有 java-tron 模块构建和本机 Maven cache。源码版本固定为 `4e8389f159d0782f1ebcd9dab2fd88388c0e6fe9`，未修改该 Java checkout，原有未跟踪目录保留。

40 组向量分布：STATICCALL 12、CALLCODE 8、DELEGATECALL 控制 2、内部 CREATE/CREATE2 18。补充覆盖非零动态能量 penalty、继承 call value 大于余额，以及 CREATE2 在 Osaka 开/关时遇到 InvalidCode 后父帧既有 return data 的处理。

Go 分别开/关 tracer 执行每个向量，共 80 个场景。逐项比较：完整返回字节、剩余能量、penalty、nonce、内部交易 note/value/rejected、四个种子账户余额、ContractState 的 cycle/usage/factor、父合约两个 storage slot。全部相同。

用 Go build overlay 替换为修复前 `tvm.go`、其余 fixture 和测试保持相同，结果为 26 个场景失败 / 54 个通过；修复后 80 个全部通过。原始 84 场景再加 Java 对照 80 场景，共 164 个默认回归场景。

Golden 数据及生成源码均保存在 `vm/testdata/frame_boundaries/`，默认 Go 测试只读 golden，不依赖 Java 或网络。再生成命令与依赖版本见该目录 README。结果文件：

- `/tmp/go-tron-vm-audit-20261105/java-run-5/java_golden.json`、`compile.log`、`run.log`
- `/tmp/go-tron-vm-audit-20261105/java-parity-final.jsonl`
- `/tmp/go-tron-vm-audit-20261105/java-parity-before-final.jsonl`
- `/tmp/go-tron-vm-audit-20261105/final-packages.log`、`final-race.log`、`fixed-core.log`、`fixed-build.log`

## 额外发现：带网络前缀的预编译栈地址

构建对照探针时还观察到一个独立差异：用 `PUSH21 0x41…04` 调用时，Java 的预编译注册表比较原始 256 位 word，将它当作普通地址；Go 先截取/归一化地址，仍调用 identity 预编译。在余额足够的 CALLCODE 示例中，Java 的 nonce=1、一个普通内部调用，Go 的 nonce=0、无内部调用，剩余能量相差 15。

证据保留在 `/tmp/go-tron-vm-audit-20261105/java-run-3/java_golden.json` 和 `java-parity.jsonl`。这不是上述三个修正引入的变化，本次未修复。主差分语料明确使用标准 `PUSH20` ABI 栈地址；持久化账户键仍为 21 字节 TRON 地址。此项应作为独立地址解析兼容性工作跟进，不能将本次 80 场景通过解释为所有地址编码均已兼容。
