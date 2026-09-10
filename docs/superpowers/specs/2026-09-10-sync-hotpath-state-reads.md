# 同步热路径：精确状态读取

本轮由 `docs/dev/sync-performance-sampling-20260910.md` 的线上采样驱动。主网 20.69M–20.75M 区间下载缓存充足、导入忙碌率 99.36%，前台交易执行与状态访问为第一优化对象。CPU profile 中 ContractRuntime 仍完整解码 native SmartContract 的 ABI；奖励查询/结算的 GetAccount 引发无关账户 map 的 prefix 物化。

## Native 合约运行时元数据

在 statecodec 增加 SmartContract 的只读投影，取 origin address、resource percent、origin energy limit、version、trx hash。完整遍历和验证 native framing、字段顺序、类型、shape、规范默认值、数值范围、UTF-8、ABI/Entry/Param 子消息与 unknown trailer；不构造 ABI 对象图或复制 bytecode。schema 不符合专用实现预期、或快路不能处理的输入，走原 Unmarshal，以通用 decoder 为接受/拒绝及错误行为 oracle。

StateDB 在本次调用内将借用字段转成已有运行时值类型，沿用 contractRuntimeLoaded 的失效逻辑，不新增跨块缓存。GetContract、磁盘字节、root、storage-key layout、mutable protobuf 行为保持一致。测试必须包括 native 真正热路径，不能继续仅用 protobuf wire benchmark 代表生产。

## 奖励路径

Java 依据为本地 java-tron `MortgageService.withdrawReward/queryReward`。queryReward 仅读 existence、allowance、votes；withdrawReward 在早退与当前无票分支不需要完整账户。沿用现有 typed/scoped StateDB 读取，保留版本读集记录和相关读取错误的 fail-closed 行为。

当结算需要写 account-vote snapshot 时，仍在任何 AddAllowance 之前读取并序列化完整 Account，保持 Java detached AccountCapsule 的历史字节。不能仅保存 votes/allowance，也不能将快照推迟到收益入账后。循环边界、旧快照结算、cursor 更新顺序及奖励算法不变。

选择性读取移除不相关 prefix 扫描后，投票缓存命中仍须记录全部 30 个允许槽及账户 generation，涵盖向空槽插入的冲突。复用临时 key 的重复读取只能查找 recorder 的 map，不能用借用字符串覆写已保存的 key；模式升级也必须保存拥有独立字节的 key。用旧代码 overlay 复现故障，再验证修复后的读取/写入捕获和 reset 生命周期。

完整奖励快照字节对照以 SystemReward 中的原始 native 行为准。ReadCycleAccountVote 的 API 会用非确定性 protobuf 重编码 map，不能用两次 API 返回的字节顺序判断持久化行是否发生变化。

## 验证与范围

固定输入差分覆盖正常、缺失、稀疏、损坏、unknown、原生编码、dirty/generation/copy/revert 和相关读取依赖；奖励验证 allowance/cursor 与完整快照字节。分别报告 native decode、StateDB first-hit、奖励查询及结算分支的 CPU/分配，不将微基准倍数称为全链收益。相关包/race、全仓测试、构建与必要系统测试通过后再评估上线。

本轮不同时扩大 peers、签名并发或提交 depth。commitment 已有 partition、cache/prefetch、不可变 owned arena；全窗队列背压仅 0.524%。分支字节本身与 layer 生命周期有关，缺少足够证据时不做提前复用或改变 root 的修改。清单现有内容校验与完整生命周期恢复也不为本轮缩短测试而改动。
