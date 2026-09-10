# 同步状态热路径实施

- [x] 保留线上采样与 HEAD 原始代码基线，确定两个互不重叠的修改范围。
- [x] 实现 native SmartContract runtime projection，差分通用 codec，测 native first-hit；包括原 HEAD production 文件的 Go overlay 对照。
- [x] 缩小奖励查询/早退分支读取范围，保留完整 pre-settlement 快照；冻结旧函数作 oracle，比较实际存储行字节并按分支测量收益。
- [x] 审查错误、生命周期与依赖记录；补齐缓存投票读集，修复 recorder borrowed scratch key。相关 race、54 个包的全仓测试、Sapling 构建和 79 项双节点系统检查通过。增量 lint 为 0；初次全仓 lint 输出 158 项受默认限额截断，取消限额后报告 1,893 条，详见结果报告。
- [x] 在 `docs/dev/sync-state-hotpaths-20260910.md` 记录局部收益、持平/变慢分支及验证边界，原始证据保存在 `build/benchmarks/20260910-sync-hotpaths/`。

本轮未部署线上，未执行新版主网采样或真实主网区间 A/B replay；这些结果不能从微基准推导。后续部署须记录实际 Linux 版本和观察窗口。
