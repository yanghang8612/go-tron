# 冷化恢复与元数据优化（2026-10-13）

本轮实现了此前采样中有直接证据的两项优化，并增加事件 replay 统计。生产数据格式和参数不需要迁移。线上吞吐收益尚未测量，不将局部基准加速换算成整节点收益。

## 实现

1. 已进入中等债务 60% 档位后，第一份健康恢复观测继续保留 60%，避免恢复滞后阶段反而降为 20%。复用观测只可保持已有准入，不能重新取得已撤销的中档。严重增长、硬压力、写停顿、L0/设备压力、过期/未知/reset 证据立即撤销；已有恢复截止不缩短。新增 `history/budget/debt_recovery` gauge 标明这类恢复状态。
2. 冷源读租约复用 Manager 私有、已验证且不可变的 manifest 和路径索引。通过不暴露内部指针的 `PinnedManifestView` 跨过 core/source bridge，去掉原 pin 时的全表验证、复制、索引重建，以及随后 `Manager.Manifest()` 的第二份复制。文件保留租约和 KV/manifest 固定顺序保持原语义；公开 `OpenPinnedManager` 与 `Manager.Manifest` 仍提供独立可变副本。
3. 构建规划、准入提示与普通 integration 内部使用私有只读清单加载。每次仍对当前文件做完整字节比较；公开加载器继续返回独立副本，发布/进度更新仍在自有容器上完成。
4. 增加 `state/snapshot/cold/history/event_replay/` 下的 `attempts`、`replayed`、`overflows`、`source_passes`、`snapshot_unavailable` 以及 `last/peak_charge_bytes`、`last/source_rows`、`last/source_passes`。指标每次构建结算，循环中只更新本地计数；包含失败尝试，不代表成功发布数。8 MiB 预算、完整源验证及溢出回退行为保持原语义。

## 同输入基准

Go 1.25.5，Apple M1 Max，darwin/arm64，默认 GOMAXPROCS=10；每侧五次，每次十个操作，以下取中位数。清单形状为 12,000 个 history trio、36,000 active 引用、零 retired；范围、路径和校验值为合成，无真实 payload。两侧均执行覆盖计算。读租约基线冻结原 pin + source bridge 的元数据路径；当前使用同一 Manager、租约和输入。

| 操作 | 基线耗时 | 优化耗时 | 基线 B/op | 优化 B/op |
| --- | ---: | ---: | ---: | ---: |
| 完整 pin + cold metadata + 覆盖计算 | 10.643 ms | 0.491 ms | 13,647,121 | 916,072 |
| 当前字节认证 + 清单加载 + 覆盖计算 | 2.541 ms | 1.797 ms | 7,223,200 | 980,688 |

第一项耗时约减 95.4%、分配约减 93.3%；第二项耗时约减 29.3%、分配约减 86.4%。第二项的基线是公开 detached loader，优化是仅限内部的 immutable borrow；公开 API 的副本承诺没有被改变。小清单收益有限，不以这些合成/暖读结果预测全节点速度。

原始日志：`build/benchmarks/cold-followup/active-only-bench.txt`。另有含 retired 的早期探索基准，最终结果以上述匹配当前零-retired 形状的数据为准。

## 验证

- `GOTOOLCHAIN=go1.25.5 go test ./core/... ./actuator ./cmd/gtron -count=1 -timeout 900s` 通过：core 325.531s、snapshots 212.169s、pruning 75.684s、CLI 98.632s，其余相关包全部通过。
- 定向 race 覆盖债务恢复/保护、清单并发替换与公开副本隔离、pin/retirement 租约以及事件 replay：通过，10.533s。
- 新增/既有用例覆盖同长度同时间文件替换、损坏/缺失输入拒绝、预算关闭/拒绝、跨代固定视图、公开 constructor/caller 修改隔离、真实 merge 后旧文件延迟回收。
- `go build ./cmd/gtron` 通过；`git diff --check` 通过。
- 代码自查重点为私有只读路径的调用者、不可变 generation 生命周期、公开 ownership 与读租约边界；不声称经过独立审查。

## 上线观察与后续范围

观察同步 TPS、冷 txNum/秒、净积压、两库债务、写停顿、device await；单列 `debt_recovery=1` 时 duty 是否保持 600000。用 event replay 的溢出率和 source passes 决定下一轮容量/spool 实验。

历史库 LSM 参数、冷 merge 份额和承诺分支编码尚未调整。本次证据不足以证明提高压实并发或放大事件缓存一定有净收益；它们需要分别对齐工作量并验证，不与这次可独立回归的改动混合上线。重启不要求清库或存量数据转换。
