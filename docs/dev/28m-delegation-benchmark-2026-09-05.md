# 28M legacy 委托索引优化：StateDB 微基准

日期：2026-09-05。此报告测量 P0a 的局部收益，不能据此认定整个 28M 同步已经达到 +50% 或两倍吞吐目标。

## 结果

同一开发机、相同基准工作量，旧/新测试二进制顺序运行，每项 `-benchtime=100ms -count=3`。以下为三次结果的中位数；括号是最小值–最大值。加速比 = 旧版耗时 / 新版耗时。

已驻留的重复委托从随 degree 线性增长，变为本次 degree 32–100K 范围内约 0.16–0.17 µs、64 B、2 次分配。D100K 的真正追加并回滚为 18.700 → 7.688 ms，删除后重加并回滚为 36.167 → 8.295 ms；包含历史序列化和发布的删除交易为 19.289 → 9.064 ms。后面三项仍包含完整大行的处理，不能称为 O(1)。

## 环境与采样边界

- Apple M1 Max，darwin/arm64，Go 1.27.1；基准输出的运行并行度后缀为 `-10`。未设置 `GOMAXPROCS`、`GOGC`、`GOMEMLIMIT` 覆盖值。
- 仓库基准 HEAD：`94da94aca85b95a6a992baf864f6836e8c402c73`。旧二进制在 canonical 热路径修改前编译；新版包含本次专用 native codec 和 StateDB 邻接缓存。两者运行相同基准主体。
- 本次正式四轮执行时间：2026-09-05 00:29:38–00:30:00 UTC（北京时间 08:29:38–08:30:00），顺序为旧 StateDB、新 StateDB、旧 history、新 history。
- 使用普通开发机；已协调避免主动并行运行其他大 benchmark，仍不能排除短暂普通测试/系统负载争用。此前与普通测试可能重叠的预跑不进入以下中位数。
- 100 ms、三次采样只用于工程方向判断；D100K 旧版某些项目单个样本仅 3–6 次操作。没有据此计算统计显著性，不能将个位数百分比变化当作稳定结论。

degree 32 的新版追加/回滚样本为 6.972–24.881 µs，波动明显；全部样本均保留在表格和原始输出中，未按结果剔除异常值。本次微基准不能验证方案要求的空/轻块回退不超过 3%。

## 固定工作量

基准位于 `core/state/delegation_performance_test.go`。初始状态通过独立 generic `statecodec.Marshal` 直接写入 native rooted-state 行，再 Commit；种子生成和初始 Commit 不计入计时。地址为唯一、合法长度的 21 字节地址。

这是**不对称星形索引**：from 有 D 条 ToAccounts，所选 to 有 1 条 FromAccounts；选中对端在 from 列表中部。只为实际读取的两个端点建立行，并不生成 D 个对端状态账户。这不覆盖两个端点都具有 D 条关系的情形，也不是一份完整、全关系一致的链上快照。

| 项目 | 每次计时包含的工作 | 有界性与适用范围 |
|---|---|---|
| ResidentDuplicate | 同一 StateDB 预热后，对已存在的双方关系再次委托 | 双方行保持不变；单一热点工作集能进入当前缓存预算。没有计入首次加载。 |
| ColdStateDuplicate | New StateDB，然后执行一次重复委托 | 底层内存数据库保持热；包含 StateDB 创建及索引加载，不代表物理磁盘冷读。 |
| AppendRevert | Snapshot；向缺失对端追加真实关系；Revert | 计时包含回滚。每次恢复初始关系数，不随 b.N 增长；回滚后可能重新加载被失效的缓存。 |
| RemoveReaddRevert | Snapshot；删除中部关系；重新加到列表尾部；Revert | 每次计时是两次真实 mutation 加回滚，不是单次 actuator；关系顺序和基准规模恢复。 |
| HistoryFlush | New；BeginDomainChangeJournalCapture；transaction mark；删除关系；FinalizeTransaction；FlushDomainChangesSince；FlushPendingDomainChanges | 使用真实 rawdb 历史行序列化和内存 writer。固定 block/txNum 覆盖相同历史键，避免存储随 b.N 增长；不含最终 Commit/commitment 或磁盘 I/O。 |

ResidentDuplicate 的 100% 重复工作量是上界场景。此次没有测量 0/50/100% 重复混合后的真实区块组成，也未覆盖多热点缓存竞争、超预算巨大行、内存高水位、history 后台债务或真实服务器 Pebble/I/O。64 B/op 是每次新增分配，不能解释为 resident 索引本身只占 64 字节。

## 已驻留重复委托

后续正式链路审查发现 `BlockChain.prepareOpenState` 每块重绑同一个 physical index store，初版无条件失效会丢掉跨块 resident。现已改为仅同一 concrete pointer 重绑保留缓存，并补充跨 Commit、CommitScope 和不可比较 wrapper 测试；真实 store/reader/generation 变化仍失效。下面保留冻结二进制的原始测量，不将直接 StateDB/Commit 基准冒充覆盖该正式链路的跨块证明。

耗时单位：µs/次。

| degree | 旧版中位数（范围） | 新版中位数（范围） | 加速比 | B/op 旧 → 新 | allocs/op 旧 → 新 |
|---:|---:|---:|---:|---:|---:|
| 32 | 12.097 (11.932–12.114) | 0.1694 (0.1635–0.2298) | 71.41× | 16,137 → 64 | 253 → 2 |
| 1,000 | 239.324 (237.351–241.700) | 0.1705 (0.1636–0.1778) | 1,403.66× | 442,768 → 64 | 5,109 → 2 |
| 10,000 | 2,012.631 (1,981.836–2,040.518) | 0.1627 (0.1608–0.1628) | 12,370.20× | 4,417,298 → 64 | 50,132 → 2 |
| 100,000 | 18,630.132 (18,451.840–18,743.917) | 0.1594 (0.1591–0.1611) | 116,876.61× | 51,542,424 → 64 | 500,155 → 2 |

## 新 StateDB 的重复委托

耗时单位：µs/次。

| degree | 旧版中位数（范围） | 新版中位数（范围） | 加速比 | B/op 旧 → 新 | allocs/op 旧 → 新 |
|---:|---:|---:|---:|---:|---:|
| 32 | 20.578 (19.749–20.600) | 11.623 (11.508–11.675) | 1.77× | 26,797 → 18,098 | 284 → 98 |
| 1,000 | 248.888 (248.691–250.986) | 78.634 (77.308–82.873) | 3.17× | 453,799 → 186,737 | 5,141 → 1,068 |
| 10,000 | 2,036.593 (2,006.506–2,051.220) | 608.909 (597.315–619.483) | 3.34× | 4,429,432 → 1,592,143 | 50,165 → 10,100 |
| 100,000 | 18,313.215 (18,268.979–18,753.458) | 5,567.720 (5,418.550–5,618.784) | 3.29× | 51,554,612 → 14,823,506 | 500,190 → 100,328 |

## 追加与回滚

耗时单位：µs/次。

| degree | 旧版中位数（范围） | 新版中位数（范围） | 加速比 | B/op 旧 → 新 | allocs/op 旧 → 新 |
|---:|---:|---:|---:|---:|---:|
| 32 | 12.800 (12.765–12.908) | 8.054 (6.972–24.881) | 1.59× | 17,494 → 13,587 | 248 → 84 |
| 1,000 | 251.664 (248.954–254.714) | 98.474 (98.292–99.961) | 2.56× | 496,298 → 344,499 | 5,109 → 1,058 |
| 10,000 | 2,093.087 (2,075.150–2,101.055) | 846.211 (837.953–847.314) | 2.47× | 4,865,981 → 2,993,594 | 50,137 → 10,023 |
| 100,000 | 18,699.681 (18,660.521–19,095.236) | 7,688.470 (7,634.098–7,866.738) | 2.43× | 55,958,946 → 28,499,285 | 500,167 → 97,993 |

## 删除、重加与回滚

耗时单位：µs/次。

| degree | 旧版中位数（范围） | 新版中位数（范围） | 加速比 | B/op 旧 → 新 | allocs/op 旧 → 新 |
|---:|---:|---:|---:|---:|---:|
| 32 | 22.443 (22.259–22.749) | 9.851 (8.585–21.007) | 2.28× | 29,612 → 14,096 | 460 → 93 |
| 1,000 | 488.074 (470.332–491.842) | 112.185 (109.377–116.522) | 4.35× | 892,682 → 357,233 | 10,182 → 1,069 |
| 10,000 | 4,071.589 (4,054.512–4,077.264) | 950.503 (938.704–957.415) | 4.28× | 8,843,483 → 3,127,906 | 100,230 → 10,026 |
| 100,000 | 36,167.056 (35,782.555–36,437.847) | 8,295.077 (8,027.635–8,560.079) | 4.36× | 103,097,074 → 29,870,068 | 1,000,282 → 97,767 |

## 删除交易与历史发布

耗时单位：µs/次。

| degree | 旧版中位数（范围） | 新版中位数（范围） | 加速比 | B/op 旧 → 新 | allocs/op 旧 → 新 |
|---:|---:|---:|---:|---:|---:|
| 32 | 27.713 (27.583–28.103) | 22.531 (22.040–22.610) | 1.23× | 41,212 → 37,136 | 358 → 203 |
| 1,000 | 298.605 (297.803–304.476) | 148.881 (146.500–153.410) | 2.01× | 724,594 → 527,634 | 5,225 → 1,180 |
| 10,000 | 2,370.775 (2,318.949–2,381.056) | 1,096.603 (1,083.732–1,096.806) | 2.16× | 6,881,609 → 4,670,946 | 50,261 → 10,223 |
| 100,000 | 19,288.715 (19,185.028–19,537.410) | 9,063.774 (8,871.097–9,546.612) | 2.13× | 75,816,509 → 45,696,056 | 500,290 → 100,455 |

## 语义验证

最终测量所用的新版二进制通过 `TestLegacyDelegationReferenceBytesHistoryRootAndRollback` 的全部六类输入：双方缺失、仅 from 存在、仅 to 存在、不对称列表、异常重复项、from==to。每类执行委托、重复委托、嵌套 rollback、删除、重加、删除不存在关系、追加另一对端七个步骤。

oracle 直接调用原来的 generic `statecodec` 和 `Get/SetAccountKV`，自行实现旧列表规则，不将优化后的 reader/writer 当作旧实现。逐步比较 native 行字节与存在性，最后比较历史记录、每个 txNum 的 as-of 结果和最终 root；嵌套 rollback 额外检查恢复后的字节。这里验证的是这六类状态测试，没有完成真实 canonical block replay 的逐块/逐交易全域比对。

## 复跑与指纹

### 本次代码验证

- `go test ./... -count=1 -timeout 300s`：通过。
- `go test -race ./core/state ./core/state/statecodec ./actuator -count=1 -timeout 300s`：通过；后补的 membership 阈值、LRU/行数与字节预算边界测试另行通过普通与 race 测试。
- `FuzzDelegationIndexNative` / `FuzzDelegationIndexMessage`：与 generic codec 的输入接受范围、字段与输出字节 differential fuzz 通过。
- `go vet ./...`：通过。
- `scripts/system_test.sh`：79 passed、0 failed、0 skipped。
- `make gtron`：Sapling 构建通过，系统测试后重新构建，保证最终本地二进制带 Sapling。
- `git diff --check`：通过。
- lint：先因 PATH 中缺少工具失败，后在 `/tmp/gtron-28m-tools` 安装仓库历史审计使用的 `golangci-lint v2.13.1`。全仓 `make lint` 报 158 项，与历史审计记录的数量相同；基于完整修改补丁（包括未跟踪的新 Go 文件）的 `--new-from-patch` 检查为 **0 issues**。已修正基准测试新发现的一处关闭资源未检查错误；全仓 lint 仍非全绿。

状态层补充验证包括 generic put/delete/prefix delete、reader/generation/reset 切换、Commit 后复用、StateDB copy 隔离、public reader 所有权、缓存命中的逻辑依赖、事务版本 reader 回退、先检查双方读取错误、sticky error，以及跨 membership 阈值的增删重加和嵌套回滚。缓存预算测试包含真实 API 的 1,024 行 LRU 竞争、合成 charge 的 32 MiB exact-fit/超额，以及超预算 decoded 形状在分配 membership 前拒绝准入；这些不是整进程 RSS 上限证明。

测试日志位于本机 `/tmp/gtron-28m-{all-tests,race-tests,cache-boundaries-race,vet,system-test,build,lint,lint-changed}.log`。增量 lint 输入为 `/tmp/gtron-28m-review.patch`。尚缺真实 canonical blocks 的 28M 顺序 A/B、proposal 转换历史区间重放、轻块回退和服务器长期 RSS/I/O/后台债务数据；本次没有部署服务器。

### 基准复跑

正式数字以冻结的两个测试二进制为准。源码后续变更应重新编译并复测；这两个 `/tmp` 文件和原始输出是本机临时产物，以下同时保存原始输出便于长期核验。

```bash
go test -c ./core/state -o /tmp/gtron-28m-state-optimized.test

# 分别令 binary 为下面的旧、新绝对路径，顺序执行。
binary=/tmp/gtron-28m-state-baseline.test
# binary=/tmp/gtron-28m-state-optimized.test
"$binary" -test.run '^$' \
  -test.bench '^BenchmarkLegacyDelegationStateDB$/^degree=(32|1000|10000|100000)$/^Canonical$/(ResidentDuplicate|ColdStateDuplicate|AppendRevert|RemoveReaddRevert)$' \
  -test.benchtime=100ms -test.count=3
"$binary" -test.run '^$' \
  -test.bench '^BenchmarkLegacyDelegationHistoryFlush$/^degree=(32|1000|10000|100000)$/^Canonical$' \
  -test.benchtime=100ms -test.count=3

/tmp/gtron-28m-state-optimized.test \
  -test.run '^TestLegacyDelegationReferenceBytesHistoryRootAndRollback$' -test.v
```

旧二进制必须使用本次保存版本，不能从优化后的 working tree 重新编译后仍称为旧基线。文件里的 `GenericReference` 子基准可用于同一新版二进制中的旧算法对照，正式表格使用的是已冻结旧二进制的 `Canonical`。

- `/tmp/gtron-28m-state-baseline.test` SHA-256：`e5e2640e29682b79d97eb2d8dc15eb99c78ae50c5dba71e6126f30d7012e39ce`。
- `/tmp/gtron-28m-state-optimized.test` SHA-256：`f64f15627d7403f80e32f267cd599d746ef5198deb8e90fe8b347ea37462e005`。
- `/tmp/gtron-28m-ab-manifest.json` 保存命令数组和逐轮 UTC 起止时间。

### old state 原始输出

`/tmp/gtron-28m-ab-old-state.txt`，SHA-256：`032bbc2aff5d21d4120552410f1d08bacd698600db1c50bf2d3638cf4fd6674d`。

```text
goos: darwin
goarch: arm64
pkg: github.com/tronprotocol/go-tron/core/state
cpu: Apple M1 Max
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	    9814	     12097 ns/op	   16137 B/op	     253 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	   10000	     12114 ns/op	   16137 B/op	     253 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	   10000	     11932 ns/op	   16137 B/op	     253 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	    6170	     20600 ns/op	   26798 B/op	     284 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	    5878	     20578 ns/op	   26797 B/op	     284 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	    6394	     19749 ns/op	   26797 B/op	     284 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   10000	     12800 ns/op	   17494 B/op	     248 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   10000	     12765 ns/op	   17493 B/op	     248 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   10000	     12908 ns/op	   17495 B/op	     248 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	    5568	     22749 ns/op	   29612 B/op	     460 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	    5659	     22259 ns/op	   29612 B/op	     460 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	    5668	     22443 ns/op	   29613 B/op	     460 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	     498	    239324 ns/op	  442768 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	     513	    241700 ns/op	  442770 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	     502	    237351 ns/op	  442761 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	     464	    248888 ns/op	  453805 B/op	    5141 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	     472	    248691 ns/op	  453780 B/op	    5141 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	     477	    250986 ns/op	  453799 B/op	    5141 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	     459	    251664 ns/op	  496298 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	     480	    248954 ns/op	  496286 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	     489	    254714 ns/op	  496333 B/op	    5109 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	     248	    488074 ns/op	  892682 B/op	   10182 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	     249	    491842 ns/op	  892725 B/op	   10182 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	     249	    470332 ns/op	  892671 B/op	   10182 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	      62	   2040518 ns/op	 4417296 B/op	   50132 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	      58	   2012631 ns/op	 4417298 B/op	   50132 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	      54	   1981836 ns/op	 4417497 B/op	   50132 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	      63	   2036593 ns/op	 4429442 B/op	   50165 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	      55	   2051220 ns/op	 4429432 B/op	   50165 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	      60	   2006506 ns/op	 4429416 B/op	   50165 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	      63	   2101055 ns/op	 4865981 B/op	   50137 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	      52	   2093087 ns/op	 4865847 B/op	   50137 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	      61	   2075150 ns/op	 4866197 B/op	   50137 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	      28	   4071589 ns/op	 8843483 B/op	  100230 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	      27	   4077264 ns/op	 8843610 B/op	  100230 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	      30	   4054512 ns/op	 8843178 B/op	  100230 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	       6	  18630132 ns/op	51542442 B/op	  500155 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	       6	  18451840 ns/op	51542405 B/op	  500155 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	       6	  18743917 ns/op	51542424 B/op	  500155 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	       6	  18313215 ns/op	51555705 B/op	  500192 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	       6	  18268979 ns/op	51554612 B/op	  500190 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	       6	  18753458 ns/op	51554509 B/op	  500188 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	       6	  18660521 ns/op	55958760 B/op	  500165 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	       6	  19095236 ns/op	55958946 B/op	  500167 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	       6	  18699681 ns/op	55959165 B/op	  500167 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	       3	  35782555 ns/op	103096437 B/op	 1000280 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	       3	  36437847 ns/op	103097210 B/op	 1000283 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	       3	  36167056 ns/op	103097074 B/op	 1000282 allocs/op
PASS
```

### new state 原始输出

`/tmp/gtron-28m-ab-new-state.txt`，SHA-256：`e16ec9d99e4dada65c8f72eb503166c10c32d57b3d0a72677be1dd9e83b95979`。

```text
goos: darwin
goarch: arm64
pkg: github.com/tronprotocol/go-tron/core/state
cpu: Apple M1 Max
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	  763034	       163.5 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	  755032	       229.8 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ResidentDuplicate-10         	  735331	       169.4 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	   10000	     11623 ns/op	   18098 B/op	      98 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	   10000	     11675 ns/op	   18098 B/op	      98 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/ColdStateDuplicate-10        	   10000	     11508 ns/op	   18098 B/op	      98 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   14937	      6972 ns/op	   13587 B/op	      84 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   16503	      8054 ns/op	   13587 B/op	      84 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/AppendRevert-10              	   16497	     24881 ns/op	   13580 B/op	      84 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	    6007	     21007 ns/op	   14092 B/op	      93 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	   13886	      8585 ns/op	   14097 B/op	      93 allocs/op
BenchmarkLegacyDelegationStateDB/degree=32/Canonical/RemoveReaddRevert-10         	   12385	      9851 ns/op	   14096 B/op	      93 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	  789776	       177.8 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	  778251	       170.5 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ResidentDuplicate-10       	  806586	       163.6 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	    1570	     82873 ns/op	  186744 B/op	    1068 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	    1562	     78634 ns/op	  186737 B/op	    1068 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/ColdStateDuplicate-10      	    1609	     77308 ns/op	  186732 B/op	    1068 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	    1251	     99961 ns/op	  344491 B/op	    1058 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	    1252	     98292 ns/op	  344504 B/op	    1058 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/AppendRevert-10            	    1222	     98474 ns/op	  344499 B/op	    1058 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	    1041	    109377 ns/op	  357215 B/op	    1068 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	    1045	    116522 ns/op	  357248 B/op	    1069 allocs/op
BenchmarkLegacyDelegationStateDB/degree=1000/Canonical/RemoveReaddRevert-10       	    1046	    112185 ns/op	  357233 B/op	    1069 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	  751232	       162.7 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	  725823	       162.8 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ResidentDuplicate-10      	  724164	       160.8 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	     199	    619483 ns/op	 1592209 B/op	   10100 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	     186	    608909 ns/op	 1592143 B/op	   10100 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/ColdStateDuplicate-10     	     195	    597315 ns/op	 1592140 B/op	   10100 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	     140	    837953 ns/op	 2993594 B/op	   10023 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	     141	    846211 ns/op	 2993542 B/op	   10023 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/AppendRevert-10           	     141	    847314 ns/op	 2993616 B/op	   10024 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	     126	    950503 ns/op	 3127855 B/op	   10026 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	     127	    957415 ns/op	 3127948 B/op	   10027 allocs/op
BenchmarkLegacyDelegationStateDB/degree=10000/Canonical/RemoveReaddRevert-10      	     126	    938704 ns/op	 3127906 B/op	   10026 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	  736158	       159.1 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	  786111	       159.4 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ResidentDuplicate-10     	  650274	       161.1 ns/op	      64 B/op	       2 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	      21	   5618784 ns/op	14823504 B/op	  100328 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	      18	   5567720 ns/op	14823533 B/op	  100328 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/ColdStateDuplicate-10    	      21	   5418550 ns/op	14823506 B/op	  100328 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	      42	   7688470 ns/op	28490920 B/op	   97936 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	      43	   7634098 ns/op	28499285 B/op	   97993 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/AppendRevert-10          	      49	   7866738 ns/op	28541449 B/op	   98278 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	      40	   8295077 ns/op	29879619 B/op	   97832 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	      39	   8027635 ns/op	29869514 B/op	   97765 allocs/op
BenchmarkLegacyDelegationStateDB/degree=100000/Canonical/RemoveReaddRevert-10     	      39	   8560079 ns/op	29870068 B/op	   97767 allocs/op
PASS
```

### old history 原始输出

`/tmp/gtron-28m-ab-old-history.txt`，SHA-256：`d435b5a54f6a52be9666a0af12ba04a0f1fee3839a53bc41b3a7ac651e190c71`。

```text
goos: darwin
goarch: arm64
pkg: github.com/tronprotocol/go-tron/core/state
cpu: Apple M1 Max
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    4502	     27713 ns/op	   41210 B/op	     358 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    4273	     27583 ns/op	   41212 B/op	     358 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    4569	     28103 ns/op	   41212 B/op	     358 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     367	    298605 ns/op	  724594 B/op	    5225 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     402	    304476 ns/op	  725009 B/op	    5225 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     396	    297803 ns/op	  723693 B/op	    5224 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	      52	   2370775 ns/op	 6877236 B/op	   50261 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	      44	   2381056 ns/op	 6881702 B/op	   50262 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	      48	   2318949 ns/op	 6881609 B/op	   50261 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	       6	  19537410 ns/op	75816456 B/op	  500288 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	       6	  19288715 ns/op	75816841 B/op	  500290 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	       6	  19185028 ns/op	75816509 B/op	  500291 allocs/op
PASS
```

### new history 原始输出

`/tmp/gtron-28m-ab-new-history.txt`，SHA-256：`505ce6c3ee1ee0a2fa3d5d3c42945c577656d7789253cb7cbfd3fcfae9aa3f72`。

```text
goos: darwin
goarch: arm64
pkg: github.com/tronprotocol/go-tron/core/state
cpu: Apple M1 Max
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    5149	     22531 ns/op	   37143 B/op	     203 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    5574	     22040 ns/op	   37136 B/op	     203 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=32/Canonical-10         	    6063	     22610 ns/op	   37136 B/op	     203 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     789	    146500 ns/op	  527634 B/op	    1180 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     824	    148881 ns/op	  527477 B/op	    1180 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=1000/Canonical-10       	     764	    153410 ns/op	  527808 B/op	    1180 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	     110	   1096806 ns/op	 4672218 B/op	   10223 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	     106	   1096603 ns/op	 4670946 B/op	   10223 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=10000/Canonical-10      	     100	   1083732 ns/op	 4655256 B/op	   10223 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	      12	   8871097 ns/op	45695858 B/op	  100455 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	      13	   9546612 ns/op	45696622 B/op	  100458 allocs/op
BenchmarkLegacyDelegationHistoryFlush/degree=100000/Canonical-10     	      12	   9063774 ns/op	45696056 B/op	  100455 allocs/op
PASS
```

### 差分测试原始输出

```text
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/missing-both
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/from-only
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/to-only
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/asymmetric-lists
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/duplicate-entries
=== RUN   TestLegacyDelegationReferenceBytesHistoryRootAndRollback/self
--- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback (0.01s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/missing-both (0.00s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/from-only (0.00s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/to-only (0.00s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/asymmetric-lists (0.00s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/duplicate-entries (0.00s)
    --- PASS: TestLegacyDelegationReferenceBytesHistoryRootAndRollback/self (0.00s)
PASS
```
