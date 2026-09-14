# Commitment 预取缓存的字节所有权

2026-09-14。基于已部署833e4a0b继续优化。状态：实现、固定输入与组合回归/race/vet已通过；原生验收待部署。

现有snapshot parent session的预取只返回presence，不返回缓存字节；但两处填充调用
prefetchIfEpoch会将缓存backing标记为exposed。后续canonical flush因此无法复用本未外泄
的空间，必须复制替换。普通Buffer预取和其他返回字节的API仍需要expose=true。

增加只返回存储结果的storePrefetchIfEpoch，沿用完整setEntryIfEpoch路径、force预取准入、
所有epoch/global snapshot version/物理键检查，仅expose=false；仅替换session leader与
shared follower丢弃返回值的两处。保留缓存预算、队列、引用来源、prefetch useful标记、
missing/error行为和snapshot/overlay优先级，已exposed的entry不能被降回private。

缓存刷新仍沿用现有锁与容量charge规则。直接Get借用、共享flight结果、cursor原始字节均
不能与可变缓存backing混淆；callback读在锁保护期内，旧snapshot不能填回新缓存。
相等或缩短值的原位复用可能保留原容量，不能声称所有retained charge/eviction轨迹与旧版
完全相同。必须验证预算、shrink/grow压力与真实返回字节，而不是只比较exposed位。

验证至少包括leader/follower、legacy及generation key、缺失/空值/error、旧snapshot跨flush、
直接Get已外泄、callback/flush并发、返回cursor后覆盖输入、完整cache状态预算与队列所有权。
固定输入对照完整prefetch→读取→canonical flush操作，覆盖变长/缩短、cold/hot、无flush控制、
直接Get控制，并报告alloc/B/op、耗时范围和真实读数。仅低层helper微基准不作整体收益证据。

本地blockbuffer/domains相关回归与race、原生Sapling验收后方可部署。沿用所有reader永久标记
和兼容回滚、端口/内存/并发/GC配置。本轮不以预取有用比例直接关闭预取，不放宽缓存版本
守卫，也不改变flush准入策略。线上采样与固定输入结论分别报告。
