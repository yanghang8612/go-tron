# 停机状态下的历史空间回收

约束：主网 gtron 保持停止，磁盘不能扩容，MySQL 不能迁移。用户明确要求保留全部历史状态查询。维护命令不能启动 Node、BlockChain 生命周期、P2P 或同步，也不能修改持久化 prune mode。

## 操作分离

1. `db reclaim-empty-history` 默认只读计划。显式执行只允许压实已证明逻辑为空、且不超过 manifest hot-prune 水位的 changeset 区间；不写业务 KV、进度或 snapshot manifest。Pebble 的只读/读写切换共用目录锁，关闭自动压实，压实并发为一，SST 输出和 WAL 恢复共用硬写预算。真实整 SST 的重写可超出请求 key 区间；EstimateDiskUsage 不作为峰值上限。保留系统空闲空间与额外控制文件余量，失败后只读重开核对链头。
2. `db offline-history` 默认只读计划，执行时每次仅一批。按下述规则验证链身份，严格读取 head、Execution、Finish、solidified 动态属性；按 solidified 与热窗口求安全边界。按真实 changeset 数量、key/value 字节和排序成本限制输入与输出/ETL 临时空间。已有覆盖先验证；新覆盖先构建、验证、fsync、发布，再删除对应热副本，WAL Sync 后才发布进度，再次 Sync。不得运行 latest、code、checkpoint 清理或全局冷文件合并。

## 旧版未绑定 manifest 的兼容规则

当前生产 manifest 的 `Chain=nil` 来自原版未启用目录签名的生成路径：`Runner.PreflightCatalog` 在没有签名密钥时直接返回，只有启用签名时才调用 `EnsureProductionManifestChainIdentity`。因此，缺少 `Chain` 本身不是清单损坏的证据；它也不能代替链归属验证。兼容仅存在于本次离线 state-history 维护入口，不修改通用 `ValidateChainIdentity`、manifest 格式或正常启动语义。

有 `Chain` 的清单始终执行标准 `ValidateChainIdentity(ExpectedChain)`。`--legacy-manifest-sha256` 即使匹配，也不能绕过已有身份的不一致。只有 `Chain=nil` 时，才允许通过 `OfflineHistoryOptions.LegacyManifestSHA256` 进入额外的只读证明路径：

1. 显式提供当前 `manifest.json` 的精确 SHA-256。先计算 digest，再加载并验证清单，随后重新核对同一 digest；计划结束和每个写入阶段前再次核对，拒绝清单在读取或维护期间变化。不得把一次读取的清单对象与另一次读取的新 digest 配对。
2. 从当前 datadir 的 canonical 读取路径（需要时含只读 freezer）取得 genesis，要求其存在、非零，并等于 CLI 配置推导的 expected genesis。
3. 对所有 active `state-domain-change/history` 段检查已注册且完整的 history/index/accessor 三件套，要求各自具有 checksum 元数据，文件格式、范围、大小和记录数关系通过检查。没有任何 state-history 段、缺少显式区块范围表或缺少 companion，均拒绝兼容。
4. 读取每个 history 段显式区块范围表的首尾行，检查声明的 tx 范围、区块数与端点一致。首尾 `StateTxRange` 必须逐字段等于热库保留的对应行，且其 block hash 等于当前 canonical hash；相邻段的 block 与 tx 边界均必须连续。
5. 继续执行原有模式、Finish、solidified、安全窗口、cold frontier、hot-prune cursor 和 stage 水位检查。兼容身份的证明不会放宽本批区块选择或允许进度超过已验证边界。

上述步骤通过后报告 `LegacyManifestVerified=true`。该字段表示“这个精确清单版本的 state-history 边界已与当前链核对”，不表示所有历史 payload 已完成全量校验，也不为 event-log、其他 family 或 ForkConfigHash 作身份认证。预检按三件套数量线性读取冷文件 header/footer 和首尾范围；canonical hash 查询还可能读取首尾区块正文，但不扫描全部冷历史 payload、不创建 ETL 临时文件。

真正删除热历史之前，仍必须对覆盖本批的**完整三件套**执行 checksum 和语义一致性校验，再逐块核对 cold/retained `StateTxRange`，逐条比对每个剩余 hot record 的区块身份、tx、逻辑键及存在性和前值。仅端点正确不足以通过删除门。新构建的三件套执行相同检查；先 fsync 并发布冷覆盖，再删除对应热副本，删除的 WAL Sync 成功后才发布 hot-prune 进度。失败和重试沿用同一持久化顺序，历史继续由 hot/cold 组合完整提供。

兼容路径不调用全局 `EnsureProductionManifestChainIdentity`，不回填或重标 `Chain`。校验时仅为本批 state-history 构造临时内存清单；实际发布继续保持 `Chain=nil`，原样保留 event-log 等无关 family 的引用与文件。一次发布改变清单 SHA 后，下一批必须读取并显式提供新的 digest，旧 digest 必须被拒绝。

回归验证覆盖：精确 pin 下的只读计划、已有覆盖裁剪及下一批新构建；发布后旧 pin 拒绝；缺失/错误 genesis、端点与热范围或 canonical 不一致、缺少 companion/历史证明均不写不删；已有 `Chain` 不能被 pin 绕过；兼容证明通过后仍拒绝错误 hot 前值和不连续的多段边界，并保持无关 family 与历史读取结果。

## 保持的语义

历史继续由 hot/cold 组合提供；最新账户、KV、commitment、代码、StateTxRange、区块、交易与其他服务数据保持原样。相同磁盘上的硬链接 checkpoint 会固定旧 SST，不能作为本次物理回收的备份。失败时依赖 Pebble 已验证的事务发布协议以及冷文件先发布后删除的顺序，不手动删除数据库内部文件。

## 验证门

- 缺失/损坏/不一致的链头、solidified、Finish、manifest、模式或覆盖必须拒绝写入。
- 含存活 key 的物理压实范围必须拒绝；预算拒绝和 WAL 恢复失败后重开全库逻辑内容等价。
- 冷批次按真实字节拒绝超限单块，故障不得把热数据删在冷覆盖之前，不得让进度领先持久化删除。
- 实际服务器操作前复核停机状态并阻止自动部署重启；每次操作记录空间、范围、输出预算和链头/manifest 前后证据。

本方案回收历史重复与压实残留，不承诺固定容量能够永久保存无限增长的全历史。后续容量优化同样必须遵守保留全部历史状态查询的要求，不以缩短历史保留范围解决空间问题。
