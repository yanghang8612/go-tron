# 独立历史暂存库：启动与迁移

`history-staging` 将状态历史 changeset 的长期热读写与 `chaindata` 分离到
`<datadir>/gtron/history-staging`。源库、目标库和冷快照目录必须是互不包含的真实目录。
目标库有独立 Pebble 指标、设备探针和 32 MiB memtable；`--db.cache 4096`
在启用后分为源库 3584 MiB、暂存库 512 MiB，总额仍为 4096 MiB。
主网进程的 Go 软限制保留 8 GiB、`GOGC=100`，systemd 硬限制 32 GiB。

现有主网库必须停机离线迁移。使用经过审查的固定 SHA 二进制及其源码
commit，由 root 执行：

```bash
/usr/bin/python3 /data/gtron/go-tron/scripts/history_staging_migrate.py run \
  --candidate /absolute/path/to/reviewed/gtron \
  --source 40位Git提交SHA \
  --sha256 64位二进制SHA256
```

若旧版生产 `manifest.json` 没有 `Chain` 元数据，必须额外传入停机后该文件的
精确 SHA256：`--legacy-manifest-sha256 64位manifestSHA256`。新版 CLI
仅在该显式 SHA 匹配时使用旧版边界认证，并继续逐桶核对冷文件 checksum、
语义、canonical hash 和 txrange；不会给旧 manifest 补写或伪造 Chain。
每次 `inspect`、`migrate`、`apply`、`resume` 都重新检查这份 SHA 和边界。

首次 staged reader 启动要认证已发布冷段。已强 SHA 核验的三文件组可在冷目录
留下独立的 advisory 物理证书；下一进程仅在 ContentID、三个精确 manifest
引用、规范目录及三文件 dev/inode/大小/权限/mtime/ctime 均匹配时复用。
缺少证书、格式损坏、文件变化或平台无可靠 ctime 时仍逐文件强 SHA。
按文件指纹复用不会每次发现指纹不变的静默介质损坏；需完整复核时使用
`--force-cold-sha`。证书仅是可丢弃的性能缓存。
启动完成日志与离线 cleanup 报告分别记录持久证书命中数和实际完成物理
SHA 的 trio 数；进程内缓存命中不计入这两个字段。
证书不代替路由、canonical、持久语义收据或首次 RPC 语义证明；每次启动
仍完整核对这些边界并在放开 API 前重查所有文件指纹。离线清理的
`cleanup-index --yes` 会在完整认证后尝试持久化证书，`--force-cold-sha`
可显式忽略缓存作全量物理复核；dry-run 严格不写。证书写入失败只使以后
重扫，不将已通过的物理认证变为失败，也不授权任何热库删除。
不能沿用旧库的 180 秒健康等待。迁移及后续主网发布默认给一次启动
12 小时的有界等待，旧库仍为 180 秒。预封锁安装的
`gtron-deploy.service` drop-in 设置 `TimeoutStartSec=26h`，覆盖新进程、
失败回滚进程各一次 12 小时认证及 2 小时构建余量。迁移可用
`--staging-health-timeout-sec` 指定 600–86400 秒；后续自动发布可配置
`STAGING_HEALTH_TIMEOUT_SEC`。超过默认值前须另设更高优先级的 root-owned
deploy unit timeout，并核有效 `TimeoutStartUSec ≥ 2 × 健康等待 + 2 小时`；
不足会在重启前拒绝。迁移 `activate-staging` 外层等待为一次健康上限
加 15 分钟。这些上限是故障边界，不是启动耗时预测。

校验期间固定进程 PID、身份和 `/proc/<pid>/exe` 指纹；服务退出、PID
变化或身份不符立即失败，API 尚未就绪时每 60 秒输出状态。离线 CLI
进度 stderr 实时转发，最新一次动作在 root-only 的
`/data/gtron/history-staging-ops/<job>_<action>.stderr.log` 保留至多
16 MiB；stdout 仍按最终 JSONL 身份和持久阶段核验。

编排器先安装旧版启动和自动部署的 guard，再停 timer、排空部署、获得
`/data/gtron/start.lock` 并写 durable migration latch。固定二进制执行
`gtron db history-staging migrate` 生成可恢复的 JSONL 计划，随后执行
`apply` 或 `resume`、`inspect --verify-complete`。只有全量路由、物理行、
canonical/Finish/Index/solid/retention 和冷段语义校验通过，编排器才将
同一 SHA 激活并检查服务健康。验收时的 `snap`/`archive` 模式和有效历史
窗口写入 root 所有的 reader marker；以后升级会原样保留它们，启动前
若服务配置发生变化则拒绝打开迁移布局。失败时服务保持停止且 latch 保留；按错误
输出的 job ID 继续：

```bash
/usr/bin/python3 /data/gtron/go-tron/scripts/history_staging_migrate.py resume \
  --job-id 32位jobID
```

现有主网服务以固定 `java-tron:java-tron` 身份运行，而离线 root CLI 会在
源库和暂存库生成 root 所有的 Pebble 文件。已验证且原服务本来运行中的
job 在激活前由独立 root release helper 执行所有权移交：核对持久激活
intent、候选和三库固定路径，证明父编排器仍持有启动锁和三库迁移锁，
并取得源、目标各自的 POSIX Pebble `LOCK`。它对两库平面文件实行严格
名称、类型、硬链接和所有权清单预检，仅把已核定的 Pebble 文件及目录
交给固定服务 UID/GID；root 专用迁移锁保持原主。冷库仅检查服务写入
权限，必要时只移交已核定的空 `etl` 目录；ancient 仅处理 `FLOCK`。
过程保留原文件内容和权限，并用 root-only `IN_PROGRESS`/`DONE` journal
允许部分移交后同一身份重试。开始服务前以服务身份检查实际访问。
若 reader 已经启动而编排器尚未写 `DONE`，重试只接受匹配的移交
`DONE` journal、已发布 marker 和当前进程精确身份，随后继续健康检查，
不会在运行中的 Pebble 库上重新 chown。此次 helper 热修明确拒绝原服务
未运行的迁移 job，因为现有 inactive 完成分支会在移交后再次用 root
打开数据库做最终校验；该分支需另行调整，不能套用此操作流程。

仅在首个 `inspect` 失败、尚无 plan 或任何迁移写入，且 gtron、部署服务及
timer 都已停止时，可以由 root 给同一 job 更换经过审查的新候选：

```bash
/usr/bin/python3 /data/gtron/go-tron/scripts/history_staging_migrate.py repin-preplan \
  --job-id 原32位jobID --candidate /absolute/path/to/new/gtron \
  --source 新40位Git提交SHA --sha256 新64位二进制SHA256 \
  --legacy-manifest-sha256 停机manifest的64位SHA256
```

编排器独占启动锁与三库锁、核对停机 manifest 的真实字节和固定候选能力，
再由新 CLI `inspect --verify-pristine` 只读证明热库无 staging 元数据、
目标库无 `CURRENT`、计划目录无已发布计划或摘要。任一证明失败就不换针。
成功后按持久 repin intent、旧版启动 PREPARED pin、新 latch 的顺序原子
写入；旧版二进制 pin、job ID 和原服务/timer 意图保持不变。写入中断时
保留 intent，同一组参数重复 `repin-preplan` 收敛，普通 `resume` 会拒绝
越过未完成换针。换针成功后再用原 job ID 执行上面的 `resume`；换针本身
不执行计划、复制、服务启动或 timer 恢复。

若旧计划进程中断时留下同一 job 的未发布隐藏临时计划，严格 pristine
检查会拒绝换针。仅在未发布任何正式计划、没有源或目标库迁移痕迹时，
可先运行独立的 `discard-preplan-tmp`，参数与上面的 `repin-preplan` 完全相同。
它先用新候选的 `inspect --verify-pristine-storage` 确认热库和暂存库未迁移，
再只删除该 job 名下、root 所有、单硬链接、首行标识精确匹配旧 job、旧候选、
三库路径与停机 manifest SHA 的隐藏 `*.tmp` 文件，并同步计划目录。目录中
任何正式计划、其他 job 文件或无法验证首行的临时文件都会阻止清理。随后
再次要求完整 `inspect --verify-pristine` 成功，才可执行 `repin-preplan`。
此动作不删数据库、已发布计划或其他作业文件；若中途断电，可用相同参数重试。

计划仅迁移部分 bucket 时不能通过完整验收，也不能激活 staged reader。
不要用旧版二进制或旧版发布脚本绕过 latch；正常发布和回滚必须通过
reader capability 与服务启动 guard。初次迁移使用的 candidate SHA 是
迁移身份；之后支持 staging 的新版二进制可按正常发布流程升级。

从零创建的新库可在首次运行时显式加 `--history.staging`。进程建立双库
身份、genesis SOURCE 路由和启动屏障；关库后，下一次启动会从源库持久
身份自动识别 staged reader，无需部署 marker。若 `chaindata` 已有旧版
Pebble 库，`--history.staging` 会直接拒绝，须走上面的离线迁移。
staging 启用时暂不支持 `--snapshot.bootstrap`；fresh 路径只能在精确 genesis head
发布首次路由屏障。

启动时先恢复未完成的 RESETTING，再绑定冷目录并核验完整路由和持久
冷段收据；失败会在 API、P2P 和后台维护启动前停止进程。在线 mover
独立检查源库和暂存库 Pebble 压力、共享设备压力、两目录可用空间以及
共同的重型工作门控，任一探针未知时本轮搬迁跳过。

源库与暂存库仍共享物理磁盘；独立 LSM 不增加设备带宽。迁移中的源、目标
副本与压实临时文件会同时占空间，热库逻辑删除也不等于物理空间已释放。
只有冷归档完成、语义认证及持久依赖和读租约均释放后，目标及旧冷文件才
进入有界 GC。在线 staging posting/directory 索引 GC 不再随节点注册，
相关逻辑仍保留供停机离线清理；已转冷但未离线清理的派生索引会占用空间。
新冷 trio 首次完整证明仍需实际强 SHA；成功发布持久绑定后会尽量记录
物理证书，异常退出前尚未落盘则下次重扫。物理证书命中可省重复读全文件，
不能保证秒级启动，也不能抵抗可伪造文件 inode/ctime 的本地对手。
同步速度和既有积压是否改善，须以上线后源/目标/冷库字节、
归档发布进度和设备物理 I/O 观测，当前实现不作收益承诺。

完整 reset 会隔离旧 active 冷历史，随后从新 epoch 的 SOURCE 历史重新
构建冷段；新文件放在 epoch 私有目录，旧目录和已发布目录在依赖释放前
保留，这会增加重建时间及临时空间。正常 mixed SOURCE/TARGET/COLD
流水线已有定向验证；异常状态下跨 COLD 的 posting ETL 重建会明确拒绝，
须先修复冷来源。fresh 无生产 reader marker 的首次建库窗口尚未经过独立
进程 freeze/kill 测试；现有历史 mode/window 配置锁仍生效，正式离线迁移
则由 root reader marker 固定 mode/window。设计经独立 Astra 审计，新增
实施版 Astra 复审受工具 thread limit 阻挡；实施结论以本轮代码审计与
测试证据为准。
