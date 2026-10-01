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
进入有界 GC。首次启动对已发布冷 trio 的实际 checksum 核验会产生一次
冷库读取开销。同步速度和既有积压是否改善，须以上线后源/目标/冷库字节、
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
