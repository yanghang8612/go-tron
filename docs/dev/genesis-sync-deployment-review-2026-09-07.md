# 从创世同步部署审查与主网空间停止保护

本文件是 2026-09-07 本地源码审查和部署方案。用户已自行移除旧库，并授权从创世同步；服务器操作由主执行者单独完成，实际安装与验收见 [现场报告](genesis-sync-live-2026-09-07.md)。本次不改 Go 运行时代码，不放开 Nile 或自动部署。

## 从创世启动结论

未发现账户 V5 或空 snapshot manifest 导致的启动阻断。

- `core/genesis.go:91–113,310–344` 的创世状态仍通过 `StateDB.Commit` 写入，内部状态根独立保存为 GenesisStateRoot。主网创世区块头不包含这个内部根，V5 编码不会因此改变主网 genesis BlockID。`core/state/statedb.go:3941–4035,4351` 的普通提交使用相同 V5 编码路径。
- `core/state/snapshots/latest_segment.go:1729` 对不存在的清单返回空视图；`cold_builder.go:2785` 在 solidified 为 0 时不要求旧历史 stage。`cmd/gtron/main.go:663` 只有显式开启 `snapshot.bootstrap` 才执行远端快照 bootstrap。空库不必携带旧 manifest，也不应开启 snapshot reset/bootstrap 来弥补不存在的问题。
- 新 snap/archive 默认保留 65,536 块热重复历史（`params/config.go:77`）。冷化目标受 solidified、Finish 和完整 StateTxRange 约束（`cold_builder.go:1142` 起），从实际第一条热历史开始构建。深度追赶阶段通常允许积累 4 倍窗口，持续繁忙时上限 8 倍窗口；字节压力可提前触发。开始同步后短时间没有冷文件是正常现象，不能仅据此判断冷化停滞。
- 有效冷覆盖发布后先裁剪对应热历史再做较重合并（`core/state/pruning/lifecycle.go:239` 附近）；没有冷覆盖不能凭空间压力删除权威历史。保留全部历史查询依赖新二进制、热状态和冷文件共同完整。

已有定向测试覆盖 MainnetGenesis HashByteEqual、创世 rooted dynamic properties 确定性、已有库 SetupGenesis 不修改最新状态，以及 V5 热/冷历史、CodeHash 和重组恢复精确内部根/编码。独立审阅未发现具体反例；这不等于已经完成全主网交易和 Sapling 的历史重放。V5 内部根与旧 V4 不同，产生新数据后不能回滚到不认识 V5 的旧二进制。

## 保留其他服务维护锁，仅允许主网

现有全局锁 `/var/lib/gtron-offline-maintenance.hold` **保留**。`gtron-nile.service`、`gtron-deploy.service`、`gtron-deploy.timer` 的维护 drop-in **原样保留**。只备份并编辑 `gtron.service.d/99-offline-disk-20260907.conf` 中已核实的这一行：

```ini
# 旧行，仅在主网文件精确替换一次
ConditionPathExists=!/var/lib/gtron-offline-maintenance.hold
# 新行
ConditionPathExists=!/var/lib/gtron-mainnet-disk-stop.hold
```

替换前必须读取实际 `systemctl cat gtron.service` 及四个单元的 drop-in；旧行必须唯一匹配，否则停止配置步骤检查差异。备份放到 drop-in 目录之外，保留其他 Conditions 和 Environment。**禁止用空 `ConditionPathExists=` 清空条件**；多个普通条件按 AND 组合，空赋值会清空之前所有种类的条件。Condition 只在启动时检查，写锁本身不会停止已运行的服务。[systemd v219 原始手册](https://raw.githubusercontent.com/systemd/systemd/v219/man/systemd.unit.xml)

新二进制应来自已推送的确定 commit，隔离源码目录构建并验证 nativeSapling；旧服务器 Git 不支持 `-C` 时使用明确 `cd`。最终 ExecStart 使用确定的新 release 路径，保持主网 datadir、18890/8090/8545/50051/6062/6071 端口和服务用户不变。显式给出 `--prune.mode snap --history.compression-format auto`；旧 drop-in 可能仍有 `GTRON_HISTORY_COMPRESSION_FORMAT=2`，CLI 显式值可以避免旧环境悄悄覆盖 auto 默认。不要改动 Nile 或自动部署的 ExecStart。检查真实监听后才调整任何端口。

## 独立空间保护

脚本：`scripts/dev/mainnet_space_guard.py`。只有 `check` 和 `guard` 两个入口：

- `check` 只读文件系统容量，适合以服务用户在 ExecStartPre 执行。容量达到或低于启动保留线、inode 达到保留线、主网专用锁存在、设备号不符或检查失败，均拒绝启动。
- `guard` 必须 root，健康时不写文件。触线或检查失败时创建并 fsync **主网专用锁**，然后只执行 `/bin/systemctl stop gtron.service`。锁写失败仍尝试停止。不会删除锁、启动或重启服务、打开数据库，也不会操作其他三个单元。现有 `Restart=on-failure` 不会将管理操作发起的正常 stop 当成自动重启请求。[systemd v219 service 手册](https://raw.githubusercontent.com/systemd/systemd/v219/man/systemd.service.xml)

配置固定为 `/etc/gtron/mainnet-space-guard.json`，必须 root 所有、普通文件、不可被组或其他用户修改；用 0644 使 java-tron 用户可以只读检查。脚本用 root:root 0755 安装到 `/usr/local/libexec/gtron-mainnet-space-guard.py`，不能让运行服务用户修改。部署时从实际 `os.stat('/data').st_dev` 取得整数设备号，填写如下配置，不能照抄占位符：

```json
{
  "data_device": "替换成实际整数，不是字符串",
  "stop_bytes": 412316860416,
  "start_bytes": 549755813888,
  "min_free_inodes": 100000
}
```

停机保留线 384 GiB、启动线 512 GiB 已由本轮主执行者确认，触线比较均为 `<=`。data inode 实际保留线取配置值与文件系统总 inode 的 5%（向上取整）两者较大值；部署时也可把配置 `min_free_inodes` 写成实测总数的 5%，不能把示例 100,000 当成唯一门槛。**根文件系统另有固定 5 GiB 可用容量和 5% 总 inode 保留线**，check 和 guard 均检查；根卷触线同样只停止主网。

检查使用 `f_bavail`，不把文件系统保留给 root 的块当作 gtron 可用空间。`/data`、主网 datadir 和已存在的 `datadir/gtron` 必须与配置设备号相同；重新挂载导致设备号变化时保守拒绝，核验实际挂载后才能更新配置。

新增 `/etc/systemd/system/gtron-mainnet-space-guard.service`：

```ini
[Unit]
Description=gtron mainnet free-space stop guard

[Service]
Type=oneshot
User=root
Group=root
ExecStart=/usr/bin/python3 /usr/local/libexec/gtron-mainnet-space-guard.py guard
TimeoutStartSec=660
```

新增 `/etc/systemd/system/gtron-mainnet-space-guard.timer`：

```ini
[Unit]
Description=Check gtron mainnet free space every 15 seconds

[Timer]
OnBootSec=15s
OnUnitInactiveSec=15s
AccuracySec=1s
Unit=gtron-mainnet-space-guard.service

[Install]
WantedBy=timers.target
```

上述单调时钟计时器选项在 v219 已存在；一次检查结束后 15 秒再次执行，避免停止等待期间堆叠多个检查。[systemd v219 timer 手册](https://raw.githubusercontent.com/systemd/systemd/v219/man/systemd.timer.xml)

追加主网专用 drop-in，例如 `gtron.service.d/98-mainnet-space-guard.conf`，不要清空已有 ExecStartPre：

```ini
[Unit]
Requires=gtron-mainnet-space-guard.timer
After=gtron-mainnet-space-guard.timer

[Service]
ExecStartPre=/usr/bin/python3 /usr/local/libexec/gtron-mainnet-space-guard.py check
```

先确认本机 Python 3 与 `/bin/systemctl` 路径、完成上述配置和全局锁保全核验，再 daemon-reload、启用并启动**新 guard timer**。以 `java-tron` 执行一次只读 check，查看新 timer/service 状态后才由主执行者显式启动 `gtron.service`。这里没有自动启动主网的安装脚本。

运行期 history pressure 是冷化调度和写入准入，**不是节点导入的停机保护**。384 GiB 是运维保留量，不是源码证明的磁盘峰值上界。所需保留量至少覆盖一个正在执行的冷构建/合并额外峰值，以及整个卷在检测间隔与正常关机期间的增长；活动构建未必立即响应取消。保留现有 SIGTERM 和 600 秒 TimeoutStopSec，脚本等 stop 最多 650 秒；超时会明确报错，主网锁仍保留。它不能保证其他服务在极端写入突发下不耗尽整个卷，部署后仍应观察实际峰值和停止耗时再调整保留线。

恢复时先确认 data free 超过 512 GiB、data inode 超过保留线、根卷超过 5 GiB 和 5% inode，且故障已解决，再由操作员只移除 `/var/lib/gtron-mainnet-disk-stop.hold`、执行 check，并显式启动主网；脚本不自动解除锁。回滚部署配置时先停止主网，恢复它原来的全局 hold 条件，再移除本轮新增主网 guard drop-in 和 timer/service；不要移除全局 hold。仅恢复 Condition 不会终止已运行的进程。

## 本地验证

`python3 scripts/dev/mainnet_space_guard_test.py` 通过 14 个测试：启动/运行阈值分离、容量和 inode 触线及精确等值边界、data 5% inode 下限、根卷双门槛在两个模式都生效、锁不自动清除、错设备拒绝、check 零写入/零 stop、健康 guard 零修改、检查失败与锁写失败仍只停止主网、锁持久且不覆盖已有锁、符号链接不跟随、非 root 无权运行 guard。子进程完全 mock；本地测试没有调用 systemctl 或启动任何节点。本次未在服务器执行脚本，实际 systemd 配置需主执行者验收。
