# M6b SR-side PBFT signing — 设计规范

**Date**: 2026-04-30
**Plan**: `docs/superpowers/plans/2026-04-30-m6b-sr-signing.md`
**Predecessor**: `docs/superpowers/specs/2026-04-26-m6-pbft-routing-design.md` (full-node receive path)

---

## 1. 背景

M6 已经把 PBFT 的**接收路径**全部跑通：`net/pbft_handler.go` 解码 `PBFT_MSG`/`PBFT_COMMIT_MSG`、SHA-256 签名恢复、SR 成员校验、三阶段状态机、quorum 后写 `pbft-signdata`。

仍然缺失的是 **SR 的发送侧**——当 go-tron 自己作为 SR 节点运行时：本地 InsertBlock 后构造 PREPREPARE，签名后广播；收到对应 PREPREPARE 后再生 PREPARE；PREPARE quorum 后生 COMMIT；COMMIT quorum 后写 `pbft-signdata`（与接收侧共用）。

PLAN.md M6 行原话：`SR 签名发送（M6b）留待后续`。

直接把整套发送侧塞进单个 agent session 会得到半成品。本 spec 把 M6b 切成两片：

- **Slice 1（本次）**：脚手架——key 接入 / 三种消息构造函数 / 注册一个 NO-OP block hook。
- **Slice 2（延后）**：实际三阶段状态机 + 转发 + COMMIT 聚合。

---

## 2. java-tron 发送侧全景（slice 2 规划用）

源码：`consensus/src/main/java/org/tron/consensus/pbft/`。

### 2.1 触发点

| 阶段 | java-tron 入口 | 触发时机 |
|---|---|---|
| BLOCK PREPREPARE | `PbftManager.blockPrePrepare(block, epoch)` | `Manager.pushBlock` / `ConsensusImpl.receiveBlock` 出块或接块成功后 |
| SRL PREPREPARE  | `PbftManager.srPrePrepare(block, currentWitness, epoch)` | `MaintenanceManager.applyBlock` 维护期切换时 |
| PREPARE | `PbftMessageHandle.onPrePrepare` 内部 | 收到 PREPREPARE 且未投过本 slot 时为每个 local SR miner 派生一条 PREPARE |
| COMMIT  | `PbftMessageHandle.onPrepare` 内部 | PREPARE 累积 ≥ `agreeNodeCount`（19）时为每个 local SR miner 派生一条 COMMIT |
| 落库 | `PbftMessageAction.action` | COMMIT 累积 ≥ 19 时收集 ≥19 签名写 `pbft-signdata` |

### 2.2 状态机入口（`PbftMessageHandle.java`）

```
onPrePrepare(msg):
  if isSwitch: remove(no); return                   // 仅 java 侧 in-memory 标志，不上线
  if preVotes.contains(no): return
  preVotes.add(no); timeOuts[no] = now
  checkPrepareMsgCache(no)
  if !checkIsCanSendMsg(epoch): return              // 非 SR 或 syncing 时不发
  for miner in localSrMinerList(epoch):
    paMessage = msg.buildPrePareMessage(miner)
    forwardMessage(paMessage)                        // 广播给所有连接的对端
    onPrepare(paMessage)                             // 自己本地也走一遍接收路径

onPrepare(msg):
  ...同 M6 接收侧...
  if !doneMsg[no] && agreePare[dataKey] >= 19:
    for miner in localSrMinerList(epoch):
      cmMessage = msg.buildCommitMessage(miner)
      doneMsg[no] = cmMessage
      forwardMessage(cmMessage)
      onCommit(cmMessage)

onCommit(msg):
  ...同 M6 接收侧...
  if agreeCommit[dataKey] >= 19:
    pbftMessageAction.action(msg, dataSignCache[dataKey])    // 持久化
```

`forwardMessage` 走 `Param.getInstance().getPbftInterface().forwardMessage(message)`（`PbftBaseImpl`），最终投到所有 `PeerConnection.fastSend(msg)`。

> **重要**：java-tron 在接收路径里**已经**对每个收到的 PREPREPARE 派生 PREPARE，对每个收到的 PREPARE 派生 COMMIT。也就是说，发送侧的逻辑是**嵌入接收路径**的，并不是一个独立的 producer-thread。M6 的 `PbftHandler.onPrePrepare/onPrepare` 已经持有所有需要的字段和锁，slice 2 只要在那两个回调里增加：(a) 派生子消息、(b) 转发、(c) 自调一次接收路径即可。

### 2.3 SR miner 列表

`PbftMessageHandle.getSrMinerList(epoch)`：

```
compareList = epoch > beforeMaintenanceTime ? currentWitness : beforeWitness
return Param.getMiners().stream()
  .filter(miner -> compareList.contains(miner.getWitnessAddress()))
  .collect(toList())
```

go-tron 等价：`rawdb.ReadShuffledWitnesses(db) ∪ rawdb.ReadPreviousShuffledWitnesses(db)` 与 local SR key 列表的交集。当前 go-tron 只支持单个 `witnessKey`（`cmd/gtron/main.go`），所以 slice 2 的 `localSrMiners` 退化成「这个 key 的地址在不在 SR 集合里」。

---

## 3. 协议消息字节布局（slice 1 实现）

参考：`consensus/src/main/java/org/tron/consensus/pbft/message/PbftMessage.java`。

### 3.1 PREPREPARE for BLOCK

```
Raw {
  msg_type  = PREPREPARE        (= 2)
  data_type = BLOCK             (= 0)
  view_n    = block.Number()
  epoch     = epoch_arg         // java 侧由 ConsensusImpl 计算并传入；slice 1 暂用 0
  data      = block.ID().Hash[:32]   // 32-byte: 前 8 字节大端写入 num
}
hash      = SHA-256(Raw.Marshal())
signature = secp256k1.Sign(hash, srPrivKey)  // 65-byte (r|s|v)
```

> **块号布局核对**：java-tron `BlockCapsule.BlockId.byteString()` = 32 字节，前 8 字节是 `Longs.toByteArray(num)`（大端）。go-tron `types.Block.ID()` 调 `binary.BigEndian.PutUint64(h[:8], num)`。一致。

### 3.2 PREPREPARE for SRL

```
data = SRL { srAddress: [...currentWitnessByteStrings] }.Marshal()
view_n = epoch
```

> Slice 2 才需要。Slice 1 暂不实现 SRL builder——SRL pre_prepare 由维护期切换驱动，与本 slice 的 block hook 触发点不同。

### 3.3 PREPARE / COMMIT（来自 PREPREPARE 派生）

`PbftMessage.buildMessageCapsule`：

```
new Raw {
  msg_type  = PREPARE | COMMIT
  data_type = inherited
  view_n    = inherited
  epoch     = inherited
  data      = inherited
}
hash      = SHA-256(new Raw.Marshal())
signature = secp256k1.Sign(hash, srPrivKey)
```

即只换 `msg_type`，其它字段从父 PREPREPARE 拷贝。**不接受 `*types.Block` 作为输入**，签名是基于父消息的。task-spec 里写的 `BuildPrepareMsg(block)` 不忠实于 java-tron，本 spec 用 `BuildPrepareMsg(parentRaw, key)` 替代。

### 3.4 `isSwitch`

`PbftMessage.setSwitch(block.isSwitch())` 仅写入 java 侧 in-memory 字段，**不在 proto 里**，不进入序列化。go-tron 不实现。`net/pbft_handler.go:307` 已注明 "isSwitch not in proto"。

---

## 4. Slice 1 实现范围

### 4.1 New file: `net/pbft_producer.go`

PBFT 消息构造逻辑是纯 proto + crypto，与 DPoS 无耦合，与 `pbft_handler.go` 同包共享类型 / 测试 helper。结构体雏形：

```go
package net

type PbftProducer struct {
    chain       *core.BlockChain
    db          ethdb.KeyValueStore
    server      *p2p.Server     // 预留给 slice 2 的 forward
    sync        *SyncService    // syncing 时不发
    srKey       *ecdsa.PrivateKey
    srAddr      common.Address
}

func NewPbftProducer(chain, db, server, sync, key) *PbftProducer

// Slice 1：纯构造，无副作用
func (p *PbftProducer) BuildBlockPrePrepareMsg(block *types.Block, epoch int64) ([]byte, error)
func (p *PbftProducer) BuildPrepareMsg(parentRaw *corepb.PBFTMessage_Raw) ([]byte, error)
func (p *PbftProducer) BuildCommitMsg(parentRaw *corepb.PBFTMessage_Raw) ([]byte, error)

// Slice 1：NO-OP hook
func (p *PbftProducer) OnBlockApplied(block *types.Block) {
    // gate 与 receive 路径一致
    if !p.allowPBFT() { return }
    if p.sync != nil && p.sync.IsSyncing() { return }
    if !p.isLocalSR() { return }
    // slice 2: build, sign, broadcast, self-onPrePrepare
    log.Printf("pbft-producer (slice 1 no-op) block=%d witness=%x", block.Number(), p.srAddr[:6])
}
```

底层共享一个内部 helper：

```go
func signPbftRaw(raw *corepb.PBFTMessage_Raw, key *ecdsa.PrivateKey) ([]byte, error) {
    rawBytes, err := proto.Marshal(raw)
    if err != nil { return nil, err }
    h := sha256.Sum256(rawBytes)
    sig, err := crypto.Sign(h[:], key)
    if err != nil { return nil, err }
    msg := &corepb.PBFTMessage{RawData: raw, Signature: sig}
    return proto.Marshal(msg)
}
```

### 4.2 `cmd/gtron/main.go` 接线

只在 `--witness` 模式下启用：

```go
if ctx.Bool("witness") {
    ...
    pbftProducer := tnet.NewPbftProducer(bc, db, p2pServer, syncService, key)
    bc.AddBlockHook(pbftProducer.OnBlockApplied)
    ...
}
```

> 与 `producer` 同样的 active-list 警告策略：即使 `witnessAddr` 不在 active list 也注册 hook，运行期 `isLocalSR()` 会自然过滤。

### 4.3 不修改的文件

- `core/blockchain.go`：用现成的 `AddBlockHook`，不改 hook 注册 API。
- `net/pbft_handler.go` / `net/pbft_data_sync.go`：M6 接收侧不动。测试要复用接收侧解析逻辑时，直接调用 `proto.Unmarshal` + `pbftSigToAddress`（已 export 给同包），不需要修改。
- `actuator/`、`core/state/dynamic_properties.go`：M11.5 / switchFork 修补区域，绝不触碰。

---

## 5. Slice 1 测试

### 5.1 Round-trip（同包，复用接收侧 helper）

`net/pbft_producer_test.go`：

- `TestBuildBlockPrePrepareMsg_RoundTrip`
  - 生成 ECDSA key，构造 `block` 模拟（`viewN=42`、`block.ID().Hash`）。
  - 调 `BuildBlockPrePrepareMsg(block, epoch=100)`。
  - `proto.Unmarshal` → 检查 `MsgType=PREPREPARE`、`DataType=BLOCK`、`ViewN=42`、`Epoch=100`、`Data == block.ID().Hash[:]`。
  - 调用 `pbftSigToAddress(rawBytes, msg.Signature)` → 应等于 `crypto.PubkeyToAddress(&key.PublicKey)`。
- `TestBuildPrepareMsg_DerivesFromParent`
  - 先构造 PREPREPARE，解析得 parent raw。
  - `BuildPrepareMsg(parent)` → 解析后 `MsgType=PREPARE`、其它字段与 parent 完全一致、签名能恢复出 SR 地址。
- `TestBuildCommitMsg_DerivesFromParent`：同上，`MsgType=COMMIT`。
- `TestBuildPrepareMsg_DifferentSignatureFromPrePrepare`：仅一个 sanity check，PREPARE 的 sig != PREPREPARE 的 sig（`msg_type` 字段不同 → hash 不同 → sig 不同）。

### 5.2 Hook 注册 / NO-OP 验证

`net/pbft_producer_test.go`：

- `TestOnBlockApplied_NoOp_DoesNotPanic`：构造 `PbftProducer`（mock chain）、构造一个 block，调用 `OnBlockApplied(block)`，仅断言没有 panic、未触发任何 DB 写、没有调用 server.Send。
- 注册逻辑测试在 `cmd/gtron` 包下不易做（main 函数不便单测）；`AddBlockHook` 本身在 M5.2 已有覆盖。本 slice 通过**编译保证 + 手动 review** main.go 来确认 hook 仅在 `--witness` 分支挂上，不增加额外测试。

### 5.3 不在范围

- 多节点广播 / quorum 端到端：slice 2 / 系统测试。
- 与活跃 java-tron 节点的字节级互通校验：需要 `make test` 之外的 integration 跑动，本 slice 在文末 §7 标记为 "pending live cross-impl verification"。

---

## 6. Slice 2（延后）范围速览

不在 slice 1 实现，本 spec 一并记录以便后续接续：

1. `OnBlockApplied` 不再 no-op：调 `BuildBlockPrePrepareMsg`、广播 `MsgPbftMsg`、立即自调一次 `PbftHandler.onPrePrepare`（或重构为 `PbftCoordinator` 把发送/接收侧揉到一处）。
2. 在 `PbftHandler.onPrePrepare` 末尾：若本 SR 未发过 PREPARE，则 `BuildPrepareMsg` + 广播 + 自调 `onPrepare`。
3. 在 `PbftHandler.onPrepare` 的 quorum 分支：若本 SR 未发过 COMMIT，则 `BuildCommitMsg` + 广播 + 自调 `onCommit`。
4. 维护期切换：在 maintenance 完成处增加 `BuildSrlPrePrepareMsg(currentWitnesses, epoch)` + 广播。
5. 与 java-tron 互通：起一个本地 java-tron + go-tron 互联系统测试，抓取真实 PBFT_MSG payload 做字节比较，把结果写入 `docs/dev/p2p-interop-status.md`。
6. SR 多 key（local miners）支持：当前 `cmd/gtron/main.go` 只支持单 `--witness.key`。若需要给一个进程挂多个 SR key，需要扩 flag/config schema。

---

## 7. 假设与遗留风险

- **字节级一致性来自源码阅读**：本 spec 的字节布局来自直接读 `PbftMessage.java`，**未做活节点字节比对**。在 slice 2 完成 + 跑 `JAVA_TRON_ADDR` 集成测试之前，**不主张 byte-for-byte parity**——任何不一致都会导致 java-tron 节点拒绝 go-tron SR 的签名。
- **Epoch 传参**：`PbftManager.blockPrePrepare(block, epoch)` 的 `epoch` 由 `ConsensusImpl.receiveBlock` 计算，等于 `chainBaseManager.getDynamicPropertiesStore().getMaintenanceTimeInterval()` 对齐的当前维护期开始时间戳。slice 1 的 `BuildBlockPrePrepareMsg` 把 `epoch` 当显式参数；slice 2 才需要正式从 DP 计算。
- **`isSwitch` 缺失**：java-tron 在 `BlockCapsule.isSwitch=true`（switch fork 时）触发 `PbftMessageHandle.onPrePrepare` 走 `remove(key)` 重新 propose 路径。go-tron 的 fork 切换（M5.3 KhaosDB）现在直接 rewind+replay，hook 只在最终上链的 block 触发，**不会**重复触发不同 BlockId 的 PREPREPARE，所以这个 in-memory 标志可省。slice 2 时如果发现互通问题再补。
- **签名恢复成本**：每条 PBFT 消息接收侧都要做 secp256k1 recover；slice 1 不增加新的接收路径调用。

---

## 8. 验证清单

- [ ] `make test` 全绿。
- [ ] `make lint` 全绿。
- [ ] Round-trip 测试覆盖三种 builder + 签名地址恢复。
- [ ] Hook 仅在 `--witness` 分支挂载（手动 review main.go）。
- [ ] `core/blockchain.go`、`net/pbft_handler.go`、`net/pbft_data_sync.go` 三个文件 diff = 0。
- [ ] 假设 §7 的 "pending live cross-impl verification" 在 slice 2 提交前不被宣称为已完成。
