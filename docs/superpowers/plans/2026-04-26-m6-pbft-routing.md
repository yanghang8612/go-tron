# M6 PBFT 消息路由与验证 — Plan

**Date**: 2026-04-26  
**Spec**: `docs/superpowers/specs/2026-04-26-m6-pbft-routing-design.md`

---

## PR-1: 协议常量 + stub 分发

**目标**：把 0x34 / 0x14 接入消息路由，不做任何逻辑，测试通过即可。

- [ ] `p2p/protocol.go`: 新增 `MsgPbftMsg byte = 0x34` / `MsgPbftCommitMsg byte = 0x14`
- [ ] `net/handler.go` `handleProtocolMessage`: 新增两个 case，调用 `h.pbftHandler.HandlePbftMsg(peer, payload)` / `h.pbftDataSync.HandleCommitMsg(peer, payload)`（stub 实现，直接 return）
- [ ] `net/pbft_handler.go`: 定义 `PbftHandler` struct 和 `HandlePbftMsg` stub
- [ ] `net/pbft_data_sync.go`: 定义 `PbftDataSyncHandler` struct 和 `HandleCommitMsg` stub
- [ ] `TronHandler` 新增字段 `pbftHandler *PbftHandler` + `pbftDataSync *PbftDataSyncHandler`，在 `NewTronHandler` 中初始化

**测试**：`TestPbftMsgDispatch` — 构造 0x34 / 0x14 消息发到 handler，断言无 panic，stub 静默丢弃。

**验证**：`make test` 全绿。

---

## PR-2: 签名恢复 + SR 校验（含前一 epoch）+ 去重 + 转发 + Lifecycle

**目标**：完整实现 `PbftMsgHandler.processMessage` 等价逻辑（不含状态机）。

- [ ] `net/pbft_handler.go` 补充字段：
  ```go
  type PbftHandler struct {
      mu       sync.Mutex           // 保护 dedup map
      smMu     sync.Mutex           // 保护状态机 maps（永远先加 mu 再加 smMu）
      chain    *core.BlockChain
      db       ethdb.Database
      server   *p2p.Server
      sync     *SyncService
      dedup    map[string]time.Time // dedupKey → expiry (TTL 10 min, cap 10000)
      quit     chan struct{}
      wg       sync.WaitGroup
  }
  ```
- [ ] 实现 `Start()` / `Stop()` — 启动 dedup GC goroutine（1s tick，清过期条目），注册为 `node.Lifecycle`
- [ ] 实现 `pbftSigToAddress(rawDataBytes, sig []byte) (common.Address, error)`:
  - `hash = sha256.Sum256(rawDataBytes)`
  - `pub, _ = crypto.SigToPub(hash[:], sig)`
  - `return crypto.PubkeyToAddress(*pub)`
- [ ] 实现过期检查：
  - BLOCK: `chain.CurrentBlock().Number() - viewN > 20` → drop
  - SRL: `nextMaintenanceTime - epoch > 2 * maintenanceInterval` → drop
- [ ] 新增 `rawdb.WritePreviousShuffledWitnesses` / `ReadPreviousShuffledWitnesses`（`accessors_witness_schedule.go`）
- [ ] 更新 `consensus/dpos/maintenance.go`：在 maintenance 边界写新 SR 列表前先将当前列表存为 previous
- [ ] 实现 SR 成员检查（`isSRMember`）：先查 `ReadShuffledWitnesses`，再查 `ReadPreviousShuffledWitnesses`
- [ ] 实现去重 cache（10 min TTL，上限 10000 条，超限时不缓存新条目直到 GC 清理）
- [ ] 实现转发：原始 payload bytes（不重新序列化）`peer.Send(MsgPbftMsg, rawPayload)` 发给非来源 peer
- [ ] `allowPBFT(chain, db)` + `isSyncing` 短路检查（使用 `chain.CurrentBlock().Number()`）
- [ ] `HandlePbftMsg`: 解析 PBFTMessage proto → 调用上述步骤 → 转发 → **不进入状态机**（stub call）

**两个 key 的定义**：
```go
func dedupKey(viewN int64, dt pbpb.PBFTMessage_DataType, srAddr common.Address, msgType pbpb.PBFTMessage_MsgType) string {
    return fmt.Sprintf("%d_%v_%x_%v", viewN, dt, srAddr, msgType)
}
func dataKey(viewN int64, dt pbpb.PBFTMessage_DataType, data []byte) string {
    return fmt.Sprintf("%d_%v_%x", viewN, dt, data)
}
```

**测试**：
- `TestPbftSigRecovery` — 用已知私钥签名 rawData，验证 address 恢复正确
- `TestPbftDedupDropsDuplicate` — 同一 dedupKey 第二次调用被静默丢弃
- `TestPbftExpiredBlockDropped` — headNum - viewN = 25 → drop
- `TestPbftNonSRDropped` — SR 列表为空时 drop
- `TestPbftPrevEpochSRAccepted` — 地址仅在 previousShuffledWitnesses 中 → 接受（不 drop）

**验证**：`make test` 全绿。

---

## PR-3: 状态机 + quorum → rawdb 写入

**目标**：onPrePrepare / onPrepare / onCommit；quorum 达成时写 PBFTCommitResult。

- [ ] 新增 `rawdb.WriteLatestPbftBlockNum` / `ReadLatestPbftBlockNum`（`accessors_pbft_sign.go`，key `"LATEST_PBFT_BLOCK_NUM"`，big-endian int64）
- [ ] `net/pbft_handler.go` 补充状态机字段（受 `smMu` 保护，PR-2 中已声明）：
  ```go
  preVotes       map[string]struct{}       // key = no
  pareVoteMap    map[string]struct{}       // key = dedupKey (PREPARE)
  commitVoteMap  map[string]struct{}       // key = dedupKey (COMMIT)
  agreePare      map[string]int            // key = dataKey
  agreeCommit    map[string]int            // key = dataKey
  dataSignCache  map[string][][]byte       // key = dataKey → []sig
  pareMsgCache   []pbftCachedMsg           // 提前到达的 PREPARE (cap 10000, 2min TTL)
  commitMsgCache []pbftCachedMsg           // 提前到达的 COMMIT
  timeOuts       map[string]time.Time      // key = no → 首次 PREPREPARE 时间
  doneMsg        map[string]struct{}       // key = no → 已进入 commit 阶段
  ```
- [ ] `onPrePrepare(msg)`:
  - `isSwitch` → `remove(no)`, return
  - 重复 → return
  - `preVotes.add(no)`, `timeOuts[no] = now`
  - `checkPrepareMsgCache(no)` — 将缓存的 PREPARE 重放
- [ ] `onPrepare(msg)`:
  - no 不在 preVotes → `pareMsgCache.put`, return
  - key 在 pareVoteMap → return
  - `pareVoteMap.put(key)`
  - `checkCommitMsgCache(no)`
  - 全节点不发 COMMIT，仅计票：`agreePare[dataKey]++`
  - agreePare ≥ 19 → `agreePare.delete(dataKey)`
- [ ] `onCommit(msg)`:
  - key 不在 pareVoteMap → `commitMsgCache.put`, return
  - key 在 commitVoteMap → return
  - `commitVoteMap.put(key)`
  - `agreeCommit[dataKey]++`, `dataSignCache[dataKey].append(sig)`
  - agreeCommit ≥ 19 → `remove(no)`, `writeQuorumResult(msg)`
- [ ] `writeQuorumResult(msg)`:
  - 构造 `corepb.PBFTCommitResult{Data: rawDataBytes, Signature: sigs}`
  - BLOCK → `rawdb.WriteBlockSignData(db, viewN, result)` + `rawdb.WriteLatestPbftBlockNum(db, viewN)`
  - SRL → `rawdb.WriteSrSignData(db, epoch, result)`
- [ ] `remove(no)`: 清理 preVotes, pareVoteMap, commitVoteMap, agreePare, agreeCommit, doneMsg, timeOuts（前缀匹配，参考 java-tron）
- [ ] GC timer：在 PR-2 的 `Start()` goroutine 中同时检查 timeOuts 超时 60s 的 no → `remove`
- [ ] 从 `HandlePbftMsg` 调用状态机分发

**测试**：
- `TestPbftStateMachineQuorum` — 模拟 19 个不同 SR 发 COMMIT，验证 WriteBlockSignData 被调用
- `TestPbftPrepareBeforePrePrepare` — PREPARE 先到，缓存后等 PREPREPARE 补全，验证不丢
- `TestPbftStateCleanup` — quorum 完成后 maps 清理干净（无内存泄漏）
- `TestPbftIsSwitch` — isSwitch=true 时 remove 发生，preVotes 清空

**验证**：`make test` 全绿。

---

## PR-4: PBFT_COMMIT_MSG（0x14）数据同步接收

**目标**：`PbftDataSyncHandler` 接收预聚合提交结果，在 InsertBlock 后触发校验写入。

- [ ] `net/pbft_data_sync.go` 实现：
  ```go
  type PbftDataSyncHandler struct {
      mu          sync.Mutex
      cache       map[int64]*corepb.PBFTCommitResult  // viewN → result (10 min TTL)
      chain       *core.BlockChain
      db          ethdb.Database
  }
  ```
- [ ] `HandleCommitMsg(peer, payload)`:
  - `allowPBFT()` 检查
  - 解析 `PBFTCommitResult`
  - 解析内层 `Raw` (`Raw.parseFrom(result.Data)`)
  - `cache[raw.ViewN] = result`（10 min TTL，cap 200）
- [ ] `ProcessOnBlock(block)` (via AddBlockHook):
  - `allowPBFT()` 检查
  - 查 `cache[block.Number()]` → 若无，计算 epoch → 查 `cache[epoch]`
  - 调用 `validPbftSign(raw, sigs, witnesses)`:
    - 每个 sig → `SHA256(raw.bytes)` → `SigToPub` → `PubkeyToAddress`
    - 检查在 witnesses 列表中（去重 set）
    - 有效 sig 数 ≥ 19
  - 通过 → `rawdb.WriteBlockSignData` / `WriteSrSignData`
- [ ] 在 `BlockChain.AddBlockHook` 中注册 `pbftDataSync.ProcessOnBlock`（在 `NewTronHandler` 或 `main.go` 中完成）

**测试**：
- `TestPbftDataSyncValid` — 注入有效 PBFTCommitResult（19 个真实 sig），触发 ProcessOnBlock，验证 rawdb 写入
- `TestPbftDataSyncInvalidSig` — 一个 sig 被篡改 → 不写入
- `TestPbftDataSyncInsufficientSigs` — 只有 18 个有效 sig → 不写入

**验证**：`make test` 全绿。

---

## PR-5: 集成 + PLAN.md 更新

**目标**：全链路接通，28 包全绿，修正 PLAN.md 错误。

- [ ] `net/handler.go` `NewTronHandler` 接受 `*PbftHandler` + `*PbftDataSyncHandler` 参数（或通过 setter）
- [ ] `cmd/gtron/main.go`: 构造 `PbftHandler` + `PbftDataSyncHandler`，注册 block hook，传入 `TronHandler`；Lifecycle 已在 PR-2 中随各 handler 实现，此处只需 `node.RegisterLifecycle`
- [ ] 修正 `PLAN.md` M6 描述中的 "0x40 起" → "0x34 (PBFT_MSG) / 0x14 (PBFT_COMMIT_MSG)"
- [ ] 更新 `PLAN.md` M6 状态 + 退出条件改为全节点接收路径
- [ ] `make test` 28 包全绿
- [ ] `make lint` 无新警告

**验证**：所有单测通过；`go vet ./net/...` 无报错。

---

## 已知限制与后续工作

| 项目 | 说明 |
|---|---|
| 上一维护期 witnesses | SR 成员检查目前只用当前 epoch 的 `shuffledWitnesses`；java-tron 还会查前一个 epoch。后续在 rawdb 加 `previousShuffledWitnesses` 键存储。 |
| SR 私钥签名/发送 | M6b：实现 PREPREPARE/PREPARE/COMMIT 广播（出块时调用 `blockPrePrepare`） |
| 多节点 testnet 验证 | 连接真实 java-tron 观察窗口（G3 门）留 M6b 后完成 |
| Striped lock | 目前用单 Mutex；高并发 SR 场景可按需升级 |
