# M6 PBFT 消息路由与验证 — 设计规范

**Date**: 2026-04-26  
**Plan**: `docs/superpowers/plans/2026-04-26-m6-pbft-routing.md`

---

## 1. 背景与范围

java-tron 在 AllowPBFT（提案 #40）激活后，在正常块同步协议之外运行一套三阶段 PBFT 协议（PREPREPARE → PREPARE → COMMIT），对每个块和每个维护期的 SR 列表进行拜占庭容错确认。确认完成后将 `PBFTCommitResult`（原始数据 + ≥2f+1 签名）持久化到 `pbft-signdata` DB 前缀（go-tron M2 PR-2 已建立），供轻客户端及 PBFT API 使用。

**M6 范围**：全节点模式（不含 SR 出块/签名发送）。实现：

1. 接收、校验、去重、转发 `PBFT_MSG`（类型码 0x34）。
2. 运行本地状态机收集足够票数后将 `PBFTCommitResult` 写入 rawdb。
3. 接收并校验 `PBFT_COMMIT_MSG`（类型码 0x14，预聚合的提交结果），同样写入 rawdb。

**不在范围**：SR 私钥签名/广播 PREPARE/COMMIT（待 M6b 或 M7）；VIEW_CHANGE 和 REQUEST 消息处理（java-tron 自身也是空实现，全节点不需要）。

---

## 2. PLAN.md 勘误

> **PLAN.md M6 第 1 条写 "0x40 起"，是错的。**

java-tron `MessageTypes.java` 实际定义：

| 常量 | 值 |
|---|---|
| `PBFT_COMMIT_MSG` | `0x14` |
| `PBFT_MSG` | `0x34` |

实现时须使用 0x14 / 0x34，与 PLAN.md 的 "0x40 起" 不一致，以本规范为准。

---

## 3. 协议消息格式

### 3.1 PBFTMessage（类型 0x34）

```
PBFTMessage {
  Raw raw_data = 1 {
    MsgType  msg_type  = 1   // VIEW_CHANGE=0 REQUEST=1 PREPREPARE=2 PREPARE=3 COMMIT=4
    DataType data_type = 2   // BLOCK=0 SRL=1
    int64    view_n    = 3   // BLOCK → 块号；SRL → epoch
    int64    epoch     = 4   // 所属维护 epoch 时间戳
    bytes    data      = 5   // BLOCK → BlockID bytes；SRL → SRL proto bytes
  }
  bytes signature = 2       // secp256k1 ECDSA, 65 bytes (r|s|v)
}
```

### 3.2 PBFTCommitResult（类型 0x14）

```
PBFTCommitResult {
  bytes data = 1             // Raw.ToByteArray()
  repeated bytes signature = 2
}
```

---

## 4. 关键常量

| 参数 | 值 | 来源 |
|---|---|---|
| agreeNodeCount | `27 * 2/3 + 1 = 19` | `Args.java:806` |
| 去重 cache TTL | 10 分钟 | `PbftMsgHandler.java:27` |
| 去重 cache 最大条目 | 10000 | `PbftMsgHandler.java:27` |
| BLOCK 消息过期阈值 | headBlockNum - viewN > 20 | `Args.java:802 default` |
| SRL 消息过期阈值 | currentEpoch - msgEpoch > 2 × maintenanceInterval | `PbftMsgHandler.java:49` |
| prepare/commit cache TTL | 2 分钟 | `PbftMessageHandle.java:47,52` |
| prepare/commit cache 最大条目 | 10000 | `PbftMessageHandle.java:47,52` |
| PBFT 状态机超时 | 60000 ms | `PbftMessageHandle.java:40` |

---

## 5. 签名校验算法

java-tron 签名格式及校验流程（`PbftBaseMessage.analyzeSignature()`）：

```
hash = SHA256(rawData.ToByteArray())   // 单次 SHA-256（isSha256=true）
pubKey = secp256k1.RecoverPub(hash, signature)
srAddress = TRON_address_from_pubkey(pubKey)  // Keccak256(pubKey[1:])[12:], prefix 0x41
```

go-tron 等价实现（使用既有 crypto 包）：

```go
import (
    "crypto/sha256"
    "github.com/tronprotocol/go-tron/crypto"
)

func pbftSigToAddress(rawDataBytes []byte, sig []byte) (common.Address, error) {
    hash := sha256.Sum256(rawDataBytes)
    pub, err := crypto.SigToPub(hash[:], sig)
    if err != nil {
        return common.Address{}, err
    }
    return crypto.PubkeyToAddress(*pub), nil
}
```

**注意**：
- `Sha256Hash.hash(true, ...)` = 单次 SHA-256（`isSha256=true` 意为使用 SHA-256 而非 SM3，并非双重哈希）。
- 签名 65 字节，go-ethereum 格式 `r(32)|s(32)|v(1)`。

---

## 6. 两个不同的 key

混淆两个 key 会导致同一 SR 重复计票或不同数据分叉被合并：

| key | 计算 | 用途 |
|---|---|---|
| **dedupKey** | `fmt.Sprintf("%d_%v_%x_%v", viewN, dataType, srAddress, msgType)` | 去重 cache；一个 (SR, viewN, msgType) 只处理一次 |
| **dataKey** | `fmt.Sprintf("%d_%v_%x", viewN, dataType, rawData.Data)` | agreePare / agreeCommit 计数；不同 data 不互相影响 |

java-tron 参考：
- `getKey() = getNo() + "_" + Hex.toHexString(publicKey)` → 去重
- `getDataKey() = getNo() + "_" + Hex.toHexString(data)` → 计票

---

## 7. 状态机（全节点）

全节点不发送 PREPARE / COMMIT，仅接收并记录。当 agreeCommit ≥ 19 时写入 rawdb。

```
收到 PBFT_MSG:
  1. allowPBFT() && !isSyncing() 检查
  2. 过期检查（BLOCK: headNum - viewN > 20；SRL: epoch stale）
  3. 解析 signature → srAddress
  4. SR 成员检查（当前或上一个维护期）
  5. 去重检查（dedupKey）
  6. 去重 cache 写入
  7. 转发给其他 peer（除来源 peer）
  8. 按 msgType 分发:
     PREPREPARE → onPrePrepare
     PREPARE    → onPrepare
     COMMIT     → onCommit

onPrePrepare(msg):
  key = msg.no = fmt.Sprintf("%d_%v", viewN, dataType)
  if msg.isSwitch → remove(key); return   // 链切换重置
  if preVotes.has(key) → return           // 重复
  preVotes.add(key)
  checkPrepareMsgCache(key)               // 提前到达的 PREPARE

onPrepare(msg):
  if !preVotes.has(msg.no) → pareMsgCache.put; return
  if pareVoteMap.has(msg.key) → return
  pareVoteMap.put(msg.key, msg)
  checkCommitMsgCache(msg.no)
  // 全节点不发 COMMIT，但仍计票
  agCou = agreePare.incr(msg.dataKey)
  if agCou >= 19 → agreePare.remove(msg.dataKey)  // 准备好接收 commit

onCommit(msg):
  if !pareVoteMap.has(msg.key) → commitMsgCache.put; return
  if commitVoteMap.has(msg.key) → return
  commitVoteMap.put(msg.key, msg)
  agCou = agreeCommit.incr(msg.dataKey)
  dataSignCache[msg.dataKey].append(msg.signature)
  if agCou >= 19:
    remove(msg.no)
    writeQuorumResult(msg, dataSignCache[msg.dataKey])

writeQuorumResult(msg, sigs):
  raw = msg.rawData
  commitResult = PBFTCommitResult{Data: raw.bytes, Signature: sigs}
  if dataType == BLOCK:
    rawdb.WriteBlockSignData(db, viewN, &commitResult)
    dp.SetLatestPbftBlockNum(viewN)
  elif dataType == SRL:
    rawdb.WriteSrSignData(db, epoch, &commitResult)
```

**isSwitch 说明**：`isSwitch` 字段由块生产者在链切换（孤块回退）时设置为 true；全节点收到时只需要清理 preVotes/pareVoteMap/commitVoteMap/计数器，不转发，以避免在切换的块上积累无效票。

---

## 8. PBFT_COMMIT_MSG（类型 0x14）处理

`PbftDataSyncHandler` 负责接收已完成 PBFT 流程的预聚合提交结果，用于加速追上的节点。

```
收到 PBFT_COMMIT_MSG:
  1. allowPBFT() 检查
  2. 解析 PBFTCommitResult
  3. 解析内层 Raw（raw = Raw.parseFrom(commitResult.Data)）
  4. 按 raw.viewN 存入 commitCache（10 分钟 TTL，200 条上限）

处理时机（每次 InsertBlock 成功后，via AddBlockHook）:
  processPBFTCommitData(block):
    1. allowPBFT() 检查
    2. 查 commitCache[block.Number()] → 若无，计算 epoch，查 commitCache[epoch]
    3. 验证签名列表 ≥ 19 个，每个签名 ecrecover 后检查 SR 成员
    4. 若通过：WriteBlockSignData / WriteSrSignData
```

复用 M5.2 的 `BlockChain.AddBlockHook`；不新建通知路径。

---

## 9. AllowPBFT 门控

两处检查，与 java-tron 保持一致：

```go
func allowPBFT(chain *core.BlockChain, db ethdb.Database) bool {
    headBlockNum := chain.CurrentBlock().Number()
    dp := state.LoadDynamicProperties(db)
    return forks.IsActive(forks.AllowPbft, headBlockNum, dp)
}

func isSyncing(ss *net.SyncService) bool {
    return ss.IsSyncing()
}
```

`forks.AllowPbft`（index 40）已在 `core/forks/forks.go` 定义。

---

## 10. 并发模型

java-tron 使用 1024 条 Striped lock 支持并发。go-tron 简化为两把 `sync.Mutex`：

- `mu`：保护 dedup cache（`dedup` map）。
- `smMu`：保护状态机 maps（preVotes / pareVoteMap / commitVoteMap / agreePare / agreeCommit / doneMsg / timeOuts / dataSignCache / 各 cache）。

**锁顺序**：任何时候需要同时持有两把锁，必须先加 `mu`，再加 `smMu`，绝不反向，以防死锁。实践上两把锁从不嵌套，各自覆盖不同的数据集合，因此正常路径不会同时加锁。

全节点仅接收不发送，消息峰值远低于 SR 节点，单锁即可。未来升级为 SR 节点时可按需引入更细粒度锁。

---

## 11. latestPbftBlockNum 存储

java-tron 将 `latestPbftBlockNum` 存储于 `commonDataBase`（独立 DB column）：

- **key**: `"LATEST_PBFT_BLOCK_NUM"` 的 UTF-8 bytes（原始字符串 key，非哈希前缀）
- **value**: `ByteArray.fromLong(blockNum)` = big-endian int64（8 bytes）
- **语义**：只增不减（有新块时才更新）

go-tron 等价实现放入 `core/rawdb/` 包：

```go
// WriteLatestPbftBlockNum 将最新 PBFT 确认块号写入 db（key: "LATEST_PBFT_BLOCK_NUM"）。
// 只增不减：若 num <= 已存储值则跳过，与 java-tron commonDataBase 语义一致。
func WriteLatestPbftBlockNum(db ethdb.KeyValueReadWriter, num int64) {
    if cur := ReadLatestPbftBlockNum(db); num <= cur {
        return
    }
    val := make([]byte, 8)
    binary.BigEndian.PutUint64(val, uint64(num))
    if err := db.Put([]byte("LATEST_PBFT_BLOCK_NUM"), val); err != nil {
        log.Crit("WriteLatestPbftBlockNum failed", "err", err)
    }
}

// ReadLatestPbftBlockNum 读取最新 PBFT 确认块号；若无记录返回 -1。
func ReadLatestPbftBlockNum(db ethdb.KeyValueReader) int64 {
    val, err := db.Get([]byte("LATEST_PBFT_BLOCK_NUM"))
    if err != nil || len(val) != 8 {
        return -1
    }
    return int64(binary.BigEndian.Uint64(val))
}
```

在 `writeQuorumResult` 中调用 `rawdb.WriteLatestPbftBlockNum`，不使用 `dp.SetLatestPbftBlockNum`（该方法不存在）。

---

## 12. 上一维护期 SR 成员检查

`verifyMsg` 须同时检查当前 epoch 和上一 epoch 的 SR 列表，与 java-tron `verifyMsg()` 逻辑对齐：

```go
// SR 成员检查（当前 epoch + 上一 epoch）
func (h *PbftHandler) isSRMember(addr common.Address) bool {
    current := rawdb.ReadShuffledWitnesses(h.db)
    for _, w := range current {
        if w == addr {
            return true
        }
    }
    prev := rawdb.ReadPreviousShuffledWitnesses(h.db)
    for _, w := range prev {
        if w == addr {
            return true
        }
    }
    return false
}
```

`ReadPreviousShuffledWitnesses` / `WritePreviousShuffledWitnesses` 是 PR-2 中新增的 rawdb 访问器，key 前缀与 `shuffledWitnessPrefix` 相邻（使用新的 `previousShuffledWitnessPrefix`）。在维护期边界 `maintenance.go` 写入新 SR 列表时，先将当前列表保存为 previous，再写入新列表。

---

## 13. 转发原始字节

转发 PBFT 消息时，必须使用从 peer 收到的原始 payload bytes，而非重新序列化 proto 对象：

```go
// 正确：保存并转发原始字节
func (h *PbftHandler) HandlePbftMsg(peer *p2p.Peer, payload []byte) {
    rawPayload := payload  // 保留原始字节，不 re-marshal
    ...
    for _, p := range h.server.Peers() {
        if p != peer {
            p.Send(MsgPbftMsg, rawPayload)
        }
    }
}
```

原因：proto 序列化不保证字段顺序，re-marshal 可能丢弃未知字段，且接收方签名校验针对原始字节进行。

---

## 14. 文件布局

```
net/pbft_handler.go                       // PbftMsgHandler：dedup + verify + forward + state machine
net/pbft_data_sync.go                     // PbftDataSyncHandler：0x14 commit 结果接收 + 验证
p2p/protocol.go                           // 新增 MsgPbftMsg = 0x34, MsgPbftCommitMsg = 0x14
net/handler.go                            // 新增 case 0x34, 0x14 分发
core/rawdb/accessors_pbft_sign.go         // 新增 WriteLatestPbftBlockNum / ReadLatestPbftBlockNum
core/rawdb/accessors_witness_schedule.go  // 新增 WritePreviousShuffledWitnesses / ReadPreviousShuffledWitnesses
consensus/dpos/maintenance.go             // 维护期边界时 save previous witnesses
```

---

## 15. 退出条件

连接到真实 java-tron 节点（主网或 Nile 测试网）运行后：

- `rawdb.ReadBlockSignData(db, N)` 对已确认块 N 返回非 nil。
- `rawdb.ReadSrSignData(db, epoch)` 对已过维护期的 epoch 返回非 nil。
- `make test` 28 个包全绿（单测，不依赖 java-tron 进程）。
- 无 DisconnectReason 被 java-tron 对端踢出。

SR 出块能力（G3 门）留待 M6b 实现。
