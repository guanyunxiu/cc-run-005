# 直播白板 · CRDT 协同（RGA + 向量时钟 + 因果一致性）

原生 HTML/CSS/JS + Canvas 前端，Node.js + `ws` 后端。每条笔画是带全局唯一 ID 的 **CRDT 节点**，
服务端不再分配 `seq`、不再定序，只做 **CRDT 合并 + 广播 + 快照持久化**。支持无冲突协同、离线编辑、
乱序到达、并发插入/删除，任意到达顺序最终收敛到同一状态。

## 核心算法

| 机制 | 实现 |
|---|---|
| 全局唯一 ID | `{ clientId, lamport }`，`clientId` 为持久化 uuid，`lamport` 为经典标量逻辑时钟 |
| 有序列表 | **RGA**（Replicated Growable Array）：每笔记录 `leftOrigin/rightOrigin` 插入锚点，邻接表 + DFS 前序线性化 |
| 并发 tie-break | 同一锚点的并发插入按 `(lamport DESC, clientId DESC)` 排序，全序确定、与到达顺序无关 |
| 删除 | **墓碑**：只置 `deleted=true`，不真删；并发 add + del 也收敛 |
| 向量时钟 | 每个副本维护 VV `{ clientId -> counter }`，每条 op 携带产生时刻的 VV |
| 因果缓冲 | VV 依赖未满足的 op 进 `pending`；每交付一条重扫，级联交付直到定点 |
| 离线编辑 | 断线期间继续作画进本地 `outbox`；重连先幂等合并服务端快照，再批量重发 outbox |
| 新客户端 join | 服务端发送 CRDT 快照（状态向量 + 全部已知 op），本地幂等合并 |

> Lamport 时钟（参与 RGA 排序）与向量时钟（参与因果交付判定）相互独立：
> 本地事件 `lamport+=1`，观测远端事件 `lamport=max(local,remote)+1`；
> 因果交付只看 op 携带的 VV（作者序号连续 `local+1` 且其他端依赖都已满足）。

## 目录结构

```
whiteboard-sync/
├── package.json
├── server.js             # 服务端：静态文件 + WS CRDT 合并/广播/快照 + REST 快照接口
├── public/
│   ├── crdt.js           # ★ CRDT 核心（浏览器/Node 共享）：RGA + VV + pending + 快照
│   ├── index.html        # 工具栏 + 撤销 + CRDT 调试面板
│   ├── style.css
│   └── app.js            # 前端：乐观本地合并 / outbox / 文档驱动渲染 / 撤销
└── scripts/
    ├── unit-crdt.js      # CRDT 算法单元自检（17 项，纯内存，无需服务端）
    ├── test-sync.js      # WebSocket 协议集成测试（需先启动服务端）
    ├── test-pending.js   # 服务端因果缓冲 + 级联广播专项（需先启动服务端）
    └── stress-crdt.js    # ★ 压测：3 客户端 × 1000 次并发随机编辑（纯内存，无需服务端）
```

## 启动

```bash
cd whiteboard-sync
npm install
npm start          # 默认 3000，PORT=8080 npm start 可改
```

打开 `http://localhost:3000`，多个浏览器输入相同房间号即可协同。

## 测试

```bash
# 1) CRDT 算法单元自检（无需服务端）
npm run test:unit

# 2) 压测：3 客户端 1000 次并发随机 add/del，乱序+离线+重传，校验最终一致（无需服务端）
npm run stress
SEED=1 npm run stress     # 换随机种子（可用 EDITS=n 调整次数）

# 3) 网络协议测试（终端 1 先启动服务端，终端 2 运行）
npm start
npm test                 # test-sync.js
npm run test:pending     # 因果缓冲级联专项
```

压测模拟：每条消息 25% 概率延迟/乱序、每轮 2% 概率进入离线窗口（期间作画进 outbox、
广播丢弃、重连靠快照补齐）、重传 10% 概率重复。1000 次编辑排空后断言：

- 服务端 + 3 个客户端 `stateDigest` 完全一致；
- 所有副本向量时钟一致、`pending` 全部清空；
- 节点数 / 墓碑数 / `applied` 集合一致；
- 全新客户端仅凭 join 快照即可恢复到同一最终状态，并能继续因果接收增量。

## 协议（JSON over WebSocket `/ws`）

| 方向 | type | 字段 | 说明 |
|---|---|---|---|
| C→S | `join` | `roomId, clientId, vv?` | 加入房间（不存在则创建） |
| S→C | `joined` | `roomId, snapshot:{vv, ops[]}, count` | 全量 CRDT 快照，客户端幂等合并 |
| C→S | `add` / `del` | op 线格式 | 单条 op（客户端绘制不等待任何确认） |
| C→S | `ops` | `{ops:[...]}` | 重连后批量发送 outbox |
| S→C | `add` / `del` | op 线格式 | 合并后广播给同房间**其他**客户端（不回发送者、无 ack） |
| S→C | `ops` | `{ops:[...]}` | 一条 op 级联冲出多条 pending 时批量推送 |
| S→C | `presence` | `count` | 在线人数变化 |

add op：
```json
{ "type":"add", "id":{"clientId":"<uuid>","lamport":3},
  "leftOrigin":{"clientId":"...","lamport":1}, "rightOrigin":null,
  "color":"#1971c2", "width":0.004,
  "points":[{"x":0.1,"y":0.2}], "vv":{"<clientId>":2} }
```
del op（墓碑）：
```json
{ "type":"del", "id":{"clientId":"<uuid>","lamport":4},
  "target":{"clientId":"...","lamport":1}, "vv":{ } }
```

## REST 快照接口

```
GET /api/rooms/:roomId/crdt
→ 200 { roomId, vv, pending, tombstoneCount, ops:[...] }
→ 404 { error:"room not found" }
```

## 并发冲突的收敛保证

- **同一位置并发插入**：两笔的 `leftOrigin` 相同，进入同一 sibling 组，按
  `(lamport, clientId)` 降序排列，所有副本顺序一致。
- **删除 + 插入并发**：删除只对目标节点打墓碑，新增是独立节点，二者正交，收敛结果一致。
- **撤销 + 并发新笔画**：`del` 只影响 `target` 指定的那一笔，并发新增不受影响。
- **乱序 / 丢依赖**：缺前置的 op 进 pending，前置到达后级联交付，不会丢更新。

## 调试面板

白板右下角面板实时显示：`clientId`、向量时钟 VV、可见笔画数、pending 缓冲数、
累计合并次数、墓碑数量、本地 outbox 待发数。
