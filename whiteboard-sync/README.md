# 直播白板 · CRDT 协同（无冲突合并 + 因果一致性）

原生 HTML/CSS/JS + Canvas 前端，Node.js + `ws` 后端。协同模型已从「服务端 seq 广播」升级为「每个客户端独立可合并的 CRDT」：支持离线编辑、乱序到达、并发冲突，任意到达顺序最终收敛到同一状态。

## 核心设计

### 1. Stroke CRDT（RGA）

- 每个 op / stroke 的全局唯一 ID = `{ client: uuid, lamport }`；`lamport` 为本地 Lamport 逻辑时钟（本地事件 +1，合并远端 op 时取 max）。
- **RGA（Replicated Growable Array）** 维护 stroke 有序列表：每条 stroke 记录插入锚点 `leftOrigin` / `rightOrigin`；并发同位置插入按 `(lamport, clientId)` 降序 tie-break，所有副本得到确定一致的全序。
- **墓碑删除**：`del` 只标记 `deleted=true`，节点保留（其他 stroke 可能锚定在它上面），并发 add + delete 依然收敛。
- **撤销** = 对自己最近一条可见 stroke 打墓碑，只影响目标 stroke，不影响并发新增。

### 2. 向量时钟（Vector Clock）

- 每个副本维护 VV：`{ clientId -> 已合并的 op 序号 }`（每客户端序号稠密）。
- 每条 op 携带其 VV（含本 op 在内的作者视角时钟）。
- 因果就绪检查：op 是其作者的下一个 op，且它依赖的其他客户端进度本地都已达到 → 可合并，否则进 pending。

### 3. 因果缓冲（Pending Buffer）

- 收到因果依赖未满足的 op 先入 `pending`；每合并一条 op 后重扫 pending，级联合并就绪的 op。
- 重复 op 按 VV 幂等丢弃。任意到达顺序（乱序 / 重复 / 离线补发）最终收敛。

### 4. 离线编辑与合并

- 断线期间本地照常绘制（本地预提交进 CRDT 文档），op 进入本地 `outbox`。
- 重连后 join 携带本地 VV，服务端回 VV 差量快照；客户端再把服务端缺失的「自己的」op 批量补发（`ops` 批量消息），服务端 CRDT 合并后广播。
- 两端离线期间各画一笔，重连后双方看到相同两笔、RGA 顺序一致（测试阶段 1 验证）。

### 5. 服务端角色

- 只做 **CRDT 合并 + 广播 + 持久化（内存态 opLog）**，不再分配 seq、不做全局排序；无 ACK 协议，客户端绘制不等待任何确认。
- 内存中维护每个房间的 CRDT 文档；每条 op 合并成功（含 pending 唤醒）后恰好广播一次。
- 新客户端 join 时发送 CRDT 快照（状态向量 + 缺失 op）；`GET /api/rooms/:roomId/crdt` 返回当前完整快照。

## 目录结构

```
whiteboard-sync/
├── package.json          # 依赖与启动脚本（唯一依赖：ws）
├── server.js             # 服务端：静态文件 + WebSocket + CRDT 合并/广播 + REST 快照
├── public/
│   ├── crdt.js           # CRDT 核心（前后端共用）：RGA + 墓碑 + VV + 因果缓冲
│   ├── index.html        # 页面：加入房间界面 + 白板界面 + 调试面板
│   ├── style.css         # 样式
│   └── app.js            # 前端：采集/平滑/渲染/CRDT 文档/outbox/撤销/调试面板
└── scripts/
    └── test-crdt.js      # 压测：内存 fuzz + 双端离线 + 3 客户端 × 1000 次并发校验
```

## 启动命令

```bash
cd whiteboard-sync
npm install
npm start          # 默认端口 3000，可用 PORT=8080 npm start 修改
```

打开浏览器访问 `http://localhost:3000`。

## 测试

```bash
npm test           # 自动拉起临时服务端，无需手动启动
```

验证内容：

- **阶段 0 · 纯内存 fuzz**：3/5/4 副本 × 300/500/1000 次随机编辑（add/del/undo），乱序 + 20% 重复投递，校验指纹 / VV / pending 全部收敛。
- **阶段 1 · 离线编辑与合并**：双端断线各画一笔进 outbox，重连后双方看到相同两笔、顺序一致。
- **阶段 2 · WS 集成压测**：3 客户端并发随机编辑 1000 次（65% add / 20% del / 15% undo，含中途离线窗口），校验最终 VV 一致、指纹一致、墓碑数一致、`GET /api/rooms/:roomId/crdt` 快照重放一致、服务端合并次数恰好 1000。

## 手动体验

1. **基本书写**：输入房间号加入，落笔即现（本地预提交，不等服务端），贝塞尔平滑。
2. **多端同步**：开两个窗口进同一房间，互画互见；坐标归一化，跨分辨率一致。
3. **撤销**：点「↩ 撤销」或 `Ctrl+Z`，只墓碑化自己最近一笔；对方并发画的笔不受影响。
4. **断线重连**：`Ctrl+C` 停掉服务端后继续画（op 进 outbox，调试面板可见 outbox 计数），重启服务端后自动重连、批量补发，双端最终一致。
5. **调试面板**：点右上角「🐞 调试」，实时显示 clientId、lamport、向量时钟 VV、节点数、墓碑数、pending 数、合并次数、重复丢弃数、outbox 长度。
6. **REST 快照**：`curl http://localhost:3000/api/rooms/room-1/crdt` 查看房间 CRDT 状态（VV、lamport、统计、全部 op）。

## 协议说明（JSON over WebSocket `/ws`）

| 方向 | type | 字段 | 说明 |
|---|---|---|---|
| C→S | `join` | `roomId, userId, clientId, vv` | 加入房间（不存在则创建），`vv` 为本地向量时钟，用于差量同步 |
| S→C | `joined` | `roomId, count, snapshot{vv, lamport, ops[]}` | CRDT 快照：状态向量 + 客户端缺失的 op（因果序） |
| C→S | `op` | `roomId, op` | 单条 op（绘制完成立即发送，不等确认） |
| C→S | `ops` | `roomId, ops[]` | 批量 op（断线重连后 outbox 补发） |
| S→C | `op` | `op` | 广播合并成功的 op 给房间内其他客户端 |
| S→C | `presence` | `count` | 房间在线人数变化 |
| REST | `GET /api/rooms/:roomId/crdt` | — | 返回 `{ roomId, vv, lamport, stats, ops[] }` 快照 |

### op 结构

```jsonc
// 新增 stroke
{
  "kind": "add",
  "id": { "client": "uuid", "lamport": 7 },   // 全局唯一 ID
  "vv": { "uuid": 7, "...": 3 },              // 向量时钟（含本 op）
  "stroke": { "color": "#1971c2", "width": 0.004, "points": [{ "x": 0.1, "y": 0.2 }] },
  "left": { "client": "uuid", "lamport": 5 }, // leftOrigin 插入锚点（null = 文档头）
  "right": null                               // rightOrigin（追加时为空）
}

// 删除 stroke（墓碑）
{
  "kind": "del",
  "id": { "client": "uuid", "lamport": 8 },
  "vv": { "uuid": 8 },
  "target": { "client": "uuid", "lamport": 5 } // 目标 stroke 的 ID
}
```

- 坐标与线宽均按画布尺寸**归一化**（0~1），跨分辨率一致。
- 心跳：服务端每 30s 协议层 `ping`，浏览器自动 `pong`，超时未响应则 `terminate` 清理连接。
- 幂等：所有 op 按 VV 去重，重复投递 / 快照与广播重叠 / 补发重发都不会重复合并。
