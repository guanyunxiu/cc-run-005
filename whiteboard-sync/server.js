/**
 * 直播白板 · CRDT 协同 —— 服务端
 * Node.js + ws
 *
 * 服务端角色：CRDT 合并 + 广播 + 持久化（内存态）
 *  - 不再分配 seq、不做全局排序：每个 op 自带 { clientId, lamport } 与向量时钟
 *  - 收 op → 因果合并到房间文档（乱序进 pending 缓冲，最终一致）→ 合并成功的 op 立即广播
 *  - 客户端绘制不等待任何服务端确认（无 ACK 协议）
 *  - join：按客户端 VV 计算差量，返回 CRDT 快照（状态向量 + 缺失 op）
 *  - GET /api/rooms/:roomId/crdt 返回房间当前 CRDT 快照
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
const StrokeCRDT = require('./public/crdt');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const HEARTBEAT_INTERVAL = 30000; // 服务端心跳周期 30s
const QUIET = process.env.QUIET === '1'; // 压测时静默合并日志
const MAX_BATCH = 5000; // 单条批量消息（离线补发）的最大 op 数

/* ---------------- 静态文件 + REST ---------------- */

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent(req.url.split('?')[0]);

  // REST：GET /api/rooms/:roomId/crdt —— 房间当前 CRDT 快照
  const apiMatch = urlPath.match(/^\/api\/rooms\/([^/]+)\/crdt$/);
  if (apiMatch) {
    if (req.method !== 'GET') {
      res.writeHead(405);
      res.end('Method Not Allowed');
      return;
    }
    const room = rooms.get(apiMatch[1]);
    if (!room) {
      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'room not found' }));
      return;
    }
    const snap = room.doc.snapshot();
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(
      JSON.stringify({
        roomId: room.id,
        vv: snap.vv, // 状态向量
        lamport: snap.lamport,
        stats: room.doc.stats(),
        ops: snap.ops, // 全部已知 op（因果序）
      })
    );
    return;
  }

  // 静态文件
  let filePath = urlPath === '/' ? '/index.html' : urlPath;
  const absPath = path.join(PUBLIC_DIR, path.normalize(filePath));
  // 防止目录穿越
  if (!absPath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(absPath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME_TYPES[path.extname(absPath)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------------- 房间与 CRDT 文档 ---------------- */

/**
 * rooms: Map<roomId, Room>
 * Room = { id, doc: StrokeCRDT, clients: Set<ws> }
 * 服务端持有全量 CRDT 状态（opLog 即持久化，内存态），用于新客户端 join 快照。
 */
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id: roomId,
      doc: new StrokeCRDT('server:' + roomId), // 服务端只合并，不产生 op
      clients: new Set(),
    });
    console.log(`[room] 创建房间 "${roomId}"`);
  }
  return rooms.get(roomId);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

/** 广播给房间内除 except 外的所有客户端 */
function broadcast(room, obj, except) {
  const msg = JSON.stringify(obj);
  for (const client of room.clients) {
    if (client !== except && client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

function broadcastPresence(room) {
  broadcast(room, { type: 'presence', roomId: room.id, count: room.clients.size }, null);
}

/* ---------------- op 校验与清洗 ---------------- */

function isCleanId(id) {
  return !!id && typeof id.client === 'string' && id.client.length > 0 && Number.isInteger(id.lamport);
}

/** 清洗远端 op，返回结构安全的副本；不合法返回 null */
function sanitizeOp(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const kind = raw.kind;
  const id = raw.id;
  if (!isCleanId(id) || id.lamport < 1) return null;
  if (!raw.vv || typeof raw.vv !== 'object') return null;

  // 向量时钟：只保留非负整数分量，且必须包含作者自己的序号
  const vv = {};
  for (const [k, v] of Object.entries(raw.vv)) {
    if (Number.isInteger(v) && v >= 0) vv[k] = v;
  }
  if (!Number.isInteger(vv[id.client]) || vv[id.client] < 1) return null;

  const op = { kind, id: { client: id.client, lamport: id.lamport }, vv };

  if (kind === 'add') {
    const s = raw.stroke;
    if (!s || !Array.isArray(s.points) || s.points.length === 0 || s.points.length > 100000) return null;
    const points = [];
    for (const p of s.points) {
      const x = Number(p && p.x);
      const y = Number(p && p.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
      points.push({ x, y });
    }
    op.stroke = {
      color: String(s.color || '#000000').slice(0, 32),
      width: Number(s.width) || 0.004,
      points,
    };
    op.left = isCleanId(raw.left) ? { client: raw.left.client, lamport: raw.left.lamport } : null;
    op.right = isCleanId(raw.right) ? { client: raw.right.client, lamport: raw.right.lamport } : null;
    return op;
  }

  if (kind === 'del') {
    if (!isCleanId(raw.target)) return null;
    op.target = { client: raw.target.client, lamport: raw.target.lamport };
    return op;
  }

  return null;
}

/* ---------------- WebSocket ---------------- */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.roomId = null;
  ws.userId = null;
  ws.clientId = null;
  console.log(`[ws] 新连接 ${req.socket.remoteAddress}`);

  // 心跳：客户端 pong 回复（浏览器 WebSocket 会自动回复协议层 pong）
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      send(ws, { type: 'error', message: 'invalid json' });
      return;
    }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    if (ws.roomId && rooms.has(ws.roomId)) {
      const room = rooms.get(ws.roomId);
      room.clients.delete(ws);
      console.log(`[room] "${ws.roomId}" 用户 ${ws.userId} 离开，剩余 ${room.clients.size} 人`);
      broadcastPresence(room);
      // 房间无人时保留 CRDT 文档（内存态持久化），保证刷新/重进可恢复
    }
    console.log(`[ws] 连接关闭 user=${ws.userId}`);
  });

  ws.on('error', () => {});
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'join':
      handleJoin(ws, msg);
      break;
    case 'op':
      handleOp(ws, msg);
      break;
    case 'ops':
      handleOps(ws, msg);
      break;
    default:
      send(ws, { type: 'error', message: `unknown type: ${msg.type}` });
  }
}

/** 加入房间：不存在则创建；按客户端 VV 返回 CRDT 差量快照 */
function handleJoin(ws, msg) {
  const roomId = String(msg.roomId || '').trim();
  const userId = String(msg.userId || '').trim();
  if (!roomId || !userId) {
    send(ws, { type: 'error', message: 'roomId and userId required' });
    return;
  }

  // 若已在其他房间，先退出
  if (ws.roomId && rooms.has(ws.roomId)) {
    rooms.get(ws.roomId).clients.delete(ws);
  }

  const room = getOrCreateRoom(roomId);
  ws.roomId = roomId;
  ws.userId = userId;
  ws.clientId = typeof msg.clientId === 'string' ? msg.clientId : null;
  room.clients.add(ws); // 先加入房间：之后合并的 op 都会广播到本连接，快照与广播无缝衔接

  // 客户端状态向量（清洗为非负整数分量）；空 VV = 新客户端，返回全量历史
  const sinceVV = {};
  if (msg.vv && typeof msg.vv === 'object') {
    for (const [k, v] of Object.entries(msg.vv)) {
      if (Number.isInteger(v) && v >= 0) sinceVV[k] = v;
    }
  }
  const missedOps = room.doc.diff(sinceVV);

  send(ws, {
    type: 'joined',
    roomId,
    userId,
    count: room.clients.size,
    snapshot: {
      vv: room.doc.vv, // 服务端状态向量
      lamport: room.doc.lamport,
      ops: missedOps, // 客户端缺失的 op（因果序）
    },
  });
  broadcastPresence(room);

  console.log(
    `[room] "${roomId}" 用户 ${userId} 加入，VV 差量补发 ${missedOps.length} 条 op，当前 ${room.clients.size} 人`
  );
}

/** 合并单条 op 并按合并结果广播 */
function mergeAndBroadcast(ws, room, rawOp) {
  const op = sanitizeOp(rawOp);
  if (!op) {
    send(ws, { type: 'error', message: 'invalid op' });
    return;
  }
  // 因果合并：乱序 op 进 pending 缓冲，返回 []；重复 op 幂等丢弃
  const merged = room.doc.apply(op);
  // 只广播真正合并成功的 op（含从 pending 中级联唤醒的），
  // 每条 op 恰好广播一次，且按服务端因果合并序发出
  for (const m of merged) {
    broadcast(room, { type: 'op', op: m }, ws);
  }
  if (!QUIET && merged.length) {
    const s = room.doc.stats();
    console.log(
      `[crdt] room="${room.id}" ${op.kind} by ${op.id.client.slice(0, 8)}#${op.id.lamport}` +
        ` merged=${merged.length} nodes=${s.nodes} tomb=${s.tombstones} pending=${s.pending}`
    );
  }
}

/** 接收单条 op（客户端绘制完一笔立即发送，不等任何确认） */
function handleOp(ws, msg) {
  if (!ws.roomId || !rooms.has(ws.roomId)) {
    send(ws, { type: 'error', message: 'not in a room' });
    return;
  }
  mergeAndBroadcast(ws, rooms.get(ws.roomId), msg.op);
}

/** 接收批量 op（断线重连后客户端 outbox 补发） */
function handleOps(ws, msg) {
  if (!ws.roomId || !rooms.has(ws.roomId)) {
    send(ws, { type: 'error', message: 'not in a room' });
    return;
  }
  if (!Array.isArray(msg.ops)) {
    send(ws, { type: 'error', message: 'ops must be an array' });
    return;
  }
  const room = rooms.get(ws.roomId);
  for (const raw of msg.ops.slice(0, MAX_BATCH)) {
    mergeAndBroadcast(ws, room, raw);
  }
}

/* ---------------- 心跳保活与断线清理 ---------------- */

const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      console.log(`[ws] 心跳超时，清理连接 user=${ws.userId}`);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL);

wss.on('close', () => clearInterval(heartbeatTimer));

/* ---------------- 启动 ---------------- */

server.listen(PORT, () => {
  console.log(`白板服务已启动（CRDT 模式）: http://localhost:${PORT}`);
  console.log(`WebSocket 地址: ws://localhost:${PORT}/ws`);
  console.log(`CRDT 快照接口: http://localhost:${PORT}/api/rooms/:roomId/crdt`);
});
