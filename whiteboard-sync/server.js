/**
 * 直播白板 —— CRDT 协同服务端
 * Node.js + ws
 *
 * 角色（CRDT 架构下服务端不再是定序中心）：
 *  1. CRDT 合并：把每个房间收到的 op 因果合并进服务端副本（VV 未满足则入 pending）
 *  2. 广播：新合并的 op 转发给房间内其他客户端（不回 ack，客户端绘制不等待确认）
 *  3. 持久化（内存态）：保留全量 CRDT 文档，新客户端 join 发送快照（VV + 全部 op）
 *  4. 静态文件与 HTTP：GET /api/rooms/:roomId/crdt 返回 CRDT 快照
 *
 * 协议（JSON over /ws）：
 *   C→S join   { type:'join', roomId, clientId, vv? }
 *   S→C joined { type:'joined', roomId, snapshot:{vv, ops[]}, count }
 *   C→S op     <add|del 线格式，见 public/crdt.js>
 *   C→S ops    { type:'ops', ops:[...] }            // 重连后批量发送 outbox
 *   S→C op     <add|del 线格式>                      // 广播给其他客户端
 *   S→C ops    { type:'ops', ops:[...] }            // 单条 op 级联出 pending 时批量推
 *   S→C presence { type:'presence', roomId, count }
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
const { CRDTDoc, uuid, isValidOp, idKey } = require('./public/crdt');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const HEARTBEAT_INTERVAL = 30000; // 服务端心跳周期 30s

/* ---------------- 静态文件服务 ---------------- */

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

  // CRDT 快照 REST 接口
  const m = /^\/api\/rooms\/([^/]+)\/crdt$/.exec(urlPath);
  if (m && req.method === 'GET') {
    const roomId = decodeURIComponent(m[1]);
    const room = rooms.get(roomId);
    if (!room) {
      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'room not found' }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(
      JSON.stringify({
        roomId,
        vv: room.doc.vv,
        pending: room.doc.pending.length,
        tombstoneCount: room.doc.tombstoneCount(),
        ops: room.doc.snapshot().ops,
      })
    );
    return;
  }

  let filePath = urlPath;
  if (filePath === '/') filePath = '/index.html';
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
 * Room = { id, doc: CRDTDoc, clients: Set<ws> }
 */
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id: roomId,
      // 服务端副本只做合并/广播/快照，自身不产生 op；固定 ID 避免污染 VV
      doc: new CRDTDoc('__server__'),
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

/**
 * 把一条 op 合并进房间文档：
 *  - delivered：新合并（可能级联出 pending 中的若干 op）→ 全部广播给其他客户端
 *  - pending：因果依赖未满足，已入缓冲，等待前置 op
 *  - duplicate：重传/回环，忽略（幂等）
 *  - invalid：拒绝
 * 返回合并状态供调用方决策（HTTP 压测复用）。
 */
function ingestOp(room, op, exceptWs) {
  const result = room.doc.receive(op);
  if (result.status === 'delivered' && result.merged.length > 0) {
    // 排除发送者（其本地已乐观合并，收到只是幂等 duplicate）；其余客户端广播
    if (result.merged.length === 1) {
      broadcast(room, result.merged[0], exceptWs || null);
    } else {
      broadcast(room, { type: 'ops', ops: result.merged }, exceptWs || null);
    }
  }
  return result;
}

/* ---------------- WebSocket ---------------- */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.roomId = null;
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
      console.log(`[room] "${ws.roomId}" 客户端 ${ws.clientId} 离开，剩余 ${room.clients.size} 人`);
      broadcastPresence(room);
      // 房间无人时保留 CRDT 文档（内存态），保证刷新/重进可恢复
    }
    console.log(`[ws] 连接关闭 client=${ws.clientId}`);
  });

  ws.on('error', () => {});
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'join':
      handleJoin(ws, msg);
      break;
    case 'add':
    case 'del':
      handleOp(ws, msg);
      break;
    case 'ops':
      handleOps(ws, msg);
      break;
    default:
      send(ws, { type: 'error', message: `unknown type: ${msg.type}` });
  }
}

/** 加入房间：不存在则创建；发送服务端全量 CRDT 快照（客户端幂等合并） */
function handleJoin(ws, msg) {
  const roomId = String(msg.roomId || '').trim();
  const clientId = String(msg.clientId || '').trim();
  if (!roomId || !clientId) {
    send(ws, { type: 'error', message: 'roomId and clientId required' });
    return;
  }

  // 若已在其他房间，先退出
  if (ws.roomId && rooms.has(ws.roomId)) {
    rooms.get(ws.roomId).clients.delete(ws);
  }

  const room = getOrCreateRoom(roomId);
  ws.roomId = roomId;
  ws.clientId = clientId;
  room.clients.add(ws);

  send(ws, {
    type: 'joined',
    roomId,
    clientId,
    snapshot: room.doc.snapshot(),
    count: room.clients.size,
  });
  broadcastPresence(room);

  console.log(
    `[room] "${roomId}" 客户端 ${clientId} 加入，快照 op=${room.doc.log.length} ` +
      `墓碑=${room.doc.tombstoneCount()} pending=${room.doc.pending.length}，当前 ${room.clients.size} 人`
  );
}

/** 接收单条 op：CRDT 合并后广播，不回 ack */
function handleOp(ws, msg) {
  if (!ws.roomId || !rooms.has(ws.roomId)) {
    send(ws, { type: 'error', message: 'not in a room' });
    return;
  }
  if (!isValidOp(msg)) {
    send(ws, { type: 'error', message: 'invalid op' });
    return;
  }

  const room = rooms.get(ws.roomId);
  const result = ingestOp(room, msg, ws);

  if (result.status === 'delivered') {
    const op = result.merged[0];
    console.log(
      `[crdt] room="${room.id}" merged ${op.type} id=${idKey(op.id)} by=${ws.clientId} ` +
        `本批合并=${result.merged.length} 总op=${room.doc.log.length} pending=${room.doc.pending.length}`
    );
  } else if (result.status === 'pending') {
    console.log(`[crdt] room="${room.id}" op ${idKey(msg.id)} 因果依赖未满足，进入 pending=${room.doc.pending.length}`);
  }
}

/** 批量接收（重连后 outbox 批量重传）：逐条合并，结果统一广播 */
function handleOps(ws, msg) {
  if (!ws.roomId || !rooms.has(ws.roomId)) {
    send(ws, { type: 'error', message: 'not in a room' });
    return;
  }
  if (!Array.isArray(msg.ops)) {
    send(ws, { type: 'error', message: 'ops[] required' });
    return;
  }
  const room = rooms.get(ws.roomId);

  let delivered = 0;
  let pending = 0;
  let duplicate = 0;
  let invalid = 0;
  for (const op of msg.ops) {
    if (!isValidOp(op)) {
      invalid += 1;
      continue;
    }
    // 批量摄入：广播在 ingestOp 内统一完成（级联出 pending 的也一并推走）
    const r = ingestOp(room, op, ws);
    if (r.status === 'delivered') delivered += 1;
    else if (r.status === 'pending') pending += 1;
    else duplicate += 1;
  }
  console.log(
    `[crdt] room="${room.id}" 批量合并 by=${ws.clientId} 新合并=${delivered} pending=${pending} ` +
      `重复=${duplicate} 非法=${invalid} 总op=${room.doc.log.length}`
  );
  if (invalid > 0) send(ws, { type: 'error', message: `${invalid} invalid op(s) skipped` });
}

/* ---------------- 心跳保活与断线清理 ---------------- */

const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      console.log(`[ws] 心跳超时，清理连接 client=${ws.clientId}`);
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
  console.log(`白板 CRDT 服务已启动: http://localhost:${PORT}`);
  console.log(`WebSocket 地址: ws://localhost:${PORT}/ws`);
  console.log(`快照接口: GET http://localhost:${PORT}/api/rooms/:roomId/crdt`);
});
