/**
 * CRDT 压测与一致性校验
 *
 * 阶段 0：纯内存 fuzz —— 多副本随机编辑 + 乱序/重复投递，校验收敛
 * 阶段 1：离线编辑与合并 —— 双端断线各画一笔，重连后双方看到相同两笔、顺序一致
 * 阶段 2：WS 集成压测 —— 3 客户端并发随机编辑 1000 次（含中途离线窗口），
 *         校验最终一致 + GET /api/rooms/:roomId/crdt 快照重放一致
 *
 * 运行：npm test（自动拉起临时服务端，无需手动启动）
 */
'use strict';

const http = require('http');
const path = require('path');
const { spawn } = require('child_process');
const WebSocket = require('ws');
const StrokeCRDT = require('../public/crdt');

let failures = 0;
function assert(cond, name) {
  if (cond) {
    console.log(`  ✔ ${name}`);
  } else {
    failures += 1;
    console.error(`  ✘ ${name}`);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 可复现的伪随机数生成器 */
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const COLORS = ['#1a1a1a', '#e03131', '#1971c2', '#2f9e44', '#f08c00', '#9c36b5'];

function randomStroke(rnd) {
  const n = 1 + Math.floor(rnd() * 8);
  let x = rnd();
  let y = rnd();
  const points = [];
  for (let i = 0; i < n; i++) {
    points.push({ x, y });
    x = Math.min(1, Math.max(0, x + (rnd() - 0.5) * 0.2));
    y = Math.min(1, Math.max(0, y + (rnd() - 0.5) * 0.2));
  }
  return {
    color: COLORS[Math.floor(rnd() * COLORS.length)],
    width: 0.002 + rnd() * 0.01,
    points,
  };
}

/** VV 归一化串（键序无关），用于跨副本比较 */
function normVV(vv) {
  return JSON.stringify(
    Object.keys(vv)
      .sort()
      .map((k) => [k, vv[k]])
  );
}

/* ================= 阶段 0：纯内存 fuzz =================
 * N 个副本各自随机产生 op，投递时打乱顺序并随机重复，
 * 全部投递完后校验：指纹一致、VV 一致、pending 清空。 */
function fuzzOnce(seed, peerCount, opCount) {
  const rnd = mulberry32(seed);
  const peers = [];
  for (let i = 0; i < peerCount; i++) peers.push(new StrokeCRDT(`peer-${seed}-${i}`));
  const mailboxes = peers.map(() => []);

  for (let i = 0; i < opCount; i++) {
    const pi = Math.floor(rnd() * peerCount);
    const p = peers[pi];
    const roll = rnd();
    let op;
    if (roll < 0.6 || p.visible().length === 0) {
      op = p.addStroke(randomStroke(rnd));
    } else if (roll < 0.85) {
      // 并发删除：目标可能是别人刚画的（删除 + 插入并发场景）
      const vis = p.visible();
      op = p.deleteStroke(vis[Math.floor(rnd() * vis.length)].id);
    } else {
      // 撤销：只应影响自己的目标 stroke
      op = p.undo() || p.addStroke(randomStroke(rnd));
    }
    for (let j = 0; j < peerCount; j++) {
      if (j === pi) continue;
      mailboxes[j].push(op);
      if (rnd() < 0.2) mailboxes[j].push(op); // 20% 重复投递
    }
  }

  // 乱序投递（洗牌）
  for (let j = 0; j < peerCount; j++) {
    const box = mailboxes[j];
    for (let i = box.length - 1; i > 0; i--) {
      const k = Math.floor(rnd() * (i + 1));
      [box[i], box[k]] = [box[k], box[i]];
    }
    for (const op of box) peers[j].apply(op);
  }

  const fp = peers[0].fingerprint();
  const vv0 = normVV(peers[0].vv);
  let ok = true;
  for (const p of peers) {
    if (p.fingerprint() !== fp) ok = false;
    if (normVV(p.vv) !== vv0) ok = false;
    if (p.pending.length !== 0) ok = false;
  }
  assert(
    ok,
    `fuzz seed=${seed}: ${peerCount} 副本 × ${opCount} ops 乱序/重复投递后收敛（指纹/VV/pending 一致）`
  );
}

function fuzzPhase() {
  console.log('\n[阶段 0] 纯内存 fuzz：乱序 / 重复投递下的收敛性');
  fuzzOnce(1, 3, 300);
  fuzzOnce(2, 5, 500);
  fuzzOnce(3, 4, 1000);
}

/* ================= 模拟客户端（与前端 app.js 同一套协议行为） ================= */
class SimClient {
  constructor(name, room, port) {
    this.name = name;
    this.room = room;
    this.port = port;
    this.clientId = StrokeCRDT.uuid();
    this.doc = new StrokeCRDT(this.clientId);
    this.outbox = [];
    this.ws = null;
    this.online = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${this.port}/ws`);
      this.ws = ws;
      const timer = setTimeout(() => reject(new Error(`${this.name} 连接超时`)), 5000);
      ws.on('open', () => {
        // join 携带本地向量时钟，服务端回 VV 差量快照
        ws.send(
          JSON.stringify({
            type: 'join',
            roomId: this.room,
            userId: this.name,
            clientId: this.clientId,
            vv: this.doc.vv,
          })
        );
      });
      ws.on('message', (data) => {
        let msg;
        try {
          msg = JSON.parse(data);
        } catch {
          return;
        }
        if (msg.type === 'joined') {
          const snap = msg.snapshot || { ops: [] };
          for (const op of snap.ops || []) this.doc.apply(op);
          this.online = true;
          // 批量补发服务端缺失的「自己的」op（含 outbox）
          const covered = (snap.vv && snap.vv[this.clientId]) || 0;
          const missing = this.doc.opLog.filter(
            (op) => op.id.client === this.clientId && (op.vv[this.clientId] || 0) > covered
          );
          this.outbox.length = 0;
          if (missing.length) {
            ws.send(JSON.stringify({ type: 'ops', roomId: this.room, ops: missing }));
          }
          clearTimeout(timer);
          resolve();
        } else if (msg.type === 'op') {
          this.doc.apply(msg.op);
        }
      });
      ws.on('close', () => {
        this.online = false;
      });
      ws.on('error', () => {});
    });
  }

  disconnect() {
    this.online = false;
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        /* ignore */
      }
    }
  }

  sendOp(op) {
    if (this.online && this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: 'op', roomId: this.room, op }));
    } else {
      this.outbox.push(op); // 断线：进 outbox，重连后批量补发
    }
  }

  add(rnd) {
    this.sendOp(this.doc.addStroke(randomStroke(rnd)));
  }

  del(rnd) {
    const vis = this.doc.visible();
    if (!vis.length) return false;
    this.sendOp(this.doc.deleteStroke(vis[Math.floor(rnd() * vis.length)].id));
    return true;
  }

  undo() {
    const op = this.doc.undo();
    if (!op) return false;
    this.sendOp(op);
    return true;
  }
}

/** 等待所有客户端收敛：VV 一致、pending 与 outbox 清空 */
async function waitConverged(clients, timeout) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    const vv0 = normVV(clients[0].doc.vv);
    const ok =
      clients.every((c) => normVV(c.doc.vv) === vv0) &&
      clients.every((c) => c.doc.pending.length === 0) &&
      clients.every((c) => c.outbox.length === 0);
    if (ok) return true;
    await sleep(100);
  }
  return false;
}

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let buf = '';
        res.on('data', (c) => (buf += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(buf) });
          } catch (e) {
            reject(e);
          }
        });
      })
      .on('error', reject);
  });
}

async function waitForServer(port, timeout = 10000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    try {
      await new Promise((resolve, reject) => {
        const req = http.get(`http://127.0.0.1:${port}/`, (res) => {
          res.resume();
          resolve();
        });
        req.on('error', reject);
      });
      return;
    } catch {
      await sleep(100);
    }
  }
  throw new Error('临时服务端启动超时');
}

/* ================= 阶段 1：离线编辑与合并 =================
 * D、E 断线期间各画一笔进 outbox，重连后双方看到相同两笔、顺序一致。 */
async function offlineScenario(port) {
  console.log('\n[阶段 1] 离线编辑与合并：双端断线各画一笔，重连后一致');
  const room = 'offline-' + Date.now();
  const rnd = mulberry32(7);
  const D = new SimClient('D', room, port);
  const E = new SimClient('E', room, port);
  await D.connect();
  await E.connect();
  D.disconnect();
  E.disconnect();
  await sleep(300);

  D.add(rnd); // 离线绘制 → outbox
  E.add(rnd);
  assert(D.outbox.length === 1 && E.outbox.length === 1, '断线期间 op 进入本地 outbox');

  await D.connect();
  await E.connect();

  const converged = await waitConverged([D, E], 10000);
  assert(converged, '重连后双端收敛（VV 一致、pending/outbox 清空）');
  assert(D.doc.visible().length === 2 && E.doc.visible().length === 2, '双方各看到 2 笔');
  assert(D.doc.fingerprint() === E.doc.fingerprint(), '双端 RGA 顺序一致（指纹相同）');
  D.disconnect();
  E.disconnect();
}

/* ================= 阶段 2：WS 集成压测 =================
 * 3 客户端并发随机编辑 1000 次（add/del/undo 混合，含中途离线窗口），
 * 校验最终一致 + REST 快照重放一致。 */
async function stressScenario(port) {
  console.log('\n[阶段 2] WS 集成压测：3 客户端并发随机编辑 1000 次（含中途离线）');
  const room = 'stress-' + Date.now();
  const clients = ['A', 'B', 'C'].map((n) => new SimClient(n, room, port));
  for (const c of clients) await c.connect();
  console.log('  ℹ 3 个客户端已加入房间');

  const rnd = mulberry32(20260917);
  const TOTAL = 1000;
  let offline = null;
  const t0 = Date.now();

  for (let i = 0; i < TOTAL; i++) {
    // 中途制造离线窗口：C 断线，期间它产生的 op 进 outbox，700 步时重连补发
    if (i === 600) {
      clients[2].disconnect();
      offline = clients[2];
    }
    if (i === 700 && offline) {
      await offline.connect();
      offline = null;
    }

    const c = clients[Math.floor(rnd() * clients.length)];
    const roll = rnd();
    if (roll < 0.65) {
      c.add(rnd);
    } else if (roll < 0.85) {
      if (!c.del(rnd)) c.add(rnd);
    } else {
      if (!c.undo()) c.add(rnd);
    }

    if (i % 20 === 0) await sleep(1); // 让出事件循环，让消息充分交错
  }
  const sendMs = Date.now() - t0;

  const converged = await waitConverged(clients, 30000);
  const totalMs = Date.now() - t0;
  assert(converged, '3 个客户端最终收敛（VV 一致、pending 与 outbox 清空）');

  const fp = clients[0].doc.fingerprint();
  for (const c of clients) {
    assert(c.doc.fingerprint() === fp, `客户端 ${c.name} 的 CRDT 指纹与 A 一致`);
  }
  const stats0 = clients[0].doc.stats();
  assert(
    clients.every((c) => c.doc.stats().tombstones === stats0.tombstones),
    `墓碑数量一致（${stats0.tombstones}）`
  );

  // REST 快照校验：GET /api/rooms/:roomId/crdt
  const { status, body } = await fetchJSON(
    `http://127.0.0.1:${port}/api/rooms/${encodeURIComponent(room)}/crdt`
  );
  assert(status === 200 && body.roomId === room && Array.isArray(body.ops), 'GET /api/rooms/:roomId/crdt 返回快照');
  assert(body.stats && body.stats.mergeCount === TOTAL, `服务端合并次数 = ${TOTAL}（无丢失、无重复合并）`);
  const replay = new StrokeCRDT('replay-check');
  for (const op of body.ops) replay.apply(op);
  assert(replay.pending.length === 0, '快照 op 因果完备（重放无 pending）');
  assert(replay.fingerprint() === fp, '服务端快照重放后与客户端状态一致');

  console.log(
    `  ℹ 发送 ${TOTAL} 次编辑耗时 ${sendMs}ms，含收敛共 ${totalMs}ms；` +
      `可见 ${clients[0].doc.visible().length} 笔，墓碑 ${stats0.tombstones}，节点 ${stats0.nodes}`
  );
  for (const c of clients) c.disconnect();
}

/* ================= 主流程 ================= */
(async () => {
  console.log('\n=== CRDT 协同一致性测试 ===');
  fuzzPhase();

  const port = 3400 + Math.floor(Math.random() * 600);
  const server = spawn(process.execPath, [path.join(__dirname, '..', 'server.js')], {
    env: Object.assign({}, process.env, { PORT: String(port), QUIET: '1' }),
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  await waitForServer(port);
  console.log(`\n临时服务端已启动（端口 ${port}）`);

  try {
    await offlineScenario(port);
    await stressScenario(port);
  } finally {
    server.kill();
  }

  console.log(failures === 0 ? '\n全部测试通过 ✅\n' : `\n${failures} 项测试失败 ❌\n`);
  process.exit(failures === 0 ? 0 : 1);
})().catch((err) => {
  console.error('测试异常:', err);
  process.exit(1);
});
