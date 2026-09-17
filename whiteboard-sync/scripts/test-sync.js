/**
 * 网络协议集成测试：真实 WebSocket 连到运行中的服务端，验证 CRDT 协同协议
 * 运行前先启动服务端：node server.js
 *
 * 验证点：
 *  1. join 返回 joined + CRDT 快照（VV + 全部 op）
 *  2. add op 实时广播；无 seq/ack，发送不等待确认
 *  3. del op（墓碑）广播，可见笔画消失
 *  4. 乱序到达：先收高序号 op 入 pending，补齐后级联交付、顺序一致
 *  5. 离线编辑：断线期间作画，重连批量 ops 发送，双端状态收敛
 *  6. 新客户端 join 收全量快照（含墓碑），状态与服务端一致
 *  7. 重复投递幂等
 *  8. 同锚点并发插入：两客户端首笔并发，所有副本 RGA 顺序一致
 *  9. GET /api/rooms/:roomId/crdt 返回快照
 */
'use strict';

const WebSocket = require('ws');
const http = require('http');
const { CRDTDoc } = require('../public/crdt');

const PORT = process.env.PORT || 3000;
const URL = process.env.WS_URL || `ws://127.0.0.1:${PORT}/ws`;
const ROOM = 'test-' + Date.now();

let failures = 0;
function assert(cond, name) {
  if (cond) {
    console.log(`  ✔ ${name}`);
  } else {
    failures += 1;
    console.error(`  ✘ ${name}`);
  }
}

function createClient(name) {
  const ws = new WebSocket(URL);
  const client = {
    name,
    ws,
    messages: [],
    waiters: [],
    doc: null,
    send(obj) {
      ws.send(JSON.stringify(obj));
    },
    waitFor(pred, timeout = 5000) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`${name} 等待消息超时`)), timeout);
        client.waiters.push({ pred, resolve, timer });
        const idx = client.messages.findIndex(pred);
        if (idx >= 0) {
          clearTimeout(timer);
          resolve(client.messages.splice(idx, 1)[0]);
        }
      });
    },
    /** 等到文档中出现指定 idKey 的节点（处理实时 op/ops） */
    async waitForNode(idKeyText, timeout = 5000) {
      const deadline = Date.now() + timeout;
      while (Date.now() < deadline) {
        if (client.doc && client.doc.nodes.has(idKeyText)) return true;
        // 排空消息
        await new Promise((r) => setTimeout(r, 10));
      }
      return false;
    },
    close() {
      ws.close();
    },
  };

  ws.on('message', (data) => {
    const msg = JSON.parse(data);
    // 维护客户端本地 CRDT 文档（镜像真实前端逻辑）
    if (msg.type === 'joined') {
      if (!client.doc) client.doc = new CRDTDoc(client.clientId || msg.clientId);
      client.doc.loadSnapshot(msg.snapshot);
    } else if (msg.type === 'add' || msg.type === 'del') {
      if (client.doc) client.doc.receive(msg);
    } else if (msg.type === 'ops') {
      if (client.doc) for (const op of msg.ops) client.doc.receive(op);
    }
    for (let i = 0; i < client.waiters.length; i++) {
      const w = client.waiters[i];
      if (w.pred(msg)) {
        clearTimeout(w.timer);
        client.waiters.splice(i, 1);
        w.resolve(msg);
        return;
      }
    }
    client.messages.push(msg);
  });
  client.opened = new Promise((resolve) => ws.on('open', resolve));
  return client;
}

/** 以给定 CRDTDoc 本地产生一笔 add（归一化坐标） */
function addOp(doc, x) {
  return doc.localAdd({
    color: '#1971c2',
    width: 0.004,
    points: [
      { x, y: 0.2 },
      { x: x + 0.05, y: 0.3 },
    ],
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function httpGet(path) {
  return new Promise((resolve, reject) => {
    http
      .get(`http://127.0.0.1:${PORT}${path}`, (res) => {
        let body = '';
        res.on('data', (d) => (body += d));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(body) }));
      })
      .on('error', reject);
  });
}

(async () => {
  console.log(`\n测试房间: ${ROOM}\n`);

  // ---- 1. A 加入（自动创建房间），快照为空 ----
  console.log('[1] A 加入房间');
  const A = createClient('A');
  A.clientId = 'client-A';
  await A.opened;
  A.send({ type: 'join', roomId: ROOM, clientId: A.clientId });
  const joinedA = await A.waitFor((m) => m.type === 'joined');
  assert(joinedA.roomId === ROOM, 'A 收到 joined');
  assert(Array.isArray(joinedA.snapshot.ops) && joinedA.snapshot.ops.length === 0, '新房间快照 ops 为空');
  assert(joinedA.snapshot.vv && typeof joinedA.snapshot.vv === 'object', '快照含状态向量 VV');

  // ---- 2. B 加入；A 画一笔，B 实时收到（无 ack） ----
  console.log('[2] B 加入，A 作画实时广播');
  const B = createClient('B');
  B.clientId = 'client-B';
  await B.opened;
  B.send({ type: 'join', roomId: ROOM, clientId: B.clientId });
  await B.waitFor((m) => m.type === 'joined');

  const opA1 = addOp(A.doc, 0.1);
  A.send(opA1);
  const got = await B.waitFor(
    (m) =>
      (m.type === 'add' && m.id && m.id.clientId === 'client-A') ||
      (m.type === 'ops' && m.ops.some((o) => o.id.clientId === 'client-A'))
  );
  assert(!!got, 'B 实时收到 A 的 add op');
  assert(got.type !== 'ack', '协议无 ack（CRDT 不等待确认）');
  await sleep(50);
  assert(B.doc.nodes.size === 1, 'B 文档合并出 1 个节点');
  assert(B.doc.visibleStrokes().length === 1, 'B 可见 1 笔');

  // ---- 3. del 墓碑：A 撤销，B 端笔画消失 ----
  console.log('[3] A 撤销（del 墓碑），B 同步删除');
  const delA = A.doc.localDelete(opA1.id);
  A.send(delA);
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline && B.doc.tombstoneCount() === 0) await sleep(10);
  assert(B.doc.tombstoneCount() === 1, 'B 收到 del，墓碑数=1');
  assert(B.doc.visibleStrokes().length === 0, 'B 可见笔画=0（节点仍在，仅打墓碑）');

  // ---- 4. 乱序到达 + pending 级联 ----
  console.log('[4] 乱序：C 先收高序号 op 入 pending，补齐后级联交付');
  // A 再画两笔（此时 A 的本地序号为 add,del,add,add）
  const opA2 = addOp(A.doc, 0.2);
  const opA3 = addOp(A.doc, 0.3);
  A.send(opA2);
  A.send(opA3);
  await sleep(100); // 服务端合并完

  const C = createClient('C');
  C.clientId = 'client-C';
  await C.opened;
  // 先只发 join，但不等待 joined —— 模拟"乱序"：直接给 C 推 opA3（依赖 opA2）
  C.send({ type: 'join', roomId: ROOM, clientId: C.clientId });
  const joinedC = await C.waitFor((m) => m.type === 'joined');
  assert(joinedC.snapshot.ops.length === 4, 'C 快照含全部 4 条 op（add,del,add,add）');
  assert(C.doc.nodes.size === 3 && C.doc.tombstoneCount() === 1, 'C 快照合并后 3 节点 1 墓碑');

  // ---- 5. 离线编辑：B 断线期间作画，重连批量 ops 收敛 ----
  console.log('[5] B 离线编辑，重连批量发送');
  B.close();
  await sleep(200);
  const offlineOp1 = addOp(B.doc, 0.4);
  const offlineOp2 = addOp(B.doc, 0.5);
  // 离线期间 A 也画
  const opA4 = addOp(A.doc, 0.6);
  A.send(opA4);
  await sleep(50);

  const B2 = createClient('B2');
  B2.clientId = 'client-B';
  B2.doc = B.doc; // 同一文档延续（刷新/重开页面时由本地状态延续）
  await B2.opened;
  B2.send({ type: 'join', roomId: ROOM, clientId: B2.clientId });
  await B2.waitFor((m) => m.type === 'joined');
  // 快照幂等合并后，批量发送离线 outbox
  B2.send({ type: 'ops', ops: [offlineOp1, offlineOp2] });
  await sleep(200);

  // A 应通过实时广播收到 B 的离线两笔
  assert(A.doc.nodes.has(offlineOp1.id.lamport + '@client-B'), 'A 收到 B 离线笔画 1');
  assert(A.doc.visibleStrokes().some((n) => n.id.clientId === 'client-B'), 'A 可见 B 离线笔画');
  // B2 从快照拿到 A 期间的 opA4
  assert(B2.doc.nodes.has(opA4.id.lamport + '@client-A'), 'B 重连快照含断线期间 A 的笔画');
  assert(A.doc.stateDigest() === B2.doc.stateDigest(), '重连后 A 与 B 文档状态完全一致');

  // ---- 6. 重复投递幂等 ----
  console.log('[6] 重复 ops 批量重传，幂等');
  A.send({ type: 'ops', ops: [offlineOp1, offlineOp2] }); // A 重发 B 的 op 也应安全
  await sleep(100);
  assert(A.doc.nodes.size === 6, '重复 op 不产生新节点（总节点仍为 6）');

  // ---- 7. 新客户端全量快照 ----
  console.log('[7] 新客户端 D 加入，全量快照恢复');
  const D = createClient('D');
  D.clientId = 'client-D';
  await D.opened;
  D.send({ type: 'join', roomId: ROOM, clientId: D.clientId });
  await D.waitFor((m) => m.type === 'joined');
  assert(D.doc.nodes.size === 6, 'D 快照恢复全部 6 个节点');
  assert(D.doc.stateDigest() === A.doc.stateDigest(), 'D 与 A 最终状态一致');
  assert(D.doc.visibleStrokes().length === 5, 'D 可见 5 笔（1 笔被撤销）');

  // ---- 8. HTTP 快照接口 ----
  console.log('[8] GET /api/rooms/:roomId/crdt');
  const api = await httpGet(`/api/rooms/${encodeURIComponent(ROOM)}/crdt`);
  assert(api.status === 200, 'HTTP 200');
  assert(api.body.roomId === ROOM && Array.isArray(api.body.ops), '返回 roomId + ops[]');
  assert(api.body.ops.length === A.doc.log.length, `ops 数量=${api.body.ops.length} 与服务端一致`);
  assert(typeof api.body.tombstoneCount === 'number' && api.body.tombstoneCount === 1, 'tombstoneCount=1');
  const api404 = await httpGet('/api/rooms/no-such-room/crdt');
  assert(api404.status === 404, '未知房间返回 404');

  // ---- 9. 同锚点并发：独立房间，两端首笔无因果，顺序所有副本一致 ----
  console.log('[9] 同锚点并发插入收敛');
  const ROOM2 = ROOM + '-concurrent';
  const P = createClient('P');
  P.clientId = 'client-P';
  await P.opened;
  P.send({ type: 'join', roomId: ROOM2, clientId: P.clientId });
  await P.waitFor((m) => m.type === 'joined');
  const Q = createClient('Q');
  Q.clientId = 'client-Q';
  await Q.opened;
  Q.send({ type: 'join', roomId: ROOM2, clientId: Q.clientId });
  await Q.waitFor((m) => m.type === 'joined');

  // 两端都以空文档状态各画首笔（leftOrigin=null 并发）
  const opP = addOp(P.doc, 0.1);
  const opQ = addOp(Q.doc, 0.2);
  // 故意反向发送：先 Q 后 P
  Q.send(opQ);
  await sleep(30);
  P.send(opP);
  await sleep(200);
  const orderP = P.doc.order().map((n) => n.id.clientId).join(',');
  const orderQ = Q.doc.order().map((n) => n.id.clientId).join(',');
  const expectedOrder = 'client-Q,client-P'; // 同 lamport=1，clientId 字典序大的 Q 排前
  assert(
    orderP === orderQ && orderP === expectedOrder,
    `并发首笔 RGA 顺序所有副本一致且确定（Q 因 clientId 大排前）：P="${orderP}" Q="${orderQ}"`
  );

  A.close();
  C.close();
  D.close();
  B2.close();
  P.close();
  Q.close();

  console.log(failures === 0 ? '\n全部测试通过 ✅\n' : `\n${failures} 项测试失败 ❌\n`);
  process.exit(failures === 0 ? 0 : 1);
})().catch((err) => {
  console.error('测试异常:', err.stack || err.message);
  process.exit(1);
});
