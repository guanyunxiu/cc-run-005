/**
 * 服务端因果缓冲（pending）专项测试：真实 WebSocket 连到运行中的服务端
 * 运行前先启动服务端：node server.js
 *
 * 场景：
 *  1. 只发依赖缺失的高序号 op a2 → 服务端必须进 pending，且不广播
 *  2. 补齐前置 a1 → 服务端交付 a1 后级联冲出 a2，旁观客户端收到 a1+a2（单条或 ops 批量）
 *  3. HTTP 快照：2 条 op、pending=0
 */
'use strict';

const WebSocket = require('ws');
const http = require('http');
const { CRDTDoc } = require('../public/crdt');

const PORT = process.env.PORT || 3000;
const URL = process.env.WS_URL || `ws://127.0.0.1:${PORT}/ws`;
const ROOM = 'pend-' + Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function assert(cond, name) {
  console.log(cond ? `  ✔ ${name}` : `  ✘ ${name}`);
  if (!cond) failures += 1;
}

function mk(name) {
  const ws = new WebSocket(URL);
  const c = {
    name,
    ws,
    got: [],
    seen: [], // 所有收到过的消息（含被 waitFor 消费的），供 hasAdd 断言
    waiters: [],
    send(o) {
      ws.send(JSON.stringify(o));
    },
    /** 是否见过某 lamport 的 add op（含 ops 批量，不受 waitFor 消费影响） */
    hasAdd(lamport) {
      for (const m of c.seen) {
        if (m.type === 'add' && m.id && m.id.lamport === lamport) return true;
        if (m.type === 'ops' && m.ops.some((o) => o.type === 'add' && o.id.lamport === lamport)) return true;
      }
      return false;
    },
    waitFor(pred, timeout = 3000) {
      return new Promise((resolve, reject) => {
        // 先在已收到的消息里找
        const idx = c.got.findIndex(pred);
        if (idx >= 0) {
          resolve(c.got.splice(idx, 1)[0]);
          return;
        }
        const timer = setTimeout(() => reject(new Error(name + ' timeout')), timeout);
        c.waiters.push({ pred, resolve, timer });
      });
    },
  };
  ws.on('message', (d) => {
    const m = JSON.parse(d);
    c.seen.push(m);
    for (let i = c.waiters.length - 1; i >= 0; i--) {
      const w = c.waiters[i];
      if (w.pred(m)) {
        clearTimeout(w.timer);
        c.waiters.splice(i, 1);
        w.resolve(m);
        return;
      }
    }
    c.got.push(m);
  });
  c.opened = new Promise((r) => ws.on('open', r));
  return c;
}

function httpGetJson(path) {
  return new Promise((resolve, reject) => {
    http
      .get(`http://127.0.0.1:${PORT}${path}`, (res) => {
        let b = '';
        res.on('data', (d) => (b += d));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(b) }));
      })
      .on('error', reject);
  });
}

(async () => {
  console.log(`\npending 专项测试房间: ${ROOM}\n`);

  const A = mk('writer');
  const B = mk('reader');
  await A.opened;
  await B.opened;
  A.send({ type: 'join', roomId: ROOM, clientId: 'writer' });
  B.send({ type: 'join', roomId: ROOM, clientId: 'reader' });
  await A.waitFor((m) => m.type === 'joined');
  await B.waitFor((m) => m.type === 'joined');

  // 构造因果 op：a1 → a2（a2.vv 依赖 a1）
  const doc = new CRDTDoc('writer');
  const a1 = doc.localAdd({ color: '#000', width: 0.01, points: [{ x: 0.1, y: 0.1 }, { x: 0.2, y: 0.2 }] });
  const a2 = doc.localAdd({ color: '#000', width: 0.01, points: [{ x: 0.3, y: 0.3 }, { x: 0.4, y: 0.4 }] });

  // 1) 只发 a2（缺 a1）→ pending，不广播
  A.send(a2);
  await sleep(150);
  assert(!B.hasAdd(2), '只发 a2（缺前置）：服务端入 pending，未提前广播给 B');

  const before = await httpGetJson(`/api/rooms/${ROOM}/crdt`);
  assert(before.body.pending === 1 && before.body.ops.length === 0, `服务端快照 pending=1、已交付 ops=0（实际 pending=${before.body.pending}, ops=${before.body.ops.length}）`);

  // 2) 补 a1 → 级联交付，B 收到 a1 与 a2（可能是两条单播，也可能合并为一个 ops 批量包）
  A.send(a1);
  const isLamport = (m, lamport) =>
    (m.type === 'add' && m.id && m.id.lamport === lamport) ||
    (m.type === 'ops' && m.ops.some((o) => o.type === 'add' && o.id.lamport === lamport));
  await B.waitFor((m) => isLamport(m, 1), 3000);
  assert(B.hasAdd(1), 'B 收到前置 a1');
  // a2 可能紧随单条到达，也可能与 a1 一起在 ops 批量里
  let deadline = Date.now() + 2000;
  while (Date.now() < deadline && !B.hasAdd(2)) {
    await sleep(20);
  }
  assert(B.hasAdd(2), '补齐 a1 后，服务端级联冲出 pending 的 a2 并广播给 B');

  // 3) 最终快照
  const after = await httpGetJson(`/api/rooms/${ROOM}/crdt`);
  assert(after.body.ops.length === 2 && after.body.pending === 0, `最终快照 ops=2、pending=0（实际 ops=${after.body.ops.length}, pending=${after.body.pending}）`);
  assert(after.body.ops[0].id.lamport === 1 && after.body.ops[1].id.lamport === 2, 'op 日志按因果顺序 [a1, a2]');

  A.ws.close();
  B.ws.close();
  console.log(failures === 0 ? '\n服务端因果缓冲级联测试通过 ✅\n' : `\n${failures} 项失败 ❌\n`);
  process.exit(failures === 0 ? 0 : 1);
})().catch((e) => {
  console.error('异常:', e);
  process.exit(1);
});
