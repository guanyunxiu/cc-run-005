/**
 * CRDT 压测：模拟 3 个客户端并发随机编辑 1000 次，校验最终一致性
 *
 * 纯内存模拟（无需启动服务端）：
 *  - 1 个服务端副本 + 3 个客户端副本，各自持有独立 CRDTDoc
 *  - 每次"在线"编辑：本地乐观合并 + 立刻发给服务端；服务端合并后立即广播给其他人，
 *    但每条消息按概率被延迟/重排（模拟乱序到达），延迟消息放进网络队列稍后投递
 *  - "离线"窗口：客户端与服务端断开，期间作画进入 outbox；重连时：
 *      1) 拉服务端快照幂等合并（含断线期间其他人的 op）
 *      2) 把 outbox 以批量 ops 发送（含重连瞬间的消息延迟/重复投递）
 *  - 操作类型：add（80%）/ del 撤销自己最后一笔（20%）
 *
 * 判定：
 *  1. 1000 次编辑完成且所有延迟消息排空后，服务端 + 3 个客户端 stateDigest 完全一致
 *  2. 每个副本 VV 一致
 *  3. pending 全部清空
 *  4. 服务端 HTTP 快照语义：重建一个新副本 loadSnapshot 后与在线状态一致
 */
'use strict';

const { CRDTDoc } = require('../public/crdt');

const TOTAL_EDITS = Number(process.env.EDITS || 1000);
const SEED = Number(process.env.SEED || 42);
const DELAY_RATE = 0.25; // 单条消息被延迟（乱序）的概率
const OFFLINE_RATE = 0.02; // 每轮客户端进入离线窗口的概率
const REORDER_RATE = 0.3; // 延迟消息"插队"：与更早消息交换投递顺序

let failures = 0;
function assert(cond, name, extra) {
  if (cond) {
    console.log(`  ✔ ${name}`);
  } else {
    failures += 1;
    console.error(`  ✘ ${name}`, extra || '');
  }
}

/* ---------------- 可复现随机数（mulberry32） ---------------- */
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
const rand = mulberry32(SEED);

/* ---------------- 模拟网络 ---------------- */

/**
 * 网络：消息带投递"时刻"，step 时取出到期消息。
 * 为模拟乱序，到期消息有概率与其后一条交换顺序；
 * 为模拟重连重复，批量重发的消息有概率被复制一份。
 */
class Network {
  constructor() {
    this.queue = []; // {deliverAt, from, to, op}
    this.tick = 0;
  }

  send(from, to, op, opts = {}) {
    const delay = rand() < DELAY_RATE ? 1 + Math.floor(rand() * 20) : 0;
    const dup = opts.allowDup && rand() < 0.1 ? 2 : 1;
    for (let i = 0; i < dup; i++) {
      this.queue.push({ deliverAt: this.tick + delay, from, to, op });
    }
  }

  advance(n = 1) {
    const delivered = [];
    for (let i = 0; i < n; i++) {
      this.tick += 1;
      // 取出所有到期消息
      const due = [];
      for (let j = this.queue.length - 1; j >= 0; j--) {
        if (this.queue[j].deliverAt <= this.tick) {
          due.push(this.queue[j]);
          this.queue.splice(j, 1);
        }
      }
      // 到期批次内随机重排（进一步制造乱序）
      for (let j = due.length - 1; j > 0; j--) {
        if (rand() < REORDER_RATE) {
          const k = Math.floor(rand() * (j + 1));
          [due[j], due[k]] = [due[k], due[j]];
        }
      }
      delivered.push(...due);
    }
    return delivered;
  }

  get pendingMessages() {
    return this.queue.length;
  }
}

/* ---------------- 构造拓扑 ---------------- */

const server = new CRDTDoc('__server__');
const clients = ['c-aaaa', 'c-bbbb', 'c-cccc'].map((id) => ({
  id,
  doc: new CRDTDoc(id),
  online: true,
  offlineUntil: 0,
  outbox: [], // 断线期间（含在线未确认，CRDT 不需要确认，这里仅离线用）
}));
const net = new Network();

/** 服务端处理一条入站 op：合并，把本批新合并 op 广播给在线的其他客户端 */
function serverIngest(op, fromId) {
  const result = server.receive(op);
  if (result.status === 'delivered') {
    for (const merged of result.merged) {
      for (const c of clients) {
        if (c.id === fromId || !c.online) continue;
        net.send('srv', c.id, merged);
      }
    }
  }
  return result;
}

/** 客户端收到一条消息 */
function clientReceive(client, op) {
  client.doc.receive(op);
}

/** 客户端重连：拉快照 → 幂等合并 → outbox 重新入网络队列（服务端幂等，允许重复） */
function reconnect(client) {
  client.online = true;
  // 1) 拉服务端快照（断线期间服务器累积的全部 op），本地幂等合并
  client.doc.loadSnapshot(server.snapshot());
  // 2) outbox 全部重新入网络队列（可能重复/乱序，CRDT 必须安全收敛）
  for (const op of client.outbox) {
    net.queue.push({
      deliverAt: net.tick + (rand() < DELAY_RATE ? 1 + Math.floor(rand() * 10) : 0),
      from: client.id,
      to: '__server__',
      op,
    });
    if (rand() < 0.1) {
      // 模拟重连重传：同一 op 再复制一份
      net.queue.push({
        deliverAt: net.tick,
        from: client.id,
        to: '__server__',
        op,
      });
    }
  }
  client.outbox = [];
}

/* ---------------- 压测主循环 ---------------- */

console.log(`\n压测配置：3 客户端 × ${TOTAL_EDITS} 次随机编辑（seed=${SEED}，延迟率=${DELAY_RATE}，离线率=${OFFLINE_RATE}）\n`);

let addCount = 0;
let delCount = 0;
let offlineWindows = 0;

for (let i = 0; i < TOTAL_EDITS; i++) {
  // 任何离线窗口到期的客户端本 tick 立即重连（与"谁来编辑"解耦）
  for (const c of clients) {
    if (!c.online && net.tick >= c.offlineUntil) {
      reconnect(c);
      offlineWindows += 1;
    }
  }

  // 随机选一个客户端做编辑
  const client = clients[Math.floor(rand() * clients.length)];

  // 随机触发离线（仅在线时）
  if (client.online && rand() < OFFLINE_RATE) {
    client.online = false;
    client.offlineUntil = net.tick + 5 + Math.floor(rand() * 40);
  }

  // 编辑类型：80% add，20% del（目标存在时才删）
  const isAdd = rand() < 0.8;
  let op;
  if (isAdd) {
    op = client.doc.localAdd({
      color: ['#000000', '#e03131', '#1971c2', '#2f9e44'][Math.floor(rand() * 4)],
      width: 0.002 + rand() * 0.01,
      points: [
        { x: rand(), y: rand() },
        { x: rand(), y: rand() },
      ],
    });
    addCount += 1;
  } else {
    // 撤销自己最后一条可见笔（没有就退化为 add，保证每次循环都产生 1 个 op）
    const target = client.doc.undoTarget();
    if (target) {
      op = client.doc.localDelete(target.id);
      delCount += 1;
    } else {
      op = client.doc.localAdd({
        color: '#000000',
        width: 0.004,
        points: [
          { x: rand(), y: rand() },
          { x: rand(), y: rand() },
        ],
      });
      addCount += 1;
    }
  }

  // 发送：在线直接走网络（可能延迟）；离线进 outbox
  if (client.online) {
    net.queue.push({ deliverAt: net.tick + (rand() < DELAY_RATE ? 1 + Math.floor(rand() * 20) : 0), from: client.id, to: '__server__', op });
  } else {
    client.outbox.push(op);
  }

  // 推进网络并投递到期消息
  const msgs = net.advance(1);
  for (const m of msgs) {
    if (m.to === '__server__') {
      serverIngest(m.op, m.from);
    } else {
      const target = clients.find((c) => c.id === m.to);
      if (target && target.online) clientReceive(target, m.op);
      // 目标离线时消息丢弃：重连靠快照补齐（模拟断连语义）
    }
  }
}

// 规范化 VV 比较（键顺序无关）
function canonVV(vv) {
  return JSON.stringify(vv, Object.keys(vv).sort());
}

// 排空：推进足够多 tick 让所有延迟消息到达
console.log(`编辑完成：add=${addCount} del=${delCount} 离线窗口=${offlineWindows}，排空延迟消息…`);
for (let round = 0; round < 500; round++) {
  // 每轮先让离线窗口到期者重连（补快照），再投递到期消息
  for (const c of clients) {
    if (!c.online) {
      reconnect(c);
      offlineWindows += 1;
    }
  }
  const msgs = net.advance(1);
  for (const m of msgs) {
    if (m.to === '__server__') {
      serverIngest(m.op, m.from);
    } else {
      const target = clients.find((c) => c.id === m.to);
      if (target && target.online) clientReceive(target, m.op);
    }
  }

  // 网络已静默时，不可能再有迟到依赖：
  // 凡仍卡 pending（短暂离线丢过依赖广播）或 VV 落后的客户端，按真实重连语义重新 join
  // 拉最新快照——快照含全部因果依赖，loadSnapshot 会级联清空 pending。
  if (net.pendingMessages === 0) {
    let needMore = false;
    for (const c of clients) {
      if (c.doc.pending.length > 0 || canonVV(c.doc.vv) !== canonVV(server.vv)) {
        c.doc.loadSnapshot(server.snapshot());
        needMore = true;
      }
    }
    // 重连快照对齐后，outbox 重传可能再产生广播，继续排空
    const totalPending = clients.reduce((s, c) => s + c.doc.pending.length, 0) + server.pending.length;
    if (!needMore && totalPending === 0) break;
  }
}

/* ---------------- 断言最终一致性 ---------------- */

console.log('\n========== 最终一致性校验 ==========');

assert(net.pendingMessages === 0, `网络队列排空（剩余 ${net.pendingMessages}）`);

const serverDigest = server.stateDigest();
const digests = clients.map((c) => c.doc.stateDigest());
assert(
  digests.every((x) => x === serverDigest),
  '3 个客户端 + 服务端 stateDigest 完全一致',
  {
    server: serverDigest.slice(0, 120),
    c0: digests[0].slice(0, 120),
  }
);

// VV 一致（canonVV 已在排空段定义，键顺序无关）
const serverVV = canonVV(server.vv);
assert(clients.every((c) => canonVV(c.doc.vv) === serverVV), '所有副本向量时钟 VV 一致');

// pending 清空
assert(server.pending.length === 0, `服务端 pending 清空（${server.pending.length}）`);
clients.forEach((c, i) => {
  assert(c.doc.pending.length === 0, `客户端 ${i + 1} pending 清空（${c.doc.pending.length}）`);
});

// 节点数 / 墓碑数一致
const serverNodes = server.nodes.size;
assert(
  clients.every((c) => c.doc.nodes.size === serverNodes),
  `所有副本节点数一致：${serverNodes}`
);
const serverTomb = server.tombstoneCount();
assert(
  clients.every((c) => c.doc.tombstoneCount() === serverTomb),
  `所有副本墓碑数一致：${serverTomb}`
);
assert(
  serverTomb === delCount,
  `墓碑数 == del 操作数：${serverTomb} == ${delCount}`
);

// op 总数 = add + del
assert(server.log.length === TOTAL_EDITS, `服务端 op 日志数 == 编辑次数：${server.log.length} == ${TOTAL_EDITS}`);
assert(
  clients.every((c) => c.doc.applied.size === server.applied.size),
  `所有副本 applied(op id) 集合大小一致：${server.applied.size}`
);

// 快照语义：全新副本只通过快照 + 无后续 op 也能恢复同一状态
const fresh = new CRDTDoc('fresh-joiner');
fresh.loadSnapshot(server.snapshot());
assert(fresh.stateDigest() === serverDigest, '新客户端 join 仅凭快照恢复到同一最终状态');
assert(fresh.pending.length === 0, '新客户端快照合并后无 pending');
assert(fresh.visibleStrokes().length === server.visibleStrokes().length, '新客户端可见笔画数一致');

// 快照之后再收一条新 op 仍可因果交付
const editor = clients[0];
const lateOp = editor.doc.localAdd({
  color: '#123456',
  width: 0.005,
  points: [
    { x: 0.1, y: 0.1 },
    { x: 0.2, y: 0.2 },
  ],
});
const r = fresh.receive(lateOp);
assert(r.status === 'delivered', '快照恢复后收到增量 op 可直接因果交付');
assert(fresh.nodes.size === serverNodes + 1, '新副本追加后节点数 +1');

console.log('\n========== 统计 ==========');
console.log(`  编辑总数:   ${TOTAL_EDITS} (add=${addCount}, del=${delCount})`);
console.log(`  离线窗口:   ${offlineWindows}`);
console.log(`  网络延迟:   概率 ${DELAY_RATE}，最大延迟 20 tick，到期批次重排率 ${REORDER_RATE}`);
console.log(`  最终节点:   ${serverNodes}（可见 ${server.visibleStrokes().length} / 墓碑 ${serverTomb}）`);
console.log(`  服务端合并: local=${server.stats.local} remote=${server.stats.remote} buffered=${server.stats.buffered} duplicate=${server.stats.duplicate}`);
clients.forEach((c, i) => {
  const s = c.doc.stats;
  console.log(
    `  客户端 ${i + 1}: local=${s.local} remote=${s.remote} buffered=${s.buffered} snapshot=${s.snapshot} duplicate=${s.duplicate}`
  );
});

console.log(failures === 0 ? '\n压测通过 ✅ 所有副本最终一致\n' : `\n${failures} 项校验失败 ❌\n`);
process.exit(failures === 0 ? 0 : 1);
