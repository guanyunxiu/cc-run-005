/**
 * Stroke CRDT —— 无冲突协同白板核心数据结构
 * 浏览器（window.StrokeCRDT）与 Node（module.exports）共用同一份实现。
 *
 * 设计要点：
 *  1. 全局唯一 ID：每个 op / stroke 的 ID = { client: uuid, lamport }。
 *     client 为 uuid，lamport 为本地 Lamport 逻辑时钟（本地事件 +1，
 *     合并远端 op 时取 max），(lamport, client) 构成确定全序。
 *  2. RGA（Replicated Growable Array）：每条 stroke 记录插入锚点
 *     leftOrigin / rightOrigin；并发同位置插入按 (lamport, client)
 *     tie-break，所有副本得到确定一致的顺序。
 *  3. 墓碑删除：delete 只标记 deleted=true，节点保留（其他 stroke 可能
 *     锚定在它上面），并发 add + delete 依然收敛。
 *  4. 因果一致性：每个 op 携带向量时钟（VV），本地做因果就绪检查，
 *     未就绪的 op 进入 pending 缓冲，每次合并后重扫缓冲，
 *     任意到达顺序（乱序 / 重复 / 离线补发）最终都收敛到同一状态。
 */
(function (global, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else global.StrokeCRDT = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /** 虚拟根锚点 key：leftOrigin 为空表示锚定到文档头部 */
  const ROOT = '';

  /** 生成 uuid v4（优先 crypto.randomUUID，降级 Math.random） */
  function uuid() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  /** ID -> 字符串 key（Map 索引用；client 为 uuid、lamport 为整数，拼接无歧义） */
  function idKey(id) {
    return id.client + ':' + id.lamport;
  }

  /** ID 全序比较：先 lamport 后 clientId，任意两个 ID 都可比较且结果处处一致 */
  function compareId(a, b) {
    if (a.lamport !== b.lamport) return a.lamport - b.lamport;
    if (a.client === b.client) return 0;
    return a.client < b.client ? -1 : 1;
  }

  function isValidId(id) {
    return !!id && typeof id.client === 'string' && Number.isInteger(id.lamport);
  }

  /* ================= 向量时钟（Vector Clock） =================
   * VV 用普通对象表示：{ clientId -> 已合并的 op 序号 }（每个客户端的序号是稠密的）。
   * op 携带的 VV 是"包含本 op 在内"的作者视角时钟。 */

  /** 因果就绪检查：op 是其作者的下一个 op，且它依赖的其他客户端进度本地都已达到 */
  function vvReady(localVV, opVV, author) {
    if ((opVV[author] || 0) !== (localVV[author] || 0) + 1) return false;
    for (const k of Object.keys(opVV)) {
      if (k !== author && (opVV[k] || 0) > (localVV[k] || 0)) return false;
    }
    return true;
  }

  /** 分量取 max 合并进本地 VV */
  function vvMergeInto(localVV, opVV) {
    for (const k of Object.keys(opVV)) {
      const v = opVV[k] || 0;
      if (v > (localVV[k] || 0)) localVV[k] = v;
    }
  }

  /**
   * StrokeCRDT 文档
   *
   * 结构：
   *  - nodes: Map<key, { id, left, right, stroke, deleted }>  全部节点（含墓碑）
   *  - pending: 因果缓冲（依赖未满足的远端 op）
   *  - opLog:   已合并 op（因果序），用于快照 / 增量同步
   */
  class StrokeCRDT {
    constructor(clientId) {
      this.clientId = clientId || uuid();
      this.lamport = 0;              // Lamport 逻辑时钟
      this.vv = {};                  // 向量时钟
      this.nodes = new Map();        // key -> RGA 节点
      this.pending = [];             // 因果缓冲
      this.danglingDels = new Map(); // 目标尚未到达的 del（防御性，因果送达下不会发生）
      this.opLog = [];               // 已合并 op（因果序）
      this.mergeCount = 0;           // 统计：成功合并次数
      this.dupCount = 0;             // 统计：重复丢弃次数
      this._orderCache = null;       // RGA 顺序缓存
    }

    /* ================= 本地操作（立即生效，不等任何确认） ================= */

    /** 本地新增一条 stroke（追加到文档末尾），返回可广播的 op */
    addStroke(stroke) {
      const order = this._order();
      const lastNode = order.length ? order[order.length - 1] : null;
      const op = {
        kind: 'add',
        id: this._nextId(),
        vv: this._nextVV(),
        stroke: {
          color: String(stroke.color || '#000000'),
          width: Number(stroke.width) || 0.004,
          points: stroke.points.map((p) => ({ x: Number(p.x), y: Number(p.y) })),
        },
        left: lastNode ? { client: lastNode.id.client, lamport: lastNode.id.lamport } : null, // leftOrigin
        right: null, // rightOrigin：追加在末尾，右侧为空
      };
      this._merge(op);
      return op;
    }

    /** 本地删除（墓碑化）指定 stroke，返回可广播的 op */
    deleteStroke(targetId) {
      const op = {
        kind: 'del',
        id: this._nextId(),
        vv: this._nextVV(),
        target: { client: targetId.client, lamport: targetId.lamport },
      };
      this._merge(op);
      return op;
    }

    /** 撤销：墓碑化自己最近一条可见 stroke；并发新增是独立节点，互不影响 */
    undo() {
      const order = this._order();
      for (let i = order.length - 1; i >= 0; i--) {
        const n = order[i];
        if (!n.deleted && n.id.client === this.clientId) {
          return this.deleteStroke(n.id);
        }
      }
      return null;
    }

    /** 下一个 op 的 ID：lamport = 本地时钟 + 1（_merge 时推进时钟） */
    _nextId() {
      return { client: this.clientId, lamport: this.lamport + 1 };
    }

    /** 下一个 op 的 VV：本地 VV 拷贝 + 作者序号 +1（_merge 时写回） */
    _nextVV() {
      const vv = Object.assign({}, this.vv);
      vv[this.clientId] = (vv[this.clientId] || 0) + 1;
      return vv;
    }

    /* ================= 远端 op：因果合并 ================= */

    /**
     * 合并远端 op。
     * 返回本次实际合并的 op 列表（含从 pending 中级联唤醒的），
     * 空数组表示"重复丢弃"或"进入因果缓冲"。
     */
    apply(op) {
      if (!op || !isValidId(op.id) || !op.vv || typeof op.vv !== 'object') return [];
      const author = op.id.client;
      const seq = op.vv[author] || 0;
      if (seq <= (this.vv[author] || 0)) {
        this.dupCount += 1; // 已合并过：幂等丢弃
        return [];
      }
      if (!vvReady(this.vv, op.vv, author)) {
        this.pending.push(op); // 因果依赖未满足：先入缓冲
        return [];
      }
      const merged = [this._merge(op)];
      this._drainPending(merged);
      return merged;
    }

    /** 每合并一条 op 后重扫 pending，把因果就绪的 op 依次合并（可能级联） */
    _drainPending(out) {
      let progressed = true;
      while (progressed) {
        progressed = false;
        for (let i = 0; i < this.pending.length; i++) {
          const op = this.pending[i];
          const author = op.id.client;
          if ((op.vv[author] || 0) <= (this.vv[author] || 0)) {
            this.pending.splice(i, 1); // 等待期间已通过别的路径合并过
            i -= 1;
            this.dupCount += 1;
            progressed = true;
            continue;
          }
          if (vvReady(this.vv, op.vv, author)) {
            this.pending.splice(i, 1);
            i -= 1;
            out.push(this._merge(op));
            progressed = true;
          }
        }
      }
    }

    /** 幂等的结构合并 + 时钟推进（调用前须通过重复 / 因果检查） */
    _merge(op) {
      if (op.kind === 'add') {
        const key = idKey(op.id);
        if (!this.nodes.has(key)) {
          const node = {
            id: { client: op.id.client, lamport: op.id.lamport },
            left: op.left ? idKey(op.left) : ROOT,
            right: op.right ? idKey(op.right) : null,
            stroke: op.stroke,
            deleted: false,
          };
          if (this.danglingDels.has(key)) {
            node.deleted = true; // 删除先于新增到达（防御性路径）
            this.danglingDels.delete(key);
          }
          this.nodes.set(key, node);
        }
      } else if (op.kind === 'del') {
        const tk = idKey(op.target);
        const node = this.nodes.get(tk);
        if (node) node.deleted = true; // 墓碑：不真删，保证并发 add+delete 收敛
        else this.danglingDels.set(tk, true);
      }
      vvMergeInto(this.vv, op.vv); // 向量时钟推进
      if (op.id.lamport > this.lamport) this.lamport = op.id.lamport; // Lamport 时钟取 max
      this.opLog.push(op);
      this.mergeCount += 1;
      this._orderCache = null;
      return op;
    }

    /* ================= RGA 有序遍历 =================
     * 树结构：节点的孩子 = 所有 leftOrigin 指向它的节点；
     * 兄弟按 (lamport, client) 降序（并发同位置插入 id 大者靠前）；
     * 深度优先遍历得到全局唯一确定的线性顺序。 */
    _order() {
      if (this._orderCache) return this._orderCache;

      const children = new Map(); // parentKey -> [node]
      for (const node of this.nodes.values()) {
        const arr = children.get(node.left);
        if (arr) arr.push(node);
        else children.set(node.left, [node]);
      }
      for (const arr of children.values()) {
        arr.sort((a, b) => compareId(b.id, a.id)); // 降序：id 大者靠前
      }

      // 迭代式 DFS（追加场景是长链，避免递归爆栈）
      const result = [];
      const visited = new Set();
      const stack = [];
      const pushChildren = (parentKey) => {
        const kids = children.get(parentKey);
        if (kids) for (let i = kids.length - 1; i >= 0; i--) stack.push(kids[i]);
      };
      pushChildren(ROOT);
      while (stack.length) {
        const node = stack.pop();
        const k = idKey(node.id);
        if (visited.has(k)) continue;
        visited.add(k);
        result.push(node);
        pushChildren(k); // 子树紧随父节点
      }

      // 防御：锚点缺失的孤儿节点（因果送达下不会发生），按 id 升序附到末尾，保证确定一致
      if (visited.size < this.nodes.size) {
        const orphans = [];
        for (const n of this.nodes.values()) {
          if (!visited.has(idKey(n.id))) orphans.push(n);
        }
        orphans.sort((a, b) => compareId(a.id, b.id));
        for (const n of orphans) result.push(n);
      }

      this._orderCache = result;
      return result;
    }

    /** 可见 stroke（RGA 顺序，过滤墓碑），渲染层使用 */
    visible() {
      return this._order().filter((n) => !n.deleted);
    }

    /* ================= 快照 / 增量同步 ================= */

    /** CRDT 快照：状态向量（VV + lamport）+ 全部已知 op（因果序） */
    snapshot() {
      return {
        vv: Object.assign({}, this.vv),
        lamport: this.lamport,
        ops: this.opLog.slice(),
      };
    }

    /** 相对某个 VV 的增量 op（join 时客户端带上自己的 VV，只补缺失部分） */
    diff(sinceVV) {
      const since = sinceVV || {};
      return this.opLog.filter(
        (op) => (op.vv[op.id.client] || 0) > (since[op.id.client] || 0)
      );
    }

    /* ================= 统计 / 调试 ================= */

    stats() {
      let tombstones = 0;
      for (const n of this.nodes.values()) if (n.deleted) tombstones += 1;
      return {
        clientId: this.clientId,
        lamport: this.lamport,
        vv: Object.assign({}, this.vv),
        nodes: this.nodes.size,
        tombstones,
        pending: this.pending.length,
        mergeCount: this.mergeCount,
        dupCount: this.dupCount,
      };
    }

    /** 状态指纹：RGA 顺序 + 墓碑标记的确定性串，用于一致性校验 */
    fingerprint() {
      const parts = [];
      for (const n of this._order()) {
        parts.push(idKey(n.id) + (n.deleted ? '!' : ''));
      }
      return parts.join(',');
    }
  }

  StrokeCRDT.ROOT = ROOT;
  StrokeCRDT.uuid = uuid;
  StrokeCRDT.idKey = idKey;
  StrokeCRDT.compareId = compareId;
  StrokeCRDT.vvReady = vvReady;
  return StrokeCRDT;
});
