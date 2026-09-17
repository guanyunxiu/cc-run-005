/**
 * CRDT 协同核心（浏览器 / Node 双端共享，无任何 DOM / Node API 依赖）
 *
 * 数据模型：
 *   Stroke 文档 = RGA（Replicated Growable Array）有序集合
 *   - 每个节点（一条 stroke 或一个删除事件）全局唯一 ID：{ clientId, lamport }
 *   - lamport：经典标量逻辑时钟（本地事件 +1；合并远端 op 时取 max，不计数）
 *   - 插入锚点：leftOrigin（插入在哪个节点之后）；rightOrigin 预留（本应用只做尾部追加）
 *   - 同锚点并发插入按 (lamport DESC, clientId DESC) tie-break，全序与到达顺序无关
 *   - 删除 = 墓碑（deleted=true），不真删；并发 add + del 也能收敛
 *
 * 因果一致性（与 lamport 相互独立的向量时钟）：
 *   - vv: 向量时钟 { clientId -> counter }，counter 为该端已交付的自己 op 序号（稠密 1..n）
 *   - 每条 op 携带产生时刻的完整 VV
 *   - canDeliver：op 作者序号必须恰好连续（op.vv[c] == local+1），
 *     且 op 依赖的其他端计数都已满足；lamport 只参与 RGA 排序，不参与因果判定
 *   - 不满足因果依赖的 op 进 pending 缓冲，每合并一条后重扫，直到定点
 *
 * Op 线格式（JSON.stringify 后传输）：
 *   add: { type:'add', id:{clientId,lamport}, leftOrigin:id|null, rightOrigin:null,
 *          color, width, points:[{x,y}], vv }
 *   del: { type:'del', id:{clientId,lamport}, target:id, vv }
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.CRDT = api;
})(typeof self !== 'undefined' ? self : globalThis, function () {
  'use strict';

  /* ---------------- 基础工具 ---------------- */

  function uuid() {
    if (typeof globalThis.crypto !== 'undefined' && typeof globalThis.crypto.randomUUID === 'function') {
      return globalThis.crypto.randomUUID();
    }
    // 兜底：v4 风格随机串
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (ch) => {
      const r = (Math.random() * 16) | 0;
      const v = ch === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  /** 节点主键：lamport@clientId，全局唯一 */
  function idKey(id) {
    return id.lamport + '@' + id.clientId;
  }

  function sameId(a, b) {
    return a !== null && b !== null && a.lamport === b.lamport && a.clientId === b.clientId;
  }

  /**
   * RGA tie-break：ID 越大排越靠前（靠近锚点）
   * 先比 lamport（逻辑时钟越大越优先），相同再按 clientId 字典序，保证全序确定
   * @returns >0: a>b ; 0:相等 ; <0: a<b
   */
  function compareId(a, b) {
    if (a.lamport !== b.lamport) return a.lamport - b.lamport;
    if (a.clientId === b.clientId) return 0;
    return a.clientId > b.clientId ? 1 : -1;
  }

  const vvClone = (vv) => ({ ...vv });

  /** VV 合并：逐维取 max */
  function vvMerge(dst, src) {
    for (const k of Object.keys(src)) {
      if ((dst[k] || 0) < src[k]) dst[k] = src[k];
    }
  }

  /**
   * 因果交付判定（向量时钟 + 同端序号连续）：
   *   op 作者 c 的序号取 op.vv[c]（稠密 1..n），要求恰好等于 localVV[c]+1（不能跳号），
   *   且 op.vv 里其他每个端 k 都有 localVV[k] >= op.vv[k]。
   *   注意：判定只看 VV，与 lamport 无关（lamport 仅用于 RGA tie-break）。
   */
  function vvCanDeliver(localVV, op) {
    const c = op.id.clientId;
    const n = op.vv[c] || 0;
    if (n !== (localVV[c] || 0) + 1) return false;
    for (const k of Object.keys(op.vv)) {
      if (k === c) continue;
      if ((localVV[k] || 0) < (op.vv[k] || 0)) return false;
    }
    return true;
  }

  function isId(v) {
    return (
      v !== null &&
      typeof v === 'object' &&
      typeof v.clientId === 'string' &&
      v.clientId.length > 0 &&
      Number.isInteger(v.lamport) &&
      v.lamport >= 1
    );
  }

  /** 线格式校验：服务端拒绝脏数据，压测用来构造合法 op */
  function isValidOp(op) {
    if (!op || typeof op !== 'object' || !isId(op.id)) return false;
    if (!op.vv || typeof op.vv !== 'object') return false;
    if (op.type === 'add') {
      if (op.leftOrigin !== null && !isId(op.leftOrigin)) return false;
      if (op.rightOrigin !== null && !isId(op.rightOrigin)) return false;
      if (typeof op.color !== 'string' || op.color.length === 0) return false;
      if (typeof op.width !== 'number' || !(op.width > 0)) return false;
      if (!Array.isArray(op.points) || op.points.length === 0) return false;
      return op.points.every(
        (p) => p && typeof p.x === 'number' && typeof p.y === 'number' && Number.isFinite(p.x) && Number.isFinite(p.y)
      );
    }
    if (op.type === 'del') return isId(op.target);
    return false;
  }

  /* ---------------- CRDT 文档 ---------------- */

  const ROOT_KEY = ''; // 左锚为 null 的节点挂在虚拟根下

  class CRDTDoc {
    /**
     * @param {string} [clientId] 本副本的客户端 ID（uuid）；服务端副本也持有一个
     */
    constructor(clientId) {
      this.clientId = clientId || uuid();
      /** 经典标量 Lamport 时钟：本地事件 +1；观测远端事件时 max(local, remote)+1 */
      this.lamport = 0;
      /** 向量时钟：{ clientId -> 已交付的该端序号（稠密计数） } */
      this.vv = {};
      /** 全部节点（含墓碑）：key -> node */
      this.nodes = new Map();
      /** RGA 邻接表：父锚 key（根为 ''） -> 子节点 key[]，按 ID 降序 */
      this.children = new Map();
      /** 缓存的 RGA 线性化结果（DFS 前序），add 后置脏 */
      this._order = null;
      /** 因果缓冲：VV 依赖未满足的 op */
      this.pending = [];
      /** 已合并 op 的 idKey 集合（add / del 的 op.id），幂等去重 */
      this.applied = new Set();
      /** 因果顺序的 op 日志，用于生成新客户端 join 快照 */
      this.log = [];
      this.stats = {
        local: 0, // 本地产生并合并
        remote: 0, // 远端实时合并
        buffered: 0, // 先入 pending、依赖满足后合并
        duplicate: 0, // 重复 op（快照/重传/回环）
        snapshot: 0, // 经快照合并
      };
    }

    /* ---- 本地产生操作（乐观合并，不等待任何确认） ---- */

    /**
     * 本地新增一条 stroke，追加在当前可见序列尾部。
     * 返回线格式 op（由调用方放进 outbox 并发给服务端）。
     */
    localAdd(stroke) {
      this.lamport += 1;
      const vv = vvClone(this.vv);
      vv[this.clientId] = (vv[this.clientId] || 0) + 1;

      const visible = this.visibleStrokes();
      const tail = visible.length > 0 ? visible[visible.length - 1] : null;

      const op = {
        type: 'add',
        id: { clientId: this.clientId, lamport: this.lamport },
        leftOrigin: tail ? { ...tail.id } : null,
        rightOrigin: null,
        color: stroke.color,
        width: stroke.width,
        points: stroke.points.map((p) => ({ x: p.x, y: p.y })),
        vv,
      };
      this.vv = vv;
      this._integrateAdd(op);
      this.applied.add(idKey(op.id));
      this.log.push(op);
      this.stats.local += 1;
      return op;
    }

    /**
     * 本地删除（撤销）一条 stroke：打墓碑。
     * @returns op 或 null（目标不存在 / 已删除）
     */
    localDelete(targetId) {
      const node = this.nodes.get(idKey(targetId));
      if (!node || node.deleted) return null;

      this.lamport += 1;
      const vv = vvClone(this.vv);
      vv[this.clientId] = (vv[this.clientId] || 0) + 1;

      const op = {
        type: 'del',
        id: { clientId: this.clientId, lamport: this.lamport },
        target: { ...targetId },
        vv,
      };
      this.vv = vv;
      node.deleted = true;
      this.applied.add(idKey(op.id));
      this.log.push(op);
      this.stats.local += 1;
      return op;
    }

    /* ---- 远端操作接入（乱序 / 重复 / 并发都从这里进） ---- */

    /**
     * 接收一条远端 op。
     * @returns {{status:'delivered'|'pending'|'duplicate'|'invalid', merged:Array}}
     *          delivered：本次连带（pending 级联）新合并的全部 op
     */
    receive(op) {
      if (!isValidOp(op)) return { status: 'invalid', merged: [] };

      const key = idKey(op.id);
      if (this.applied.has(key)) {
        this.stats.duplicate += 1;
        return { status: 'duplicate', merged: [] };
      }
      if (!vvCanDeliver(this.vv, op)) {
        // 因果依赖未满足：入 pending（缓冲内同 key 去重）
        if (!this.pending.some((p) => idKey(p.id) === key)) this.pending.push(op);
        return { status: 'pending', merged: [] };
      }

      const merged = this._deliver(op, false);
      this._drainPending(merged);
      return { status: 'delivered', merged };
    }

    /** 每合并一条就重扫 pending，找出新满足的，循环到定点（无新增为止）；级联合并进 out */
    _drainPending(out) {
      for (;;) {
        let progressed = false;
        for (let i = 0; i < this.pending.length; i++) {
          const op = this.pending[i];
          if (this.applied.has(idKey(op.id))) {
            this.pending.splice(i, 1);
            i -= 1;
            progressed = true;
            continue;
          }
          if (vvCanDeliver(this.vv, op)) {
            this.pending.splice(i, 1);
            this._deliver(op, true, out);
            i -= 1;
            progressed = true;
          }
        }
        if (!progressed) break;
      }
    }

    _deliver(op, fromPending, out) {
      if (this.applied.has(idKey(op.id))) return out || [];

      // 经典 Lamport：观测到远端事件 max(local, remote)+1；VV 逐维取 max
      this.lamport = Math.max(this.lamport, op.id.lamport) + 1;
      vvMerge(this.vv, op.vv);
      if (op.type === 'add') this._integrateAdd(op);
      else this._integrateDel(op);

      this.applied.add(idKey(op.id));
      this.log.push(op);
      if (fromPending) this.stats.buffered += 1;
      else this.stats.remote += 1;
      if (out) out.push(op);
      return out || [op];
    }

    /* ---- RGA 集成 ---- */

    /**
     * 把 add op 挂进 RGA 树：父锚 = leftOrigin，兄弟按 ID 降序排列。
     * 最终线性序 = 对该树做 DFS 前序，仅由 (锚点, ID) 决定，与集成顺序无关，
     * 因此任意到达顺序（含快照乱序补放）都收敛到同一序列。
     */
    _integrateAdd(op) {
      const parentKey = op.leftOrigin ? idKey(op.leftOrigin) : ROOT_KEY;
      // 因果交付保证锚点已存在；脏数据兜底降级为根锚点
      if (op.leftOrigin && !this.nodes.has(parentKey)) {
        op.leftOrigin = null;
      }
      const node = {
        id: { ...op.id },
        leftOrigin: op.leftOrigin ? { ...op.leftOrigin } : null,
        rightOrigin: op.rightOrigin ? { ...op.rightOrigin } : null,
        color: op.color,
        width: op.width,
        points: op.points.map((p) => ({ x: p.x, y: p.y })),
        deleted: false,
      };
      const key = idKey(node.id);
      this.nodes.set(key, node);

      const siblings = this.children.get(parentKey) || [];
      // 降序插入：新 ID 比当前大就插在它前面
      let i = 0;
      while (i < siblings.length && compareId(this.nodes.get(siblings[i]).id, node.id) > 0) i += 1;
      siblings.splice(i, 0, key);
      this.children.set(parentKey, siblings);
      this._order = null;
    }

    _integrateDel(op) {
      const node = this.nodes.get(idKey(op.target));
      if (node) node.deleted = true;
      // 目标缺失只会发生在丢消息（VV 不应放行）；墓碑事件本身仍记录在 applied/log 中
    }

    /** RGA 线性化：从虚拟根 DFS 前序（父在前、兄弟 ID 大者在前） */
    order() {
      if (this._order) return this._order;
      const out = [];
      const stack = [];
      const pushChildren = (parentKey) => {
        const kids = this.children.get(parentKey);
        if (!kids) return;
        // 栈是 LIFO，逆序压入使弹出顺序 = ID 降序
        for (let i = kids.length - 1; i >= 0; i--) stack.push(kids[i]);
      };
      pushChildren(ROOT_KEY);
      while (stack.length > 0) {
        const key = stack.pop();
        const node = this.nodes.get(key);
        if (!node) continue;
        out.push(node);
        pushChildren(key);
      }
      this._order = out;
      return out;
    }

    /** 可见 stroke（无墓碑），按 RGA 顺序，渲染层直接使用 */
    visibleStrokes() {
      return this.order().filter((n) => !n.deleted);
    }

    /** 撤销目标：RGA 顺序中最后一条本端创建的可见 stroke */
    undoTarget() {
      const order = this.order();
      for (let i = order.length - 1; i >= 0; i--) {
        const n = order[i];
        if (!n.deleted && n.id.clientId === this.clientId) return n;
      }
      return null;
    }

    tombstoneCount() {
      let n = 0;
      for (const node of this.nodes.values()) if (node.deleted) n += 1;
      return n;
    }

    /* ---- 快照（新客户端 join / 重连） ---- */

    snapshot() {
      // log 即因果集成顺序，新副本按序集成本地就构成同一文档
      return { vv: vvClone(this.vv), ops: this.log.map(cloneOp) };
    }

    /**
     * 幂等合并服务端快照：
     *  - 已 applied（含本地 outbox 里的自己的 op）自动跳过；
     *  - 快照内 op 按因果顺序排列，绕过 pending 判定直接集成（锚点必然存在）；
     *  - Lamport 对快照内全部 op 取一次 max（批量观测，不逐条 +1）；
     *  - VV 与服务端快照逐维取 max，再尝试冲刷此前积压的 pending。
     */
    loadSnapshot(snap) {
      if (!snap || !Array.isArray(snap.ops)) return;
      let maxLamport = this.lamport;
      for (const op of snap.ops) {
        if (!isValidOp(op)) continue;
        if (op.id.lamport > maxLamport) maxLamport = op.id.lamport;
        const key = idKey(op.id);
        if (this.applied.has(key)) continue;
        vvMerge(this.vv, op.vv);
        if (op.type === 'add') this._integrateAdd(op);
        else this._integrateDel(op);
        this.applied.add(key);
        this.log.push(op);
        this.stats.snapshot += 1;
      }
      this.lamport = maxLamport;
      if (snap.vv) vvMerge(this.vv, snap.vv);
      this._drainPending();
    }

    /** 供压测断言：与集成顺序无关、只取决于最终文档状态的规范化摘要 */
    stateDigest() {
      return JSON.stringify(
        this.order().map((n) => [
          idKey(n.id),
          n.deleted ? 1 : 0,
          n.color,
          n.width,
          n.points.length,
        ])
      );
    }
  }

  function cloneOp(op) {
    return JSON.parse(JSON.stringify(op));
  }

  return { CRDTDoc, uuid, idKey, sameId, compareId, vvMerge, vvCanDeliver, isValidOp };
});
