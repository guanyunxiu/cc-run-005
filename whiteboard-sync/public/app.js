/**
 * 直播白板 CRDT 协同 —— 前端
 * 原生 HTML/CSS/JS + Canvas
 *
 * 核心机制（CRDT 版）：
 *  1. 本地状态是一份 CRDT 文档（RGA 有序链表 + 墓碑 + 向量时钟），见 crdt.js
 *  2. Pointer Events 采集；quadraticCurveTo 三点中点插值平滑
 *  3. 本地乐观合并：落笔即渲染，pointerup 产生带 {clientId,lamport}+VV 的 add op，
 *     不等待任何服务端确认；op 同时进入 outbox
 *  4. 离线编辑：断线期间继续画，op 攒在 outbox；重连 join 拿快照幂等合并后批量重发
 *  5. 乱序到达：VV 依赖未满足的 op 进 pending，每合并一条重扫，收敛后自动应用
 *  6. 撤销 = del op（墓碑），只影响目标 stroke，并发新增不受影响
 *  7. 渲染层从 CRDT 文档按 RGA 顺序读取可见 stroke（墓碑触发全量重绘）
 */
'use strict';

(() => {
  /* ================= DOM ================= */
  const joinScreen = document.getElementById('join-screen');
  const boardScreen = document.getElementById('board-screen');
  const roomInput = document.getElementById('room-input');
  const joinBtn = document.getElementById('join-btn');
  const roomLabel = document.getElementById('room-label');
  const statusEl = document.getElementById('status');
  const statusText = document.getElementById('status-text');
  const presenceEl = document.getElementById('presence');
  const widthSlider = document.getElementById('width-slider');
  const widthValue = document.getElementById('width-value');
  const undoBtn = document.getElementById('undo-btn');
  const container = document.getElementById('canvas-container');
  const canvas = document.getElementById('board');

  // 调试面板
  const dbgClient = document.getElementById('dbg-client');
  const dbgVV = document.getElementById('dbg-vv');
  const dbgStrokes = document.getElementById('dbg-strokes');
  const dbgPending = document.getElementById('dbg-pending');
  const dbgMerged = document.getElementById('dbg-merged');
  const dbgTomb = document.getElementById('dbg-tomb');
  const dbgOutbox = document.getElementById('dbg-outbox');
  const dbgToggle = document.getElementById('dbg-toggle');
  const dbgBody = document.getElementById('dbg-body');

  const ctx = canvas.getContext('2d');

  /* ================= 身份与 CRDT 文档 ================= */
  // clientId（uuid）持久化，刷新页面后仍是同一副本，历史 op 归属不变
  const clientId =
    localStorage.getItem('wb-clientId') ||
    (() => {
      const id = CRDT.uuid();
      localStorage.setItem('wb-clientId', id);
      return id;
    })();

  // 本地 CRDT 文档：所有渲染状态的唯一来源（浏览器全局命名空间为 CRDT，见 crdt.js）
  const document_ = new CRDT.CRDTDoc(clientId);

  let roomId = null;

  // 双缓冲：离屏 Canvas 承载已提交的可见笔迹
  const historyCanvas = document.createElement('canvas');
  const historyCtx = historyCanvas.getContext('2d');

  let cssW = 0; // 画布 CSS 尺寸（坐标归一化的基准）
  let cssH = 0;

  // 当前正在书写的笔迹（尚未提交，只画在主 Canvas 上）
  let currentStroke = null; // { color, width, points: [{x,y}] }
  let drawing = false;
  let activePointerId = null;

  let currentColor = '#1a1a1a';
  let currentWidthPx = 4; // 线宽（CSS px，发送时按画布宽度归一化）

  /* ================= WebSocket 连接管理 ================= */
  let ws = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let joined = false; // 收到 joined 快照后才允许发送操作
  // outbox：所有本地已产生的 op（含在线/离线）。重连拿到快照后整体批量重发，幂等去重
  const outbox = [];

  function setStatus(state) {
    // state: 'connected' | 'reconnecting' | 'offline'
    statusEl.className = 'status status-' + state;
    statusText.textContent =
      state === 'connected' ? '已连接' : state === 'reconnecting' ? '重连中' : '离线';
  }

  function connect() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws`);

    ws.onopen = () => {
      reconnectAttempts = 0;
      setStatus('connected');
      joined = false;
      // join 携带 clientId 与当前 VV（服务端目前发全量快照，客户端幂等合并）
      sendRaw({ type: 'join', roomId, clientId, vv: document_.vv });
    };

    ws.onmessage = (e) => {
      let msg;
      try {
        msg = JSON.parse(e.data);
      } catch {
        return;
      }
      handleMessage(msg);
    };

    ws.onclose = () => {
      joined = false;
      setStatus(navigator.onLine ? 'reconnecting' : 'offline');
      scheduleReconnect();
    };

    ws.onerror = () => {
      ws.close();
    };
  }

  /** 指数退避重连：1s, 2s, 4s, ... 上限 15s */
  function scheduleReconnect() {
    clearTimeout(reconnectTimer);
    const delay = Math.min(15000, 1000 * 2 ** reconnectAttempts);
    reconnectAttempts += 1;
    reconnectTimer = setTimeout(connect, delay);
  }

  window.addEventListener('online', () => {
    reconnectAttempts = 0;
    clearTimeout(reconnectTimer);
    connect();
  });
  window.addEventListener('offline', () => setStatus('offline'));

  function sendRaw(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
      return true;
    }
    return false;
  }

  /* ================= 消息处理 ================= */
  function handleMessage(msg) {
    switch (msg.type) {
      case 'joined': {
        joined = true;
        // 1. 幂等合并服务端全量 CRDT 快照（自己 outbox 里已合并的 op 自动跳过）
        document_.loadSnapshot(msg.snapshot);
        // 2. 重连后批量重发 outbox（服务端按 op.id 幂等去重）
        flushOutbox();
        updatePresence(msg.count);
        refresh();
        break;
      }
      case 'add':
      case 'del': {
        // 单条远端 op（线格式本身的 type 即 add/del；含服务端 pending 级联广播）
        // 乱序/缺依赖时 doc 内部入 pending，依赖满足后自动级联合并
        document_.receive(msg);
        refresh();
        break;
      }
      case 'ops': {
        // 批量（服务端一条 op 级联冲出多条 pending 时推送）
        for (const op of msg.ops) document_.receive(op);
        refresh();
        break;
      }
      case 'presence':
        updatePresence(msg.count);
        break;
      case 'error':
        console.error('服务端错误:', msg.message);
        break;
    }
  }

  function updatePresence(count) {
    if (typeof count === 'number') {
      presenceEl.textContent = `在线 ${count} 人`;
    }
  }

  /**
   * outbox 批量发送：重连拿到快照后，只重发快照里尚未包含的本地 op
   * （已被服务端确认的直接丢弃），避免每次重连重传全部历史。
   * 服务端对重传仍幂等，这里只是省带宽。
   */
  function flushOutbox() {
    if (outbox.length === 0 || !joined || !ws || ws.readyState !== WebSocket.OPEN) return;
    const unconfirmed = outbox.filter((op) => !document_.applied.has(CRDT.idKey(op.id)));
    outbox.length = 0;
    outbox.push(...unconfirmed);
    if (unconfirmed.length > 0) {
      sendRaw({ type: 'ops', ops: unconfirmed });
    }
  }

  /* ================= 渲染（从 CRDT 文档读取） ================= */

  /** 文档变更后：按 RGA 顺序全量重绘已提交可见笔迹 + 调试面板 */
  function refresh() {
    replayHistory();
    updateDebugPanel();
  }

  /** 主 Canvas = 离屏历史（RGA 可见笔迹）+ 当前正在书写的笔迹 */
  function composite() {
    ctx.save();
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(historyCanvas, 0, 0);
    ctx.restore();
    if (currentStroke) {
      drawStroke(ctx, currentStroke);
    }
  }

  /**
   * 贝塞尔平滑笔迹：
   * 不用 lineTo 折线；以相邻点中点为终点、中间点为控制点做 quadraticCurveTo，
   * 三点插值平滑，消除折线感。
   */
  function drawStroke(targetCtx, stroke) {
    const pts = stroke.points;
    if (!pts || pts.length === 0) return;

    const px = pts.map((p) => ({ x: p.x * cssW, y: p.y * cssH }));
    const lineWidth = Math.max(1, stroke.width * cssW);

    targetCtx.save();
    targetCtx.strokeStyle = stroke.color;
    targetCtx.fillStyle = stroke.color;
    targetCtx.lineWidth = lineWidth;
    targetCtx.lineCap = 'round';
    targetCtx.lineJoin = 'round';

    if (px.length === 1) {
      // 单点：画圆点
      targetCtx.beginPath();
      targetCtx.arc(px[0].x, px[0].y, lineWidth / 2, 0, Math.PI * 2);
      targetCtx.fill();
    } else if (px.length === 2) {
      targetCtx.beginPath();
      targetCtx.moveTo(px[0].x, px[0].y);
      targetCtx.lineTo(px[1].x, px[1].y);
      targetCtx.stroke();
    } else {
      targetCtx.beginPath();
      targetCtx.moveTo(px[0].x, px[0].y);
      // 中间点作为控制点，相邻中点作为曲线终点
      for (let i = 1; i < px.length - 1; i++) {
        const midX = (px[i].x + px[i + 1].x) / 2;
        const midY = (px[i].y + px[i + 1].y) / 2;
        targetCtx.quadraticCurveTo(px[i].x, px[i].y, midX, midY);
      }
      // 收尾到最后一个真实点，保证跟手
      const last = px[px.length - 1];
      targetCtx.lineTo(last.x, last.y);
      targetCtx.stroke();
    }
    targetCtx.restore();
  }

  /* ================= 画布尺寸（支持 devicePixelRatio 与窗口缩放） ================= */
  function resizeCanvas() {
    const rect = container.getBoundingClientRect();
    cssW = rect.width;
    cssH = rect.height;
    const dpr = window.devicePixelRatio || 1;

    for (const c of [canvas, historyCanvas]) {
      c.width = Math.round(cssW * dpr);
      c.height = Math.round(cssH * dpr);
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    historyCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // 尺寸变化后按归一化坐标从 CRDT 文档重放，避免拉伸变形
    replayHistory();
  }

  /** 从 CRDT 文档重放：清空离屏画布，按 RGA 顺序绘制所有非墓碑 stroke */
  function replayHistory() {
    historyCtx.save();
    historyCtx.setTransform(1, 0, 0, 1, 0, 0);
    historyCtx.clearRect(0, 0, historyCanvas.width, historyCanvas.height);
    historyCtx.restore();
    for (const node of document_.order()) {
      if (!node.deleted) drawStroke(historyCtx, node);
    }
    composite();
  }

  window.addEventListener('resize', resizeCanvas);

  /* ================= 笔迹采集（Pointer Events） ================= */

  /** 归一化坐标：与设备分辨率无关，保证多端画面一致 */
  function toNormalized(e) {
    const rect = canvas.getBoundingClientRect();
    return {
      x: (e.clientX - rect.left) / rect.width,
      y: (e.clientY - rect.top) / rect.height,
    };
  }

  canvas.addEventListener('pointerdown', (e) => {
    if (drawing) return;
    e.preventDefault();
    canvas.setPointerCapture(e.pointerId);
    activePointerId = e.pointerId;
    drawing = true;

    currentStroke = {
      color: currentColor,
      width: currentWidthPx / cssW, // 归一化线宽
      points: [toNormalized(e)],
    };
    // 本地乐观渲染：落笔瞬间立即显示，不等服务端
    composite();
  });

  canvas.addEventListener('pointermove', (e) => {
    if (!drawing || e.pointerId !== activePointerId) return;
    e.preventDefault();
    // getCoalescedEvents 获取高频采样点，快速移动时更平滑
    const events =
      typeof e.getCoalescedEvents === 'function' && e.getCoalescedEvents().length > 0
        ? e.getCoalescedEvents()
        : [e];
    for (const ev of events) {
      currentStroke.points.push(toNormalized(ev));
    }
    composite();
  });

  function finishStroke(e) {
    if (!drawing || (e && e.pointerId !== activePointerId)) return;
    drawing = false;
    activePointerId = null;

    const stroke = currentStroke;
    currentStroke = null;
    if (!stroke || stroke.points.length === 0) return;

    // 1. 乐观合并进本地 CRDT 文档（lamport+1、生成 op.id 与 VV、RGA 追加到尾部）
    const op = document_.localAdd(stroke);
    // 2. 进入 outbox：在线实时发；离线攒着重连批量发；不等待任何确认
    outbox.push(op);
    if (joined && ws && ws.readyState === WebSocket.OPEN) {
      sendRaw(op);
    }
    refresh();
  }

  canvas.addEventListener('pointerup', finishStroke);
  canvas.addEventListener('pointercancel', finishStroke);

  /* ================= 撤销（del op = 墓碑） ================= */

  undoBtn.addEventListener('click', () => {
    const target = document_.undoTarget(); // RGA 顺序中最后一条本端可见 stroke
    if (!target) return;
    const op = document_.localDelete(target.id);
    if (!op) return;
    outbox.push(op);
    if (joined && ws && ws.readyState === WebSocket.OPEN) {
      sendRaw(op);
    }
    refresh();
  });

  /* ================= 调试面板 ================= */

  // VV 按 clientId 排序展示，避免键顺序抖动
  function formatVV(vv) {
    const keys = Object.keys(vv).sort();
    if (keys.length === 0) return '{}';
    return (
      '{' +
      keys
        .map((k) => `${k.slice(0, 8)}…:${vv[k]}`)
        .join(', ') +
      '}'
    );
  }

  function updateDebugPanel() {
    dbgClient.textContent = clientId.slice(0, 8) + '…';
    dbgVV.textContent = formatVV(document_.vv);
    dbgStrokes.textContent = document_.visibleStrokes().length;
    dbgPending.textContent = document_.pending.length;
    dbgMerged.textContent =
      document_.stats.local + document_.stats.remote + document_.stats.buffered + document_.stats.snapshot;
    dbgTomb.textContent = document_.tombstoneCount();
    dbgOutbox.textContent = outbox.length;
  }

  dbgToggle.addEventListener('click', () => {
    const collapsed = dbgBody.classList.toggle('collapsed');
    dbgToggle.textContent = collapsed ? '▸' : '▾';
  });

  /* ================= 工具栏 ================= */
  document.querySelectorAll('.color-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.color-btn').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      currentColor = btn.dataset.color;
    });
  });

  widthSlider.addEventListener('input', () => {
    currentWidthPx = Number(widthSlider.value);
    widthValue.textContent = widthSlider.value;
  });

  /* ================= 加入房间 ================= */
  function joinRoom() {
    const id = roomInput.value.trim();
    if (!id) {
      roomInput.focus();
      return;
    }
    roomId = id;
    roomLabel.textContent = `房间：${roomId}`;
    joinScreen.classList.add('hidden');
    boardScreen.classList.remove('hidden');
    resizeCanvas();
    updateDebugPanel();
    setStatus('reconnecting');
    connect();
  }

  joinBtn.addEventListener('click', joinRoom);
  roomInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') joinRoom();
  });
  roomInput.focus();
})();
