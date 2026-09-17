/**
 * 直播白板 · CRDT 协同 —— 前端
 * 原生 HTML/CSS/JS + Canvas
 *
 * 核心机制：
 *  1. Pointer Events 统一鼠标 / 触控笔 / 触摸；quadraticCurveTo 中点插值平滑
 *  2. 本地状态即 CRDT 文档（RGA 有序链表 + 墓碑）：落笔即合并进本地文档，
 *     不等待任何服务端确认；渲染层按 RGA 顺序读取可见 stroke 绘制
 *  3. 每个 op 携带 { clientId, lamport } 与向量时钟；断线期间 op 进入
 *     outbox，重连后批量补发，服务端 CRDT 合并后再广播
 *  4. 撤销 = 对自己最近一条 stroke 打墓碑（del op），并发新增互不影响
 *  5. 调试面板：VV / lamport / pending / 合并次数 / 墓碑数 / outbox
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
  const debugToggle = document.getElementById('debug-toggle');
  const debugPanel = document.getElementById('debug-panel');
  const dbgClient = document.getElementById('dbg-client');
  const dbgLamport = document.getElementById('dbg-lamport');
  const dbgVV = document.getElementById('dbg-vv');
  const dbgNodes = document.getElementById('dbg-nodes');
  const dbgTombs = document.getElementById('dbg-tombs');
  const dbgPending = document.getElementById('dbg-pending');
  const dbgMerges = document.getElementById('dbg-merges');
  const dbgDups = document.getElementById('dbg-dups');
  const dbgOutbox = document.getElementById('dbg-outbox');
  const container = document.getElementById('canvas-container');
  const canvas = document.getElementById('board');

  const ctx = canvas.getContext('2d');

  /* ================= 状态 ================= */
  // userId 仅作展示身份，持久化；CRDT 节点身份 clientId 每次页面加载生成新 uuid
  // （若复用旧 clientId 而不持久化向量时钟，重发的 op id 会与历史冲突）
  const userId =
    localStorage.getItem('wb-userId') ||
    (() => {
      const id = 'u-' + Math.random().toString(36).slice(2, 10);
      localStorage.setItem('wb-userId', id);
      return id;
    })();
  const clientId = StrokeCRDT.uuid();

  // 本地状态 = CRDT 文档（Map<strokeId, Stroke> + RGA 有序链表 + 因果缓冲）
  const doc = new StrokeCRDT(clientId);
  const outbox = []; // 断线期间本地产生的 op，重连后批量补发

  let roomId = null;

  // 双缓冲：离屏 Canvas 承载已提交的 CRDT 可见内容
  const historyCanvas = document.createElement('canvas');
  const historyCtx = historyCanvas.getContext('2d');

  let cssW = 0; // 画布 CSS 尺寸（坐标归一化的基准）
  let cssH = 0;

  // 当前正在书写的笔迹（只画在主 Canvas 上，pointerup 后才成为 CRDT op）
  let currentStroke = null; // { color, width, points: [{x,y}] }
  let drawing = false;
  let activePointerId = null;

  let currentColor = '#1a1a1a';
  let currentWidthPx = 4; // 线宽（CSS px，发送时按画布宽度归一化）

  /* ================= WebSocket 连接管理 ================= */
  let ws = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let joinAcked = false; // 收到 joined 后才允许发送 op

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
      joinAcked = false;
      // 加入房间并带上本地向量时钟，服务端只回传缺失的 op（VV 差量）
      send({ type: 'join', roomId, userId, clientId, vv: doc.vv });
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
      joinAcked = false;
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

  function send(obj) {
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
        joinAcked = true;
        // 应用 CRDT 快照（新加入=全量，重连=按 VV 差量）；op 按因果序到达，直接可合并
        const snap = msg.snapshot || { ops: [] };
        for (const op of snap.ops || []) {
          doc.apply(op);
        }
        renderAll();
        updateDebug();
        updatePresence(msg.count);
        // 重连后批量补发服务端缺失的「自己的」op（含 outbox；服务端重启丢状态也能恢复）
        resyncOwnOps(snap.vv);
        break;
      }
      case 'op': {
        // 远端 op：因果合并（乱序自动进 pending，重复幂等丢弃）
        const merged = doc.apply(msg.op);
        if (merged.length) scheduleRender();
        updateDebug();
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
   * 把服务端状态向量尚未覆盖的「自己的」op 批量补发。
   * outbox 中的 op 必然包含在 missing 里，统一走批量通道。
   */
  function resyncOwnOps(serverVV) {
    const covered = (serverVV && serverVV[clientId]) || 0;
    const missing = doc.opLog.filter(
      (op) => op.id.client === clientId && (op.vv[clientId] || 0) > covered
    );
    outbox.length = 0;
    if (missing.length && ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'ops', roomId, ops: missing }));
    }
  }

  /** 本地 op 发送：在线直发，断线进 outbox 等重连补发 */
  function sendOp(op) {
    if (!(joinAcked && send({ type: 'op', roomId, op }))) {
      outbox.push(op);
    }
    updateDebug();
  }

  /* ================= 渲染：从 CRDT 文档读取可见 stroke ================= */

  /** 全量重绘历史层：RGA 顺序可能因并发合并改变，墓碑需要擦除，故整体重画 */
  function renderAll() {
    historyCtx.save();
    historyCtx.setTransform(1, 0, 0, 1, 0, 0);
    historyCtx.clearRect(0, 0, historyCanvas.width, historyCanvas.height);
    historyCtx.restore();
    for (const node of doc.visible()) {
      drawStroke(historyCtx, node.stroke);
    }
    composite();
  }

  // 远端 op 高频到达时用 rAF 合帧，避免每 op 全量重绘
  let renderScheduled = false;
  function scheduleRender() {
    if (renderScheduled) return;
    renderScheduled = true;
    requestAnimationFrame(() => {
      renderScheduled = false;
      renderAll();
    });
  }

  /** 主 Canvas = 离屏历史 + 当前正在书写的笔迹 */
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

    // 尺寸变化后按归一化坐标重绘 CRDT 可见内容，避免拉伸变形
    renderAll();
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
    // 本地预提交：落笔瞬间立即渲染，不等服务端
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
    // 实时显示当前笔迹（主 Canvas 只画当前笔迹 + 离屏历史）
    composite();
  });

  function finishStroke(e) {
    if (!drawing || (e && e.pointerId !== activePointerId)) return;
    drawing = false;
    activePointerId = null;

    const stroke = currentStroke;
    currentStroke = null;
    if (!stroke || stroke.points.length === 0) return;

    // 1. 合并进本地 CRDT 文档（立即生效，不等待任何服务端确认）
    const op = doc.addStroke(stroke);
    renderAll();
    updateDebug();

    // 2. 广播 op；断线则进 outbox 等重连批量补发
    sendOp(op);
  }

  canvas.addEventListener('pointerup', finishStroke);
  canvas.addEventListener('pointercancel', finishStroke);

  /* ================= 撤销（墓碑化自己最近一笔） ================= */
  function doUndo() {
    const op = doc.undo(); // 只影响自己的目标 stroke，并发新增是独立节点
    if (!op) return;
    renderAll();
    updateDebug();
    sendOp(op);
  }

  undoBtn.addEventListener('click', doUndo);
  window.addEventListener('keydown', (e) => {
    if (boardScreen.classList.contains('hidden')) return;
    if ((e.ctrlKey || e.metaKey) && (e.key === 'z' || e.key === 'Z')) {
      e.preventDefault();
      doUndo();
    }
  });

  /* ================= 调试面板 ================= */
  debugToggle.addEventListener('click', () => {
    debugPanel.classList.toggle('hidden');
    updateDebug();
  });

  function updateDebug() {
    if (debugPanel.classList.contains('hidden')) return;
    const s = doc.stats();
    dbgClient.textContent = s.clientId.slice(0, 8);
    dbgLamport.textContent = String(s.lamport);
    dbgVV.textContent =
      Object.keys(s.vv).length === 0
        ? '(空)'
        : Object.entries(s.vv)
            .map(([k, v]) => `${k.slice(0, 8)} → ${v}`)
            .join('\n');
    dbgNodes.textContent = String(s.nodes);
    dbgTombs.textContent = String(s.tombstones);
    dbgPending.textContent = String(s.pending);
    dbgMerges.textContent = String(s.mergeCount);
    dbgDups.textContent = String(s.dupCount);
    dbgOutbox.textContent = String(outbox.length);
  }

  setInterval(updateDebug, 1000); // 低频兜底刷新

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
    setStatus('reconnecting');
    connect();
  }

  joinBtn.addEventListener('click', joinRoom);
  roomInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') joinRoom();
  });
  roomInput.focus();
})();
