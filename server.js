const express = require('express');
const { WebSocketServer } = require('ws');
const { createServer } = require('http');
const { v4: uuid } = require('uuid');
const path = require('path');

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// In-memory state
const sessions = {}; // sessionId -> { panelists, alerts, timer, log }

function getSession(id) {
  if (!sessions[id]) {
    sessions[id] = {
      id,
      panelists: [],
      alerts: {},   // panelistId -> [alert]
      timer: { total: 300, left: 300, running: false, startedAt: null },
      log: [],
    };
  }
  return sessions[id];
}

// WebSocket clients: Map<ws, { sessionId, role, panelistId }>
const clients = new Map();

function broadcast(sessionId, msg, excludeWs = null) {
  for (const [ws, meta] of clients) {
    if (meta.sessionId === sessionId && ws !== excludeWs && ws.readyState === 1) {
      ws.send(JSON.stringify(msg));
    }
  }
}

function broadcastToRole(sessionId, role, msg) {
  for (const [ws, meta] of clients) {
    if (meta.sessionId === sessionId && meta.role === role && ws.readyState === 1) {
      ws.send(JSON.stringify(msg));
    }
  }
}

function broadcastToPanelist(sessionId, panelistId, msg) {
  for (const [ws, meta] of clients) {
    if (meta.sessionId === sessionId && meta.panelistId === panelistId && ws.readyState === 1) {
      ws.send(JSON.stringify(msg));
    }
  }
}

function getOnlineIds(sessionId) {
  const ids = new Set();
  for (const [, meta] of clients) {
    if (meta.sessionId === sessionId && meta.panelistId) ids.add(meta.panelistId);
  }
  return ids;
}

function fullState(session) {
  const onlineIds = getOnlineIds(session.id);
  return {
    type: 'STATE',
    panelists: session.panelists.map(p => ({ ...p, online: onlineIds.has(p.id) })),
    alerts: session.alerts,
    timer: session.timer,
    log: session.log.slice(0, 50),
  };
}

// Timer tick logic
const timerIntervals = {};

function startTimerInterval(sessionId) {
  if (timerIntervals[sessionId]) return;
  timerIntervals[sessionId] = setInterval(() => {
    const s = sessions[sessionId];
    if (!s || !s.timer.running) { clearInterval(timerIntervals[sessionId]); delete timerIntervals[sessionId]; return; }
    if (s.timer.left <= 0) {
      s.timer.left = 0;
      s.timer.running = false;
      clearInterval(timerIntervals[sessionId]);
      delete timerIntervals[sessionId];
      autoAlert(sessionId, "Time's up!", "🔴 Your time has ended. Please wrap up now.", 'danger');
    } else {
      s.timer.left--;
      if (s.timer.left === 300) autoAlert(sessionId, '5 min left', '⏰ You have 5 minutes remaining.', 'warning');
      if (s.timer.left === 60) autoAlert(sessionId, '1 min left', '⚡ One minute left — please wrap up.', 'warning');
    }
    broadcast(sessionId, { type: 'TIMER', timer: s.timer });
  }, 1000);
}

function autoAlert(sessionId, label, msg, level) {
  const s = sessions[sessionId];
  if (!s) return;
  const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  s.panelists.forEach(p => {
    if (!s.alerts[p.id]) s.alerts[p.id] = [];
    const alert = { id: uuid(), label, msg, level, time, auto: true };
    s.alerts[p.id].unshift(alert);
    s.log.unshift({ who: p.name, label, time, level });
    broadcastToPanelist(sessionId, p.id, { type: 'ALERT', panelistId: p.id, alert });
  });
  broadcastToRole(sessionId, 'moderator', { type: 'LOG_UPDATE', log: s.log.slice(0, 50), alerts: s.alerts });
}

wss.on('connection', (ws) => {
  clients.set(ws, { sessionId: null, role: null, panelistId: null });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const meta = clients.get(ws);

    switch (msg.type) {

      case 'JOIN': {
        const sessionId = msg.sessionId || uuid().slice(0, 8).toUpperCase();
        const session = getSession(sessionId);
        meta.sessionId = sessionId;
        meta.role = msg.role;
        if (msg.role === 'panelist' && msg.panelistId) meta.panelistId = msg.panelistId;
        clients.set(ws, meta);
        ws.send(JSON.stringify({ type: 'JOINED', sessionId, ...fullState(session) }));
        // Notify everyone of presence change
        broadcast(sessionId, fullState(session));
        break;
      }

      case 'ADD_PANELIST': {
        const s = getSession(meta.sessionId);
        const p = { id: uuid().slice(0, 8), name: msg.name, email: msg.email || '', color: msg.color };
        s.panelists.push(p);
        s.alerts[p.id] = [];
        broadcast(meta.sessionId, fullState(s));
        break;
      }

      case 'REMOVE_PANELIST': {
        const s = getSession(meta.sessionId);
        s.panelists = s.panelists.filter(p => p.id !== msg.panelistId);
        delete s.alerts[msg.panelistId];
        broadcast(meta.sessionId, fullState(s));
        break;
      }

      case 'BUZZ_ONE': {
        const s = getSession(meta.sessionId);
        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        const alert = { id: uuid(), label: msg.label, msg: msg.msg, level: msg.level || 'info', time, auto: false };
        if (!s.alerts[msg.panelistId]) s.alerts[msg.panelistId] = [];
        s.alerts[msg.panelistId].unshift(alert);
        const p = s.panelists.find(x => x.id === msg.panelistId);
        if (p) s.log.unshift({ who: p.name, label: msg.label, time, level: alert.level });
        broadcastToPanelist(meta.sessionId, msg.panelistId, { type: 'ALERT', panelistId: msg.panelistId, alert });
        broadcastToRole(meta.sessionId, 'moderator', { type: 'LOG_UPDATE', log: s.log.slice(0, 50), alerts: s.alerts });
        break;
      }

      case 'BUZZ_ALL': {
        const s = getSession(meta.sessionId);
        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        s.panelists.forEach(p => {
          const alert = { id: uuid(), label: msg.label, msg: msg.msg, level: msg.level || 'info', time, auto: false };
          if (!s.alerts[p.id]) s.alerts[p.id] = [];
          s.alerts[p.id].unshift(alert);
          s.log.unshift({ who: p.name, label: msg.label, time, level: alert.level });
          broadcastToPanelist(meta.sessionId, p.id, { type: 'ALERT', panelistId: p.id, alert });
        });
        broadcastToRole(meta.sessionId, 'moderator', { type: 'LOG_UPDATE', log: s.log.slice(0, 50), alerts: s.alerts });
        break;
      }

      case 'ACK_ALERT': {
        const s = getSession(meta.sessionId);
        if (s.alerts[msg.panelistId]) {
          s.alerts[msg.panelistId] = s.alerts[msg.panelistId].filter(a => a.id !== msg.alertId);
        }
        broadcastToRole(meta.sessionId, 'moderator', { type: 'LOG_UPDATE', log: s.log.slice(0, 50), alerts: s.alerts });
        ws.send(JSON.stringify({ type: 'ALERTS_UPDATE', panelistId: msg.panelistId, alerts: s.alerts[msg.panelistId] || [] }));
        break;
      }

      case 'TIMER_SET': {
        const s = getSession(meta.sessionId);
        clearInterval(timerIntervals[meta.sessionId]);
        delete timerIntervals[meta.sessionId];
        s.timer = { total: msg.secs, left: msg.secs, running: false };
        broadcast(meta.sessionId, { type: 'TIMER', timer: s.timer });
        break;
      }

      case 'TIMER_TOGGLE': {
        const s = getSession(meta.sessionId);
        if (s.timer.left <= 0) break;
        s.timer.running = !s.timer.running;
        if (s.timer.running) startTimerInterval(meta.sessionId);
        else { clearInterval(timerIntervals[meta.sessionId]); delete timerIntervals[meta.sessionId]; }
        broadcast(meta.sessionId, { type: 'TIMER', timer: s.timer });
        break;
      }

      case 'TIMER_RESET': {
        const s = getSession(meta.sessionId);
        clearInterval(timerIntervals[meta.sessionId]);
        delete timerIntervals[meta.sessionId];
        s.timer = { total: s.timer.total, left: s.timer.total, running: false };
        broadcast(meta.sessionId, { type: 'TIMER', timer: s.timer });
        break;
      }
    }
  });

  ws.on('close', () => {
    const meta = clients.get(ws);
    clients.delete(ws);
    if (meta?.sessionId) {
      const s = sessions[meta.sessionId];
      if (s) broadcast(meta.sessionId, fullState(s));
    }
  });
});

// REST: create a new session
app.post('/api/session', (req, res) => {
  const id = uuid().slice(0, 8).toUpperCase();
  getSession(id);
  res.json({ sessionId: id });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Buzzr running on http://localhost:${PORT}`));
