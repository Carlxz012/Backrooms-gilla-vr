// Servidor: sirve el juego (index.html) y hace de "sala" multijugador por WebSocket.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  // Solo se sirve el juego (index.html) para cualquier ruta
  fs.readFile(INDEX, (e, d) => {
    if (e) { res.writeHead(500); res.end('Falta index.html junto a server.js'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(d);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 16384 });
const rooms = new Map(); // codigo -> { clients: Map(id -> {ws, pr}), dirty, code }
let counter = 0;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

function broadcast(room) {
  const p = {};
  for (const [id, c] of room.clients) p[id] = c.pr;
  const msg = JSON.stringify({ t: 'st', p });
  for (const c of room.clients.values()) if (c.ws.readyState === 1) c.ws.send(msg);
  room.dirty = false;
}

wss.on('connection', (ws) => {
  let room = null, id = null, msgs = 0;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const rl = setInterval(() => { msgs = 0; }, 1000);

  ws.on('message', (raw) => {
    if (++msgs > 80) return; // limite de mensajes por segundo
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join' && !room) {
      const code = String(m.code || '');
      if (!/^\d{6}$/.test(code)) return send(ws, { t: 'err', m: 'Código no válido' });
      let r = rooms.get(code);
      if (!r) {
        if (!m.create) return send(ws, { t: 'err', m: 'Ese lobby no existe' });
        r = { clients: new Map(), dirty: false, code };
        rooms.set(code, r);
      }
      if (r.clients.size >= 20) return send(ws, { t: 'err', m: 'Lobby lleno (máx. 20 jugadores)' });
      id = 'p' + String(++counter).padStart(7, '0'); // el id mas bajo = el que entro primero = anfitrion
      r.clients.set(id, { ws, pr: {} });
      room = r;
      send(ws, { t: 'joined', id });
      r.dirty = true;
    } else if (m.t === 'sig' && room && typeof m.to === 'string' && m.d && typeof m.d === 'object') {
      const c = room.clients.get(m.to);
      if (c) send(c.ws, { t: 'sig', from: id, d: m.d });
    } else if (m.t === 'pr' && room && m.p && typeof m.p === 'object') {
      const c = room.clients.get(id); if (!c) return;
      for (const k of Object.keys(m.p).slice(0, 24)) {
        if (!/^[A-Za-z_][A-Za-z0-9_]{0,15}$/.test(k)) continue;
        const v = m.p[k];
        if (v === null) delete c.pr[k]; else c.pr[k] = v;
      }
      if (JSON.stringify(c.pr).length > 8000) c.pr = {};
      room.dirty = true;
    }
  });

  ws.on('close', () => {
    clearInterval(rl);
    if (room) {
      room.clients.delete(id); room.dirty = true;
      if (!room.clients.size) rooms.delete(room.code);
    }
  });
});

setInterval(() => { for (const r of rooms.values()) if (r.dirty) broadcast(r); }, 50);
setInterval(() => {
  wss.clients.forEach((ws) => { if (ws.isAlive === false) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch (e) {} });
}, 30000);

server.listen(PORT, () => console.log('Backrooms: El Gorila escuchando en el puerto ' + PORT));
