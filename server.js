// ============================================================
// МУЛЬТИПЛЕЕР-СЕРВЕР «НЕБО: ПВО / ДРОН»
// Node.js + Express + Socket.io
//  - отдаёт index.html (и весь статик) при заходе на сайт
//  - ретранслирует координаты/повороты игроков (~10 Гц)
//  - ретранслирует выстрелы (трассеры у других клиентов)
//  - передаёт попадания между игроками (урон в PvP)
// Запуск:  npm install && npm start  ->  http://localhost:3000
// ============================================================
const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { pingInterval: 20000, pingTimeout: 25000 });

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 8;          // слотов достаточно: рассчитано на 2 игроков
const NUM_POOL = Array.from({ length: MAX_PLAYERS }, (_, i) => i + 1); // номера «ИГРОК 1..8»

// ---------- статика: index.html открывается на / ----------
app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ---------- состояние игроков ----------
const players = new Map(); // socket.id -> { num, state, lastSeen }
const freeNums = [...NUM_POOL];
const STALE_MS = 90000; // страховка: соединения без событий дольше 90 с принудительно закрываются

setInterval(() => {
  const now = Date.now();
  for (const [id, p] of players) {
    if (now - p.lastSeen > STALE_MS) {
      const s = io.sockets.sockets.get(id);
      if (s) s.disconnect(true);
      else { players.delete(id); if (!freeNums.includes(p.num)) freeNums.push(p.num); freeNums.sort((a, b) => a - b); io.emit('leave', { id }); }
    }
  }
}, 20000).unref();

const clampN = (v, min = -12000, max = 12000) => {
  v = Number(v);
  return Number.isFinite(v) ? Math.max(min, Math.min(max, v)) : 0;
};
const safeDir = v => clampN(v, -1, 1);

io.on('connection', socket => {
  if (players.size >= MAX_PLAYERS) {
    socket.emit('full');
    socket.disconnect(true);
    return;
  }
  const num = freeNums.length ? freeNums.shift() : players.size + 1;
  players.set(socket.id, { num, state: { on: false, mode: 'kr', hp: 100, hpMax: 100, x: 0, y: 0, z: 0, ry: 0, rx: 0, rz: 0 }, lastSeen: Date.now() });

  // позднему игроку сразу отправляем всех, кто уже в бою
  const others = [...players.entries()]
    .filter(([id]) => id !== socket.id)
    .map(([id, p]) => ({ id, num: p.num, s: p.state }));
  socket.emit('init', { id: socket.id, num, players: others });
  socket.broadcast.emit('join', { id: socket.id, num });
  console.log(`[+] Игрок ${num} подключился (${socket.id}) · онлайн: ${players.size}`);

  // координаты и повороты игрока (клиент шлёт ~10 раз в секунду)
  socket.on('state', s => {
    const p = players.get(socket.id);
    if (!p || !s || typeof s !== 'object') return;
    p.lastSeen = Date.now();
    p.state = {
      on: !!s.on,
      mode: String(s.mode || 'kr').slice(0, 8),
      hp: clampN(s.hp, 0, 999),
      hpMax: clampN(s.hpMax, 1, 999),
      x: clampN(s.x), y: clampN(s.y), z: clampN(s.z),
      ry: clampN(s.ry), rx: clampN(s.rx), rz: clampN(s.rz)
    };
    // ретрансляция всем остальным
    socket.broadcast.emit('state', { id: socket.id, num: p.num, s: p.state });
  });

  // выстрел: позиция стволов + направление, чтобы у других игроков трассер был честным
  socket.on('shoot', d => {
    if (!players.has(socket.id) || !d || typeof d !== 'object') return;
    players.get(socket.id).lastSeen = Date.now();
    socket.broadcast.emit('shoot', {
      id: socket.id,
      x: clampN(d.x), y: clampN(d.y), z: clampN(d.z),
      dx: safeDir(d.dx), dy: safeDir(d.dy), dz: safeDir(d.dz),
      d: clampN(d.d, 0, 2000)
    });
  });

  // попадание по другому игроку: сервер пересылает урон только жертве
  socket.on('hit', d => {
    if (!d || !players.has(d.t)) return;
    const dmg = Math.round(Math.max(1, Math.min(60, Number(d.dmg) || 10)));
    console.log(`[hit] ${socket.id.slice(0, 5)} -> ${String(d.t).slice(0, 5)} dmg=${dmg}`);
    io.to(d.t).emit('hit', { by: socket.id, dmg });
  });

  socket.on('disconnect', () => {
    const p = players.get(socket.id);
    players.delete(socket.id);
    if (p && !freeNums.includes(p.num)) freeNums.push(p.num); // номер возвращается в пул
    freeNums.sort((a, b) => a - b);
    io.emit('leave', { id: socket.id });
    console.log(`[-] Игрок ${p ? p.num : '?'} отключился · онлайн: ${players.size}`);
  });
});

server.listen(PORT, () => {
  console.log('===========================================');
  console.log(`  НЕБО: ПВО / ДРОН — мультиплеер-сервер`);
  console.log(`  Открой в браузере: http://localhost:${PORT}`);
  console.log('===========================================');
});
