// ============================================================
// МУЛЬТИПЛЕЕР-СЕРВЕР «НЕБО: ПВО / ДРОН»
// Node.js + Express + Socket.io
//  - отдаёт index.html (и весь статик) при заходе на сайт
//  - ретранслирует координаты/повороты игроков (~10 Гц)
//  - ретранслирует выстрелы (трассеры у других клиентов)
//  - передаёт попадания между игроками (урон в PvP)
//  - ЕДИНСТВЕННЫЙ считает HP 4 ключевых точек «Битвы» (damagePoint -> pointUpdated)
//  - ретранслирует снапшоты ботов (~8 Гц), чтобы боты выглядели одинаково у обоих
//  - синхронизирует профиль игрока (syncProfile -> profileOk/profileSynced):
//    купленные в браузере дроны и ракеты признаются сервером и работают в онлайне
// Запуск:  npm install && npm start  ->  http://localhost:3000
// ============================================================
const path = require('path');
const fs = require('fs');
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

// ---------- профили игроков: покупки из localStorage синхронизируются с сервером ----------
// Клиент при подключении шлёт syncProfile {pid, profile} (кредиты, купленные дроны,
// улучшения). Сервер объединяет его с ранее сохранённым профилем этого же pid —
// покупки не теряются ни на одной из сторон, признаёт профиль (profileOk) и
// рассылает остальным краткую сводку (profileSynced).
const OWNED_KEYS = ['kr', 'scout', 'kam', 'heavy', 'bomber', 'fpv', 'aa'];
const PROF_FILE = path.join(__dirname, 'profiles.json');
let savedProfiles = {}; // pid -> профиль (переживает переподключения и перезапуск сервера)
try { savedProfiles = JSON.parse(fs.readFileSync(PROF_FILE, 'utf8')) || {}; } catch (e) {}
function saveProfiles() { try { fs.writeFileSync(PROF_FILE, JSON.stringify(savedProfiles)); } catch (e) {} }
function sanitizeProfile(raw) {
  const p = (raw && typeof raw === 'object') ? raw : {};
  const t = (p.training && typeof p.training === 'object') ? p.training : {};
  const ow = (p.owned && typeof p.owned === 'object') ? p.owned : {};
  const out = {
    credits: Math.max(0, Math.round(Number(p.credits) || 0)),
    gun: Math.max(0, Math.min(3, Math.round(Number(p.gun) || 0))),
    missiles: !!p.missiles,
    rkts: !!p.rkts,
    owned: {},
    selected: OWNED_KEYS.includes(p.selected) ? p.selected : 'kr',
    training: { pvo: !!t.pvo, drone: (Array.isArray(t.drone) ? t.drone : []).slice(0, 16).map(x => String(x).slice(0, 8)) }
  };
  for (const k of OWNED_KEYS) out.owned[k] = (k === 'kr') ? true : !!ow[k];
  return out;
}
function mergeProfiles(a, b) { // объединение: всё купленное с любой стороны остаётся
  const x = sanitizeProfile(a), y = sanitizeProfile(b);
  const out = {
    credits: Math.max(x.credits, y.credits),
    gun: Math.max(x.gun, y.gun),
    missiles: x.missiles || y.missiles,
    rkts: x.rkts || y.rkts,
    owned: {},
    selected: y.selected, // последний выбор игрока
    training: { pvo: x.training.pvo || y.training.pvo, drone: [...new Set([...x.training.drone, ...y.training.drone])].slice(0, 16) }
  };
  for (const k of OWNED_KEYS) out.owned[k] = x.owned[k] || y.owned[k];
  return out;
}

// ---------- общее состояние мира: HP 4 ключевых точек Битвы ----------
// Сервер — единственный, кто вычитает урон: клиенты только присылают damagePoint,
// а получают уже готовое состояние через pointUpdated / pointsReset / init.
const POINT_DEFS = { fuel: 200, power: 240, fuelbase: 280, outpost: 380 };
let worldPoints = {};
function resetWorld() {
  worldPoints = {};
  for (const id in POINT_DEFS) worldPoints[id] = { hp: POINT_DEFS[id], max: POINT_DEFS[id], active: true };
}
resetWorld();

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
  socket.emit('init', { id: socket.id, num, players: others, points: worldPoints });
  socket.broadcast.emit('join', { id: socket.id, num });
  console.log(`[+] Игрок ${num} подключился (${socket.id}) · онлайн: ${players.size} · транспорт: ${socket.conn.transport.name}`);
  socket.conn.on('upgrade', t => console.log(`[transport] ${socket.id.slice(0, 5)}: upgrade -> ${t.name}`));

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

  // урон по ключевой точке Битвы: считает ТОЛЬКО сервер
  socket.on('damagePoint', d => {
    const p = players.get(socket.id);
    if (!p || !d || typeof d !== 'object') return;
    p.lastSeen = Date.now();
    const pt = worldPoints[d.id];
    if (!pt || !pt.active) return;                     // неизвестный id или уже разрушена
    const dmg = Math.round(Math.max(1, Math.min(200, Number(d.dmg) || 0)));
    pt.hp = Math.max(0, pt.hp - dmg);
    if (pt.hp <= 0) pt.active = false;
    io.emit('pointUpdated', { id: d.id, hp: pt.hp, active: pt.active, by: socket.id, p: d.p ? 1 : 0, dmg });
    if (pt.hp <= 0) console.log(`[world] Точка «${d.id}» уничтожена игроком ${p.num}`);
  });

  // профиль игрока (кредиты/купленные дроны/улучшения из localStorage):
  // сервер сохраняет, объединяет с прошлым профилем этого pid и ПРИЗНАЁТ покупки —
  // клиент получает profileOk и разрешает тяжёлый дрон/ракеты в онлайне
  socket.on('syncProfile', d => {
    const p = players.get(socket.id);
    if (!p || !d || typeof d !== 'object') return;
    p.lastSeen = Date.now();
    const pid = String(d.pid || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 40);
    const merged = pid ? mergeProfiles(savedProfiles[pid] || {}, d.profile) : sanitizeProfile(d.profile);
    if (pid) { savedProfiles[pid] = merged; saveProfiles(); }
    p.profile = merged;
    socket.emit('profileOk', { profile: merged });
    socket.broadcast.emit('profileSynced', { id: socket.id, num: p.num,
      prof: { gun: merged.gun, missiles: merged.missiles, rkts: merged.rkts, owned: merged.owned } });
    console.log(`[profile] Игрок ${p.num}: CR=${merged.credits} · пушки ур.${merged.gun} · ракеты=${(merged.rkts || merged.missiles) ? 'да' : 'нет'} · дроны: ${OWNED_KEYS.filter(k => merged.owned[k]).join(', ')}`);
  });

  // новый бой у кого-то из игроков: мир точек восстанавливается для всех
  socket.on('battleStart', () => {
    const p = players.get(socket.id);
    if (!p) return;
    p.lastSeen = Date.now();
    resetWorld();
    io.emit('pointsReset', { points: worldPoints });
    console.log('[world] Точки восстановлены — новый бой');
  });

  // снапшоты ботов (~8 Гц): позиции дронов/машин ПВО ретранслируются другим игрокам
  socket.on('bots', d => {
    const p = players.get(socket.id);
    if (!p || !d || !Array.isArray(d.b)) return;
    p.lastSeen = Date.now();
    const b = d.b.slice(0, 40).map(e => ({
      k: Math.round(Number(e && e.k)) || 0,
      t: String(e && e.t || 'kr').slice(0, 8),
      b: e && e.b === 'b' ? 'b' : 'r',
      v: e && e.v ? 1 : 0,
      x: clampN(e && e.x), y: clampN(e && e.y), z: clampN(e && e.z),
      ry: clampN(e && e.ry, -9, 9), rx: clampN(e && e.rx, -9, 9)
    }));
    socket.broadcast.emit('bots', { id: socket.id, b });
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
