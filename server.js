/**
 * خادم WebSocket بسيط لمزامنة لاعبين أونلاين (باتل رويال)
 * ------------------------------------------------------
 * يدعم عدة "غرف" (rooms)، وكل غرفة فيها عدة لاعبين يتبادلون:
 *  - موضعهم وحركتهم (state)
 *  - إطلاق النار (shoot)
 *  - الإصابات والموت (hit / death)
 *  - الدردشة النصية البسيطة (chat) [اختياري]
 *
 * شغّله محليًا: npm install && npm start
 * (التفاصيل كاملة في README.md)
 */

const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS_PER_ROOM = 20;
const STATE_BROADCAST_MIN_INTERVAL_MS = 40; // تقليل الحمل: لا نرسل أسرع من هذا لكل لاعب

// ---- خادم HTTP بسيط (صفحة فحص صحة + يستضيف اتصال WS) ----
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, players: countAllPlayers() }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('✅ خادم باتل رويال يعمل. اتصل عبر WebSocket على نفس هذا العنوان.');
});

const wss = new WebSocket.Server({ server: httpServer });

/** rooms: Map<roomId, Map<playerId, {ws, name, x,y,z, rotY, hp, alive, lastSent}>> */
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, new Map());
  return rooms.get(roomId);
}

function countAllPlayers() {
  let n = 0;
  for (const room of rooms.values()) n += room.size;
  return n;
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(roomId, obj, exceptId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const msg = JSON.stringify(obj);
  for (const [pid, p] of room) {
    if (pid === exceptId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg);
  }
}

function publicPlayerList(roomId) {
  const room = rooms.get(roomId);
  if (!room) return [];
  return [...room.entries()].map(([id, p]) => ({
    id, name: p.name, x: p.x, y: p.y, z: p.z, rotY: p.rotY, hp: p.hp, alive: p.alive
  }));
}

wss.on('connection', (ws) => {
  const playerId = crypto.randomUUID();
  let roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        roomId = String(msg.room || 'default').slice(0, 40);
        const room = getRoom(roomId);

        if (room.size >= MAX_PLAYERS_PER_ROOM) {
          send(ws, { type: 'error', message: 'الغرفة ممتلئة' });
          ws.close();
          return;
        }

        room.set(playerId, {
          ws,
          name: String(msg.name || 'لاعب').slice(0, 20),
          x: 0, y: 0, z: 0, rotY: 0,
          hp: 100, alive: true,
          lastSent: 0
        });

        // أرسل للاعب الجديد هويته وقائمة اللاعبين الحاليين
        send(ws, {
          type: 'welcome',
          id: playerId,
          players: publicPlayerList(roomId)
        });

        // أخبر البقية بانضمام لاعب جديد
        broadcast(roomId, {
          type: 'player_joined',
          id: playerId,
          name: room.get(playerId).name
        }, playerId);

        console.log(`[+] ${playerId} انضم للغرفة "${roomId}" (${room.size} لاعبين)`);
        break;
      }

      case 'state': {
        if (!roomId) return;
        const room = rooms.get(roomId);
        const p = room && room.get(playerId);
        if (!p) return;

        p.x = msg.x; p.y = msg.y; p.z = msg.z; p.rotY = msg.rotY;

        const now = Date.now();
        if (now - p.lastSent < STATE_BROADCAST_MIN_INTERVAL_MS) return;
        p.lastSent = now;

        broadcast(roomId, {
          type: 'state',
          id: playerId,
          x: p.x, y: p.y, z: p.z, rotY: p.rotY
        }, playerId);
        break;
      }

      case 'shoot': {
        if (!roomId) return;
        broadcast(roomId, {
          type: 'shoot',
          id: playerId,
          x: msg.x, z: msg.z, vx: msg.vx, vz: msg.vz, weapon: msg.weapon || 'default'
        }, playerId);
        break;
      }

      case 'hit': {
        // لاعب يبلغ أنه أصاب لاعبًا آخر؛ الخادم يحدّث الصحة ويبث النتيجة لمنع الغش البسيط لاحقًا
        if (!roomId) return;
        const room = rooms.get(roomId);
        const target = room && room.get(msg.targetId);
        if (!target || !target.alive) return;

        target.hp = Math.max(0, target.hp - (msg.damage || 10));
        if (target.hp <= 0) target.alive = false;

        broadcast(roomId, {
          type: 'hit',
          sourceId: playerId,
          targetId: msg.targetId,
          hp: target.hp,
          alive: target.alive
        });
        break;
      }

      case 'chat': {
        if (!roomId) return;
        const room = rooms.get(roomId);
        const p = room && room.get(playerId);
        if (!p) return;
        broadcast(roomId, { type: 'chat', id: playerId, name: p.name, text: String(msg.text || '').slice(0, 200) });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    room.delete(playerId);
    broadcast(roomId, { type: 'player_left', id: playerId });
    if (room.size === 0) rooms.delete(roomId);
    console.log(`[-] ${playerId} غادر الغرفة "${roomId}"`);
  });

  ws.on('error', () => {});
});

httpServer.listen(PORT, () => {
  console.log(`🚀 الخادم يعمل على المنفذ ${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
  console.log(`   فحص الصحة: http://localhost:${PORT}/health`);
});
