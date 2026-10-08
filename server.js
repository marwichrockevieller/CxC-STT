// Servidor relay para o jogo de cartas: fila de matchmaking + salas por código.
// Ele NÃO conhece as regras do jogo; só junta 2 jogadores e repassa os pacotes.
//
// Rodar local:  npm install && node server.js   (porta 8080, ou a da variável PORT)

const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 8080;
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // sem 0/O/1/I pra evitar confusão
const ROOM_TTL_MS = 10 * 60 * 1000;

const queues = new Map(); // version -> ws esperando oponente
const rooms = new Map(); // code -> { host: ws, version, createdAt }

// HTTP simples: serve de "health check" e também pra acordar o servidor no plano grátis.
const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain",
    "Access-Control-Allow-Origin": "*",
  });
  res.end("ok");
});

const wss = new WebSocketServer({ server, maxPayload: 1 << 20 });

function sendJson(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = "";
    for (let i = 0; i < 5; i++) code += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  return null;
}

function pair(host, guest) {
  host.partner = guest;
  guest.partner = host;
  sendJson(host, { t: "matched", id: 1 });
  sendJson(guest, { t: "matched", id: 2 });
}

function removeFromWaiting(ws) {
  if (ws.queueVersion !== undefined && queues.get(ws.queueVersion) === ws) {
    queues.delete(ws.queueVersion);
  }
  if (ws.roomCode && rooms.get(ws.roomCode)?.host === ws) {
    rooms.delete(ws.roomCode);
  }
}

function handleHello(ws, msg) {
  const version = String(msg.version || "");
  ws.helloDone = true;

  if (msg.t === "queue") {
    const waiting = queues.get(version);
    if (waiting && waiting !== ws && waiting.readyState === WebSocket.OPEN) {
      queues.delete(version);
      waiting.queueVersion = undefined;
      pair(waiting, ws); // quem esperava primeiro vira host
    } else {
      ws.queueVersion = version;
      queues.set(version, ws);
      sendJson(ws, { t: "queued" });
    }
  } else if (msg.t === "create") {
    const code = newCode();
    if (!code) return sendJson(ws, { t: "error", msg: "server_busy" });
    ws.roomCode = code;
    rooms.set(code, { host: ws, version, createdAt: Date.now() });
    sendJson(ws, { t: "room", code });
  } else if (msg.t === "join") {
    const code = String(msg.code || "").toUpperCase().trim();
    const room = rooms.get(code);
    if (!room || room.version !== version || room.host.readyState !== WebSocket.OPEN) {
      sendJson(ws, { t: "error", msg: "room_not_found" });
      return ws.close();
    }
    rooms.delete(code);
    room.host.roomCode = undefined;
    pair(room.host, ws);
  } else {
    sendJson(ws, { t: "error", msg: "bad_request" });
    ws.close();
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.helloDone = false;
  ws.partner = null;

  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      // pacote do jogo: repassa pro adversário, se houver
      if (ws.partner && ws.partner.readyState === WebSocket.OPEN) {
        ws.partner.send(data, { binary: true });
      }
      return;
    }
    if (ws.helloDone) return; // controle só vale uma vez por conexão
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return ws.close();
    }
    handleHello(ws, msg);
  });

  ws.on("close", () => {
    removeFromWaiting(ws);
    if (ws.partner) {
      sendJson(ws.partner, { t: "left" });
      ws.partner.partner = null;
      ws.partner = null;
    }
  });

  ws.on("error", () => {});
});

// Heartbeat: derruba conexões mortas e mantém as vivas ativas atrás do proxy.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
  // salas esquecidas
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.createdAt > ROOM_TTL_MS) {
      sendJson(room.host, { t: "error", msg: "room_expired" });
      rooms.delete(code);
      room.host.close();
    }
  }
}, 25000);

server.listen(PORT, () => console.log("Relay ouvindo na porta " + PORT));
