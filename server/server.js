const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
app.get("/", (_req, res) => res.status(200).json({ service: "ROBOT BATTLE realtime server", status: "ok" }));
app.get("/health", (_req, res) => res.status(200).json({ status: "ok" }));

const server = http.createServer(app);
const allowedOrigins = new Set([
  "https://mgandykim2010-star.github.io",
  "http://localhost:5500",
  "http://127.0.0.1:5500"
]);
const io = new Server(server, {
  cors: {
    origin(origin, callback) {
      if (!origin || allowedOrigins.has(origin)) return callback(null, true);
      return callback(new Error("Origin not allowed"));
    },
    methods: ["GET", "POST"]
  },
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6
});

const rooms = new Map();
const MAX_PLAYERS = 10;
const cleanCode = value => String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 6);
const cleanPresence = value => {
  const p = value && typeof value === "object" ? value : {};
  const out = {};
  if (typeof p.n === "string") out.n = p.n.slice(0, 12);
  if (typeof p.c === "string" && ["ASSASSIN", "TANKER", "SNIPER", "CONTROLLER"].includes(p.c)) out.c = p.c;
  if (Number.isFinite(Number(p.l))) out.l = Math.max(1, Math.min(5, Number(p.l) | 0));
  if (p.h === 0 || p.h === 1) out.h = p.h;
  if (p.tm === 0 || p.tm === 1) out.tm = p.tm;
  for (const key of ["i", "ti"]) {
    if (Array.isArray(p[key]) && p[key].length <= 8 && p[key].every(v => Number.isFinite(Number(v)))) {
      out[key] = p[key].slice(0, 8).map(v => Math.max(-100000, Math.min(100000, Number(v))));
    }
  }
  return out;
};
function roomPeers(code) {
  const room = rooms.get(code);
  if (!room) return [];
  return [...room.entries()].map(([peer, presence]) => ({ peer, presence }));
}
function publishPeers(code) {
  io.to(code).emit("room:peers", roomPeers(code));
}
function removeFromRoom(socket) {
  const code = socket.data.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  if (room) {
    room.delete(socket.id);
    if (room.size === 0) rooms.delete(code);
    else publishPeers(code);
  }
  socket.leave(code);
  socket.data.roomCode = null;
}
io.on("connection", socket => {
  socket.on("room:join", (payload, ack) => {
    const code = cleanCode(payload && payload.code);
    if (code.length < 4 || code.length > 6) {
      if (typeof ack === "function") ack({ ok: false, error: "방 코드는 영문·숫자 4~6자리입니다." });
      return;
    }
    removeFromRoom(socket);
    let room = rooms.get(code);
    if (!room) {
      room = new Map();
      rooms.set(code, room);
    }
    if (room.size >= MAX_PLAYERS) {
      if (room.size === 0) rooms.delete(code);
      if (typeof ack === "function") ack({ ok: false, error: "방이 가득 찼습니다. (최대 10명)" });
      return;
    }
    room.set(socket.id, {});
    socket.data.roomCode = code;
    socket.join(code);
    if (typeof ack === "function") ack({ ok: true, peer: socket.id, peers: roomPeers(code) });
    publishPeers(code);
  });

  socket.on("room:presence", patch => {
    const code = socket.data.roomCode;
    const room = code && rooms.get(code);
    if (!room || !room.has(socket.id)) return;
    room.set(socket.id, { ...room.get(socket.id), ...cleanPresence(patch) });
    socket.to(code).emit("room:peer-presence", { peer: socket.id, presence: room.get(socket.id) });
  });

  socket.on("room:message", message => {
    const code = socket.data.roomCode;
    if (!code || !rooms.has(code) || !message || typeof message.event !== "string") return;
    const allowed = new Set(["go", "goTeam", "st", "teamSt"]);
    if (!allowed.has(message.event)) return;
    socket.to(code).emit("room:event", {
      event: message.event,
      peer: socket.id,
      data: message.data
    });
  });

  socket.on("disconnect", () => removeFromRoom(socket));
});

const port = Number(process.env.PORT) || 3000;
server.listen(port, "0.0.0.0", () => {
  console.log(`ROBOT BATTLE realtime server listening on ${port}`);
});
