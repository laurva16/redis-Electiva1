/**
 * server.js
 * -----------------------------------------------------------------------
 * Rol dentro de la arquitectura:
 *   REDIS -> (vía Processor) -> APLICACIÓN WEB -> DASHBOARD
 *
 * Este servidor:
 *   1. Sirve el dashboard estático (public/index.html).
 *   2. Expone endpoints REST para el estado inicial (estado actual,
 *      histórico reciente, alertas recientes) que el dashboard consulta
 *      al cargar la página.
 *   3. Se suscribe a los canales Pub/Sub que publica el Processor
 *      (application-metrics, application-alerts) y al canal de eventos
 *      crudos (application-events), y reenvía todo por WebSockets
 *      (Socket.io) para que el dashboard se actualice SIN recargar.
 * -----------------------------------------------------------------------
 */

const path = require("path");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const Redis = require("ioredis");

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, "public")));

const redis = new Redis(REDIS_URL); // para consultas REST
const redisSub = new Redis(REDIS_URL); // dedicado a Pub/Sub

redis.on("connect", () => console.log("[Server] Conectado a Redis (REST)"));
redisSub.on("connect", () => console.log("[Server] Conectado a Redis (sub)"));

// ---------------------------------------------------------------------
// REST: estado inicial para pintar el dashboard al cargar la página
// ---------------------------------------------------------------------

app.get("/api/state", async (req, res) => {
  try {
    const services = await redis.smembers("services:known");
    const states = {};
    for (const svc of services) {
      const h = await redis.hgetall(`service:state:${svc}`);
      if (Object.keys(h).length) states[svc] = h;
    }
    res.json({ services: states });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/metrics", async (req, res) => {
  try {
    const raw = await redis.get("application:metrics:latest");
    res.json(raw ? JSON.parse(raw) : null);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/alerts", async (req, res) => {
  try {
    const raw = await redis.lrange("application:alerts:recent", 0, 19);
    res.json(raw.map((r) => JSON.parse(r)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Histórico reciente desde el Stream (últimos N eventos de un servicio)
app.get("/api/history/:service", async (req, res) => {
  try {
    const entries = await redis.xrevrange(
      "application:stream",
      "+",
      "-",
      "COUNT",
      50
    );
    const filtered = entries
      .map(([id, fields]) => {
        const obj = {};
        for (let i = 0; i < fields.length; i += 2) obj[fields[i]] = fields[i + 1];
        return { id, ...JSON.parse(obj.payload) };
      })
      .filter((e) => e.entity_id === req.params.service)
      .reverse();
    res.json(filtered);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------
// WebSockets: tiempo real
// ---------------------------------------------------------------------

redisSub.subscribe(
  "application-events",
  "application-metrics",
  "application-alerts",
  (err) => {
    if (err) console.error("[Server] Error al suscribirse:", err.message);
    else console.log("[Server] Suscrito a canales de eventos/metrics/alerts");
  }
);

redisSub.on("message", (channel, message) => {
  const data = JSON.parse(message);
  if (channel === "application-events") io.emit("event", data);
  if (channel === "application-metrics") io.emit("metrics", data);
  if (channel === "application-alerts") io.emit("alert", data);
});

io.on("connection", (socket) => {
  console.log("[Server] Cliente dashboard conectado:", socket.id);
});

server.listen(PORT, () => {
  console.log(`[Server] Dashboard disponible en http://localhost:${PORT}`);
});
