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

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const redis = new Redis(REDIS_URL); // para consultas REST
const redisSub = new Redis(REDIS_URL); // dedicado a Pub/Sub

redis.on("connect", () => console.log("[Server] Conectado a Redis (REST)"));
redisSub.on("connect", () => console.log("[Server] Conectado a Redis (sub)"));

// ---------------------------------------------------------------------
// REST: estado inicial y configuración de fuente
// ---------------------------------------------------------------------

app.get("/api/source", async (req, res) => {
  try {
    const source = (await redis.get("config:data_source")) || "simulator";
    res.json({ source });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/source", async (req, res) => {
  try {
    const { source } = req.body;
    if (!["simulator", "github"].includes(source)) {
      return res.status(400).json({ error: "Fuente no válida. Use 'simulator' o 'github'." });
    }
    await redis.set("config:data_source", source);
    // Purgar conjuntos y hashes de la fuente anterior para no mezclar servicios
    await redis.del("services:known");
    await redis.del("service:latency:ranking");
    const oldKeys = await redis.keys("service:state:*");
    if (oldKeys.length) await redis.del(...oldKeys);

    await redis.publish("system:config", JSON.stringify({ source }));
    io.emit("source-changed", { source });
    console.log(`[Server] 🔄 Fuente de datos conmutada a: [${source.toUpperCase()}]`);
    res.json({ ok: true, source });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Endpoint para comprobar/auditar directamente los datos oficiales de GitHub Status
app.get("/api/github/raw", async (req, res) => {
  try {
    const t0 = performance.now();
    const ghRes = await fetch("https://www.githubstatus.com/api/v2/summary.json", {
      headers: { "User-Agent": "Grupo3-RedisMonitoring/1.0" },
      signal: AbortSignal.timeout(4000),
    });
    const roundTripMs = Math.round(performance.now() - t0);
    const json = await ghRes.json();
    res.json({
      verified_url: "https://www.githubstatus.com/api/v2/summary.json",
      official_status_page: "https://www.githubstatus.com",
      http_round_trip_ms: roundTripMs,
      github_page: json.page,
      components_count: json.components?.length || 0,
      components: json.components?.slice(0, 8),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/state", async (req, res) => {
  try {
    const source = (await redis.get("config:data_source")) || "simulator";
    const services = await redis.smembers("services:known");
    const states = {};
    for (const svc of services) {
      const h = await redis.hgetall(`service:state:${svc}`);
      if (Object.keys(h).length) {
        const isGh = h.source === "github-api" || h.official_component === "true";
        if ((source === "github" && isGh) || (source === "simulator" && !isGh)) {
          states[svc] = h;
        }
      }
    }
    res.json({ services: states, source });
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

// Endpoint para disparar alerta de prueba en la demostración (Punto 29 del documento)
app.post("/api/alerts/test", async (req, res) => {
  try {
    const currentSource = (await redis.get("config:data_source")) || "simulator";
    const svcName = currentSource === "github" ? "api-requests" : "payments";
    const alert = {
      type: "HIGH_LATENCY",
      service: svcName,
      message: `[Demostración en vivo] Latencia crítica detectada en ${svcName}: 485ms (umbral 300ms)`,
      severity: "CRITICAL",
      timestamp: new Date().toISOString(),
    };
    await redis.publish("application-alerts", JSON.stringify(alert));
    await redis.lpush("application:alerts:recent", JSON.stringify(alert));
    await redis.ltrim("application:alerts:recent", 0, 99);
    res.json({ ok: true, alert });
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
  "system:config",
  (err) => {
    if (err) console.error("[Server] Error al suscribirse:", err.message);
    else console.log("[Server] Suscrito a canales de eventos/metrics/alerts/config");
  }
);

redisSub.on("message", (channel, message) => {
  const data = JSON.parse(message);
  if (channel === "application-events") io.emit("event", data);
  if (channel === "application-metrics") io.emit("metrics", data);
  if (channel === "application-alerts") io.emit("alert", data);
  if (channel === "system:config") io.emit("source-changed", data);
});

io.on("connection", (socket) => {
  console.log("[Server] Cliente dashboard conectado:", socket.id);
});

server.listen(PORT, () => {
  console.log(`[Server] Dashboard disponible en http://localhost:${PORT}`);
});
