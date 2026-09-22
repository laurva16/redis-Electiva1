/**
 * publisher.js
 * -----------------------------------------------------------------------
 * Rol dentro de la arquitectura (ver punto 5 del enunciado):
 *   FUENTE DE DATOS -> PUBLISHER -> REDIS
 *
 * Responsabilidades:
 *   1. Captura los eventos generados por la fuente activa:
 *        - Modo Real: API pública de GitHub Status (https://www.githubstatus.com)
 *        - Modo Simulado: Simulador estocástico (Random Walk + Incidentes)
 *   2. Soporta cambio dinámico de fuente en caliente vía canal Redis "system:config".
 *   3. Soporta fallback automático: Si la API de GitHub falla o no hay internet,
 *      conmuta transparentemente al simulador sin interrumpir el flujo.
 *   4. Normaliza los eventos al formato JSON estándar de la actividad.
 *   5. Publica en Redis usando múltiples estructuras:
 *        - Pub/Sub  -> canal "application-events" (comunicación inmediata)
 *        - Streams  -> "application:stream"        (histórico reciente, MAXLEN)
 *        - Hashes   -> "service:state:<service>"    (estado actual, TTL 60s)
 * -----------------------------------------------------------------------
 */

const Redis = require("ioredis");
const { generateEvent } = require("../simulator/simulator");
const { fetchGitHubEvents } = require("./github-source");

const PUBLISH_INTERVAL_MS = 5000;
const CHANNEL_EVENTS = "application-events";
const CHANNEL_CONFIG = "system:config";
const CONFIG_KEY_SOURCE = "config:data_source";
const STREAM_KEY = "application:stream";
const STREAM_MAXLEN = 500; // limita crecimiento del histórico reciente
const STATE_TTL_SECONDS = 60; // estado actual expira si el servicio deja de reportar

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const redis = new Redis(REDIS_URL);
const redisSub = new Redis(REDIS_URL);

// Fuente de datos inicial (por argumento CLI, variable de entorno, o simulador por defecto)
let currentSource =
  process.argv.includes("--github") || process.env.DATA_SOURCE === "github"
    ? "github"
    : "simulator";

redis.on("connect", () => console.log("[Publisher] Conectado a Redis"));
redis.on("error", (err) => console.error("[Publisher] Error Redis:", err.message));
redisSub.on("error", (err) => console.error("[Publisher Sub] Error Redis:", err.message));

// Escuchar cambios de configuración en vivo desde el Dashboard/Servidor
redisSub.on("connect", async () => {
  console.log("[Publisher] Conectado a Redis (sub) para configuración en caliente");
  await redisSub.subscribe(CHANNEL_CONFIG);

  // Sincronizar estado persistido en Redis si existe
  try {
    const savedSource = await redis.get(CONFIG_KEY_SOURCE);
    if (savedSource && (savedSource === "github" || savedSource === "simulator")) {
      currentSource = savedSource;
    } else {
      await redis.set(CONFIG_KEY_SOURCE, currentSource);
    }
    console.log(`[Publisher] Modo inicial activo: [${currentSource.toUpperCase()}]`);
  } catch (e) {
    console.warn("[Publisher] No se pudo leer configuración previa de Redis:", e.message);
  }
});

redisSub.on("message", async (channel, message) => {
  if (channel === CHANNEL_CONFIG) {
    try {
      const config = JSON.parse(message);
      if (config.source && (config.source === "github" || config.source === "simulator")) {
        if (currentSource !== config.source) {
          currentSource = config.source;
          console.log(`[Publisher] 🔄 Conmutando en caliente a modo: [${currentSource.toUpperCase()}]`);
          await redis.del("services:known");
        }
      }
    } catch (err) {
      console.error("[Publisher] Error procesando mensaje de configuración:", err.message);
    }
  }
});

async function publishEvent(event) {
  const payload = JSON.stringify(event);

  // 1) Pub/Sub: comunicación inmediata a los Subscribers activos
  await redis.publish(CHANNEL_EVENTS, payload);

  // 2) Streams: histórico reciente que puede procesarse después,
  //    aunque no haya ningún Subscriber conectado en ese momento
  await redis.xadd(
    STREAM_KEY,
    "MAXLEN",
    "~",
    STREAM_MAXLEN,
    "*",
    "entity_id",
    event.entity_id,
    "payload",
    payload
  );

  // 3) Hash: estado actual del servicio (consulta O(1) sin recorrer historial)
  const hashKey = `service:state:${event.entity_id}`;
  const hashObj = {
    entity_id: event.entity_id,
    service_name: event.data.service_name || event.entity_id,
    timestamp: event.timestamp,
    latency_ms: event.data.latency_ms,
    status: event.data.status,
    incident: event.data.incident || "",
    source: event.data.source || currentSource,
  };
  if (event.data.requests_per_second !== undefined) hashObj.requests_per_second = event.data.requests_per_second;
  if (event.data.error_rate !== undefined) hashObj.error_rate = event.data.error_rate;
  if (event.data.cpu !== undefined) hashObj.cpu = event.data.cpu;
  if (event.data.memory !== undefined) hashObj.memory = event.data.memory;

  await redis.hset(hashKey, hashObj);
  await redis.expire(hashKey, STATE_TTL_SECONDS); // TTL: evita datos "zombie"

  // Mantiene el set de servicios conocidos (útil para el dashboard)
  await redis.sadd("services:known", event.entity_id);
}

async function tick() {
  let events = [];
  let usedSource = currentSource;

  try {
    if (currentSource === "github") {
      try {
        events = await fetchGitHubEvents();
      } catch (ghErr) {
        console.warn(
          `[Publisher] ⚠️ Falla al consultar GitHub Status (${ghErr.message}). Activando fallback automático al Simulador.`
        );
        events = generateEvent();
        usedSource = "simulator (fallback)";
      }
    } else {
      events = generateEvent();
    }

    for (const ev of events) {
      await publishEvent(ev);
    }

    console.log(
      `[Publisher] Publicados ${events.length} eventos [Fuente: ${usedSource}] @ ${new Date().toLocaleTimeString()}`
    );
  } catch (err) {
    console.error("[Publisher] Error en ciclo de publicación:", err.message);
  }
}

console.log(
  `[Publisher] Iniciando publicación cada ${PUBLISH_INTERVAL_MS / 1000}s en canal "${CHANNEL_EVENTS}"`
);

tick();
setInterval(tick, PUBLISH_INTERVAL_MS);
