/**
 * publisher.js
 * -----------------------------------------------------------------------
 * Rol dentro de la arquitectura (ver punto 5 del enunciado):
 *   FUENTE DE DATOS -> PUBLISHER -> REDIS
 *
 * Responsabilidades:
 *   1. Captura los eventos generados por el simulador (o, en un modo
 *      real, por OpenTelemetry / un generador de carga).
 *   2. Normaliza el evento al formato JSON estándar de la actividad.
 *   3. Publica en Redis usando AL MENOS dos estructuras diferentes:
 *        - Pub/Sub  -> canal "application-events" (comunicación inmediata)
 *        - Streams  -> "application:stream"        (histórico reciente)
 *        - Hashes   -> "service:state:<service>"    (estado actual, TTL)
 *
 * El intervalo de publicación es de 5 segundos, justificado porque las
 * métricas de aplicaciones (latencia, CPU, error rate) cambian rápido y
 * un dashboard de observabilidad necesita refresco frecuente para ser útil.
 * -----------------------------------------------------------------------
 */

const Redis = require("ioredis");
const { generateEvent } = require("../simulator/simulator");

const PUBLISH_INTERVAL_MS = 5000;
const CHANNEL = "application-events";
const STREAM_KEY = "application:stream";
const STREAM_MAXLEN = 500; // limita crecimiento del histórico reciente
const STATE_TTL_SECONDS = 60; // estado actual expira si el servicio deja de reportar

const redis = new Redis(process.env.REDIS_URL || "redis://127.0.0.1:6379");

redis.on("connect", () => console.log("[Publisher] Conectado a Redis"));
redis.on("error", (err) => console.error("[Publisher] Error Redis:", err.message));

async function publishEvent(event) {
  const payload = JSON.stringify(event);

  // 1) Pub/Sub: comunicación inmediata a los Subscribers activos
  await redis.publish(CHANNEL, payload);

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
  await redis.hset(hashKey, {
    entity_id: event.entity_id,
    timestamp: event.timestamp,
    requests_per_second: event.data.requests_per_second,
    latency_ms: event.data.latency_ms,
    error_rate: event.data.error_rate,
    cpu: event.data.cpu,
    memory: event.data.memory,
    status: event.data.status,
    incident: event.data.incident || "",
  });
  await redis.expire(hashKey, STATE_TTL_SECONDS); // TTL: evita datos "zombie"

  // Mantiene el set de servicios conocidos (útil para el dashboard)
  await redis.sadd("services:known", event.entity_id);
}

async function tick() {
  try {
    const events = generateEvent(); // un evento por servicio
    for (const ev of events) {
      await publishEvent(ev);
    }
    console.log(
      `[Publisher] Publicados ${events.length} eventos @ ${new Date().toLocaleTimeString()}`
    );
  } catch (err) {
    console.error("[Publisher] Error publicando evento:", err.message);
  }
}

console.log(
  `[Publisher] Iniciando publicación cada ${PUBLISH_INTERVAL_MS / 1000}s en canal "${CHANNEL}"`
);
tick();
setInterval(tick, PUBLISH_INTERVAL_MS);
