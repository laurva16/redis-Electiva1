/**
 * processor.js  (Subscriber + Processor)
 * -----------------------------------------------------------------------
 * Rol dentro de la arquitectura:
 *   REDIS -> SUBSCRIBER/PROCESSOR -> (vuelve a escribir en Redis)
 *
 * Responsabilidades:
 *   1. Suscribirse al canal Pub/Sub "application-events".
 *   2. Calcular al menos DOS métricas derivadas:
 *        - latencia promedio global
 *        - % de errores promedio global
 *        - disponibilidad (servicios en estado OK / total)
 *        - servicio con mayor latencia (ranking vía Sorted Set)
 *   3. Generar alertas según reglas de negocio (SI ... ENTONCES ...).
 *   4. Guardar resultados en Redis para que el servidor web los lea o
 *      los reciba también por Pub/Sub ("application-alerts",
 *      "application-metrics") y los reenvíe al dashboard vía WebSockets.
 * -----------------------------------------------------------------------
 */

const Redis = require("ioredis");

const CHANNEL_IN = "application-events";
const CHANNEL_CONFIG = "system:config";
const CHANNEL_METRICS = "application-metrics"; // el server.js se suscribe aquí
const CHANNEL_ALERTS = "application-alerts"; // el server.js se suscribe aquí

const RANKING_KEY = "service:latency:ranking"; // Sorted Set
const METRICS_TTL_SECONDS = 120;
const ALERTS_LIST_KEY = "application:alerts:recent";
const ALERTS_LIST_MAXLEN = 100;

let activeSource = "simulator";

// Umbrales de alerta (justificables durante la presentación)
const THRESHOLDS = {
  latency_ms: 300, // alerta si un servicio supera 300ms
  error_rate: 10, // alerta si supera 10% de error
  cpu: 90, // alerta si CPU > 90%
};

const redisSub = new Redis(process.env.REDIS_URL || "redis://127.0.0.1:6379");
const redisPub = new Redis(process.env.REDIS_URL || "redis://127.0.0.1:6379");

// Ventana en memoria con la última lectura de cada servicio,
// para poder calcular métricas agregadas sin recorrer todo Redis.
const lastReadingByService = new Map();

redisSub.on("connect", async () => {
  console.log("[Processor] Conectado a Redis (sub)");
  // Leer fuente inicial guardada
  try {
    const s = await redisPub.get("config:data_source");
    if (s) activeSource = s;
  } catch (e) {}
});
redisPub.on("connect", () => console.log("[Processor] Conectado a Redis (pub)"));
redisSub.on("error", (err) => console.error("[Processor Sub] Error Redis:", err.message));
redisPub.on("error", (err) => console.error("[Processor Pub] Error Redis:", err.message));

redisSub.subscribe(CHANNEL_IN, CHANNEL_CONFIG, (err) => {
  if (err) {
    console.error("[Processor] Error al suscribirse:", err.message);
    return;
  }
  console.log(`[Processor] Suscrito a "${CHANNEL_IN}" y "${CHANNEL_CONFIG}"`);
});

redisSub.on("message", async (channel, message) => {
  if (channel === CHANNEL_CONFIG) {
    try {
      const cfg = JSON.parse(message);
      if (cfg.source) {
        activeSource = cfg.source;
        lastReadingByService.clear();
        await redisPub.del(RANKING_KEY);
        await redisPub.del(ALERTS_LIST_KEY);
        await redisPub.del("services:known");
        const oldHashes = await redisPub.keys("service:state:*");
        if (oldHashes.length) await redisPub.del(...oldHashes);
        console.log(`[Processor] 🔄 Fuente sincronizada a: [${activeSource.toUpperCase()}]`);
      }
    } catch (e) {}
    return;
  }

  if (channel !== CHANNEL_IN) return;
  try {
    const event = JSON.parse(message);
    const isGh = event.data && (event.data.source === "github-api" || event.data.official_component === "true" || event.data.official_component === true);
    if ((activeSource === "github" && isGh) || (activeSource === "simulator" && !isGh)) {
      lastReadingByService.set(event.entity_id, event);
      await updateRanking(event);
      await evaluateAlerts(event);
      await computeAndBroadcastMetrics();
    }
  } catch (err) {
    console.error("[Processor] Error procesando evento:", err.message);
  }
});

// Sorted Set: ranking de servicios por latencia (permite responder
// rápidamente "¿cuál es el servicio con mayor latencia?")
async function updateRanking(event) {
  await redisPub.zadd(RANKING_KEY, event.data.latency_ms, event.entity_id);
}

// Reglas de alerta simples tipo "SI condición ENTONCES alerta"
async function evaluateAlerts(event) {
  const { entity_id, data } = event;
  const displayName = data.service_name || entity_id;
  const alerts = [];

  if (data.latency_ms > THRESHOLDS.latency_ms) {
    alerts.push({
      type: "HIGH_LATENCY",
      service: displayName,
      message: `Latencia alta en ${displayName}: ${data.latency_ms}ms (umbral ${THRESHOLDS.latency_ms}ms)`,
      severity: "WARNING",
    });
  }
  if (data.error_rate != null && data.error_rate > THRESHOLDS.error_rate) {
    alerts.push({
      type: "HIGH_ERROR_RATE",
      service: displayName,
      message: `Tasa de errores alta en ${displayName}: ${data.error_rate}% (umbral ${THRESHOLDS.error_rate}%)`,
      severity: "CRITICAL",
    });
  }
  if (data.cpu != null && data.cpu > THRESHOLDS.cpu) {
    alerts.push({
      type: "HIGH_CPU",
      service: displayName,
      message: `CPU elevada en ${displayName}: ${data.cpu}% (umbral ${THRESHOLDS.cpu}%)`,
      severity: "WARNING",
    });
  }
  if (data.status === "CRITICAL") {
    alerts.push({
      type: "SERVICE_CRITICAL",
      service: displayName,
      message: `${displayName} está en estado CRÍTICO`,
      severity: "CRITICAL",
    });
  }

  for (const alert of alerts) {
    alert.timestamp = new Date().toISOString();
    const payload = JSON.stringify(alert);

    // Publica la alerta en tiempo real
    await redisPub.publish(CHANNEL_ALERTS, payload);

    // Guarda un histórico corto de alertas recientes (lista acotada)
    await redisPub.lpush(ALERTS_LIST_KEY, payload);
    await redisPub.ltrim(ALERTS_LIST_KEY, 0, ALERTS_LIST_MAXLEN - 1);
  }
}

// Calcula métricas derivadas agregadas sobre todos los servicios activos
async function computeAndBroadcastMetrics() {
  const now = Date.now();
  for (const [svcId, r] of lastReadingByService.entries()) {
    if (now - new Date(r.timestamp).getTime() > 20000) {
      lastReadingByService.delete(svcId);
    }
  }

  const readings = Array.from(lastReadingByService.values()).filter((r) => {
    const isGh = r.data && (r.data.source === "github-api" || r.data.official_component === "true" || r.data.official_component === true);
    return activeSource === "github" ? isGh : !isGh;
  });
  if (readings.length === 0) return;

  const avgLatency =
    readings.reduce((sum, r) => sum + (Number(r.data.latency_ms) || 0), 0) / readings.length;
  const avgErrorRate =
    readings.reduce((sum, r) => sum + (Number(r.data.error_rate) || 0), 0) / readings.length;
  const avgRps =
    readings.reduce((sum, r) => sum + (Number(r.data.requests_per_second) || 0), 0) /
    readings.length;

  const okCount = readings.filter((r) => r.data.status === "OK").length;
  const availability = (okCount / readings.length) * 100;

  // Servicio con mayor latencia (top del Sorted Set)
  const top = await redisPub.zrevrange(RANKING_KEY, 0, 0, "WITHSCORES");
  const worstLatencyService = top.length
    ? { service: top[0], latency_ms: Number(top[1]) }
    : null;

  const metrics = {
    timestamp: new Date().toISOString(),
    avg_latency_ms: Math.round(avgLatency * 10) / 10,
    avg_error_rate: Math.round(avgErrorRate * 100) / 100,
    avg_requests_per_second: Math.round(avgRps * 10) / 10,
    availability_pct: Math.round(availability * 10) / 10,
    worst_latency_service: worstLatencyService,
    services_count: readings.length,
    source: activeSource,
  };

  // Guarda las métricas derivadas con TTL (dato temporal, no permanente)
  await redisPub.set(
    "application:metrics:latest",
    JSON.stringify(metrics),
    "EX",
    METRICS_TTL_SECONDS
  );

  await redisPub.publish(CHANNEL_METRICS, JSON.stringify(metrics));
}

console.log("[Processor] Escuchando eventos y calculando métricas...");
