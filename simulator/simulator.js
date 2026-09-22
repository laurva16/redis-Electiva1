/**
 * simulator.js
 * -----------------------------------------------------------------------
 * Simula un conjunto de microservicios que reportan métricas de forma
 * periódica y con comportamiento realista (sin saltos bruscos entre
 * lecturas consecutivas), tal como exige la actividad.
 *
 * No se conecta a Redis directamente: expone una función generateEvent()
 * que el Publisher invoca. Así el resto del sistema NO depende de esta
 * fuente concreta (podría reemplazarse por OpenTelemetry u otra fuente
 * real sin tocar el Publisher).
 * -----------------------------------------------------------------------
 */

const SERVICES = [
  "frontend",
  "api",
  "authentication",
  "payments",
  "database",
];

// Estado interno "físico" de cada servicio (para evolución realista)
const state = {};
SERVICES.forEach((svc) => {
  state[svc] = {
    requests_per_second: 40 + Math.random() * 40,
    latency_ms: 80 + Math.random() * 60,
    error_rate: Math.random() * 1.5,
    cpu: 30 + Math.random() * 20,
    memory: 40 + Math.random() * 20,
    incidentTicks: 0, // cuenta regresiva de un incidente activo
    incidentType: null,
  };
});

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

// Pequeña caminata aleatoria (random walk) para que los valores no salten
function walk(value, delta, min, max) {
  return clamp(value + (Math.random() - 0.5) * delta, min, max);
}

// Con baja probabilidad, dispara un "evento especial" en un servicio:
// pico de latencia, caída, aumento de errores, recuperación forzada.
function maybeTriggerIncident(svc) {
  const s = state[svc];
  if (s.incidentTicks > 0) return; // ya hay un incidente en curso

  const roll = Math.random();
  if (roll < 0.02) {
    s.incidentTicks = 4 + Math.floor(Math.random() * 4); // 4-7 ciclos
    s.incidentType = "LATENCY_SPIKE";
  } else if (roll < 0.03) {
    s.incidentTicks = 3 + Math.floor(Math.random() * 3);
    s.incidentType = "ERROR_SPIKE";
  } else if (roll < 0.035) {
    s.incidentTicks = 3 + Math.floor(Math.random() * 3);
    s.incidentType = "SERVICE_DOWN";
  }
}

function applyIncidentEffects(svc) {
  const s = state[svc];
  if (s.incidentTicks <= 0) return;

  switch (s.incidentType) {
    case "LATENCY_SPIKE":
      s.latency_ms = walk(s.latency_ms, 120, 200, 900);
      s.error_rate = walk(s.error_rate, 1, 0, 8);
      break;
    case "ERROR_SPIKE":
      s.error_rate = walk(s.error_rate, 6, 5, 40);
      s.latency_ms = walk(s.latency_ms, 60, 100, 400);
      break;
    case "SERVICE_DOWN":
      s.error_rate = walk(s.error_rate, 10, 60, 100);
      s.latency_ms = walk(s.latency_ms, 200, 500, 2000);
      s.requests_per_second = walk(s.requests_per_second, 20, 0, 10);
      break;
  }
  s.incidentTicks -= 1;
  if (s.incidentTicks === 0) s.incidentType = null;
}

function evolveNormal(svc) {
  const s = state[svc];
  s.requests_per_second = walk(s.requests_per_second, 8, 5, 150);
  s.latency_ms = walk(s.latency_ms, 15, 40, 250);
  s.error_rate = walk(s.error_rate, 0.6, 0, 5);
  s.cpu = walk(s.cpu, 5, 10, 95);
  s.memory = walk(s.memory, 3, 20, 90);
}

function statusFor(s) {
  if (s.error_rate > 20 || s.latency_ms > 500) return "CRITICAL";
  if (s.error_rate > 5 || s.latency_ms > 250 || s.cpu > 85) return "WARNING";
  return "OK";
}

/**
 * Genera un lote de eventos (uno por servicio) siguiendo el formato
 * general exigido por la actividad (entity_id, timestamp, location, data).
 */
function generateEvent() {
  const now = new Date().toISOString();

  return SERVICES.map((svc) => {
    maybeTriggerIncident(svc);
    if (state[svc].incidentTicks > 0) {
      applyIncidentEffects(svc);
    } else {
      evolveNormal(svc);
    }

    const s = state[svc];
    const status = statusFor(s);

    return {
      entity_id: svc,
      timestamp: now,
      location: { latitude: 5.7147, longitude: -72.9308 }, // UPTC Sogamoso (referencial)
      data: {
        requests_per_second: Math.round(s.requests_per_second * 10) / 10,
        latency_ms: Math.round(s.latency_ms),
        error_rate: Math.round(s.error_rate * 100) / 100,
        cpu: Math.round(s.cpu),
        memory: Math.round(s.memory),
        status,
        incident: s.incidentType,
      },
    };
  });
}

module.exports = { generateEvent, SERVICES };
