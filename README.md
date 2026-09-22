# Grupo 3 — Monitoreo de Aplicaciones y Servicios (Redis)

Sistema de observabilidad simulada para 5 microservicios (`frontend`, `api`,
`authentication`, `payments`, `database`), construido sobre Redis como base
de datos en memoria y plataforma de eventos.

## Arquitectura

```
Simulador (5 servicios) 
      │
      ▼
  Publisher   →  normaliza eventos a JSON y escribe en Redis
      │
      ▼
   REDIS
   ├── Pub/Sub   "application-events"      (comunicación inmediata)
   ├── Streams   "application:stream"      (histórico reciente, TTL/MAXLEN)
   ├── Hashes    "service:state:<svc>"     (estado actual, TTL 60s)
   ├── Sorted Set "service:latency:ranking" (ranking por latencia)
   └── String     "application:metrics:latest" (métricas derivadas, TTL)
      │
      ▼
Processor/Subscriber → calcula métricas y alertas → publica en
   "application-metrics" y "application-alerts"
      │
      ▼
 Servidor web (Express + Socket.io) → Dashboard (Chart.js, tiempo real)
```

## Requisitos previos

- Node.js 18+
- Redis corriendo en `localhost:6379` (o definir `REDIS_URL`)

Instalar Redis rápidamente:
```bash
# Docker (recomendado)
docker run -d --name redis-grupo3 -p 6379:6379 redis:7
```

## Instalación

```bash
cd grupo3-monitoreo
npm install
```

## Ejecución

### Opción 1: Todo en uno con un solo comando (Recomendado)
```bash
# Modo Simulado (microservicios estocásticos):
npm run dev

# O Modo Real directo (API pública de GitHub Status en vivo):
npm run dev:github
```

> **💡 Conmutador en tiempo real:** Incluso una vez iniciado el proyecto en cualquiera de los dos modos, puedes alternar entre el **Simulador** y la **API de GitHub** con un solo clic directamente desde el botón en el encabezado del Dashboard. Redis sincroniza el cambio de fuente en caliente entre el servidor y el Publisher vía Pub/Sub (`system:config`).

### Opción 2: En 3 terminales independientes
```bash
# Terminal 1 — Publisher (Simulador o con --github para API real)
npm run publisher
# (o: npm run publisher:github)

# Terminal 2 — Processor/Subscriber (métricas y alertas)
npm run processor

# Terminal 3 — Servidor web / Dashboard
npm start
```

Luego abre: **http://localhost:3000**

## Variables de entorno

| Variable    | Descripción                     | Default                     |
|-------------|----------------------------------|------------------------------|
| `REDIS_URL` | Cadena de conexión a Redis       | `redis://127.0.0.1:6379`    |
| `PORT`      | Puerto del servidor web          | `3000`                       |

## Estructura del repositorio

```
grupo3-monitoreo/
├── simulator/simulator.js     # Fuente de datos (simulador realista)
├── publisher/publisher.js     # Captura, normaliza y publica en Redis
├── processor/processor.js     # Subscriber + cálculo de métricas/alertas
├── server/server.js           # API REST + WebSockets
├── server/public/index.html   # Dashboard (Chart.js + Socket.io)
├── docs/documento_tecnico.md  # Documento técnico del entregable
└── package.json
```

## Qué cumple de los requisitos mínimos

- ✅ Redis funcionando (Pub/Sub, Streams, Hashes, Sorted Set, TTL — 5 estructuras).
- ✅ Publisher y Subscriber/Processor independientes.
- ✅ Generación continua de datos (cada 5s) con comportamiento realista.
- ✅ 2+ métricas derivadas: latencia promedio, % error promedio, disponibilidad,
  servicio con mayor latencia.
- ✅ Alertas: latencia alta, error rate alto, CPU alta, servicio crítico.
- ✅ Dashboard con KPIs, 2 gráficas temporales, estado actual, topología,
  alertas y actualización en tiempo real vía WebSockets (sin recargar).
- ✅ Modo simulado íntegro (no depende de ninguna API externa).
- ✅ Estado actual (Hash, TTL 60s) vs. histórico reciente (Stream, MAXLEN 500).
