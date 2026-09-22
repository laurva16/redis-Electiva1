# Documento Técnico — Grupo 3: Monitoreo de Aplicaciones y Servicios

**Curso:** Tendencias Modernas de Bases de Datos
**Actividad:** Sistemas de datos en tiempo real con Redis

> Plantilla lista para completar con datos específicos del equipo (nombres,
> capturas de pantalla, decisiones concretas tomadas durante el desarrollo).

## 1. Descripción del problema

El monitoreo de aplicaciones (observabilidad) requiere conocer, en todo
momento, el estado de salud de los distintos servicios que componen un
sistema (frontend, API, autenticación, pagos, base de datos), así como
detectar automáticamente condiciones anómalas (latencia alta, errores,
saturación de CPU) para poder reaccionar antes de que afecten al usuario
final. A diferencia de un almacenamiento tradicional, estos datos cambian
constantemente y pierden valor si no se procesan casi en tiempo real.

## 2. Arquitectura

*(Insertar aquí el diagrama de arquitectura — ver `README.md` para el
diagrama en texto, o exportar uno gráfico.)*

Componentes:
- **Simulador**: genera métricas por servicio cada 5 segundos con
  comportamiento realista (random walk + incidentes simulados).
- **Publisher**: normaliza los eventos y los escribe en Redis.
- **Redis**: núcleo de comunicación y almacenamiento en memoria.
- **Processor/Subscriber**: calcula métricas derivadas y evalúa alertas.
- **Servidor web**: expone REST + WebSockets.
- **Dashboard**: visualización en tiempo real.

## 3. Tecnologías utilizadas

- **Backend:** Node.js, Express, ioredis, Socket.io
- **Base de datos en memoria:** Redis 7
- **Frontend:** HTML/CSS/JavaScript, Chart.js (vía CDN)
- **Comunicación en tiempo real:** WebSockets (Socket.io)

## 4. Fuente de datos

Se utiliza un **simulador propio** (modo simulado), ya que la actividad no
exige una API pública para este dominio y se sugiere explícitamente
investigar OpenTelemetry como posible fuente real. El simulador:

- Representa 5 microservicios (`frontend`, `api`, `authentication`,
  `payments`, `database`).
- Evoluciona los valores mediante una caminata aleatoria acotada (nunca hay
  saltos bruscos entre lecturas consecutivas).
- Introduce incidentes simulados con baja probabilidad: picos de latencia,
  aumento de errores y caída de servicio, seguidos de recuperación gradual.

*(Si el equipo decide integrar OpenTelemetry como fuente real adicional,
documentar aquí el endpoint/instrumentación utilizada y qué ocurre cuando
no está disponible — el sistema debe seguir funcionando en modo simulado.)*

## 5. Estructura de los eventos

```json
{
  "entity_id": "payments",
  "timestamp": "2026-09-19T10:30:00Z",
  "location": { "latitude": 5.7147, "longitude": -72.9308 },
  "data": {
    "requests_per_second": 82,
    "latency_ms": 143,
    "error_rate": 2.8,
    "cpu": 74,
    "memory": 68,
    "status": "WARNING",
    "incident": null
  }
}
```

## 6. Estructuras de Redis utilizadas y justificación

| Estructura   | Clave                              | Uso                                                       | Justificación                                                                 |
|--------------|-------------------------------------|-------------------------------------------------------------|--------------------------------------------------------------------------------|
| Pub/Sub      | `application-events`               | Comunicación inmediata Publisher → Subscriber/Dashboard   | Entrega en tiempo real, baja latencia, desacopla productor y consumidores.     |
| Streams      | `application:stream`               | Histórico reciente de eventos (MAXLEN ~500)                | Persiste eventos aunque no haya Subscribers conectados en el momento.          |
| Hashes       | `service:state:<servicio>`         | Estado actual de cada servicio (TTL 60s)                    | Acceso O(1) al último valor sin recorrer historial; expira si el servicio calla.|
| Sorted Set   | `service:latency:ranking`          | Ranking de servicios por latencia                          | Permite obtener el "peor" servicio en O(log N) con `ZREVRANGE`.               |
| String + TTL | `application:metrics:latest`       | Última métrica derivada agregada                            | Dato temporal (2 min); no tiene sentido conservarlo indefinidamente.           |
| Set          | `services:known`                   | Registro de servicios activos conocidos                     | Permite reconstruir el listado de servicios al recargar el dashboard.          |

Esto cumple con el mínimo de **dos estructuras diferentes** exigido por la
actividad (en este proyecto se usan cinco).

## 7. Canales Pub/Sub

- `application-events`: eventos crudos normalizados (Publisher → todos).
- `application-metrics`: métricas derivadas agregadas (Processor → Server).
- `application-alerts`: alertas generadas por el Processor (Processor → Server).

## 8. Streams

`application:stream` conserva hasta 500 eventos recientes (`MAXLEN ~`), lo
que permite reconstruir el histórico reciente de cualquier servicio (ver
endpoint `GET /api/history/:service`) sin necesidad de una base de datos
relacional. Se diferencia de Pub/Sub en que **persiste** los eventos: un
consumidor que se conecta tarde puede seguir leyéndolos, mientras que un
mensaje Pub/Sub perdido no puede recuperarse.

## 9. Procesamiento realizado

El Processor calcula, sobre cada evento recibido:

- **Latencia promedio global** (`avg_latency_ms`)
- **% de error promedio global** (`avg_error_rate`)
- **Solicitudes por segundo promedio** (`avg_requests_per_second`)
- **Disponibilidad** (`availability_pct` = servicios en estado OK / total)
- **Servicio con mayor latencia** (vía Sorted Set `ZREVRANGE`)

## 10. Reglas de alerta

| Regla                                   | Condición                | Severidad |
|------------------------------------------|---------------------------|-----------|
| Latencia alta                             | `latency_ms > 300`        | WARNING   |
| Tasa de error alta                        | `error_rate > 10%`        | CRITICAL  |
| CPU elevada                               | `cpu > 90%`                | WARNING   |
| Servicio en estado crítico                | `status == "CRITICAL"`    | CRITICAL  |

## 11. Diseño del dashboard

- KPIs: latencia promedio, % error promedio, req/s, disponibilidad, peor servicio.
- Topología de servicios con color según estado (OK / WARNING / CRITICAL).
- 2 gráficas temporales: latencia promedio y % error promedio (Chart.js).
- Tabla de estado actual por servicio.
- Panel de alertas recientes.
- Actualización en tiempo real vía Socket.io (sin recargar la página).

## 12. Instrucciones de instalación y ejecución

Ver `README.md` del repositorio.

## 13. Dificultades encontradas

*(Completar por el equipo: p. ej. ajuste de umbrales de alerta, manejo de
reconexión de Redis, sincronización de estado inicial vs. eventos en vivo, etc.)*

## 14. Conclusiones

*(Completar por el equipo: ventajas y limitaciones observadas al usar Redis
como base de datos en memoria frente a una base de datos tradicional,
lecciones aprendidas sobre Pub/Sub vs. Streams, etc.)*

## 15. Posibles respuestas rápidas a las preguntas de sustentación

- **¿Por qué Redis?** Porque el caso de uso exige lecturas/escrituras de muy
  baja latencia sobre datos que cambian constantemente; una base relacional
  añadiría overhead innecesario para el "estado actual".
- **¿Qué pasa si el Subscriber se desconecta?** Los eventos en Pub/Sub se
  pierden para ese Subscriber, pero el Stream conserva los últimos 500
  eventos, por lo que se puede reconstruir el histórico reciente al reconectar.
- **¿Qué pasa si Redis se reinicia?** Se pierde todo lo que no esté
  persistido (Redis aquí se usa sin persistencia obligatoria, como memoria
  caché); el sistema se repuebla solo en segundos gracias al Publisher.
- **¿Cómo controlan el crecimiento de los datos?** TTL en Hashes (60s) y
  en métricas (120s), y `MAXLEN ~500` en el Stream.
