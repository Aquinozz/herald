# Herald

Webhook Delivery Engine — recebe eventos de apps e entrega de forma confiável nos endpoints
dos clientes, mesmo quando o destino está fora do ar.

**Stack:** Java 21 · Spring Boot 3.5 · Kafka · MySQL · MongoDB · Docker

**Serviços:**
| Serviço | Porta | Papel |
|---------|-------|-------|
| gateway | 8080 | Roteamento (wip) |
| endpoint-service | 8081 | CRUD de apps/endpoints + ingestão |
| webhook-dispatcher | 8082 | Entrega HTTP (wip) |
| retry-consumer | 8083 | Retry/DLQ (wip) |

Em desenvolvimento — v0.1.0
