# Herald 🦍🍌

**Webhook Delivery Engine** — recebe eventos de apps e os entrega de forma **confiável** nos endpoints dos clientes, mesmo quando o destino está fora do ar ou lento.

Projeto de portfólio em microservices que replica a arquitetura de entrega de webhooks usada por empresas como Stripe, GitHub e Twilio: desacoplamento via Kafka, retry com backoff exponencial, idempotência e Dead Letter Queue.

---

## ✨ Fluxo

```mermaid
flowchart LR
    A[Cliente] -->|POST /api/v1/apps/:id/events| B[endpoint-service]
    B -->|publica em `ingress`| C[(Kafka)]
    C --> D[Planner]
    D -->|1 msg por endpoint em `delivery`| C
    C --> E[Delivery Worker]
    E -->|POST com HMAC| F[Endpoint do cliente]
    E -->|falha: retry 2ⁿ·delay em `retry`| C
    C -->|delay passa| E
    E -->|exauriu tentativas em `dlq`| G[DeadLetter no MongoDB]
```

O cliente recebe **202 Accepted** na hora — a entrega é **assíncrona** e desacoplada por fila.

---

## 🚀 Como usar

### 1. Suba a infraestrutura

```bash
docker compose up -d
```

Sobe: **MySQL 8** (3306), **MongoDB 7** (27017), **Redis 7** (6379), **Kafka + Zookeeper** (9093), **Prometheus** (9090) e **Grafana** (3000, login `admin`/`admin`).

> O Kafka usa a porta **9093** para não conflitar com outros brokers locais na 9092. Prometheus e Grafana rodam com `network_mode: host` (contam a rede do host) para fazer scrape em `localhost:8081/8082` sem depender de firewall do docker.

### 2. Suba os serviços

```bash
./mvnw -pl gateway spring-boot:run            # :8080 (roteamento + rate limit)
./mvnw -pl endpoint-service spring-boot:run    # :8081
./mvnw -pl webhook-dispatcher spring-boot:run  # :8082
./mvnw -pl retry-consumer spring-boot:run      # :8083
```

> A partir de agora as chamadas entram pelo **gateway** (`localhost:8080/api/v1/**`), que faz **rate limiting por app** usando o header `X-App-Key`.

### 3. Teste

```bash
# cria app (gera apiKey + secretHmac)
curl -X POST localhost:8080/api/v1/apps \
  -H "Content-Type: application/json" -d '{"nome":"Minha Loja"}'

# cadastra o endpoint que vai receber os webhooks
curl -X POST localhost:8080/api/v1/apps/1/endpoints \
  -H "Content-Type: application/json" -d '{"url":"https://meu-site.com/webhook"}'

# publica um evento (retorna 202 e entrega de forma confiável)
curl -X POST localhost:8080/api/v1/apps/1/events \
  -H "Content-Type: application/json" -d '{"type":"payment.confirmed","data":{"order":42}}'
```

Para exercitar o **rate limiting** (cota configurável em `herald.rate-limit.*`):

```bash
# estoura a cota do app e recebe 429 + Retry-After
for i in $(seq 1 50); do
  curl -s -o /dev/null -w "%{http_code} " localhost:8080/api/v1/apps \
    -H "X-App-Key: <apiKey do app>"
done; echo
```

---

## 🧱 Arquitetura

Mono-repo Maven com multi-módulo:

| Módulo | Porta | Papel | Status |
|--------|-------|-------|--------|
| `common` | — | DTOs e utilitários compartilhados (`HmacSigner`, `KafkaTopics`, `DeliveryMessage`) | ✅ |
| `endpoint-service` | 8081 | CRUD de apps/endpoints (MySQL) + validação + **ingestão** de eventos | ✅ |
| `webhook-dispatcher` | 8082 | Entrada: publica eventos no `ingress`. Worker: entrega HTTP com assinatura HMAC | ✅ |
| `retry-consumer` | 8083 | Backoff exponencial e registro na Dead Letter Queue | ✅ |
| `gateway` | 8080 | Roteamento `/api/v1/**` → serviços e **rate limiting por app** (Redis) | ✅ |

### Tópicos Kafka
| Tópico | Papel |
|--------|-------|
| `webhook.events.ingress` | Eventos recebidos (chave = `appId`) |
| `webhook.events.delivery` | Entregas individuais (1 evento → 1 endpoint) |
| `webhook.events.retry` | Retentativa agendada com delay |
| `webhook.events.dlq` | Eventos que exauriram as tentativas |

### Bando de dados
| Banco | O que guarda |
|-------|-------------|
| **MySQL** | Dados de negócio: apps, endpoints, chaves |
| **MongoDB** | Auditoria: logs de cada tentativa (`DeliveryAttempt`) e eventos na DLQ (`DeadLetter`) |
| **Redis** | Contadores do **token bucket** do rate limiting (1 bucket por app) |

---

## 📈 Observabilidade

Métricas extras expostas por **Micrometer** nos endpoints `/actuator/prometheus` do `endpoint-service`, `webhook-dispatcher` e `gateway`, e coletadas pelo Prometheus. As métricas de processo de entrega:

| Métrica | Tipo | O que mede |
|---------|------|-----------|
| `herald_delivery_total{status,appId}` | contador | Entregas HTTP, por status e app |
| `herald_delivery_duration_seconds` | histograma | Latência das entregas (P50/P95/P99) |
| `herald_retry_scheduled_total` | contador | Retentativas agendadas |
| `herald_dlq_total` | contador | Eventos enviados à Dead Letter Queue |

### Acessando

- **Grafana** → http://localhost:3000 — dashboard **"Herald - Observabilidade"** já provisionada a partir de `./grafana/`. Login: `admin` / `admin`.
- **Prometheus** → http://localhost:9090

A dashboard inclui: volume de entregas por app, latência P50/P95/P99, taxa de sucesso, retentativas e eventos na DLQ. O dashboard e o datasource são provisionados por arquivos (`grafana/datasources.yml`, `grafana/dashboards-provider.yml`, `grafana/herald-dashboard.json`), então sobrevivem a `docker compose down`.

---

## 🛠️ Confiabilidade implementada

- **Desacoplamento assíncrono** — resposta 202 imediata, fila intermediária
- **Entrega de at-least-once** com **retry exponencial** (`2ⁿ · 10s`, configurável)
- **Idempotência** — dedup por `(eventId, endpointId)` + header `Idempotency-Key`
- **Autenticidade** — assinatura `HMAC-SHA256` via `X-Webhook-Signature`
- **Dead Letter Queue** — eventos irremediavelmente falhos ficam registrados com o motivo
- **Timeout** — cada entrega tem limite de 5s
- **Rate limiting por app** — token bucket no Redis via gateway (header `X-App-Key`), resposta **429** com `Retry-After`

---

## 🧪 Testes

`./mvnw clean verify` roda a suíte inteira com **Testcontainers**:

| Módulo | Cobertura |
|--------|-----------|
| endpoint-service | CRUD + ingestão/validação + evento chega no tópico |
| webhook-dispatcher | entrega 200/500/indisponível, roteamento p/ retry/DLQ, dedup |
| retry-consumer | reenvio após delay, persistência de DeadLetter |

> Requer **Docker** rodando (Testcontainers).

---

## 📌 Notas técnicas

- A entrega é **at-least-once**: em caso de falha do worker o evento pode ser entregue mais de uma vez — a idempotência no receptor protege contra efeitos duplicados.
- O delay do retry usa `Thread.sleep` no consumer (didático e fino p/ este escopo); em ambiente de produção usar-se-ia um mecanismo nativo de fila atrasada.
- As chaves de apps (secret para assinatura) são armazenadas e resolvidas **somente** no `endpoint-service` — nunca trafegam no corpo das mensagens Kafka.

---

**Stack:** Java 21 · Spring Boot 3.5 · Apache Kafka · MySQL · MongoDB · Docker Compose · Testcontainers

Feito por [Aquinozz](https://github.com/Aquinozz) — v0.2.0