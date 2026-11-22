# MaltaShop "place an order" prototype

A small event-driven system that implements the **place-order flow** of the fictional
MaltaShop Ltd platform as four independent services that coordinate through a
message broker instead of one shared database.

| Service | Responsibility | Owns (own data store) | Port |
|---|---|---|---|
| `order-service` | REST API, order record, **saga orchestrator** | `orders` (SQLite file in its own volume) | 8001 |
| `inventory-service` | Stock levels, reservations | `stock`, `reservations` | 8002 |
| `payment-service` | Card payment through a (mock) external provider | `payments` | 8003 |
| `notification-service` | Customer emails (simulated) | `notifications` | 8004 |
| `rabbitmq` | Durable topic exchange `maltashop.events`, dead-letter queue | queues | 15672 (UI) |

No service reads another service's data. They only exchange events.

## How an order flows

```
Client ──POST /orders──▶ Order ──order.placed──▶ Inventory ──inventory.reserved──▶ Order
                                     │                                             │
                                     └──────────▶ Notification ("received")        ├─payment.requested─▶ Payment
                                                                                   │
        payment.succeeded ──▶ Order: CONFIRMED ──order.confirmed──▶ Notification   │
        payment.failed    ──▶ Order: CANCELLED ──inventory.release──▶ Inventory (compensation)
                                               └─order.cancelled───▶ Notification
        inventory.rejected──▶ Order: CANCELLED ──order.cancelled───▶ Notification
```

The Order service is the saga orchestrator. Order status moves
`PENDING → STOCK_RESERVED → CONFIRMED`, or ends in `CANCELLED`.
`POST /orders` returns **202 Accepted** immediately; the saga finishes asynchronously, so a
slow or failing Notification service can never delay or break checkout.

## Run it

Requirements: Docker with the Compose plugin.

```bash
docker compose up --build        # whole system, one command
```

Wait until all containers are healthy (about 30 seconds), then in another terminal:

```bash
./scripts/demo.sh                # three scenarios + correlated log lines
```

Or by hand:

```bash
curl -i -X POST http://localhost:8001/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: my-first-order' \
  -H 'Idempotency-Key: my-first-order' \
  -d '{"customer_email":"maria@example.mt",
       "items":[{"sku":"SKU-KETTLE","quantity":1,"unit_price_cents":2999}],
       "card_token":"tok_visa"}'

curl http://localhost:8001/orders/<order_id>      # CONFIRMED after about a second
docker compose logs --no-log-prefix | grep my-first-order   # one ID across all services
```

Scenarios to try:

| Request | Outcome |
|---|---|
| `SKU-KETTLE` x1, `tok_visa` | `CONFIRMED` |
| `SKU-LAMP` x9 (only 5 in stock) | `CANCELLED`, reason `out of stock`, payment never attempted |
| any item, `card_token: "tok_declined"` | `CANCELLED`, reason `card_declined`, reserved stock is released (compensation) |

Demo stock: `SKU-TV-55` 10, `SKU-KETTLE` 25, `SKU-LAMP` 5. Inspect with
`curl localhost:8002/stock`, `localhost:8003/payments`, `localhost:8004/notifications`.
Open the broker UI at <http://localhost:15672> (user and password `maltashop`) to see
queues and the `maltashop.dead` dead-letter queue.

Stop and wipe all data: `docker compose down --volumes`.

## API

| Method and path | Service | Purpose |
|---|---|---|
| `POST /orders` | order | Place an order. Headers: `Idempotency-Key` (optional, makes retries safe), `X-Correlation-ID` (optional, generated if absent, returned in the response). Returns 202, or 200 with the original order on a repeated key. |
| `GET /orders/{id}` | order | Current order and saga status |
| `GET /stock`, `/payments`, `/notifications` | inventory, payment, notification | Read-only views for the demo |
| `GET /health` | all | Liveness: process is up |
| `GET /ready` | all | Readiness: broker queue bound. Used by Docker health checks |

## Environment variables

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `BROKER_URL` | all services | set in `docker-compose.yml` | AMQP URL, e.g. `amqp://user:pass@rabbitmq:5672/%2F` |
| `DB_PATH` | all services | `/data/<service>.db` | Location of the service's own SQLite file (a named volume) |
| `LOG_LEVEL` | all services | `INFO` | Python log level |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | compose | `maltashop` | Broker credentials. Copy `.env.example` to `.env` to override |

## Tests and CI

```bash
python -m unittest discover -s tests -v    # no Docker or packages needed
```

The tests wire the four services together with an in-memory broker and check the happy
path, both compensation paths, duplicate message delivery (stock decremented once, one
charge, one email per kind), idempotency keys, a broker outage, and the JSON log schema.

`.github/workflows/ci.yml` runs three gates in order: tests, image builds, then a full
`docker compose up --wait` followed by `scripts/smoke_test.sh`, which checks the three
scenarios above through the real REST API and broker.

## Design decisions

- **Saga by orchestration.** One place (`order-service/app/logic.py`) holds the whole
  business flow, which is easy to read, test and monitor. Inventory and Payment stay ignorant of orders.
- **At-least-once delivery, idempotent consumers.** Every handler tolerates duplicates:
  reservations are keyed by `order_id`, payments by `order_id`, notifications by
  `(order_id, kind)`, and Order only applies an event from a legal state. A duplicate
  re-publishes the outgoing event, so a crash between "commit" and "publish" cannot strand a saga.
- **Structured logs with a correlation ID.** Each line is JSON with `timestamp`, `level`,
  `service`, `correlation_id`, `message`. The ID enters on the HTTP header, travels in the AMQP
  `correlation_id` property, and is restored in the consumer, so one `grep` follows an order
  across all services.
- **Tokens, not card numbers.** Only a provider token ever enters the system.
- **Poison messages go to a dead-letter queue** instead of looping forever.
- **Docker:** multi-stage build (build tooling stays out of the image), slim base, non-root
  user, health check against `/ready`, all configuration from environment variables.

## Known limitations (what would change before production)

- **No transactional outbox.** The database commit and the event publish are separate steps.
  Duplicate-replay covers a crash after commit, and a failed publish at order creation cancels
  the order explicitly, but an outbox table plus a relay would remove the gap properly.
- **SQLite files** stand in for per-service databases. Production would use a managed
  PostgreSQL instance (or one database per service) with backups.
- **A new broker connection per publish.** Fine for a demo; production would pool channels.
- **No retry with backoff** before dead-lettering, and no tooling to replay dead letters.
- **Single broker node** and **no authentication or rate limiting** on the API; production
  would add an API gateway, TLS and a clustered or managed broker.
- **No tracing or metrics.** Logs only; OpenTelemetry traces and Prometheus metrics would be next.
- **Shared code is copied** (`logging_setup.py`, `messaging.py`) rather than packaged, to keep
  services independently deployable. A small internal library would be the next step.
- **Mock payment provider.** A real one would be called outside the DB transaction with
  `order_id` as its idempotency key.
