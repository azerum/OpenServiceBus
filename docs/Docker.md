# Docker

The OpenServiceBus container image is multi-stage Alpine, runs as a non-root user, and
defaults to SQLite-backed storage so messages survive container recreates when you mount a
volume.

## Pull

```bash
docker pull ghcr.io/mauritsarissen/openservicebus:latest
# or
docker pull mauritsarissen/openservicebus:latest
```

Both registries are kept in sync by the release pipeline.

## Ports

| Port   | Protocol       | When                                                   |
| ------ | -------------- | ------------------------------------------------------ |
| `5672` | AMQP           | Always exposed - Service Bus SDK + AMQPNetLite clients |
| `5300` | HTTP           | Always exposed - management REST API + `/health`       |
| `5673` | HTTP/WebSocket | Only when `OPENSERVICEBUS__WEBSOCKETS__ENABLED=true`   |

## One-shot run

```bash
docker run -d --name openservicebus \
  -p 5672:5672 -p 5300:5300 \
  -v osb-data:/data \
  ghcr.io/mauritsarissen/openservicebus:latest
```

Then point the SDK at:

```text
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

## docker-compose

The repo ships a [`docker-compose.yml`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/docker-compose.yml)
you can copy:

```yaml
services:
  openservicebus:
    image: ghcr.io/mauritsarissen/openservicebus:latest
    container_name: openservicebus
    ports:
      - "5672:5672"
      - "5300:5300"
    environment:
      OPENSERVICEBUS__STORAGE__MODE: Sqlite
      OPENSERVICEBUS__STORAGE__DATASOURCE: /data/broker.db
      # OPENSERVICEBUS_CONFIG: /etc/openservicebus/config.json
    volumes:
      - osb-data:/data
      # - ./config.json:/etc/openservicebus/config.json:ro
    restart: unless-stopped

volumes:
  osb-data:
```

`docker compose up -d` and you've got a persistent broker.

## Environment variables

| Variable                               | Default in image  | Notes                                                                     |
| -------------------------------------- | ----------------- | ------------------------------------------------------------------------- |
| `OPENSERVICEBUS__STORAGE__MODE`        | `Sqlite`          | Override to `InMemory` for ephemeral mode                                 |
| `OPENSERVICEBUS__STORAGE__DATASOURCE`  | `/data/broker.db` | Where the SQLite file lives - must be on a mounted volume for persistence |
| `OPENSERVICEBUS__AMQP__PORT`           | `5672`            | AMQP listener                                                             |
| `OPENSERVICEBUS__AMQP__REQUIRESASAUTH` | `false`           | Flip to `true` and provide `__SASKEYS__<name>` to enforce SAS             |
| `OPENSERVICEBUS__WEBSOCKETS__ENABLED`  | `false`           | Start the AMQP-over-WebSocket bridge                                      |
| `OPENSERVICEBUS__WEBSOCKETS__PORT`     | `5673`            | WebSocket port                                                            |
| `OPENSERVICEBUS_CONFIG`                | -                 | Path to a mounted `config.json` for declarative bootstrap                 |
| `ASPNETCORE_URLS`                      | `http://+:5300`   | Kestrel binding (rarely changed)                                          |

Full reference: [Configuration](Configuration).

## Persistence

The container persists messages **only if** you mount `/data`. Without a volume:

- Container running: queues and messages survive in `/data/broker.db` inside the layer.
- Container removed: the broker.db disappears with the layer.

With a named volume (`-v osb-data:/data`) the same `broker.db` is reused across container
recreates - proven in the M19 smoke test (send via SDK → `docker stop && rm` → `docker run`
from the same volume → SDK receives the same message).

> ⚠️ Queue descriptors (LockDuration, MaxDeliveryCount, RequiresSession, etc.) are
> reset to defaults on restart because the in-memory `QueueManager` rebuilds from
> the store's queue names only. **Use `config.json` to make per-queue settings durable.**

See [Persistence](Persistence) for the SQLite schema and restart semantics.

## Bootstrap with `config.json`

Mount a config file as read-only and point `OPENSERVICEBUS_CONFIG` at it:

```bash
docker run -d --name openservicebus \
  -p 5672:5672 -p 5300:5300 \
  -v osb-data:/data \
  -v $PWD/config.json:/etc/openservicebus/config.json:ro \
  -e OPENSERVICEBUS_CONFIG=/etc/openservicebus/config.json \
  ghcr.io/mauritsarissen/openservicebus:latest
```

On startup the host parses the file, creates the declared queues + topics + subscriptions
with the right per-entity settings, then opens the listener. See
[Configuration](Configuration) for the schema.

## Healthcheck

The image declares a healthcheck against `/health`:

```dockerfile
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:5300/health || exit 1
```

So `docker ps` reports `healthy` / `unhealthy` based on whether the broker can serve
HTTP - useful for compose health gates.

## Building locally

The repo's [`Dockerfile`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/Dockerfile)
is the same one the release pipeline uses:

```bash
docker build -t openservicebus:dev .
docker run --rm -p 5672:5672 -p 5300:5300 openservicebus:dev
```

The build is layer-cache friendly - `.csproj` files copied first for restore, source second
for compile. Cold build: ~30s. Warm rebuild (source-only edit): a few seconds.
