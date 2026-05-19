# Getting Started

Three install paths cover essentially every use case. Pick whichever matches yours.

## 1. Embedded test fixture (recommended for tests)

```bash
dotnet add package OpenServiceBus.Testing
```

`OpenServiceBus.Testing` ships an `OpenServiceBusTestHost` that boots a full broker on a
free ephemeral port. No containers, no SQL Server, no external processes - the entire
broker lives in your test process.

```csharp
using Azure.Messaging.ServiceBus;
using OpenServiceBus.Testing;

await using var host = await OpenServiceBusTestHost.StartAsync();
await host.CreateQueueAsync("orders");

await using var client = new ServiceBusClient(host.ConnectionString);
await client.CreateSender("orders").SendMessageAsync(new ServiceBusMessage("hi"));

var receiver = client.CreateReceiver("orders");
var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
await receiver.CompleteMessageAsync(msg);
```

Common knobs:

```csharp
await using var host = await OpenServiceBusTestHost.StartAsync(o =>
{
    o.Port = 5672;                              // fixed port (default: free ephemeral)
    o.TimeProvider = new FakeTimeProvider();    // drive TTL/lock/schedule deterministically
    o.RequireSasAuth = true;                    // enable $cbs validation
    o.EnableWebSocketBridge = true;             // expose ws:// transport too
});
```

The test host exposes `host.Queues`, `host.Topics`, `host.Store` (the raw `IMessageStore`)
so tests can poke at internal state directly. Disposing the host releases the port.

→ See [Testing](Testing) for advanced patterns: parameterized stores, time-travel, parity tests.

## 2. Docker container (persistent, container-orchestrated)

```bash
docker run -d --name openservicebus \
  -p 5672:5672 \
  -p 5300:5300 \
  -v osb-data:/data \
  ghcr.io/mauritsarissen/openservicebus:latest
```

The default image runs SQLite-backed at `/data/broker.db` - mount the volume and your
queues + messages survive recreates. See [Docker](Docker) for compose files, every
env var, and the WebSocket recipe.

## 3. From source / standalone host

```bash
git clone https://github.com/mauritsarissen/OpenServiceBus
cd OpenServiceBus
dotnet run --project src/OpenServiceBus.Host
```

The host binds:

- `amqp://localhost:5672` - AMQP listener (Service Bus SDK + AMQPNetLite clients)
- `http://localhost:5300` - REST management API + `/health`

To run the Explorer UI alongside:

```bash
dotnet run --project src/OpenServiceBus.Explorer
# Open http://localhost:5400
```

## Connection string

The Azure SDK accepts the same connection string against all three install paths:

```text
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

`UseDevelopmentEmulator=true` makes the SDK skip TLS and dial plain TCP - same trick the
Microsoft emulator uses. Replace `localhost:5672` with whatever the host/port pair is
(test fixtures use ephemeral ports - read `host.ConnectionString` instead of hardcoding).

## What to read next

- **[Configuration](Configuration)** for every env var, the `config.json` schema, and
  declarative queue/topic bootstrap.
- **[Topics and Subscriptions](Topics-and-Subscriptions)** if you need pub-sub with filters.
- **[Persistence](Persistence)** if you need messages to survive a process restart.
- **[OpenTelemetry](OpenTelemetry)** to plug the broker into Jaeger/Honeycomb/Grafana/etc.
