# OpenServiceBus

[![CI](https://github.com/mauritsarissen/OpenServiceBus/actions/workflows/ci.yml/badge.svg)](https://github.com/mauritsarissen/OpenServiceBus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A zero-dependency, embeddable Azure Service Bus emulator for .NET. Speaks real AMQP 1.0 so any Service Bus client — including `Azure.Messaging.ServiceBus` and Azure Functions `ServiceBusTrigger` — connects unmodified.

## Why?

Microsoft ships an official Service Bus emulator, but it requires Docker, SQL Server, and is EULA-gated. OpenServiceBus fills the gap: an MIT-licensed broker that runs as a NuGet package inside your unit-test fixture — no containers, no SQL, no external processes.

| | OpenServiceBus | Official emulator |
| --- | --- | --- |
| License | MIT | EULA-gated |
| Embeddable in tests | Yes (`OpenServiceBus.Testing`) | No |
| Dependencies | None | Docker + SQL Server |
| Footprint | ~MB | ~GB |
| Persistence | In-memory (SQLite optional, post-MVP) | SQL Server |

## Quickstart

> Status: in active development. Milestone progress lives in [the implementation plan](#status).

### As an embedded test fixture (planned, M10)

```csharp
await using var host = new OpenServiceBusTestHost();
await host.CreateQueueAsync("orders");

var client = new ServiceBusClient(host.ConnectionString);
var sender = client.CreateSender("orders");
await sender.SendMessageAsync(new ServiceBusMessage("hello"));

var receiver = client.CreateReceiver("orders");
var msg = await receiver.ReceiveMessageAsync();
await receiver.CompleteMessageAsync(msg);
```

### As a standalone process

```bash
dotnet run --project src/OpenServiceBus.Host
```

Then connect with:

```text
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

The `UseDevelopmentEmulator=true` flag tells the Azure SDK to skip TLS and accept the broker on plain TCP 5672.

## Architecture

- `OpenServiceBus.Abstractions` — public contracts (`IMessageStore`, `ILockManager`, `IEntityRegistry`, options records). Zero runtime deps.
- `OpenServiceBus.Core` — domain types (`StoredMessage`, `QueueDescriptor`, `LockToken`).
- `OpenServiceBus.Broker` — `InMemoryMessageStore`, `QueueManager`, `LockManager`, time provider wiring.
- `OpenServiceBus.Amqp` — AMQP 1.0 listener (via [AMQPNetLite.Listener](https://github.com/Azure/amqpnetlite)), link handlers, Service Bus protocol mapping, `$management` and `$cbs` endpoints.
- `OpenServiceBus.Management` — REST API for entity CRUD.
- `OpenServiceBus.Host` — executable hosting the AMQP listener and management API.
- `OpenServiceBus.Testing` — embeddable test fixture (`OpenServiceBusTestHost`).
- `OpenServiceBus.Explorer` — browser-based UI for testing the broker end-to-end with the real `Azure.Messaging.ServiceBus` SDK. Connect, create queues, send and receive messages, complete or abandon under peek-lock. Run with:

  ```bash
  # Terminal 1: broker
  dotnet run --project src/OpenServiceBus.Host

  # Terminal 2: explorer
  dotnet run --project src/OpenServiceBus.Explorer
  # → http://localhost:5400
  ```

  The Explorer grows with each milestone — M4 will surface annotations, M5 adds DLQ + lock-renewal buttons, M7 a scheduling form, etc.

## Status

Active development. See [the implementation plan](docs/PLAN.md) for the milestone roadmap.

MVP target: Queues end-to-end with the Azure SDK and Azure Functions `ServiceBusTrigger`.

Out of scope for v1.0 (post-MVP): Topics + Subscriptions, Sessions, Duplicate detection, Auto-forwarding, Transactions, persistent storage (SQLite/SQL), Docker image.

## Development

```bash
dotnet build
dotnet test
```

Targets: `net8.0;net10.0` for libraries; `net10.0` for the executable host.

## License

[MIT](LICENSE)
