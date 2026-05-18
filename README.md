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
| Config format | Microsoft `config.json` compatible | Microsoft `config.json` |

## Feature coverage (v1.0)

Queues end-to-end with the `Azure.Messaging.ServiceBus` SDK and Azure Functions `ServiceBusTrigger`:

- Peek-lock receive (`PeekLock` and `ReceiveAndDelete` modes)
- Complete / Abandon / Dead-letter via dispositions and `$management`
- Lock renewal (`RenewMessageLockAsync`)
- Dead-letter queue as a triggerable sub-entity (`<queue>/$DeadLetterQueue`)
- TTL per message and per queue (smaller wins; optional auto-DLQ on expiration)
- Scheduled messages (`ScheduleMessageAsync`, `CancelScheduledMessageAsync`)
- Defer + receive-by-sequence-number + update-disposition
- Batched send (`SendMessagesAsync`) split into individual stored messages
- Peek via `$management com.microsoft:peek-message`
- Lock-token = delivery-tag UUID — matches Service Bus wire format
- Standard AMQP properties + `x-opt-*` annotations (sequence, enqueued time, locked-until, delivery-count, deadletter-source, scheduled-enqueue-time, message-state)
- Optional SAS auth via `$cbs put-token` (flag-gated, default off)
- Bootstrap queues from Microsoft-emulator-compatible `config.json`

Out of scope for v1.0 (post-MVP): Topics + Subscriptions, Sessions, Duplicate detection, Auto-forwarding, Transactions, persistent storage (SQLite/SQL), Docker image.

## Quickstart

### As an embedded test fixture

```csharp
await using var host = await OpenServiceBusTestHost.StartAsync();
await host.CreateQueueAsync("orders");

await using var client = new ServiceBusClient(host.ConnectionString);
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

Then connect with the standard Service Bus emulator connection string:

```text
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

The `UseDevelopmentEmulator=true` flag tells the Azure SDK to skip TLS and accept the broker on plain TCP 5672.

### Declarative bootstrap with `config.json`

OpenServiceBus reads the same `config.json` format as the official Microsoft Service Bus emulator. Drop a file like this next to the host and queues are declared at startup:

```json
{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "openservicebus-demo",
        "Queues": [
          {
            "Name": "orders",
            "Properties": {
              "LockDuration": "PT1M",
              "MaxDeliveryCount": 3,
              "DefaultMessageTimeToLive": "PT1H",
              "DeadLetteringOnMessageExpiration": true
            }
          }
        ]
      }
    ]
  }
}
```

Resolution order:

1. `--config <path>` CLI argument
2. `OPENSERVICEBUS_CONFIG` environment variable
3. `config.json` in the host's content-root directory

Fields that map to post-MVP features (`RequiresSession`, `RequiresDuplicateDetection`, `ForwardTo`, `Topics`, …) are accepted for compatibility but logged as warnings.

A working example lives at [`samples/config.sample.json`](samples/config.sample.json).

## Architecture

- `OpenServiceBus.Core` — domain types (`StoredMessage`, `QueueDescriptor`, `LockToken`), storage abstractions, `config.json` POCOs + loader.
- `OpenServiceBus.InMemoryStorage` — `InMemoryMessageStore`, `QueueManager`, `LockManager`, TTL/scheduled sweepers, DI helpers.
- `OpenServiceBus.Amqp` — AMQP 1.0 listener (via [AMQPNetLite.Listener](https://github.com/Azure/amqpnetlite)), link handlers, Service Bus protocol mapping, `$management` and `$cbs` endpoints, SAS validation.
- `OpenServiceBus.Management` — REST API for entity CRUD.
- `OpenServiceBus.Host` — executable hosting the AMQP listener + management API + `config.json` bootstrap.
- `OpenServiceBus.Testing` — embeddable test fixture (`OpenServiceBusTestHost`).
- `OpenServiceBus.Explorer` — browser-based UI for testing the broker end-to-end. Create queues, send and receive messages, complete or abandon under peek-lock.

```bash
# Terminal 1: broker (AMQP :5672, management REST :5300)
dotnet run --project src/OpenServiceBus.Host

# Terminal 2: explorer UI (:5400)
dotnet run --project src/OpenServiceBus.Explorer
```

## Samples

- [`samples/OpenServiceBus.Functions.Sample`](samples/OpenServiceBus.Functions.Sample) — minimal isolated-worker Functions app used by the M11 "feels real" integration test.
- [`samples/TriggerDemo`](samples/TriggerDemo) — interactive Functions app you start with `func start`. Exercises peek-lock, retry-to-DLQ, DLQ trigger, manual disposition, batched receive, and HTTP-driven output bindings — all against a running OpenServiceBus.Host instance.
- [`samples/config.sample.json`](samples/config.sample.json) — sample `config.json` for declarative queue bootstrap.

## Development

```bash
dotnet build
dotnet test
```

Targets: `net8.0;net10.0` for libraries; `net10.0` for the executable host.

The Azure Functions integration test (M11) requires `func` (Azure Functions Core Tools v4) and the .NET 8 runtime; it skips gracefully when either is missing.

## Status

v1.0 — Queues, the Azure SDK, and Azure Functions triggers covered end-to-end. See [the implementation plan](docs/PLAN.md) for the roadmap.

## License

[MIT](LICENSE)
