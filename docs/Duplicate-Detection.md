# Duplicate Detection

Enable `RequiresDuplicateDetection` on a queue (or subscription, via the underlying
backing queue) and the broker silently drops repeat sends with the same `MessageId`
within a sliding time window. The sender sees a normal "accepted" disposition either
way - same observable behavior as Azure Service Bus.

## Enable it

```csharp
await host.Queues.CreateAsync(new QueueDescriptor
{
    Name = "deduped",
    RequiresDuplicateDetection = true,
    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5),
});
```

Default window when null: **10 minutes** (matches Service Bus default).

Or in `config.json`:

```json
{
  "Name": "deduped",
  "Properties": {
    "RequiresDuplicateDetection": true,
    "DuplicateDetectionHistoryTimeWindow": "PT5M"
  }
}
```

## How it works

```csharp
var sender = client.CreateSender("deduped");
await sender.SendMessageAsync(new ServiceBusMessage("first")  { MessageId = "k" });
await sender.SendMessageAsync(new ServiceBusMessage("second") { MessageId = "k" }); // silently dropped
await sender.SendMessageAsync(new ServiceBusMessage("third")  { MessageId = "other" });

// Queue contains 2 messages: "first" (MessageId k) and "third" (MessageId other).
```

The second send returns the **original** `StoredMessage` to internal callers - so anything
tracking sequence numbers stays consistent. The wire-level disposition is still
`Accepted` - the SDK has no idea the dup was dropped.

After the window passes, the same `MessageId` is treated as a fresh send:

```csharp
await sender.SendMessageAsync(new ServiceBusMessage("first") { MessageId = "k" });

await Task.Delay(TimeSpan.FromMinutes(6)); // window expires

await sender.SendMessageAsync(new ServiceBusMessage("new")   { MessageId = "k" }); // accepted, new seq number
```

## What counts as a `MessageId`?

Whatever AMQP carries in `properties.message-id`. The Azure SDK auto-generates one if you
don't set it - but if every message has a unique auto-generated id, dedup is a no-op.
For dedup to be meaningful, **the sender must set a deterministic `MessageId`** (e.g.
based on a business event id) so retries get the same value.

## Storage

- In-memory store: per-queue `ConcurrentDictionary<string, DateTimeOffset>` + a lazy sweep
  on each check. Cheap because expired entries get purged on the next dedup query.
- SQLite store: `dedup_history` table indexed on `(queue_name, expires_at)` - also lazily
  swept before each dedup check.

## Tests

- [`tests/OpenServiceBus.IntegrationTests/DuplicateDetectionTests.cs`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/tests/OpenServiceBus.IntegrationTests/DuplicateDetectionTests.cs)
  - SDK-level: same-MessageId-twice drops the dup, different-MessageId both survive.
- [`tests/OpenServiceBus.InMemoryStorage.Tests/DuplicateDetectionTests.cs`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/tests/OpenServiceBus.InMemoryStorage.Tests/DuplicateDetectionTests.cs)
  - store-level: window expiry, original-message return.
- [`tests/OpenServiceBus.SqliteStorage.Tests/SqliteMessageStoreTests.cs`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/tests/OpenServiceBus.SqliteStorage.Tests/SqliteMessageStoreTests.cs)
  - same coverage against the persistent store.
