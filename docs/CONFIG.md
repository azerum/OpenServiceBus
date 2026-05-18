# `config.json` — declarative queue bootstrap

OpenServiceBus reads the same `config.json` format as the official Microsoft Service Bus
emulator, so files can be lifted across without modification. The file is consumed once at
host startup and applied to the queue registry via `IQueueRegistry.CreateAsync`.

## Resolution

The host looks for the file, in order:

1. **CLI flag** — `dotnet run --project src/OpenServiceBus.Host -- --config /path/to/config.json`
2. **Environment variable** — `OPENSERVICEBUS_CONFIG=/path/to/config.json`
3. **Default** — `config.json` in the host's content-root directory

If none of these resolves to an existing file, the host starts without pre-declared queues
and logs an informational message. Parse errors are logged but do **not** stop the host.

## Schema

```json
{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "<arbitrary label>",
        "Queues": [
          {
            "Name": "queue-name",
            "Properties": {
              "LockDuration": "PT1M",
              "MaxDeliveryCount": 10,
              "DefaultMessageTimeToLive": "PT1H",
              "DeadLetteringOnMessageExpiration": false,
              "RequiresSession": false,
              "RequiresDuplicateDetection": false,
              "DuplicateDetectionHistoryTimeWindow": "PT20S",
              "ForwardTo": "",
              "ForwardDeadLetteredMessagesTo": ""
            }
          }
        ],
        "Topics": []
      }
    ],
    "Logging": {
      "Type": "File"
    }
  }
}
```

### Supported fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `Name` | string | _required_ | Queue name. Must be non-empty. |
| `LockDuration` | ISO 8601 duration | `PT1M` | Peek-lock duration; e.g. `PT30S`, `PT5M`. |
| `MaxDeliveryCount` | int | `10` | Auto-DLQ after this many abandons. |
| `DefaultMessageTimeToLive` | ISO 8601 duration | `null` | When unset, messages never expire. |
| `DeadLetteringOnMessageExpiration` | bool | `false` | When true, expired messages move to the DLQ. |

### Accepted but not honoured (warnings logged)

These map to features tracked in the post-MVP roadmap and are accepted for compatibility so
existing emulator configs don't need editing:

| Field | Reason |
| --- | --- |
| `RequiresSession` | Sessions land in M14. |
| `RequiresDuplicateDetection` / `DuplicateDetectionHistoryTimeWindow` | Duplicate detection lands in M15. |
| `ForwardTo` / `ForwardDeadLetteredMessagesTo` | Auto-forwarding lands in M16. |
| `Topics` | Topics + Subscriptions land in M13. |

## Example

A working `config.json` is included at [`samples/config.sample.json`](../samples/config.sample.json).
Run the host against it with:

```bash
dotnet run --project src/OpenServiceBus.Host -- --config samples/config.sample.json
```

You should see one log line per queue:

```
info: OpenServiceBus.Host.ConfigBootstrapHostedService[0]
      Bootstrapped queue 'orders' (lockDuration=00:01:00, maxDeliveryCount=3, ttl=01:00:00, dlqOnExpire=True)
```

## Programmatic loader

The parser is exposed publicly for tests and tooling that need to inspect or transform a
config file without booting a host:

```csharp
using OpenServiceBus.Core.Configuration;

var result = EmulatorConfigLoader.LoadFromFile("config.json");

foreach (var warning in result.Warnings)
    Console.WriteLine($"warning: {warning}");

foreach (var queue in result.Queues)
    Console.WriteLine($"queue: {queue.Name} lock={queue.LockDuration}");
```
