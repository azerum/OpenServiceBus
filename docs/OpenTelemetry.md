# OpenTelemetry

OpenServiceBus emits **traces** and **metrics** through the standard .NET diagnostic
primitives (`System.Diagnostics.ActivitySource` + `System.Diagnostics.Metrics.Meter`).
**No new dependency** in Core - point your OpenTelemetry pipeline at the source name
`OpenServiceBus` and you're done.

## Hook it up

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;
using OpenServiceBus.Core.Diagnostics;

services.AddOpenTelemetry()
    .WithTracing(b => b
        .AddSource(OpenServiceBusDiagnostics.SourceName)
        .AddOtlpExporter())
    .WithMetrics(b => b
        .AddMeter(OpenServiceBusDiagnostics.SourceName)
        .AddOtlpExporter());
```

The constant `OpenServiceBusDiagnostics.SourceName` is `"OpenServiceBus"` - same name for
both ActivitySource and Meter. Standard `OTEL_EXPORTER_OTLP_*` env vars route the data to
Jaeger, Tempo, Honeycomb, Grafana, Datadog, etc.

## Spans (Activities)

| Span name     | Kind       | Where                                                                                 |
| ------------- | ---------- | ------------------------------------------------------------------------------------- |
| `osb.send`    | `Producer` | Every accepted send (queue and topic - incl. batched and tx-commit-replay)            |
| `osb.receive` | `Consumer` | Every message handed to a consumer under peek-lock                                    |
| `osb.settle`  | `Consumer` | Every disposition (`complete` / `abandon` / `defer` / `deadletter` / `transactional`) |

### Tags

Tag keys follow the OpenTelemetry messaging semantic conventions where they apply, with
Service Bus-specific extensions under `messaging.servicebus.*`:

| Key                                       | Value                                                             |
| ----------------------------------------- | ----------------------------------------------------------------- |
| `messaging.system`                        | `"servicebus"`                                                    |
| `messaging.destination.name`              | queue or topic name                                               |
| `messaging.message.id`                    | `properties.message-id`                                           |
| `messaging.message.conversation_id`       | `properties.correlation-id`                                       |
| `messaging.operation.type`                | `send` / `publish` / `receive` / `settle`                         |
| `messaging.servicebus.delivery_count`     | current redelivery attempt                                        |
| `messaging.servicebus.session_id`         | session id when set                                               |
| `messaging.servicebus.disposition_status` | `complete` / `abandon` / `defer` / `deadletter` / `transactional` |
| `messaging.servicebus.dead_letter_reason` | reason on Rejected dispositions                                   |
| `messaging.servicebus.dead_letter_source` | source entity on DLQ counters                                     |
| `osb.fanout.subscribers`                  | (topic publish only) - number of subscribers the message reached  |

All tag keys are exposed as public constants on
[`OpenServiceBusDiagnostics`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/src/OpenServiceBus.Core/Diagnostics/OpenServiceBusDiagnostics.cs).

## Metrics

### Counters

| Name                         | Unit        | Notes                                                   |
| ---------------------------- | ----------- | ------------------------------------------------------- |
| `osb.messages.sent`          | `{message}` | Tagged with `messaging.destination.name`                |
| `osb.messages.received`      | `{message}` | Tagged with destination                                 |
| `osb.messages.dispositioned` | `{message}` | Tagged with destination + `disposition_status`          |
| `osb.messages.deadlettered`  | `{message}` | Tagged with `dead_letter_source` + `dead_letter_reason` |
| `osb.messages.expired`       | `{message}` | TTL sweeper; tagged with destination                    |

### Histograms

| Name                         | Unit        | Notes                                                            |
| ---------------------------- | ----------- | ---------------------------------------------------------------- |
| `osb.message.delivery_count` | `{attempt}` | Observed delivery count at disposition - spots redelivery storms |

### Observable gauges

| Name              | Unit        | Notes                                                                                                                                          |
| ----------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `osb.queue.depth` | `{message}` | Current message count per queue (active + locked + deferred + scheduled). Tagged with destination. One reading per queue per collection cycle. |

## Cost when nothing's listening

Both `ActivitySource.StartActivity(...)` and `Counter<T>.Add(...)` are designed to be
nearly free when no listener is attached:

- `StartActivity` returns `null` immediately if no `ActivityListener` subscribed to the
  source. The `if (activity is not null)` blocks that follow skip the `SetTag` work.
- `Counter<T>.Add` is a single tag-array allocation + dictionary lookup with no listener;
  the actual recording is a no-op.

So you can leave the instrumentation enabled in production without measurable overhead
until you actually wire up an exporter.

## Tests

- [`tests/OpenServiceBus.IntegrationTests/DiagnosticsTests.cs`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/tests/OpenServiceBus.IntegrationTests/DiagnosticsTests.cs)
  - `ActivityListener` + `MeterListener` captures, asserts the semantic-convention tags
    on `osb.send` / `osb.settle`, the DLQ counter tags, and a forced `osb.queue.depth` gauge
    scrape after enqueue.
