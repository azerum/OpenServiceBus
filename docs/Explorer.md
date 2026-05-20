# Explorer UI

A browser-based console for exploring queues + topics, sending and receiving messages,
managing rules, and watching dispositions during development.

```bash
# Terminal 1: broker
dotnet run --project src/OpenServiceBus.Host

# Terminal 2: Explorer
dotnet run --project src/OpenServiceBus.Explorer
```

Open <http://localhost:5400>. The Explorer talks to the broker's REST management API
(default `http://localhost:5300`) - no AMQP knowledge needed in the UI.

## Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│ OpenServiceBus / orders                          ● connected  ⚙ ☾  │
├─────────────┬───────────────────────────────────────────────────────┤
│ Search…     │  orders                              [Queue]          │
│             │  12 active · lock 60s · ttl ∞                         │
│ QUEUES   3  │                                                       │
│ • orders 12 │  Overview | Send | Receive (3) | Rules                │
│   $DLQ    0 │  ─────────                                            │
│ • billing 0 │                                                       │
│             │  (selected tab content)                               │
│ TOPICS   2  │                                                       │
│ ▼ events    │                                                       │
│   ↳ all   5 │                                                       │
│   ↳ eu    3 │                                                       │
│ ▼ logs      │                                                       │
│             │                                                       │
│ ⚙ Connection│                                                       │
└─────────────┴───────────────────────────────────────────────────────┘
```

## Features

- **Entity tree** - queues + DLQ siblings, topics with collapsible subscription children
  and per-subscription DLQs. Live filter box. Click any entity to inspect.
- **Create dropdown** - modals for queue, topic, subscription with **every** feature flag
  exposed: lock duration, max-delivery, TTL, sessions, dedup + window,
  forward-to + forward-DLQ-to.
- **Overview tab** - full property dump, quick-action buttons (Send, Receive, Manage rules).
- **Send tab** - body editor, advanced fields (CorrelationId, Subject, ContentType,
  SessionId, PartitionKey, TTL, scheduled-for) and a custom application-properties editor
  (key/value rows). One click = real SDK send through the broker.
- **Receive tab** - peek-lock messages stay locked until you settle them. Each message
  shows id, sequence, delivery count, lock deadline, expires-at, dead-letter info,
  application properties. Disposition buttons grouped by intent: Complete (success),
  Abandon / Renew / Defer (neutral), DLQ (danger). Session ID input surfaces on
  session-enabled entities only.
- **Rules tab** (subscriptions only) - SQL / Correlation / True / False editor with
  examples in the help text. `$Default` rule visually distinguished from custom rules.
- **Light/dark theme** with persisted preference.

## Connection panel

Bottom of the sidebar is a collapsible **Connection** drawer with the SDK connection
string and management URL. Persisted in localStorage so it survives refreshes.

The "Connect" button does both a `/health` probe (REST) and an SDK `ServiceBusClient`
construction (AMQP) - green chip = both working.

## Architecture note

The Explorer is its own ASP.NET Core app (`OpenServiceBus.Explorer`) hosting a static HTML
page + a thin proxy at `/api/*` that:

- Proxies queue/topic/subscription/rule CRUD straight to the broker's REST API.
- Translates "receive next" / "complete" / "abandon" / etc. into real Azure SDK calls
  against the broker over AMQP - so what you exercise in the UI is exactly what your
  production code would exercise.

This means the Explorer is **not** a peek window into broker internals; it's a real
client. The lock you take from the UI is a real peek-lock; abandoning it actually
increments delivery count; dead-lettering routes to the real DLQ.

## See also

- [Configuration](Configuration) - every per-entity setting the modals expose.
- [Architecture](Architecture) - how the Explorer fits in the assembly graph.
