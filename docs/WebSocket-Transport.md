# WebSocket Transport

For clients that can't open raw AMQP TCP - browsers, restrictive corporate firewalls, some
PaaS sandboxes - OpenServiceBus exposes the same AMQP pipeline over a WebSocket bridge at
`/$servicebus/websocket/`. The Azure SDK with `TransportType.AmqpWebSockets` connects
unmodified.

## Enable it

### Standalone host / Docker

Flip the env var:

```bash
export OPENSERVICEBUS__WEBSOCKETS__ENABLED=true
export OPENSERVICEBUS__WEBSOCKETS__PORT=5673          # optional, this is the default
dotnet run --project src/OpenServiceBus.Host
```

In Docker:

```bash
docker run -d --name openservicebus \
  -p 5672:5672 -p 5300:5300 -p 5673:5673 \
  -e OPENSERVICEBUS__WEBSOCKETS__ENABLED=true \
  -v osb-data:/data \
  ghcr.io/mauritsarissen/openservicebus:latest
```

### Test fixture

```csharp
await using var host = await OpenServiceBusTestHost.StartAsync(o =>
{
    o.EnableWebSocketBridge = true;
});

Console.WriteLine(host.WebSocketPort);                // free ephemeral port
Console.WriteLine(host.WebSocketConnectionString);    // ready for AmqpWebSockets transport
```

## Client side

```csharp
await using var client = new ServiceBusClient(
    host.WebSocketConnectionString!,
    new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpWebSockets });

await client.CreateSender("orders").SendMessageAsync(new ServiceBusMessage("hi"));
```

The SDK builds `ws://{host}:{port}/$servicebus/websocket/` from the connection string's
endpoint, adds `Sec-WebSocket-Protocol: amqp`, upgrades, and starts streaming AMQP frames
as binary WebSocket messages.

`UseDevelopmentEmulator=true` keeps the scheme `ws://` (no TLS). With a real TLS-enabled
deployment the SDK would use `wss://` - out of scope for v1 since the broker is local.

## How it works

```
┌──────────┐    WebSocket    ┌──────────────────┐   loopback TCP   ┌──────────────────┐
│ SDK      │ ───────────────►│ WebSocketBridge  │ ────────────────►│ AmqpListenerHost │
│ (Amqp    │ ◄───────────────│ (HttpListener +  │◄──────────────── │ (AMQPNetLite     │
│ WebSocks)│   ws frames     │  byte pump)      │   raw AMQP       │  ContainerHost)  │
└──────────┘                 └──────────────────┘                  └──────────────────┘
                              :5673                                 :5672
```

1. ASP.NET Core hosted service `WebSocketBridgeService` opens an `HttpListener` on the
   configured port + path.
2. On every incoming WebSocket upgrade, the bridge accepts the `amqp` sub-protocol, opens
   a loopback TCP connection to AMQPNetLite's existing AMQP port, and starts two parallel
   pump tasks: WebSocket → TCP and TCP → WebSocket.
3. The bridge looks like just another TCP client to AMQPNetLite - every existing feature
   ($cbs, $management, sessions, transactions, auto-forwarding, …) works unchanged.

This is intentionally a pump and not a deeper integration: it keeps the AMQP pipeline
identical, and the loopback hop has negligible latency.

## Why a separate port?

AMQPNetLite owns port 5672 with a raw `Socket`, so we can't co-host HTTP on it. The bridge
sits on `5673` by default - adjust via `OpenServiceBus:WebSockets:Port` if you want to
match Azure's port-443 deployment shape.

If you only expose one port externally (e.g. behind a corporate proxy), point that proxy at
`:5673` and forget about `:5672`.

## Tests

- [`tests/OpenServiceBus.IntegrationTests/WebSocketTransportTests.cs`](https://github.com/mauritsarissen/OpenServiceBus/blob/main/tests/OpenServiceBus.IntegrationTests/WebSocketTransportTests.cs)
  - SDK round-trip via `TransportType=AmqpWebSockets`, plus a "both transports coexist"
    test proving TCP and WebSocket connect to the same backing broker.
