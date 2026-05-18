using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// M21 gate: AMQP-over-WebSocket. The bridge accepts a WebSocket upgrade at
/// <c>/$servicebus/websocket/</c> on its own port, then tunnels binary frames to the AMQP
/// listener. With <see cref="ServiceBusTransportType.AmqpWebSockets"/> the SDK builds
/// <c>ws://{host}:{port}/$servicebus/websocket/</c> from the connection string — exactly
/// what the bridge serves.
/// </summary>
public class WebSocketTransportTests
{
    [Fact]
    public async Task SendAndReceive_OverAmqpWebSockets_RoundTripsThroughTheBridge()
    {
        // Arrange — start the broker with the WebSocket bridge enabled on a free port.
        await using var harness = await IntegrationHarness.StartAsync(enableWebSocketBridge: true);
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ws-q" });

        harness.WebSocketConnectionString.ShouldNotBeNull(
            "the bridge must publish a connection string when enabled");

        await using var client = new ServiceBusClient(
            harness.WebSocketConnectionString!,
            new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpWebSockets });

        // Act — full send/receive/complete cycle over the WebSocket transport.
        await client.CreateSender("ws-q").SendMessageAsync(new ServiceBusMessage("ws-hello") { MessageId = "wsm1" });
        var receiver = client.CreateReceiver("ws-q");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        // Assert
        msg.ShouldNotBeNull();
        msg.MessageId.ShouldBe("wsm1");
        msg.Body.ToString().ShouldBe("ws-hello");
        await receiver.CompleteMessageAsync(msg);
        (await harness.Store.CountAsync("ws-q")).ShouldBe(0L);
    }

    [Fact]
    public async Task SameBroker_BothTransports_Coexist()
    {
        // The TCP path and the WebSocket bridge share the underlying broker — proving the
        // bridge doesn't accidentally hijack the listener.
        await using var harness = await IntegrationHarness.StartAsync(enableWebSocketBridge: true);
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "dual-q" });

        await using var tcpClient = new ServiceBusClient(harness.ConnectionString);
        await using var wsClient  = new ServiceBusClient(
            harness.WebSocketConnectionString!,
            new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpWebSockets });

        await tcpClient.CreateSender("dual-q").SendMessageAsync(new ServiceBusMessage("from-tcp") { MessageId = "t1" });
        await wsClient .CreateSender("dual-q").SendMessageAsync(new ServiceBusMessage("from-ws")  { MessageId = "w1" });

        var receiver = wsClient.CreateReceiver("dual-q");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        first.ShouldNotBeNull();
        second.ShouldNotBeNull();

        var seen = new[] { first.MessageId, second.MessageId };
        seen.ShouldContain("t1");
        seen.ShouldContain("w1");
    }
}
