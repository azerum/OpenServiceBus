using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// The first time the broker is exercised end-to-end by the real Azure SDK.
/// If these pass, OpenServiceBus speaks enough Service Bus to be useful.
/// </summary>
public class AzureSdkRoundTripTests
{
    [Fact]
    public async Task CompleteMessageAsync_AfterReceive_RemovesMessageFromQueue()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("orders");
        await sender.SendMessageAsync(new ServiceBusMessage("hello-from-sdk")
        {
            MessageId = "id-1",
            CorrelationId = "corr-1",
            Subject = "the-subject",
        });
        var receiver = client.CreateReceiver("orders", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        // Act
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        await receiver.CompleteMessageAsync(received);
        for (var i = 0; i < 20 && await harness.Store.CountAsync("orders") > 0; i++)
        {
            await Task.Delay(50);
        }

        // Assert
        received.ShouldNotBeNull("the Azure SDK should have received the message");
        received.MessageId.ShouldBe("id-1");
        received.CorrelationId.ShouldBe("corr-1");
        received.Subject.ShouldBe("the-subject");
        received.Body.ToString().ShouldBe("hello-from-sdk");
        (await harness.Store.CountAsync("orders")).ShouldBe(0L);
    }

    [Fact]
    public async Task ReceiveMessageAsync_25MessagesSentInOrder_AreReceivedInTheSameOrder()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "stream" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("stream");
        for (var i = 0; i < 25; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}") { MessageId = $"id-{i}" });
        }
        var receiver = client.CreateReceiver("stream", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        // Act
        var received = new List<string>();
        for (var i = 0; i < 25; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            msg.ShouldNotBeNull($"missing message {i}");
            received.Add(msg.MessageId);
            await receiver.CompleteMessageAsync(msg);
        }

        // Assert
        received.ShouldBe(Enumerable.Range(0, 25).Select(i => $"id-{i}"));
    }

    [Fact]
    public async Task AbandonMessageAsync_AfterReceive_RedeliversTheSameMessage()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "abandon-test" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("abandon-test");
        await sender.SendMessageAsync(new ServiceBusMessage("retry-me") { MessageId = "m-1" });
        var receiver = client.CreateReceiver("abandon-test", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        // Act
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        await receiver.AbandonMessageAsync(first);
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        await receiver.CompleteMessageAsync(second);

        // Assert
        first.ShouldNotBeNull();
        first.MessageId.ShouldBe("m-1");
        second.ShouldNotBeNull("abandoned message must be redelivered");
        second.MessageId.ShouldBe("m-1");
    }
}
