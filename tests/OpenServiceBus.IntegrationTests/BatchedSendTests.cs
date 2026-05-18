using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Batched sends via <c>SendMessagesAsync</c> and <c>ServiceBusMessageBatch</c> produce a single
/// AMQP transfer with <c>message-format=0x80013700</c> and a body of multiple Data sections.
/// The broker must split the envelope so each inner message gets its own sequence number.
/// </summary>
public class BatchedSendTests
{
    [Fact]
    public async Task SendMessagesAsync_ArrayBatchOfFour_SplitsIntoFourIndividualMessagesWithOwnSequenceNumbers()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "batched" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("batched");
        var batch = new[]
        {
            new ServiceBusMessage("first")  { MessageId = "id-1" },
            new ServiceBusMessage("second") { MessageId = "id-2" },
            new ServiceBusMessage("third")  { MessageId = "id-3" },
            new ServiceBusMessage("fourth") { MessageId = "id-4" },
        };

        // Act
        await sender.SendMessagesAsync(batch);
        var receiver = client.CreateReceiver("batched");
        var ids = new List<string>();
        for (var i = 0; i < 4; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            msg.ShouldNotBeNull($"missing message {i}");
            ids.Add(msg.MessageId);
            msg.SequenceNumber.ShouldBe(i + 1L, $"each inner message gets its own sequence number ({i + 1})");
            await receiver.CompleteMessageAsync(msg);
        }

        // Assert
        (await harness.Store.CountAsync("batched")).ShouldBe(0L);
        ids.ShouldBe(new[] { "id-1", "id-2", "id-3", "id-4" });
    }

    [Fact]
    public async Task SendMessagesAsync_ServiceBusMessageBatch_DeliversEachAddedMessageIndividually()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "explicit-batch" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("explicit-batch");
        using var batch = await sender.CreateMessageBatchAsync();
        batch.TryAddMessage(new ServiceBusMessage("a") { MessageId = "a" }).ShouldBeTrue();
        batch.TryAddMessage(new ServiceBusMessage("b") { MessageId = "b" }).ShouldBeTrue();
        batch.TryAddMessage(new ServiceBusMessage("c") { MessageId = "c" }).ShouldBeTrue();

        // Act
        await sender.SendMessagesAsync(batch);

        // Assert
        (await harness.Store.CountAsync("explicit-batch")).ShouldBe(3L);
        var receiver = client.CreateReceiver("explicit-batch");
        for (var i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            msg.ShouldNotBeNull();
            await receiver.CompleteMessageAsync(msg);
        }
    }
}
