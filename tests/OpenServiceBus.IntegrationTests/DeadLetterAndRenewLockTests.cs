using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Full Azure SDK exercise: explicit dead-letter via <c>DeadLetterMessageAsync</c>,
/// lock renewal via <c>RenewMessageLockAsync</c>, and auto-dead-letter when MaxDeliveryCount is exceeded.
/// </summary>
public class DeadLetterAndRenewLockTests
{
    [Fact]
    public async Task DeadLetterMessageAsync_WithReasonAndDescription_MovesMessageToDlqWithFieldsPopulated()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("orders");
        await sender.SendMessageAsync(new ServiceBusMessage("bad-order") { MessageId = "ord-1" });
        var receiver = client.CreateReceiver("orders");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();

        // Act
        await receiver.DeadLetterMessageAsync(msg, "FraudCheckFailed", "Card flagged");
        var dlqReceiver = client.CreateReceiver("orders", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        // Assert
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("ord-1");
        dlqMsg.DeadLetterReason.ShouldBe("FraudCheckFailed");
        dlqMsg.DeadLetterErrorDescription.ShouldBe("Card flagged");
        dlqMsg.DeadLetterSource.ShouldBe("orders");
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task AbandonMessageAsync_LoopPastMaxDeliveryCount_DeadLettersWithMaxDeliveryCountExceededReason()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "flaky", MaxDeliveryCount = 3 });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("flaky");
        await sender.SendMessageAsync(new ServiceBusMessage("retry-me") { MessageId = "f-1" });
        var receiver = client.CreateReceiver("flaky");

        // Act
        for (var i = 0; i < 3; i++)
        {
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            m.ShouldNotBeNull($"attempt {i + 1}");
            await receiver.AbandonMessageAsync(m);
        }
        var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        var dlqReceiver = client.CreateReceiver("flaky", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        // Assert
        none.ShouldBeNull("message is no longer in the main queue after auto-DLQ");
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("f-1");
        dlqMsg.DeadLetterReason.ShouldBe("MaxDeliveryCountExceeded");
        dlqMsg.DeadLetterSource.ShouldBe("flaky");
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task RenewMessageLockAsync_OnPeekLockedMessage_ExtendsLockedUntilTimestamp()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "longwork",
            LockDuration = TimeSpan.FromSeconds(20),
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("longwork");
        await sender.SendMessageAsync(new ServiceBusMessage("slow") { MessageId = "lw-1" });
        var receiver = client.CreateReceiver("longwork");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        var originalLockedUntil = msg.LockedUntil;

        // Act
        await receiver.RenewMessageLockAsync(msg);

        // Assert
        msg.LockedUntil.ShouldBeGreaterThan(originalLockedUntil, "lock must have been extended");
        await receiver.CompleteMessageAsync(msg);
    }
}
