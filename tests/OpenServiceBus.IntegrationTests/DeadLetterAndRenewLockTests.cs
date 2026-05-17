using Azure.Messaging.ServiceBus;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Full Azure SDK exercise of M5: explicit dead-letter via <c>DeadLetterMessageAsync</c>,
/// lock renewal via <c>RenewMessageLockAsync</c>, and auto-dead-letter when MaxDeliveryCount is exceeded.
/// </summary>
public class DeadLetterAndRenewLockTests
{
    [Fact]
    public async Task DeadLetterMessageAsync_with_reason_and_description_moves_message_to_DLQ()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("orders");
        await sender.SendMessageAsync(new ServiceBusMessage("bad-order") { MessageId = "ord-1" });

        var receiver = client.CreateReceiver("orders");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();

        await receiver.DeadLetterMessageAsync(msg, "FraudCheckFailed", "Card flagged");

        // The DLQ receiver should now see the message with reason/description and source set.
        var dlqReceiver = client.CreateReceiver("orders", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("ord-1");
        dlqMsg.DeadLetterReason.ShouldBe("FraudCheckFailed");
        dlqMsg.DeadLetterErrorDescription.ShouldBe("Card flagged");
        dlqMsg.DeadLetterSource.ShouldBe("orders");

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task AbandonMessageAsync_loop_past_MaxDeliveryCount_dead_letters_with_default_reason()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "flaky", MaxDeliveryCount = 3 });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("flaky");
        await sender.SendMessageAsync(new ServiceBusMessage("retry-me") { MessageId = "f-1" });

        var receiver = client.CreateReceiver("flaky");
        for (var i = 0; i < 3; i++)
        {
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            m.ShouldNotBeNull($"attempt {i + 1}");
            await receiver.AbandonMessageAsync(m);
        }

        // The 4th receive on the main queue should yield null (the message has been dead-lettered).
        var none = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        none.ShouldBeNull();

        var dlqReceiver = client.CreateReceiver("flaky", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("f-1");
        dlqMsg.DeadLetterReason.ShouldBe("MaxDeliveryCountExceeded");
        dlqMsg.DeadLetterSource.ShouldBe("flaky");

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task RenewMessageLockAsync_extends_the_peek_lock()
    {
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

        await receiver.RenewMessageLockAsync(msg);

        // After renew, the SDK's LockedUntil on the same message instance should be later.
        msg.LockedUntil.ShouldBeGreaterThan(originalLockedUntil, "lock must have been extended");

        await receiver.CompleteMessageAsync(msg);
    }
}
