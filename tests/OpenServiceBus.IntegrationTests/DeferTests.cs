using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// M8 defer flow via the real Azure SDK:
///   1. <c>DeferMessageAsync</c> parks the message in Deferred state.
///   2. <c>ReceiveDeferredMessagesAsync</c> retrieves by sequence number.
///   3. The retrieved message can be Completed / Abandoned / Dead-lettered via <c>$management</c>.
/// </summary>
public class DeferTests
{
    [Fact]
    public async Task DeferMessageAsync_hides_the_message_from_normal_receive_and_ReceiveDeferred_finds_it()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "defer-flow" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("defer-flow");
        await sender.SendMessageAsync(new ServiceBusMessage("payload") { MessageId = "m-1" });

        var receiver = client.CreateReceiver("defer-flow");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        var deferredSeq = first.SequenceNumber;

        await receiver.DeferMessageAsync(first);

        // Normal receive must NOT return the deferred message.
        var ghost = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        ghost.ShouldBeNull("deferred messages are invisible to the normal receive path");

        // ReceiveDeferred by sequence number brings it back.
        var deferred = await receiver.ReceiveDeferredMessagesAsync(new[] { deferredSeq });
        deferred.Count.ShouldBe(1);
        deferred[0].MessageId.ShouldBe("m-1");

        await receiver.CompleteMessageAsync(deferred[0]);
        (await harness.Store.CountAsync("defer-flow")).ShouldBe(0L);
    }

    [Fact]
    public async Task Abandoning_a_deferred_received_message_keeps_it_in_Deferred_state()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "defer-abandon" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("defer-abandon");
        await sender.SendMessageAsync(new ServiceBusMessage("p") { MessageId = "id" });

        var receiver = client.CreateReceiver("defer-abandon");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        var seq = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        // Retrieve, then abandon (via update-disposition).
        var deferred = await receiver.ReceiveDeferredMessagesAsync(new[] { seq });
        await receiver.AbandonMessageAsync(deferred[0]);

        // Normal receive still doesn't return it.
        var ghost = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        ghost.ShouldBeNull();

        // ReceiveDeferred still does.
        var again = await receiver.ReceiveDeferredMessagesAsync(new[] { seq });
        again.Count.ShouldBe(1);
        await receiver.CompleteMessageAsync(again[0]);
    }

    [Fact]
    public async Task Dead_lettering_a_deferred_received_message_routes_it_to_the_DLQ()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "defer-dlq" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("defer-dlq");
        await sender.SendMessageAsync(new ServiceBusMessage("p") { MessageId = "doomed" });

        var receiver = client.CreateReceiver("defer-dlq");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        var seq = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        var deferred = await receiver.ReceiveDeferredMessagesAsync(new[] { seq });
        await receiver.DeadLetterMessageAsync(deferred[0], "DeferredAndRejected", "Manual reject after defer");

        // Main queue empty, DLQ has the message with reason populated.
        (await harness.Store.CountAsync("defer-dlq")).ShouldBe(0L);

        var dlqReceiver = client.CreateReceiver("defer-dlq", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("doomed");
        dlqMsg.DeadLetterReason.ShouldBe("DeferredAndRejected");
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }
}
