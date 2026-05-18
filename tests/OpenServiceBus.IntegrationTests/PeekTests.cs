using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// PeekMessage(s) over <c>$management com.microsoft:peek-message</c> — read without locking.
/// Verifies sequence-number stamping and the Scheduled vs Active <c>x-opt-message-state</c>.
/// </summary>
public class PeekTests
{
    [Fact]
    public async Task PeekMessagesAsync_returns_messages_in_sequence_order_without_locking()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "peeky" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("peeky");
        for (var i = 1; i <= 3; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"body-{i}") { MessageId = $"m-{i}" });
        }

        var receiver = client.CreateReceiver("peeky");
        var peeked = await receiver.PeekMessagesAsync(maxMessages: 5);

        peeked.Count.ShouldBe(3);
        peeked.Select(m => m.MessageId).ShouldBe(new[] { "m-1", "m-2", "m-3" });
        peeked.Select(m => m.SequenceNumber).ShouldBe(new[] { 1L, 2L, 3L });
        peeked.All(m => m.State == ServiceBusMessageState.Active).ShouldBeTrue();

        // Peek must not lock — a subsequent receive should still pull #1 first.
        var recv = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        recv.ShouldNotBeNull();
        recv.MessageId.ShouldBe("m-1");
        await receiver.CompleteMessageAsync(recv);
    }

    [Fact]
    public async Task PeekMessagesAsync_reports_Scheduled_message_state()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "peek-sched" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("peek-sched");
        await sender.ScheduleMessageAsync(
            new ServiceBusMessage("future") { MessageId = "soon" },
            DateTimeOffset.UtcNow.AddMinutes(5));

        var receiver = client.CreateReceiver("peek-sched");
        var peeked = await receiver.PeekMessagesAsync(maxMessages: 5);

        peeked.Count.ShouldBe(1);
        peeked[0].MessageId.ShouldBe("soon");
        peeked[0].State.ShouldBe(ServiceBusMessageState.Scheduled);
    }

    [Fact]
    public async Task PeekMessageAsync_returns_null_when_the_queue_is_empty()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "empty-peek" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var receiver = client.CreateReceiver("empty-peek");

        var msg = await receiver.PeekMessageAsync();
        msg.ShouldBeNull();
    }
}
