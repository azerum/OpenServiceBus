using Azure.Messaging.ServiceBus;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Verifies that the Service Bus SDK reads the broker-stamped system properties
/// (delivery count, enqueued time, sequence number, locked-until) back to non-default values.
/// Before M4 these threw NRE or returned defaults.
/// </summary>
public class SystemPropertiesTests
{
    [Fact]
    public async Task Received_message_exposes_broker_stamped_system_properties()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "sys-props",
            LockDuration = TimeSpan.FromSeconds(45),
        });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var beforeSend = DateTimeOffset.UtcNow;

        var sender = client.CreateSender("sys-props");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "id-1" });

        var receiver = client.CreateReceiver("sys-props", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();

        msg.MessageId.ShouldBe("id-1");
        msg.DeliveryCount.ShouldBe(1, "the Azure SDK reports DeliveryCount as attempts (1-indexed) where wire delivery-count is 0");

        msg.SequenceNumber.ShouldBe(1L);

        msg.EnqueuedTime.ShouldBeGreaterThanOrEqualTo(beforeSend.AddSeconds(-1));
        msg.EnqueuedTime.ShouldBeLessThanOrEqualTo(DateTimeOffset.UtcNow.AddSeconds(1));

        msg.LockedUntil.ShouldBeGreaterThan(DateTimeOffset.UtcNow.AddSeconds(30),
            "configured lock duration is 45s so locked-until should be ~45s out");

        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task Delivery_count_increments_on_abandon_round_trip()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "retry-counts" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("retry-counts");
        await sender.SendMessageAsync(new ServiceBusMessage("retry") { MessageId = "m-1" });

        var receiver = client.CreateReceiver("retry-counts", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        // First attempt: wire delivery-count 0 → SDK reports DeliveryCount 1
        var a = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        a.ShouldNotBeNull();
        a.DeliveryCount.ShouldBe(1);
        await receiver.AbandonMessageAsync(a);

        // Second attempt: wire delivery-count 1 → SDK reports DeliveryCount 2
        var b = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        b.ShouldNotBeNull();
        b.DeliveryCount.ShouldBe(2);
        await receiver.AbandonMessageAsync(b);

        var c = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        c.ShouldNotBeNull();
        c.DeliveryCount.ShouldBe(3);
        await receiver.CompleteMessageAsync(c);
    }
}
