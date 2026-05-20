using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Exercises scheduled-message support through the real Azure SDK:
/// <list type="bullet">
///   <item><c>ServiceBusSender.ScheduleMessageAsync</c> schedules a message for future delivery.</item>
///   <item>The message is invisible to receivers until its scheduled time.</item>
///   <item><c>CancelScheduledMessageAsync</c> removes the message before it activates.</item>
/// </list>
/// </summary>
public class ScheduledMessagesTests
{
    [Fact]
    public async Task ScheduleMessageAsync_FutureDeliveryTime_HidesMessageUntilScheduledTimePassesThenDeliversIt()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "sched" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("sched");
        var receiver = client.CreateReceiver("sched");
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(2);

        // Act
        var seq = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("future") { MessageId = "sched-1" },
            deliverAt);
        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        ServiceBusReceivedMessage? activated = null;
        while (DateTime.UtcNow < deadline && activated is null)
        {
            activated = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        }

        // Assert
        seq.ShouldBeGreaterThan(0L);
        early.ShouldBeNull("scheduled message should not be visible before its time");
        activated.ShouldNotBeNull("scheduled message must become deliverable after its time");
        activated!.MessageId.ShouldBe("sched-1");
        await receiver.CompleteMessageAsync(activated);
    }

    [Fact]
    public async Task CancelScheduledMessageAsync_BeforeActivation_PreventsDelivery()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "cancel-sched" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("cancel-sched");
        var seq = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("never-delivered") { MessageId = "doomed" },
            DateTimeOffset.UtcNow.AddSeconds(2));

        // Act
        await sender.CancelScheduledMessageAsync(seq);
        await Task.Delay(2500);
        var receiver = client.CreateReceiver("cancel-sched");
        var nothing = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));

        // Assert
        (await harness.Store.CountAsync("cancel-sched")).ShouldBe(0L);
        nothing.ShouldBeNull("cancelled scheduled message must never be delivered");
    }
}
