using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// ReceiveAndDelete mode: the SDK opens the link with snd-settle-mode = settled. The broker
/// must auto-settle each message on send so the lock doesn't expire and re-deliver.
/// </summary>
public class ReceiveAndDeleteTests
{
    [Fact]
    public async Task ReceiveMessageAsync_InReceiveAndDeleteMode_ConsumesMessagesWithoutRedeliveryAfterLockDuration()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "rad", LockDuration = TimeSpan.FromSeconds(2) });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("rad");
        await sender.SendMessageAsync(new ServiceBusMessage("one") { MessageId = "id-1" });
        await sender.SendMessageAsync(new ServiceBusMessage("two") { MessageId = "id-2" });
        await sender.SendMessageAsync(new ServiceBusMessage("three") { MessageId = "id-3" });
        var receiver = client.CreateReceiver("rad", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });

        // Act
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var third = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(3000);
        var ghost = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));

        // Assert
        first.ShouldNotBeNull();
        first.MessageId.ShouldBe("id-1");
        second.ShouldNotBeNull();
        second.MessageId.ShouldBe("id-2");
        third.ShouldNotBeNull();
        third.MessageId.ShouldBe("id-3");
        ghost.ShouldBeNull("ReceiveAndDelete must not redeliver after lock-duration");
        (await harness.Store.CountAsync("rad")).ShouldBe(0L);
    }
}
