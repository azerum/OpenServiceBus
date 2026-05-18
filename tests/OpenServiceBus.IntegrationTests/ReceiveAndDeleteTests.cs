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
    public async Task ReceiveAndDelete_consumes_messages_without_redelivery()
    {
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

        // Receive all three — no Complete calls in this mode.
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        first.MessageId.ShouldBe("id-1");

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        second.ShouldNotBeNull();
        second.MessageId.ShouldBe("id-2");

        var third = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        third.ShouldNotBeNull();
        third.MessageId.ShouldBe("id-3");

        // Wait past the 2s lock duration. In PeekLock mode (without our fix) the messages would
        // re-appear; in ReceiveAndDelete they must stay gone.
        await Task.Delay(3000);

        var ghost = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        ghost.ShouldBeNull("ReceiveAndDelete must not redeliver after lock-duration");

        (await harness.Store.CountAsync("rad")).ShouldBe(0L);
    }
}
