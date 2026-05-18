using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Verifies M6 TTL behavior end-to-end through the real Azure SDK:
/// <list type="bullet">
///   <item><c>ServiceBusMessage.TimeToLive</c> sets <c>header.ttl</c>, expired messages drop or dead-letter.</item>
///   <item>The dead-lettered message has <c>DeadLetterReason="TTLExpiredException"</c>.</item>
/// </list>
/// </summary>
public class TtlTests
{
    [Fact]
    public async Task SDK_message_TimeToLive_causes_expiry_with_DeadLettering_enabled()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "ttl-sdk",
            DeadLetteringOnMessageExpiration = true,
        });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("ttl-sdk");
        await sender.SendMessageAsync(new ServiceBusMessage("perishable")
        {
            MessageId = "expire-1",
            TimeToLive = TimeSpan.FromMilliseconds(200),
        });

        // Give the sweeper a beat to fire (sweep interval is 500ms; allow up to 2s).
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline && await harness.Store.CountAsync("ttl-sdk/$DeadLetterQueue") == 0)
        {
            await Task.Delay(100);
        }

        // Main queue is empty, DLQ has the expired message.
        (await harness.Store.CountAsync("ttl-sdk")).ShouldBe(0L);

        var dlqReceiver = client.CreateReceiver("ttl-sdk", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("expire-1");
        dlqMsg.DeadLetterReason.ShouldBe("TTLExpiredException");
        dlqMsg.DeadLetterSource.ShouldBe("ttl-sdk");

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task SDK_message_TimeToLive_drops_the_message_without_DLQ_setting()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ttl-drop" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("ttl-drop");
        await sender.SendMessageAsync(new ServiceBusMessage("perishable")
        {
            MessageId = "drop-1",
            TimeToLive = TimeSpan.FromMilliseconds(200),
        });

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline && await harness.Store.CountAsync("ttl-drop") > 0)
        {
            await Task.Delay(100);
        }

        (await harness.Store.CountAsync("ttl-drop")).ShouldBe(0L);
        (await harness.Store.CountAsync("ttl-drop/$DeadLetterQueue")).ShouldBe(0L);
    }
}
