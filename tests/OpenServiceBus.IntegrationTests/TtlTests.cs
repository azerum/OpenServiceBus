using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// Verifies TTL behavior end-to-end through the real Azure SDK:
/// <list type="bullet">
///   <item><c>ServiceBusMessage.TimeToLive</c> sets <c>header.ttl</c>, expired messages drop or dead-letter.</item>
///   <item>The dead-lettered message has <c>DeadLetterReason="TTLExpiredException"</c>.</item>
/// </list>
/// </summary>
public class TtlTests
{
    [Fact]
    public async Task SendMessageAsync_TimeToLiveWithDeadLetteringEnabled_ExpiresMessageIntoDlqWithTtlReason()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "ttl-sdk",
            DeadLetteringOnMessageExpiration = true,
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("ttl-sdk");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("perishable")
        {
            MessageId = "expire-1",
            TimeToLive = TimeSpan.FromMilliseconds(200),
        });
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline && await harness.Store.CountAsync("ttl-sdk/$DeadLetterQueue") == 0)
        {
            await Task.Delay(100);
        }
        var dlqReceiver = client.CreateReceiver("ttl-sdk", new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        // Assert
        (await harness.Store.CountAsync("ttl-sdk")).ShouldBe(0L);
        dlqMsg.ShouldNotBeNull();
        dlqMsg.MessageId.ShouldBe("expire-1");
        dlqMsg.DeadLetterReason.ShouldBe("TTLExpiredException");
        dlqMsg.DeadLetterSource.ShouldBe("ttl-sdk");

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task SendMessageAsync_TimeToLiveWithoutDeadLetteringSetting_DropsExpiredMessage()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ttl-drop" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("ttl-drop");

        // Act
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

        // Assert
        (await harness.Store.CountAsync("ttl-drop")).ShouldBe(0L);
        (await harness.Store.CountAsync("ttl-drop/$DeadLetterQueue")).ShouldBe(0L);
    }
}
