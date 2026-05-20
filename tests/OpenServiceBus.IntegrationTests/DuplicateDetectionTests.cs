using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// A dup-detection-enabled queue silently drops repeat sends with the same
/// <c>MessageId</c>. The Azure SDK sees a normal "accepted" disposition each time, but
/// only the first message is stored and delivered.
/// </summary>
public class DuplicateDetectionTests
{
    [Fact]
    public async Task SendMessageAsync_SameMessageIdTwice_OnlyTheFirstSurvives()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "deduped",
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5),
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("deduped");

        // Act - three sends, two of which share MessageId "dup".
        await sender.SendMessageAsync(new ServiceBusMessage("first") { MessageId = "dup" });
        await sender.SendMessageAsync(new ServiceBusMessage("second") { MessageId = "dup" });
        await sender.SendMessageAsync(new ServiceBusMessage("unique") { MessageId = "other" });

        // Assert
        (await harness.Store.CountAsync("deduped")).ShouldBe(2L,
            "the duplicate 'dup' must be silently dropped; 'first' and 'unique' remain");

        var receiver = client.CreateReceiver("deduped");
        var seen = new List<string>();
        for (var i = 0; i < 2; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            msg.ShouldNotBeNull();
            seen.Add(msg.Body.ToString());
            await receiver.CompleteMessageAsync(msg);
        }
        seen.ToArray().ShouldBe(new[] { "first", "unique" }, "the second 'dup' send was never enqueued");
    }

    [Fact]
    public async Task SendMessageAsync_DupOnQueueWithoutDetection_BothSurvive()
    {
        // Arrange - same scenario, but the queue is not dup-detect-enabled.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "no-dedup" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("no-dedup");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("a") { MessageId = "same" });
        await sender.SendMessageAsync(new ServiceBusMessage("b") { MessageId = "same" });

        // Assert
        (await harness.Store.CountAsync("no-dedup")).ShouldBe(2L,
            "without RequiresDuplicateDetection the same MessageId is allowed twice");
    }
}
