using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.SqliteStorage;
using OpenServiceBus.Testing;

namespace OpenServiceBus.SqliteStorage.Tests;

/// <summary>
/// End-to-end SDK proof that <see cref="SqliteMessageStore"/> works as the broker's backing
/// store. Boots <see cref="OpenServiceBusTestHost"/> with a SQLite (in-memory) factory and
/// drives it via the real <c>Azure.Messaging.ServiceBus</c> client. This is the parity gate
/// — anything an in-memory test does should work here too.
/// </summary>
public class SqliteSdkRoundTripTests
{
    private static Task<OpenServiceBusTestHost> StartHostAsync() => OpenServiceBusTestHost.StartAsync(o =>
    {
        o.StoreFactory = tp => new SqliteMessageStore(
            new SqliteStorageOptions { DataSource = ":memory:" }, tp,
            NullLogger<SqliteMessageStore>.Instance);
    });

    [Fact]
    public async Task SendAndReceive_RoundTripsViaSqliteStore()
    {
        await using var host = await StartHostAsync();
        await host.CreateQueueAsync("q");
        await using var client = new ServiceBusClient(host.ConnectionString);

        await client.CreateSender("q").SendMessageAsync(new ServiceBusMessage("hi") { MessageId = "m1" });
        var receiver = client.CreateReceiver("q");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        msg.ShouldNotBeNull();
        msg.MessageId.ShouldBe("m1");
        msg.Body.ToString().ShouldBe("hi");
        await receiver.CompleteMessageAsync(msg);

        (await host.Store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task AbandonRedeliversWithIncrementedDeliveryCount()
    {
        await using var host = await StartHostAsync();
        await host.CreateQueueAsync("q");
        await using var client = new ServiceBusClient(host.ConnectionString);

        await client.CreateSender("q").SendMessageAsync(new ServiceBusMessage("retry") { MessageId = "r1" });
        var receiver = client.CreateReceiver("q");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        first.DeliveryCount.ShouldBe(1);

        await receiver.AbandonMessageAsync(first);
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        second.ShouldNotBeNull();
        second.MessageId.ShouldBe("r1");
        second.DeliveryCount.ShouldBe(2, "abandon increments the wire delivery-count");
    }

    [Fact]
    public async Task DeadLetter_ExplicitReason_LandsOnDlqWithReasonStamped()
    {
        await using var host = await StartHostAsync();
        await host.CreateQueueAsync("bad");
        await using var client = new ServiceBusClient(host.ConnectionString);

        await client.CreateSender("bad").SendMessageAsync(new ServiceBusMessage("nope") { MessageId = "x" });
        var receiver = client.CreateReceiver("bad");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        await receiver.DeadLetterMessageAsync(msg, deadLetterReason: "broken", deadLetterErrorDescription: "manual");

        var dlqRcv = client.CreateReceiver("bad/$DeadLetterQueue");
        var dlq = await dlqRcv.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        dlq.ShouldNotBeNull();
        dlq.DeadLetterReason.ShouldBe("broken");
        dlq.MessageId.ShouldBe("x");
    }

    [Fact]
    public async Task DuplicateDetection_DropsRepeatMessageId()
    {
        await using var host = await StartHostAsync();
        await host.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "dedup",
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5),
        });
        await using var client = new ServiceBusClient(host.ConnectionString);
        var sender = client.CreateSender("dedup");

        await sender.SendMessageAsync(new ServiceBusMessage("first")  { MessageId = "k" });
        await sender.SendMessageAsync(new ServiceBusMessage("second") { MessageId = "k" });
        await sender.SendMessageAsync(new ServiceBusMessage("third")  { MessageId = "other" });

        (await host.Store.CountAsync("dedup")).ShouldBe(2L, "the second 'k' is silently dropped");
    }

    [Fact]
    public async Task Sessions_TwoMessagesSameSession_DeliveredInOrderToSessionReceiver()
    {
        await using var host = await StartHostAsync();
        await host.Queues.CreateAsync(new QueueDescriptor { Name = "sess", RequiresSession = true });
        await using var client = new ServiceBusClient(host.ConnectionString);
        var sender = client.CreateSender("sess");

        await sender.SendMessageAsync(new ServiceBusMessage("a") { SessionId = "S", MessageId = "1" });
        await sender.SendMessageAsync(new ServiceBusMessage("b") { SessionId = "S", MessageId = "2" });

        var sessionRcv = await client.AcceptSessionAsync("sess", "S");
        var m1 = await sessionRcv.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var m2 = await sessionRcv.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        m1.ShouldNotBeNull();
        m2.ShouldNotBeNull();
        m1.MessageId.ShouldBe("1");
        m2.MessageId.ShouldBe("2");
    }
}
