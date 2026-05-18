using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.WireTests;

public class QueueReceiveTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task Accept_AfterReceive_RemovesMessageFromTheQueue()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "orders" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "sender", "orders");
            await sender.SendAsync(new Message("payload") { Properties = new Properties { MessageId = "m-1" } });
            (await harness.Store.CountAsync("orders")).ShouldBe(1L);
            var receiver = new ReceiverLink(session, "receiver", "orders");

            // Act
            var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            received.ShouldNotBeNull("expected the message to be delivered");
            receiver.Accept(received);
            await WaitForCountAsync(harness.Store, "orders", expected: 0L, TimeSpan.FromSeconds(2));

            // Assert
            received.Properties?.MessageId.ShouldBe("m-1");
            (received.Body as string).ShouldBe("payload");
            (await harness.Store.CountAsync("orders")).ShouldBe(0L);

            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Release_AfterReceive_MakesMessageAvailableForRedelivery()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "redo" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "sender", "redo");
            await sender.SendAsync(new Message("retry-me") { Properties = new Properties { MessageId = "m-1" } });
            var receiver = new ReceiverLink(session, "receiver", "redo");

            // Act
            var first = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            first.ShouldNotBeNull();
            receiver.Release(first);
            var second = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            second.ShouldNotBeNull("released message should be redelivered");
            receiver.Accept(second);
            await WaitForCountAsync(harness.Store, "redo", expected: 0L, TimeSpan.FromSeconds(2));

            // Assert
            first.Properties?.MessageId.ShouldBe("m-1");
            second.Properties?.MessageId.ShouldBe("m-1");
            (await harness.Store.CountAsync("redo")).ShouldBe(0L);

            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_FullyPopulatedAmqpProperties_RoundTripsAllStandardAndApplicationProperties()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "props" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "sender", "props");
            var sent = new Message("body")
            {
                Properties = new Properties
                {
                    MessageId = "id-42",
                    CorrelationId = "corr-7",
                    ContentType = "application/json",
                    Subject = "the-label",
                    ReplyTo = "reply-here",
                    To = "props",
                    GroupId = "session-A",
                },
                ApplicationProperties = new ApplicationProperties(),
            };
            sent.ApplicationProperties["custom-string"] = "value";
            sent.ApplicationProperties["custom-int"] = 123;
            await sender.SendAsync(sent);
            var receiver = new ReceiverLink(session, "receiver", "props");

            // Act
            var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            received.ShouldNotBeNull();
            received.Properties.MessageId.ShouldBe("id-42");
            received.Properties.CorrelationId.ShouldBe("corr-7");
            ((string)received.Properties.ContentType).ShouldBe("application/json");
            received.Properties.Subject.ShouldBe("the-label");
            received.Properties.ReplyTo.ShouldBe("reply-here");
            received.Properties.To.ShouldBe("props");
            received.Properties.GroupId.ShouldBe("session-A");
            received.ApplicationProperties["custom-string"].ShouldBe("value");
            Convert.ToInt32(received.ApplicationProperties["custom-int"]).ShouldBe(123);

            receiver.Accept(received);
            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task ExpireLocks_AfterLockDurationPasses_MakesMessageAvailableAgain()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var harness = await TestListenerHarness.StartAsync(timeProvider: fakeTime);
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "expiring",
            LockDuration = TimeSpan.FromSeconds(30),
        });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "sender", "expiring");
            await sender.SendAsync(new Message("body") { Properties = new Properties { MessageId = "m-1" } });
            var receiver = new ReceiverLink(session, "receiver", "expiring");
            var first = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
            first.ShouldNotBeNull();
            first.Properties?.MessageId.ShouldBe("m-1");

            // Act
            fakeTime.Advance(TimeSpan.FromMinutes(1));
            harness.Store.ExpireLocks("expiring", fakeTime.GetUtcNow()).ShouldBe(1);
            var redelivered = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            redelivered.ShouldNotBeNull("lock expired → message must be redelivered");
            redelivered.Properties?.MessageId.ShouldBe("m-1");
            receiver.Accept(redelivered);

            await receiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    private static async Task WaitForCountAsync(IMessageStore store, string queue, long expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await store.CountAsync(queue) == expected) return;
            await Task.Delay(20);
        }
        (await store.CountAsync(queue)).ShouldBe(expected, "timed out waiting for count");
    }
}
