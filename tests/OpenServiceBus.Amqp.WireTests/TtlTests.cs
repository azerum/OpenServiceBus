using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using OpenServiceBus.Amqp.Queues;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.Amqp.WireTests;

public class TtlTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task Send_PerMessageTtlNoDlqSetting_DropsMessageAfterExpiration()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "ttl-drop" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "ttl-drop");
            var msg = new Message("expires-fast")
            {
                Header = new Header { Ttl = 200 },
                Properties = new Properties { MessageId = "m-1" },
            };

            // Act
            await sender.SendAsync(msg);
            (await harness.Store.CountAsync("ttl-drop")).ShouldBe(1L);
            await Task.Delay(900);

            // Assert
            (await harness.Store.CountAsync("ttl-drop")).ShouldBe(0L, "message should be dropped after TTL expires");

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_PerMessageTtlWithDeadLetteringOnExpiration_MovesExpiredMessageToDlq()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "ttl-dlq",
            DeadLetteringOnMessageExpiration = true,
        });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "ttl-dlq");
            var msg = new Message("expires-fast")
            {
                Header = new Header { Ttl = 200 },
                Properties = new Properties { MessageId = "m-1" },
            };

            // Act
            await sender.SendAsync(msg);
            await Task.Delay(900);
            var dlqReceiver = new ReceiverLink(session, "dlq-r", "ttl-dlq/$DeadLetterQueue");
            var dlqMsg = await dlqReceiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            (await harness.Store.CountAsync("ttl-dlq")).ShouldBe(0L);
            (await harness.Store.CountAsync("ttl-dlq/$DeadLetterQueue")).ShouldBe(1L, "expired message should land in the DLQ");
            dlqMsg.ShouldNotBeNull();
            dlqMsg.Properties?.MessageId.ShouldBe("m-1");
            (dlqMsg.ApplicationProperties["DeadLetterReason"] as string).ShouldBe(QueueReceiverSource.TtlExpiredReason);
            dlqMsg.MessageAnnotations.Map[new Symbol("x-opt-deadletter-source")].ShouldBe("ttl-dlq");

            dlqReceiver.Accept(dlqMsg);
            await dlqReceiver.CloseAsync();
            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_NoPerMessageTtlWithQueueDefault_AppliesQueueDefaultTtl()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "default-ttl",
            DefaultMessageTimeToLive = TimeSpan.FromMilliseconds(200),
        });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "default-ttl");

            // Act
            await sender.SendAsync(new Message("uses-queue-default") { Properties = new Properties { MessageId = "m-1" } });
            (await harness.Store.CountAsync("default-ttl")).ShouldBe(1L);
            await Task.Delay(900);

            // Assert
            (await harness.Store.CountAsync("default-ttl")).ShouldBe(0L);

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_PerMessageTtlShorterThanQueueDefault_PerMessageTtlWins()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "min-wins",
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(10),
        });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "min-wins");

            // Act
            await sender.SendAsync(new Message("short")
            {
                Header = new Header { Ttl = 200 },
                Properties = new Properties { MessageId = "m-1" },
            });
            await Task.Delay(900);

            // Assert
            (await harness.Store.CountAsync("min-wins")).ShouldBe(0L, "per-message TTL of 200ms should win over the 10-minute queue default");

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task Send_MessageWithoutAnyTtl_NeverExpires()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "forever" });
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var sender = new SenderLink(session, "s", "forever");

            // Act
            await sender.SendAsync(new Message("persistent") { Properties = new Properties { MessageId = "m-1" } });
            await Task.Delay(700);

            // Assert
            (await harness.Store.CountAsync("forever")).ShouldBe(1L);

            await sender.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }
}
