using Amqp;
using Amqp.Framing;
using Amqp.Sasl;

namespace OpenServiceBus.Amqp.WireTests;

public class CbsTests
{
    private static ConnectionFactory CreateClientFactory()
    {
        var factory = new ConnectionFactory();
        factory.SASL.Profile = SaslProfile.Anonymous;
        return factory;
    }

    [Fact]
    public async Task PutToken_ValidRequest_Returns202AcceptedWithCorrelationIdAndKebabCaseKeys()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var replyAddress = "cbs-reply-" + Guid.NewGuid().ToString("N");
            var receiver = new ReceiverLink(session, "cbs-receiver", new Attach
            {
                Source = new Source { Address = "$cbs" },
                Target = new Target { Address = replyAddress },
            }, null);
            receiver.SetCredit(10, true);
            var sender = new SenderLink(session, "cbs-sender", new Attach
            {
                Source = new Source { Address = replyAddress },
                Target = new Target { Address = "$cbs" },
            }, null);
            var requestId = Guid.NewGuid().ToString("N");
            var request = new Message("sas-token-payload")
            {
                Properties = new Properties
                {
                    MessageId = requestId,
                    ReplyTo = replyAddress,
                },
                ApplicationProperties = new ApplicationProperties(),
            };
            request.ApplicationProperties["operation"] = "put-token";
            request.ApplicationProperties["type"] = "servicebus.windows.net:sastoken";
            request.ApplicationProperties["name"] = "amqp://localhost/myqueue";
            request.ApplicationProperties["expiration"] = DateTime.UtcNow.AddHours(1);

            // Act
            await sender.SendAsync(request);
            var response = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

            // Assert
            response.ShouldNotBeNull("expected a CBS put-token response within 5s");
            receiver.Accept(response);

            response.Properties.ShouldNotBeNull();
            response.Properties.CorrelationId.ShouldBe(requestId);

            response.ApplicationProperties.ShouldNotBeNull();
            response.ApplicationProperties.Map.ContainsKey("status-code").ShouldBeTrue("response must use kebab-case 'status-code'");
            response.ApplicationProperties.Map.ContainsKey("status-description").ShouldBeTrue("response must use kebab-case 'status-description'");
            response.ApplicationProperties.Map.ContainsKey("statusCode").ShouldBeFalse("response must NOT use camelCase 'statusCode' for CBS");

            ((int)response.ApplicationProperties["status-code"]).ShouldBe(202);
            ((string)response.ApplicationProperties["status-description"]).ShouldBe("Accepted");

            await sender.CloseAsync();
            await receiver.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    [Fact]
    public async Task PutToken_FiveSequentialRequests_EchoesCorrelationIdForEachOne()
    {
        // Arrange
        await using var harness = await TestListenerHarness.StartAsync();
        var factory = CreateClientFactory();
        var conn = await factory.CreateAsync(new Address(harness.AmqpUri));
        try
        {
            var session = new Session(conn);
            var replyAddress = "cbs-reply-" + Guid.NewGuid().ToString("N");
            var receiver = new ReceiverLink(session, "cbs-receiver", new Attach
            {
                Source = new Source { Address = "$cbs" },
                Target = new Target { Address = replyAddress },
            }, null);
            receiver.SetCredit(10, true);
            var sender = new SenderLink(session, "cbs-sender", new Attach
            {
                Source = new Source { Address = replyAddress },
                Target = new Target { Address = "$cbs" },
            }, null);

            // Act + Assert (per-iteration verification)
            for (var i = 0; i < 5; i++)
            {
                var msgId = $"req-{i}-{Guid.NewGuid():N}";
                var req = new Message("payload")
                {
                    Properties = new Properties { MessageId = msgId, ReplyTo = replyAddress },
                    ApplicationProperties = new ApplicationProperties(),
                };
                req.ApplicationProperties["operation"] = "put-token";
                await sender.SendAsync(req);
                var resp = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
                resp.ShouldNotBeNull();
                receiver.Accept(resp);
                resp.Properties.CorrelationId.ShouldBe(msgId, $"iteration {i}");
            }

            await sender.CloseAsync();
            await receiver.CloseAsync();
            await session.CloseAsync();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }
}
