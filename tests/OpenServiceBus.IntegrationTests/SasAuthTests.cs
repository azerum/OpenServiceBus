using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// M9 — when <c>RequireSasAuth=true</c>, $cbs put-token validates the SDK's SAS against the
/// configured keys. The SDK derives a SAS from the connection string's SharedAccessKey, so
/// matching keys → connection works; mismatched → SDK gets an auth failure from CBS.
/// </summary>
public class SasAuthTests
{
    private const string KeyName = "RootManageSharedAccessKey";
    private const string CorrectKey = "SAS_KEY_VALUE";

    [Fact]
    public async Task SendMessageAsync_RequireSasAuthDisabled_AcceptsAnyToken()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            // RequireSasAuth left at default (false).
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "open" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("open");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { MessageId = "id-1" });

        // Assert
        (await harness.Store.CountAsync("open")).ShouldBe(1L);
    }

    [Fact]
    public async Task SendMessageAsync_RequireSasAuthEnabledWithMatchingKey_Succeeds()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            opts.RequireSasAuth = true;
            opts.SasKeys[KeyName] = CorrectKey;
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "authed" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("authed");

        // Act
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { MessageId = "id-1" });

        // Assert
        (await harness.Store.CountAsync("authed")).ShouldBe(1L);
    }

    [Fact]
    public async Task SendMessageAsync_RequireSasAuthEnabledWithMismatchedKey_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            opts.RequireSasAuth = true;
            opts.SasKeys[KeyName] = "DIFFERENT-SERVER-KEY";
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "blocked" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("blocked");

        // Act
        var ex = await Should.ThrowAsync<UnauthorizedAccessException>(async () =>
            await sender.SendMessageAsync(new ServiceBusMessage("nope") { MessageId = "x" }));

        // Assert
        ex.Message.ShouldContain("Unauthorized");
    }
}
