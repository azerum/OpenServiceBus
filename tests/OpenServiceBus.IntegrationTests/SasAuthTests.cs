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
    private const string CorrectKey = "SAS_KEY_VALUE"; // same as the default emulator-mode key

    [Fact]
    public async Task When_RequireSasAuth_is_off_any_token_is_accepted_default_behaviour()
    {
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            // RequireSasAuth left at default (false).
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "open" });

        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("open");
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { MessageId = "id-1" });

        (await harness.Store.CountAsync("open")).ShouldBe(1L);
    }

    [Fact]
    public async Task SDK_succeeds_when_RequireSasAuth_is_on_and_the_key_matches()
    {
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            opts.RequireSasAuth = true;
            opts.SasKeys[KeyName] = CorrectKey;
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "authed" });

        // The default IntegrationHarness ConnectionString uses key="SAS_KEY_VALUE" — matches.
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("authed");
        await sender.SendMessageAsync(new ServiceBusMessage("hi") { MessageId = "id-1" });

        (await harness.Store.CountAsync("authed")).ShouldBe(1L);
    }

    [Fact]
    public async Task SDK_fails_when_RequireSasAuth_is_on_and_the_key_mismatches()
    {
        await using var harness = await IntegrationHarness.StartAsync(opts =>
        {
            opts.RequireSasAuth = true;
            opts.SasKeys[KeyName] = "DIFFERENT-SERVER-KEY"; // broker expects this
        });
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "blocked" });

        // Client signs with SAS_KEY_VALUE (different from broker) → CBS put-token returns 401.
        // The SDK surfaces this as UnauthorizedAccessException wrapping the 401.
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("blocked");

        var ex = await Should.ThrowAsync<UnauthorizedAccessException>(async () =>
            await sender.SendMessageAsync(new ServiceBusMessage("nope") { MessageId = "x" }));
        ex.Message.ShouldContain("Unauthorized");
    }
}
