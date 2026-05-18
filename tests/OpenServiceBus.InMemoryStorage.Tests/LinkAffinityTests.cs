using OpenServiceBus.InMemoryStorage;

namespace OpenServiceBus.InMemoryStorage.Tests;

/// <summary>
/// When a lock is taken from a specific receiver link, only that link's $management session
/// can renew it. Cross-link renew attempts fail (lock-lost). Matches Service Bus's lock-link
/// affinity rule.
/// </summary>
public class LinkAffinityTests
{
    [Fact]
    public async Task TryRenewLock_succeeds_when_requesting_link_matches_associated_link()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), associatedLinkName: "receiver-A");
        locked.ShouldNotBeNull();

        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60), requestingLinkName: "receiver-A");
        renewed.ShouldNotBeNull();
    }

    [Fact]
    public async Task TryRenewLock_returns_null_when_a_different_link_attempts_to_renew()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), associatedLinkName: "receiver-A");
        locked.ShouldNotBeNull();

        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60), requestingLinkName: "receiver-B");
        renewed.ShouldBeNull("a different receiver link must not be able to renew another link's lock");
    }

    [Fact]
    public async Task TryRenewLock_works_when_neither_side_specifies_a_link_name()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60));
        renewed.ShouldNotBeNull("backwards-compat: no link affinity when neither side provides a name");
    }
}
