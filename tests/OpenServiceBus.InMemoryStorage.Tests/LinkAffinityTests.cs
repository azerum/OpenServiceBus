namespace OpenServiceBus.InMemoryStorage.Tests;

/// <summary>
/// When a lock is taken from a specific receiver link, only that link's $management session
/// can renew it. Cross-link renew attempts fail (lock-lost). Matches Service Bus's lock-link
/// affinity rule.
/// </summary>
public class LinkAffinityTests
{
    [Fact]
    public async Task TryRenewLockAsync_RequestingLinkMatchesAssociatedLink_Succeeds()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), associatedLinkName: "receiver-A");
        locked.ShouldNotBeNull();

        // Act
        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60), requestingLinkName: "receiver-A");

        // Assert
        renewed.ShouldNotBeNull();
    }

    [Fact]
    public async Task TryRenewLockAsync_DifferentLinkAttemptsToRenew_ReturnsNull()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), associatedLinkName: "receiver-A");
        locked.ShouldNotBeNull();

        // Act
        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60), requestingLinkName: "receiver-B");

        // Assert
        renewed.ShouldBeNull("a different receiver link must not be able to renew another link's lock");
    }

    [Fact]
    public async Task TryRenewLockAsync_NeitherSideSpecifiesLinkName_Succeeds()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        // Act
        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60));

        // Assert
        renewed.ShouldNotBeNull("backwards-compat: no link affinity when neither side provides a name");
    }
}
