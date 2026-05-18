using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class RenewLockTests
{
    [Fact]
    public async Task TryRenewLockAsync_ExistingLock_ExtendsDeadlineToNowPlusDuration()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        var originalUntil = locked.LockedUntil;
        time.Advance(TimeSpan.FromSeconds(10));

        // Act
        var newUntil = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60));

        // Assert
        newUntil.ShouldNotBeNull();
        newUntil!.Value.ShouldBeGreaterThan(originalUntil, "renew must push the deadline beyond the original");
        newUntil.Value.ShouldBe(time.GetUtcNow() + TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task TryRenewLockAsync_UnknownLockToken_ReturnsNull()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        var renewed = await store.TryRenewLockAsync("q", Guid.NewGuid(), TimeSpan.FromSeconds(30));

        // Assert
        renewed.ShouldBeNull();
    }

    [Fact]
    public async Task TryRemoveLockedAsync_LockedMessage_ReleasesLockAndReturnsStoredMessage()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1, 2, 3]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        // Act
        var removed = await store.TryRemoveLockedAsync("q", locked.LockToken);
        var secondRemove = await store.TryRemoveLockedAsync("q", locked.LockToken);

        // Assert
        removed.ShouldNotBeNull();
        removed!.SequenceNumber.ShouldBe(locked.Message.SequenceNumber);
        removed.EncodedMessage.ShouldBe(new byte[] { 1, 2, 3 });
        (await store.CountAsync("q")).ShouldBe(0L, "the message must be gone from the queue");
        secondRemove.ShouldBeNull("the lock token is now invalid");
    }

    [Fact]
    public async Task TryRenewLockAsync_AfterLockExpires_ReturnsNull()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));
        locked.ShouldNotBeNull();
        time.Advance(TimeSpan.FromSeconds(30));
        store.ExpireLocks("q", time.GetUtcNow()).ShouldBe(1);

        // Act
        var renewed = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60));

        // Assert
        renewed.ShouldBeNull();
    }
}
